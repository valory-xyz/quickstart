"""Unit tests for scripts.pearl_migration.

Covers discovery, filesystem copy + collisions, prompts, status helpers,
the orchestrator, and the on-chain transfer wrappers (with mocks). The
goal is 100% line coverage of `scripts.pearl_migration` so the project
keeps satisfying `--cov-fail-under=100`.
"""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import types
from pathlib import Path
from typing import Any, Iterable, Optional

import pytest

from scripts.pearl_migration import detect, filesystem, prompts, status


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_wallet(operate_root: Path, master_eoa: str = "0xeoa") -> None:
    wallets = operate_root / "wallets"
    wallets.mkdir(parents=True, exist_ok=True)
    (wallets / "ethereum.json").write_text(json.dumps({
        "address": master_eoa,
        "safes": {"gnosis": "0xsafe"},
        "safe_chains": ["gnosis"],
        "ledger_type": "ethereum",
    }))
    (wallets / "ethereum.txt").write_text("encrypted-master-key")


def _write_service(
    operate_root: Path,
    config_id: str,
    name: str = "Trader Agent",
    agent_addresses: list[str] | None = None,
) -> Path:
    sdir = operate_root / "services" / config_id
    sdir.mkdir(parents=True, exist_ok=True)
    (sdir / "config.json").write_text(json.dumps({
        "name": name,
        "service_config_id": config_id,
        "agent_addresses": agent_addresses or ["0xagent1"],
        "hash": "bafy-test",
    }))
    (sdir / "persistent_data").mkdir()
    (sdir / "persistent_data" / "log.txt").write_text("hello")
    return sdir


def _write_key(operate_root: Path, addr: str) -> None:
    keys = operate_root / "keys"
    keys.mkdir(parents=True, exist_ok=True)
    (keys / addr).write_text(json.dumps({"address": addr, "private_key": "enc"}))


def _fake_service(
    config_id: str,
    *,
    name: str = "Test Service",
    agent_addresses: list[str] | None = None,
    path: Path | None = None,
    hash_: str = "bafy-test",
    chain_configs: dict | None = None,
) -> Any:
    """Build a duck-typed stand-in for `operate.services.service.Service`.

    The real Service requires a fully-formed config.json (12+ fields, nested
    dataclasses for AgentRelease / ChainConfigs / etc.). For unit tests we
    only need the attributes the migration code actually reads, so a
    `SimpleNamespace` with those fields keeps the fixtures small.
    """
    import types
    return types.SimpleNamespace(
        service_config_id=config_id,
        name=name,
        agent_addresses=list(agent_addresses or []),
        path=path or Path(f"/tmp/{config_id}"),
        hash=hash_,
        chain_configs=chain_configs if chain_configs is not None else {},
    )


@pytest.fixture
def patch_service_load(monkeypatch: pytest.MonkeyPatch):
    """Monkeypatch `detect.Service.load` to return fake services.

    Yields the mapping dict; tests register entries keyed by the source
    directory path. Any path not in the map raises (so missed expectations
    don't silently pass via the swallow-exceptions branch in `services()`).
    """
    registry: dict[Path, Any] = {}

    def _fake_load(path: Path) -> Any:
        resolved = path.resolve() if isinstance(path, Path) else Path(path).resolve()
        if resolved not in registry:
            raise RuntimeError(f"_fake_load: no fake registered for {resolved}")
        return registry[resolved]

    monkeypatch.setattr(detect.Service, "load", staticmethod(_fake_load))
    return registry


# ---------------------------------------------------------------------------
# detect.py
# ---------------------------------------------------------------------------

class TestDiscover:
    def test_noop_when_paths_resolve_equal(self, tmp_path: Path) -> None:
        # Both stores point at the same .operate
        store = tmp_path / ".operate"
        store.mkdir()
        d = detect.discover(quickstart_root=store, pearl_root=store)
        assert d.mode is detect.Mode.NOOP

    def test_fresh_copy_when_pearl_missing(self, tmp_path: Path) -> None:
        qs = tmp_path / "qs/.operate"
        qs.mkdir(parents=True)
        pl = tmp_path / "home/.operate"
        d = detect.discover(quickstart_root=qs, pearl_root=pl)
        assert d.mode is detect.Mode.FRESH_COPY

    def test_fresh_copy_when_pearl_exists_but_empty(self, tmp_path: Path) -> None:
        qs = tmp_path / "qs/.operate"
        qs.mkdir(parents=True)
        pl = tmp_path / "home/.operate"
        pl.mkdir(parents=True)  # exists, but no wallet
        d = detect.discover(quickstart_root=qs, pearl_root=pl)
        assert d.mode is detect.Mode.FRESH_COPY

    def test_merge_when_pearl_has_wallet(self, tmp_path: Path) -> None:
        qs = tmp_path / "qs/.operate"
        qs.mkdir(parents=True)
        pl = tmp_path / "home/.operate"
        pl.mkdir(parents=True)
        _write_wallet(pl)
        d = detect.discover(quickstart_root=qs, pearl_root=pl)
        assert d.mode is detect.Mode.MERGE


class TestListServices:
    def test_lists_only_sc_prefixed_dirs_with_config(
        self, tmp_path: Path, patch_service_load: dict[Path, Any]
    ) -> None:
        root = tmp_path / ".operate"
        _write_service(root, "sc-aaa", name="A", agent_addresses=["0x1", "0x2"])
        _write_service(root, "sc-bbb", name="B", agent_addresses=["0x3"])
        # Decoy: directory not prefixed
        (root / "services" / "garbage").mkdir(parents=True)
        # Decoy: missing config.json
        (root / "services" / "sc-empty").mkdir()

        # Register fakes for the two valid sc-* dirs. The decoys never
        # reach Service.load (filtered out by the prefix / config.json
        # checks in OperateStore.services()).
        patch_service_load[(root / "services/sc-aaa").resolve()] = _fake_service(
            "sc-aaa", name="A", agent_addresses=["0x1", "0x2"],
        )
        patch_service_load[(root / "services/sc-bbb").resolve()] = _fake_service(
            "sc-bbb", name="B", agent_addresses=["0x3"],
        )

        store = detect.OperateStore(root=root.resolve())
        svcs = store.services()
        ids = [s.service_config_id for s in svcs]
        assert ids == ["sc-aaa", "sc-bbb"]
        assert svcs[0].name == "A"
        assert svcs[0].agent_addresses == ["0x1", "0x2"]

    def test_services_swallows_load_errors(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A bad service should be skipped, not abort the whole listing."""
        root = tmp_path / ".operate"
        _write_service(root, "sc-aaa")
        _write_service(root, "sc-bbb")

        good = _fake_service("sc-bbb", name="B")

        def _flaky_load(path: Path) -> Any:
            if path.name == "sc-aaa":
                raise RuntimeError("corrupt config")
            return good

        monkeypatch.setattr(detect.Service, "load", staticmethod(_flaky_load))
        store = detect.OperateStore(root=root.resolve())
        assert [s.service_config_id for s in store.services()] == ["sc-bbb"]


# ---------------------------------------------------------------------------
# filesystem.py
# ---------------------------------------------------------------------------

class TestFreshCopy:
    def test_copies_whole_tree(self, tmp_path: Path) -> None:
        src_root = tmp_path / "src/.operate"
        _write_service(src_root, "sc-aaa", agent_addresses=["0xagent"])
        _write_key(src_root, "0xagent")
        _write_wallet(src_root)
        src = detect.OperateStore(root=src_root.resolve())

        dest = tmp_path / "home/.operate"
        filesystem.fresh_copy_store(src, dest)
        assert (dest / "wallets" / "ethereum.json").exists()
        assert (dest / "services" / "sc-aaa" / "config.json").exists()
        assert (dest / "keys" / "0xagent").exists()

    def test_refuses_existing_dest(self, tmp_path: Path) -> None:
        src_root = tmp_path / "src/.operate"
        src_root.mkdir(parents=True)
        src = detect.OperateStore(root=src_root.resolve())
        dest = tmp_path / "home/.operate"
        dest.mkdir(parents=True)
        with pytest.raises(FileExistsError):
            filesystem.fresh_copy_store(src, dest)


class TestMergeService:
    def _setup(
        self, tmp_path: Path,
    ) -> tuple[detect.OperateStore, detect.OperateStore, Any]:
        src_root = tmp_path / "src/.operate"
        _write_service(src_root, "sc-aaa", agent_addresses=["0xagent1"])
        _write_key(src_root, "0xagent1")
        src = detect.OperateStore(root=src_root.resolve())

        dest_root = tmp_path / "dest/.operate"
        dest_root.mkdir(parents=True)
        _write_wallet(dest_root, master_eoa="0xpearl")
        dest = detect.OperateStore(root=dest_root.resolve())

        svc = _fake_service(
            "sc-aaa",
            agent_addresses=["0xagent1"],
            path=(src_root / "services" / "sc-aaa").resolve(),
        )
        return src, dest, svc

    def test_clean_merge_copies_service_and_key(self, tmp_path: Path) -> None:
        src, dest, svc = self._setup(tmp_path)
        outcome = filesystem.merge_service(svc, src, dest)
        assert outcome.service_copied is True
        assert outcome.keys_copied == ["0xagent1"]
        assert (dest.services_dir / "sc-aaa" / "config.json").exists()
        assert (dest.keys_dir / "0xagent1").exists()

    def test_collision_skip(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        src, dest, svc = self._setup(tmp_path)
        # Pre-create a colliding service dir.
        (dest.services_dir / "sc-aaa").mkdir(parents=True)
        (dest.services_dir / "sc-aaa" / "marker.txt").write_text("dest-original")
        # And a colliding key.
        (dest.keys_dir).mkdir(parents=True, exist_ok=True)
        (dest.keys_dir / "0xagent1").write_text("dest-key")

        # Always pick "skip".
        monkeypatch.setattr(
            filesystem, "collision",
            lambda target, kind: prompts.CollisionChoice.SKIP,
        )

        outcome = filesystem.merge_service(svc, src, dest)
        assert outcome.service_skipped is True
        assert outcome.keys_skipped == ["0xagent1"]
        assert (dest.services_dir / "sc-aaa" / "marker.txt").read_text() == "dest-original"
        assert (dest.keys_dir / "0xagent1").read_text() == "dest-key"
        assert outcome.backups_made == []

    def test_collision_overwrite_with_backup(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        src, dest, svc = self._setup(tmp_path)
        (dest.services_dir / "sc-aaa").mkdir(parents=True)
        (dest.services_dir / "sc-aaa" / "marker.txt").write_text("dest-original")
        (dest.keys_dir).mkdir(parents=True, exist_ok=True)
        (dest.keys_dir / "0xagent1").write_text("dest-key")

        monkeypatch.setattr(
            filesystem, "collision",
            lambda target, kind: prompts.CollisionChoice.OVERWRITE_WITH_BACKUP,
        )

        outcome = filesystem.merge_service(svc, src, dest)
        assert outcome.service_copied is True
        assert outcome.keys_copied == ["0xagent1"]
        # Backups exist for each collision.
        assert len(outcome.backups_made) == 2
        for bak in outcome.backups_made:
            assert bak.exists()
        # Fresh copy in place.
        assert (dest.services_dir / "sc-aaa" / "config.json").exists()

# ---------------------------------------------------------------------------
# prompts.py
# ---------------------------------------------------------------------------

class TestPrompts:
    @pytest.fixture(autouse=True)
    def _attended_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Force ATTENDED=true so the middleware helper goes down the input()
        # path. Tests that want unattended mode override this explicitly.
        monkeypatch.setenv("ATTENDED", "true")

    def test_yes_no_explicit_yes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("builtins.input", lambda _: "y")
        assert prompts.yes_no("?", default=False) is True

    def test_yes_no_explicit_no(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("builtins.input", lambda _: "n")
        assert prompts.yes_no("?", default=True) is False

    def test_yes_no_rejects_garbage_then_accepts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        answers = iter(["maybe", "perhaps", "no"])
        monkeypatch.setattr("builtins.input", lambda _: next(answers))
        assert prompts.yes_no("?", default=True) is False

    def test_yes_no_unattended_returns_default_true(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ATTENDED", "false")
        # No input mock — would hang if middleware were called.
        assert prompts.yes_no("?", default=True) is True

    def test_yes_no_unattended_returns_default_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ATTENDED", "false")
        assert prompts.yes_no("?", default=False) is False

    def test_yes_no_unattended_no_default_returns_true(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # No declared default in unattended mode -> match middleware's
        # "yes to everything" assumption.
        monkeypatch.setenv("ATTENDED", "false")
        assert prompts.yes_no("?") is True

    def test_yes_no_attended_no_default_delegates_to_middleware(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # `default=None` in attended mode -> delegate to middleware loop.
        monkeypatch.setattr("builtins.input", lambda _: "y")
        assert prompts.yes_no("?") is True

    def test_yes_no_attended_default_honors_empty_input(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # With a declared default, empty input picks the default (legacy [Y/n] feel).
        monkeypatch.setattr("builtins.input", lambda _: "")
        assert prompts.yes_no("?", default=True) is True
        assert prompts.yes_no("?", default=False) is False

    def test_yes_no_eof_returns_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Closed/piped stdin in attended mode falls back to the
        declared default — the rename-source and "different machine?"
        prompts at the end of a successful migration both pass an
        explicit default, so this avoids a raw EOFError traceback at
        the very end of a working migration."""
        def boom(_: str) -> str:
            raise EOFError
        monkeypatch.setattr("builtins.input", boom)
        assert prompts.yes_no("?", default=True) is True
        assert prompts.yes_no("?", default=False) is False

    def test_collision_eof_defaults_to_skip(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """`collision()` is invoked from `merge_service` AFTER on-chain
        ops have committed. A raw EOFError traceback here would abort
        the for-loop, skip drain + rename, and leave the user with no
        summary. Default to SKIP (the safe choice) and warn so it's
        visible."""
        def boom(_: str) -> str:
            raise EOFError
        monkeypatch.setattr("builtins.input", boom)
        result = prompts.collision(tmp_path / "x", kind="key")
        assert result == prompts.CollisionChoice.SKIP
        assert "stdin closed" in capsys.readouterr().out

    def test_collision_invalid_input_re_prompts(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Invalid input must surface a hint rather than silently
        re-prompt forever — easier to spot mistyped input."""
        answers = iter(["3", "foo", "1"])
        monkeypatch.setattr("builtins.input", lambda _: next(answers))
        result = prompts.collision(tmp_path / "x", kind="key")
        assert result == prompts.CollisionChoice.SKIP
        out = capsys.readouterr().out
        assert "Invalid choice" in out

    def test_ask_password_validating_succeeds_within_attempts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        answers = iter(["wrong", "right"])
        monkeypatch.setattr(prompts, "password", lambda _: next(answers))
        result = prompts.ask_password_validating(
            prompt="pw: ",
            validate=lambda p: p == "right",
            attempts=3,
        )
        assert result == "right"

    def test_ask_password_validating_returns_none_on_exhaustion(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(prompts, "password", lambda _: "wrong")
        result = prompts.ask_password_validating(
            prompt="pw: ",
            validate=lambda _: False,
            attempts=2,
        )
        assert result is None


# ---------------------------------------------------------------------------
# status.py (the OS-level bits — no mocking of the chain here)
# ---------------------------------------------------------------------------

class TestStatusOSChecks:
    def test_pearl_daemon_running_false_on_unused_port(self) -> None:
        # Port 1 is privileged + nothing listens on it under tests.
        assert status.pearl_daemon_running(port=1) is False

    def test_is_root_owned_false_for_user_path(self, tmp_path: Path) -> None:
        f = tmp_path / "x"
        f.write_text("hi")
        assert status.is_root_owned(f) is False

    def test_is_root_owned_false_for_missing_path(self, tmp_path: Path) -> None:
        assert status.is_root_owned(tmp_path / "doesnt-exist") is False

    def test_any_root_owned_under_handles_missing(self, tmp_path: Path) -> None:
        assert status.any_root_owned_under(tmp_path / "doesnt-exist") is False

    def test_pearl_daemon_running_true_when_listening(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Stand up a real localhost listener — conftest allows 127.0.0.1.
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        try:
            assert status.pearl_daemon_running(port=port) is True
        finally:
            srv.close()

    def test_docker_quickstart_containers_returns_intersection(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        completed = subprocess.CompletedProcess(
            args=[], returncode=0,
            stdout="abci0\nnode0\nrandom_other\n",
        )
        monkeypatch.setattr(status.subprocess, "run", lambda *a, **k: completed)
        assert status.docker_quickstart_containers() == ["abci0", "node0"]

    def test_docker_quickstart_containers_handles_missing_docker(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def boom(*_a: Any, **_k: Any) -> None:
            raise FileNotFoundError("docker not installed")
        monkeypatch.setattr(status.subprocess, "run", boom)
        assert status.docker_quickstart_containers() == []

    def test_docker_quickstart_containers_propagates_timeout(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A hung docker daemon must NOT be silently mistaken for "no containers".
        def boom(*_a: Any, **_k: Any) -> None:
            raise subprocess.TimeoutExpired(cmd="docker", timeout=10)
        monkeypatch.setattr(status.subprocess, "run", boom)
        with pytest.raises(subprocess.TimeoutExpired):
            status.docker_quickstart_containers()

    def test_docker_quickstart_containers_raises_on_nonzero_exit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-zero exit from `docker ps` (permission denied on
        /var/run/docker.sock, daemon refusing) MUST raise. Returning
        `[]` would let callers conclude "no containers" and race a
        still-running deployment."""
        def fake_run(*_a: Any, **_k: Any) -> Any:
            return types.SimpleNamespace(
                returncode=1,
                stdout="",
                stderr="permission denied while trying to connect to "
                       "the Docker daemon socket",
            )
        monkeypatch.setattr(status.subprocess, "run", fake_run)
        with pytest.raises(RuntimeError, match="exited 1"):
            status.docker_quickstart_containers()

    def test_is_root_owned_true(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        f = tmp_path / "x"
        f.write_text("hi")

        class FakeStat:
            st_uid = 0

        monkeypatch.setattr(Path, "stat", lambda self, **kwargs: FakeStat())
        assert status.is_root_owned(f) is True

    def test_is_root_owned_propagates_oserror(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """OSError from stat() (e.g. permission denied) MUST propagate.
        Returning False would let `fix_root_ownership` skip chown and
        the subsequent copytree would corrupt the destination."""
        f = tmp_path / "x"
        f.write_text("hi")
        monkeypatch.setattr(Path, "exists", lambda self, **kwargs: True)
        def boom(self: Path, **kwargs: Any) -> None:
            raise OSError("permission denied")
        monkeypatch.setattr(Path, "stat", boom)
        with pytest.raises(OSError, match="permission denied"):
            status.is_root_owned(f)

    def test_any_root_owned_under_root_itself(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(status, "is_root_owned", lambda p: True)
        assert status.any_root_owned_under(tmp_path) is True

    def test_any_root_owned_under_descendant(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import stat as _statmod
        child = tmp_path / "child"
        child.write_text("x")

        # Root not owned by root, but the child is.
        monkeypatch.setattr(status, "is_root_owned", lambda p: False)

        class FakeStatRoot:
            st_uid = 1000
            st_mode = _statmod.S_IFDIR | 0o755

        class FakeStatChild:
            st_uid = 0
            st_mode = _statmod.S_IFREG | 0o644

        def fake_stat(self: Path, **kwargs: Any) -> Any:
            return FakeStatChild() if self == child else FakeStatRoot()
        monkeypatch.setattr(Path, "stat", fake_stat)
        assert status.any_root_owned_under(tmp_path) is True

    def test_any_root_owned_under_returns_false_when_clean(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Walking the tree to completion without finding any root-owned
        file returns False — covers the loop-falls-through branch.
        Real files owned by the current user (not 0) — no stat() mocking
        needed (rglob calls is_dir() which would otherwise break a stub)."""
        (tmp_path / "a").write_text("hi")
        (tmp_path / "b").write_text("hi")
        monkeypatch.setattr(status, "is_root_owned", lambda p: False)
        assert status.any_root_owned_under(tmp_path) is False

    def test_any_root_owned_under_propagates_descendant_oserror(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A permission error walking the tree means we can't tell
        whether a root-owned file is hiding beneath. Returning False
        would silently skip chown and corrupt the destination copy —
        propagate so `fix_root_ownership` aborts."""
        (tmp_path / "x").write_text("hi")
        monkeypatch.setattr(status, "is_root_owned", lambda p: False)

        original_stat = Path.stat

        def stat_raises_for_child(self: Path, **kwargs: Any) -> Any:
            if self.name == "x":
                raise OSError("permission")
            return original_stat(self, **kwargs)
        monkeypatch.setattr(Path, "stat", stat_raises_for_child)
        with pytest.raises(OSError, match="permission"):
            status.any_root_owned_under(tmp_path)

    def test_any_root_owned_under_propagates_rglob_oserror(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """rglob() failure is the same hazard as descendant stat failure
        — propagate, don't silently report 'no root-owned files'."""
        monkeypatch.setattr(status, "is_root_owned", lambda p: False)

        def rglob_raises(self: Path, pattern: str) -> Iterable[Path]:
            raise OSError("nope")
        monkeypatch.setattr(Path, "rglob", rglob_raises)
        with pytest.raises(OSError, match="nope"):
            status.any_root_owned_under(tmp_path)


# ---------------------------------------------------------------------------
# status.py — on-chain query wrappers (mocked)
# ---------------------------------------------------------------------------

def _install_fake_autonomy(monkeypatch: pytest.MonkeyPatch, registry_obj: Any) -> None:
    """Install a stub `autonomy.chain.base` exposing a `registry_contracts` attribute."""
    autonomy_pkg = types.ModuleType("autonomy")
    autonomy_chain = types.ModuleType("autonomy.chain")
    autonomy_chain_base = types.ModuleType("autonomy.chain.base")
    autonomy_chain_base.registry_contracts = registry_obj  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "autonomy", autonomy_pkg)
    monkeypatch.setitem(sys.modules, "autonomy.chain", autonomy_chain)
    monkeypatch.setitem(sys.modules, "autonomy.chain.base", autonomy_chain_base)


class TestStatusOnChain:
    def test_service_nft_owner_returns_owner(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        instance = types.SimpleNamespace(
            functions=types.SimpleNamespace(
                ownerOf=lambda sid: types.SimpleNamespace(call=lambda: "0xowner"),
            )
        )
        registry = types.SimpleNamespace(
            service_registry=types.SimpleNamespace(
                get_instance=lambda **kw: instance,
            ),
        )
        _install_fake_autonomy(monkeypatch, registry)
        assert status.service_nft_owner(
            ledger_api=object(),
            service_registry_address="0xreg",
            service_id=42,
        ) == "0xowner"

    def test_service_nft_owner_returns_none_on_revert(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from web3.exceptions import ContractLogicError
        instance = types.SimpleNamespace(
            functions=types.SimpleNamespace(
                ownerOf=lambda sid: types.SimpleNamespace(
                    call=lambda: (_ for _ in ()).throw(ContractLogicError("revert: NOT_MINTED")),
                ),
            ),
        )
        registry = types.SimpleNamespace(
            service_registry=types.SimpleNamespace(
                get_instance=lambda **kw: instance,
            ),
        )
        _install_fake_autonomy(monkeypatch, registry)
        assert status.service_nft_owner(
            ledger_api=object(),
            service_registry_address="0xreg",
            service_id=42,
        ) is None

    def test_service_nft_owner_propagates_other_errors(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Network/RPC errors must NOT be collapsed into "owner unknown".
        instance = types.SimpleNamespace(
            functions=types.SimpleNamespace(
                ownerOf=lambda sid: types.SimpleNamespace(
                    call=lambda: (_ for _ in ()).throw(ConnectionError("rpc down")),
                ),
            ),
        )
        registry = types.SimpleNamespace(
            service_registry=types.SimpleNamespace(
                get_instance=lambda **kw: instance,
            ),
        )
        _install_fake_autonomy(monkeypatch, registry)
        with pytest.raises(ConnectionError):
            status.service_nft_owner(
                ledger_api=object(),
                service_registry_address="0xreg",
                service_id=42,
            )

    def test_safe_owners_uses_gnosis_helper(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        operate_pkg = types.ModuleType("operate")
        operate_utils = types.ModuleType("operate.utils")
        operate_gnosis = types.ModuleType("operate.utils.gnosis")
        operate_gnosis.get_owners = lambda ledger_api, safe: ["0xa", "0xb"]  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "operate", operate_pkg)
        monkeypatch.setitem(sys.modules, "operate.utils", operate_utils)
        monkeypatch.setitem(sys.modules, "operate.utils.gnosis", operate_gnosis)

        assert status.safe_owners(ledger_api=object(), safe="0xs") == ["0xa", "0xb"]

    def test_safe_threshold_returns_int(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        instance = types.SimpleNamespace(
            functions=types.SimpleNamespace(
                getThreshold=lambda: types.SimpleNamespace(call=lambda: 2),
            )
        )
        registry = types.SimpleNamespace(
            gnosis_safe=types.SimpleNamespace(
                get_instance=lambda **kw: instance,
            ),
        )
        _install_fake_autonomy(monkeypatch, registry)
        assert status.safe_threshold(ledger_api=object(), safe="0xs") == 2


# ---------------------------------------------------------------------------
# detect.py — additional branches
# ---------------------------------------------------------------------------

class TestDetectExtras:
    def test_list_services_returns_empty_when_no_services_dir(
        self, tmp_path: Path
    ) -> None:
        store = detect.OperateStore(root=tmp_path)
        assert store.services() == []

    def test_list_services_skips_malformed_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sdir = tmp_path / "services" / "sc-bad"
        sdir.mkdir(parents=True)
        (sdir / "config.json").write_text("{ this is not json")

        def _boom(_: Path) -> Any:
            raise RuntimeError("malformed")
        monkeypatch.setattr(detect.Service, "load", staticmethod(_boom))

        store = detect.OperateStore(root=tmp_path)
        assert store.services() == []

    def test_operate_app_lazy_build_and_caching(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`operate_app()` defers OperateApp construction and reuses on repeat calls."""
        instances: list[Any] = []

        class FakeApp:
            def __init__(self, home: Path) -> None:
                self.home = home
                self.password = None
                instances.append(self)

        # Insert a fake `operate.cli` so the lazy `from operate.cli import OperateApp`
        # picks it up.
        import sys, types
        fake_cli = types.ModuleType("operate.cli")
        fake_cli.OperateApp = FakeApp  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "operate.cli", fake_cli)

        store = detect.OperateStore(root=tmp_path)
        # First call with password → constructs and assigns.
        app1 = store.operate_app(password="pw1")
        assert isinstance(app1, FakeApp)
        assert app1.password == "pw1"
        # Second call with a different password → reuses cached app, sets pw.
        app2 = store.operate_app(password="pw2")
        assert app2 is app1
        assert app1.password == "pw2"
        # Third call without a password → still cached, pw unchanged.
        app3 = store.operate_app()
        assert app3 is app1
        assert app1.password == "pw2"
        assert len(instances) == 1

    def test_operate_app_first_call_without_password(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import sys, types
        class FakeApp:
            def __init__(self, home: Path) -> None:
                self.home = home
                self.password = "preset"

        fake_cli = types.ModuleType("operate.cli")
        fake_cli.OperateApp = FakeApp  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "operate.cli", fake_cli)

        store = detect.OperateStore(root=tmp_path)
        app = store.operate_app()  # no password
        assert app.password == "preset"  # didn't overwrite

    def test_discovery_is_noop_property(self, tmp_path: Path) -> None:
        store = detect.OperateStore(root=tmp_path.resolve())
        d = detect.Discovery(quickstart=store, pearl=store, mode=detect.Mode.NOOP)
        assert d.is_noop is True
        # Distinct stores → can construct with non-NOOP mode.
        store2 = detect.OperateStore(root=(tmp_path / "other").resolve())
        d2 = detect.Discovery(quickstart=store, pearl=store2, mode=detect.Mode.FRESH_COPY)
        assert d2.is_noop is False

    def test_discovery_rejects_inconsistent_invariants(self, tmp_path: Path) -> None:
        """Same root + non-NOOP, or different roots + NOOP, both refused."""
        store = detect.OperateStore(root=tmp_path.resolve())
        store2 = detect.OperateStore(root=(tmp_path / "other").resolve())
        with pytest.raises(ValueError, match="expected Mode.NOOP"):
            detect.Discovery(quickstart=store, pearl=store, mode=detect.Mode.MERGE)
        with pytest.raises(ValueError, match="different roots"):
            detect.Discovery(quickstart=store, pearl=store2, mode=detect.Mode.NOOP)

    def test_operate_store_post_init_resolves_relative_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`__post_init__` must turn a relative `root` into an absolute one."""
        monkeypatch.chdir(tmp_path)
        rel = Path("relstore")
        rel.mkdir()
        store = detect.OperateStore(root=rel)
        assert store.root.is_absolute()
        assert store.root == (tmp_path / "relstore").resolve()

    def test_operate_store_post_init_emits_info_when_symlink_resolves(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """When `Path.resolve()` changes the path (e.g. symlink), `__post_init__`
        must emit an informational message naming both paths so subsequent
        log lines / error messages aren't referring to a path the user
        doesn't recognise. Locks the user-visible message contract."""
        real = tmp_path / "real_store"
        real.mkdir()
        link = tmp_path / "link_store"
        link.symlink_to(real)
        store = detect.OperateStore(root=link)
        assert store.root == real.resolve()
        out = capsys.readouterr().out
        assert "Resolved store root" in out
        assert str(link) in out
        assert str(real.resolve()) in out

    def test_operate_store_post_init_resolve_failure_becomes_value_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """OSError from Path.resolve() should become a clean ValueError."""
        def boom(self: Path, *, strict: bool = False) -> Path:
            raise OSError("permission denied: '/some/parent'")
        monkeypatch.setattr(Path, "resolve", boom)
        with pytest.raises(ValueError, match="Cannot resolve store root"):
            detect.OperateStore(root=tmp_path)


# ---------------------------------------------------------------------------
# wallet.py — quickstart-side password alignment
# ---------------------------------------------------------------------------

class TestAlignQuickstartPassword:
    def test_no_op_when_passwords_match(self) -> None:
        from scripts.pearl_migration import wallet as wallet_mod

        update_calls: list[str] = []
        qs_app = types.SimpleNamespace(
            password="same",
            keys_manager=types.SimpleNamespace(path=Path("/tmp/nope")),
        )
        qs_wallet = types.SimpleNamespace(
            update_password=lambda new: update_calls.append(new),
        )
        wallet_mod.align_quickstart_password(
            qs_app=qs_app, qs_wallet=qs_wallet, new_password="same",
        )
        assert update_calls == []
        assert qs_app.password == "same"

    def test_reencrypts_master_and_agent_keys(self, tmp_path: Path) -> None:
        from eth_account import Account
        from operate.keys import Key
        from operate.operate_types import LedgerType
        from scripts.pearl_migration import wallet as wallet_mod

        # Two agent keyfiles encrypted with the OLD password.
        old_pw, new_pw = "old-pw", "new-pw"
        keys_dir = tmp_path / "keys"
        keys_dir.mkdir()
        agent_pks: dict[str, bytes] = {}
        for i in (1, 2):
            raw = bytes([i]) * 32
            addr = Account.from_key(raw).address
            agent_pks[addr] = raw
            keyfile = Account.encrypt(raw, old_pw)
            key = Key(  # type: ignore[call-arg]
                ledger=LedgerType.ETHEREUM,
                address=addr,
                private_key=json.dumps(keyfile),
            )
            (keys_dir / addr).write_text(json.dumps(key.json), encoding="utf-8")
        # A `.bak` sibling — must be skipped by the walker.
        (keys_dir / "stale.bak").write_text("{}", encoding="utf-8")

        master_calls: list[str] = []
        qs_wallet = types.SimpleNamespace(
            update_password=lambda new: master_calls.append(new),
        )
        keys_manager = types.SimpleNamespace(path=keys_dir, password=old_pw)
        qs_app = types.SimpleNamespace(
            password=old_pw, keys_manager=keys_manager,
        )

        wallet_mod.align_quickstart_password(
            qs_app=qs_app, qs_wallet=qs_wallet, new_password=new_pw,
        )

        # Master keyfile re-encrypt was delegated to operate's update_password.
        assert master_calls == [new_pw]
        # qs_app and the keys manager now report the new password.
        assert qs_app.password == new_pw
        assert keys_manager.password == new_pw
        # Each agent keyfile decrypts under the new password to the
        # original raw private key, and rejects the old password.
        for addr, raw in agent_pks.items():
            blob = json.loads((keys_dir / addr).read_text(encoding="utf-8"))
            inner = json.loads(blob["private_key"])
            assert bytes(Account.decrypt(inner, new_pw)) == raw
            with pytest.raises(Exception):
                Account.decrypt(inner, old_pw)

    def test_reencrypt_failure_is_propagated_and_warned(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Failure inside _reencrypt_agent_key MUST propagate so the caller
        aborts the migration rather than half-converting the keys dir."""
        from scripts.pearl_migration import wallet as wallet_mod

        keys_dir = tmp_path / "keys"
        keys_dir.mkdir()
        (keys_dir / "0xagent").write_text("{}", encoding="utf-8")

        warn_calls: list[str] = []
        monkeypatch.setattr(wallet_mod, "warn", lambda msg: warn_calls.append(msg))

        def boom(**kwargs: Any) -> None:
            raise RuntimeError("disk full")
        monkeypatch.setattr(wallet_mod, "_reencrypt_agent_key", boom)

        qs_wallet = types.SimpleNamespace(update_password=lambda new: None)
        qs_app = types.SimpleNamespace(
            password="old",
            keys_manager=types.SimpleNamespace(path=keys_dir, password="old"),
        )
        with pytest.raises(RuntimeError, match="disk full"):
            wallet_mod.align_quickstart_password(
                qs_app=qs_app, qs_wallet=qs_wallet, new_password="new",
            )
        assert any("failed to re-encrypt" in msg for msg in warn_calls)

    def test_skips_walk_when_keys_dir_missing(self, tmp_path: Path) -> None:
        """A quickstart with no agent keys yet (services pre-deploy) must
        still re-encrypt the master keyfile and update qs_app.password."""
        from scripts.pearl_migration import wallet as wallet_mod

        master_calls: list[str] = []
        qs_wallet = types.SimpleNamespace(
            update_password=lambda new: master_calls.append(new),
        )
        keys_manager = types.SimpleNamespace(
            path=tmp_path / "absent_keys", password="old",
        )
        qs_app = types.SimpleNamespace(password="old", keys_manager=keys_manager)
        wallet_mod.align_quickstart_password(
            qs_app=qs_app, qs_wallet=qs_wallet, new_password="new",
        )
        assert master_calls == ["new"]
        assert qs_app.password == "new"
        assert keys_manager.password == "new"


# ---------------------------------------------------------------------------
# filesystem.py — extras
# ---------------------------------------------------------------------------

class TestFilesystemExtras:
    def test_rename_source_for_rollback(self, tmp_path: Path) -> None:
        src_root = tmp_path / ".operate"
        src_root.mkdir()
        store = detect.OperateStore(root=src_root)
        new = filesystem.rename_source_for_rollback(store)
        assert new.exists()
        assert not src_root.exists()
        assert ".operate.migrated.bak." in new.name

    def test_fix_root_ownership_no_services_dir(self, tmp_path: Path) -> None:
        store = detect.OperateStore(root=tmp_path)
        # Should be a no-op, no exceptions.
        filesystem.fix_root_ownership(store)

    def test_fix_root_ownership_empty_services_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """services/ exists but contains no service dirs — chown loop is
        skipped and subprocess.run is never called."""
        (tmp_path / "services").mkdir()
        called: list[Any] = []
        monkeypatch.setattr(
            filesystem.subprocess,
            "run",
            lambda *a, **k: called.append((a, k)),
        )
        store = detect.OperateStore(root=tmp_path)
        filesystem.fix_root_ownership(store)
        assert called == []

    def test_fix_root_ownership_runs_chown(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # fix_root_ownership now always chowns each service dir (detection
        # via Path.rglob is unreliable on Python 3.14 against trees the
        # current user can't traverse, so unconditional chown is safer
        # than risking a mid-copy permission denied).
        pdata = tmp_path / "services" / "sc-aaa" / "persistent_data"
        pdata.mkdir(parents=True)
        invoked: list[list[str]] = []
        def fake_run(cmd: list[str], **kw: Any) -> Any:
            invoked.append(cmd)
            return subprocess.CompletedProcess(args=cmd, returncode=0)
        monkeypatch.setattr(filesystem.subprocess, "run", fake_run)
        store = detect.OperateStore(root=tmp_path)
        filesystem.fix_root_ownership(store)
        assert invoked and "chown" in invoked[0]

    def test_fix_root_ownership_chown_failure_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pdata = tmp_path / "services" / "sc-aaa" / "persistent_data"
        pdata.mkdir(parents=True)
        def fake_run(cmd: list[str], **kw: Any) -> Any:
            raise subprocess.CalledProcessError(1, cmd)
        monkeypatch.setattr(filesystem.subprocess, "run", fake_run)
        store = detect.OperateStore(root=tmp_path.resolve())
        # Must raise — caller would otherwise corrupt the destination.
        with pytest.raises(RuntimeError, match="could not chown"):
            filesystem.fix_root_ownership(store)

    def test_fix_root_ownership_refuses_outside_store(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Construct a service dir whose entry under services/ is itself a
        # symlink to a directory OUTSIDE the store. fix_root_ownership
        # resolves the service dir and refuses if it escapes store root.
        store_root = tmp_path / "store"
        store_root.mkdir()
        (store_root / "services").mkdir()
        outside = tmp_path / "elsewhere"
        outside.mkdir()
        (store_root / "services" / "sc-aaa").symlink_to(outside)
        store = detect.OperateStore(root=store_root.resolve())
        with pytest.raises(RuntimeError, match="not inside store root"):
            filesystem.fix_root_ownership(store)

    def test_merge_service_missing_source_key_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Missing agent key MUST raise `MissingAgentKey` (an OSError
        subclass) so `_run_mode_b`'s existing OSError catch aggregates
        it into `MigrationOutcome.unmigratable`. A silent skip would
        leave the user with an unstartable service after on-chain ops
        have committed."""
        src_root = tmp_path / "src/.operate"
        _write_service(src_root, "sc-aaa", agent_addresses=["0xmissing"])
        # Note: no key file written.
        src = detect.OperateStore(root=src_root.resolve())
        dest_root = tmp_path / "dest/.operate"
        dest_root.mkdir(parents=True)
        dest = detect.OperateStore(root=dest_root.resolve())
        svc = _fake_service(
            "sc-aaa",
            agent_addresses=["0xmissing"],
            path=(src_root / "services" / "sc-aaa").resolve(),
        )
        with pytest.raises(filesystem.MissingAgentKey, match="0xmissing"):
            filesystem.merge_service(svc, src, dest)
        # MissingAgentKey IS-A OSError so the orchestrator's catch
        # handles it generically.
        try:
            filesystem.merge_service(svc, src, dest)
        except OSError:
            pass
        else:
            pytest.fail("MissingAgentKey must be an OSError subclass")


# ---------------------------------------------------------------------------
# prompts.py — extras
# ---------------------------------------------------------------------------

class TestPromptsExtras:
    def test_password_attended_uses_getpass(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Middleware's `ask_or_get_from_env` calls getpass.getpass directly
        # when ATTENDED=true regardless of isatty.
        monkeypatch.setenv("ATTENDED", "true")
        import getpass
        monkeypatch.setattr(getpass, "getpass", lambda prompt: "secret")
        assert prompts.password("pw: ") == "secret"

    def test_password_unattended_reads_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ATTENDED", "false")
        monkeypatch.setenv("OPERATE_PASSWORD", "from-env")
        assert prompts.password("pw: ") == "from-env"

    def test_password_unattended_missing_env_returns_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # raise_if_missing=False in our wrapper -> returns empty string when
        # the env var is unset in unattended mode.
        monkeypatch.setenv("ATTENDED", "false")
        monkeypatch.delenv("OPERATE_PASSWORD", raising=False)
        assert prompts.password("pw: ") == ""

    def test_collision_skip(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("builtins.input", lambda _: "1")
        choice = prompts.collision(target=Path("/x"), kind="service")
        assert choice == prompts.CollisionChoice.SKIP

    def test_collision_overwrite(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("builtins.input", lambda _: "2")
        choice = prompts.collision(target=Path("/x"), kind="service")
        assert choice == prompts.CollisionChoice.OVERWRITE_WITH_BACKUP

    def test_collision_retries_on_garbage(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        answers = iter(["", "??", "2"])
        monkeypatch.setattr("builtins.input", lambda _: next(answers))
        choice = prompts.collision(target=Path("/x"), kind="key")
        assert choice == prompts.CollisionChoice.OVERWRITE_WITH_BACKUP

    def test_info_warn_print(self, capsys: pytest.CaptureFixture[str]) -> None:
        prompts.info("hello")
        prompts.warn("uhoh")
        out = capsys.readouterr().out
        assert "hello" in out and "uhoh" in out

    def test_fatal_exits_with_code(self) -> None:
        with pytest.raises(SystemExit) as excinfo:
            prompts.fatal("bad", code=7)
        assert excinfo.value.code == 7

    def test_backup_suffix_format(self) -> None:
        s = prompts.backup_suffix()
        assert s.startswith("bak.")
        assert s[len("bak."):].isdigit()

    def test_ask_password_prompts_warning_message(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Failures should print the warning, including the "remaining" path.
        answers = iter(["w1", "w2"])
        monkeypatch.setattr(prompts, "password", lambda _: next(answers))
        result = prompts.ask_password_validating(
            prompt="pw: ", validate=lambda _: False, attempts=2,
        )
        assert result is None
        captured = capsys.readouterr().out
        assert "Wrong password" in captured


# ---------------------------------------------------------------------------
# stop.py
# ---------------------------------------------------------------------------

class TestStop:
    def test_stop_via_middleware_calls_into_module(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from scripts.pearl_migration import stop

        called: dict[str, Any] = {}
        fake_quickstart = types.ModuleType("operate.quickstart")
        fake_stop = types.ModuleType("operate.quickstart.stop_service")
        def fake_stop_service(operate: Any, config_path: str) -> None:
            called["operate"] = operate
            called["config_path"] = config_path
        fake_stop.stop_service = fake_stop_service  # type: ignore[attr-defined]
        operate_pkg = sys.modules.get("operate") or types.ModuleType("operate")
        monkeypatch.setitem(sys.modules, "operate", operate_pkg)
        monkeypatch.setitem(sys.modules, "operate.quickstart", fake_quickstart)
        monkeypatch.setitem(
            sys.modules, "operate.quickstart.stop_service", fake_stop,
        )

        stop.stop_via_middleware(operate="OP", config_path="cfg.json")
        assert called == {"operate": "OP", "config_path": "cfg.json"}

    def test_force_remove_known_containers_no_leftovers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from scripts.pearl_migration import stop
        monkeypatch.setattr(stop, "docker_quickstart_containers", lambda: [])
        assert stop.force_remove_known_containers() == []

    def test_force_remove_known_containers_removes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from scripts.pearl_migration import stop
        monkeypatch.setattr(stop, "docker_quickstart_containers", lambda: ["abci0"])
        invoked: list[list[str]] = []
        monkeypatch.setattr(stop.subprocess, "run",
                            lambda cmd, **kw: invoked.append(cmd))
        result = stop.force_remove_known_containers()
        assert result == ["abci0"]
        assert invoked and "rm" in invoked[0]

    def test_force_remove_known_containers_handles_missing_docker(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from scripts.pearl_migration import stop
        monkeypatch.setattr(stop, "docker_quickstart_containers", lambda: ["abci0"])
        def boom(*_a: Any, **_k: Any) -> None:
            raise FileNotFoundError("nope")
        monkeypatch.setattr(stop.subprocess, "run", boom)
        assert stop.force_remove_known_containers() == []


# ---------------------------------------------------------------------------
# transfer.py
# ---------------------------------------------------------------------------

class TestTransfer:
    def test_transfer_service_nft_encodes_and_dispatches(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from scripts.pearl_migration import transfer

        encoded: dict[str, Any] = {}
        instance = types.SimpleNamespace(
            encode_abi=lambda abi_element_identifier, args: (
                encoded.update(name=abi_element_identifier, args=args) or "0xdeadbeef"
            ),
        )
        registry = types.SimpleNamespace(
            service_registry=types.SimpleNamespace(
                get_instance=lambda **kw: instance,
            ),
        )
        _install_fake_autonomy(monkeypatch, registry)

        sent: dict[str, Any] = {}
        operate_pkg = types.ModuleType("operate")
        operate_utils = types.ModuleType("operate.utils")
        operate_gnosis = types.ModuleType("operate.utils.gnosis")
        def fake_send(txd: bytes, safe: str, ledger_api: Any, crypto: Any, to: str) -> str:
            sent.update(txd=txd, safe=safe, to=to)
            return "0xtxhash"
        operate_gnosis.send_safe_txs = fake_send  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "operate", operate_pkg)
        monkeypatch.setitem(sys.modules, "operate.utils", operate_utils)
        monkeypatch.setitem(sys.modules, "operate.utils.gnosis", operate_gnosis)

        result = transfer.transfer_service_nft(
            ledger_api=object(),
            crypto=object(),
            service_registry_address="0xreg",
            qs_master_safe="0xqs",
            pearl_master_safe="0xpl",
            service_id=99,
        )
        assert result == "0xtxhash"
        # `transferFrom`, NOT `safeTransferFrom` — see transfer.py docstring.
        # A regression to safeTransferFrom would silently revert on Safes
        # without `CompatibilityFallbackHandler` (no `onERC721Received`).
        assert encoded == {"name": "transferFrom", "args": ["0xqs", "0xpl", 99]}
        assert sent["safe"] == "0xqs"
        assert sent["to"] == "0xreg"
        assert sent["txd"] == bytes.fromhex("deadbeef")

    def test_swap_service_safe_owner_uses_approve_hash_then_exec_pattern(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Swap MUST be the Safe-A-controlling-Safe-B pattern: qs master
        Safe first calls service Safe.approveHash(inner_tx_hash), then
        calls service Safe.execTransaction(...) with a packed
        approved-hash signature. Calling swapOwner directly from the qs
        master Safe (single send_safe_txs) fails because Gnosis
        swapOwner requires `msg.sender == address(this)` (i.e. it MUST
        go through execTransaction on the service Safe itself)."""
        from scripts.pearl_migration import transfer

        # autonomy.chain.base.registry_contracts.gnosis_safe stub.
        autonomy_pkg = types.ModuleType("autonomy")
        autonomy_chain = types.ModuleType("autonomy.chain")
        autonomy_base = types.ModuleType("autonomy.chain.base")
        encoded: list[tuple[str, list]] = []
        class _FakeInstance:
            def encode_abi(self, *, abi_element_identifier: str, args: list) -> str:
                encoded.append((abi_element_identifier, args))
                # Return distinguishable hex per element so we can verify
                # which calldata went to which send_safe_txs call.
                return {
                    "swapOwner": "0x1100",
                    "approveHash": "0x2200",
                    "execTransaction": "0x3300",
                }[abi_element_identifier]
        class _FakeGnosisSafe:
            @staticmethod
            def get_instance(*, ledger_api: Any, contract_address: str) -> Any:
                assert contract_address == "0xservice"
                return _FakeInstance()
            @staticmethod
            def get_raw_safe_transaction_hash(**kw: Any) -> dict:
                # Reflect args back so the test asserts the inner-tx-hash
                # request is for a self-call on the service Safe with
                # the swapOwner calldata.
                assert kw["contract_address"] == "0xservice"
                assert kw["to_address"] == "0xservice"
                assert kw["data"] == bytes.fromhex("1100")
                return {"tx_hash": "0xabcdef"}
        autonomy_base.registry_contracts = types.SimpleNamespace(  # type: ignore[attr-defined]
            gnosis_safe=_FakeGnosisSafe(),
        )
        monkeypatch.setitem(sys.modules, "autonomy", autonomy_pkg)
        monkeypatch.setitem(sys.modules, "autonomy.chain", autonomy_chain)
        monkeypatch.setitem(sys.modules, "autonomy.chain.base", autonomy_base)

        # operate stubs.
        sends: list[dict[str, Any]] = []
        operate_pkg = types.ModuleType("operate")
        operate_services = types.ModuleType("operate.services")
        operate_protocol = types.ModuleType("operate.services.protocol")
        operate_utils = types.ModuleType("operate.utils")
        operate_gnosis = types.ModuleType("operate.utils.gnosis")
        operate_protocol.get_packed_signature_for_approved_hash = (  # type: ignore[attr-defined]
            lambda *, owners: b"PACKED_SIG_FOR_" + owners[0].encode()
        )
        class _SafeOperation:
            class CALL:
                value = 0
        operate_gnosis.SafeOperation = _SafeOperation  # type: ignore[attr-defined]
        operate_gnosis.get_prev_owner = (  # type: ignore[attr-defined]
            lambda *, ledger_api, safe, owner: "0xprev"
        )
        operate_gnosis.send_safe_txs = (  # type: ignore[attr-defined]
            lambda **kw: sends.append(kw) or "0xtx"
        )
        monkeypatch.setitem(sys.modules, "operate", operate_pkg)
        monkeypatch.setitem(sys.modules, "operate.services", operate_services)
        monkeypatch.setitem(sys.modules, "operate.services.protocol", operate_protocol)
        monkeypatch.setitem(sys.modules, "operate.utils", operate_utils)
        monkeypatch.setitem(sys.modules, "operate.utils.gnosis", operate_gnosis)

        transfer.swap_service_safe_owner(
            ledger_api="LA", crypto="CR",
            service_safe="0xservice",
            old_owner="0xqsafe", new_owner="0xpearl",
        )

        # Three encode_abi calls: swapOwner (for the inner-hash and exec
        # data fields), approveHash, execTransaction.
        assert [name for name, _ in encoded] == [
            "swapOwner", "approveHash", "execTransaction",
        ]
        assert encoded[0][1] == ["0xprev", "0xqsafe", "0xpearl"]
        assert encoded[1][1] == ["0xabcdef"]  # approveHash(inner_tx_hash)
        # execTransaction args: to=service, value=0, data=swapOwner bytes,
        # operation=0, safeTxGas=0, baseGas=0, gasPrice=0, gasToken=zero,
        # refundReceiver=zero, signatures=packed(qs_master_safe).
        exec_args = encoded[2][1]
        assert exec_args[0] == "0xservice"
        assert exec_args[2] == bytes.fromhex("1100")
        assert exec_args[-1] == b"PACKED_SIG_FOR_0xqsafe"

        # Two send_safe_txs calls — both originated by qs master Safe and
        # targeting service Safe.
        assert len(sends) == 2
        assert sends[0]["safe"] == "0xqsafe" and sends[0]["to"] == "0xservice"
        assert sends[0]["txd"] == bytes.fromhex("2200")  # approveHash
        assert sends[1]["safe"] == "0xqsafe" and sends[1]["to"] == "0xservice"
        assert sends[1]["txd"] == bytes.fromhex("3300")  # execTransaction


# ---------------------------------------------------------------------------
# migrate_to_pearl.py — orchestrator
# ---------------------------------------------------------------------------

@pytest.fixture
def orch(monkeypatch: pytest.MonkeyPatch):
    """Fresh import of the orchestrator with `operate.*` stubs in place.

    The orchestrator's lazy `from operate...` imports are resolved by these
    stubs, so tests can run without the real middleware installed.
    """
    # Stub operate.cli.OperateApp + operate.operate_types + operate.ledger.profiles.
    operate_pkg = types.ModuleType("operate")
    operate_cli = types.ModuleType("operate.cli")
    operate_types = types.ModuleType("operate.operate_types")
    operate_ledger_pkg = types.ModuleType("operate.ledger")
    operate_ledger_profiles = types.ModuleType("operate.ledger.profiles")

    class FakeChain:
        GNOSIS = None  # filled in below
        def __init__(self, value: str) -> None:
            self.value = value
            self.name = value.upper()
        def __eq__(self, other: object) -> bool:
            return isinstance(other, FakeChain) and self.value == other.value
        def __hash__(self) -> int:
            return hash(self.value)

    FakeChain.GNOSIS = FakeChain("gnosis")  # type: ignore[assignment]
    FakeChain.OPTIMISM = FakeChain("optimism")  # type: ignore[attr-defined]

    class FakeOnChainState:
        PRE_REGISTRATION = types.SimpleNamespace(name="PRE_REGISTRATION")
        DEPLOYED = types.SimpleNamespace(name="DEPLOYED")

    class FakeLedgerType:
        ETHEREUM = "ethereum"

    operate_types.Chain = FakeChain  # type: ignore[attr-defined]
    operate_types.OnChainState = FakeOnChainState  # type: ignore[attr-defined]
    operate_types.LedgerType = FakeLedgerType  # type: ignore[attr-defined]
    operate_ledger_profiles.CONTRACTS = {  # type: ignore[attr-defined]
        FakeChain.GNOSIS: {"service_registry": "0xreg"},
        FakeChain.OPTIMISM: {"service_registry": "0xreg-o"},
    }
    # OperateApp constructor is replaced per-test via _load_wallet patch below.
    operate_cli.OperateApp = lambda **kw: types.SimpleNamespace()  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "operate", operate_pkg)
    monkeypatch.setitem(sys.modules, "operate.cli", operate_cli)
    monkeypatch.setitem(sys.modules, "operate.operate_types", operate_types)
    monkeypatch.setitem(sys.modules, "operate.ledger", operate_ledger_pkg)
    monkeypatch.setitem(sys.modules, "operate.ledger.profiles", operate_ledger_profiles)

    # Force a fresh import so any cached imports pick up the stubs.
    # `from scripts.pearl_migration import migrate_to_pearl` returns the
    # parent package's cached attribute even after `sys.modules.pop`, so
    # we also delete the attribute on the package object before
    # importing — otherwise a previous test's `m` (with stale Chain
    # binding) silently leaks into this test.
    import scripts.pearl_migration as _pkg
    sys.modules.pop("scripts.pearl_migration.migrate_to_pearl", None)
    if hasattr(_pkg, "migrate_to_pearl"):
        delattr(_pkg, "migrate_to_pearl")
    from scripts.pearl_migration import migrate_to_pearl as m
    return m, FakeChain, FakeOnChainState


class TestParseArgs:
    def test_help(self, orch: Any) -> None:
        m, *_ = orch
        with pytest.raises(SystemExit):
            m._parse_args(["--help"])

    def test_defaults(self, orch: Any) -> None:
        m, *_ = orch
        ns = m._parse_args([])
        assert ns.config_path is None
        assert ns.dry_run is False


class TestSelectServices:
    def test_no_services_aborts(self, orch: Any, tmp_path: Path) -> None:
        m, *_ = orch
        store = detect.OperateStore(root=tmp_path)
        with pytest.raises(SystemExit):
            m._select_services(store, None)

    def test_single_service_auto_selected(
        self, orch: Any, tmp_path: Path, patch_service_load: dict[Path, Any],
    ) -> None:
        m, *_ = orch
        _write_service(tmp_path, "sc-aaa", name="A")
        patch_service_load[(tmp_path / "services/sc-aaa").resolve()] = (
            _fake_service("sc-aaa", name="A")
        )
        store = detect.OperateStore(root=tmp_path)
        sel = m._select_services(store, None)
        assert [s.service_config_id for s in sel] == ["sc-aaa"]

    def test_multi_service_picks_one(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        patch_service_load: dict[Path, Any],
    ) -> None:
        m, *_ = orch
        _write_service(tmp_path, "sc-aaa", name="A")
        _write_service(tmp_path, "sc-bbb", name="B")
        patch_service_load[(tmp_path / "services/sc-aaa").resolve()] = _fake_service("sc-aaa", name="A")
        patch_service_load[(tmp_path / "services/sc-bbb").resolve()] = _fake_service("sc-bbb", name="B")
        store = detect.OperateStore(root=tmp_path)
        monkeypatch.setattr("builtins.input", lambda _: "2")
        sel = m._select_services(store, None)
        assert [s.service_config_id for s in sel] == ["sc-bbb"]

    def test_multi_service_picks_all(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        patch_service_load: dict[Path, Any],
    ) -> None:
        m, *_ = orch
        _write_service(tmp_path, "sc-aaa", name="A")
        _write_service(tmp_path, "sc-bbb", name="B")
        patch_service_load[(tmp_path / "services/sc-aaa").resolve()] = _fake_service("sc-aaa", name="A")
        patch_service_load[(tmp_path / "services/sc-bbb").resolve()] = _fake_service("sc-bbb", name="B")
        store = detect.OperateStore(root=tmp_path)
        monkeypatch.setattr("builtins.input", lambda _: "3")
        sel = m._select_services(store, None)
        assert [s.service_config_id for s in sel] == ["sc-aaa", "sc-bbb"]

    def test_multi_service_retries_on_bad_input(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        patch_service_load: dict[Path, Any],
    ) -> None:
        m, *_ = orch
        _write_service(tmp_path, "sc-aaa", name="A")
        _write_service(tmp_path, "sc-bbb", name="B")
        patch_service_load[(tmp_path / "services/sc-aaa").resolve()] = _fake_service("sc-aaa", name="A")
        patch_service_load[(tmp_path / "services/sc-bbb").resolve()] = _fake_service("sc-bbb", name="B")
        store = detect.OperateStore(root=tmp_path)
        answers = iter(["foo", "9", "1"])
        monkeypatch.setattr("builtins.input", lambda _: next(answers))
        sel = m._select_services(store, None)
        assert [s.service_config_id for s in sel] == ["sc-aaa"]

    def test_multi_service_eof_on_stdin_aborts_cleanly(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        patch_service_load: dict[Path, Any],
    ) -> None:
        """Closed/piped stdin (CI, automation) must NOT spin in `input()`
        forever. Surface as a `fatal()` (sys.exit) instead of an
        EOFError traceback."""
        m, *_ = orch
        _write_service(tmp_path, "sc-aaa", name="A")
        _write_service(tmp_path, "sc-bbb", name="B")
        patch_service_load[(tmp_path / "services/sc-aaa").resolve()] = _fake_service("sc-aaa", name="A")
        patch_service_load[(tmp_path / "services/sc-bbb").resolve()] = _fake_service("sc-bbb", name="B")
        store = detect.OperateStore(root=tmp_path)
        def boom_input(_: str) -> str:
            raise EOFError
        monkeypatch.setattr("builtins.input", boom_input)
        with pytest.raises(SystemExit):
            m._select_services(store, None)

    def test_config_path_matches_by_name(
        self, orch: Any, tmp_path: Path, patch_service_load: dict[Path, Any],
    ) -> None:
        m, *_ = orch
        _write_service(tmp_path, "sc-aaa", name="Trader Agent")
        patch_service_load[(tmp_path / "services/sc-aaa").resolve()] = (
            _fake_service("sc-aaa", name="Trader Agent")
        )
        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({"name": "Trader Agent"}))
        store = detect.OperateStore(root=tmp_path)
        sel = m._select_services(store, str(cfg))
        assert [s.service_config_id for s in sel] == ["sc-aaa"]

    def test_config_path_matches_by_hash(
        self, orch: Any, tmp_path: Path, patch_service_load: dict[Path, Any],
    ) -> None:
        m, *_ = orch
        # Different name in service config — match by hash instead.
        sdir = tmp_path / "services" / "sc-aaa"
        sdir.mkdir(parents=True)
        (sdir / "config.json").write_text(json.dumps({
            "name": "Different Name",
            "agent_addresses": [],
            "hash": "bafy-test",
        }))
        patch_service_load[(tmp_path / "services/sc-aaa").resolve()] = (
            _fake_service("sc-aaa", name="Different Name", hash_="bafy-test")
        )
        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({"name": "Trader Agent", "hash": "bafy-test"}))
        store = detect.OperateStore(root=tmp_path)
        sel = m._select_services(store, str(cfg))
        assert [s.service_config_id for s in sel] == ["sc-aaa"]

    def test_config_path_no_match_aborts(
        self, orch: Any, tmp_path: Path, patch_service_load: dict[Path, Any],
    ) -> None:
        m, *_ = orch
        _write_service(tmp_path, "sc-aaa", name="A")
        patch_service_load[(tmp_path / "services/sc-aaa").resolve()] = (
            _fake_service("sc-aaa", name="A", hash_="bafy-test")
        )
        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({"name": "Z", "hash": "h"}))
        store = detect.OperateStore(root=tmp_path)
        with pytest.raises(SystemExit):
            m._select_services(store, str(cfg))

    def test_config_path_unreadable_aborts(
        self, orch: Any, tmp_path: Path, patch_service_load: dict[Path, Any],
    ) -> None:
        m, *_ = orch
        _write_service(tmp_path, "sc-aaa", name="A")
        patch_service_load[(tmp_path / "services/sc-aaa").resolve()] = (
            _fake_service("sc-aaa", name="A")
        )
        store = detect.OperateStore(root=tmp_path)
        with pytest.raises(SystemExit):
            m._select_services(store, str(tmp_path / "missing.json"))


class TestPreflight:
    def test_noop_exits(self, orch: Any, tmp_path: Path) -> None:
        m, *_ = orch
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=tmp_path),
            pearl=detect.OperateStore(root=tmp_path),
            mode=detect.Mode.NOOP,
        )
        with pytest.raises(SystemExit) as excinfo:
            m._preflight(d)
        assert excinfo.value.code == 0

    def test_pearl_running_aborts(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, *_ = orch
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=tmp_path / "qs"),
            pearl=detect.OperateStore(root=tmp_path / "pl"),
            mode=detect.Mode.FRESH_COPY,
        )
        monkeypatch.setattr(m, "pearl_daemon_running", lambda: True)
        with pytest.raises(SystemExit):
            m._preflight(d)

    def test_with_leftover_containers_warns(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        m, *_ = orch
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=tmp_path / "qs"),
            pearl=detect.OperateStore(root=tmp_path / "pl"),
            mode=detect.Mode.FRESH_COPY,
        )
        monkeypatch.setattr(m, "pearl_daemon_running", lambda: False)
        monkeypatch.setattr(m, "docker_quickstart_containers", lambda: ["abci0"])
        m._preflight(d)
        assert "abci0" in capsys.readouterr().out


class TestLoadWallet:
    def test_no_wallet_aborts(self, orch: Any, tmp_path: Path) -> None:
        m, *_ = orch
        store = detect.OperateStore(root=tmp_path)
        with pytest.raises(SystemExit):
            m._load_wallet(store, "label")

    @staticmethod
    def _stub_master_wallet_manager(
        monkeypatch: pytest.MonkeyPatch, *, valid_password: Optional[str] = None,
    ) -> list[str]:
        """Replace `MasterWalletManager` so `is_password_valid` is observable
        and the constructor never touches disk. Returns the list it appends to."""
        seen: list[str] = []

        class FakeMWM:
            def __init__(self, path: Path) -> None:
                self.path = path

            def is_password_valid(self, pw: str) -> bool:
                seen.append(pw)
                return valid_password is not None and pw == valid_password

        operate_pkg = sys.modules.setdefault("operate", types.ModuleType("operate"))
        operate_wallet = sys.modules.setdefault(
            "operate.wallet", types.ModuleType("operate.wallet"),
        )
        fake_master = types.ModuleType("operate.wallet.master")
        fake_master.MasterWalletManager = FakeMWM  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "operate", operate_pkg)
        monkeypatch.setitem(sys.modules, "operate.wallet", operate_wallet)
        monkeypatch.setitem(sys.modules, "operate.wallet.master", fake_master)
        return seen

    def test_password_failure_aborts_without_building_app(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Wrong password: validator runs but OperateApp is never built."""
        m, *_ = orch
        _write_wallet(tmp_path)
        store = detect.OperateStore(root=tmp_path)

        self._stub_master_wallet_manager(monkeypatch, valid_password=None)
        # OperateApp should never be constructed on a bad password.
        operate_app_calls = []
        operate_cli = sys.modules.setdefault("operate.cli", types.ModuleType("operate.cli"))
        operate_cli.OperateApp = lambda **kw: operate_app_calls.append(kw) or types.SimpleNamespace()  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "operate.cli", operate_cli)

        monkeypatch.setattr(m, "ask_password_validating", lambda **kw: None)
        with pytest.raises(SystemExit):
            m._load_wallet(store, "label")
        assert operate_app_calls == [], (
            "OperateApp must NOT be built when password validation fails — "
            "would otherwise mutate Pearl's .operate (migrations + version backup)."
        )

    def test_happy_path_validates_then_builds_app(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, *_ = orch
        _write_wallet(tmp_path)
        store = detect.OperateStore(root=tmp_path)

        seen = self._stub_master_wallet_manager(monkeypatch, valid_password="thepw")

        fake_wallet = types.SimpleNamespace(name="loaded")
        fake_wm = types.SimpleNamespace(load=lambda lt: fake_wallet)
        fake_app = types.SimpleNamespace(wallet_manager=fake_wm, password=None)
        operate_cli = sys.modules.setdefault("operate.cli", types.ModuleType("operate.cli"))
        operate_cli.OperateApp = lambda **kw: fake_app  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "operate.cli", operate_cli)

        # `ask_password_validating` exercises the validate callable.
        def fake_ask(prompt: str, validate, **kw: Any) -> str:
            assert validate("wrong") is False
            assert validate("thepw") is True
            return "thepw"
        monkeypatch.setattr(m, "ask_password_validating", fake_ask)

        app, wallet = m._load_wallet(store, "label")
        assert wallet is fake_wallet
        assert app.password == "thepw"
        assert seen == ["wrong", "thepw"]


class TestRunModeA:
    def test_dry_run_with_existing_pearl(
        self, orch: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"
        qs_root.mkdir(parents=True)
        pl_root = tmp_path / "pl/.operate"
        pl_root.mkdir(parents=True)
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root),
            pearl=detect.OperateStore(root=pl_root),
            mode=detect.Mode.FRESH_COPY,
        )
        outcome = m._run_mode_a(disc=d, dry_run=True)
        out = capsys.readouterr().out
        assert "[dry-run]" in out
        assert pl_root.exists()  # not actually moved
        # Mode A always returns an empty (is_complete) outcome — guards the
        # uniform-MigrationOutcome contract documented on _run_mode_a.
        assert outcome.is_complete is True
        assert outcome.unmigratable == ()
        assert outcome.drain_failures == ()

    def test_real_run_copies_and_renames(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"
        _write_service(qs_root, "sc-aaa", agent_addresses=["0xa"])
        _write_key(qs_root, "0xa")
        _write_wallet(qs_root)
        pl_root = tmp_path / "pl/.operate"
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root.resolve()),
            pearl=detect.OperateStore(root=pl_root.resolve()),
            mode=detect.Mode.FRESH_COPY,
        )
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: ["abci0"])
        monkeypatch.setattr(m, "fix_root_ownership", lambda store: None)
        outcome = m._run_mode_a(disc=d, dry_run=False)
        assert (pl_root / "wallets" / "ethereum.json").exists()
        # Source has been renamed away.
        assert not qs_root.exists()
        assert outcome.is_complete is True

    def test_dry_run_with_existing_pearl_backup_print(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # Cover the "[dry-run] would back up" branch by ensuring pearl exists.
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"
        qs_root.mkdir(parents=True)
        pl_root = tmp_path / "pl/.operate"
        pl_root.mkdir(parents=True)
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root),
            pearl=detect.OperateStore(root=pl_root),
            mode=detect.Mode.FRESH_COPY,
        )
        m._run_mode_a(disc=d, dry_run=True)
        assert "Would back up" in capsys.readouterr().out

    def test_force_cleanup_failure_is_fatal_in_mode_a(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Mode A has no per-service aggregation: an unstoppable
        deployment MUST be `fatal()`, not silently proceed to copy a
        live `.operate/`."""
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"
        _write_service(qs_root, "sc-aaa", agent_addresses=["0xa"])
        _write_key(qs_root, "0xa")
        _write_wallet(qs_root)
        pl_root = tmp_path / "pl/.operate"
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root.resolve()),
            pearl=detect.OperateStore(root=pl_root.resolve()),
            mode=detect.Mode.FRESH_COPY,
        )
        def boom() -> list:
            raise subprocess.TimeoutExpired(cmd=["docker", "rm"], timeout=30)
        monkeypatch.setattr(m, "force_remove_known_containers", boom)
        monkeypatch.setattr(m, "fix_root_ownership", lambda store: None)
        with pytest.raises(SystemExit):
            m._run_mode_a(disc=d, dry_run=False)

    def test_real_run_backs_up_existing_empty_pearl(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Covers the non-dry-run branch where Pearl dir exists but has no wallet."""
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"
        _write_service(qs_root, "sc-aaa", agent_addresses=["0xa"])
        _write_key(qs_root, "0xa")
        _write_wallet(qs_root)
        pl_root = tmp_path / "pl/.operate"
        pl_root.mkdir(parents=True)  # exists but empty (no wallet)
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root.resolve()),
            pearl=detect.OperateStore(root=pl_root.resolve()),
            mode=detect.Mode.FRESH_COPY,
        )
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        monkeypatch.setattr(m, "fix_root_ownership", lambda store: None)
        m._run_mode_a(disc=d, dry_run=False)
        out = capsys.readouterr().out
        assert "Backed up empty Pearl dir" in out
        assert (pl_root / "wallets" / "ethereum.json").exists()
        # Backup sibling exists
        siblings = list((tmp_path / "pl").iterdir())
        assert any(".bak." in p.name for p in siblings)


class TestStepTerminate:
    """Direct unit tests for `_step_terminate` — narrows the test surface
    so a step-helper signature regression is caught locally rather than
    only when the whole `_migrate_one_service` is exercised."""

    def _manager(
        self, FakeOnChainState: Any, *, state_seq: list, terminate=None,
    ) -> Any:
        states = list(state_seq)
        return types.SimpleNamespace(
            terminate_service_on_chain_from_safe=(
                terminate or (lambda **kw: None)
            ),
            _get_on_chain_state=lambda s, c: states.pop(0) if states else FakeOnChainState.PRE_REGISTRATION,
        )

    def test_skip_when_already_pre_registration(
        self, orch: Any,
    ) -> None:
        m, FakeChain, FakeOnChainState = orch
        signed = {"n": 0}
        manager = self._manager(
            FakeOnChainState, state_seq=[FakeOnChainState.PRE_REGISTRATION],
        )
        m._step_terminate(
            manager=manager, service=object(), sid="sc-aaa", chain_str="gnosis",
            ensure_signable=lambda: signed.update(n=signed["n"] + 1),
            on_chain_state_cls=FakeOnChainState,
        )
        assert signed["n"] == 0   # never signed because already there

    def test_success_path_signs_and_advances_state(
        self, orch: Any,
    ) -> None:
        m, FakeChain, FakeOnChainState = orch
        signed = {"n": 0}
        manager = self._manager(
            FakeOnChainState,
            state_seq=[FakeOnChainState.DEPLOYED, FakeOnChainState.PRE_REGISTRATION],
        )
        m._step_terminate(
            manager=manager, service=object(), sid="sc-aaa", chain_str="gnosis",
            ensure_signable=lambda: signed.update(n=signed["n"] + 1),
            on_chain_state_cls=FakeOnChainState,
        )
        assert signed["n"] == 1   # ensure_signable was called

    def test_terminate_raise_wraps_into_unmigratable(self, orch: Any) -> None:
        m, FakeChain, FakeOnChainState = orch
        def boom(**kw: Any) -> None:
            raise RuntimeError("revert: NotEnoughFunds")
        manager = self._manager(
            FakeOnChainState, state_seq=[FakeOnChainState.DEPLOYED],
            terminate=boom,
        )
        with pytest.raises(m._Unmigratable) as ei:
            m._step_terminate(
                manager=manager, service=object(), sid="sc-aaa", chain_str="gnosis",
                ensure_signable=lambda: None, on_chain_state_cls=FakeOnChainState,
            )
        assert "could not unstake/terminate" in ei.value.reason

    def test_post_terminate_state_read_failure_wraps(self, orch: Any) -> None:
        """Terminate succeeded but the verification RPC raises. Without the
        wrap, the raw exception propagates past `_run_mode_b`'s narrow
        `except _Unmigratable` and aborts the entire batch with on-chain
        state already advanced. The fix converts it to a per-service
        `_Unmigratable` so the next re-run resumes from the next step."""
        m, FakeChain, FakeOnChainState = orch
        calls = {"n": 0}
        def states(s: Any, c: str) -> Any:
            calls["n"] += 1
            if calls["n"] == 1:
                return FakeOnChainState.DEPLOYED  # before terminate
            raise ConnectionError("rpc 502")  # post-terminate read fails
        manager = types.SimpleNamespace(
            terminate_service_on_chain_from_safe=lambda **kw: None,
            _get_on_chain_state=states,
        )
        with pytest.raises(m._Unmigratable) as ei:
            m._step_terminate(
                manager=manager, service=object(), sid="sc-aaa", chain_str="gnosis",
                ensure_signable=lambda: None, on_chain_state_cls=FakeOnChainState,
            )
        assert "post-state verification" in ei.value.reason
        assert "rpc 502" in ei.value.reason

    def test_state_unchanged_after_terminate_raises(self, orch: Any) -> None:
        m, FakeChain, FakeOnChainState = orch
        manager = self._manager(
            FakeOnChainState,
            # before=DEPLOYED, after-terminate also DEPLOYED → invariant broken.
            state_seq=[FakeOnChainState.DEPLOYED, FakeOnChainState.DEPLOYED],
        )
        with pytest.raises(m._Unmigratable) as ei:
            m._step_terminate(
                manager=manager, service=object(), sid="sc-aaa", chain_str="gnosis",
                ensure_signable=lambda: None, on_chain_state_cls=FakeOnChainState,
            )
        assert "expected PRE_REGISTRATION" in ei.value.reason

    @pytest.mark.parametrize("bug_cls", [
        TypeError, AttributeError, NameError, ImportError,
    ])
    def test_programming_bug_propagates_not_wrapped(
        self, orch: Any, bug_cls: type,
    ) -> None:
        """Step exception wrapper must NOT swallow programming bugs as
        'chain failed'. Covers every member of `_PROGRAMMING_BUGS`."""
        m, FakeChain, FakeOnChainState = orch
        def buggy(**kw: Any) -> None:
            raise bug_cls("simulated programming bug")
        manager = self._manager(
            FakeOnChainState, state_seq=[FakeOnChainState.DEPLOYED],
            terminate=buggy,
        )
        with pytest.raises(bug_cls):
            m._step_terminate(
                manager=manager, service=object(), sid="sc-aaa", chain_str="gnosis",
                ensure_signable=lambda: None, on_chain_state_cls=FakeOnChainState,
            )

    @pytest.mark.parametrize("rpc_cls", [
        KeyError,           # web3 / middleware: missing chain metadata
        LookupError,        # eth_abi / contract attribute resolution
        RuntimeError,       # generic chain-side
        ConnectionError,    # transport-layer
    ])
    def test_legitimate_rpc_failures_get_wrapped_not_re_raised(
        self, orch: Any, rpc_cls: type,
    ) -> None:
        """Regression guard: KeyError / LookupError MUST become per-service
        `_Unmigratable`, not crash the whole batch. Round 4 specifically
        removed these from `_PROGRAMMING_BUGS` for this reason; this test
        prevents a future re-widening from regressing the contract."""
        m, FakeChain, FakeOnChainState = orch
        def chain_failure(**kw: Any) -> None:
            raise rpc_cls("simulated chain-side failure")
        manager = self._manager(
            FakeOnChainState, state_seq=[FakeOnChainState.DEPLOYED],
            terminate=chain_failure,
        )
        with pytest.raises(m._Unmigratable):
            m._step_terminate(
                manager=manager, service=object(), sid="sc-aaa", chain_str="gnosis",
                ensure_signable=lambda: None, on_chain_state_cls=FakeOnChainState,
            )


class TestStepTransferNft:
    """Direct unit tests for `_step_transfer_nft` — covers the idempotent
    skip branch (NFT already on Pearl), the unreadable-owner branch (RPC
    error → None), the third-party-owner branch, the success path, and
    the wrap-on-failure branch."""

    @pytest.fixture
    def fake_transfer(self, monkeypatch: pytest.MonkeyPatch):  # noqa: ANN201
        """Install a stub transfer module; return the call recorder."""
        calls: list = []
        def fake_nft(**kw: Any) -> str:
            calls.append(kw); return "0x1"
        fake_mod = types.ModuleType("scripts.pearl_migration.transfer")
        fake_mod.transfer_service_nft = fake_nft  # type: ignore[attr-defined]
        fake_mod.swap_service_safe_owner = lambda **kw: None  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "scripts.pearl_migration.transfer", fake_mod)
        return calls

    def test_skip_when_nft_already_on_pearl(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch, fake_transfer: list,
    ) -> None:
        m, *_ = orch
        signed = {"n": 0}
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xpl")
        m._step_transfer_nft(
            ledger_api="LA",
            qs_wallet=types.SimpleNamespace(crypto="CR"),
            registry_addr="0xreg",
            qs_master_safe="0xqs", pearl_master_safe="0xpl",
            token_id=42, sid="sc-aaa", chain_str="gnosis",
            ensure_signable=lambda: signed.update(n=signed["n"] + 1),
        )
        assert fake_transfer == []
        assert signed["n"] == 0

    def test_owner_unreadable_raises(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch, fake_transfer: list,
    ) -> None:
        m, *_ = orch
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: None)
        with pytest.raises(m._Unmigratable) as ei:
            m._step_transfer_nft(
                ledger_api="LA",
                qs_wallet=types.SimpleNamespace(crypto="CR"),
                registry_addr="0xreg",
                qs_master_safe="0xqs", pearl_master_safe="0xpl",
                token_id=42, sid="sc-aaa", chain_str="gnosis",
                ensure_signable=lambda: None,
            )
        assert "could not read NFT owner" in ei.value.reason

    def test_owner_third_party_raises(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch, fake_transfer: list,
    ) -> None:
        m, *_ = orch
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xstranger")
        with pytest.raises(m._Unmigratable) as ei:
            m._step_transfer_nft(
                ledger_api="LA",
                qs_wallet=types.SimpleNamespace(crypto="CR"),
                registry_addr="0xreg",
                qs_master_safe="0xqs", pearl_master_safe="0xpl",
                token_id=42, sid="sc-aaa", chain_str="gnosis",
                ensure_signable=lambda: None,
            )
        assert "expected quickstart" in ei.value.reason

    def test_success_path_calls_transfer_then_signs(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch, fake_transfer: list,
    ) -> None:
        m, *_ = orch
        signed = {"n": 0}
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xqs")
        m._step_transfer_nft(
            ledger_api="LA",
            qs_wallet=types.SimpleNamespace(crypto="CR"),
            registry_addr="0xreg",
            qs_master_safe="0xqs", pearl_master_safe="0xpl",
            token_id=42, sid="sc-aaa", chain_str="gnosis",
            ensure_signable=lambda: signed.update(n=signed["n"] + 1),
        )
        assert signed["n"] == 1
        assert len(fake_transfer) == 1
        assert fake_transfer[0]["service_id"] == 42

    def test_transfer_failure_wraps_into_unmigratable(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        m, *_ = orch
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xqs")
        fake_mod = types.ModuleType("scripts.pearl_migration.transfer")
        def boom(**kw: Any) -> None:
            raise RuntimeError("nonce stale")
        fake_mod.transfer_service_nft = boom  # type: ignore[attr-defined]
        fake_mod.swap_service_safe_owner = lambda **kw: None  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "scripts.pearl_migration.transfer", fake_mod)
        with pytest.raises(m._Unmigratable) as ei:
            m._step_transfer_nft(
                ledger_api="LA",
                qs_wallet=types.SimpleNamespace(crypto="CR"),
                registry_addr="0xreg",
                qs_master_safe="0xqs", pearl_master_safe="0xpl",
                token_id=42, sid="sc-aaa", chain_str="gnosis",
                ensure_signable=lambda: None,
            )
        assert "NFT transfer failed" in ei.value.reason


class TestStepSwapServiceSafeOwner:
    """Direct unit tests for `_step_swap_service_safe_owner` — covers
    already-swapped skip, owners-unreadable wrap, qs-not-an-owner refuse,
    success path, and the half-state error message after swap failure."""

    @pytest.fixture
    def fake_swap(self, monkeypatch: pytest.MonkeyPatch):  # noqa: ANN201
        calls: list = []
        fake_mod = types.ModuleType("scripts.pearl_migration.transfer")
        fake_mod.transfer_service_nft = lambda **kw: "0x1"  # type: ignore[attr-defined]
        def fake(**kw: Any) -> None:
            calls.append(kw)
        fake_mod.swap_service_safe_owner = fake  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "scripts.pearl_migration.transfer", fake_mod)
        return calls

    def test_skip_when_already_swapped(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch, fake_swap: list,
    ) -> None:
        m, *_ = orch
        signed = {"n": 0}
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xpl"])
        m._step_swap_service_safe_owner(
            ledger_api="LA", qs_wallet=types.SimpleNamespace(crypto="CR"),
            service_safe="0xms", qs_master_safe="0xqs",
            pearl_master_safe="0xpl", sid="sc-aaa", chain_str="gnosis",
            ensure_signable=lambda: signed.update(n=signed["n"] + 1),
        )
        assert fake_swap == []
        assert signed["n"] == 0

    def test_owners_unreadable_wraps(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch, fake_swap: list,
    ) -> None:
        m, *_ = orch
        def boom(**kw: Any) -> Any:
            raise RuntimeError("rpc 502")
        monkeypatch.setattr(m, "safe_owners", boom)
        with pytest.raises(m._Unmigratable) as ei:
            m._step_swap_service_safe_owner(
                ledger_api="LA", qs_wallet=types.SimpleNamespace(crypto="CR"),
                service_safe="0xms", qs_master_safe="0xqs",
                pearl_master_safe="0xpl", sid="sc-aaa", chain_str="gnosis",
                ensure_signable=lambda: None,
            )
        assert "could not read service Safe owners" in ei.value.reason

    def test_qs_not_owner_raises(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch, fake_swap: list,
    ) -> None:
        m, *_ = orch
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xstranger"])
        with pytest.raises(m._Unmigratable) as ei:
            m._step_swap_service_safe_owner(
                ledger_api="LA", qs_wallet=types.SimpleNamespace(crypto="CR"),
                service_safe="0xms", qs_master_safe="0xqs",
                pearl_master_safe="0xpl", sid="sc-aaa", chain_str="gnosis",
                ensure_signable=lambda: None,
            )
        assert "can't swap" in ei.value.reason

    def test_success_path_signs_then_swaps(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch, fake_swap: list,
    ) -> None:
        m, *_ = orch
        signed = {"n": 0}
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xqs"])
        m._step_swap_service_safe_owner(
            ledger_api="LA", qs_wallet=types.SimpleNamespace(crypto="CR"),
            service_safe="0xms", qs_master_safe="0xqs",
            pearl_master_safe="0xpl", sid="sc-aaa", chain_str="gnosis",
            ensure_signable=lambda: signed.update(n=signed["n"] + 1),
        )
        assert signed["n"] == 1 and len(fake_swap) == 1

    def test_swap_failure_wraps_into_half_state_message(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        m, *_ = orch
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xqs"])
        fake_mod = types.ModuleType("scripts.pearl_migration.transfer")
        fake_mod.transfer_service_nft = lambda **kw: "0x1"  # type: ignore[attr-defined]
        def boom(**kw: Any) -> None:
            raise RuntimeError("safe tx revert")
        fake_mod.swap_service_safe_owner = boom  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "scripts.pearl_migration.transfer", fake_mod)
        with pytest.raises(m._Unmigratable) as ei:
            m._step_swap_service_safe_owner(
                ledger_api="LA", qs_wallet=types.SimpleNamespace(crypto="CR"),
                service_safe="0xms", qs_master_safe="0xqs",
                pearl_master_safe="0xpl", sid="sc-aaa", chain_str="gnosis",
                ensure_signable=lambda: None,
            )
        # Half-state message must call out the inconsistency.
        assert "service Safe owner swap failed" in ei.value.reason
        assert "still lists 0xqs as owner" in ei.value.reason


class TestUnmigratableExceptionInit:
    """Verify _Unmigratable.args is populated so traceback/log formatters preserve fields."""

    def test_args_populated(self, orch: Any) -> None:
        m, *_ = orch
        exc = m._Unmigratable(
            service_id="sc-aaa", chain="gnosis", reason="boom",
        )
        # Without __post_init__, the dataclass-synthesized __init__ leaves
        # args=(). With it, the structured fields propagate to Exception.
        assert exc.args == ("sc-aaa", "gnosis", "boom")

    def test_repr_shows_all_fields(self, orch: Any) -> None:
        m, *_ = orch
        exc = m._Unmigratable(
            service_id="sc-aaa", chain="gnosis", reason="boom",
        )
        # The dataclass-synthesized `__repr__` (which overrides Exception's
        # default) renders every field by name.
        r = repr(exc)
        assert "sc-aaa" in r and "gnosis" in r and "boom" in r

    def test_chain_none_keeps_args_aligned_with_init(self, orch: Any) -> None:
        """`chain=None` is part of args even if it's None — args MUST stay
        positionally aligned with `__init__`'s signature so serialization
        round-trips (`cls(*args)`) reconstruct the same object. The round-4
        attempt to filter None broke this contract; round 5 restored it
        + added `__reduce__` so the alignment is explicit."""
        m, *_ = orch
        exc = m._Unmigratable(
            service_id="sc-aaa", chain=None, reason="config malformed",
        )
        assert exc.args == ("sc-aaa", None, "config malformed")
        # str(exc) (custom __str__) elides "on None" so the user-visible
        # message stays clean.
        s = str(exc)
        assert "on None" not in s
        assert "sc-aaa" in s and "config malformed" in s

    def test_serialization_round_trips(self, orch: Any) -> None:
        """`__reduce__` must reconstruct the exception identically — verified
        end-to-end via copy.copy (which uses __reduce__ under the hood).
        Guards against multiprocessing transport breaking on _Unmigratable."""
        import copy
        m, *_ = orch
        original = m._Unmigratable(
            service_id="sc-aaa", chain=None, reason="boom",
        )
        clone = copy.copy(original)
        assert clone == original
        assert clone.service_id == "sc-aaa"
        assert clone.chain is None
        assert clone.reason == "boom"

    @pytest.mark.parametrize("chain", [None, "gnosis"])
    def test_pickle_round_trip(self, chain: Any) -> None:  # noqa: ANN001
        """Stated motivation for `__reduce__` is multiprocessing transport,
        which uses pickle (NOT copy). Verify the actual contract directly,
        for both chain=None and chain set.

        Imports from the canonical module path (not the orch fixture) so
        pickle's class-by-qualname lookup resolves correctly — the orch
        fixture intentionally re-imports the module in some configurations
        and would fail pickle's `cls is sys.modules[mod].cls` identity
        check, which would mask the real contract being tested."""
        import pickle  # noqa: S403 — internal serialization, not untrusted input
        from scripts.pearl_migration.migrate_to_pearl import _Unmigratable
        original = _Unmigratable(
            service_id="sc-aaa", chain=chain, reason="boom",
        )
        clone = pickle.loads(pickle.dumps(original))  # noqa: S301
        assert isinstance(clone, _Unmigratable)
        assert clone == original
        assert clone.service_id == "sc-aaa"
        assert clone.chain == chain
        assert clone.reason == "boom"


class TestMigrateOneService:
    @pytest.fixture(autouse=True)
    def _stub_threshold(self, orch: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default the threshold pre-flight to 1 so individual tests don't all
        have to mock it. Tests that exercise the threshold check override."""
        m, *_ = orch
        monkeypatch.setattr(m, "safe_threshold", lambda **kw: 1)
        # `_migrate_one_service` re-probes containers after force-cleanup
        # to detect the docker-daemon-hang case. Default to "no leftovers"
        # so tests don't all have to stub this themselves.
        monkeypatch.setattr(m, "docker_quickstart_containers", lambda: [])

    def _make_service_obj(
        self, fake_chain_cls: Any, *, multisig: str = "0xms", token: int = 7,
    ) -> Any:
        chain_data = types.SimpleNamespace(token=token, multisig=multisig)
        ledger_config = types.SimpleNamespace(rpc="http://rpc")
        chain_config = types.SimpleNamespace(
            chain_data=chain_data, ledger_config=ledger_config,
        )
        return types.SimpleNamespace(chain_configs={"gnosis": chain_config})

    def _setup_manager(
        self, FakeChain: Any, FakeOnChainState: Any,
        *,
        terminate: Any = None,
        state_seq: Optional[list] = None,
        multisig: str = "0xms",
    ) -> tuple[Any, Any]:
        """Build a manager mock. `state_seq` lets the probe return a different
        state on the second call (after-terminate check)."""
        if terminate is None:
            terminate = lambda **kw: None  # noqa: E731 -- default no-op
        states = list(state_seq or [
            FakeOnChainState.DEPLOYED,         # before terminate
            FakeOnChainState.PRE_REGISTRATION, # after terminate
        ])
        svc_obj = self._make_service_obj(FakeChain, multisig=multisig)
        return svc_obj, types.SimpleNamespace(
            load=lambda service_config_id: svc_obj,
            terminate_service_on_chain_from_safe=terminate,
            _get_on_chain_state=lambda s, c: states.pop(0) if states else FakeOnChainState.PRE_REGISTRATION,
            get_eth_safe_tx_builder=lambda ledger_config: types.SimpleNamespace(
                ledger_api="LA",
            ),
        )

    def test_success(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, FakeChain, FakeOnChainState = orch
        manager_calls: dict[str, Any] = {}

        def fake_terminate(service_config_id: str, chain: str) -> None:
            manager_calls["term"] = chain

        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState, terminate=fake_terminate,
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        qs_wallet = types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"})
        pearl_wallet = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"})

        monkeypatch.setattr(m, "stop_via_middleware",
                            lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        # Idempotency probes: NFT not yet transferred, qs Safe still owns service Safe.
        monkeypatch.setattr(m, "service_nft_owner",
                            lambda **kw: "0xqs")
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xqs"])

        # Stub the lazy transfer-module import.
        fake_transfer = types.ModuleType("scripts.pearl_migration.transfer")
        moves: list[Any] = []
        def fake_nft(**kw: Any) -> str:
            moves.append(("nft", kw)); return "0x1"
        def fake_swap(**kw: Any) -> None:
            moves.append(("swap", kw))
        fake_transfer.transfer_service_nft = fake_nft  # type: ignore[attr-defined]
        fake_transfer.swap_service_safe_owner = fake_swap  # type: ignore[attr-defined]
        monkeypatch.setitem(
            sys.modules, "scripts.pearl_migration.transfer", fake_transfer,
        )

        ref = _fake_service("sc-aaa", name="A", agent_addresses=["0xa"], path=tmp_path)
        m._migrate_one_service(
            svc=ref, qs_app=qs_app, qs_wallet=qs_wallet,
            pearl_wallet=pearl_wallet, config_path="cfg.json",
        )
        assert manager_calls["term"] == "gnosis"
        assert [step for step, _ in moves] == ["nft", "swap"]

    def test_idempotent_skips_when_already_done(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Re-run after partial completion: NFT already on Pearl, Safe owner already swapped."""
        m, FakeChain, FakeOnChainState = orch
        terminate_calls: list[Any] = []

        def fake_terminate(**kw: Any) -> None:
            terminate_calls.append(kw)

        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState,
            terminate=fake_terminate,
            state_seq=[FakeOnChainState.PRE_REGISTRATION],  # already there
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        qs_wallet = types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"})
        pearl_wallet = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"})

        monkeypatch.setattr(m, "stop_via_middleware",
                            lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        # NFT already on Pearl, Safe owner already swapped to Pearl.
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xpl")
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xpl"])

        fake_transfer = types.ModuleType("scripts.pearl_migration.transfer")
        nft_called: list[Any] = []
        swap_called: list[Any] = []
        fake_transfer.transfer_service_nft = lambda **kw: nft_called.append(kw)  # type: ignore[attr-defined]
        fake_transfer.swap_service_safe_owner = lambda **kw: swap_called.append(kw)  # type: ignore[attr-defined]
        monkeypatch.setitem(
            sys.modules, "scripts.pearl_migration.transfer", fake_transfer,
        )

        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        m._migrate_one_service(
            svc=ref, qs_app=qs_app, qs_wallet=qs_wallet,
            pearl_wallet=pearl_wallet, config_path=None,
        )
        # Every on-chain step was probed-and-skipped.
        assert terminate_calls == [], "terminate must be skipped when already PRE_REGISTRATION"
        assert nft_called == [], "transfer_service_nft must be skipped when NFT already on Pearl"
        assert swap_called == [], "swap_service_safe_owner must be skipped when already swapped"

    def test_nft_owner_unreadable_raises_unmigratable(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(FakeChain, FakeOnChainState)
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware",
                            lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: None)  # RPC failure
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xqs"])
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )
        assert ei.value.chain == "gnosis"
        assert "could not read NFT owner" in ei.value.reason

    def test_nft_owner_unexpected_third_party_raises(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(FakeChain, FakeOnChainState)
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        # NFT now lives on some unrelated address.
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xstranger")
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xqs"])
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )
        assert "NFT owner is" in ei.value.reason

    def test_nft_transfer_failure_raises_unmigratable(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(FakeChain, FakeOnChainState)
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xqs")
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xqs"])

        fake_transfer = types.ModuleType("scripts.pearl_migration.transfer")
        def boom(**kw: Any) -> None:
            raise RuntimeError("revert: nonce stale")
        fake_transfer.transfer_service_nft = boom  # type: ignore[attr-defined]
        fake_transfer.swap_service_safe_owner = lambda **kw: None  # type: ignore[attr-defined]
        monkeypatch.setitem(
            sys.modules, "scripts.pearl_migration.transfer", fake_transfer,
        )

        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )
        assert "NFT transfer failed" in ei.value.reason

    def test_swap_failure_raises_unmigratable_post_nft(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Critical: swap fails AFTER NFT transferred -> structured error explains the half-state."""
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(FakeChain, FakeOnChainState)
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xqs")
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xqs"])

        # Track ordering: NFT transfer MUST land before swap raises so the
        # half-state message is true. A future refactor that reorders the
        # steps would make the error message a lie — pin the contract here.
        order: list[str] = []
        fake_transfer = types.ModuleType("scripts.pearl_migration.transfer")
        def fake_nft(**kw: Any) -> str:
            order.append("nft"); return "0x1"
        def swap_boom(**kw: Any) -> None:
            order.append("swap"); raise RuntimeError("safe tx revert")
        fake_transfer.transfer_service_nft = fake_nft  # type: ignore[attr-defined]
        fake_transfer.swap_service_safe_owner = swap_boom  # type: ignore[attr-defined]
        monkeypatch.setitem(
            sys.modules, "scripts.pearl_migration.transfer", fake_transfer,
        )

        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )
        # Half-state must be called out so the user knows what's been moved.
        assert "service multisig" in ei.value.reason
        assert "owner swap failed" in ei.value.reason
        # And the half-state message must actually be true: NFT transferred FIRST.
        assert order == ["nft", "swap"], (
            f"swap must run AFTER nft transfer for the half-state message to be true; saw {order}"
        )

    def test_terminate_failure_raises_unmigratable(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, FakeChain, FakeOnChainState = orch
        def boom(service_config_id: str, chain: str) -> None:
            raise RuntimeError("can't unstake yet")
        # State is DEPLOYED before terminate runs (so the probe says "act").
        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState,
            terminate=boom, state_seq=[FakeOnChainState.DEPLOYED],
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware",
                            lambda operate, config_path: (_ for _ in ()).throw(RuntimeError("stop fails too")))
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xqs")
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xqs"])
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path="cfg.json",
            )
        assert ei.value.chain == "gnosis"
        assert "could not unstake" in ei.value.reason

    def test_wrong_state_after_terminate_raises(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, FakeChain, FakeOnChainState = orch
        # Both probes return DEPLOYED — terminate "succeeds" but state never moves.
        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState,
            state_seq=[FakeOnChainState.DEPLOYED, FakeOnChainState.DEPLOYED],
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware",
                            lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xqs")
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xqs"])
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path="cfg.json",
            )
        assert "expected PRE_REGISTRATION" in ei.value.reason

    def test_safe_owners_unreadable_raises_unmigratable(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(FakeChain, FakeOnChainState)
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        # NFT was already transferred (so we get past step 2), but reading
        # owners fails (RPC down).
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xpl")
        def owners_boom(**kw: Any) -> Any:
            raise RuntimeError("rpc down")
        monkeypatch.setattr(m, "safe_owners", owners_boom)
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )
        assert "could not read service Safe owners" in ei.value.reason

    def test_safe_owner_swap_required_but_qs_not_owner(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Swap can't proceed if the qs Safe was already removed by some third party."""
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(FakeChain, FakeOnChainState)
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        # NFT already on Pearl (so step 2 is skipped without needing real transfer).
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xpl")
        # Service Safe owners: neither qs nor pearl present.
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xstranger"])
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )
        assert "can't swap" in ei.value.reason

    def test_multi_chain_per_iteration_ledger_capture(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Service spans 2 chains: each iteration's `_ledger_api(_cfg=...)`
        default-arg binding must capture THAT iteration's chain_config.

        If the binding regresses to a global `nonlocal` only, both iterations
        would build the ledger from chain N+1's config (Python's late-binding
        in for-loop closures). This test pins the per-chain capture by
        recording which RPC each step's ledger came from.
        """
        m, FakeChain, FakeOnChainState = orch
        # Two distinct chain_configs, distinguished by RPC.
        cc_g = types.SimpleNamespace(
            chain_data=types.SimpleNamespace(token=1, multisig="0xms-g"),
            ledger_config=types.SimpleNamespace(rpc="https://rpc.example/gnosis"),
        )
        cc_o = types.SimpleNamespace(
            chain_data=types.SimpleNamespace(token=2, multisig="0xms-o"),
            ledger_config=types.SimpleNamespace(rpc="https://rpc.example/optimism"),
        )
        svc_obj = types.SimpleNamespace(
            chain_configs={"gnosis": cc_g, "optimism": cc_o},
        )

        # Record which ledger_config each manager.get_eth_safe_tx_builder
        # call was invoked with — that's the proxy for "did the closure
        # capture the per-iteration config?"
        builds: list = []
        manager = types.SimpleNamespace(
            load=lambda service_config_id: svc_obj,
            terminate_service_on_chain_from_safe=lambda **kw: None,
            _get_on_chain_state=lambda s, c: FakeOnChainState.PRE_REGISTRATION,
            get_eth_safe_tx_builder=(
                lambda ledger_config: (
                    builds.append(ledger_config.rpc)
                    or types.SimpleNamespace(ledger_api=f"LA-{ledger_config.rpc}")
                )
            ),
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)

        # NFT already on the per-chain Pearl Safe, Safe already swapped:
        # step 2 + step 3 skip, but step 1 (terminate) still runs and
        # invokes ensure_signable → safe_threshold → _ledger_api per chain.
        # `service_id` lets us return the right per-chain Pearl address
        # (token=1 → gnosis, token=2 → optimism).
        per_chain_pearl = {1: "0xpl-g", 2: "0xpl-o"}
        monkeypatch.setattr(
            m, "service_nft_owner",
            lambda *, ledger_api, service_registry_address, service_id:
                per_chain_pearl[service_id],
        )
        monkeypatch.setattr(
            m, "safe_owners",
            lambda *, ledger_api, safe:
                ["0xpl-g"] if safe == "0xms-g" else ["0xpl-o"],
        )
        # Override the autouse stub so safe_threshold actually runs against
        # the per-chain ledger_api.
        seen_thresholds: list = []
        def threshold_recorder(*, ledger_api: Any, safe: str) -> int:
            seen_thresholds.append(ledger_api)
            return 1
        monkeypatch.setattr(m, "safe_threshold", threshold_recorder)
        # Force a state mismatch so terminate runs — that triggers the
        # threshold guard (which calls _ledger_api → records the build).
        states = iter([
            FakeOnChainState.DEPLOYED, FakeOnChainState.PRE_REGISTRATION,  # gnosis
            FakeOnChainState.DEPLOYED, FakeOnChainState.PRE_REGISTRATION,  # optimism
        ])
        manager._get_on_chain_state = lambda s, c: next(states)  # type: ignore[attr-defined]

        monkeypatch.setattr(m, "stop_via_middleware",
                            lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])

        ref = _fake_service("sc-aaa", name="A", path=tmp_path)
        m._migrate_one_service(
            svc=ref, qs_app=qs_app,
            qs_wallet=types.SimpleNamespace(crypto="CR", safes={
                FakeChain.GNOSIS: "0xqs-g", FakeChain.OPTIMISM: "0xqs-o",
            }),
            pearl_wallet=types.SimpleNamespace(safes={
                FakeChain.GNOSIS: "0xpl-g", FakeChain.OPTIMISM: "0xpl-o",
            }),
            config_path=None,
        )

        # Each chain's ledger_api was built from its OWN rpc. The bug we're
        # guarding against would build both ledgers from the optimism rpc
        # (last iteration's chain_config bound by all closures).
        assert "https://rpc.example/gnosis" in builds
        assert "https://rpc.example/optimism" in builds
        # And the two threshold calls received distinct ledger handles
        # (one per chain).
        assert "LA-https://rpc.example/gnosis" in seen_thresholds
        assert "LA-https://rpc.example/optimism" in seen_thresholds

    def test_multi_chain_steps_2_and_3_use_per_iteration_ledger(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Sibling to the previous test: this time NFT is still on the QS
        Safe and the service Safe is still QS-owned, so step 2 (NFT transfer)
        AND step 3 (Safe owner swap) BOTH run on BOTH chains. Asserts that
        the ledger_api passed into `transfer_service_nft` and
        `swap_service_safe_owner` was the per-chain one — closes the
        coverage gap where the prior test only exercised the threshold
        guard, leaving the late-binding risk on steps 2/3 unverified."""
        m, FakeChain, FakeOnChainState = orch
        cc_g = types.SimpleNamespace(
            chain_data=types.SimpleNamespace(token=1, multisig="0xms-g"),
            ledger_config=types.SimpleNamespace(rpc="https://rpc.example/gnosis"),
        )
        cc_o = types.SimpleNamespace(
            chain_data=types.SimpleNamespace(token=2, multisig="0xms-o"),
            ledger_config=types.SimpleNamespace(rpc="https://rpc.example/optimism"),
        )
        svc_obj = types.SimpleNamespace(
            chain_configs={"gnosis": cc_g, "optimism": cc_o},
        )

        # Each call to get_eth_safe_tx_builder gets a fresh ledger handle
        # tagged with the chain's RPC, so we can fingerprint which one
        # downstream calls received.
        manager = types.SimpleNamespace(
            load=lambda service_config_id: svc_obj,
            terminate_service_on_chain_from_safe=lambda **kw: None,
            _get_on_chain_state=lambda s, c: FakeOnChainState.PRE_REGISTRATION,
            get_eth_safe_tx_builder=(
                lambda ledger_config:
                    types.SimpleNamespace(ledger_api=f"LA-{ledger_config.rpc}")
            ),
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)

        # NFT still owned by per-chain QS Safe -> step 2 must run.
        per_chain_qs = {1: "0xqs-g", 2: "0xqs-o"}
        monkeypatch.setattr(
            m, "service_nft_owner",
            lambda *, ledger_api, service_registry_address, service_id:
                per_chain_qs[service_id],
        )
        # Service Safe still QS-owned -> step 3 must run.
        monkeypatch.setattr(
            m, "safe_owners",
            lambda *, ledger_api, safe:
                ["0xqs-g"] if safe == "0xms-g" else ["0xqs-o"],
        )
        monkeypatch.setattr(m, "safe_threshold", lambda *, ledger_api, safe: 1)
        monkeypatch.setattr(m, "stop_via_middleware",
                            lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])

        nft_calls: list = []
        swap_calls: list = []
        from scripts.pearl_migration import transfer as transfer_mod
        monkeypatch.setattr(
            transfer_mod, "transfer_service_nft",
            lambda *, ledger_api, crypto, service_registry_address,
                   qs_master_safe, pearl_master_safe, service_id:
                nft_calls.append((ledger_api, service_id)),
        )
        monkeypatch.setattr(
            transfer_mod, "swap_service_safe_owner",
            lambda *, ledger_api, crypto, service_safe, old_owner, new_owner:
                swap_calls.append((ledger_api, service_safe)),
        )

        ref = _fake_service("sc-aaa", name="A", path=tmp_path)
        m._migrate_one_service(
            svc=ref, qs_app=qs_app,
            qs_wallet=types.SimpleNamespace(crypto="CR", safes={
                FakeChain.GNOSIS: "0xqs-g", FakeChain.OPTIMISM: "0xqs-o",
            }),
            pearl_wallet=types.SimpleNamespace(safes={
                FakeChain.GNOSIS: "0xpl-g", FakeChain.OPTIMISM: "0xpl-o",
            }),
            config_path=None,
        )

        # Step 2: each chain's NFT transfer received its own ledger handle.
        assert ("LA-https://rpc.example/gnosis", 1) in nft_calls
        assert ("LA-https://rpc.example/optimism", 2) in nft_calls
        # Step 3: each chain's swap received its own ledger handle, keyed by
        # the per-chain service Safe (multisig) address.
        assert ("LA-https://rpc.example/gnosis", "0xms-g") in swap_calls
        assert ("LA-https://rpc.example/optimism", "0xms-o") in swap_calls

    def test_creates_pearl_safe_when_missing(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pearl missing a Safe on this chain -> create_safe is invoked, then proceed."""
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState,
            state_seq=[FakeOnChainState.PRE_REGISTRATION],  # already there, skip terminate
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        # NFT already on (the soon-to-be-created) Pearl, swap already done.
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xpl-new")
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xpl-new"])

        # Pearl wallet starts with NO Safe on Gnosis. `create_safe` should be
        # called and populate `safes[chain]`.
        pearl_safes: dict = {}
        creates: list = []

        def fake_create_safe(*, chain: Any, rpc: Any) -> None:
            creates.append({"chain": chain, "rpc": rpc})
            pearl_safes[chain] = "0xpl-new"
        pearl_wallet = types.SimpleNamespace(safes=pearl_safes, create_safe=fake_create_safe)

        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        m._migrate_one_service(
            svc=ref, qs_app=qs_app,
            qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
            pearl_wallet=pearl_wallet, config_path=None,
        )
        assert len(creates) == 1
        assert creates[0]["chain"] == FakeChain.GNOSIS
        assert pearl_safes[FakeChain.GNOSIS] == "0xpl-new"

    def test_pearl_safe_creation_failure_is_unmigratable(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState,
            state_seq=[FakeOnChainState.PRE_REGISTRATION],
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])

        def boom(**kw: Any) -> None:
            raise RuntimeError("rpc rejected safe deployment")
        pearl_wallet = types.SimpleNamespace(safes={}, create_safe=boom)
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=pearl_wallet, config_path=None,
            )
        assert "could not create Pearl master Safe" in ei.value.reason

    def test_force_remove_timeout_becomes_unmigratable(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`force_remove_known_containers` raising `TimeoutExpired` (docker
        daemon hang) MUST surface as `_Unmigratable` — not propagate raw
        and abort the batch with on-chain ops about to commit."""
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState,
            state_seq=[FakeOnChainState.PRE_REGISTRATION],
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        def boom() -> list:
            raise subprocess.TimeoutExpired(cmd=["docker", "rm"], timeout=30)
        monkeypatch.setattr(m, "force_remove_known_containers", boom)
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )
        assert "could not confirm containers are stopped" in ei.value.reason

    @pytest.mark.parametrize("bug_cls", [
        TypeError, AttributeError, NameError, ImportError,
    ])
    def test_stop_via_middleware_programming_bug_propagates(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        bug_cls: type,
    ) -> None:
        """A programming bug in middleware's stop_service must NOT be
        demoted to a `warn(...)` and silently fall through to the
        on-chain branch — propagate as a real traceback."""
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState,
            state_seq=[FakeOnChainState.PRE_REGISTRATION],
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        def buggy(operate: Any, config_path: str) -> None:
            raise bug_cls("middleware refactor")
        monkeypatch.setattr(m, "stop_via_middleware", buggy)
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(bug_cls):
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path="cfg.json",
            )

    @pytest.mark.parametrize("bug_cls", [
        TypeError, AttributeError, NameError, ImportError,
    ])
    def test_pearl_create_safe_programming_bug_propagates(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        bug_cls: type,
    ) -> None:
        """Programming bug in `pearl_wallet.create_safe` must propagate,
        not get wrapped as `_Unmigratable("could not create Pearl Safe")`."""
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState,
            state_seq=[FakeOnChainState.PRE_REGISTRATION],
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware",
                            lambda operate, config_path: None)
        def buggy(**kw: Any) -> None:
            raise bug_cls("middleware refactor")
        pearl_wallet = types.SimpleNamespace(safes={}, create_safe=buggy)
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(bug_cls):
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=pearl_wallet, config_path=None,
            )

    @pytest.mark.parametrize("bug_cls", [
        TypeError, AttributeError, NameError, ImportError,
    ])
    def test_ensure_signable_programming_bug_propagates(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        bug_cls: type,
    ) -> None:
        """A programming bug in `safe_threshold` (caller of
        `_ensure_signable`) must propagate, not become an
        `_Unmigratable("could not read threshold")`."""
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(FakeChain, FakeOnChainState)
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware",
                            lambda operate, config_path: None)
        def buggy(**kw: Any) -> None:
            raise bug_cls("middleware refactor")
        monkeypatch.setattr(m, "safe_threshold", buggy)
        # Force the signing branch by ensuring NFT is still on QS.
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xqs")
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xqs"])
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(bug_cls):
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )

    def test_force_remove_runtime_error_becomes_unmigratable(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`docker_quickstart_containers` raising `RuntimeError` (docker
        daemon refusing to talk) MUST surface as `_Unmigratable`."""
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState,
            state_seq=[FakeOnChainState.PRE_REGISTRATION],
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        def boom() -> list:
            raise RuntimeError("docker ps exited 1: permission denied")
        monkeypatch.setattr(m, "force_remove_known_containers", boom)
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )
        assert "could not confirm containers are stopped" in ei.value.reason

    def test_re_probe_runtime_error_becomes_unmigratable(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Re-probe via `docker_quickstart_containers` failing AFTER a
        clean `force_remove_known_containers` MUST also become
        `_Unmigratable`. Covers the daemon-hangs-between-rm-and-ps race."""
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState,
            state_seq=[FakeOnChainState.PRE_REGISTRATION],
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        def boom() -> list:
            raise RuntimeError("docker ps exited 1: daemon gone")
        monkeypatch.setattr(m, "docker_quickstart_containers", boom)
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )
        assert "could not verify containers stopped" in ei.value.reason

    def test_leftover_containers_after_cleanup_become_unmigratable(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If `docker rm -f` reported success but containers are still
        listed afterwards (daemon refused without erroring), MUST surface
        as `_Unmigratable` — proceeding would race the live deployment."""
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState,
            state_seq=[FakeOnChainState.PRE_REGISTRATION],
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: ["abci0"])
        # Override the autouse stub: re-probe still finds the container.
        monkeypatch.setattr(m, "docker_quickstart_containers", lambda: ["abci0"])
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )
        assert "still running after cleanup" in ei.value.reason
        assert "abci0" in ei.value.reason

    def test_qs_wallet_missing_chain_safe_is_unmigratable(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`qs_wallet.safes[chain]` raising KeyError (chain added to service
        after wallet creation) must surface as a per-service `_Unmigratable`,
        NOT crash the batch — `_PROGRAMMING_BUGS` deliberately excludes
        KeyError so this wrap at the call site is the only thing keeping
        the contract."""
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState,
            state_seq=[FakeOnChainState.PRE_REGISTRATION],
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])

        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                # No Safe registered for Gnosis -> KeyError on access.
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )
        assert "no Safe on gnosis" in ei.value.reason

    def test_contracts_missing_chain_is_unmigratable(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """CONTRACTS[chain]['service_registry'] raising KeyError (newer chain
        enum without a registered ServiceRegistry) must surface as a per-
        service `_Unmigratable`, not crash the batch."""
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState,
            state_seq=[FakeOnChainState.PRE_REGISTRATION],
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        # Strip the registry entry for Gnosis to simulate a chain without
        # a registered ServiceRegistry.
        from operate.ledger.profiles import CONTRACTS as _CONTRACTS
        original = _CONTRACTS.get(FakeChain.GNOSIS, {}).copy()
        _CONTRACTS[FakeChain.GNOSIS] = {
            k: v for k, v in original.items() if k != "service_registry"
        }
        try:
            ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
            with pytest.raises(m._Unmigratable) as ei:
                m._migrate_one_service(
                    svc=ref, qs_app=qs_app,
                    qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                    pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                    config_path=None,
                )
            assert "no ServiceRegistry contract" in ei.value.reason
        finally:
            _CONTRACTS[FakeChain.GNOSIS] = original

    def test_threshold_gt_1_raises_unmigratable(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Multi-sig master Safe with threshold > 1 must be refused upfront."""
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(FakeChain, FakeOnChainState)
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        monkeypatch.setattr(m, "safe_threshold", lambda **kw: 2)   # multi-sig
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xqs")
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xqs"])

        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )
        assert "threshold" in ei.value.reason and "single-owner only" in ei.value.reason

    def test_threshold_zero_distinct_message(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Threshold == 0 (malformed Safe) must NOT say 'lower the threshold'."""
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(FakeChain, FakeOnChainState)
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        monkeypatch.setattr(m, "safe_threshold", lambda **kw: 0)
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xqs")
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xqs"])
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )
        assert "appears unconfigured" in ei.value.reason
        assert "Lower the threshold" not in ei.value.reason

    def test_threshold_check_skipped_when_already_migrated(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Resumed run on a fully-migrated service must NOT call safe_threshold.

        This catches the regression where the threshold pre-check was placed
        before the per-step probes — a user who raised their qs Safe threshold
        after migrating would have been refused on a no-op resume.
        """
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(
            FakeChain, FakeOnChainState,
            state_seq=[FakeOnChainState.PRE_REGISTRATION],   # already terminated
        )
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        # NFT already on Pearl, Safe already swapped → no signing needed.
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xpl")
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xpl"])

        threshold_calls: list = []
        def boom_threshold(**kw: Any) -> int:
            threshold_calls.append(kw)
            # Simulate user having raised threshold post-migration; would
            # raise _Unmigratable IF the check ran.
            return 2
        monkeypatch.setattr(m, "safe_threshold", boom_threshold)

        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        # Should complete cleanly: no signing → no threshold check.
        m._migrate_one_service(
            svc=ref, qs_app=qs_app,
            qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
            pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
            config_path=None,
        )
        assert threshold_calls == [], (
            "safe_threshold must NOT be called when no signing branch runs"
        )

    def test_threshold_actually_invoked_when_signing(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sanity that the threshold pre-check DOES run when we actually sign.

        The autouse `_stub_threshold` masks the call site in most tests; this
        one bypasses the stub by overriding it to record invocations.
        """
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(FakeChain, FakeOnChainState)
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xqs")
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xqs"])

        invocations: list = []
        def recording_threshold(**kw: Any) -> int:
            invocations.append(kw)
            return 1
        monkeypatch.setattr(m, "safe_threshold", recording_threshold)

        fake_transfer = types.ModuleType("scripts.pearl_migration.transfer")
        fake_transfer.transfer_service_nft = lambda **kw: "0x1"  # type: ignore[attr-defined]
        fake_transfer.swap_service_safe_owner = lambda **kw: None  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "scripts.pearl_migration.transfer", fake_transfer)

        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        m._migrate_one_service(
            svc=ref, qs_app=qs_app,
            qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
            pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
            config_path=None,
        )
        assert invocations, "safe_threshold should be invoked before signing"
        # Each call must pass the QS master Safe address (not Pearl's).
        for call in invocations:
            assert call.get("safe") == "0xqs"

    def test_threshold_read_failure_raises_unmigratable(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(FakeChain, FakeOnChainState)
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        monkeypatch.setattr(m, "stop_via_middleware", lambda operate, config_path: None)
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])

        def boom(**kw: Any) -> Any:
            raise RuntimeError("rpc 502")
        monkeypatch.setattr(m, "safe_threshold", boom)

        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(m._Unmigratable) as ei:
            m._migrate_one_service(
                svc=ref, qs_app=qs_app,
                qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
                pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
                config_path=None,
            )
        assert "could not read quickstart master Safe threshold" in ei.value.reason

    def test_print_summary_clean_prints_success(
        self, orch: Any, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """All-clean migration: prints a positive 'fully migrated: N' headline."""
        m, *_ = orch
        outcome = m.MigrationOutcome(
            migrated=tuple(_fake_service(f"sc-{i}") for i in range(4)),
        )
        m._print_migration_summary(outcome)
        out = capsys.readouterr().out
        assert "Migration summary" in out
        assert "fully migrated: 4 service(s)" in out
        assert "Migration incomplete" not in out

    def test_print_summary_formats_each_failure_line(
        self, orch: Any, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Direct check on the per-failure formatting — would catch a typo
        in the `[chain] kind address: reason` template."""
        m, FakeChain, _ = orch
        unm = (m._Unmigratable(
            service_id="sc-zzz", chain="gnosis", reason="cannot unstake yet",
        ),)
        drains = (
            m._DrainFailure(
                chain=FakeChain.GNOSIS, source_kind="Safe",
                source_address="0xsafe-addr", reason="insufficient gas",
            ),
            m._DrainFailure(
                chain=FakeChain.OPTIMISM, source_kind="EOA",
                source_address="0xeoa-addr", reason="rpc 502",
            ),
        )
        outcome = m.MigrationOutcome(
            migrated=tuple(_fake_service(f"sc-{i}") for i in range(2)),
            unmigratable=unm,
            drain_failures=drains,
        )
        m._print_migration_summary(outcome)
        out = capsys.readouterr().out
        assert "Migration incomplete" in out
        assert "fully migrated: 2 service(s)" in out
        assert "un-migrated services: 1" in out
        assert "sc-zzz" in out and "cannot unstake yet" in out
        assert "drain failures: 2" in out
        # Per-line format checks (chain.name comes from FakeChain.name attribute).
        assert "[GNOSIS] Safe 0xsafe-addr: insufficient gas" in out
        assert "[OPTIMISM] EOA 0xeoa-addr: rpc 502" in out

    def test_no_config_path_skips_middleware_stop(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, FakeChain, FakeOnChainState = orch
        _, manager = self._setup_manager(FakeChain, FakeOnChainState)
        qs_app = types.SimpleNamespace(service_manager=lambda: manager)
        called: list[str] = []
        monkeypatch.setattr(m, "stop_via_middleware",
                            lambda **kw: called.append("nope"))
        monkeypatch.setattr(m, "force_remove_known_containers", lambda: [])
        monkeypatch.setattr(m, "service_nft_owner", lambda **kw: "0xqs")
        monkeypatch.setattr(m, "safe_owners", lambda **kw: ["0xqs"])
        fake_transfer = types.ModuleType("scripts.pearl_migration.transfer")
        fake_transfer.transfer_service_nft = lambda **kw: "0x1"  # type: ignore[attr-defined]
        fake_transfer.swap_service_safe_owner = lambda **kw: None  # type: ignore[attr-defined]
        monkeypatch.setitem(
            sys.modules, "scripts.pearl_migration.transfer", fake_transfer,
        )
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        m._migrate_one_service(
            svc=ref, qs_app=qs_app,
            qs_wallet=types.SimpleNamespace(crypto="CR", safes={FakeChain.GNOSIS: "0xqs"}),
            pearl_wallet=types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"}),
            config_path=None,
        )
        assert called == []  # config_path None bypasses stop_via_middleware


class TestDrainMaster:
    def test_announces_pearl_safe_creation(
        self, orch: Any, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """When Pearl lacks a Safe on a chain we announce + create rather than skip."""
        m, FakeChain, _ = orch
        pearl_safes: dict = {}

        def fake_create_safe(*, chain: Any, rpc: Any) -> None:
            pearl_safes[chain] = "0xpl"

        qs = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xqs"},
                                   address="0xqs_eoa", drain=lambda **kw: {})
        pearl = types.SimpleNamespace(safes=pearl_safes,
                                      create_safe=fake_create_safe,
                                      address="0xpe")
        m._drain_master(
            qs_wallet=qs, pearl_wallet=pearl,
            chain_rpcs={FakeChain.GNOSIS: "https://rpc/gnosis"},
        )
        out = capsys.readouterr().out
        assert "Pearl has no master Safe" in out and "creating one" in out

    def test_drain_empty_moved_emits_info_line(
        self, orch: Any, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """When `qs_wallet.drain()` returns `{}`, emit an explicit "no
        balances to move" line so silence isn't ambiguous between
        "nothing to drain" and "silently dropped"."""
        m, FakeChain, _ = orch
        qs = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xqs"},
                                   address="0xqs_eoa", drain=lambda **kw: {})
        pearl = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"},
                                      address="0xpe")
        m._drain_master(qs_wallet=qs, pearl_wallet=pearl)
        out = capsys.readouterr().out
        assert "no balances to move from Safe on GNOSIS" in out
        assert "no balances to move from EOA on GNOSIS" in out

    def test_drain_safe_and_eoa(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, FakeChain, _ = orch
        called: list[dict[str, Any]] = []
        def fake_drain(withdrawal_address: str, chain: Any, from_safe: bool) -> dict[str, int]:
            called.append({"to": withdrawal_address, "from_safe": from_safe})
            return {"0xtoken": 100}
        qs = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xqs"},
                                   address="0xqs_eoa", drain=fake_drain)
        pearl = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"},
                                      address="0xpe")
        failures = m._drain_master(qs_wallet=qs, pearl_wallet=pearl)
        assert failures == []
        assert {c["from_safe"] for c in called} == {True, False}
        assert called[0]["to"] == "0xpl"
        assert called[1]["to"] == "0xpe"

    def test_drain_safe_failure_returns_failure_record(
        self, orch: Any, capsys: pytest.CaptureFixture[str],
    ) -> None:
        m, FakeChain, _ = orch
        def fake_drain(withdrawal_address: str, chain: Any, from_safe: bool) -> dict[str, int]:
            if from_safe:
                raise RuntimeError("safe drain boom")
            return {}
        qs = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xqs"},
                                   address="0xqs_eoa", drain=fake_drain)
        pearl = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"},
                                      address="0xpe")
        failures = m._drain_master(qs_wallet=qs, pearl_wallet=pearl)
        assert len(failures) == 1
        assert failures[0].source_kind == "Safe"
        assert "boom" in failures[0].reason
        assert "drain (Safe)" in capsys.readouterr().out

    def test_drain_eoa_failure_returns_failure_record(
        self, orch: Any, capsys: pytest.CaptureFixture[str],
    ) -> None:
        m, FakeChain, _ = orch
        def fake_drain(withdrawal_address: str, chain: Any, from_safe: bool) -> dict[str, int]:
            if not from_safe:
                raise RuntimeError("eoa drain boom")
            return {}
        qs = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xqs"},
                                   address="0xqs_eoa", drain=fake_drain)
        pearl = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"},
                                      address="0xpe")
        failures = m._drain_master(qs_wallet=qs, pearl_wallet=pearl)
        assert len(failures) == 1
        assert failures[0].source_kind == "EOA"
        assert "drain (EOA)" in capsys.readouterr().out

    def test_drain_creates_pearl_safe_when_missing(
        self, orch: Any,
    ) -> None:
        """If Pearl has no Safe on a chain, create one then drain into it."""
        m, FakeChain, _ = orch
        # Track create_safe calls and have them populate Pearl's safes dict.
        pearl_safes: dict = {}
        creates: list = []

        def fake_create_safe(*, chain: Any, rpc: Any) -> None:
            creates.append({"chain": chain, "rpc": rpc})
            pearl_safes[chain] = "0xpl-fresh"

        called: list = []
        def fake_drain(withdrawal_address: str, chain: Any, from_safe: bool) -> dict:
            called.append({"to": withdrawal_address, "from_safe": from_safe})
            return {}

        qs = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xqs"},
                                   address="0xqs_eoa", drain=fake_drain)
        pearl = types.SimpleNamespace(safes=pearl_safes,
                                      create_safe=fake_create_safe,
                                      address="0xpe")
        failures = m._drain_master(
            qs_wallet=qs, pearl_wallet=pearl,
            chain_rpcs={FakeChain.GNOSIS: "https://rpc.example/gnosis"},
        )
        assert failures == []
        assert creates == [{"chain": FakeChain.GNOSIS,
                            "rpc": "https://rpc.example/gnosis"}]
        # Drains then ran into the freshly-created Pearl Safe.
        assert called[0]["to"] == "0xpl-fresh"
        assert called[1]["to"] == "0xpe"

    @pytest.mark.parametrize("bug_cls", [
        TypeError, AttributeError, NameError, ImportError,
    ])
    def test_drain_programming_bug_propagates(
        self, orch: Any, bug_cls: type,
    ) -> None:
        """A programming bug in `qs_wallet.drain()` (e.g. middleware
        signature drift) MUST propagate as a real traceback, NOT get
        silently aggregated into `_DrainFailure` where the user would
        chase 'RPC retry' instead of the real bug."""
        m, FakeChain, _ = orch
        def buggy_drain(withdrawal_address: str, chain: Any, from_safe: bool) -> dict:
            raise bug_cls("simulated middleware refactor bug")
        qs = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xqs"},
                                   address="0xqs_eoa", drain=buggy_drain)
        pearl = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"},
                                      address="0xpe")
        with pytest.raises(bug_cls):
            m._drain_master(qs_wallet=qs, pearl_wallet=pearl)

    @pytest.mark.parametrize("bug_cls", [
        TypeError, AttributeError, NameError, ImportError,
    ])
    def test_drain_create_safe_programming_bug_propagates(
        self, orch: Any, bug_cls: type,
    ) -> None:
        """Same policy for the Pearl Safe creation site in `_drain_master`."""
        m, FakeChain, _ = orch
        def buggy_create(*, chain: Any, rpc: Any) -> None:
            raise bug_cls("simulated middleware refactor bug")
        qs = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xqs"},
                                   address="0xqs_eoa", drain=lambda **kw: {})
        pearl = types.SimpleNamespace(safes={}, create_safe=buggy_create,
                                      address="0xpe")
        with pytest.raises(bug_cls):
            m._drain_master(
                qs_wallet=qs, pearl_wallet=pearl,
                chain_rpcs={FakeChain.GNOSIS: "https://rpc/gnosis"},
            )

    def test_drain_pearl_safe_creation_failure_records_failure(
        self, orch: Any, capsys: pytest.CaptureFixture[str],
    ) -> None:
        m, FakeChain, _ = orch

        def boom(*, chain: Any, rpc: Any) -> None:
            raise RuntimeError("create reverted")
        qs = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xqs"},
                                   address="0xqs_eoa", drain=lambda **kw: {})
        pearl = types.SimpleNamespace(safes={}, create_safe=boom, address="0xpe")
        # Provide an RPC so the create_safe boom is reached (new pre-check
        # would short-circuit otherwise).
        failures = m._drain_master(
            qs_wallet=qs, pearl_wallet=pearl,
            chain_rpcs={FakeChain.GNOSIS: "https://rpc/gnosis"},
        )
        assert len(failures) == 1
        assert failures[0].source_kind == "Safe+EOA"
        assert "Pearl Safe creation failed" in failures[0].reason
        assert "could not create Pearl Safe" in capsys.readouterr().out

    def test_drain_skips_chain_with_no_rpc_records_failure(
        self, orch: Any, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Pre-check: if Pearl needs a Safe on a chain but `chain_rpcs` has
        no entry, we MUST NOT pass `rpc=None` to `create_safe` (silent
        fallback to public RPC). Instead append a `_DrainFailure` and
        skip the chain so the user sees why funds weren't moved."""
        m, FakeChain, _ = orch
        creates: list = []

        def fake_create_safe(*, chain: Any, rpc: Any) -> None:
            creates.append(chain)
        qs = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xqs"},
                                   address="0xqs_eoa", drain=lambda **kw: {})
        pearl = types.SimpleNamespace(safes={}, create_safe=fake_create_safe,
                                      address="0xpe")
        failures = m._drain_master(qs_wallet=qs, pearl_wallet=pearl, chain_rpcs={})
        # create_safe MUST NOT have been called with rpc=None.
        assert creates == []
        assert len(failures) == 1
        assert failures[0].source_kind == "Safe+EOA"
        assert "no RPC available" in failures[0].reason
        assert "no RPC available" in capsys.readouterr().out

    def test_drain_per_chain_isolation(
        self, orch: Any,
    ) -> None:
        """Docstring claims one chain's RPC outage shouldn't prevent
        draining the others. Pin it: chain A's Safe creation fails,
        chain B's drain MUST still run on both rails."""
        m, FakeChain, _ = orch
        drained: list = []

        def fake_drain(withdrawal_address: str, chain: Any, from_safe: bool) -> dict:
            drained.append((chain, from_safe))
            return {}

        def fake_create_safe(*, chain: Any, rpc: Any) -> None:
            if chain == FakeChain.GNOSIS:
                raise RuntimeError("rpc down")

        qs = types.SimpleNamespace(
            safes={FakeChain.GNOSIS: "0xqs-g", FakeChain.OPTIMISM: "0xqs-o"},
            address="0xqs_eoa", drain=fake_drain,
        )
        pearl = types.SimpleNamespace(
            safes={FakeChain.OPTIMISM: "0xpl-o"},   # missing GNOSIS
            create_safe=fake_create_safe, address="0xpe",
        )
        failures = m._drain_master(
            qs_wallet=qs, pearl_wallet=pearl,
            chain_rpcs={FakeChain.GNOSIS: "https://rpc/gnosis"},
        )
        # Gnosis aborted (Safe creation failed) -> single failure.
        assert {(f.chain, f.source_kind) for f in failures} == {
            (FakeChain.GNOSIS, "Safe+EOA"),
        }
        # Optimism still drained both rails — Gnosis short-circuited at
        # `continue` and did NOT skip Optimism.
        assert (FakeChain.OPTIMISM, True) in drained
        assert (FakeChain.OPTIMISM, False) in drained
        assert (FakeChain.GNOSIS, True) not in drained

    def test_safe_failure_does_not_skip_eoa_on_same_chain(
        self, orch: Any,
    ) -> None:
        """Safe drain failure on chain X must still attempt EOA drain on
        chain X. A future refactor combining both into one try block
        would silently strand EOA funds."""
        m, FakeChain, _ = orch
        calls: list = []

        def fake_drain(withdrawal_address: str, chain: Any, from_safe: bool) -> dict:
            calls.append(from_safe)
            if from_safe:
                raise RuntimeError("safe drain boom")
            return {}
        qs = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xqs"},
                                   address="0xqs_eoa", drain=fake_drain)
        pearl = types.SimpleNamespace(safes={FakeChain.GNOSIS: "0xpl"},
                                      address="0xpe")
        m._drain_master(qs_wallet=qs, pearl_wallet=pearl)
        # Both rails attempted — Safe first (raised), EOA second (succeeded).
        assert calls == [True, False]


class TestRunModeB:
    def test_password_align_runs_when_passwords_differ(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """qs_app.password != pearl_app.password -> user is asked to confirm
        and align_quickstart_password is invoked with Pearl's password."""
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"; qs_root.mkdir(parents=True)
        pl_root = tmp_path / "pl/.operate"; pl_root.mkdir(parents=True)
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root),
            pearl=detect.OperateStore(root=pl_root),
            mode=detect.Mode.MERGE,
        )
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        passwords = iter(["qs-pw", "pearl-pw"])
        monkeypatch.setattr(
            m, "_load_wallet",
            lambda store, label: (
                types.SimpleNamespace(password=next(passwords)), "WALLET",
            ),
        )
        align_calls: list[dict[str, Any]] = []
        monkeypatch.setattr(
            m, "align_quickstart_password",
            lambda **kw: align_calls.append(kw),
        )
        monkeypatch.setattr(m, "fix_root_ownership", lambda store: None)
        monkeypatch.setattr(m, "_migrate_one_service", lambda **kw: None)
        monkeypatch.setattr(
            m, "merge_service",
            lambda service, src, dest: types.SimpleNamespace(),
        )
        monkeypatch.setattr(m, "_drain_master", lambda **kw: [])
        monkeypatch.setattr(m, "rename_source_for_rollback", lambda src: src.root)
        monkeypatch.setattr(m, "yes_no", lambda *a, **k: True)

        m._run_mode_b(disc=d, services=[ref], config_path=None, dry_run=False)

        assert len(align_calls) == 1
        assert align_calls[0]["new_password"] == "pearl-pw"
        out = capsys.readouterr().out
        assert "ALIGNING QUICKSTART PASSWORD" in out

    def test_password_align_aborts_on_user_decline(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"; qs_root.mkdir(parents=True)
        pl_root = tmp_path / "pl/.operate"; pl_root.mkdir(parents=True)
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root),
            pearl=detect.OperateStore(root=pl_root),
            mode=detect.Mode.MERGE,
        )
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        passwords = iter(["qs-pw", "pearl-pw"])
        monkeypatch.setattr(
            m, "_load_wallet",
            lambda store, label: (
                types.SimpleNamespace(password=next(passwords)), "WALLET",
            ),
        )
        align_called = {"n": 0}
        monkeypatch.setattr(
            m, "align_quickstart_password",
            lambda **kw: align_called.update(n=align_called["n"] + 1),
        )
        monkeypatch.setattr(m, "yes_no", lambda *a, **k: False)

        with pytest.raises(SystemExit):
            m._run_mode_b(disc=d, services=[ref], config_path=None, dry_run=False)
        assert align_called["n"] == 0

    def test_dry_run_short_circuits(
        self, orch: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        m, *_ = orch
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=tmp_path / "qs"),
            pearl=detect.OperateStore(root=tmp_path / "pl"),
            mode=detect.Mode.MERGE,
        )
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        m._run_mode_b(disc=d, services=[ref], config_path=None, dry_run=True)
        assert "[dry-run]" in capsys.readouterr().out

    def test_happy_path(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"
        qs_root.mkdir(parents=True)
        pl_root = tmp_path / "pl/.operate"
        pl_root.mkdir(parents=True)
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root),
            pearl=detect.OperateStore(root=pl_root),
            mode=detect.Mode.MERGE,
        )
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        monkeypatch.setattr(m, "_load_wallet",
                            lambda store, label: (types.SimpleNamespace(password="pw"), "WALLET"))
        monkeypatch.setattr(m, "fix_root_ownership", lambda store: None)
        monkeypatch.setattr(m, "_migrate_one_service", lambda **kw: None)
        monkeypatch.setattr(m, "merge_service",
                            lambda service, src, dest: types.SimpleNamespace())
        drain_called = {"n": 0}
        def fake_drain(**kw: Any) -> list:
            drain_called["n"] += 1
            return []   # empty failures = full success
        monkeypatch.setattr(m, "_drain_master", fake_drain)
        monkeypatch.setattr(m, "yes_no", lambda *a, **k: True)
        rename_called = {"n": 0}
        def fake_rename(src: Any) -> Path:
            rename_called["n"] += 1
            return src.root
        monkeypatch.setattr(m, "rename_source_for_rollback", fake_rename)

        m._run_mode_b(disc=d, services=[ref], config_path=None, dry_run=False)
        assert drain_called["n"] == 1
        assert rename_called["n"] == 1

    def test_unmigratable_skips_drain(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"
        qs_root.mkdir(parents=True)
        pl_root = tmp_path / "pl/.operate"
        pl_root.mkdir(parents=True)
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root),
            pearl=detect.OperateStore(root=pl_root),
            mode=detect.Mode.MERGE,
        )
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        monkeypatch.setattr(m, "_load_wallet",
                            lambda store, label: (types.SimpleNamespace(password="pw"), "WALLET"))
        monkeypatch.setattr(m, "fix_root_ownership", lambda store: None)
        def boom(**kw: Any) -> None:
            raise m._Unmigratable(
                service_id="sc-aaa", chain="gnosis", reason="cannot unstake",
            )
        monkeypatch.setattr(m, "_migrate_one_service", boom)
        called = {"drain": 0, "rename": 0}
        monkeypatch.setattr(m, "_drain_master",
                            lambda **kw: called.update(drain=called["drain"] + 1))
        monkeypatch.setattr(m, "rename_source_for_rollback",
                            lambda store: called.update(rename=called["rename"] + 1) or store.root)
        m._run_mode_b(disc=d, services=[ref], config_path=None, dry_run=False)
        assert called["drain"] == 0
        # Nothing migrated AND incomplete -> never rename source.
        assert called["rename"] == 0
        out = capsys.readouterr().out
        assert "Skipping the master-Safe" in out
        assert "Migration incomplete" in out
        assert "Source `.operate/` left in place" in out

    def test_merge_service_oserror_aggregates_as_unmigratable(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """`merge_service` runs AFTER on-chain ops. An OSError here would,
        without the wrap in `_run_mode_b`, propagate past `except _Unmigratable`
        and abort the whole batch with no aggregation. The fix converts to
        `_Unmigratable(chain=None)` so the user gets a per-service summary
        and the on-chain-committed-but-files-not-copied state is recorded."""
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"
        qs_root.mkdir(parents=True)
        pl_root = tmp_path / "pl/.operate"
        pl_root.mkdir(parents=True)
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root),
            pearl=detect.OperateStore(root=pl_root),
            mode=detect.Mode.MERGE,
        )
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        monkeypatch.setattr(m, "_load_wallet",
                            lambda store, label: (types.SimpleNamespace(password="pw"), "WALLET"))
        monkeypatch.setattr(m, "fix_root_ownership", lambda store: None)
        monkeypatch.setattr(m, "_migrate_one_service", lambda **kw: None)
        def boom_copy(service: Any, src: Any, dest: Any) -> None:
            raise OSError(28, "no space left on device")
        monkeypatch.setattr(m, "merge_service", boom_copy)
        called = {"drain": 0, "rename": 0}
        monkeypatch.setattr(m, "_drain_master",
                            lambda **kw: called.update(drain=called["drain"] + 1))
        monkeypatch.setattr(m, "rename_source_for_rollback",
                            lambda store: called.update(rename=called["rename"] + 1) or store.root)
        m._run_mode_b(disc=d, services=[ref], config_path=None, dry_run=False)
        # Drain skipped (unmigratable present); rename skipped (incomplete).
        assert called["drain"] == 0
        assert called["rename"] == 0
        out = capsys.readouterr().out
        # User-facing message names the service and tells them what to do.
        assert "filesystem copy failed" in out
        assert "Manually copy" in out
        assert "sc-aaa" in out

    def test_merge_service_shutil_error_also_aggregates(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """shutil.Error (separate exception type from OSError — used for
        copytree multi-error aggregation) is also caught."""
        import shutil as _shutil
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"
        qs_root.mkdir(parents=True)
        pl_root = tmp_path / "pl/.operate"
        pl_root.mkdir(parents=True)
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root),
            pearl=detect.OperateStore(root=pl_root),
            mode=detect.Mode.MERGE,
        )
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        monkeypatch.setattr(m, "_load_wallet",
                            lambda store, label: (types.SimpleNamespace(password="pw"), "WALLET"))
        monkeypatch.setattr(m, "fix_root_ownership", lambda store: None)
        monkeypatch.setattr(m, "_migrate_one_service", lambda **kw: None)
        def boom_copy(service: Any, src: Any, dest: Any) -> None:
            raise _shutil.Error([("a", "b", "permission denied")])
        monkeypatch.setattr(m, "merge_service", boom_copy)
        monkeypatch.setattr(m, "_drain_master", lambda **kw: [])
        # Should not raise; drain/rename gating happens at the higher level.
        m._run_mode_b(disc=d, services=[ref], config_path=None, dry_run=False)

    def test_drain_failure_blocks_rename(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """All services migrate but a drain fails -> source MUST NOT be renamed."""
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"
        qs_root.mkdir(parents=True)
        pl_root = tmp_path / "pl/.operate"
        pl_root.mkdir(parents=True)
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root),
            pearl=detect.OperateStore(root=pl_root),
            mode=detect.Mode.MERGE,
        )
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        monkeypatch.setattr(m, "_load_wallet",
                            lambda store, label: (types.SimpleNamespace(password="pw"), "WALLET"))
        monkeypatch.setattr(m, "fix_root_ownership", lambda store: None)
        monkeypatch.setattr(m, "_migrate_one_service", lambda **kw: None)
        monkeypatch.setattr(m, "merge_service",
                            lambda service, src, dest: types.SimpleNamespace())
        # Drain returns a non-empty failure list.
        m_FakeChain = orch[1]
        fake_failure = m._DrainFailure(
            chain=m_FakeChain.GNOSIS, source_kind="Safe",
            source_address="0xqs", reason="rpc timeout",
        )
        monkeypatch.setattr(m, "_drain_master", lambda **kw: [fake_failure])
        rename_called = {"n": 0}
        monkeypatch.setattr(m, "rename_source_for_rollback",
                            lambda store: rename_called.update(n=rename_called["n"] + 1))
        # Force yes_no="yes" so we'd rename if the orchestrator asked.
        monkeypatch.setattr(m, "yes_no", lambda *a, **k: True)
        m._run_mode_b(disc=d, services=[ref], config_path=None, dry_run=False)
        assert rename_called["n"] == 0   # rename refused
        out = capsys.readouterr().out
        assert "Migration incomplete" in out
        assert "rpc timeout" in out
        assert "Source `.operate/` left in place" in out

    def test_collects_chain_rpcs_for_drain(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Per-service chain_configs feed `chain_rpcs` so the drain step can
        create Pearl Safes on chains it doesn't yet have."""
        m, FakeChain, _ = orch
        qs_root = tmp_path / "qs/.operate"
        qs_root.mkdir(parents=True)
        pl_root = tmp_path / "pl/.operate"
        pl_root.mkdir(parents=True)
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root),
            pearl=detect.OperateStore(root=pl_root),
            mode=detect.Mode.MERGE,
        )
        # Service with a real chain_config so the chain_rpcs map gets populated.
        chain_config = types.SimpleNamespace(
            chain_data=types.SimpleNamespace(token=1, multisig="0xms"),
            ledger_config=types.SimpleNamespace(rpc="https://rpc.example/gnosis"),
        )
        ref = _fake_service(
            "sc-aaa", name="A", agent_addresses=[], path=tmp_path,
            chain_configs={"gnosis": chain_config},
        )
        monkeypatch.setattr(m, "_load_wallet",
                            lambda store, label: (types.SimpleNamespace(password="pw"), "WALLET"))
        monkeypatch.setattr(m, "fix_root_ownership", lambda store: None)
        monkeypatch.setattr(m, "_migrate_one_service", lambda **kw: None)
        monkeypatch.setattr(m, "merge_service",
                            lambda service, src, dest: types.SimpleNamespace())
        captured: dict = {}
        def fake_drain(**kw: Any) -> list:
            captured.update(kw)
            return []
        monkeypatch.setattr(m, "_drain_master", fake_drain)
        monkeypatch.setattr(m, "yes_no", lambda *a, **k: False)
        monkeypatch.setattr(m, "rename_source_for_rollback", lambda store: store.root)

        m._run_mode_b(disc=d, services=[ref], config_path=None, dry_run=False)
        assert captured["chain_rpcs"] == {FakeChain.GNOSIS: "https://rpc.example/gnosis"}

    def test_chain_rpcs_conflict_warns_keeps_first(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Two services declaring different RPCs for the same chain: warn
        loudly, keep the first deterministically. Catches a regression where
        someone replaces `setdefault`-style first-wins with `=`-style last-wins
        without a warning."""
        m, FakeChain, _ = orch
        qs_root = tmp_path / "qs/.operate"
        qs_root.mkdir(parents=True)
        pl_root = tmp_path / "pl/.operate"
        pl_root.mkdir(parents=True)
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root),
            pearl=detect.OperateStore(root=pl_root),
            mode=detect.Mode.MERGE,
        )
        # Two services on the same chain with DIFFERENT RPCs.
        cc_a = types.SimpleNamespace(
            chain_data=types.SimpleNamespace(token=1, multisig="0xms-a"),
            ledger_config=types.SimpleNamespace(rpc="https://rpc.private/gnosis"),
        )
        cc_b = types.SimpleNamespace(
            chain_data=types.SimpleNamespace(token=2, multisig="0xms-b"),
            ledger_config=types.SimpleNamespace(rpc="https://rpc.public/gnosis"),
        )
        ref_a = _fake_service("sc-aaa", name="A", path=tmp_path,
                              chain_configs={"gnosis": cc_a})
        ref_b = _fake_service("sc-bbb", name="B", path=tmp_path,
                              chain_configs={"gnosis": cc_b})

        monkeypatch.setattr(m, "_load_wallet",
                            lambda store, label: (types.SimpleNamespace(password="pw"), "WALLET"))
        monkeypatch.setattr(m, "fix_root_ownership", lambda store: None)
        monkeypatch.setattr(m, "_migrate_one_service", lambda **kw: None)
        monkeypatch.setattr(m, "merge_service",
                            lambda service, src, dest: types.SimpleNamespace())
        captured: dict = {}
        monkeypatch.setattr(m, "_drain_master",
                            lambda **kw: captured.update(kw) or [])
        monkeypatch.setattr(m, "yes_no", lambda *a, **k: False)
        monkeypatch.setattr(m, "rename_source_for_rollback", lambda store: store.root)

        m._run_mode_b(disc=d, services=[ref_a, ref_b], config_path=None, dry_run=False)

        # First-wins (deterministic).
        assert captured["chain_rpcs"][FakeChain.GNOSIS] == "https://rpc.private/gnosis"
        # And the user is told.
        out = capsys.readouterr().out
        assert "different RPC" in out
        assert "rpc.private" in out and "rpc.public" in out

    def test_failed_load_aborts_before_any_action(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If `services()` dropped any malformed configs, refuse to migrate."""
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"
        qs_root.mkdir(parents=True)
        pl_root = tmp_path / "pl/.operate"
        pl_root.mkdir(parents=True)
        qs_store = detect.OperateStore(root=qs_root)
        pl_store = detect.OperateStore(root=pl_root)
        d = detect.Discovery(quickstart=qs_store, pearl=pl_store, mode=detect.Mode.MERGE)
        # Pretend the store had a malformed config that didn't load.
        monkeypatch.setattr(qs_store.__class__, "failed_services",
                            lambda self: [(qs_root / "services/sc-bad", RuntimeError("bad"))])
        # Capture whether any wallet loading happens (it must not).
        load_called = {"n": 0}
        monkeypatch.setattr(m, "_load_wallet",
                            lambda store, label: load_called.update(n=load_called["n"] + 1))
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        with pytest.raises(SystemExit):
            m._run_mode_b(disc=d, services=[ref], config_path=None, dry_run=False)
        assert load_called["n"] == 0

    def test_rename_declined(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"
        qs_root.mkdir(parents=True)
        pl_root = tmp_path / "pl/.operate"
        pl_root.mkdir(parents=True)
        d = detect.Discovery(
            quickstart=detect.OperateStore(root=qs_root),
            pearl=detect.OperateStore(root=pl_root),
            mode=detect.Mode.MERGE,
        )
        ref = _fake_service("sc-aaa", name="A", agent_addresses=[], path=tmp_path)
        monkeypatch.setattr(m, "_load_wallet",
                            lambda store, label: (types.SimpleNamespace(password="pw"), "WALLET"))
        monkeypatch.setattr(m, "fix_root_ownership", lambda store: None)
        monkeypatch.setattr(m, "_migrate_one_service", lambda **kw: None)
        monkeypatch.setattr(m, "merge_service",
                            lambda service, src, dest: types.SimpleNamespace())
        monkeypatch.setattr(m, "_drain_master", lambda **kw: [])
        monkeypatch.setattr(m, "yes_no", lambda *a, **k: False)
        called = {"rename": 0}
        monkeypatch.setattr(m, "rename_source_for_rollback",
                            lambda store: called.update(rename=called["rename"] + 1))
        m._run_mode_b(disc=d, services=[ref], config_path=None, dry_run=False)
        assert called["rename"] == 0


class TestFinalPrompt:
    def test_complete_yes_branch(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        m, *_ = orch
        monkeypatch.setattr(m, "yes_no", lambda *a, **k: True)
        m._final_prompt(m.MigrationOutcome(migrated=(_fake_service("sc-aaa"),)))
        out = capsys.readouterr().out
        assert "different machine" in out.lower() or "Copy `~/.operate/`" in out

    def test_complete_no_branch(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        m, *_ = orch
        monkeypatch.setattr(m, "yes_no", lambda *a, **k: False)
        m._final_prompt(m.MigrationOutcome(migrated=(_fake_service("sc-aaa"),)))
        assert "Start Pearl" in capsys.readouterr().out

    def test_no_default_arg_required(self, orch: Any) -> None:
        """`_final_prompt` must require an explicit outcome — the previous
        default-arg sentinel was a footgun (silent success on caller bugs)."""
        m, *_ = orch
        with pytest.raises(TypeError):
            m._final_prompt()

    def test_incomplete_skips_prompts_and_warns(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Partial migration: must NOT ask the 'different machine?' question
        and must explicitly say the source is preserved."""
        m, *_ = orch
        # Track that yes_no is never called.
        called = {"n": 0}
        monkeypatch.setattr(m, "yes_no",
                            lambda *a, **k: called.update(n=called["n"] + 1) or False)
        outcome = m.MigrationOutcome(
            unmigratable=(m._Unmigratable(
                service_id="sc-aaa", chain="gnosis", reason="boom",
            ),),
        )
        m._final_prompt(outcome)
        out = capsys.readouterr().out
        assert "Migration incomplete" in out
        assert "Resolve the issues" in out
        assert called["n"] == 0


class TestMain:
    def test_noop_returns_via_preflight(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        m, *_ = orch
        store = tmp_path / ".operate"
        store.mkdir()
        monkeypatch.setattr(m, "discover", lambda quickstart_root, pearl_root: detect.Discovery(
            quickstart=detect.OperateStore(root=store),
            pearl=detect.OperateStore(root=store),
            mode=detect.Mode.NOOP,
        ))
        with pytest.raises(SystemExit):
            m.main([])

    def test_discover_value_error_becomes_fatal(
        self, orch: Any, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Invariant violations from Discovery/__post_init__ surface via fatal()
        instead of leaking a stacktrace to the user."""
        m, *_ = orch
        def boom(quickstart_root: Any, pearl_root: Any) -> Any:
            raise ValueError("Discovery: stores share root /x but mode is FRESH_COPY")
        monkeypatch.setattr(m, "discover", boom)
        with pytest.raises(SystemExit):
            m.main([])
        # The fatal() output goes to stderr.
        err = capsys.readouterr().err
        assert "Discovery failed" in err

    def test_fresh_copy_dry_run(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        m, *_ = orch
        qs = detect.OperateStore(root=tmp_path / "qs")
        pl = detect.OperateStore(root=tmp_path / "pl")
        monkeypatch.setattr(m, "discover", lambda quickstart_root, pearl_root: detect.Discovery(
            quickstart=qs, pearl=pl, mode=detect.Mode.FRESH_COPY,
        ))
        monkeypatch.setattr(m, "pearl_daemon_running", lambda: False)
        monkeypatch.setattr(m, "docker_quickstart_containers", lambda: [])
        called = {"a": 0, "fp": 0}
        monkeypatch.setattr(m, "_run_mode_a", lambda disc, dry_run: called.update(a=called["a"] + 1))
        monkeypatch.setattr(m, "_final_prompt", lambda **kw: called.update(fp=called["fp"] + 1))
        rc = m.main(["--dry-run"])
        assert rc == 0
        assert called["a"] == 1
        assert called["fp"] == 0  # dry-run skips final prompt

    def test_merge_invokes_select_and_run_b(
        self, orch: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        patch_service_load: dict[Path, Any],
    ) -> None:
        m, *_ = orch
        qs_root = tmp_path / "qs/.operate"
        _write_service(qs_root, "sc-aaa", name="A")
        patch_service_load[(qs_root / "services/sc-aaa").resolve()] = (
            _fake_service("sc-aaa", name="A")
        )
        qs = detect.OperateStore(root=qs_root.resolve())
        pl = detect.OperateStore(root=tmp_path / "pl/.operate")
        monkeypatch.setattr(m, "discover", lambda quickstart_root, pearl_root: detect.Discovery(
            quickstart=qs, pearl=pl, mode=detect.Mode.MERGE,
        ))
        monkeypatch.setattr(m, "pearl_daemon_running", lambda: False)
        monkeypatch.setattr(m, "docker_quickstart_containers", lambda: [])
        called = {"b": 0, "fp": 0, "outcome": None}
        def fake_run_b(**kw: Any) -> Any:
            called["b"] += 1
            return m.MigrationOutcome(migrated=(_fake_service("sc-x"),))
        monkeypatch.setattr(m, "_run_mode_b", fake_run_b)
        def fake_final(outcome: Any = None) -> None:
            called["fp"] += 1
            called["outcome"] = outcome
        monkeypatch.setattr(m, "_final_prompt", fake_final)
        rc = m.main([])
        assert rc == 0
        assert called["b"] == 1
        assert called["fp"] == 1
        assert called["outcome"] is not None and called["outcome"].is_complete


def test_orchestrator_module_is_runnable_as_script(
    orch: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`if __name__ == "__main__"` block — execute via runpy with main mocked.

    Uses `runpy.run_module(..., run_name="__main__")` so the guard executes
    and the line gets covered. main() is stubbed to a noop returning 0.
    """
    import runpy

    # `runpy.run_module(..., run_name="__main__")` re-executes the source in a
    # fresh namespace, so we can't monkeypatch the cached module. Instead we
    # stub `discover` so preflight short-circuits via NOOP -> SystemExit(0).
    monkeypatch.setattr(sys, "argv", ["migrate_to_pearl"])
    fake_store = detect.OperateStore(root=Path("/tmp"))
    monkeypatch.setattr(
        "scripts.pearl_migration.detect.discover",
        lambda quickstart_root, pearl_root: detect.Discovery(
            quickstart=fake_store, pearl=fake_store, mode=detect.Mode.NOOP,
        ),
    )
    with pytest.raises(SystemExit):
        runpy.run_module(
            "scripts.pearl_migration.migrate_to_pearl",
            run_name="__main__",
        )
