# -*- coding: utf-8 -*-
"""End-to-end test: two quickstarts × two services each → one Pearl `.operate`.

Mirrors the structure of `test_run_service.py` (pexpect-driven, real docker,
Tenderly funding) and reuses its helpers. Runs under the `e2e` marker.

Scenario
--------
1. Set up two independent quickstart cwds (qs1, qs2). Each is a fresh copy
   of the repo with its own `.operate` (i.e. its own master wallet).
2. In each quickstart cwd, deploy two distinct service configs sequentially
   via `run_service.sh`, then stop them. Result: 4 distinct on-chain
   services across two stores.
3. Run `migrate_to_pearl.sh` from qs1 → Pearl `.operate` (Mode A: simple
   copy because the target home didn't exist yet).
4. Run `migrate_to_pearl.sh` from qs2 → same Pearl `.operate` (Mode B:
   merge, including on-chain NFT transfer + Safe owner swap + master
   wallet drains).
5. Assert all four service config dirs and their referenced agent keys
   landed in Pearl's `.operate`.
6. Spin up the middleware against Pearl's `.operate` and `deploy_service_locally`
   each migrated service. Verify the ABCI container comes up for each — i.e.
   they are able to start.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import pexpect
import pytest
from dotenv import load_dotenv
from operate.cli import OperateApp
from operate.constants import OPERATE
from operate.operate_types import LedgerType

# Reuse the existing helpers from test_run_service for prompt handling,
# logging, funding via Tenderly, docker/health checks, and cleanup. Keeping
# this DRY also keeps behaviour consistent across e2e tests.
from test_run_service import (
    CONTAINER_STOP_WAIT,
    SERVICE_INIT_WAIT,
    STARTUP_WAIT,
    check_docker_status,
    cleanup_directory,
    ensure_service_stopped,
    get_config_specific_settings,
    log_expect_match,
    send_input_safely,
    setup_logging,
)

pytestmark = [pytest.mark.e2e, pytest.mark.integration]

# Single source of truth for env vars: the repo-root `.env`. CI writes it
# from secrets in the workflow's "Create .env file" step; locally the user
# maintains it. `os.environ` always wins (`override=False`), so explicit
# shell exports stay authoritative.
load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Two configs per quickstart. Pick the cheapest, fastest two on-chain agents.
# Using the same two configs for both quickstarts gives 4 distinct services
# on-chain (different master wallets per quickstart cwd).
QS_CONFIGS = (
    "configs/config_predict_trader.json",
    "configs/config_optimus.json",
)

MIGRATION_TIMEOUT = 1200  # 20 min — Mode B can include several Safe txs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _copy_repo_to(dest: Path, logger: logging.Logger) -> None:
    """Copy the repo into `dest`, excluding heavy/local-only paths.

    `.venv` is symlinked rather than copied: a deep copy via shutil
    dereferences interpreter symlinks and silently drops parts of the
    site-packages tree, and a fresh `poetry install` in `dest` triggers
    PEP 517 sdist rebuilds (halo, python-baseconv) that fail under
    Poetry 2.4 + setuptools >= 80. Sharing the runner's venv avoids
    both pitfalls."""
    shutil.copytree(
        ".", dest, dirs_exist_ok=True,
        ignore=shutil.ignore_patterns(
            ".operate", ".pytest_cache", "__pycache__",
            "*.pyc", "logs", "*.log", ".env", ".venv",
        ),
    )
    src_venv = Path(".venv").resolve()
    if src_venv.is_dir():
        link = dest / ".venv"
        if link.exists() or link.is_symlink():
            link.unlink()
        link.symlink_to(src_venv)
    logger.info(f"Copied repo to {dest}")


@contextlib.contextmanager
def _override_test_password(password: Optional[str]) -> Iterator[None]:
    """Temporarily set `os.environ['TEST_PASSWORD']` for prompt-map building.

    `test_run_service.get_config_specific_settings` reads the env var at
    call time and bakes it into the prompts dict, so to make a quickstart
    use a different master password we just swap the var around the call
    and restore on exit. No effect when `password` is None.
    """
    if password is None:
        yield
        return
    original = os.environ.get("TEST_PASSWORD")
    os.environ["TEST_PASSWORD"] = password
    try:
        yield
    finally:
        if original is None:
            os.environ.pop("TEST_PASSWORD", None)
        else:
            os.environ["TEST_PASSWORD"] = original


def _spawn_run_service(
    cwd: Path,
    config_path: str,
    env: Dict[str, str],
    logger: logging.Logger,
    password: Optional[str] = None,
) -> None:
    """Run `./run_service.sh <cfg>` in `cwd`, walking the existing prompt map.

    `password` overrides `TEST_PASSWORD` for the duration of the call (and
    inside the spawned subprocess), so two quickstarts in the same test
    can use different master passwords.
    """
    with _override_test_password(password):
        settings = get_config_specific_settings(config_path)
        spawn_env = {**env, "TEST_PASSWORD": password} if password else env
        logger.info(f"  -> run_service.sh {config_path} in {cwd}")
        child = pexpect.spawn(
            f"bash ./run_service.sh {config_path}",
            encoding="utf-8", timeout=600, env=spawn_env, cwd=str(cwd),
            logfile=sys.stdout,
        )
        try:
            while True:
                patterns = list(settings["prompts"].keys())
                idx = child.expect(patterns, timeout=600)
                pattern = patterns[idx]
                log_expect_match(child, pattern, idx, logger)
                response = settings["prompts"][pattern]
                if callable(response):
                    output = child.before + child.after
                    response = response(output, logger)
                send_input_safely(child, response, logger)
        except pexpect.EOF:
            logger.info("  -> run_service.sh completed")
    time.sleep(SERVICE_INIT_WAIT)
    if not check_docker_status(logger, config_path):
        raise RuntimeError(f"Service for {config_path} failed to come up")


def _spawn_stop_service(
    cwd: Path,
    config_path: str,
    env: Dict[str, str],
    logger: logging.Logger,
    password: Optional[str] = None,
) -> None:
    """Stop the deployment for one config in `cwd`."""
    with _override_test_password(password):
        settings = get_config_specific_settings(config_path)
        spawn_env = {**env, "TEST_PASSWORD": password} if password else env
        logger.info(f"  -> stop_service.sh {config_path} in {cwd}")
        child = pexpect.spawn(
            f"bash ./stop_service.sh {config_path}",
            encoding="utf-8", timeout=120, env=spawn_env, cwd=str(cwd),
            logfile=sys.stdout,
        )
        try:
            while True:
                patterns = list(settings["prompts"].keys())
                idx = child.expect(patterns, timeout=120)
                pattern = patterns[idx]
                log_expect_match(child, pattern, idx, logger)
                response = settings["prompts"][pattern]
                if callable(response):
                    output = child.before + child.after
                    response = response(output, logger)
                send_input_safely(child, response, logger)
        except pexpect.EOF:
            logger.info("  -> stop_service.sh completed")
    time.sleep(CONTAINER_STOP_WAIT)


def _spawn_migrate_to_pearl(
    cwd: Path,
    pearl_home: Path,
    password: str,
    pearl_password: str,
    env: Dict[str, str],
    logger: logging.Logger,
) -> None:
    """Run `./migrate_to_pearl.sh` in `cwd`, answering the prompt sequence.

    Drives both Mode A and Mode B paths transparently — the script is the
    same; only the password prompts and on-chain steps differ. We always
    try to answer:
      * multi-service "Select [1-N]:" → highest number ("all")
      * collision prompt (shouldn't fire on a happy-path test) → skip ("1")
      * quickstart password
      * Pearl password (Mode B only)
      * Rename source for rollback? → "y"
      * Run Pearl on a different machine? → "n"
    """
    logger.info(
        f"  -> migrate_to_pearl.sh in {cwd} (pearl_home={pearl_home})"
    )
    child = pexpect.spawn(
        f"bash ./migrate_to_pearl.sh --pearl-home {pearl_home}",
        encoding="utf-8", timeout=MIGRATION_TIMEOUT, env=env, cwd=str(cwd),
        logfile=sys.stdout,
    )
    # Track which password we're answering next.
    pw_state = {"asked_qs": False}

    pattern_map = {
        # multi-service selection: pick the largest number which is "all"
        r"Select \[1-(\d+)\]:": lambda out, *_: _all_choice(out),
        r"Choice \[1/2\]:": "1",  # if a collision somehow happened, skip
        r"\s*quickstart password:\s*$": password,
        r"\s*Pearl password:\s*$": pearl_password,
        r"Rename the source quickstart `\.operate/` to keep it as a rollback\?": "y",
        r"Do you want to run Pearl on a different machine than this one\?": "n",
    }

    def _all_choice(out: str) -> str:
        import re
        m = re.search(r"Select \[1-(\d+)\]:", out)
        return m.group(1) if m else "1"

    try:
        while True:
            patterns = list(pattern_map.keys())
            idx = child.expect(patterns, timeout=MIGRATION_TIMEOUT)
            pattern = patterns[idx]
            log_expect_match(child, pattern, idx, logger)
            resp = pattern_map[pattern]
            if callable(resp):
                out = child.before + child.after
                resp = resp(out, logger)
            send_input_safely(child, resp, logger)
            if "quickstart password" in pattern:
                pw_state["asked_qs"] = True
    except pexpect.EOF:
        logger.info("  -> migrate_to_pearl.sh completed")


def _list_pearl_services(pearl_home: Path) -> List[Path]:
    services_dir = pearl_home / "services"
    if not services_dir.exists():
        return []
    return sorted(p for p in services_dir.iterdir() if p.name.startswith("sc-"))


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestMigrateToPearlEndToEnd:
    """4 services across 2 quickstart cwds → 1 Pearl `.operate` → all start.

    State is shared across the four `test_NN_*` methods via class attributes
    set in `setup_class` (`work_root`, `qs_dirs`, `pearl_home`, `test_env`,
    `password`). Each step also depends on on-disk state created by the
    previous step (the per-quickstart `.operate` dirs, then Pearl's `.operate`
    dir). The `test_NN_*` numbering keeps pytest's default alphabetical order
    deterministic; running a single step in isolation will fail by design.
    `_failed` is set on the first failure so subsequent steps short-circuit
    rather than produce cascading misleading errors.
    """

    logger: logging.Logger
    work_root: tempfile.TemporaryDirectory
    qs_dirs: Tuple[Path, Path]
    pearl_home: Path
    test_env: Dict[str, str]
    password: str          # qs1 master password (and Pearl's, since Pearl
                           # inherits qs1's wallet via the Mode A copy)
    qs2_password: str      # qs2's master password — deliberately different,
                           # so test_03 exercises Mode B with two distinct
                           # passwords (quickstart vs Pearl).
    _failed: bool = False

    @classmethod
    def setup_class(cls) -> None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        cls.logger = setup_logging(Path(f"test_migrate_to_pearl_{ts}.log"))
        cls.password = os.getenv("TEST_PASSWORD", "test_secret")
        # Derive qs2's password from the base password so we don't need a
        # second secret in CI; the suffix just makes it deterministically
        # different.
        cls.qs2_password = f"{cls.password}_qs2"

        cls.work_root = tempfile.TemporaryDirectory(prefix="migrate_to_pearl_e2e_")
        root = Path(cls.work_root.name)

        # Two independent quickstart cwds (each gets its own .operate / wallet).
        qs1 = root / "qs1"
        qs2 = root / "qs2"
        qs1.mkdir()
        qs2.mkdir()
        _copy_repo_to(qs1, cls.logger)
        _copy_repo_to(qs2, cls.logger)

        # Pearl side gets its own HOME so `~/.operate` (the script default
        # target) is fully isolated from the developer / CI runner state.
        pearl_home_root = root / "home"
        pearl_home_root.mkdir()
        cls.pearl_home = pearl_home_root / OPERATE  # e.g. <root>/home/.operate
        cls.qs_dirs = (qs1, qs2)

        # Env shared across all spawned subprocesses. We drop VIRTUAL_ENV so
        # poetry inside the spawned shell creates / uses its own env, just
        # like test_run_service does.
        cls.test_env = os.environ.copy()
        cls.test_env.pop("VIRTUAL_ENV", None)
        cls.test_env.pop("POETRY_ACTIVE", None)
        cls.test_env["HOME"] = str(pearl_home_root)
        cls.test_env["TEST_PASSWORD"] = cls.password

    @classmethod
    def teardown_class(cls) -> None:
        # Best-effort: stop everything in both quickstart cwds before cleaning.
        # Use the per-quickstart password so the cleanup's stop_service.sh
        # doesn't hang on a password prompt.
        qs_dirs = getattr(cls, "qs_dirs", ())
        passwords = (
            getattr(cls, "password", None),
            getattr(cls, "qs2_password", None),
        )
        for qs, pw in zip(qs_dirs, passwords):
            for cfg in QS_CONFIGS:
                try:
                    with _override_test_password(pw):
                        ensure_service_stopped(cfg, str(qs), cls.logger)
                except Exception as exc:  # pylint: disable=broad-except
                    cls.logger.warning(f"teardown stop failed: {exc}")
        if hasattr(cls, "work_root"):
            cleanup_directory(cls.work_root.name, cls.logger)

    # ------------------------------------------------------------------
    # Helpers for the implicit step ordering
    # ------------------------------------------------------------------
    def setup_method(self) -> None:
        """Skip downstream steps once any earlier step has failed.

        Pytest-default behaviour would still run later steps after a failure,
        producing misleading cascade errors (e.g. "Pearl home doesn't exist"
        because step 02 never wrote it). Short-circuit instead.
        """
        if type(self)._failed:
            pytest.skip("earlier step in the migration sequence failed; skipping")

    def _step(self, fn) -> None:
        """Run a step body, marking the class as failed if it raises."""
        try:
            fn()
        except BaseException:
            type(self)._failed = True
            raise

    # ------------------------------------------------------------------
    # Setup phase: deploy 2 services in each quickstart, then stop them.
    # ------------------------------------------------------------------
    def test_01_deploy_two_services_per_quickstart(self) -> None:
        def _body() -> None:
            for qs in self.qs_dirs:
                # qs1 uses the base password; qs2 uses a different one so
                # the Mode B merge in test_03 has two distinct passwords
                # to handle (quickstart-side != Pearl-side).
                pw = self.password if qs is self.qs_dirs[0] else self.qs2_password
                self.logger.info(
                    f"=== Deploying services in {qs} (password={'qs1' if pw == self.password else 'qs2'}) ==="
                )
                for cfg in QS_CONFIGS:
                    _spawn_run_service(qs, cfg, self.test_env, self.logger, password=pw)
                    _spawn_stop_service(qs, cfg, self.test_env, self.logger, password=pw)

            # Sanity check: each cwd should now have 2 sc-* dirs in its .operate.
            for qs in self.qs_dirs:
                services = _list_pearl_services(qs / OPERATE)
                assert len(services) == 2, (
                    f"Expected 2 services in {qs / OPERATE}, found {len(services)}: "
                    f"{[p.name for p in services]}"
                )

        self._step(_body)

    # ------------------------------------------------------------------
    # Migration phase: qs1 (Mode A) then qs2 (Mode B).
    # ------------------------------------------------------------------
    def test_02_migrate_first_quickstart_mode_a(self) -> None:
        def _body() -> None:
            assert not self.pearl_home.exists(), \
                "Pearl home should not exist before the first migration"

            _spawn_migrate_to_pearl(
                cwd=self.qs_dirs[0],
                pearl_home=self.pearl_home,
                password=self.password,
                pearl_password=self.password,  # unused on Mode A
                env=self.test_env,
                logger=self.logger,
            )

            services = _list_pearl_services(self.pearl_home)
            assert len(services) == 2, (
                f"After Mode A, expected 2 services in {self.pearl_home / 'services'}, "
                f"found {[p.name for p in services]}"
            )
            # Master wallet from qs1 must be present.
            assert (self.pearl_home / "wallets" / "ethereum.json").exists()
            assert (self.pearl_home / "wallets" / "ethereum.txt").exists()

        self._step(_body)

    def test_03_migrate_second_quickstart_mode_b(self) -> None:
        def _body() -> None:
            assert self.pearl_home.exists(), \
                "Pearl home must exist before the Mode B migration"

            # qs2 has its own master password; Pearl's wallet is qs1's
            # (carried over by the Mode A copy in test_02), so its password
            # is `self.password`. The migration script prompts for both.
            _spawn_migrate_to_pearl(
                cwd=self.qs_dirs[1],
                pearl_home=self.pearl_home,
                password=self.qs2_password,
                pearl_password=self.password,
                env=self.test_env,
                logger=self.logger,
            )

            services = _list_pearl_services(self.pearl_home)
            assert len(services) == 4, (
                f"After Mode B merge, expected 4 services in {self.pearl_home / 'services'}, "
                f"found {[p.name for p in services]}"
            )

            # Each service's referenced agent key must be present.
            for svc_dir in services:
                cfg = json.loads((svc_dir / "config.json").read_text())
                for addr in cfg.get("agent_addresses", []):
                    assert (self.pearl_home / "keys" / addr).exists(), (
                        f"Missing key {addr} for service {svc_dir.name}"
                    )

        self._step(_body)

    # ------------------------------------------------------------------
    # Verification phase: each migrated service can be started by Pearl.
    # ------------------------------------------------------------------
    def test_04_all_services_can_start_from_pearl_home(self) -> None:
        def _body() -> None:
            operate = OperateApp(home=self.pearl_home)
            operate.password = self.password
            # Sanity: the wallet manager unlocks with the same TEST_PASSWORD,
            # demonstrating the migrated wallet is usable.
            assert operate.wallet_manager.is_password_valid(self.password), (
                "Migrated master wallet rejected the test password"
            )
            # Loading the wallet shouldn't raise.
            operate.wallet_manager.load(LedgerType.ETHEREUM)

            manager = operate.service_manager()
            services = _list_pearl_services(self.pearl_home)
            assert len(services) == 4

            # ---- Direct on-chain assertions of the migration's invariants ----
            # Don't rely on "deploy succeeded" as a proxy: a wrong-owner state
            # with sufficient remaining permissions could still pass that.
            # Read NFT owner + Safe owner list directly per chain.
            from operate.ledger.profiles import CONTRACTS
            from operate.operate_types import Chain
            from scripts.pearl_migration.status import (
                safe_owners as _safe_owners,
                service_nft_owner as _service_nft_owner,
            )
            pearl_safes = operate.wallet_manager.load(LedgerType.ETHEREUM).safes
            for svc_dir in services:
                cfg = json.loads((svc_dir / "config.json").read_text())
                sid = cfg["service_config_id"]
                svc_obj = manager.load(service_config_id=sid)
                for chain_str, chain_config in svc_obj.chain_configs.items():
                    chain_enum = Chain(chain_str)
                    pearl_safe = pearl_safes[chain_enum]
                    sftxb = manager.get_eth_safe_tx_builder(
                        ledger_config=chain_config.ledger_config,
                    )
                    nft_owner = _service_nft_owner(
                        ledger_api=sftxb.ledger_api,
                        service_registry_address=CONTRACTS[chain_enum]["service_registry"],
                        service_id=chain_config.chain_data.token,
                    )
                    assert nft_owner is not None and nft_owner.lower() == pearl_safe.lower(), (
                        f"[{chain_str}] service NFT {chain_config.chain_data.token} "
                        f"owner is {nft_owner}, expected Pearl Safe {pearl_safe}"
                    )
                    owners = _safe_owners(
                        ledger_api=sftxb.ledger_api,
                        safe=chain_config.chain_data.multisig,
                    )
                    owners_lower = {o.lower() for o in owners}
                    assert pearl_safe.lower() in owners_lower, (
                        f"[{chain_str}] service Safe {chain_config.chain_data.multisig} "
                        f"owners {owners} should include Pearl Safe {pearl_safe}"
                    )

            # Build a service-name -> origin config path map by reading the
            # QS_CONFIGS files. This avoids `test_run_service.get_service_config`'s
            # substring matching (which only works on config-file paths, not
            # human names like "Trader Agent").
            name_to_qs_cfg: Dict[str, str] = {}
            for qs_cfg_path in QS_CONFIGS:
                qs_cfg_data = json.loads(Path(qs_cfg_path).read_text())
                name_to_qs_cfg[qs_cfg_data["name"]] = qs_cfg_path

            started: List[str] = []
            for svc_dir in services:
                cfg = json.loads((svc_dir / "config.json").read_text())
                sid = cfg["service_config_id"]
                self.logger.info(
                    f"=== Re-deploying migrated service {sid} ({cfg['name']}) ==="
                )

                service = manager.load(service_config_id=sid)
                chain = service.home_chain

                # 1) On-chain re-deploy from Pearl's master Safe. After our
                # Mode B migration the service is in PRE_REGISTRATION owned
                # by the Pearl master Safe. This walks it back through
                # ACTIVE_REGISTRATION -> FINISHED_REGISTRATION -> DEPLOYED,
                # exercising the chain-side code Pearl will run when the
                # user clicks "Run" on the migrated service.
                self.logger.info(f"  on-chain deploy from Safe ({chain})")
                manager.deploy_service_onchain_from_safe(service_config_id=sid)

                # 2) Local docker deploy.
                self.logger.info(f"  local docker deploy ({chain})")
                manager.deploy_service_locally(
                    service_config_id=sid, chain=chain, use_docker=True,
                )
                time.sleep(STARTUP_WAIT)

                # Resolve back to the config path so check_docker_status can
                # find the agent-specific container_name (it does substring
                # matching on the path).
                origin_cfg = name_to_qs_cfg.get(cfg["name"])
                assert origin_cfg is not None, (
                    f"Migrated service name {cfg['name']!r} doesn't map back to "
                    f"any of QS_CONFIGS; cannot resolve container name."
                )
                assert check_docker_status(self.logger, origin_cfg), (
                    f"Service {sid} failed to start under Pearl `.operate`"
                )
                started.append(sid)

                # Stop again before the next iteration so concurrent
                # containers don't compete for ports.
                manager.stop_service_locally(service_config_id=sid, force=True)
                time.sleep(CONTAINER_STOP_WAIT)

            assert len(started) == 4, f"Only started {len(started)}/4 services"

        self._step(_body)


if __name__ == "__main__":
    pytest.main(["-v", __file__, "-s", "--log-cli-level=INFO"])
