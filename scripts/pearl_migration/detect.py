"""Locate quickstart and Pearl `.operate` stores and classify migration mode.

Reuses the middleware's official primitives where possible:

- `operate.services.service.Service` — service config dataclass + loader.
- `operate.wallet.master.MasterWalletManager` — wallet existence check.
- `operate.constants.{OPERATE, SERVICES_DIR, KEYS_DIR, WALLETS_DIR}`.
- `operate.services.service.SERVICE_CONFIG_PREFIX`.

`OperateApp` is built lazily via `OperateStore.operate_app(password=...)` so
read-only paths (preflight, dry-run, mode classification) don't trigger
middleware migrations or version-bump backups.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from operate.constants import KEYS_DIR, SERVICES_DIR, WALLETS_DIR
from operate.operate_types import LedgerType
from operate.services.service import SERVICE_CONFIG_PREFIX, Service
from operate.wallet.master import MasterWalletManager

if TYPE_CHECKING:
    from operate.cli import OperateApp


class Mode(Enum):
    """Migration mode."""

    FRESH_COPY = "fresh_copy"  # Pearl `.operate` does not exist (or has no wallet)
    MERGE = "merge"            # Pearl has its own master wallet; we must merge
    NOOP = "noop"              # Quickstart `.operate` already is `~/.operate`


@dataclass(frozen=True)
class OperateStore:
    """One on-disk `.operate` directory.

    Thin facade — listing services and checking wallet existence delegate to
    the middleware. Creating a real `OperateApp` is deferred until the caller
    actually needs to run migrations / sign on-chain (see `operate_app`).

    `frozen=True`: prevents accidental mutation of `root` (which would
    invalidate the cached `OperateApp`). The lazy app is held in a
    private dict to satisfy the immutability constraint while still
    allowing memoization.
    """

    root: Path                                  # absolute, resolved
    _state: Dict[str, "OperateApp"] = field(
        default_factory=dict, repr=False, compare=False, hash=False,
    )

    def __post_init__(self) -> None:
        # Enforce the "absolute + resolved" invariant the docstring promises.
        # Always resolve so absolute paths with `..` segments / symlinks
        # also normalize. Use object.__setattr__ because `frozen=True`
        # blocks plain assignment. Wrap in try/except so a permissions error
        # on Path.resolve() doesn't surface as a raw OSError out of a
        # dataclass constructor.
        try:
            resolved = self.root.resolve()
        except OSError as exc:
            raise ValueError(
                f"Cannot resolve store root '{self.root}': {exc}. "
                "Pass an accessible absolute path."
            ) from exc
        if resolved != self.root:
            # Tell the user when resolution changed the path — otherwise
            # subsequent log lines, error messages and the rename-source
            # step refer to a path they may not recognise (e.g. a symlink
            # target). Lazy import: prompts.py depends on operate.* which
            # we want kept off the detect.py import path.
            from .prompts import info as _info
            _info(f"Resolved store root: {self.root} -> {resolved}")
            object.__setattr__(self, "root", resolved)

    @property
    def wallets_dir(self) -> Path:
        return self.root / WALLETS_DIR

    @property
    def keys_dir(self) -> Path:
        return self.root / KEYS_DIR

    @property
    def services_dir(self) -> Path:
        return self.root / SERVICES_DIR

    def has_master_wallet(self) -> bool:
        """True iff the Ethereum master wallet keystore is present.

        Uses the middleware's `MasterWalletManager.exists` so we share the
        same "what counts as a wallet" definition the daemon uses.
        """
        if not self.wallets_dir.exists():
            return False
        return MasterWalletManager(path=self.wallets_dir).exists(LedgerType.ETHEREUM)

    def services(self) -> List[Service]:
        """Return all `sc-*` services as middleware `Service` objects.

        Side-effect-free: uses `Service.load(path)` directly rather than
        spinning up an `OperateApp` (which would run migrations). Services
        whose `config.json` won't load are skipped with a `warn(...)` —
        they get reported via `failed_services()` so callers can refuse
        to drain master wallets when any service was orphaned.
        """
        loaded, _ = self._enumerate()
        return loaded

    def failed_services(self) -> List[Tuple[Path, BaseException]]:
        """Return `(path, exception)` for any `sc-*` dir that couldn't load.

        Companion to `services()` — exposes the dropped entries so callers
        can refuse destructive follow-ups (drains, source rename) when the
        migration plan is incomplete.
        """
        _, failed = self._enumerate()
        return failed

    def _enumerate(
        self,
    ) -> Tuple[List[Service], List[Tuple[Path, BaseException]]]:
        # Local import to avoid a circular dependency at module load.
        from .prompts import warn

        loaded: List[Service] = []
        failed: List[Tuple[Path, BaseException]] = []
        if not self.services_dir.exists():
            return loaded, failed
        for child in sorted(self.services_dir.iterdir()):
            if not child.is_dir() or not child.name.startswith(SERVICE_CONFIG_PREFIX):
                continue
            if not (child / "config.json").exists():
                continue
            try:
                loaded.append(Service.load(path=child))
            except Exception as exc:  # pylint: disable=broad-except
                # Bad config / version mismatch / etc. Surface so the caller
                # can refuse to drain master if any service was orphaned.
                warn(f"Skipping {child.name}: cannot load config ({exc!r}).")
                failed.append((child, exc))
        return loaded, failed

    def operate_app(self, password: Optional[str] = None) -> "OperateApp":
        """Lazily build (and cache) an `OperateApp` for this `.operate` root.

        Heavy: instantiation runs the middleware's migration suite and may
        backup the dir on a version bump. Only call this once you're
        committed to actually mutating / signing against the store. Pass
        `password` when known so `app.password = ...` is set right away.
        """
        cached = self._state.get("app")
        if cached is None:
            from operate.cli import OperateApp  # local import: heavy module

            cached = OperateApp(home=self.root)
            self._state["app"] = cached
        if password is not None:
            cached.password = password
        return cached


@dataclass(frozen=True)
class Discovery:
    """Result of discovery.

    Carries its own invariant: `mode == NOOP` iff the two stores point at
    the same root. The factory `discover()` upholds this; `__post_init__`
    re-checks so direct construction (in tests, etc.) can't lie.
    """

    quickstart: OperateStore
    pearl: OperateStore
    mode: Mode

    def __post_init__(self) -> None:
        same_root = self.quickstart.root == self.pearl.root
        if same_root and self.mode != Mode.NOOP:
            raise ValueError(
                f"Discovery: stores share root {self.quickstart.root} but "
                f"mode is {self.mode}; expected Mode.NOOP."
            )
        if not same_root and self.mode == Mode.NOOP:
            raise ValueError(
                f"Discovery: mode is NOOP but stores have different roots "
                f"({self.quickstart.root} vs {self.pearl.root})."
            )

    @property
    def is_noop(self) -> bool:
        return self.mode == Mode.NOOP


def discover(
    quickstart_root: Optional[Path] = None,
    pearl_root: Optional[Path] = None,
) -> Discovery:
    """Discover the two stores and classify the migration mode.

    `quickstart_root` defaults to `<cwd>/.operate`. `pearl_root` defaults to
    `~/.operate`. Both are resolved (symlinks followed) before comparison so
    the no-op detection is reliable.
    """
    qs_root = (quickstart_root or Path.cwd() / ".operate").resolve()
    pl_root = (pearl_root or Path.home() / ".operate").resolve()

    qs = OperateStore(root=qs_root)
    pl = OperateStore(root=pl_root)

    if qs_root == pl_root:
        return Discovery(quickstart=qs, pearl=pl, mode=Mode.NOOP)

    if not pl_root.exists() or not pl.has_master_wallet():
        return Discovery(quickstart=qs, pearl=pl, mode=Mode.FRESH_COPY)

    return Discovery(quickstart=qs, pearl=pl, mode=Mode.MERGE)
