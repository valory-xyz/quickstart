#!/usr/bin/env python3
# ------------------------------------------------------------------------------
#
#   Copyright 2026 Valory AG
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# ------------------------------------------------------------------------------

"""Migrate quickstart `.operate/` to Pearl's `~/.operate/`.

Two modes (auto-detected):

* **Fresh copy** — `~/.operate` does not exist (or is empty / has no Pearl
  master wallet). The whole `.operate` is copied over. Pearl will pick it up
  on first launch and re-use the same master wallet.

* **Merge** — `~/.operate` already has a Pearl master wallet. Per service we
  unstake on-chain, transfer the service NFT from the quickstart master Safe
  to the Pearl master Safe, swap the service Safe owner, and copy the service
  files. After all services succeed, the quickstart master Safe + EOA balances
  are drained into Pearl's master Safe + EOA. If any service can't be unstaked
  yet, drains are skipped so funds remain available for a later attempt.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Literal,
    NoReturn,
    Optional,
    Tuple,
    Type,
)

from operate.operate_types import Chain, OnChainState
from operate.quickstart.utils import (
    print_section,
    print_title,
    wei_to_token,
)
from operate.services.service import Service

# Programming bugs we DO NOT want wrapped as `_Unmigratable("NFT transfer
# failed: ...")`. Letting these propagate surfaces the real fault (with a
# real traceback) instead of disguising it as a chain-side issue. Add to
# this list cautiously — anything in it MUST NEVER originate from a normal
# RPC / on-chain failure path. Notable omissions:
#   * `KeyError` / `LookupError`: web3 + middleware code raises these on
#     missing chain metadata (e.g. `CONTRACTS[chain]`, `tx_receipt["status"]`,
#     ABI lookups). One bad chain shouldn't crash the whole multi-service
#     batch — let it become a per-service `_Unmigratable` instead.
_PROGRAMMING_BUGS: Tuple[Type[BaseException], ...] = (
    TypeError,
    AttributeError,
    NameError,
    ImportError,
)


def _reraise_if_programming_bug(exc: BaseException) -> None:
    """Re-raise `exc` if it's a programming bug.

    Use at every `except Exception` site that wraps the exception into
    a chain-side failure (`_Unmigratable`, `_DrainFailure`, or a `warn(...)`
    downgrade). Without it, a `TypeError` / `AttributeError` introduced
    by a middleware refactor would be silently logged as "RPC failed,
    retry" and the user would chase the wrong root cause.
    """
    if isinstance(exc, _PROGRAMMING_BUGS):
        raise exc


def _wrap_step_failure(
    *, sid: str, chain: str, prefix: str, exc: BaseException,
) -> "_Unmigratable":
    """Build the `_Unmigratable` for a step's `except` block.

    Re-raises members of `_PROGRAMMING_BUGS` (TypeError, AttributeError,
    NameError, ImportError) so they surface as real tracebacks rather
    than getting smuggled into the user's "chain failed" summary.
    Everything else — including `KeyError`/`LookupError` raised by web3
    or middleware on missing chain metadata — gets wrapped so a single
    bad chain doesn't crash the whole multi-service batch.
    """
    _reraise_if_programming_bug(exc)
    return _Unmigratable(
        service_id=sid, chain=chain, reason=f"{prefix}: {exc}.",
    )


from scripts.pearl_migration.detect import (
    Discovery,
    Mode,
    OperateStore,
    discover,
)

if TYPE_CHECKING:
    from operate.cli import OperateApp
    from operate.services.manage import ServiceManager
    from operate.services.service import Service
    from operate.operate_types import Chain
    from operate.wallet.master import MasterWallet
from scripts.pearl_migration.filesystem import (
    fix_root_ownership,
    fresh_copy_store,
    merge_service,
    rename_source_for_rollback,
)
from scripts.pearl_migration.prompts import (
    ask_password_validating,
    fatal,
    info,
    warn,
    yes_no,
)
from scripts.pearl_migration.status import (
    docker_quickstart_containers,
    pearl_daemon_running,
    safe_owners,
    safe_threshold,
    service_nft_owner,
)
from scripts.pearl_migration.stop import (
    force_remove_known_containers,
    stop_via_middleware,
)
from scripts.pearl_migration.wallet import align_quickstart_password


# ---------------------------------------------------------------------------
# args
# ---------------------------------------------------------------------------


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="migrate_to_pearl.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "config_path",
        nargs="?",
        help=(
            "Optional path to a quickstart agent config JSON. "
            "If given, only the matching service is migrated. "
            "If omitted, the script lists all services found and asks."
        ),
    )
    p.add_argument(
        "--quickstart-home",
        type=Path,
        default=None,
        help="Override quickstart `.operate` location (default: ./.operate).",
    )
    p.add_argument(
        "--pearl-home",
        type=Path,
        default=None,
        help="Override Pearl `.operate` location (default: ~/.operate).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover and report only; do not stop services, transfer, or copy anything.",
    )
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# Service selection
# ---------------------------------------------------------------------------

def _select_services(
    qs: OperateStore,
    config_path: Optional[str],
) -> List[Service]:
    services = qs.services()
    if not services:
        fatal(f"No services found under {qs.services_dir}.")

    if config_path is None:
        if len(services) == 1:
            return services
        print()
        print("Multiple services found in this quickstart:")
        for idx, svc in enumerate(services, 1):
            print(f"  {idx}) {svc.name}  ({svc.service_config_id})")
        print(f"  {len(services) + 1}) all of the above")
        while True:
            try:
                raw = input("Select [1-{}]: ".format(len(services) + 1)).strip()
            except EOFError:
                # Piped/closed stdin (CI, automation) — re-prompting forever
                # would hang. Surface as a clean fatal instead of a raw
                # `EOFError: EOF when reading a line` traceback.
                fatal("No selection provided (stdin closed). Re-run with "
                      "`--quickstart-config <path>` or attach a tty.")
            if raw.isdigit():
                n = int(raw)
                if 1 <= n <= len(services):
                    return [services[n - 1]]
                if n == len(services) + 1:
                    return services
            warn(f"Invalid selection '{raw}'. Enter a number 1-{len(services) + 1}.")

    # config_path given: match by 'name' or 'hash'
    import json
    try:
        cfg = json.loads(Path(config_path).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        fatal(f"Cannot read config {config_path}: {exc}")
    name = cfg.get("name")
    hash_ = cfg.get("hash")
    matches = [s for s in services if s.name == name]
    if not matches and hash_:
        matches = [s for s in services if s.hash == hash_]
    if not matches:
        fatal(f"No service in {qs.services_dir} matches config '{config_path}'.")
    return matches


# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------

def _preflight(disc: Discovery) -> None:
    info(f"Quickstart .operate: {disc.quickstart.root}")
    info(f"Pearl .operate:      {disc.pearl.root}")
    info(f"Mode:                {disc.mode.value}")

    if disc.mode == Mode.NOOP:
        print()
        print("Quickstart's .operate is already at Pearl's expected location.")
        print("Nothing to migrate. Start Pearl and enter your master password.")
        sys.exit(0)

    if pearl_daemon_running():
        fatal(
            "Pearl appears to be running (port 8765 is in use). "
            "Quit Pearl before re-running this script."
        )

    leftovers = docker_quickstart_containers()
    if leftovers:
        warn(
            "Quickstart docker containers detected: "
            + ", ".join(leftovers)
            + ". They will be stopped during migration."
        )


# ---------------------------------------------------------------------------
# Wallet loading
# ---------------------------------------------------------------------------

def _load_wallet(
    store: OperateStore, label: str,
) -> Tuple["OperateApp", "MasterWallet"]:
    """Prompt for the password for `store` and return (OperateApp, wallet).

    Validates the password against a lightweight `MasterWalletManager`
    BEFORE building the full `OperateApp` for the store. `OperateApp`
    instantiation triggers middleware migrations and a version-bump
    backup of the `.operate` dir; we want neither to happen against
    Pearl's store on a typo'd password.
    """
    from operate.operate_types import LedgerType
    from operate.wallet.master import MasterWalletManager

    if not store.has_master_wallet():
        fatal(f"No master wallet at {store.wallets_dir}; cannot continue.")

    print()
    print(f"Enter the {label} master password:")
    # Lightweight validator — touches only `wallets/`, doesn't run migrations.
    validator = MasterWalletManager(path=store.wallets_dir)
    pw = ask_password_validating(
        prompt=f"  {label} password: ",
        validate=validator.is_password_valid,
    )
    if pw is None:
        fatal(f"Could not unlock {label} master wallet.")

    # Password good; now safe to build the full app (with migrations).
    app = store.operate_app(password=pw)
    wallet = app.wallet_manager.load(LedgerType.ETHEREUM)
    return app, wallet


# ---------------------------------------------------------------------------
# Shared cleanup helper (used by both Mode A and Mode B)
# ---------------------------------------------------------------------------

def _ensure_containers_stopped(
    on_failure: Callable[[str], NoReturn],
) -> List[str]:
    """Force-remove known quickstart containers and re-probe to confirm.

    Both modes need the same three-step guarantee before touching the
    qs `.operate/`:
      1. `force_remove_known_containers()` — best-effort `docker rm -f`.
      2. Re-probe via `docker_quickstart_containers()` — catches the
         case where step 1 returned cleanly but the daemon refused the
         rm with a non-zero exit (the old `check=False` behavior; now
         tightened to `check=True` but the re-probe is still the
         belt-and-braces guard against future regressions).
      3. Surface any "still running" via `on_failure`.

    `on_failure` MUST NOT return — it must either raise or
    `sys.exit`. Callers decide the failure shape:
      * Mode A passes `fatal` (global, no aggregation possible).
      * Mode B passes a closure that raises `_Unmigratable(service_id=...)`
        so the orchestrator can keep migrating other services.

    Returns the list of containers that step 1 force-removed (may be
    empty). Callers can log this; failure paths never return so the
    caller doesn't need to check.
    """
    try:
        leftovers = force_remove_known_containers()
    except (subprocess.TimeoutExpired, RuntimeError) as exc:
        on_failure(
            f"could not confirm containers are stopped: {exc}. "
            "Stop the running deployment manually (`docker ps`, "
            "`docker rm -f`) and re-run."
        )
    try:
        remaining = docker_quickstart_containers()
    except RuntimeError as exc:
        on_failure(f"could not verify containers stopped: {exc}.")
    if remaining:
        on_failure(
            f"quickstart containers still running after cleanup: "
            f"{remaining}. Stop them manually (`docker ps`, "
            "`docker rm -f`) before re-running."
        )
    return leftovers


# ---------------------------------------------------------------------------
# Mode A — fresh copy
# ---------------------------------------------------------------------------

def _run_mode_a(disc: Discovery, dry_run: bool) -> MigrationOutcome:
    """Run the fresh-copy mode and return an empty `MigrationOutcome`
    (which is `is_complete=True` by construction) so the caller can
    plumb a uniform `MigrationOutcome` through `_final_prompt`.

    Mode A has no per-service / per-chain partial-failure state: any
    `OSError` / `shutil.Error` from the copy or rename callees propagates
    uncaught and aborts the run. There's no `try/except` here because
    there's no recovery to attempt — a half-copy is worse than no copy.
    """
    print_section("FRESH COPY")
    info("Pearl's `.operate` does not yet exist (or is empty). Doing a full copy.")

    # If Pearl dir exists but no wallet, back it up so the copy is clean.
    if disc.pearl.root.exists():
        from scripts.pearl_migration.prompts import backup_suffix as _bs
        bak = disc.pearl.root.with_name(f"{disc.pearl.root.name}.bak.{_bs()}")
        if dry_run:
            info(f"[dry-run] Would back up empty Pearl dir to {bak}.")
        else:
            disc.pearl.root.rename(bak)
            info(f"Backed up empty Pearl dir to {bak}.")

    if dry_run:
        info(f"[dry-run] Would copy {disc.quickstart.root} -> {disc.pearl.root}")
        return MigrationOutcome()

    # Stop and verify any quickstart containers are gone. Mode A has no
    # per-service aggregation, so failures escalate to `fatal()` — copying
    # `.operate/` while the daemon is still writing to `persistent_data/`
    # or `deployment/nodes/.../{config,data}/` would corrupt the migration,
    # and the subsequent `rename_source_for_rollback` would make it
    # unrecoverable.
    leftovers = _ensure_containers_stopped(on_failure=fatal)
    if leftovers:
        info(f"Removed leftover containers: {', '.join(leftovers)}")

    # Fix root-owned persistent_data BEFORE the copy so the copy is owned by us.
    fix_root_ownership(disc.quickstart)

    fresh_copy_store(disc.quickstart, disc.pearl.root)
    print()
    info("Renaming source `.operate/` so a re-run won't pick it up.")
    rename_source_for_rollback(disc.quickstart)
    return MigrationOutcome()


# ---------------------------------------------------------------------------
# Mode B — merge
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _Unmigratable(Exception):
    """Raised when a single service can't proceed but others might.

    Carries structured context so the orchestrator can print a useful
    "what was left behind" summary at the end without parsing strings.

    `frozen=True` because:
      * `MigrationOutcome` is `frozen=True` and contains
        `Tuple[_Unmigratable, ...]`; if `_Unmigratable` were unhashable
        the outcome's auto-generated `__hash__` would fail at runtime.
      * Exceptions shouldn't be mutated after raise anyway.

    `__post_init__` forwards all three fields to `Exception.__init__` so
    `traceback.format_exception` and any code reading `exc.args` sees
    the structured payload — the dataclass-synthesized `__init__`
    doesn't populate `args` automatically.

    `__reduce__` is explicit so `multiprocessing` / `concurrent.futures`
    serialization round-trips correctly. Default `Exception.__reduce__`
    would call `cls(*self.args)` which only works as long as `args`
    matches our positional `__init__` signature — keeping both aligned
    is the contract.
    """

    service_id: str
    chain: Optional[str]
    reason: str

    def __post_init__(self) -> None:
        super().__init__(self.service_id, self.chain, self.reason)

    def __reduce__(self) -> tuple:
        return (self.__class__, (self.service_id, self.chain, self.reason))

    def __str__(self) -> str:  # pragma: no cover - trivial formatter
        where = f" on {self.chain}" if self.chain else ""
        return f"{self.service_id}{where}: {self.reason}"


DrainSourceKind = Literal["Safe", "EOA", "Safe+EOA"]


@dataclass(frozen=True)
class _DrainFailure:
    """One per-source drain that didn't complete.

    `chain` is the middleware `Chain` enum (not a stringified name) so the
    summary table sorts and groups consistently with everything else that
    talks chains. `source_kind` is constrained at the type level.
    """

    chain: "Chain"
    source_kind: DrainSourceKind
    source_address: str
    reason: str

    @property
    def chain_name(self) -> str:
        # Display helper: the summary template formats by chain name, not enum repr.
        return self.chain.name


@dataclass(frozen=True)
class MigrationOutcome:
    """Aggregate result of a Mode B run.

    One value object propagates from `_run_mode_b` through `_print_migration_summary`
    and `_final_prompt`, replacing the prior `(bool, list, list, int)` data clump.
    """

    migrated: Tuple[Service, ...] = ()
    unmigratable: Tuple["_Unmigratable", ...] = ()
    drain_failures: Tuple[_DrainFailure, ...] = ()

    @property
    def is_complete(self) -> bool:
        return not self.unmigratable and not self.drain_failures

    @property
    def migrated_count(self) -> int:
        return len(self.migrated)


def _run_mode_b(
    disc: Discovery,
    services: List[Service],
    config_path: Optional[str],
    dry_run: bool,
) -> MigrationOutcome:
    """Returns the structured `MigrationOutcome`. `outcome.is_complete` False
    suppresses the source-`.operate/` rename so the user can resume."""
    print_section("MERGE")
    info(f"Will migrate {len(services)} service(s): " + ", ".join(s.name for s in services))

    if dry_run:
        info("[dry-run] Stopping at discovery; no on-chain or filesystem actions taken.")
        return MigrationOutcome()

    # If `OperateStore.services()` dropped any malformed configs we MUST NOT
    # drain — the dropped service's NFT/Safe would be orphaned with empty
    # master wallets behind. Refuse before touching anything.
    failed_loads = disc.quickstart.failed_services()
    if failed_loads:
        for path, exc in failed_loads:
            warn(f"  service config could not be loaded: {path.name} ({exc!r})")
        fatal(
            "Refusing to migrate while one or more service configs failed to load — "
            "the master wallet drain would orphan those services. Resolve the "
            "config errors above and re-run.",
        )

    # ---- load BOTH master wallets up front -----------------------------------
    qs_app, qs_wallet = _load_wallet(disc.quickstart, "quickstart")
    pearl_app, pearl_wallet = _load_wallet(disc.pearl, "Pearl")

    # ---- align master passwords if they differ -------------------------------
    # Pearl's keys manager will use Pearl's password to decrypt every agent
    # key it inherits from us. If qs and Pearl were initialised with
    # different passwords, the agent keys must be re-encrypted with Pearl's
    # password BEFORE merging — otherwise the first deploy from Pearl fails
    # with `DecryptError: Decrypt error! Bad password?`.
    if qs_app.password != pearl_app.password:
        print_section("ALIGNING QUICKSTART PASSWORD")
        info(
            "The quickstart and Pearl master passwords differ. Before "
            "merging, the quickstart's master keyfile and every agent key "
            "under `keys/` will be re-encrypted with Pearl's password. The "
            "original keyfiles are kept as `.bak` siblings."
        )
        if not yes_no("Proceed with re-encryption?", default=True):
            fatal("Migration aborted by user; nothing on-chain has changed.")
        align_quickstart_password(
            qs_app=qs_app,
            qs_wallet=qs_wallet,
            new_password=pearl_app.password,
        )

    fix_root_ownership(disc.quickstart)

    unmigratable: List[_Unmigratable] = []
    migrated: List[Service] = []
    # Collected from each migrated service's chain_configs; passed to the
    # drain step so Pearl Safes can be created on chains where Pearl
    # didn't have one yet (using the same RPC the migration was driven by).
    chain_rpcs: dict = {}

    from operate.operate_types import Chain as _Chain

    for svc in services:
        print_section(f"Migrating service: {svc.name}  ({svc.service_config_id})")
        # Snapshot the service's chain RPCs up front — used by the drain
        # step even if the per-service migration partially fails. If two
        # services declare different RPCs for the same chain, the first
        # wins (deterministic) but we warn so the user can investigate
        # rather than silently inheriting one over the other.
        for chain_str, chain_config in svc.chain_configs.items():
            chain_key = _Chain(chain_str)
            new_rpc = chain_config.ledger_config.rpc
            existing_rpc = chain_rpcs.get(chain_key)
            if existing_rpc is None:
                chain_rpcs[chain_key] = new_rpc
            elif existing_rpc != new_rpc:
                warn(
                    f"  service {svc.name} declares a different RPC for "
                    f"{chain_str} than a previous service "
                    f"({existing_rpc!r} vs {new_rpc!r}); keeping the first "
                    "for the master-wallet drain step.",
                )

        try:
            _migrate_one_service(
                svc=svc,
                qs_app=qs_app,
                qs_wallet=qs_wallet,
                pearl_wallet=pearl_wallet,
                config_path=config_path,
            )
        except _Unmigratable as exc:
            warn(f"Skipping service {svc.name}: {exc}")
            unmigratable.append(exc)
            continue
        # Filesystem copy runs AFTER on-chain ops have committed. A copy
        # failure here is the worst-case "half-migrated" state: NFT is
        # already owned by Pearl Safe + service Safe owner is already
        # swapped, but the local files haven't moved. Convert to
        # `_Unmigratable` so it gets aggregated into the outcome and the
        # user is told exactly what to do — instead of a raw traceback
        # that aborts the whole batch, skips drain + rename, and leaves
        # them with no idea the on-chain side is committed.
        try:
            merge_service(service=svc, src=disc.quickstart, dest=disc.pearl)
        except (OSError, shutil.Error) as exc:
            warn(f"Skipping service {svc.name}: filesystem copy failed: {exc}")
            unmigratable.append(_Unmigratable(
                service_id=svc.service_config_id, chain=None,
                reason=(
                    f"on-chain migration committed but filesystem copy "
                    f"failed: {exc}. Manually copy "
                    f"`services/{svc.service_config_id}/` and the agent "
                    f"keys (under `keys/`) from {disc.quickstart.root} to "
                    f"{disc.pearl.root} before launching Pearl."
                ),
            ))
            continue
        migrated.append(svc)

    # ---- final master-wallet drains ------------------------------------------
    drain_failures: List[_DrainFailure] = []
    if unmigratable:
        print()
        warn(
            "One or more services could not be migrated. "
            "Skipping the master-Safe and master-EOA drains so funds remain "
            "available for completing those migrations later.",
        )
    else:
        drain_failures = _drain_master(
            qs_wallet=qs_wallet, pearl_wallet=pearl_wallet,
            chain_rpcs=chain_rpcs,
        )

    outcome = MigrationOutcome(
        migrated=tuple(migrated),
        unmigratable=tuple(unmigratable),
        drain_failures=tuple(drain_failures),
    )

    # ---- final summary -------------------------------------------------------
    _print_migration_summary(outcome)

    # ---- rename source -------------------------------------------------------
    # Never rename when the migration is incomplete: the user needs the
    # source `.operate/` discoverable to resume.
    if migrated and outcome.is_complete:
        print()
        if yes_no(
            "Rename the source quickstart `.operate/` to keep it as a rollback?",
            default=True,
        ):
            rename_source_for_rollback(disc.quickstart)
    elif not outcome.is_complete:
        info(
            "Source `.operate/` left in place (migration incomplete) — "
            f"re-run `migrate_to_pearl.sh` from {disc.quickstart.root.parent} "
            "after addressing the issues above.",
        )

    return outcome


def _print_migration_summary(outcome: MigrationOutcome) -> None:
    if outcome.is_complete:
        # Print a positive confirmation so the rename prompt that follows
        # has context (the user shouldn't be asked "rollback?" with no
        # preceding "from what?" headline).
        print_section("Migration summary")
        info(f"  fully migrated: {outcome.migrated_count} service(s)")
        return
    print_section("Migration incomplete — action required")
    info(f"  fully migrated: {outcome.migrated_count} service(s)")
    if outcome.unmigratable:
        warn(f"  un-migrated services: {len(outcome.unmigratable)}")
        for exc in outcome.unmigratable:
            warn(f"    - {exc}")
    if outcome.drain_failures:
        warn(f"  drain failures: {len(outcome.drain_failures)}")
        for f in outcome.drain_failures:
            warn(
                f"    - [{f.chain_name}] {f.source_kind} {f.source_address}: "
                f"{f.reason}",
            )
        warn(
            "  Funds may still be present on the quickstart wallet on these "
            "chains. Re-run after resolving the underlying issue (RPC, gas, "
            "etc.) to retry the drain.",
        )


def _step_terminate(
    *, manager: "ServiceManager", service: "Service", sid: str, chain_str: str,
    ensure_signable,                            # callable[[], None]
    on_chain_state_cls: OnChainState,
) -> None:
    """Move on-chain to PRE_REGISTRATION (idempotent).

    `_get_on_chain_state` is a private middleware method (no public
    equivalent yet); revisit if/when middleware exposes one.
    """
    on_chain_state = manager._get_on_chain_state(service, chain_str)  # noqa: SLF001
    if on_chain_state == on_chain_state_cls.PRE_REGISTRATION:
        info("    already in PRE_REGISTRATION; skipping terminate.")
        return
    ensure_signable()
    try:
        manager.terminate_service_on_chain_from_safe(
            service_config_id=sid, chain=chain_str,
        )
    except Exception as exc:  # pylint: disable=broad-except
        raise _wrap_step_failure(
            sid=sid, chain=chain_str,
            prefix="could not unstake/terminate; funds left in master "
                   "Safe/EOA so you can retry",
            exc=exc,
        )
    # Verification read after terminate succeeded. Wrap so a transient
    # RPC failure here becomes a per-service `_Unmigratable` ("terminated
    # but unverified") rather than aborting the whole batch with a raw
    # traceback past `_run_mode_b`'s `except _Unmigratable`.
    try:
        on_chain_state = manager._get_on_chain_state(service, chain_str)  # noqa: SLF001
    except Exception as exc:  # pylint: disable=broad-except
        raise _wrap_step_failure(
            sid=sid, chain=chain_str,
            prefix="terminate succeeded but post-state verification "
                   "failed (on-chain progressed; re-run will resume from "
                   "the next step)",
            exc=exc,
        )
    if on_chain_state != on_chain_state_cls.PRE_REGISTRATION:
        raise _Unmigratable(
            service_id=sid, chain=chain_str,
            reason=f"after terminate, service is in state {on_chain_state.name}, "
                   "expected PRE_REGISTRATION.",
        )


def _step_transfer_nft(
    *, ledger_api: Any, qs_wallet: Any, registry_addr: str,
    qs_master_safe: str, pearl_master_safe: str,
    token_id: int, sid: str, chain_str: str,
    ensure_signable,
) -> None:
    """Transfer the service NFT to Pearl's master Safe (idempotent)."""
    from scripts.pearl_migration.transfer import (
        PostConditionUnknown,
        transfer_service_nft,
    )

    current_owner = service_nft_owner(
        ledger_api=ledger_api,
        service_registry_address=registry_addr,
        service_id=token_id,
    )
    if current_owner is None:
        raise _Unmigratable(
            service_id=sid, chain=chain_str,
            reason=f"could not read NFT owner for token {token_id}; "
                   "RPC error or contract revert.",
        )
    if current_owner.lower() == pearl_master_safe.lower():
        info("    NFT already owned by Pearl master Safe; skipping transfer.")
        return
    if current_owner.lower() != qs_master_safe.lower():
        raise _Unmigratable(
            service_id=sid, chain=chain_str,
            reason=f"NFT owner is {current_owner}, expected quickstart "
                   f"({qs_master_safe}) or Pearl ({pearl_master_safe}) Safe.",
        )
    ensure_signable()
    info(f"  transferring service NFT {token_id} {qs_master_safe} -> {pearl_master_safe}")
    try:
        transfer_service_nft(
            ledger_api=ledger_api, crypto=qs_wallet.crypto,
            service_registry_address=registry_addr,
            qs_master_safe=qs_master_safe,
            pearl_master_safe=pearl_master_safe,
            service_id=token_id,
        )
    except PostConditionUnknown as exc:
        # Distinct from "inner reverted": tx submitted, but the post-tx
        # ownerOf read failed all retries. State is INDETERMINATE — DO NOT
        # re-run blindly (a re-run after the NFT actually moved would hit
        # `ERC721: caller not owner`). Surface unwrapped so the user sees
        # the original guidance. `from exc` (PEP 3134) marks this as an
        # intentional translation so the underlying RPC failure stays
        # visible in the traceback chain.
        raise _Unmigratable(
            service_id=sid, chain=chain_str,
            reason=str(exc),
        ) from exc
    except Exception as exc:  # pylint: disable=broad-except
        raise _wrap_step_failure(
            sid=sid, chain=chain_str,
            prefix="NFT transfer failed", exc=exc,
        )


def _step_swap_service_safe_owner(
    *, ledger_api: Any, qs_wallet: Any, service_safe: str,
    qs_master_safe: str, pearl_master_safe: str,
    sid: str, chain_str: str,
    ensure_signable,
) -> None:
    """Swap the service multisig's owner from qs Safe to Pearl Safe (idempotent)."""
    from scripts.pearl_migration.transfer import (
        PostConditionUnknown,
        swap_service_safe_owner,
    )

    try:
        owners = safe_owners(ledger_api=ledger_api, safe=service_safe)
    except Exception as exc:  # pylint: disable=broad-except
        raise _wrap_step_failure(
            sid=sid, chain=chain_str,
            prefix=f"could not read service Safe owners on {service_safe}",
            exc=exc,
        )
    owners_lower = {o.lower() for o in owners}
    pearl_present = pearl_master_safe.lower() in owners_lower
    qs_present = qs_master_safe.lower() in owners_lower
    if pearl_present and not qs_present:
        info(f"    service Safe {service_safe} already swapped; skipping.")
        return
    if not qs_present:
        raise _Unmigratable(
            service_id=sid, chain=chain_str,
            reason=f"service Safe {service_safe} owners are {owners}; "
                   f"quickstart Safe ({qs_master_safe}) not found, can't swap.",
        )
    ensure_signable()
    info(f"  swapping owner on service Safe {service_safe}")
    try:
        swap_service_safe_owner(
            ledger_api=ledger_api, crypto=qs_wallet.crypto,
            service_safe=service_safe,
            old_owner=qs_master_safe, new_owner=pearl_master_safe,
        )
    except PostConditionUnknown as exc:
        # State is INDETERMINATE — execTransaction submitted but the
        # post-tx getOwners read failed all retries. The NFT is already
        # owned by Pearl, so a re-run that hits a post-condition mismatch
        # (because the swap actually landed) is the WORST outcome:
        # rename_source_for_rollback would erase qs/.operate believing
        # success. Surface unwrapped so the user gets the explicit
        # "verify on a block explorer first" guidance. `from exc`
        # preserves the underlying RPC failure in the traceback chain.
        raise _Unmigratable(
            service_id=sid, chain=chain_str,
            reason=str(exc),
        ) from exc
    except Exception as exc:  # pylint: disable=broad-except
        raise _wrap_step_failure(
            sid=sid, chain=chain_str,
            prefix=(
                f"service Safe owner swap failed; NFT is now owned by Pearl "
                f"Safe but service multisig {service_safe} still lists "
                f"{qs_master_safe} as owner"
            ),
            exc=exc,
        )


def _migrate_one_service(
    svc: Service,
    qs_app: "OperateApp",
    qs_wallet: "MasterWallet",
    pearl_wallet: "MasterWallet",
    config_path: Optional[str],
) -> None:
    """Stop, terminate, and on-chain transfer one service.

    Per-chain orchestration: ensures Pearl has a master Safe on the chain,
    builds the lazy ledger handle and threshold guard, then dispatches to
    `_step_terminate` -> `_step_transfer_nft` -> `_step_swap_service_safe_owner`.
    Each step is independently idempotent so re-runs after a partial failure
    resume from the first incomplete step.

    Filesystem copy is done by the caller after this returns successfully.
    """
    from operate.ledger.profiles import CONTRACTS

    manager = qs_app.service_manager()
    sid = svc.service_config_id
    # Reload via the manager so we get a fully-validated Service tied to the
    # live store (vs the snapshot returned by `OperateStore.services()`).
    service = manager.load(service_config_id=sid)

    # ---- stop deployment -----------------------------------------------------
    if config_path:
        try:
            stop_via_middleware(operate=qs_app, config_path=config_path)
        except Exception as exc:  # pylint: disable=broad-except
            # Real programming bugs (e.g. middleware API drift) must NOT
            # be demoted to a `warn(...)` and silently fall through to
            # the on-chain branch.
            _reraise_if_programming_bug(exc)
            warn(f"stop_service via middleware failed: {exc}; trying force cleanup.")
    # Per-service variant of the global stop-and-verify: convert any
    # cleanup failure into `_Unmigratable(service_id=...)` so the
    # orchestrator can keep migrating other services. Proceeding to NFT
    # transfer with containers possibly still signing with the agent key
    # would race.
    def _bail(reason: str) -> NoReturn:
        raise _Unmigratable(service_id=sid, chain=None, reason=reason)

    _ensure_containers_stopped(on_failure=_bail)

    # ---- per-chain on-chain operations --------------------------------------
    for chain_str, chain_config in service.chain_configs.items():
        chain = Chain(chain_str)
        chain_data = chain_config.chain_data
        info(f"  chain {chain_str}: service NFT id = {chain_data.token}, multisig = {chain_data.multisig}")

        # Both `qs_wallet.safes[chain]` and `CONTRACTS[chain][...]` raise
        # KeyError on missing entries — legitimate per-service conditions
        # (chain added to a service after the wallet was created, or a
        # newer chain enum without a registered ServiceRegistry contract).
        # `_PROGRAMMING_BUGS` deliberately excludes KeyError/LookupError
        # so we MUST wrap here; otherwise a missing chain crashes the
        # whole batch instead of becoming a per-service `_Unmigratable`.
        try:
            qs_master_safe = qs_wallet.safes[chain]
        except KeyError:
            raise _Unmigratable(
                service_id=sid, chain=chain_str,
                reason=f"quickstart master wallet has no Safe on {chain_str}; "
                       "service references a chain the wallet was never "
                       "configured for.",
            )
        try:
            registry_addr = CONTRACTS[chain]["service_registry"]
        except KeyError:
            raise _Unmigratable(
                service_id=sid, chain=chain_str,
                reason=f"no ServiceRegistry contract registered for {chain_str} "
                       "in middleware's CONTRACTS table; cannot read NFT owner "
                       "or build the transfer.",
            )
        # Ensure Pearl has a master Safe on this chain. `create_safe` is
        # the same factory that bootstraps Pearl on a new chain — required
        # before the Safe owner swap can name a real `new_owner`.
        if chain not in pearl_wallet.safes:
            info(f"    Pearl has no master Safe on {chain_str}; creating one.")
            try:
                pearl_wallet.create_safe(
                    chain=chain, rpc=chain_config.ledger_config.rpc,
                )
            except Exception as exc:  # pylint: disable=broad-except
                _reraise_if_programming_bug(exc)
                raise _Unmigratable(
                    service_id=sid, chain=chain_str,
                    reason=f"could not create Pearl master Safe on {chain_str}: {exc}.",
                )
        pearl_master_safe = pearl_wallet.safes[chain]

        # `ledger_api` and the threshold guard are needed only inside the
        # signing branches — defer so terminate-raise paths exercise without
        # a fully populated wallet/ledger.
        #
        # `_cfg=chain_config` (default-arg binding) defends against a
        # future refactor that defers iteration (e.g. collects callables
        # to run later): without this binding, every deferred closure would
        # read the LAST iteration's `chain_config`, signing chain N's tx
        # against chain N+1's RPC. `nonlocal ledger_api` provides the
        # within-iteration memoization (one ledger_api built per chain).
        ledger_api: Optional[Any] = None

        def _ledger_api(_cfg=chain_config) -> Any:
            nonlocal ledger_api
            if ledger_api is None:
                ledger_api = manager.get_eth_safe_tx_builder(
                    ledger_config=_cfg.ledger_config,
                ).ledger_api
            return ledger_api

        def _ensure_signable() -> None:
            """Raise `_Unmigratable` unless the qs master Safe is single-signer.

            Called lazily, only inside branches that actually sign. Resumed
            runs that find every step already done skip this entirely.
            """
            try:
                t = safe_threshold(ledger_api=_ledger_api(), safe=qs_master_safe)
            except Exception as exc:  # pylint: disable=broad-except
                _reraise_if_programming_bug(exc)
                raise _Unmigratable(
                    service_id=sid, chain=chain_str,
                    reason=f"could not read quickstart master Safe threshold on "
                           f"{qs_master_safe}: {exc}.",
                )
            if t == 1:
                return
            if t == 0:
                raise _Unmigratable(
                    service_id=sid, chain=chain_str,
                    reason=f"quickstart master Safe {qs_master_safe} reports "
                           f"threshold 0 — appears unconfigured. Inspect on-chain "
                           "state before retrying.",
                )
            raise _Unmigratable(
                service_id=sid, chain=chain_str,
                reason=f"quickstart master Safe {qs_master_safe} has threshold "
                       f"{t}; this script signs single-owner only. Lower the "
                       "threshold to 1 (temporarily) and re-run.",
            )

        _step_terminate(
            manager=manager, service=service, sid=sid, chain_str=chain_str,
            ensure_signable=_ensure_signable, on_chain_state_cls=OnChainState,
        )
        _step_transfer_nft(
            ledger_api=_ledger_api(), qs_wallet=qs_wallet,
            registry_addr=registry_addr,
            qs_master_safe=qs_master_safe, pearl_master_safe=pearl_master_safe,
            token_id=chain_data.token, sid=sid, chain_str=chain_str,
            ensure_signable=_ensure_signable,
        )
        _step_swap_service_safe_owner(
            ledger_api=_ledger_api(), qs_wallet=qs_wallet,
            service_safe=chain_data.multisig,
            qs_master_safe=qs_master_safe, pearl_master_safe=pearl_master_safe,
            sid=sid, chain_str=chain_str,
            ensure_signable=_ensure_signable,
        )


def _format_amount(amount: int, chain: "Chain", asset: str) -> str:
    """Best-effort token formatter; falls back to raw wei when the asset
    isn't in the middleware's `CHAIN_TO_METADATA` table.

    Only catches the lookup failures (`KeyError` for unknown chain/asset);
    everything else propagates so a bug in the formatter doesn't masquerade
    as "raw wei output".
    """
    try:
        return wei_to_token(amount, chain.value, asset)
    except KeyError:
        return f"{amount} of {asset}"


def _drain_master(
    qs_wallet: "MasterWallet", pearl_wallet: "MasterWallet",
    chain_rpcs: Optional[Dict["Chain", str]] = None,
) -> List[_DrainFailure]:
    """Drain quickstart master Safe + EOA into Pearl per chain.

    Returns a list of `_DrainFailure` entries — empty on full success.
    Per-chain failures are accumulated rather than aborting; one chain's
    RPC outage shouldn't prevent draining the others.

    `chain_rpcs` (chain -> rpc URL) is used to spin up a Pearl master
    Safe on chains where Pearl doesn't yet have one — so we can actually
    drain into them instead of stranding the funds.
    """
    print_section("Draining quickstart master Safe + EOA into Pearl's.")
    failures: List[_DrainFailure] = []
    chain_rpcs = chain_rpcs or {}

    for chain in qs_wallet.safes:
        if chain not in pearl_wallet.safes:
            # Pre-check: we need an RPC URL to create a Safe. If no
            # migrated service used this chain, `chain_rpcs` won't have
            # an entry, and passing `rpc=None` to `create_safe` would
            # silently rely on whatever fallback the middleware does
            # (often an unreliable public endpoint). Surface as a
            # `_DrainFailure` immediately so the user knows funds on
            # this chain weren't moved and why.
            rpc = chain_rpcs.get(chain)
            if rpc is None:
                warn(
                    f"  no RPC available for {chain.name}; skipping drain "
                    "(quickstart had a Safe here but no migrated service "
                    "exposed an RPC for this chain)."
                )
                failures.append(_DrainFailure(
                    chain=chain,
                    source_kind="Safe+EOA",
                    source_address=qs_wallet.safes[chain],
                    reason="no RPC available — quickstart had a Safe on "
                           f"{chain.name} but no migrated service used "
                           "this chain; cannot create Pearl Safe to "
                           "drain into.",
                ))
                continue
            info(f"  Pearl has no master Safe on {chain.name}; creating one.")
            try:
                pearl_wallet.create_safe(chain=chain, rpc=rpc)
            except Exception as exc:  # pylint: disable=broad-except
                _reraise_if_programming_bug(exc)
                warn(f"  could not create Pearl Safe on {chain.name}: {exc}")
                failures.append(_DrainFailure(
                    chain=chain,
                    source_kind="Safe+EOA",
                    source_address=qs_wallet.safes[chain],
                    reason=f"Pearl Safe creation failed: {exc}.",
                ))
                continue

        # Master Safe -> Pearl master Safe (Pearl Safe receives ERC20s + native).
        info(f"  [{chain.name}] master Safe -> Pearl master Safe ({pearl_wallet.safes[chain]})")
        try:
            moved = qs_wallet.drain(
                withdrawal_address=pearl_wallet.safes[chain],
                chain=chain,
                from_safe=True,
            )
            if moved:
                for asset, amount in moved.items():
                    info(f"    moved {_format_amount(amount, chain, asset)}")
            else:
                # Distinguish "nothing to drain" from a silent middleware
                # drop — explicit log line so the user can correlate with
                # on-chain balance state if anything looks off later.
                info(f"    no balances to move from Safe on {chain.name}")
        except Exception as exc:  # pylint: disable=broad-except
            _reraise_if_programming_bug(exc)
            warn(f"  drain (Safe) on {chain.name} failed: {exc}")
            failures.append(_DrainFailure(
                chain=chain, source_kind="Safe",
                source_address=qs_wallet.safes[chain], reason=str(exc),
            ))

        # Master EOA -> Pearl master EOA.
        info(f"  [{chain.name}] master EOA -> Pearl master EOA ({pearl_wallet.address})")
        try:
            moved = qs_wallet.drain(
                withdrawal_address=pearl_wallet.address,
                chain=chain,
                from_safe=False,
            )
            if moved:
                for asset, amount in moved.items():
                    info(f"    moved {_format_amount(amount, chain, asset)}")
            else:
                info(f"    no balances to move from EOA on {chain.name}")
        except Exception as exc:  # pylint: disable=broad-except
            _reraise_if_programming_bug(exc)
            warn(f"  drain (EOA) on {chain.name} failed: {exc}")
            failures.append(_DrainFailure(
                chain=chain, source_kind="EOA",
                source_address=qs_wallet.address, reason=str(exc),
            ))

    return failures


# ---------------------------------------------------------------------------
# Final user message
# ---------------------------------------------------------------------------

def _final_prompt(outcome: MigrationOutcome) -> None:
    if not outcome.is_complete:
        # Don't ask the "different machine?" question on a partial migration
        # — the source `.operate/` is still authoritative; copying Pearl's
        # half-state to another machine would just propagate the partial
        # state. Print clear remediation guidance and return.
        print_section("Migration incomplete — see warnings above.")
        print()
        print("  Some services or drains were left incomplete. The source")
        print("  quickstart `.operate/` is preserved. Resolve the issues")
        print("  flagged in the summary above and re-run `migrate_to_pearl.sh`.")
        return
    print_section("Migration complete.")
    if yes_no(
        "Do you want to run Pearl on a different machine than this one?",
        default=False,
    ):
        print()
        print("  Copy `~/.operate/` from this machine to `~/.operate` on the other")
        print("  machine (same path). Pearl's bundled middleware will auto-migrate")
        print("  any schema differences on first launch. Then start Pearl on that")
        print("  machine and enter your master password.")
    else:
        print()
        print("  Start Pearl now and enter your master password. Your migrated")
        print("  services will appear in the dashboard.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    print_title("Quickstart -> Pearl migration")

    try:
        disc = discover(
            quickstart_root=args.quickstart_home,
            pearl_root=args.pearl_home,
        )
    except ValueError as exc:
        # The discover() pipeline can raise ValueError from any of:
        #   * `OperateStore.__post_init__` — unresolvable / inaccessible root,
        #   * `Discovery.__post_init__` — same-root vs NOOP-mode mismatch,
        #   * `discover()` itself — for any future invariants added inline.
        # Surface as a clean fatal() rather than a raw stacktrace.
        fatal(f"Discovery failed: {exc}")
    _preflight(disc)

    # Both branches return a `MigrationOutcome`. No `Optional` / `assert`
    # — the type system enforces what was previously a runtime defence
    # (which would have been stripped under `python -O`).
    if disc.mode == Mode.FRESH_COPY:
        outcome = _run_mode_a(disc=disc, dry_run=args.dry_run)
    else:
        services = _select_services(disc.quickstart, args.config_path)
        outcome = _run_mode_b(
            disc=disc,
            services=services,
            config_path=args.config_path,
            dry_run=args.dry_run,
        )

    if not args.dry_run:
        _final_prompt(outcome)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
