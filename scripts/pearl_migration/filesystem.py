"""File-level migration: copy `services/sc-{uuid}/` and the agent keys it references.

No path rewriting: the middleware re-resolves `provision_type=computed`
env vars (including `STORE_PATH`) at every deployment. The override lives
inside `Service.deploy(...)` in the middleware — grep for `"STORE_PATH": "/data"`
in `olas-operate-middleware/operate/services/service.py` if you need to
verify. Either way, a plain `cp -r` is sufficient once collisions are handled.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List

from operate.services.service import Service

from .detect import OperateStore
from .prompts import CollisionChoice, backup_suffix, collision, info, warn


class MissingAgentKey(OSError):
    """Raised by `merge_service` when an agent key referenced by the
    service config is not present at the source `keys/` dir.

    Subclasses `OSError` so the existing `(OSError, shutil.Error)` catch
    in `_run_mode_b` aggregates it into `MigrationOutcome.unmigratable`
    with the standard "on-chain committed but filesystem copy failed"
    message — the user is then told exactly which key is missing and
    that the on-chain side is already done.

    Previously this case was a `warn(...)` + skip, which left the user
    with an unstartable service (Pearl can't sign for the missing
    agent) and no entry in `MigrationOutcome` to surface the problem.
    """


@dataclass
class CopyOutcome:
    service_id: str
    service_copied: bool
    service_skipped: bool
    keys_copied: List[str]
    keys_skipped: List[str]
    backups_made: List[Path]


def fix_root_ownership(store: OperateStore) -> None:
    """`sudo chown -RP` every service directory whose resolved path stays inside the store root, to the current uid:gid.

    Docker leaves root-owned files in `persistent_data/` AND under
    `deployment/nodes/node0/{config,data}/` (tendermint validator keys
    and state). The original `run_service.sh` cleanup block only chowns
    `persistent_data` because it then deletes and recreates `deployment`,
    but migration copies the whole service tree — so any root-owned file
    anywhere under it will fail the subsequent `shutil.copytree`.

    Detection-via-`Path.rglob`+`stat` was tried first but is unreliable
    on Python 3.14 against trees containing dirs the current user can't
    traverse (rglob silently skips, leaving root-owned files inside
    undetected). `chown` with current uid:gid is idempotent — a no-op
    when nothing's actually root-owned — so paying the sudo invocation
    is cheaper than discovering the gap mid-copy. Raises on any failure:
    callers (`_run_mode_a` / `_run_mode_b`) immediately follow with the
    copy, and silent skip would corrupt the destination mid-copy.
    """
    if not store.services_dir.exists():
        return
    store_root = store.root.resolve()
    uid = os.getuid()
    gid = os.getgid()
    successful: List[Path] = []
    for service_dir in store.services_dir.iterdir():
        # Defence in depth: --quickstart-home with a symlink/typo could
        # point us outside the actual store. Validate the resolution to
        # avoid `chown -R`-ing whatever a stray symlink points at.
        try:
            resolved = service_dir.resolve()
            resolved.relative_to(store_root)
        except (OSError, ValueError) as exc:
            raise RuntimeError(
                f"refusing to chown {service_dir}: not inside store root "
                f"{store_root} ({exc})"
            )
        warn(f"Root-owned files found under {service_dir}; running 'sudo -n chown -RP {uid}:{gid}'.")
        try:
            subprocess.run(
                # `-n` (sudo): non-interactive — fail fast on machines that
                # require a sudo password instead of hanging for 120s on a
                # blocked tty prompt.
                # `-RP` (chown): recursive but DO NOT traverse symbolic links
                # — a stray symlink under `persistent_data/` (e.g. pointing
                # at `/etc`) would otherwise have its target chowned to the
                # current user.
                ["sudo", "-n", "chown", "-RP", f"{uid}:{gid}", str(service_dir)],
                check=True,
                timeout=120,
            )
            successful.append(service_dir)
        except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            # Don't continue: the next step copies this tree as the current
            # user, which will error out partway and leave a half-populated
            # destination. Better to abort cleanly here. Include the list
            # of dirs already mutated so the user has an audit trail —
            # `chown -R` to the current uid is irreversible without backups.
            already = ", ".join(str(t) for t in successful) or "(none)"
            raise RuntimeError(
                f"could not chown {service_dir}: {exc}. "
                f"Already mutated: {already}. "
                f"Currently in indeterminate state (chown may have walked "
                f"partway before failing): {service_dir}. "
                f"Run 'sudo chown -RP {uid}:{gid} {service_dir}' manually "
                "(plus enable passwordless sudo if required) and re-run."
            )


def fresh_copy_store(src: OperateStore, dest_root: Path) -> None:
    """Mode A: copy the whole `.operate` to `dest_root`.

    Refuses if `dest_root` already exists (the orchestrator handles the
    "exists but empty Pearl init" case explicitly before calling here).
    """
    if dest_root.exists():
        raise FileExistsError(
            f"Destination already exists: {dest_root} — refuse to overwrite."
        )
    dest_root.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src.root, dest_root, symlinks=True)
    info(f"Copied {src.root} -> {dest_root}")


def _backup_then_remove(target: Path) -> Path:
    """Rename `target` to `target.bak.<ts>` and return the new path."""
    bak = target.with_name(f"{target.name}.{backup_suffix()}")
    target.rename(bak)
    return bak


def merge_service(
    service: Service,
    src: OperateStore,
    dest: OperateStore,
) -> CopyOutcome:
    """Copy one service directory + its referenced keys into `dest`.

    On collision the user is prompted (skip vs overwrite-with-backup). Backups
    are timestamped siblings — never deleted by this script.

    Assumes both stores share the same master password — the caller must
    have already aligned them via `align_quickstart_password` so the
    agent keys can be copied verbatim and decrypt under the destination
    password at deploy time.
    """
    backups: List[Path] = []
    sid = service.service_config_id

    # ---- service directory --------------------------------------------------
    dest.services_dir.mkdir(parents=True, exist_ok=True)
    dest_service_dir = dest.services_dir / sid

    service_copied = False
    service_skipped = False
    if dest_service_dir.exists():
        choice = collision(dest_service_dir, kind="service")
        if choice == CollisionChoice.SKIP:
            service_skipped = True
        else:
            backups.append(_backup_then_remove(dest_service_dir))
            shutil.copytree(service.path, dest_service_dir, symlinks=True)
            service_copied = True
    else:
        shutil.copytree(service.path, dest_service_dir, symlinks=True)
        service_copied = True

    # ---- agent keys ---------------------------------------------------------
    dest.keys_dir.mkdir(parents=True, exist_ok=True)
    keys_copied: List[str] = []
    keys_skipped: List[str] = []
    for addr in service.agent_addresses:
        src_key = src.keys_dir / addr
        if not src_key.exists():
            # Pearl cannot sign for an agent whose key is absent; a
            # "skip + warn" here would leave the user with an unstartable
            # service and no entry in the migration summary. Raise so the
            # caller aggregates it into `unmigratable` with the
            # on-chain-already-committed remediation message.
            raise MissingAgentKey(
                f"agent key {addr} referenced by {sid} not found at "
                f"{src_key} — without it Pearl cannot sign for this "
                "agent and the service will not start."
            )
        dest_key = dest.keys_dir / addr
        if dest_key.exists():
            choice = collision(dest_key, kind="key")
            if choice == CollisionChoice.SKIP:
                keys_skipped.append(addr)
                continue
            backups.append(_backup_then_remove(dest_key))
        shutil.copy2(src_key, dest_key)
        keys_copied.append(addr)

    return CopyOutcome(
        service_id=sid,
        service_copied=service_copied,
        service_skipped=service_skipped,
        keys_copied=keys_copied,
        keys_skipped=keys_skipped,
        backups_made=backups,
    )


def rename_source_for_rollback(src: OperateStore) -> Path:
    """Rename the source `.operate` so a re-run won't pick it up.

    Returns the new path. Never deletes.
    """
    new_path = src.root.with_name(f"{src.root.name}.migrated.{backup_suffix()}")
    src.root.rename(new_path)
    info(f"Renamed source -> {new_path}")
    return new_path
