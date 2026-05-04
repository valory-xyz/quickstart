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
from .status import any_root_owned_under


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
    """chown -R the user if any persistent_data is root-owned.

    Mirrors the cleanup block at the top of `run_service.sh` (look for the
    `.operate/services/sc-*` chown loop). Raises on any failure: the
    callers (`_run_mode_a` / `_run_mode_b`) immediately follow with
    `shutil.copytree` / `merge_service` over the same tree, so silently
    leaving root-owned files would corrupt the destination mid-copy.
    """
    if not store.services_dir.exists():
        return
    store_root = store.root.resolve()
    needs_chown = []
    for service_dir in store.services_dir.iterdir():
        # Defence in depth: --quickstart-home with a symlink/typo could
        # point us outside the actual store. Validate BOTH the service
        # dir AND the persistent_data resolution; either could escape via
        # symlinks (`services/sc-foo -> ../somewhere`) and we'd otherwise
        # `chown -R` whatever they pointed at.
        for candidate in (service_dir, service_dir / "persistent_data"):
            try:
                resolved = candidate.resolve()
                resolved.relative_to(store_root)
            except (OSError, ValueError) as exc:
                raise RuntimeError(
                    f"refusing to chown {candidate}: not inside store root "
                    f"{store_root} ({exc})"
                )
        pdata = service_dir / "persistent_data"
        if any_root_owned_under(pdata):
            needs_chown.append(pdata)
    if not needs_chown:
        return

    uid = os.getuid()
    gid = os.getgid()
    for pdata in needs_chown:
        warn(f"Root-owned files found under {pdata}; running 'sudo chown -R {uid}:{gid}'.")
        try:
            subprocess.run(
                ["sudo", "chown", "-R", f"{uid}:{gid}", str(pdata)],
                check=True,
                timeout=120,
            )
        except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            # Don't continue: the next step copies this tree as the current
            # user, which will error out partway and leave a half-populated
            # destination. Better to abort cleanly here.
            raise RuntimeError(
                f"could not chown {pdata}: {exc}. Run 'sudo chown -R {uid}:{gid} "
                f"{pdata}' manually and re-run the migration."
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
