"""Stop the running quickstart deployment cleanly."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING, List

from .status import QUICKSTART_CONTAINER_NAMES, docker_quickstart_containers

if TYPE_CHECKING:
    from operate.cli import OperateApp


def stop_via_middleware(operate: "OperateApp", config_path: str) -> None:
    """Defer to the middleware's quickstart `stop_service` flow.

    This composes down the docker stack the way `./stop_service.sh` does,
    so middleware bookkeeping (DeploymentStatus etc.) is updated correctly.
    """
    from operate.quickstart.stop_service import stop_service

    stop_service(operate=operate, config_path=config_path)


def force_remove_known_containers() -> List[str]:
    """Best-effort cleanup of any quickstart containers still around.

    Mirrors the `docker rm -f` block at the top of `run_service.sh`
    (matching the suffix patterns in `QUICKSTART_CONTAINER_NAMES`) so a
    half-stopped deployment can't conflict with Pearl picking the
    migrated services up.

    Returns the list of container names that were forcibly removed.

    Raises `subprocess.TimeoutExpired` on `docker rm -f` hang and
    `RuntimeError` on a non-zero `docker rm -f` exit (daemon refused the
    rm, permission denied on `/var/run/docker.sock`, container in a
    transitional state, etc.) — silently returning `[]` would be
    indistinguishable from "nothing to remove", letting on-chain steps
    proceed against a still-running deployment that may sign txs with
    the agent key. Only `FileNotFoundError` (docker not installed) is
    swallowed, in which case the upstream `docker_quickstart_containers()`
    would already have returned `[]`.
    """
    leftovers = docker_quickstart_containers()
    if not leftovers:
        return []
    try:
        subprocess.run(
            ["docker", "rm", "-f", *leftovers],
            check=True,
            capture_output=True,
            timeout=30,
        )
    except FileNotFoundError:
        return []
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or b"").decode("utf-8", errors="replace").strip()
        raise RuntimeError(
            f"`docker rm -f` exited {exc.returncode}: {stderr or '(no stderr)'}"
        ) from exc
    return leftovers


__all__ = [
    "QUICKSTART_CONTAINER_NAMES",
    "force_remove_known_containers",
    "stop_via_middleware",
]
