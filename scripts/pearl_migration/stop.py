"""Stop the running quickstart deployment cleanly."""

from __future__ import annotations

import subprocess
from typing import List, TYPE_CHECKING

from .status import QUICKSTART_CONTAINER_FRAGMENTS, docker_quickstart_containers

if TYPE_CHECKING:
    from operate.cli import OperateApp


def stop_via_middleware(operate: "OperateApp", config_path: str) -> None:
    """Defer to the middleware's quickstart `stop_service` flow.

    Composes down the docker stack the way `./stop_service.sh` does,
    so middleware bookkeeping (DeploymentStatus etc.) is updated.

    Middleware's `stop_service` calls `ask_password_if_needed`, which
    interactively re-prompts for a password we've already validated.
    Set `OPERATE_PASSWORD` plus `ATTENDED=false` for the duration of
    the call so middleware reads the password from the env (only when
    BOTH are set; otherwise `ask_or_get_from_env` raises) and skips
    the redundant prompt. Restore the env afterwards so unattended
    mode doesn't leak into post-stop steps. Callers must ensure
    `user.json`'s hash already matches `operate.password` — see
    `align_user_account_to_wallet` for the full divergence background;
    a diverged hash would loop forever in unattended mode.
    """
    import os

    from operate.quickstart.stop_service import stop_service

    saved_env = {k: os.environ.get(k) for k in ("OPERATE_PASSWORD", "ATTENDED")}
    if operate.password is not None:
        os.environ["OPERATE_PASSWORD"] = operate.password
        os.environ["ATTENDED"] = "false"
    try:
        stop_service(operate=operate, config_path=config_path)
    finally:
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def force_remove_known_containers() -> List[str]:
    """Best-effort cleanup of any quickstart containers still around.

    Mirrors the `docker rm -f` block at the top of `run_service.sh`
    (matching the substring fragments in `QUICKSTART_CONTAINER_FRAGMENTS`)
    so a half-stopped deployment can't conflict with Pearl picking the
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
        subprocess.run(  # nosec B607  # `docker` looked up via PATH is the desired UX
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
    "QUICKSTART_CONTAINER_FRAGMENTS",
    "force_remove_known_containers",
    "stop_via_middleware",
]
