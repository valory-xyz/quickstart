"""Source-of-truth queries.

Every step in the migration calls into here to ask the network / OS / disk
"is X already done?". We keep these reads side-effect-free so the orchestrator
can use them as both pre-flight checks and idempotency probes after a crash.
"""

from __future__ import annotations

import socket
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from aea.crypto.base import LedgerApi


PEARL_DAEMON_PORT = 8765
# Substring fragments used to detect quickstart-managed containers. The
# middleware names ABCI / Tendermint containers as `<service>_abci_<idx>`
# and `<service>_tm_<idx>` (see `autonomy/deploy/base.py:get_abci_container_name`),
# so the fragments `_abci_0` and `_tm_0` catch the per-service variants
# (`trader_abci_0`, `meme_factory_tm_0`, ...). `abci0` / `node0` are the
# old monolithic names from pre-`<service>_` deployments — kept so legacy
# installs are still detected.
QUICKSTART_CONTAINER_FRAGMENTS = ("abci0", "node0", "_abci_0", "_tm_0")


def pearl_daemon_running(host: str = "127.0.0.1", port: int = PEARL_DAEMON_PORT) -> bool:
    """TCP probe: True if anything is listening on Pearl's daemon port.

    Only treats "connection refused" / "timeout" as "not running". Any
    other socket error (file-descriptor exhaustion, network unreachable,
    permission denied) propagates so the caller doesn't silently proceed
    against a Pearl that's actually up but momentarily unreachable.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        try:
            sock.connect((host, port))
            return True
        except (ConnectionRefusedError, socket.timeout):
            return False


def docker_quickstart_containers() -> List[str]:
    """Return any quickstart-managed containers currently present.

    Distinguishes "docker isn't installed" (return []) from "docker daemon
    is hung" (raise). Silently swallowing the latter would let the caller
    proceed believing there are no containers when there actually are.
    """
    try:
        result = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.Names}}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except FileNotFoundError:
        return []   # docker not installed at all — caller can proceed.
    # subprocess.TimeoutExpired and any other error propagates.
    if result.returncode != 0:
        # Non-zero with docker installed = daemon refusing to talk
        # (permission denied on /var/run/docker.sock, daemon crash,
        # etc.). Returning [] would let callers conclude "no containers"
        # and race a still-running deployment. Raise so the orchestrator
        # surfaces a clear error instead.
        stderr = (result.stderr or "").strip() or "(no stderr)"
        raise RuntimeError(
            f"`docker ps` exited {result.returncode}: {stderr}. "
            "Cannot tell whether quickstart containers are still running."
        )
    # Substring match — `_abci_0` / `_tm_0` are fragments, not exact names
    # (see comment on `QUICKSTART_CONTAINER_FRAGMENTS`). Exact-set matching
    # would silently skip per-service containers like `trader_abci_0` and
    # let the migration race a still-running deployment.
    names = result.stdout.split()
    return sorted(
        name for name in names
        if any(frag in name for frag in QUICKSTART_CONTAINER_FRAGMENTS)
    )


def is_root_owned(path: Path) -> bool:
    """True if `path` exists and is owned by uid 0.

    `OSError` from `path.stat()` (permission denied, broken symlink) is
    propagated rather than treated as "not root-owned" — silently
    returning `False` would let `fix_root_ownership` skip the chown
    and the subsequent `shutil.copytree` would corrupt the destination.
    """
    if not path.exists():
        return False
    return path.stat().st_uid == 0


def any_root_owned_under(path: Path) -> bool:
    """Recursively check if any file/dir under `path` is root-owned.

    Permission errors (`OSError`) are NOT swallowed: a permission
    denied while walking the tree means we can't tell whether a
    root-owned file is hiding beneath, so we MUST propagate so the
    caller (`fix_root_ownership`) refuses to proceed instead of
    silently skipping the chown and corrupting the destination copy.
    """
    if not path.exists():
        return False
    if is_root_owned(path):
        return True
    for child in path.rglob("*"):
        if child.stat().st_uid == 0:
            return True
    return False


def service_nft_owner(
    ledger_api: "LedgerApi",
    service_registry_address: str,
    service_id: int,
) -> Optional[str]:
    """ServiceRegistry.ownerOf(service_id). Returns checksum address or None on revert.

    Only catches `web3.exceptions.ContractLogicError` (the contract revert
    we expect for non-existent / burnt tokens). Network/RPC errors and any
    other unexpected exception propagate so the caller can distinguish
    "token doesn't exist" from "we can't tell right now".
    """
    from autonomy.chain.base import registry_contracts
    from web3.exceptions import ContractLogicError

    instance = registry_contracts.service_registry.get_instance(
        ledger_api=ledger_api,
        contract_address=service_registry_address,
    )
    try:
        return instance.functions.ownerOf(service_id).call()
    except ContractLogicError:
        return None


def safe_owners(ledger_api: "LedgerApi", safe: str) -> List[str]:
    """Return the current owner list of a Gnosis Safe."""
    from operate.utils.gnosis import get_owners

    return list(get_owners(ledger_api=ledger_api, safe=safe))


def safe_threshold(ledger_api: "LedgerApi", safe: str) -> int:
    """Return the current signature threshold of a Gnosis Safe."""
    from autonomy.chain.base import registry_contracts

    instance = registry_contracts.gnosis_safe.get_instance(
        ledger_api=ledger_api,
        contract_address=safe,
    )
    return int(instance.functions.getThreshold().call())
