"""On-chain transfers signed by the quickstart master Safe.

Two operations are needed in Mode B per service per chain:

1. **NFT transfer** — call `transferFrom(from, to, tokenId)` on the chain's
   ServiceRegistry to move the on-chain service NFT from the quickstart master
   Safe to the Pearl master Safe. This is the only piece without an existing
   middleware helper; we encode the ABI call here and dispatch through
   `operate.utils.gnosis.send_safe_txs`.

   We deliberately use `transferFrom`, not `safeTransferFrom`. The "safe"
   variant calls `onERC721Received` on the recipient — which a Gnosis Safe
   only implements if it has the `CompatibilityFallbackHandler` (or
   equivalent ERC-721 receiver) installed. Pearl Safes created by the
   middleware do install it, but a freshly-created Safe without it would
   revert with an opaque `execution reverted`. Both endpoints here are
   Safes we control end-to-end, so the receiver hook adds no real safety.

2. **Service-Safe owner swap** — replace the quickstart master Safe with the
   Pearl master Safe in the service multisig's owner list. Cannot use
   `operate.utils.gnosis.swap_owner` directly because that helper signs
   directly as an EOA owner of the service Safe, but after `terminate`
   the service Safe's owner is the quickstart master Safe (a contract).
   We use the Safe-A-controlling-Safe-B `approveHash + execTransaction`
   pattern instead — see `swap_service_safe_owner`.

Post-condition reads: BOTH operations are followed by an on-chain state
read to confirm success. Gnosis Safe's `execTransaction` does NOT revert
the outer Ethereum transaction when the inner call fails — it emits an
`ExecutionFailure` event and returns `false`, so a `tx.status == 1`
receipt is NOT proof the swap/transfer actually happened. Without these
post-condition checks the migration would log success while leaving the
service uncontrollable from Pearl, and `rename_source_for_rollback`
would then erase the quickstart side — unrecoverable for the user.

Outcomes per operation, in order of severity:

  1. `send_safe_txs` raises before mining (outer-tx revert, gas estimate
     refusal, signing error) — bubbles up unwrapped as whatever the
     middleware raised. Nothing on-chain happened.
  2. Outer tx mines AND post-condition read confirms inner success —
     function returns the tx hash.
  3. Outer tx mines but the post-condition shows the inner call reverted
     (Gnosis Safe's `ExecutionFailure` event; receipt status is still 1
     because Safe doesn't bubble the inner failure) — `RuntimeError`
     with the explicit "inner call likely reverted" message. The qs side
     still owns the asset; safe to investigate.
  4. Outer tx mines but the post-condition read itself fails after
     retries — `PostConditionUnknown`. State is INDETERMINATE; the user
     must verify on a block explorer before any retry.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Callable, TypeVar

import requests.exceptions
import web3.exceptions

if TYPE_CHECKING:
    from aea.crypto.base import Crypto, LedgerApi


T = TypeVar("T")


# Exception classes that represent transient RPC / network failures. Computed
# once at module load: these libraries are hard transitive deps (web3 is used
# directly two functions below; requests is web3's HTTPProvider transport),
# so an ImportError here is a real misconfiguration that should fail loud at
# import time, not silently fall back to a narrower retry net.
#
# Programming bugs (TypeError, AttributeError, NameError) and post-condition
# shape mismatches must propagate immediately so the user gets a real
# traceback instead of `PostConditionUnknown`'s "verify on a block explorer"
# framing — that framing only makes sense when the failure is actually
# network-shaped.
#
# `asyncio.TimeoutError` is included for Python 3.10 (where it's a distinct
# subclass of Exception); on 3.11+ it's an alias for the builtin and the
# duplicate entry is idempotent.
_RPC_EXCEPTION_TYPES: tuple = (
    ConnectionError,
    TimeoutError,
    asyncio.TimeoutError,
    OSError,
    web3.exceptions.Web3Exception,
    requests.exceptions.RequestException,
)


class PostConditionUnknown(RuntimeError):
    """Post-condition read could not be completed despite retries.

    On-chain state is INDETERMINATE. `send_safe_txs` returned a tx hash
    (the outer Ethereum tx mined with status == 1), but for Safe
    `execTransaction` that ONLY means the outer call ran — the inner
    call may have emitted `ExecutionFailure` and returned `false`
    without bubbling up. The verification read that would have told us
    which (`ownerOf` for the NFT, `getOwners` for the swap) failed
    repeatedly, so we genuinely don't know whether the inner call
    landed.

    Distinct from a clean RuntimeError ("post-condition mismatch =
    inner call reverted") so the caller can stop the migration without
    re-running blindly — a re-run when the on-chain side already
    succeeded would fail with `ERC721: caller not owner` (transfer) or
    GS026/stale-prev-owner (swap), and the user would chase the wrong
    root cause.
    """

    def __init__(self, tx_hash: str, last_exc: BaseException) -> None:
        super().__init__(
            f"On-chain state INDETERMINATE after Safe tx {tx_hash}: the "
            f"outer tx mined, but the post-tx verification read failed "
            f"after retries ({last_exc!r}). DO NOT re-run blindly. "
            f"To diagnose: (1) inspect Safe's `ExecutionSuccess` / "
            f"`ExecutionFailure` event at tx {tx_hash} on a block "
            "explorer, (2) read the post-condition directly — "
            "`ServiceRegistry.ownerOf(tokenId)` for an NFT transfer or "
            "`Safe.getOwners()` for a service-Safe swap. Re-run the "
            "migration only after confirming the inner call did NOT land."
        )
        self.tx_hash = tx_hash
        self.last_exc = last_exc


def _read_with_retry(
    fn: "Callable[[], T]",
    *,
    tx_hash: str,
    attempts: int = 3,
    backoff: float = 0.5,
) -> T:
    """Run `fn()` up to `attempts` times.

    Between attempts, sleeps `backoff * 2**n` where `n` is the
    failed-attempt index starting at 0; no sleep after the final
    attempt. Default `attempts=3, backoff=0.5` -> sleeps 0.5s, 1.0s.

    Catches only RPC/network-shaped exceptions (see
    `_RPC_EXCEPTION_TYPES`). Programming bugs (`TypeError`,
    `AttributeError`, `NameError`, `ValueError` from a wrong arg
    shape) propagate immediately so the user sees a real traceback
    instead of `PostConditionUnknown`'s "verify on block explorer"
    framing — that framing only makes sense when the failure is
    actually network-shaped.

    On exhaustion, re-raises wrapped in `PostConditionUnknown` so the
    caller can distinguish "inner call reverted" from "we don't know".
    """
    if attempts < 1:
        raise ValueError(f"attempts must be >= 1, got {attempts}")
    last_exc: BaseException
    for n in range(attempts):
        try:
            return fn()
        except _RPC_EXCEPTION_TYPES as exc:
            last_exc = exc
            if n + 1 < attempts:
                time.sleep(backoff * (2 ** n))
    raise PostConditionUnknown(tx_hash=tx_hash, last_exc=last_exc)


def transfer_service_nft(
    ledger_api: "LedgerApi",
    crypto: "Crypto",
    service_registry_address: str,
    qs_master_safe: str,
    pearl_master_safe: str,
    service_id: int,
) -> str:
    """Transfer the ERC721 service NFT from `qs_master_safe` to `pearl_master_safe`.

    The tx is composed as a Safe transaction originated by `qs_master_safe`,
    targeting the ServiceRegistry contract. `crypto` must belong to one of the
    Safe's owners (the quickstart master EOA).

    Returns the transaction hash from `send_safe_txs`. Raises on chain errors.
    """
    from autonomy.chain.base import registry_contracts
    from operate.utils.gnosis import send_safe_txs

    instance = registry_contracts.service_registry.get_instance(
        ledger_api=ledger_api,
        contract_address=service_registry_address,
    )
    # `transferFrom` (not `safeTransferFrom`): see module docstring for why.
    txd_hex = instance.encode_abi(
        abi_element_identifier="transferFrom",
        args=[qs_master_safe, pearl_master_safe, int(service_id)],
    )
    tx_hash = send_safe_txs(
        txd=bytes.fromhex(txd_hex[2:]),
        safe=qs_master_safe,
        ledger_api=ledger_api,
        crypto=crypto,
        to=service_registry_address,
    )

    # Post-condition: the outer tx mining with status=1 is NOT proof the
    # NFT moved — Gnosis Safe's execTransaction returns success even when
    # the inner call reverts (ExecutionFailure event). Re-read ownerOf
    # and verify it landed on the Pearl Safe. Retry the read so a
    # transient RPC hiccup post-mining doesn't get reported as a transfer
    # failure (the tx is already on-chain — re-running would double-submit).
    new_owner = _read_with_retry(
        lambda: instance.functions.ownerOf(int(service_id)).call(),
        tx_hash=tx_hash,
    )
    if new_owner.lower() != pearl_master_safe.lower():
        raise RuntimeError(
            f"NFT transfer reported success (tx {tx_hash}) but ServiceRegistry "
            f"still reports owner={new_owner}, expected {pearl_master_safe}. "
            "Inner call likely reverted (check Safe's ExecutionFailure event)."
        )
    return tx_hash


def swap_service_safe_owner(
    ledger_api: "LedgerApi",
    crypto: "Crypto",
    service_safe: str,
    old_owner: str,
    new_owner: str,
) -> None:
    """Replace `old_owner` with `new_owner` on `service_safe`.

    `old_owner` is the quickstart master Safe (a contract owner of the
    service multisig after `terminate` swapped agents -> master Safe);
    `crypto` is the master EOA that owns the quickstart master Safe.

    Two on-chain transactions, both originated by the qs master Safe and
    signed by the master EOA. This mirrors operate's
    `EthSafeTxBuilder.get_safe_b_native_transfer_messages` Safe-A-
    controlling-Safe-B pattern (protocol.py:get_safe_b_native_transfer_messages),
    adapted for a swapOwner inner instead of a value transfer.

    Why we can't just call swapOwner from the qs master Safe directly:
    Gnosis Safe's `swapOwner` has `authorized` requiring
    `msg.sender == address(this)`. It MUST go through the service
    Safe's own `execTransaction`. Since the service Safe's only owner
    is a contract (the qs master Safe), the qs master Safe authorizes
    that `execTransaction` via the on-chain `approveHash` path:

      1) qs_master_safe -> service_safe.approveHash(inner_tx_hash)
         (registers qs_master_safe as having pre-approved the hash).
      2) qs_master_safe -> service_safe.execTransaction(<swapOwner args>,
         signatures=packed_approved_hash(qs_master_safe))
         (Safe's signature check sees the approved-hash signature,
         looks up approvedHashes[qs_master_safe][inner_tx_hash] == 1,
         and proceeds. The inner call is a self-call into swapOwner,
         which then sees msg.sender == address(this).)
    """
    from autonomy.chain.base import registry_contracts
    from operate.services.protocol import (
        get_packed_signature_for_approved_hash,
    )
    from operate.utils.gnosis import (
        SafeOperation,
        get_owners,
        get_prev_owner,
        send_safe_txs,
    )

    prev_owner = get_prev_owner(
        ledger_api=ledger_api, safe=service_safe, owner=old_owner,
    )
    service_safe_instance = registry_contracts.gnosis_safe.get_instance(
        ledger_api=ledger_api,
        contract_address=service_safe,
    )
    swap_owner_data_hex = service_safe_instance.encode_abi(
        abi_element_identifier="swapOwner",
        args=[prev_owner, old_owner, new_owner],
    )
    swap_owner_data = bytes.fromhex(swap_owner_data_hex[2:])

    # Hash of the inner Safe tx that the service Safe will execute on
    # itself. Must match the parameters passed to execTransaction below.
    inner_tx_hash_hex = registry_contracts.gnosis_safe.get_raw_safe_transaction_hash(
        ledger_api=ledger_api,
        contract_address=service_safe,
        to_address=service_safe,
        value=0,
        data=swap_owner_data,
        operation=SafeOperation.CALL.value,
        safe_tx_gas=0,
    ).get("tx_hash")

    # Step 1: qs master Safe -> service Safe.approveHash(inner_tx_hash).
    approve_hash_data_hex = service_safe_instance.encode_abi(
        abi_element_identifier="approveHash",
        args=[inner_tx_hash_hex],
    )
    send_safe_txs(
        txd=bytes.fromhex(approve_hash_data_hex[2:]),
        safe=old_owner,
        ledger_api=ledger_api,
        crypto=crypto,
        to=service_safe,
    )

    # Step 2: qs master Safe -> service Safe.execTransaction(...) with a
    # packed approved-hash signature pointing back at the qs master Safe.
    exec_data_hex = service_safe_instance.encode_abi(
        abi_element_identifier="execTransaction",
        args=[
            service_safe,                             # to (self-call)
            0,                                        # value
            swap_owner_data,                          # data
            SafeOperation.CALL.value,                 # operation
            0,                                        # safeTxGas
            0,                                        # baseGas
            0,                                        # gasPrice
            "0x" + "00" * 20,                         # gasToken
            "0x" + "00" * 20,                         # refundReceiver
            get_packed_signature_for_approved_hash(
                owners=(old_owner,),
            ),
        ],
    )
    exec_tx_hash = send_safe_txs(
        txd=bytes.fromhex(exec_data_hex[2:]),
        safe=old_owner,
        ledger_api=ledger_api,
        crypto=crypto,
        to=service_safe,
    )

    # Post-condition: as with `transfer_service_nft`, the outer tx mining
    # is NOT proof the swap landed — Safe's execTransaction emits
    # ExecutionFailure and returns false on inner-call revert without
    # bubbling up. Re-read getOwners() and verify the swap. Retry the
    # read so a transient RPC hiccup post-mining doesn't surface as
    # "swap failed" when the on-chain side actually completed.
    owners_after = {
        o.lower() for o in _read_with_retry(
            lambda: get_owners(ledger_api=ledger_api, safe=service_safe),
            tx_hash=exec_tx_hash,
        )
    }
    if new_owner.lower() not in owners_after or old_owner.lower() in owners_after:
        raise RuntimeError(
            f"swapOwner reported success (tx {exec_tx_hash}) but service Safe "
            f"{service_safe} still has owners {sorted(owners_after)}. "
            f"Expected {new_owner} present and {old_owner} absent. Inner call "
            "likely reverted (check Safe's ExecutionFailure event for the hash)."
        )
