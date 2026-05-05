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
   Pearl master Safe in the service multisig's owner list. Reuses
   `operate.utils.gnosis.swap_owner` directly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aea.crypto.base import Crypto, LedgerApi


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
    return send_safe_txs(
        txd=bytes.fromhex(txd_hex[2:]),
        safe=qs_master_safe,
        ledger_api=ledger_api,
        crypto=crypto,
        to=service_registry_address,
    )


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
    send_safe_txs(
        txd=bytes.fromhex(exec_data_hex[2:]),
        safe=old_owner,
        ledger_api=ledger_api,
        crypto=crypto,
        to=service_safe,
    )
