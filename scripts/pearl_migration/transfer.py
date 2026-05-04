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

    Thin wrapper over `operate.utils.gnosis.swap_owner` so callers don't need
    to import middleware internals.
    """
    from operate.utils.gnosis import swap_owner

    swap_owner(
        ledger_api=ledger_api,
        crypto=crypto,
        safe=service_safe,
        old_owner=old_owner,
        new_owner=new_owner,
    )
