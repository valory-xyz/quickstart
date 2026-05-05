"""Quickstart wallet password alignment for Mode B merges.

Mode B merges qs/.operate into an existing Pearl/.operate. The two stores
may have been initialised with different master passwords. After the
merge, Pearl's keys manager — which uses Pearl's master password — must
be able to decrypt every agent key it now sees. A verbatim copy of qs's
agent keyfiles leaves them encrypted with qs's password and breaks the
first deploy with `DecryptError: Decrypt error! Bad password?`.

Rather than carrying two passwords through `merge_service` and re-keying
files mid-copy, we re-encrypt the qs store IN PLACE before merging:
qs's master keyfile and every key under qs/.operate/keys/ get re-encrypted
with Pearl's password. After this step qs and Pearl share a password and
the rest of the migration treats agent keys as opaque blobs.

Mode A never hits this — it copies qs into a fresh Pearl store, so Pearl
inherits qs's password by construction.
"""
from __future__ import annotations

import json
from typing import TYPE_CHECKING

from .prompts import info, warn

if TYPE_CHECKING:
    from operate.app import OperateApp
    from operate.wallet.master import MasterWallet


def align_quickstart_password(
    qs_app: "OperateApp",
    qs_wallet: "MasterWallet",
    new_password: str,
) -> None:
    """Re-encrypt qs's master keyfile + every agent key with `new_password`.

    Mutates `qs_app.password` and `qs_wallet`'s on-disk keyfile in place.
    Idempotent when `qs_app.password == new_password` (no-op).
    """
    from eth_account import Account

    if qs_app.password == new_password:
        info("  quickstart and Pearl already share a master password.")
        return

    info("  re-encrypting quickstart master keyfile with Pearl's password...")
    # Operate's `update_password` re-encrypts the master keyfile and updates
    # the in-memory wallet password; covers the master EOA case.
    qs_wallet.update_password(new_password)

    # Now walk the keys directory and re-encrypt each agent key. The keys
    # manager wraps each file as an operate `Key` (ledger / address /
    # private_key), where private_key holds the JSON-encoded eth_account
    # keyfile encrypted with the master password.
    keys_manager = qs_app.keys_manager
    keys_dir = keys_manager.path
    if keys_dir.exists():
        old_password = qs_app.password
        keys_manager.password = old_password  # explicit: decrypt with old
        re_encrypted = 0
        for key_path in keys_dir.iterdir():
            if not key_path.is_file() or key_path.suffix == ".bak":
                continue
            try:
                _reencrypt_agent_key(
                    key_path=key_path,
                    keys_manager=keys_manager,
                    old_password=old_password,
                    new_password=new_password,
                    Account=Account,
                )
                re_encrypted += 1
            except Exception as exc:  # pylint: disable=broad-except
                warn(
                    f"  failed to re-encrypt {key_path.name}: {exc}. "
                    "Restore the .bak sibling and rerun the migration."
                )
                raise
        info(f"  re-encrypted {re_encrypted} agent key(s) with Pearl's password.")

    # All keyfiles on qs/ side now use Pearl's password. Plumb that through
    # so subsequent operate calls (signing, key lookup, deploy) authenticate.
    qs_app.password = new_password
    keys_manager.password = new_password


def _reencrypt_agent_key(
    *,
    key_path,
    keys_manager,
    old_password: str,
    new_password: str,
    Account,
) -> None:
    """Decrypt with `old_password`, re-encrypt with `new_password`, atomic."""
    from operate.keys import Key

    key = Key.from_json(  # type: ignore[attr-defined]
        obj=json.loads(key_path.read_text(encoding="utf-8")),
    )
    decrypted = key.get_decrypted_json(old_password)
    raw_pk_hex = decrypted["private_key"]
    if raw_pk_hex.startswith("0x") or raw_pk_hex.startswith("0X"):
        raw_pk_hex = raw_pk_hex[2:]
    raw_pk = bytes.fromhex(raw_pk_hex)

    new_keyfile = Account.encrypt(raw_pk, new_password)
    new_key = Key(  # type: ignore[call-arg]
        ledger=key.ledger,
        address=key.address,
        private_key=json.dumps(new_keyfile),
    )

    # Atomic write: write to a tmp sibling and rename. A crash mid-write
    # would otherwise leave the keyfile truncated and unrecoverable.
    tmp_path = key_path.with_suffix(key_path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(new_key.json), encoding="utf-8")
    tmp_path.replace(key_path)
