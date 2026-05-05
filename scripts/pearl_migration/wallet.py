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

Crash safety: a snapshot of `wallets/` and `keys/` is taken before any
mutation. On any failure during re-encryption, the snapshot directory
path is included in the raised exception so the user has a concrete
recovery command. The snapshot is NEVER auto-deleted — on success the
user may remove it once Pearl has launched; on failure it is the only
recovery artifact, so removing it before the issue is resolved would
be unrecoverable.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from .prompts import backup_suffix, info, warn

if TYPE_CHECKING:
    from operate.app import OperateApp
    from operate.wallet.master import MasterWallet


def align_quickstart_password(
    qs_app: "OperateApp",
    qs_wallet: "MasterWallet",
    new_password: str,
) -> None:
    """Re-encrypt qs's master keyfile + every agent key with `new_password`.

    Mutates `qs_app.password`, the keys manager's password, and qs's
    on-disk keyfiles in place. Idempotent when `qs_app.password ==
    new_password` (no-op).
    """
    # Capture old password BEFORE any mutation. `qs_wallet.update_password`
    # writes through to `qs_app.password` (wallets share state via the
    # wallet manager), so reading after the master-keyfile rotate would
    # silently surface as a `DecryptError` on every agent key.
    old_password = qs_app.password

    if old_password == new_password:
        info("  quickstart and Pearl already share a master password.")
        return

    keys_manager = qs_app.keys_manager
    keys_dir: Path = keys_manager.path
    wallets_dir: Path = qs_wallet.path

    # Snapshot wallets/ and keys/ to a timestamped sibling under the
    # qs `.operate/`. If anything below this point fails the user has
    # a concrete recovery path printed in the exception message — there
    # is otherwise NO way back: the master keyfile is rotated to Pearl's
    # password, agent keys are atomically replaced, and operate doesn't
    # cache the old plaintext anywhere.
    snapshot_dir = wallets_dir.parent / f".pre-align.{backup_suffix()}"
    snapshot_dir.mkdir(parents=True, exist_ok=False)
    if wallets_dir.exists():
        shutil.copytree(wallets_dir, snapshot_dir / wallets_dir.name, symlinks=True)
    if keys_dir.exists():
        shutil.copytree(keys_dir, snapshot_dir / keys_dir.name, symlinks=True)
    info(f"  pre-align snapshot: {snapshot_dir}")

    try:
        info("  re-encrypting quickstart master keyfile with Pearl's password...")
        qs_wallet.update_password(new_password)

        if keys_dir.exists():
            re_encrypted = 0
            for key_path in keys_dir.iterdir():
                if not key_path.is_file():
                    continue
                # Skip `.tmp` partial-write artifacts and operate's own
                # `.bak` siblings. Defensive — snapshot-based recovery is
                # the normal path; this only fires if a foreign writer
                # left a stray sibling we'd otherwise try to parse as a
                # keyfile and abort the whole walk.
                if key_path.suffix == ".tmp":
                    warn(f"  ignored stale partial-write artifact: {key_path}")
                    continue
                if key_path.suffix == ".bak":
                    continue
                _reencrypt_agent_key(
                    key_path=key_path,
                    old_password=old_password,
                    new_password=new_password,
                )
                re_encrypted += 1
            info(f"  re-encrypted {re_encrypted} agent key(s) with Pearl's password.")

        # All keyfiles on qs/ side now use Pearl's password. Plumb through
        # so subsequent operate calls authenticate against the new password.
        qs_app.password = new_password
        keys_manager.password = new_password
    except Exception as exc:  # pylint: disable=broad-except
        warn(
            f"  failed to re-encrypt qs wallet: {exc}. "
            f"Recover with: rm -rf {wallets_dir} {keys_dir} && "
            f"mv {snapshot_dir}/{wallets_dir.name} {wallets_dir} && "
            f"mv {snapshot_dir}/{keys_dir.name} {keys_dir}"
        )
        raise


def _reencrypt_agent_key(
    *,
    key_path: Path,
    old_password: str,
    new_password: str,
) -> None:
    """Decrypt with `old_password`, re-encrypt with `new_password`, atomic."""
    from eth_account import Account
    from operate.keys import Key

    key = Key.from_json(  # type: ignore[attr-defined]
        obj=json.loads(key_path.read_text(encoding="utf-8")),
    )
    decrypted = key.get_decrypted_json(old_password)
    raw_pk_hex = decrypted["private_key"]
    if raw_pk_hex.startswith(("0x", "0X")):
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
