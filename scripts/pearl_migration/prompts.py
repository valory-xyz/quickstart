"""Interactive prompts.

Thin wrappers over `operate.quickstart.utils` so we share the same UX with
the existing migration script (`scripts/predict_trader/migrate_legacy_quickstart.py`)
and inherit middleware features for free:

- `ATTENDED=false` env var → `ask_yes_or_no` returns True automatically (CI mode).
- `OPERATE_PASSWORD` env var → `ask_or_get_from_env(..., is_pass=True)` short-circuits.
"""

from __future__ import annotations

import sys
import time
from enum import Enum
from pathlib import Path
from typing import Callable, Optional

from operate.quickstart.utils import ask_or_get_from_env, ask_yes_or_no


class CollisionChoice(Enum):
    SKIP = "skip"
    OVERWRITE_WITH_BACKUP = "overwrite"


def yes_no(question: str, default: Optional[bool] = None) -> bool:
    """Yes/no prompt.

    * Attended mode (default): prompts via middleware's `ask_yes_or_no`,
      which loops until the user types yes/no (no empty-input shortcut).
      If `default` is supplied we still pass it through to a one-line
      empty-input fallback so code that wants the legacy "[Y/n]" feel
      keeps working.
    * Unattended mode (`ATTENDED=false`): the middleware unconditionally
      returns True. We bypass it when `default` is supplied so each prompt
      gets its declared safe default (e.g. the rename-source prompt
      defaulting to True is fine; "different machine?" defaulting to False
      is what we want — middleware would say True here).
    """
    if not _attended():
        return True if default is None else default
    if default is None:
        return ask_yes_or_no(question)
    suffix = " [Y/n] " if default else " [y/N] "
    while True:
        try:
            raw = input(question + suffix).strip().lower()
        except EOFError:
            # Closed/piped stdin — fall back to the declared default
            # rather than crashing. The end-of-migration prompts
            # (rename-source, "different machine?") both pass an
            # explicit default, so this is meaningful.
            return default
        if not raw:
            return default
        if raw in ("y", "yes"):
            return True
        if raw in ("n", "no"):
            return False


def _attended() -> bool:
    import os
    return os.environ.get("ATTENDED", "true").lower() == "true"


def password(
    prompt: str = "Password: ", env_var_name: str = "OPERATE_PASSWORD",
) -> str:
    """Password prompt, defers to middleware's `ask_or_get_from_env`.

    Reads `OPERATE_PASSWORD` (or the supplied `env_var_name`) when set or
    when `ATTENDED=false`. Otherwise falls back to interactive `getpass`.
    """
    return ask_or_get_from_env(
        prompt=prompt,
        is_pass=True,
        env_var_name=env_var_name,
        raise_if_missing=False,
    )


def collision(target: Path, kind: str) -> CollisionChoice:
    """Ask the user how to resolve a destination collision.

    `kind` is "service" or "key" — used only for the message.
    Tells the user this most likely means a previous migration was retried.

    Called from `merge_service` AFTER on-chain ops have committed, so a
    raw `EOFError` traceback here (closed/piped stdin in CI) would abort
    the for-loop, skip drain + rename, and leave the user with no
    summary. We default to SKIP (the safe choice — assumes the existing
    dest is the intended migrated copy) and warn so it's visible.
    """
    print()
    print(f"  ! {kind.capitalize()} already exists at destination: {target}")
    print(
        "    This usually means you're re-running a migration that previously "
        "got partway. Pick how to handle it:"
    )
    print("      1) Skip — leave the destination untouched (safe if it's the migrated copy).")
    print("      2) Overwrite with backup — rename existing to .bak.<ts> then copy fresh.")
    while True:
        try:
            raw = input("    Choice [1/2]: ").strip()
        except EOFError:
            warn(
                f"stdin closed while resolving collision for {target}; "
                "defaulting to SKIP (existing destination preserved)."
            )
            return CollisionChoice.SKIP
        if raw == "1":
            return CollisionChoice.SKIP
        if raw == "2":
            return CollisionChoice.OVERWRITE_WITH_BACKUP
        warn(f"Invalid choice {raw!r}; enter 1 or 2.")


def backup_suffix() -> str:
    """Timestamp suffix used for `.bak.<ts>` paths."""
    return f"bak.{int(time.time())}"


def info(msg: str) -> None:
    print(f"  - {msg}")


def warn(msg: str) -> None:
    print(f"  ! {msg}")


def fatal(msg: str, code: int = 1) -> None:
    print(f"  X {msg}", file=sys.stderr)
    sys.exit(code)


def ask_password_validating(
    prompt: str,
    validate: Callable[[str], bool],
    *,
    attempts: int = 3,
    not_match_msg: str = "Wrong password.",
) -> Optional[str]:
    """Prompt for a password up to `attempts` times, calling `validate(pw) -> bool`.

    Returns the password on success, None if the user gave up. Validation
    exceptions are surfaced (so RPC failures abort cleanly rather than masquerade
    as a wrong password).
    """
    for i in range(attempts):
        pw = password(prompt)
        if validate(pw):
            return pw
        remaining = attempts - i - 1
        if remaining:
            warn(f"{not_match_msg} {remaining} attempt(s) left.")
        else:
            warn(not_match_msg)
    return None
