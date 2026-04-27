#!/bin/bash

# ------------------------------------------------------------------------------
#
#   Copyright 2026 Valory AG
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# ------------------------------------------------------------------------------

set -euo pipefail

KEYS_DIR=".operate/keys"

if ! [ -d "$KEYS_DIR" ]; then
    echo "Directory not found: $KEYS_DIR" >&2
    exit 1
fi

if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "Python is required to parse JSON files." >&2
    exit 1
fi

processed=0

for key_file in "$KEYS_DIR"/0x*; do
    [ -e "$key_file" ] || continue
    [ -f "$key_file" ] || continue

    file_name="$(basename "$key_file")"

    # Only process names that start with 0x and contain no dot.
    if [[ "$file_name" == *.* ]]; then
        continue
    fi

    private_key="$($PYTHON_CMD - "$key_file" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
with path.open("r", encoding="utf-8") as f:
    data = json.load(f)

private_key = data.get("private_key")
if not isinstance(private_key, str) or private_key == "":
    raise ValueError("missing or invalid 'private_key' field")

print(private_key)
PY
    )" || {
        echo "Skipping invalid key file: $key_file" >&2
        continue
    }

    printf '%s\n' "$private_key" > "$key_file.pk"
    processed=$((processed + 1))
done

echo "Wrote .pk files for $processed key file(s)."
