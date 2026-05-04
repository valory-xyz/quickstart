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

# Migrate this quickstart's `.operate/` to Pearl's `~/.operate/`.
# Pass --help for full usage. Optionally pass a configs/<agent>.json to
# restrict the migration to a single service.

# force utf mode for python, cause sometimes there are issues with local codepages
export PYTHONUTF8=1

set -euo pipefail   # exit on error / undefined var / pipeline failure

# Tooling pre-checks — the script signs on-chain transactions, so failing
# fast on a missing prerequisite is much better than a half-applied poetry
# install or a confusing import error halfway through.
if ! command -v poetry >/dev/null 2>&1; then
    echo >&2 "Poetry is not installed (https://python-poetry.org/docs/)."
    exit 1
fi

poetry install --only main --no-cache
poetry run python -m scripts.pearl_migration.migrate_to_pearl "$@"
