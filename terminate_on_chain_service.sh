#!/bin/bash

# ------------------------------------------------------------------------------
#
#   Copyright 2023-2024 Valory AG
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


# force utf mode for python, cause sometimes there are issues with local codepages
export PYTHONUTF8=1

set -e  # Exit script on first error

ATTENDED_ARG=""

while [[ $# -gt 0 ]]; do
   case $1 in
       --attended=*)
           value="${1#*=}"
           if [[ "$value" != "true" && "$value" != "false" ]]; then
               echo "Error: --attended only accepts true/false values"
               exit 1
           fi
           if [[ "$value" == "false" ]]; then
               ATTENDED_ARG="--attended=false"
           fi
           shift
           ;;
       --help|-h)
           echo "Usage: ./terminate.sh <config_path> [--attended=true|false]"
           echo
           echo "Arguments:"
           echo "  <config_path>              Path to config file (required)"
           echo "  --attended=true|false      Run in attended/unattended mode (default: true)"
           echo "  --help,-h                 Show this help message"
           exit 0
           ;;
       --*)
           echo "Error: Unknown flag $1"
           echo "Use --help for available options"
           exit 1
           ;;
       *)
           CONFIG_PATH="$1"
           shift
           ;;
   esac
done

[[ -z "$CONFIG_PATH" ]] && { echo "Error: Config path required"; exit 1; }

poetry install --only main --no-cache
poetry run python -m operate.cli terminate "$CONFIG_PATH" $ATTENDED_ARG
