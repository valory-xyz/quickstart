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

# Display information of the Git repository
current_branch=$(git rev-parse --abbrev-ref HEAD)
latest_commit_hash=$(git rev-parse HEAD)
echo "Current branch: $current_branch"
echo "Commit hash: $latest_commit_hash"

# Check if user is inside a venv
if [[ "$VIRTUAL_ENV" != "" ]]
then
    echo "Please exit the virtual environment!"
    exit 1
fi

# Check dependencies
if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo >&2 "Python is not installed!";
    exit 1
fi

if ! [[ $($PYTHON_CMD --version) =~ ^(Python\ 3\.[8-9])|(Python\ 3\.10)|(Python\ 3\.11) ]]; then
    echo "Python version >=3.8.0, <3.12.0 is required"
    exit 1
fi
echo "`$PYTHON_CMD --version` is compatible"

command -v poetry >/dev/null 2>&1 ||
{ echo >&2 "Poetry is not installed!";
  exit 1
}

command -v docker >/dev/null 2>&1 ||
{ echo >&2 "Docker is not installed!";
  exit 1
}

docker rm -f abci0 node0 trader_abci_0 trader_tm_0 &> /dev/null ||
{ echo >&2 "Docker is not running!";
  exit 1
}

# own the data directory (if required)
# this is for migrating from an old version of the agents (using open-autonomy <0.19.9), which used to save these files as root
# it can be removed after some time with the next major release of the quickstart
if [ -d ".operate/services" ]; then
    for service_dir in .operate/services/sc-*; do
        if [ -d "$service_dir/persistent_data" ]; then
            # Check if directory itself is owned by root
            if [ "$(stat -c '%U' "$service_dir/persistent_data" 2>/dev/null || stat -f '%Su' "$service_dir/persistent_data" 2>/dev/null)" = "root" ]; then
                echo "Changing ownership of $service_dir/persistent_data from root to current user. Please enter sudo password."
                sudo chown -R $(id -u):$(id -g) "$service_dir/persistent_data"
            fi
            # Check if any files within persistent_data are owned by root
            if find "$service_dir/persistent_data" -user root -print -quit | grep -q .; then
                echo "Changing ownership of root-owned files in $service_dir/persistent_data to current user. Please enter sudo password"
                sudo chown -R $(id -u):$(id -g) "$service_dir/persistent_data"
            fi
        fi
    done
fi

# Install dependencies and run the agent througth the middleware
poetry install --only main --no-cache
poetry run pip install --upgrade packaging  # TODO: update packaging version from open-aea
poetry run python -m operate.cli quickstart $@