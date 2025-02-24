# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------------
#
#   Copyright 2023 Valory AG
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

"""This package contains utils for working with the staking contract."""

import hashlib
import json
import sys
from pathlib import Path
from operate.cli import OperateApp
from operate.constants import OPERATE_HOME
from operate.quickstart.run_service import configure_local_config, get_service
from operate.services.service import Service


def get_subgraph_api_key() -> str:
    """Get subgraph api key."""
    subgraph_api_key_path = OPERATE_HOME / "subgraph_api_key.txt"
    if subgraph_api_key_path.exists():
        return subgraph_api_key_path.read_text()
    
    subgraph_api_key = input("Please enter your subgraph api key: ")
    subgraph_api_key_path.parent.mkdir(parents=True, exist_ok=True)
    subgraph_api_key_path.write_text(subgraph_api_key)
    return subgraph_api_key 


def get_service_from_config(config_path: Path) -> Service:
    """Get service safe."""
    if not config_path.exists():
        print("No trader agent config found!")
        sys.exit(1)

    with open(config_path, "r") as config_file:
        template = json.load(config_file)
    
    operate = OperateApp()
    manager = operate.service_manager()
    configure_local_config(template)
    return get_service(manager, template)

def verify_password(password: str, path : Path) -> bool:
    user_json_path = path / "user.json"
    print("\nVerifying password...")
    
    if not user_json_path.exists():
        print("No user.json found - first time setup")
        return True
        
    with open(user_json_path, 'r') as f:
        user_data = json.load(f)
        
    stored_hash = user_data.get("password_sha")
    if not stored_hash:
        print("No password hash stored - first time setup")
        return True
        
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    is_valid = password_hash == stored_hash
    
    return is_valid

def validate_config_params(config_data: dict, required_params: list[str]) -> None:
    """
    Validates required configuration parameters from the source config.
    
    Args:
        config_data (dict): Configuration dictionary to validate
        required_params (list[str]): List of required parameter keys
        
    Raises:
        ValueError: If any required parameter is missing or empty
    """
    missing_params = []
    for param in required_params:
        if not config_data.get(param):  # Checks for None, empty string, or missing key
            missing_params.append(param)
    
    if missing_params:
        raise ValueError(
            f"Missing required configuration parameters: {', '.join(missing_params)}. "
            "Please ensure all required parameters are provided in the local_config.json file."
        )