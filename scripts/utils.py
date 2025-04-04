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

import json
import sys
from pathlib import Path
from operate.cli import OperateApp
from operate.constants import OPERATE_HOME
from operate.quickstart.run_service import configure_local_config, get_service
from operate.services.service import Service
from operate.operate_types import Chain


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
    configure_local_config(template, operate)
    return get_service(manager, template)

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

def handle_missing_rpcs(config: dict) -> dict:
    """
    Checks for required RPC endpoints and prompts the user to input them if missing.
    
    Args:
        config: The configuration dictionary from optimus
        
    Returns:
        Updated dictionary with RPC endpoints
    """
    
    required_rpcs = {
        Chain.OPTIMISTIC.value: "optimism_rpc",
        Chain.BASE.value: "base_rpc",
        Chain.MODE.value: "mode_rpc"
    }
    
    rpc_mapping = {}
    missing_rpcs = []
    
    # Check which RPCs are missing
    for chain, config_key in required_rpcs.items():
        if config_key in config and config[config_key]:
            rpc_mapping[chain] = config[config_key]
        else:
            missing_rpcs.append((chain, config_key))
    
    # If any RPCs are missing, prompt the user to input them
    if missing_rpcs:
        print("\nSome required RPC endpoints are missing. Please provide them:")
        
        for chain, config_key in missing_rpcs:
            while True:
                user_input = input(f"Enter {chain} RPC endpoint: ").strip()
                
                if user_input:
                    rpc_mapping[chain] = user_input
                    # Update the original config as well
                    config[config_key] = user_input
                    break
                else:
                    print(f"Error: RPC endpoint for {chain} is required. Please enter a valid RPC endpoint.")
    
    return rpc_mapping    

def input_with_default_value(prompt: str, default_value: str) -> str:
    user_input = input(f"{prompt} [{default_value}]: ")
    return str(user_input) if user_input else default_value
