from dataclasses import dataclass
import hashlib
import os
from typing import TypedDict
from getpass import getpass
from halo import Halo
import json
from pathlib import Path
import sys
import shutil
import logging

from aea_ledger_ethereum import Account
from operate.cli import OperateApp
from operate.constants import KEYS_JSON, OPERATE
from operate.keys import Key
from operate.operate_types import Chain, LedgerType
from operate.quickstart.run_service import get_service, QuickstartConfig
from operate.services.service import Service
from operate.utils.common import print_section, print_title
from migrate_legacy_mech import verify_password


MODIUS_PATH = Path(__file__).parent.parent.parent / ".olas-modius"
OPERATE_HOME = Path(__file__).parent.parent.parent / OPERATE
DATA_FILES = ("current_pool.json", "gas_costs.json", "assets.json")

class StakingVariables(TypedDict):
    USE_STAKING: bool
    STAKING_PROGRAM: str
    AGENT_ID: int
    CUSTOM_SERVICE_REGISTRY_ADDRESS: str
    CUSTOM_SERVICE_REGISTRY_TOKEN_UTILITY_ADDRESS: str
    CUSTOM_OLAS_ADDRESS: str
    CUSTOM_STAKING_ADDRESS: str
    MECH_ACTIVITY_CHECKER_CONTRACT: str
    MIN_STAKING_BOND_OLAS: int
    MIN_STAKING_DEPOSIT_OLAS: int

@dataclass
class ModiusData:
    password: str
    agent_eoa: dict
    master_eoa: dict
    rpc: str
    service_id: int
    service_safe: str
    use_staking: bool
    staking_variables: StakingVariables

def parse_modius_files() -> ModiusData:
    print_section("Parsing .olas-modius files")
    
    # Get user input for staking configuration
    while True:
        staking_input = input("Was this a staked Modius agent? (yes/no): ").lower()
        if staking_input in ['yes', 'no']:
            use_staking = staking_input == 'yes'
            break
        print("Please enter 'yes' or 'no'")
    
    agent_key_file = next(MODIUS_PATH.glob("keys/*"))
    agent_key = json.loads(agent_key_file.read_text())
    
    master_wallet_file = MODIUS_PATH / "wallets/ethereum.txt"
    master_key = json.loads(master_wallet_file.read_text())
    
    config_file = MODIUS_PATH / "local_config.json"
    config = json.loads(config_file.read_text())
    rpc = config.get("mode_rpc")
    
    service_dir = next(MODIUS_PATH.glob("services/*"))
    service_config = json.loads((service_dir / "config.json").read_text())
    
    service_id = service_config["chain_configs"]["34443"]["chain_data"]["token"]
    service_safe = service_config["chain_configs"]["34443"]["chain_data"]["multisig"]
    
    print_section("Verifying credentials...")
    password = None
    while password is None:
        password = getpass("Enter local user account password [hidden input]: ")
        if verify_password(password):
            break
        password = None
        print("Invalid password!")

    staking_vars = {
        "USE_STAKING": use_staking,
        "STAKING_PROGRAM": "optimus_alpha" if use_staking else "no_staking",
        "AGENT_ID": 40,
        "CUSTOM_SERVICE_REGISTRY_ADDRESS": "0x3C1fF68f5aa342D296d4DEe4Bb1cACCA912D95fE" if use_staking else "0x9338b5153AE39BB89f50468E608eD9d764B755fD",
        "CUSTOM_SERVICE_REGISTRY_TOKEN_UTILITY_ADDRESS": "0x34C895f302D0b5cf52ec0Edd3945321EB0f83dd5" if use_staking else "0xa45E64d13A30a51b91ae0eb182e88a40e9b18eD8",
        "CUSTOM_OLAS_ADDRESS": "0xcfD1D50ce23C46D3Cf6407487B2F8934e96DC8f9" if use_staking else "0x0000000000000000000000000000000000000000",
        "CUSTOM_STAKING_ADDRESS": "0x5fc25f50E96857373C64dC0eDb1AbCBEd4587e91" if use_staking else "0x43fB32f25dce34EB76c78C7A42C8F40F84BCD237",
        "MECH_ACTIVITY_CHECKER_CONTRACT": "0x07bc3C23DbebEfBF866Ca7dD9fAA3b7356116164" if use_staking else "0x0000000000000000000000000000000000000000",
        "MIN_STAKING_BOND_OLAS": 20000000000000000000 if use_staking else 1,
        "MIN_STAKING_DEPOSIT_OLAS": 20000000000000000000 if use_staking else 1
    }

    return ModiusData(
        password,
        agent_key,
        master_key,
        rpc,
        service_id,
        service_safe,
        use_staking,
        staking_vars,
    )

def copy_data_files(target_data_dir: Path) -> None:
    target_data_dir.mkdir(exist_ok=True)
    
    for data_file in DATA_FILES:
        src_file = MODIUS_PATH / data_file
        if src_file.exists():
            target_file = target_data_dir / data_file
            shutil.copy2(src_file, target_file)

def populate_operate(operate: OperateApp, modius_data: ModiusData) -> Service:
    print_section("Setting up Operate")
    operate.setup()
    
    if operate.user_account is None:
        spinner = Halo(text="Creating user account...", spinner="dots").start()
        operate.create_user_account(modius_data.password)
        spinner.succeed("User account created")
    else:
        operate.password = modius_data.password

    # Setup local config
    qs_config_path = OPERATE_HOME / "local_config.json"
    if not qs_config_path.exists():
        spinner = Halo(text="Creating quickstart config...", spinner="dots").start()
        source_config = json.loads((MODIUS_PATH / "local_config.json").read_text())
        qs_config = QuickstartConfig(
            path=OPERATE_HOME / "local_config.json",
            password_migrated=True,
            principal_chain="mode",
            rpc={"mode": modius_data.rpc},
            user_provided_args={
                "TENDERLY_ACCESS_KEY": source_config.get("tenderly_access_key", ""),
                "TENDERLY_ACCOUNT_SLUG": source_config.get("tenderly_account_slug", ""),
                "TENDERLY_PROJECT_SLUG": source_config.get("tenderly_project_slug", ""),
                "COINGECKO_API_KEY": source_config.get("coingecko_api_key", "")
            },
            staking_vars=modius_data.staking_variables,
        )
        qs_config.store()
        spinner.succeed("Quickstart config created")   

    # Setup wallet
    if not operate.wallet_manager.exists(LedgerType.ETHEREUM):
        spinner = Halo(text="Setting up wallet...", spinner="dots").start()
        operate.wallet_manager.setup()
        
        source_wallets = MODIUS_PATH / "wallets"
        shutil.copytree(source_wallets, operate.wallet_manager.path, dirs_exist_ok=True)
        
        wallet_json_path = operate.wallet_manager.path / "ethereum.json"
        with open(wallet_json_path, 'r') as f:
            wallet_data = json.load(f)
            
        if "6" in wallet_data.get("safes", {}):
            wallet_data["safes"] = {"mode": wallet_data["safes"]["6"]}
        wallet_data["safe_chains"] = ["mode"]
        if wallet_data["ledger_type"] == 0:
            wallet_data["ledger_type"] = "ethereum"
        
        with open(wallet_json_path, 'w') as f:
            json.dump(wallet_data, fp=f, indent=2)
        spinner.succeed("Wallet setup complete")
    
    # Copy keys directory
    spinner = Halo(text="Setting up agent keys...", spinner="dots").start()
    operate.keys_manager.setup()
    source_keys = MODIUS_PATH / "keys"
    target_keys = operate.keys_manager.path
    target_keys.mkdir(parents=True, exist_ok=True)

    # Copy and update key files
    for key_file in source_keys.glob("*"):
        if key_file.is_file():
            # Read the key file
            with open(key_file, 'r') as f:
                key_data = json.load(f)
            
            # Update ledger value if it's 0
            if key_data.get("ledger") == 0:
                key_data["ledger"] = "ethereum"
            
            # Write updated key data to target
            target_file = target_keys / key_file.name
            with open(target_file, 'w') as f:
                json.dump(key_data, fp=f, indent=4)

    spinner.succeed("Agent keys setup complete")

    # Setup service
    with open(OPERATE_HOME.parent / "configs" / "config_modius.json", "r") as config_file:
        service_template = json.load(config_file)

    service_template["configurations"]["mode"] |= {
        "staking_program_id": modius_data.staking_variables["STAKING_PROGRAM"],
        "rpc": modius_data.rpc,
        "agent_id": modius_data.staking_variables["AGENT_ID"],
        "use_staking": modius_data.use_staking,
        "cost_of_bond": modius_data.staking_variables["MIN_STAKING_BOND_OLAS"],
    }

    service_manager = operate.service_manager()
    if len(service_manager.json) == 0:
        spinner = Halo(text="Creating service...", spinner="dots").start()
        service = get_service(service_manager, service_template)
        
        # Get service keys
        service_source = next(MODIUS_PATH.glob("services/*"))
        with open(service_source / KEYS_JSON, 'r') as f:
            service_keys = json.load(f)

        # Copy keys
        with open(service.path / KEYS_JSON, "w") as f:
            json.dump(obj=service_keys, fp=f, indent=2)

        # Clean up any keys that don't match service keys
        valid_addresses = {key["address"].lower() for key in service_keys}
        keys_dir = operate.keys_manager.path
        for key_file in keys_dir.glob("*"):
            if key_file.name.lower() not in valid_addresses:
                key_file.unlink()

        service.keys = [Key(**service_keys[0])]
        service.chain_configs["mode"].chain_data.token = modius_data.service_id
        service.chain_configs["mode"].chain_data.multisig = modius_data.service_safe

        copy_data_files(service.path / "data")
        
        service.store()
        spinner.succeed("Service created")

    return get_service(service_manager, service_template)

def main() -> None:
    print_title("Modius Quickstart Migration")
    if not MODIUS_PATH.exists():
        print("No .olas-modius folder found!")
        sys.exit(1)
    
    modius_data = parse_modius_files()
    operate = OperateApp(home=OPERATE_HOME)
    service = populate_operate(operate, modius_data)
    print_section("Modius Migration complete!")

if __name__ == "__main__":
    main()