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

MECH_PATH = Path(__file__).parent.parent.parent / ".mech_quickstart"
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
class MechData:
    password: str
    agent_eoa: dict
    master_eoa: dict
    rpc: str
    service_id: int
    service_safe: str
    use_staking: bool
    staking_variables: StakingVariables
    api_keys: str

def verify_password(password: str, path : Path= MECH_PATH) -> bool:
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
 
def find_api_keys_file(base_paths: list[Path], filename: str) -> Path | None:
    """Search for API keys file in multiple locations."""
    for base_path in base_paths:
        for name in [filename, f"{filename}.json"]:
            file_path = base_path / name
            if file_path.exists():
                return file_path
    return None

def parse_mech_files() -> MechData:
    print_section("Parsing .mech_quickstart files")
    
    config_file = MECH_PATH / "local_config.json"
    config = json.loads(config_file.read_text())
    use_staking = config.get("use_staking", False)
    api_keys_path = config.get("api_keys_path")
    
    # Handle API keys
    default_api_keys = {
        "openai": ["dummy_api_key"],
        "google_api_key": ["dummy_api_key"]
    }
    
    api_keys = default_api_keys
    if api_keys_path:
        filename = Path(api_keys_path).name
        search_paths = [
            Path.cwd(),
            MECH_PATH,
            MECH_PATH.parent,
            Path(__file__).parent,
            Path(__file__).parent.parent
        ]
        
        if api_keys_file := find_api_keys_file(search_paths, filename):
            try:
                api_keys = json.loads(api_keys_file.read_text())
                print(f"Loaded API keys from: {api_keys_file}")
            except Exception as e:
                print(f"Error loading API keys, using defaults: {e}")
    
    agent_key_file = next(MECH_PATH.glob("keys/*"))
    agent_key = json.loads(agent_key_file.read_text())
    
    master_wallet_file = MECH_PATH / "wallets/ethereum.txt"
    master_key = json.loads(master_wallet_file.read_text())
    
    rpc = config.get("gnosis_rpc")
    
    service_dir = next(MECH_PATH.glob("services/*"))
    service_config = json.loads((service_dir / "config.json").read_text())
    
    service_id = service_config["chain_configs"]["100"]["chain_data"]["token"]
    service_safe = service_config["chain_configs"]["100"]["chain_data"]["multisig"]
    
    print_section("Verifying credentials...")
    password = None
    while password is None:
        password = getpass("Enter local user account password [hidden input]: ")
        if verify_password(password):
            break
        password = None
        print("Invalid password!") 

    staking_vars = {
        "USE_STAKING": use_staking == "True",
        "STAKING_PROGRAM": "mech_marketplace" if use_staking else "no_staking",
        "AGENT_ID": 37,
        "CUSTOM_SERVICE_REGISTRY_ADDRESS": "0x9338b5153AE39BB89f50468E608eD9d764B755fD",
        "CUSTOM_SERVICE_REGISTRY_TOKEN_UTILITY_ADDRESS": "0xa45E64d13A30a51b91ae0eb182e88a40e9b18eD8",
        "CUSTOM_OLAS_ADDRESS": "0xcE11e14225575945b8E6Dc0D4F2dD4C570f79d9f" if use_staking else "0x0000000000000000000000000000000000000000",
        "CUSTOM_STAKING_ADDRESS": "0x998dEFafD094817EF329f6dc79c703f1CF18bC90" if use_staking else "0x43fB32f25dce34EB76c78C7A42C8F40F84BCD237",
        "MECH_ACTIVITY_CHECKER_CONTRACT": "0x32B5A40B43C4eDb123c9cFa6ea97432380a38dDF" if use_staking else "0x0000000000000000000000000000000000000000",
        "MIN_STAKING_BOND_OLAS": 50000000000000000000 if use_staking else 1,
        "MIN_STAKING_DEPOSIT_OLAS": 50000000000000000000 if use_staking else 1
    }

    return MechData(
        password,
        agent_key,
        master_key,
        rpc,
        service_id,
        service_safe,
        use_staking,
        staking_vars,
        json.dumps(api_keys)
    )

def copy_data_files(target_data_dir: Path) -> None:
    target_data_dir.mkdir(exist_ok=True)
    
    for data_file in DATA_FILES:
        src_file = MECH_PATH / data_file
        if src_file.exists():
            target_file = target_data_dir / data_file
            shutil.copy2(src_file, target_file)

def populate_operate(operate: OperateApp, mech_data: MechData) -> Service:
    print_section("Setting up Operate")
    operate.setup()
    
    if operate.user_account is None:
        spinner = Halo(text="Creating user account...", spinner="dots").start()
        operate.create_user_account(mech_data.password)
        spinner.succeed("User account created")
    else:
        operate.password = mech_data.password

    qs_config_path = OPERATE_HOME / "local_config.json"
    if not qs_config_path.exists():
        spinner = Halo(text="Creating quickstart config...", spinner="dots").start()
        qs_config = QuickstartConfig(
            path=OPERATE_HOME / "local_config.json",
            password_migrated=True,
            principal_chain="gnosis",
            rpc={"mode": mech_data.rpc},
            user_provided_args={
                "API_KEYS": mech_data.api_keys
            },
            staking_vars=mech_data.staking_variables,
        )
        qs_config.store()
        spinner.succeed("Quickstart config created")     

    # Setup wallet
    if not operate.wallet_manager.exists(LedgerType.ETHEREUM):
        spinner = Halo(text="Setting up wallet...", spinner="dots").start()
        operate.wallet_manager.setup()
        
        source_wallets = MECH_PATH / "wallets"
        shutil.copytree(source_wallets, operate.wallet_manager.path, dirs_exist_ok=True)
        
        wallet_json_path = operate.wallet_manager.path / "ethereum.json"
        with open(wallet_json_path, 'r') as f:
            wallet_data = json.load(f)
            
        if "2" in wallet_data.get("safes", {}):
            wallet_data["safes"] = {"gnosis": wallet_data["safes"]["2"]}
        wallet_data["safe_chains"] = ["gnosis"]
        if wallet_data["ledger_type"] == 0:
            wallet_data["ledger_type"] = "ethereum"
        
        with open(wallet_json_path, 'w') as f:
            json.dump(wallet_data, fp=f, indent=2)
        spinner.succeed("Wallet setup complete")
    
    spinner = Halo(text="Setting up agent keys...", spinner="dots").start()
    operate.keys_manager.setup()
    source_keys = MECH_PATH / "keys"
    target_keys = operate.keys_manager.path
    target_keys.mkdir(parents=True, exist_ok=True)

    for key_file in source_keys.glob("*"):
        if key_file.is_file():
            with open(key_file, 'r') as f:
                key_data = json.load(f)
            
            if key_data.get("ledger") == 0:
                key_data["ledger"] = "ethereum"
            
            target_file = target_keys / key_file.name
            with open(target_file, 'w') as f:
                json.dump(key_data, fp=f, indent=4)

    spinner.succeed("Agent keys setup complete")

    with open(OPERATE_HOME.parent / "configs" / "config_mech.json", "r") as config_file:
        service_template = json.load(config_file)

    service_template["configurations"]["gnosis"] |= {
        "staking_program_id": mech_data.staking_variables["STAKING_PROGRAM"],
        "rpc": mech_data.rpc,
        "agent_id": mech_data.staking_variables["AGENT_ID"],
        "use_staking": mech_data.use_staking,
        "cost_of_bond": mech_data.staking_variables["MIN_STAKING_BOND_OLAS"],
    }

    service_manager = operate.service_manager()
    if len(service_manager.json) == 0:
        spinner = Halo(text="Creating service...", spinner="dots").start()
        service = get_service(service_manager, service_template)
        
        # Clean up auto-generated keys and use mech keys
        keys_dir = operate.keys_manager.path
        for key_file in keys_dir.glob("*"):
            key_file.unlink()

        # Copy mech keys
        source_keys = MECH_PATH / "keys"
        for key_file in source_keys.glob("*"):
            if key_file.is_file():
                with open(key_file, 'r') as f:
                    key_data = json.load(f)
                if key_data.get("ledger") == 0:
                    key_data["ledger"] = "ethereum"
                
                target_file = keys_dir / key_file.name
                with open(target_file, 'w') as f:
                    json.dump(key_data, fp=f, indent=4)
                service.keys = [Key(**key_data)]
        service.chain_configs["gnosis"].chain_data.token = mech_data.service_id
        service.chain_configs["gnosis"].chain_data.multisig = mech_data.service_safe

        copy_data_files(service.path / "data")
        
        service.store()
        spinner.succeed("Service created")

    return get_service(service_manager, service_template)

def main() -> None:
    print_title("Mech Quickstart Migration")
    if not MECH_PATH.exists():
        print("No .mech_quickstart folder found!")
        sys.exit(1)
    
    mech_data = parse_mech_files()
    operate = OperateApp(home=OPERATE_HOME)
    service = populate_operate(operate, mech_data)
    print_section("Mech Migration complete!")

if __name__ == "__main__":
    main()