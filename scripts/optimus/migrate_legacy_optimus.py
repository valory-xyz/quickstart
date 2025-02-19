from dataclasses import dataclass
from getpass import getpass
from halo import Halo
import json
from pathlib import Path
import sys
import shutil
from operate.cli import OperateApp
from operate.constants import KEYS_JSON, OPERATE
from operate.keys import Key
from operate.operate_types import Chain, LedgerType
from operate.quickstart.run_service import get_service, QuickstartConfig
from operate.services.service import Service
from operate.quickstart.utils import print_section, print_title
from scripts.utils import verify_password

OPTIMUS_PATH = Path(__file__).parent.parent.parent / ".optimus"
OPERATE_HOME = Path(__file__).parent.parent.parent / OPERATE
DATA_FILES = ("current_pool.json", "gas_costs.json", "assets.json")

@dataclass
class OptimusData:
    password: str
    agent_eoa: dict
    rpc: dict[str, str]
    service_id: int
    service_safe: str
    use_staking: bool
    staking_program_id: str
    principal_chain: str
    user_provided_args: dict

def transform_to_new_format(wallet_data: dict) -> dict:
    """Transform legacy wallet format to new format for ethereum.json"""
    chain_mapping = {
        "4": "optimistic",
        "5": "base",
        "6": "mode"
    }
    
    # Convert safes to use chain names
    new_safes = {}
    for chain_id, safe_address in wallet_data.get('safes', {}).items():
        chain_name = chain_mapping.get(chain_id) or chain_mapping.get(str(chain_id))
        if chain_name:
            new_safes[chain_name] = safe_address
    
    # Convert safe_chains to use chain names
    new_safe_chains = []
    for chain_id in wallet_data.get('safe_chains', []):
        chain_name = chain_mapping.get(chain_id) or chain_mapping.get(str(chain_id))
        if chain_name:
            new_safe_chains.append(chain_name)
    new_safe_chains.sort()

    return {
        "address": wallet_data.get('address'),
        "safes": new_safes,
        "safe_chains": new_safe_chains,
        "ledger_type": "ethereum",
        "safe_nonce": wallet_data.get('safe_nonce')
    }

def parse_optimus_files() -> OptimusData:
    print_section("Parsing .optimus files")
    
    agent_key_file = next(OPTIMUS_PATH.glob("keys/*"))
    agent_key = json.loads(agent_key_file.read_text())
        
    config_file = OPTIMUS_PATH / "local_config.json"
    config = json.loads(config_file.read_text())
    use_staking = config.get("use_staking", False)

    # Map RPC endpoints from source config to new format
    rpc_mapping = {
        "optimistic": config.get("optimism_rpc"),
        "base": config.get("base_rpc"),
        "mode": config.get("mode_rpc")
    }
    # Remove any None values
    rpc = {k: v for k, v in rpc_mapping.items() if v is not None}

    staking_program_id = "optimus_alpha" if use_staking else "no_staking"

    service_dir = next(OPTIMUS_PATH.glob("services/*"))
    service_config = json.loads((service_dir / "config.json").read_text())
    
    service_id = service_config["chain_configs"]["34443"]["chain_data"]["token"]
    service_safe = service_config["chain_configs"]["34443"]["chain_data"]["multisig"]
    
    print_section("Verifying credentials...")
    password = None
    while password is None:
        password = getpass("Enter local user account password [hidden input]: ")
        if verify_password(password, OPTIMUS_PATH):
            break
        password = None
        print("Invalid password!")

    return OptimusData(
        password,
        agent_key,
        rpc,
        service_id,
        service_safe,
        use_staking,
        staking_program_id,
        "optimistic",
        {
            "TENDERLY_ACCESS_KEY": config.get("tenderly_access_key", ""),
            "TENDERLY_ACCOUNT_SLUG": config.get("tenderly_account_slug", ""),
            "TENDERLY_PROJECT_SLUG": config.get("tenderly_project_slug", ""),
            "COINGECKO_API_KEY": config.get("coingecko_api_key", "")
        }
    )

def copy_data_files(target_data_dir: Path) -> None:
    target_data_dir.mkdir(exist_ok=True)
    
    for data_file in DATA_FILES:
        src_file = OPTIMUS_PATH / data_file
        if src_file.exists():
            target_file = target_data_dir / data_file
            shutil.copy2(src_file, target_file)

def populate_operate(operate: OperateApp, optimus_data: OptimusData) -> Service:
    print_section("Setting up Operate")
    operate.setup()
    
    if operate.user_account is None:
        spinner = Halo(text="Creating user account...", spinner="dots").start()
        operate.create_user_account(optimus_data.password)
        spinner.succeed("User account created")
    else:
        operate.password = optimus_data.password

    # Setup local config
    qs_config_path = OPERATE_HOME / "local_config.json"
    if not qs_config_path.exists():
        spinner = Halo(text="Creating quickstart config...", spinner="dots").start()
        
        qs_config = QuickstartConfig(
            path=OPERATE_HOME / "local_config.json",
            password_migrated=True,
            rpc=optimus_data.rpc,
            staking_program_id=optimus_data.staking_program_id,
            principal_chain=optimus_data.principal_chain,
            user_provided_args=optimus_data.user_provided_args
        )
        
        qs_config.store()
        spinner.succeed("Quickstart config created")   

   # Setup wallet
    if not operate.wallet_manager.exists(LedgerType.ETHEREUM):
        spinner = Halo(text="Setting up wallet...", spinner="dots").start()
        operate.wallet_manager.setup()
        
        source_wallet_dir = OPTIMUS_PATH / "wallets"
        target_wallet_dir = operate.wallet_manager.path
        
        # Copy both files
        source_txt = source_wallet_dir / "ethereum.txt"
        source_json = source_wallet_dir / "ethereum.json"
        
        # Copy files if they exist
        if source_txt.exists():
            shutil.copy2(source_txt, target_wallet_dir / "ethereum.txt")
        if source_json.exists():
            shutil.copy2(source_json, target_wallet_dir / "ethereum.json")
            
            # Read and transform the json file
            with open(target_wallet_dir / "ethereum.json", 'r') as f:
                wallet_data = json.load(f)
            
            new_format = transform_to_new_format(wallet_data)
            
            # Update the json file with new format
            with open(target_wallet_dir / "ethereum.json", 'w') as f:
                json.dump(new_format, fp=f, indent=2)
        
        spinner.succeed("Wallet setup complete")
    
    # Setup agent keys
    spinner = Halo(text="Setting up agent keys...", spinner="dots").start()
    operate.keys_manager.setup()
    source_keys = OPTIMUS_PATH / "keys"
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

    # Load legacy service config
    service_source = next(OPTIMUS_PATH.glob("services/*"))
    with open(service_source / "config.json", 'r') as f:
        legacy_config = json.load(f)

    # Setup service
    with open(OPERATE_HOME.parent / "configs" / "config_optimus.json", "r") as config_file:
        service_template = json.load(config_file)

    # Map chain IDs to names
    chain_id_to_name = {
        "10": "optimistic",
        "8453": "base",
        "34443": "mode"
    }

    # Update service template with legacy config values
    for chain_id, chain_config in legacy_config["chain_configs"].items():
        chain_name = chain_id_to_name.get(chain_id)
        if chain_name and chain_name in service_template["configurations"]:
            # Update basic configuration
            service_template["configurations"][chain_name].update({
                "rpc": chain_config["ledger_config"]["rpc"],
                "staking_program_id": chain_config["chain_data"]["user_params"]["staking_program_id"],
                "agent_id":40,
                "use_staking": chain_config["chain_data"]["user_params"].get("use_staking", False),
                "cost_of_bond": chain_config["chain_data"]["user_params"]["cost_of_bond"],
            })

    service_manager = operate.service_manager()
    if len(service_manager.json) == 0:
        spinner = Halo(text="Creating service...", spinner="dots").start()
        service = get_service(service_manager, service_template)
        
        # Copy keys from legacy config
        with open(service_source / KEYS_JSON, 'r') as f:
            service_keys = json.load(f)

        with open(service.path / KEYS_JSON, "w") as f:
            json.dump(obj=service_keys, fp=f, indent=2)

        valid_addresses = {key["address"].lower() for key in service_keys}
        keys_dir = operate.keys_manager.path
        for key_file in keys_dir.glob("*"):
            if key_file.name.lower() not in valid_addresses:
                key_file.unlink()

        service.keys = [Key(**service_keys[0])]
        
        # Update chain data fields for each chain after service creation
        for chain_id, chain_config in legacy_config["chain_configs"].items():
            chain_name = chain_id_to_name.get(chain_id)
            if chain_name and chain_name in service.chain_configs:
                chain_data_fields = {
                    "instances": chain_config["chain_data"]["instances"],
                    "token": chain_config["chain_data"]["token"],
                    "multisig": chain_config["chain_data"]["multisig"],
                    "staked": chain_config["chain_data"]["staked"],
                    "on_chain_state": chain_config["chain_data"]["on_chain_state"]
                }
                
                # Update the chain data in the service configuration
                service.chain_configs[chain_name].chain_data.instances = chain_data_fields["instances"]
                service.chain_configs[chain_name].chain_data.token = chain_data_fields["token"]
                service.chain_configs[chain_name].chain_data.multisig = chain_data_fields["multisig"]
                service.chain_configs[chain_name].chain_data.staked = chain_data_fields["staked"]
                service.chain_configs[chain_name].chain_data.on_chain_state = chain_data_fields["on_chain_state"]

        copy_data_files(service.path / "data")
        
        service.store()
        spinner.succeed("Service created")

    return get_service(service_manager, service_template)

def main() -> None:
    print_title("Optimus Legacy Migration")
    if not OPTIMUS_PATH.exists():
        print("No .optimus folder found!")
        sys.exit(1)
    
    optimus_data = parse_optimus_files()
    operate = OperateApp(home=OPERATE_HOME)
    service = populate_operate(operate, optimus_data)
    print_section("Optimus Migration complete!")

if __name__ == "__main__":
    main()