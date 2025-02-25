from pathlib import Path
import json
import shutil
from dataclasses import dataclass
from operate.quickstart.run_service import QuickstartConfig
from operate.operate_types import Chain
from operate.quickstart.utils import print_section, print_title
from scripts.utils import validate_config_params

OPTIMUS_PATH = Path(__file__).parent.parent.parent / ".optimus"
OPERATE_HOME = Path(__file__).parent.parent.parent / ".operate"

@dataclass
class OptimusConfig:
    rpc: dict[str, str]
    tenderly_access_key: str
    tenderly_account_slug: str
    tenderly_project_slug: str
    coingecko_api_key: str
    use_staking: bool
    staking_program_id: str
    principal_chain: str

def parse_optimus_config() -> OptimusConfig:
    """Parse essential config data from optimus."""
    print_section("Parsing .optimus configuration...")
    
    config_file = OPTIMUS_PATH / "local_config.json"
    config = json.loads(config_file.read_text())
    
    # Validate required parameters
    required_params = [
        "tenderly_access_key",
        "tenderly_account_slug", 
        "tenderly_project_slug",
        "coingecko_api_key",
        "optimism_rpc",
        "base_rpc",
        "mode_rpc"
    ]
    validate_config_params(config, required_params)
    
    # Map RPC endpoints from source config to new format
    rpc_mapping = {
        Chain.OPTIMISTIC.value : config.get("optimism_rpc"),
        Chain.BASE.value: config.get("base_rpc"),
        Chain.MODE.value: config.get("mode_rpc")
    }
    
    use_staking = config.get("use_staking", False)
    staking_program_id = "optimus_alpha" if use_staking else "no_staking"
    
    # Default to optimistic if available, otherwise first available chain
    principal_chain = Chain.OPTIMISTIC.value if Chain.OPTIMISTIC.value in rpc_mapping else next(iter(rpc_mapping), Chain.OPTIMISTIC.value)
    
    return OptimusConfig(
        rpc=rpc_mapping,
        tenderly_access_key=config["tenderly_access_key"],
        tenderly_account_slug=config["tenderly_account_slug"],
        tenderly_project_slug=config["tenderly_project_slug"],
        coingecko_api_key=config["coingecko_api_key"],
        use_staking=use_staking,
        staking_program_id=staking_program_id,
        principal_chain=principal_chain
    )

def copy_optimus_to_operate():
    """Copy all files from .optimus to .operate."""
    print_section("Copying files from .optimus to .operate...")
    
    # Create .operate directory if it doesn't exist
    OPERATE_HOME.mkdir(parents=True, exist_ok=True)
    
    # Copy all contents except local_config.json
    for item in OPTIMUS_PATH.iterdir():
        if item.name != "local_config.json":
            target = OPERATE_HOME / item.name
            if item.is_dir():
                shutil.copytree(item, target, dirs_exist_ok=True)
            else:
                shutil.copy2(item, target)

def create_operate_config(optimus_config: OptimusConfig):
    """Create new local_config.json for operate using QuickstartConfig."""
    print_section("Creating new operate configuration...")
    
    qs_config = QuickstartConfig(
        path=OPERATE_HOME / "local_config.json",
        password_migrated=True,
        principal_chain=optimus_config.principal_chain,
        rpc=optimus_config.rpc,
        user_provided_args={
            "TENDERLY_ACCESS_KEY": optimus_config.tenderly_access_key,
            "TENDERLY_ACCOUNT_SLUG": optimus_config.tenderly_account_slug,
            "TENDERLY_PROJECT_SLUG": optimus_config.tenderly_project_slug,
            "COINGECKO_API_KEY": optimus_config.coingecko_api_key
        },
        staking_program_id=optimus_config.staking_program_id,
    )
    qs_config.store()

def main():
    print_title("Optimus to Operate Migration")
    
    if not OPTIMUS_PATH.exists():
        print("Error: No .optimus folder found!")
        return
    
    try:
        # Parse optimus config first
        optimus_config = parse_optimus_config()
        
        # Copy all files
        copy_optimus_to_operate()
        
        # Create new config
        create_operate_config(optimus_config)
        
        print_section("Migration completed successfully!")
        
    except Exception as e:
        print(f"Error during migration: {e}")
        raise

if __name__ == "__main__":
    main()