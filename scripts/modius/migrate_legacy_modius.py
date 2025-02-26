from pathlib import Path
import json
import shutil
from dataclasses import dataclass
from operate.quickstart.run_service import QuickstartConfig
from operate.operate_types import Chain
from operate.quickstart.utils import print_section, print_title
from scripts.utils import validate_config_params

# Modified paths for Modius
MODIUS_PATH = Path(__file__).parent.parent.parent / ".olas-modius"
OPERATE_HOME = Path(__file__).parent.parent.parent / ".operate"

@dataclass
class ModiusConfig:
    rpc: str
    tenderly_access_key: str
    tenderly_account_slug: str
    tenderly_project_slug: str
    coingecko_api_key: str
    use_staking: bool
    staking_program_id: str

def parse_modius_config() -> ModiusConfig:
    """Parse essential config data from modius quickstart."""
    print_section("Parsing .olas-modius configuration...")
    
    config_file = MODIUS_PATH / "local_config.json"
    config = json.loads(config_file.read_text())
    
    # Validate required parameters
    required_params = [
        "tenderly_access_key",
        "tenderly_account_slug", 
        "tenderly_project_slug",
        "coingecko_api_key",
        "mode_rpc"
    ]
    validate_config_params(config, required_params)
    
    use_staking = config.get("use_staking", False)
    staking_program_id = "optimus_alpha" if use_staking else "no_staking"
    
    return ModiusConfig(
        rpc=config.get("mode_rpc"),
        tenderly_access_key=config["tenderly_access_key"],
        tenderly_account_slug=config["tenderly_account_slug"],
        tenderly_project_slug=config["tenderly_project_slug"],
        coingecko_api_key=config["coingecko_api_key"],
        use_staking=use_staking,
        staking_program_id=staking_program_id
    )

def copy_modius_to_operate():
    """Copy all files from .olas-modius to .operate."""
    print_section("Copying files from .olas-modius to .operate...")
    
    # Create .operate directory if it doesn't exist
    OPERATE_HOME.mkdir(parents=True, exist_ok=True)
    
    # Copy all contents except local_config.json
    for item in MODIUS_PATH.iterdir():
        if item.name != "local_config.json":
            target = OPERATE_HOME / item.name
            if item.is_dir():
                shutil.copytree(item, target, dirs_exist_ok=True)
            else:
                shutil.copy2(item, target)

def create_operate_config(modius_config: ModiusConfig):
    """Create new local_config.json for operate using QuickstartConfig."""
    print_section("Creating new operate configuration...")
    
    qs_config = QuickstartConfig(
        path=OPERATE_HOME / "local_config.json",
        password_migrated=True,
        principal_chain=Chain.MODE.value,
        rpc={Chain.MODE.value: modius_config.rpc},
        user_provided_args={
            "TENDERLY_ACCESS_KEY": modius_config.tenderly_access_key,
            "TENDERLY_ACCOUNT_SLUG": modius_config.tenderly_account_slug,
            "TENDERLY_PROJECT_SLUG": modius_config.tenderly_project_slug,
            "COINGECKO_API_KEY": modius_config.coingecko_api_key
        },
        staking_program_id=modius_config.staking_program_id,
    )
    qs_config.store()

def main():
    print_title("Modius Quickstart Migration")
    
    if not MODIUS_PATH.exists():
        print("Error: No .olas-modius folder found!")
        return
    
    try:
        # Parse modius config first
        modius_config = parse_modius_config()
        
        # Copy all files
        copy_modius_to_operate()
        
        # Create new config
        create_operate_config(modius_config)
        
        print_section("Migration completed successfully!")
        
    except Exception as e:
        print(f"Error during migration: {e}")
        raise

if __name__ == "__main__":
    main()