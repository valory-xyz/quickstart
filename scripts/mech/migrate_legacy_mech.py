from pathlib import Path
import json
import shutil
from dataclasses import dataclass
from operate.quickstart.run_service import QuickstartConfig
from operate.operate_types import Chain
from operate.quickstart.utils import print_section, print_title
from scripts.utils import validate_config_params

MECH_PATH = Path(__file__).parent.parent.parent / ".mech_quickstart"
OPERATE_HOME = Path(__file__).parent.parent.parent / ".operate"

@dataclass
class MechConfig:
    rpc: str
    mech_type: str
    api_keys: str
    use_staking: bool
    staking_program_id: str

def find_api_keys_file(base_paths: list[Path], filename: str) -> Path | None:
    """Search for API keys file in multiple locations."""
    for base_path in base_paths:
        for name in [filename, f"{filename}.json"]:
            file_path = base_path / name
            if file_path.exists():
                return file_path
    return None

def find_and_load_api_keys(config: dict) -> str:
    """Find and load API keys file from config path."""
    api_keys_path = config.get("api_keys_path")
    if not api_keys_path:
        raise ValueError("No API keys path specified in config")
    
    filename = Path(api_keys_path).name
    search_paths = [
        Path.cwd(),
        MECH_PATH,
        MECH_PATH.parent,
        Path(__file__).parent,
        Path(__file__).parent.parent
    ]
    
    api_keys_file = find_api_keys_file(search_paths, filename)
    if not api_keys_file:
        raise ValueError(f"Could not find API keys file '{filename}' in any of the search paths: {[str(p) for p in search_paths]}")
        
    try:
        api_keys = json.loads(api_keys_file.read_text())
        print(f"Loaded API keys from: {api_keys_file}")
        return json.dumps(api_keys)
    except Exception as e:
        raise ValueError(f"Error loading API keys: {e}")

def parse_mech_config() -> MechConfig:
    """Parse essential config data from mech quickstart."""
    print_section("Parsing .mech_quickstart configuration...")
    
    config_file = MECH_PATH / "local_config.json"
    config = json.loads(config_file.read_text())

    # Validate required parameters
    required_params = [
        "gnosis_rpc",
        "mech_type"
    ]
    validate_config_params(config, required_params)
    
    use_staking = config.get("use_staking", False)
    staking_program_id = "mech_marketplace" if use_staking else "no_staking"
    
    return MechConfig(
        rpc=config.get("gnosis_rpc"),
        mech_type=config.get("mech_type"),
        api_keys=find_and_load_api_keys(config),
        use_staking=use_staking,
        staking_program_id=staking_program_id
    )

def copy_mech_to_operate():
    """Copy all files from .mech_quickstart to .operate."""
    print_section("Copying files from .mech_quickstart to .operate...")
    
    # Create .operate directory if it doesn't exist
    OPERATE_HOME.mkdir(parents=True, exist_ok=True)
    
    # Copy all contents except local_config.json
    for item in MECH_PATH.iterdir():
        if item.name != "local_config.json":
            target = OPERATE_HOME / item.name
            if item.is_dir():
                shutil.copytree(item, target, dirs_exist_ok=True)
            else:
                shutil.copy2(item, target)

def create_operate_config(mech_config: MechConfig):
    """Create new local_config.json for operate using QuickstartConfig."""
    print_section("Creating new operate configuration...")
    
    qs_config = QuickstartConfig(
        path=OPERATE_HOME / "local_config.json",
        password_migrated=True,
        principal_chain=Chain.GNOSIS.value,
        rpc={Chain.GNOSIS.value: mech_config.rpc},
        user_provided_args={
            "API_KEYS": mech_config.api_keys,
            "MECH_TYPE": mech_config.mech_type
        },
        staking_program_id=mech_config.staking_program_id,
    )
    qs_config.store()

def main():
    print_title("Mech Quickstart Migration")
    
    if not MECH_PATH.exists():
        print("Error: No .mech_quickstart folder found!")
        return
    
    try:
        # Parse mech config first
        mech_config = parse_mech_config()
        
        # Copy all files
        copy_mech_to_operate()
        
        # Create new config
        create_operate_config(mech_config)
        
        print_section("Migration completed successfully!")
        
    except Exception as e:
        print(f"Error during migration: {e}")
        raise

if __name__ == "__main__":
    main()