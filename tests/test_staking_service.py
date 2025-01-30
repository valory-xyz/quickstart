import re
import sys
import logging
import pexpect 
import os
import json
import time
import pytest
import tempfile
import shutil
import typing as t
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from web3 import Web3
from eth_account import Account
import requests
import docker
from dotenv import load_dotenv

from operate.services.protocol import StakingState
from operate.data import DATA_DIR
from operate.data.contracts.staking_token.contract import StakingTokenContract
from operate.operate_types import Chain, LedgerType
from operate.wallet.master import MasterWalletManager

# Import from existing test script
from test_run_service import (
    cleanup_directory, get_config_specific_settings, setup_logging, get_config_files,
    validate_backup_owner, handle_env_var_prompt, get_service_config, check_docker_status,
    check_service_health, check_shutdown_logs, ensure_service_stopped,
    BaseTestService, ColoredFormatter, STARTUP_WAIT, SERVICE_INIT_WAIT, CONTAINER_STOP_WAIT
)

def get_test_configs(excluded_agents: List[str] = None) -> List[str]:
    """Get list of configs to test, excluding specified agents."""
    if excluded_agents is None:
        excluded_agents = []
        
    all_configs = get_config_files()
    return [
        config for config in all_configs 
        if not any(excluded in config.lower() for excluded in excluded_agents)
    ]

class StakingOptionParser:
    """Parser that selects options based on available slots."""
    
    @staticmethod
    def parse_staking_options(output: str, logger: logging.Logger) -> List[Dict]:
        """Parse staking options and their available slots from CLI output."""
        options = []
        logger.info("Starting to parse staking options...")
        
        # Pattern to match option lines with slots
        pattern = r'(\d+)\)\s*(.*?)\s*\(available slots\s*:\s*([∞\d]+)\)'
        matches = re.finditer(pattern, output)
        
        for match in matches:
            number = int(match.group(1))
            name = match.group(2).strip()
            slots_str = match.group(3)
            slots = float('inf') if slots_str == '∞' else int(slots_str)
            
            option = {
                'number': number,
                'name': name,
                'slots': slots
            }
            options.append(option)
            logger.debug(f"Found option: {option}")
            
        logger.info(f"Found {len(options)} staking options")
        return options

    @staticmethod
    def select_staking_option(options: List[Dict], logger: logging.Logger) -> str:
        """Select option with maximum available slots, never selecting option 1."""
        if not options:
            logger.warning("No options parsed, defaulting to option 2")
            return "2"
        
        # Filter out option 1 and options with no slots
        valid_options = [
            opt for opt in options 
            if opt['number'] != 1 and opt['slots'] > 0
        ]
        
        if not valid_options:
            logger.warning("No valid options with available slots, defaulting to option 2")
            return "2"
        
        # Select option with maximum available slots
        selected = max(valid_options, key=lambda x: x['slots'])
        logger.info(f"Selected option {selected['number']}: {selected['name']} with {selected['slots']} slots")
        
        return str(selected['number'])

def handle_staking_choice(output: str, logger: logging.Logger) -> str:
    """Handle staking choice based on available slots."""
    try:
        parser = StakingOptionParser()
        options = parser.parse_staking_options(output, logger)
        selected = parser.select_staking_option(options, logger)
        
        # Final safety check
        if selected == "1":
            logger.warning("Safety check caught attempt to select option 1, forcing option 2")
            return "2"
        
        logger.info(f"Final choice: option {selected}")
        return selected
        
    except Exception as e:
        logger.error(f"Error in staking choice handler: {str(e)}")
        logger.warning("Falling back to option 2")
        return "2"

def get_staking_config_settings(config_path: str) -> dict:
    """Get config specific settings with updated staking handler."""
    
    # Get original settings from main test script
    settings = get_config_specific_settings(config_path)
    
    # Remove any default staking choice patterns if they exist
    if r"Enter your choice" in settings["prompts"]:
        settings["prompts"].pop(r"Enter your choice", None)
    
    # Add our custom staking handler with highest priority
    settings["prompts"][r"Enter your choice \(1 - \d+\):"] = handle_staking_choice
    
    return settings

class StakingStatusChecker:
    """Handles checking staking status and service state."""
    
    def __init__(self, config_path: str, logger: logging.Logger):
        """Initialize the checker with config and logger."""
        self.logger = logger
        self.config_path = Path(config_path)
        self.config = self._load_config()
        
        # Initialize chain
        self.chain_name = self.config.get("home_chain", "gnosis")
        self.chain = Chain[self.chain_name.upper()]
        temp_data = Chain[self.chain_name.upper()]
        logger.info(f"Chain[self.chain_name.upper()] :{temp_data}")
        
        # Get .operate directory from the current working directory
        # since we're already chdir'd to the temp directory in setup
        operate_dir = Path(os.getcwd()) / ".operate"
        keys_dir = operate_dir / "keys"
        
        if not keys_dir.exists():
            raise RuntimeError(f"Keys directory not found at {keys_dir}")
        
        # Load local config to get RPC URL
        local_config_path = operate_dir / "local_config.json"
        try:
            with open(local_config_path) as f:
                local_config = json.load(f)
                rpc_url = local_config.get("rpc", {}).get(self.chain_name)
        except Exception as e:
            self.logger.error(f"Failed to load RPC URL from local config: {e}")
            rpc_url = None
        
        # Setup wallet manager and get ledger
        self.wallet_manager = MasterWalletManager(
            path=keys_dir,
            password=os.getenv("MASTER_WALLET_PASSWORD", "DUMMY_PWD"),
            logger=self.logger
        ).setup()
        
        self.master_wallet = self.wallet_manager.load(ledger_type=LedgerType.ETHEREUM)
        self.ledger_api = self.master_wallet.ledger_api(chain=self.chain, rpc=rpc_url)
        
        # Initialize staking contract
        self.staking_contract = t.cast(
            StakingTokenContract,
            StakingTokenContract.from_dir(
                directory=str(DATA_DIR / "contracts" / "staking_token")
            ),
        )
        
    def _load_config(self) -> dict:
        """Load and parse the agent config file."""
        try:
            with open(self.config_path) as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load config: {e}")
            raise
            
    def get_staking_state(self, service_id: int, staking_address: str) -> StakingState:
        """Get the current staking state for a service."""
        try:  
            # Get staking state using ledger API
            state = StakingState(
                self.staking_contract.get_instance(
                    ledger_api=self.ledger_api,
                    contract_address=staking_address,
                )
                .functions.getStakingState(service_id)
                .call()
            )
            
            self.logger.info(f"Got staking state for service {service_id}: {state}")
            return state
            
        except Exception as e:
            self.logger.error(f"Failed to get staking state: {e}")
            raise
            
    def check_service_staking(self, service_id: int, staking_address: str) -> bool:
        """Check if service is properly staked."""
        try:
            state = self.get_staking_state(service_id, staking_address)
            
            # Validate staking state
            is_staked = state == StakingState.STAKED
            
            self.logger.info(
                f"Service {service_id} staking check:\n"
                f"Status: {'Passed' if is_staked else 'Failed'}"
            )
            return is_staked
            
        except Exception as e:
            self.logger.error(f"Staking check failed: {e}")
            return False

def get_service_token(test_instance) -> Optional[int]:
    """Get service token from runtime config."""
    try:
        # Find the service config directory
        services_dir = Path(os.getcwd()) / ".operate" / "services"
        if not services_dir.exists():
            test_instance.logger.error(f"Services directory not found at {services_dir}")
            return None
            
        # Find service directory by looking for config.json
        for service_dir in services_dir.iterdir():
            config_file = service_dir / "config.json"
            if config_file.exists():
                with open(config_file) as f:
                    runtime_config = json.load(f)
                
                # Get token from chain_data
                chain_name = runtime_config.get("home_chain", "gnosis")
                chain_config = runtime_config.get("chain_configs", {}).get(chain_name, {})
                chain_data = chain_config.get("chain_data", {})
                token = chain_data.get("token")
                
                if token:
                    test_instance.logger.info(f"Found service token: {token}")
                    return token
                    
        test_instance.logger.error("No service token found in runtime config")
        return None
        
    except Exception as e:
        test_instance.logger.error(f"Error getting service token: {e}")
        return None

def verify_staking(test_instance) -> bool:
    """Verify staking status for a service."""
    checker = StakingStatusChecker(test_instance.config_path, test_instance.logger)
    
    try:
        # Find and load runtime config
        services_dir = Path(os.getcwd()) / ".operate" / "services"
        runtime_config = None
        
        for service_dir in services_dir.iterdir():
            config_file = service_dir / "config.json"
            if config_file.exists():
                with open(config_file) as f:
                    runtime_config = json.load(f)
                break
                
        if not runtime_config:
            test_instance.logger.error("No runtime config found")
            return False
        
        # Get chain data from runtime config
        chain_name = runtime_config.get("home_chain", "gnosis")
        chain_data = runtime_config.get("chain_configs", {}).get(chain_name, {}).get("chain_data", {})
        
        # Get service token and staking program
        service_token = chain_data.get("token")
        staking_program_id = chain_data.get("user_params", {}).get("staking_program_id")
        
        if not service_token:
            test_instance.logger.error("Could not find service token")
            return False
            
        if not staking_program_id or staking_program_id == "no_staking":
            test_instance.logger.info("Service is not using staking")
            return True
            
        # Get staking contract address
        staking_address = test_instance.config.get("staking_programs", {}).get(staking_program_id)
        if not staking_address:
            test_instance.logger.error(f"Could not find staking contract for program: {staking_program_id}")
            return False
        
        # Check staking status
        test_instance.logger.info(f"Checking staking for token {service_token} on {staking_program_id} with {staking_address}")
        return checker.check_service_staking(service_token, staking_address)
        
    except Exception as e:
        test_instance.logger.error(f"Error checking staking status: {e}")
        return False

class StakingBaseTestService(BaseTestService):
    """Extended base test service with staking-specific configuration."""
    
    @classmethod
    def setup_class(cls):
        """Override setup to ensure staking config is properly initialized."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        cls.log_file = Path(f'test_staking_{timestamp}.log')
        cls.logger = setup_logging(cls.log_file)
            
        # Create temporary directory and store original path
        cls.original_cwd = os.getcwd()
        cls.temp_dir = tempfile.TemporaryDirectory(prefix='staking_test_')
        cls.logger.info(f"Created temporary directory: {cls.temp_dir.name}")
        
        # Copy project files with exclusions
        exclude_patterns = ['.git', '.pytest_cache', '__pycache__', '*.pyc', 'logs', '*.log']
        def ignore_patterns(path, names):
            return set(n for n in names if any(p in n or any(p.endswith(n) for p in exclude_patterns) for p in exclude_patterns))
        
        shutil.copytree(cls.original_cwd, cls.temp_dir.name, dirs_exist_ok=True, ignore=ignore_patterns)
        # Copy .git directory if it exists
        git_dir = Path(cls.original_cwd) / '.git'
        if git_dir.exists():
            shutil.copytree(git_dir, Path(cls.temp_dir.name) / '.git', symlinks=True)    
                    
        # Switch to temporary directory
        os.chdir(cls.temp_dir.name)
        cls.logger.info(f"Changed working directory to: {cls.temp_dir.name}")
        
        # Load config
        with open(cls.config_path) as f:
            cls.config = json.load(f)
        
        # Setup environment
        cls._setup_environment()
        
        # Setup .operate directory and wallet
        operate_dir = Path(cls.temp_dir.name) / ".operate"
        operate_dir.mkdir(exist_ok=True)
        keys_dir = operate_dir / "keys"
        keys_dir.mkdir(exist_ok=True)
        
        # Initialize wallet manager
        cls.wallet_manager = MasterWalletManager(
            path=keys_dir,
            password="DUMMY_PWD",  # Use env var in production
            logger=cls.logger
        ).setup()
        
        # Create wallet if it doesn't exist
        if not cls.wallet_manager.exists(LedgerType.ETHEREUM):
            cls.logger.info("Creating new Ethereum wallet...")
            cls.wallet_manager.create(LedgerType.ETHEREUM)
        
        # Important: Load staking-specific settings with staking handler
        cls.config_settings = get_staking_config_settings(cls.config_path)
        cls.logger.info(f"Loaded staking settings for config: {cls.config_path}")
        
        # Start the service
        cls.start_service()
        time.sleep(STARTUP_WAIT)
        
        cls._setup_complete = True

    # @classmethod
    # def teardown_class(cls):
    #     """Override teardown to ensure proper cleanup."""
    #     try:
    #         if hasattr(cls, '_setup_complete') and cls._setup_complete:
    #             # Stop the service
    #             cls.stop_service()
    #             time.sleep(CONTAINER_STOP_WAIT)
                
    #             # Change back to original directory
    #             os.chdir(cls.original_cwd)
                
    #             # Cleanup temporary directory
    #             if hasattr(cls, 'temp_dir'):
    #                 cls.temp_dir.cleanup()
                    
    #     except Exception as e:
    #         cls.logger.error(f"Error in teardown: {str(e)}")
    #         raise

class TestAgentStaking:
    """Test class for staking-specific tests."""
    
    logger = setup_logging(Path(f'test_staking_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'))

    @pytest.fixture(autouse=True)
    def setup(self, request):
        """Setup with staking-specific configuration."""
        # Set environment variable for wallet password
        os.environ["MASTER_WALLET_PASSWORD"] = "DUMMY_PWD"
        config_path = request.param
        temp_dir = None

        try:
            temp_dir = tempfile.TemporaryDirectory(prefix='staking_test_')
            shutil.copytree('.', temp_dir.name, dirs_exist_ok=True, 
                          ignore=shutil.ignore_patterns('.git', '.pytest_cache', '__pycache__', 
                                                      '*.pyc', 'logs', '*.log', '.env'))
            
            if not ensure_service_stopped(config_path, temp_dir.name, self.logger):
                raise RuntimeError("Failed to stop existing service")
            
            # Create test class with staking-specific base
            self.test_class = type(
                f'TestStakingService_{Path(config_path).stem}',
                (StakingBaseTestService,),  # Use our staking-specific base
                {
                    'config_path': config_path,
                }
            )
            
            self.test_class.setup_class()
            yield
            
            if self.test_class._setup_complete:
                self.test_class.teardown_class()
                
        finally:
            if temp_dir:
                temp_dir_path = temp_dir.name
                try:
                    temp_dir.cleanup()
                except Exception:
                    self.logger.warning("Built-in cleanup failed, trying custom cleanup...")
                    cleanup_directory(temp_dir_path, self.logger)

    @pytest.mark.parametrize(
        'setup',
        get_test_configs(excluded_agents=["mech", "meme", "optimus", "modius"]),
        indirect=True,
        ids=lambda x: Path(x).stem
    )
    def test_agent_staking(self, setup):
        """Run staking-specific tests."""
        test_instance = self.test_class()

        # Run standard health check
        test_instance.test_health_check()

         # Verify staking status
        assert verify_staking(test_instance), "Staking verification failed"
        
        
        # Additional staking-specific tests will go here
        # TODO: Add time fast-forward
        # TODO: Add termination script
        # TODO: Add rewards claiming
        # TODO: Add staking reset
        # TODO: Add unstaking verification
        
        # Run shutdown logs test
        test_instance.test_shutdown_logs()

if __name__ == "__main__":
    pytest.main(["-v", __file__, "-s", "--log-cli-level=INFO"])