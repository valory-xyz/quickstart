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
from typing import Optional, List, Dict
import requests
import docker
from operate.services.protocol import StakingState
from operate.data import DATA_DIR
from operate.data.contracts.staking_token.contract import StakingTokenContract
from operate.operate_types import Chain, LedgerType
from operate.wallet.master import MasterWalletManager
# Import from existing test script
from test_run_service import (
    cleanup_directory,
    create_funding_handler,
    create_token_funding_handler,
    get_config_specific_settings,
    setup_logging,
    get_config_files,
    get_service_config,
    ensure_service_stopped,
    BaseTestService,
    STARTUP_WAIT,
    CONTAINER_STOP_WAIT
)

def get_included_test_configs() -> List[str]:
    """Get list of configs to test for staking-enabled services."""
    included_agents = ["trader"]  # List of agents with staking enabled
    all_configs = get_config_files()
    return [
        config for config in all_configs 
        if any(included in config.lower() for included in included_agents)
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
            return state == StakingState.STAKED
            
        except Exception as e:
            self.logger.error(f"Staking check failed: {e}")
            return False


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

    def get_staking_config_settings(self) -> dict:
        """Get config specific settings with updated staking handler."""
        settings = get_config_specific_settings(self.config_path)
        if r"Enter your choice" in settings["prompts"]:
            settings["prompts"].pop(r"Enter your choice", None)
        settings["prompts"][r"Enter your choice \(1 - \d+\):"] = self.handle_staking_choice
        return settings
    
    def handle_staking_choice(self, output: str, logger: logging.Logger) -> str:
        """Handle staking choice based on available slots."""
        try:
            parser = StakingOptionParser()
            options = parser.parse_staking_options(output, logger)
            selected = parser.select_staking_option(options, logger)
            if selected == "1":
                logger.warning("Safety check caught attempt to select option 1, forcing option 2")
                return "2"
            logger.info(f"Final choice: option {selected}")
            return selected
        except Exception as e:
            logger.error(f"Error in staking choice handler: {str(e)}")
            logger.warning("Falling back to option 2")
            return "2"
    
    @classmethod
    def setup_class(cls):
        """Setup staking-specific configuration."""
        # Save original start_service and setup flag since super() will set them
        original_start = cls.start_service
        cls.start_service = lambda: None
        
        # Don't let super() set _setup_complete
        original_setup_complete = cls._setup_complete
        super().setup_class()
        cls._setup_complete = original_setup_complete
        with open(cls.config_path) as f:
            cls.config = json.load(f)        
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
        
        # Initialize staking contract and settings
        cls.staking_contract = StakingTokenContract.from_dir(
            directory=str(DATA_DIR / "contracts" / "staking_token")
        )
        cls.config_settings = cls().get_staking_config_settings()
        cls.logger.info(f"Loaded staking settings for config: {cls.config_path}")
        
        # Restore and call original start_service
        cls.start_service = original_start
        cls.start_service()
        time.sleep(STARTUP_WAIT)
        
        # Now set setup complete
        cls._setup_complete = True

    def assert_service_stopped(self):
        """Assert that all service containers are stopped and removed."""
        try:
            self.logger.info("Verifying service is fully stopped...")
            self.stop_service()
            time.sleep(CONTAINER_STOP_WAIT)
            
            # Verify containers are stopped
            client = docker.from_env()
            service_config = get_service_config(self.config_path)
            container_name = service_config["container_name"]
            
            # Check for any containers with this name
            containers = client.containers.list(
                all=True,  # Include stopped containers
                filters={"name": container_name}
            )
            
            if containers:
                self.logger.error(f"Found {len(containers)} containers that should be stopped:")
                for container in containers:
                    self.logger.error(f"Container {container.name} - Status: {container.status}")
                assert False, f"Service containers for {container_name} still exist"
                
            self.logger.info("Service successfully stopped and containers removed")
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking stopped service: {str(e)}")
            raise

    def fast_forward_time(self, seconds: int = 86400*3):
        """
        Fast forward blockchain time based on agent's chain configuration.
        Default is 72 hours (86400*3 seconds).
        
        Args:
            seconds (int): Number of seconds to fast forward
        """
        try:
            self.logger.info(f"Fast forwarding time by {seconds} seconds...")
            
            # Get chain and RPC from agent's config
            with open(self.config_path) as f:
                config = json.load(f)
                
            # Get chain name from config
            chain_name = config.get("home_chain", "gnosis").lower()
            
            # Map chain names to environment variables for RPCs
            rpc_mapping = {
                "gnosis": "GNOSIS_RPC_URL",
                "mode": "MODIUS_RPC_URL",
                "optimism": "OPTIMISM_RPC_URL", 
                "base": "BASE_RPC_URL"
            }
            
            env_var = rpc_mapping.get(chain_name)
            if not env_var:
                raise ValueError(f"Unsupported chain: {chain_name}")
                
            rpc_url = os.getenv(env_var)
            if not rpc_url:
                raise ValueError(f"{env_var} environment variable not set")
            
            self.logger.info(f"Using RPC for chain: {chain_name}")
            
            # Prepare request payload
            headers = {"Content-Type": "application/json"}
            payload = {
                "jsonrpc": "2.0",
                "method": "evm_increaseTime",
                "params": [seconds],
                "id": 1
            }
            
            # Make request to increase time
            response = requests.post(rpc_url, headers=headers, json=payload)
            
            if response.status_code != 200:
                raise Exception(f"RPC request failed with status {response.status_code}")
                
            result = response.json()
            if 'error' in result:
                raise Exception(f"RPC error: {result['error']}")
                
            self.logger.info(f"Successfully fast forwarded time by {seconds} seconds on {chain_name} chain")
            return response.status_code == 200
            
        except Exception as e:
            self.logger.error(f"Error fast forwarding time: {str(e)}")
            raise    

    def run_termination_script(self):
        """
        Run the termination script and handle its interactive prompts.
        """
        try:
            self.logger.info("Running termination script...")
            
            # Get chain and RPC from config
            with open(self.config_path) as f:
                config = json.load(f)
            # Get chain name from config
            chain_name = config.get("home_chain", "gnosis").lower()
            
            # Map chain names to environment variables for RPCs
            rpc_mapping = {
                "gnosis": "GNOSIS_RPC_URL",
                "mode": "MODIUS_RPC_URL",
                "optimism": "OPTIMISM_RPC_URL", 
                "base": "BASE_RPC_URL"
            }
            
            env_var = rpc_mapping.get(chain_name)
            if not env_var:
                raise ValueError(f"Unsupported chain: {chain_name}")
                
            rpc_url = os.getenv(env_var)
            if not rpc_url:
                raise ValueError(f"{env_var} environment variable not set")
            
            self.logger.info(f"Using RPC for chain: {chain_name}")
            
            # Define expected prompts and responses
            prompts = {
                r"Do you want to continue\? \(yes/no\):": "yes",
                r"Enter local user account password \[hidden input\]:": os.getenv('TEST_PASSWORD', 'test_secret'),
                r"\[(?:gnosis|optimistic|base|mode)\].*Please make sure Master (EOA|Safe) .*has at least.*(?:ETH|xDAI)": 
                    lambda output, logger: create_funding_handler(rpc_url, "staking")(output, logger),
                r"\[(?:gnosis|optimistic|base|mode)\].*Please make sure Master (?:EOA|Safe) .*has at least.*(?:USDC|OLAS)":
                    lambda output, logger: create_token_funding_handler(rpc_url)(output, logger)
            }
            
            # Initialize success flag
            termination_success = False
            
            # Run termination script with output logging
            process = pexpect.spawn(
                f'bash ./terminate_on_chain_service.sh {self.config_path}',
                encoding='utf-8',
                timeout=300,
                cwd=self.temp_dir.name,
                logfile=sys.stdout
            )
            
            # Compile success pattern once
            success_pattern = re.compile(r"Service (\d+) is now terminated and unbonded.*")
            
            # Handle interactive prompts
            while True:
                try:
                    patterns = list(prompts.keys())
                    patterns.append(pexpect.EOF)
                    
                    index = process.expect(patterns)
                    
                    # Safely handle process output
                    before_output = process.before if process.before else ""
                    after_output = process.after if process.after else ""
                    current_output = str(before_output) + str(after_output)
                    
                    # Check for success message in current output
                    if success_pattern.search(current_output):
                        self.logger.info("Found termination success message!")
                        termination_success = True
                    
                    # If EOF reached, break
                    if index == len(patterns) - 1:
                        break
                        
                    # Get matching prompt and send response
                    matched_prompt = patterns[index]
                    response = prompts[matched_prompt]
                    
                    # Handle callable responses (for funding handlers)
                    if callable(response):
                        response = response(current_output, self.logger)
                        
                    process.sendline(response)
                    
                except pexpect.TIMEOUT:
                    self.logger.error("Timeout waiting for prompt")
                    return False
                    
            # Final check for success in any remaining output
            final_output = str(process.before) if process.before else ""
            if success_pattern.search(final_output):
                self.logger.info("Found termination success message in final output!")
                termination_success = True
                
            if termination_success:
                self.logger.info("Termination confirmed as successful")
                return True
                
            self.logger.error("Termination success message not found in output")
            return False
            
        except Exception as e:
            self.logger.error(f"Error running termination script: {str(e)}")
            raise
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
        get_included_test_configs(),
        indirect=True,
        ids=lambda x: Path(x).stem
    )
    def test_agent_staking(self, setup):
        """Run staking-specific tests."""
        test_instance = self.test_class()

        # Run standard health check
        test_instance.test_health_check()

        # Verify staking status after service is up
        assert verify_staking(test_instance), "Staking verification failed after service running"

        # Stop the service and verify it's stopped
        assert test_instance.assert_service_stopped(), "Service stoppage failed"

        # Fast-forward ledger time by 72 hours
        assert test_instance.fast_forward_time(), "Fast-forwading time failed"

        # Terminating the service
        assert test_instance.run_termination_script(), "Service termination failed"

        # Verify staking status after service is terminated and unstaked
        assert verify_staking(test_instance) == False, "Staking verification failed after termination"
        
        # Run shutdown logs test
        test_instance.test_shutdown_logs()

if __name__ == "__main__":
    pytest.main(["-v", __file__, "-s", "--log-cli-level=INFO"])