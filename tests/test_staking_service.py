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
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from web3 import Web3
from eth_account import Account
import requests
import docker
from dotenv import load_dotenv

# Import from existing test script
from test_run_service import (
    cleanup_directory, get_config_specific_settings, setup_logging, get_config_files, validate_backup_owner,
    handle_env_var_prompt, get_service_config, check_docker_status,
    check_service_health, check_shutdown_logs, ensure_service_stopped,
    handle_native_funding, handle_erc20_funding, create_funding_handler,
    create_token_funding_handler, BaseTestService, ColoredFormatter,
    STARTUP_WAIT, SERVICE_INIT_WAIT, CONTAINER_STOP_WAIT
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
        exclude_patterns = ['.git', '.pytest_cache', '__pycache__', '*.pyc', 'logs', '*.log', '.env']
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
        
        # Setup environment
        cls._setup_environment()

        # Important: Load staking-specific settings
        cls.config_settings = get_staking_config_settings(cls.config_path)
        cls.logger.info(f"Loaded staking settings for config: {cls.config_path}")
        
        # Start the service
        cls.start_service()
        time.sleep(STARTUP_WAIT)
        
        cls._setup_complete = True

class TestAgentStaking:
    """Test class for staking-specific tests."""
    
    logger = setup_logging(Path(f'test_staking_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'))

    @pytest.fixture(autouse=True)
    def setup(self, request):
        """Setup with staking-specific configuration."""
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
        
        # Additional staking-specific tests will go here
        # TODO: Add staking verification
        # TODO: Add time fast-forward
        # TODO: Add termination script
        # TODO: Add rewards claiming
        # TODO: Add staking reset
        # TODO: Add unstaking verification
        
        # Run shutdown logs test
        test_instance.test_shutdown_logs()

if __name__ == "__main__":
    pytest.main(["-v", __file__, "-s", "--log-cli-level=INFO"])