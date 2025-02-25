from dataclasses import dataclass
import os
from typing import TypedDict
from dotenv import load_dotenv
from getpass import getpass
from halo import Halo
import json
from pathlib import Path
import sys

from aea_ledger_ethereum import Account, EthereumCrypto, LocalAccount
from autonomy.chain.config import ChainType
from autonomy.chain.base import registry_contracts
from operate.cli import OperateApp
from operate.constants import (
    KEYS_JSON,
    OPERATE,
    ZERO_ADDRESS,
)
from operate.keys import Key
from operate.ledger.profiles import ERC20_TOKENS
from operate.operate_types import Chain, LedgerType, OnChainState
from operate.quickstart.run_service import get_service, QuickstartConfig
from operate.services.protocol import StakingManager, StakingState
from operate.services.service import Service
from operate.quickstart.utils  import ask_yes_or_no, CHAIN_TO_METADATA, print_section, print_title
from operate.quickstart.run_service import NO_STAKING_PROGRAM_ID
from operate.utils.gnosis import get_asset_balance, get_assets_balances

TRADER_RUNNER_PATH = Path(__file__).parent.parent.parent / ".trader_runner"
OPERATE_HOME = Path(__file__).parent.parent.parent / OPERATE
DATA_FILES = (
    "available_tools_store.json",
    "checkpoint.txt",
    "multi_bets.json",
    "policy_store_multi_bet_failure_adjusting.json",
    "policy_store.json",
    "utilized_tools.json"
)


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
class TraderData:
    password: str
    agent_eoa: Path
    master_eoa: Path
    rpc: str
    service_id: int
    service_safe: str
    subgraph_api_key: str
    staking_variables: StakingVariables


def decrypt_private_keys(eoa: Path, password: str) -> dict[str, str]:
    if not password:
        private_key = "0x" + eoa.read_text()
        account: LocalAccount = Account.from_key(private_key)
        address = account.address
    else:
        crypto = EthereumCrypto(private_key_path=eoa, password=password)
        private_key = crypto.private_key
        address = crypto.address

    return {
        "address": address,
        "private_key": private_key,
        "ledger": LedgerType.ETHEREUM.value,
    }


def parse_trader_runner() -> TraderData:
    load_dotenv(TRADER_RUNNER_PATH / ".env")

    subgraph_api_key = os.getenv('SUBGRAPH_API_KEY')
    staking_variables = {
        "USE_STAKING": os.getenv('USE_STAKING').lower() == "true",
        "STAKING_PROGRAM": os.getenv('STAKING_PROGRAM'),
        "AGENT_ID": int(os.getenv('AGENT_ID')),
        "CUSTOM_SERVICE_REGISTRY_ADDRESS": os.getenv('CUSTOM_SERVICE_REGISTRY_ADDRESS'),
        "CUSTOM_SERVICE_REGISTRY_TOKEN_UTILITY_ADDRESS": os.getenv('CUSTOM_SERVICE_REGISTRY_TOKEN_UTILITY_ADDRESS'),
        "CUSTOM_OLAS_ADDRESS": os.getenv('CUSTOM_OLAS_ADDRESS'),
        "CUSTOM_STAKING_ADDRESS": os.getenv('CUSTOM_STAKING_ADDRESS'),
        "MECH_ACTIVITY_CHECKER_CONTRACT": os.getenv('MECH_ACTIVITY_CHECKER_CONTRACT'),
        "MIN_STAKING_BOND_OLAS": int(os.getenv('MIN_STAKING_BOND_OLAS')),
        "MIN_STAKING_DEPOSIT_OLAS": int(os.getenv('MIN_STAKING_DEPOSIT_OLAS'))
    }

    print_section("Parsing .trader_runner file")
    agent_eoa = TRADER_RUNNER_PATH / "agent_pkey.txt"
    print(f"Found the Agent key: {agent_eoa}")
    master_eoa = TRADER_RUNNER_PATH / "operator_pkey.txt"
    print(f"Found the Master key: {master_eoa}")
    rpc = (TRADER_RUNNER_PATH / "rpc.txt").read_text().strip()
    print(f"Found RPC: {rpc[:10]}...{rpc[-4:]}")
    service_id = int((TRADER_RUNNER_PATH / "service_id.txt").read_text().strip())
    print(f"Found Service ID: {service_id}")
    service_safe = (TRADER_RUNNER_PATH / "service_safe_address.txt").read_text().strip()
    print(f"Found Service Safe: {service_safe}")

    print_section("Decrypting private keys...")
    password = None
    while password is None:
        password = getpass("Enter local user account password [hidden input]: ")
        try:
            decrypt_private_keys(agent_eoa, password)
            decrypt_private_keys(master_eoa, password)
            break
        except ValueError:
            password = None
            print("Invalid password!")

    return TraderData(
        password,
        agent_eoa,
        master_eoa,
        rpc,
        service_id,
        service_safe,
        subgraph_api_key,
        staking_variables,
    )


def populate_operate(operate: OperateApp, trader_data: TraderData) -> Service:
    print_section("Setting up Operate")
    operate.setup()
    if operate.user_account is None:
        spinner = Halo(text="Creating user account...", spinner="dots").start()
        operate.create_user_account(trader_data.password)
        spinner.succeed("User account created")
    else:
        operate.password = trader_data.password

    qs_config_path = OPERATE_HOME / "local_config.json"
    if not qs_config_path.exists():
        spinner = Halo(text="Creating quickstart config...", spinner="dots").start()
        qs_config = QuickstartConfig(
            path=OPERATE_HOME / "local_config.json",
            password_migrated=True,
            principal_chain="gnosis",
            rpc={"gnosis": trader_data.rpc},
            user_provided_args={"SUBGRAPH_API_KEY": trader_data.subgraph_api_key},
            staking_program_id = trader_data.staking_variables["STAKING_PROGRAM"],
        )
        qs_config.store()
        spinner.succeed("Quickstart config created")

    if not operate.wallet_manager.exists(LedgerType.ETHEREUM):
        spinner = Halo(text="Creating master account...", spinner="dots").start()
        operate.wallet_manager.setup()
        master_eoa = decrypt_private_keys(trader_data.master_eoa, trader_data.password)
        with open(operate.wallet_manager.path / "ethereum.txt", "w") as f:
            json.dump(
                obj=Account.encrypt(
                    private_key=master_eoa["private_key"],
                    password=trader_data.password,
                ),
                fp=f,
                indent=2,
            )
        with open(operate.wallet_manager.path / "ethereum.json", "w") as f:
            json.dump(
                obj={
                    "address": master_eoa["address"],
                    "safes": {},
                    "safe_chains": [],
                    "ledger_type": master_eoa["ledger"],
                    "safe_nonce": None,
                },
                fp=f,
                indent=2,
            )
        spinner.succeed("Master account created")

    master_wallet = operate.wallet_manager.load(LedgerType.ETHEREUM)
    if Chain.GNOSIS not in master_wallet.safes:
        backup_owner=input("Please input your backup owner for the master safe (leave empty to skip): ")
        spinner = Halo(text="Creating master safe...", spinner="dots").start()
        master_wallet.create_safe(
            chain=Chain.GNOSIS,
            rpc=trader_data.rpc,
            backup_owner=None if backup_owner == "" else backup_owner,
        )
        spinner.succeed("Master safe created")

    agent_eoa = decrypt_private_keys(trader_data.agent_eoa, trader_data.password)
    agent_eoa_path = operate.keys_manager.path / agent_eoa["address"]
    if not agent_eoa_path.exists():
        spinner = Halo(text="Creating agent EOA...", spinner="dots").start()
        operate.keys_manager.setup()
        with open(agent_eoa_path, "w") as f:
            json.dump(obj=agent_eoa, fp=f, indent=2)
        spinner.succeed("Agent EOA created")

    with open(OPERATE_HOME.parent / "configs" / "config_predict_trader.json", "r") as config_file:
        service_template = json.load(config_file)

    service_template["configurations"][Chain.GNOSIS.value] |= {
        "staking_program_id": trader_data.staking_variables["STAKING_PROGRAM"],
        "rpc": trader_data.rpc,
        "agent_id": int(trader_data.staking_variables["AGENT_ID"]),
        "use_staking": trader_data.staking_variables["USE_STAKING"],
        "cost_of_bond": max(1, int(trader_data.staking_variables["MIN_STAKING_BOND_OLAS"])),
    }
    service_manager = operate.service_manager()
    if len(service_manager.json) == 0:
        spinner = Halo(text="Creating service...", spinner="dots").start()
        service = get_service(service_manager, service_template)

        # overwrite service config with the migrated agent EOA and service safe
        with open(service.path / KEYS_JSON, "w") as f:
            json.dump(obj=[agent_eoa], fp=f, indent=2)

        service.keys = [Key(**agent_eoa)]
        service.chain_configs[Chain.GNOSIS.value].chain_data.token = trader_data.service_id
        service.chain_configs[Chain.GNOSIS.value].chain_data.multisig = trader_data.service_safe
        service.store()
        spinner.succeed("Service created")
    
    return get_service(service_manager, service_template)


def migrate_to_master_safe(operate: OperateApp, trader_data: TraderData, service: Service) -> None:
    print_section("Migrating service to .operate")
    chain_config = service.chain_configs[service.home_chain]
    ledger_config = chain_config.ledger_config
    ocm = operate.service_manager().get_on_chain_manager(ledger_config=ledger_config)
    staking_contract = trader_data.staking_variables["CUSTOM_STAKING_ADDRESS"]
    operate.service_manager().get_on_chain_manager(ledger_config=ledger_config)
    wallet_manager = operate.wallet_manager.load(LedgerType.ETHEREUM)
    os.environ["CUSTOM_CHAIN_RPC"] = os.environ["GNOSIS_CHAIN_RPC"] = trader_data.rpc

    if (
        trader_data.staking_variables['STAKING_PROGRAM'] != NO_STAKING_PROGRAM_ID and
        ocm.staking_status(
            service_id=chain_config.chain_data.token,
            staking_contract=staking_contract,
        ) != StakingState.UNSTAKED
    ):
        if not ask_yes_or_no(
            f"Your service {chain_config.chain_data.token} will be unstaked "
            f"from staking program {trader_data.staking_variables['STAKING_PROGRAM']} during this migration.\n"
            "Do you want to continue?"
        ):
            print("Cancelled.")
            sys.exit(1)

        spinner = Halo(text=f"Unstaking service {chain_config.chain_data.token}...", spinner="dots").start()
        staking_manager = StakingManager(
            key=wallet_manager.key_path,
            chain_type=ChainType.GNOSIS,
            password=operate.password,
        )
        ts_start = staking_manager.service_info(staking_contract, chain_config.chain_data.token)[3]
        minimum_staking_duration = staking_manager.staking_ctr.get_min_staking_duration(
            ledger_api=staking_manager.ledger_api,
            contract_address=staking_contract,
        ).get("data")
        
        current_block = staking_manager.ledger_api.api.eth.get_block("latest")
        current_timestamp = current_block.timestamp
        staked_duration = current_timestamp - ts_start
        
        if staked_duration < minimum_staking_duration:
            print(
                f"Cannot unstake service {chain_config.chain_data.token}."
                f"Please try after {(minimum_staking_duration - staked_duration) / 3600:.2f} hrs."
            )
            spinner.fail("Failed to unstake service")
            sys.exit(1)
        
        ocm.unstake(service_id=chain_config.chain_data.token, staking_contract=staking_contract)
        spinner.succeed("Service unstaked")
    else:
        print(f"Service {chain_config.chain_data.token} is not staked. Skipping unstaking.")

    service_manager = operate.service_manager()
    if service_manager._get_on_chain_state(service=service, chain=service.home_chain) in (
        OnChainState.ACTIVE_REGISTRATION,
        OnChainState.FINISHED_REGISTRATION,
        OnChainState.DEPLOYED,
    ):
        spinner = Halo(text=f"Terminating service {chain_config.chain_data.token}...", spinner="dots").start()
        service_manager.terminate_service_on_chain(service_config_id=service.service_config_id)
        spinner.succeed("Service terminated")

    if (
        service_manager._get_on_chain_state(service=service, chain=service.home_chain)
        == OnChainState.TERMINATED_BONDED
    ):
        spinner = Halo(text=f"Unbonding service {chain_config.chain_data.token}...", spinner="dots").start()
        service_manager.unbond_service_on_chain(service_config_id=service.service_config_id)
        spinner.succeed("Service unbonded")

    service_owner = registry_contracts.service_registry.get_service_owner(
        ledger_api=ocm.ledger_api,
        contract_address=trader_data.staking_variables["CUSTOM_SERVICE_REGISTRY_ADDRESS"],
        service_id=chain_config.chain_data.token,
    )['service_owner']
    if service_owner != wallet_manager.safes[Chain.GNOSIS]:
        spinner = Halo(text=f"Transfering service {chain_config.chain_data.token} from master EOA to master safe...", spinner="dots").start()
        if service_owner != wallet_manager.crypto.address:
            spinner.fail(
                f"Service owner is not the master EOA. "
                f"Please ensure {wallet_manager.crypto.address} owns service {chain_config.chain_data.token} manually."
            )
            sys.exit(1)

        service_registry = registry_contracts.service_registry.get_instance(
            ledger_api=ocm.ledger_api,
            contract_address=trader_data.staking_variables["CUSTOM_SERVICE_REGISTRY_ADDRESS"],
        )
        tx = service_registry.functions.transferFrom(
            wallet_manager.crypto.address,
            wallet_manager.safes[Chain.GNOSIS],
            chain_config.chain_data.token,
        ).build_transaction({
            "from": wallet_manager.crypto.address,
            "nonce": ocm.ledger_api.api.eth.get_transaction_count(wallet_manager.crypto.address),
        })
        signed_tx = ocm.ledger_api.api.eth.account.sign_transaction(tx, private_key=ocm.crypto.private_key)
        tx_hash = ocm.ledger_api.api.eth.send_raw_transaction(signed_tx.rawTransaction)
        ocm.ledger_api.api.eth.wait_for_transaction_receipt(tx_hash)
        spinner.succeed("Service transfered from master EOA to master safe")

    assets_balances = get_assets_balances(
        ledger_api=ocm.ledger_api,
        asset_addresses={token[Chain.GNOSIS] for token in ERC20_TOKENS},
        addresses={ocm.crypto.address}
    )
    for asset, balance in assets_balances[ocm.crypto.address].items():
        if balance == 0:
            continue

        spinner = Halo(text=f"Transferring {balance} {asset} from master EOA to master safe...", spinner="dots").start()
        erc20_contract = registry_contracts.erc20.get_instance(
            ledger_api=ocm.ledger_api,
            contract_address=asset,
        )
        tx = erc20_contract.functions.transfer(
            wallet_manager.safes[Chain.GNOSIS],
            balance,
        ).build_transaction({
            "from": ocm.crypto.address,
            "nonce": ocm.ledger_api.api.eth.get_transaction_count(ocm.crypto.address),
        })
        signed_tx = ocm.ledger_api.api.eth.account.sign_transaction(tx, private_key=ocm.crypto.private_key)
        tx_hash = ocm.ledger_api.api.eth.send_raw_transaction(signed_tx.rawTransaction)
        ocm.ledger_api.api.eth.wait_for_transaction_receipt(tx_hash)
        spinner.succeed(f"{balance} {asset} transferred from master EOA to master safe.")

    xdai_balance = get_asset_balance(
        ledger_api=ocm.ledger_api,
        asset_address=ZERO_ADDRESS,
        address=ocm.crypto.address
    )
    gas_fund_requirements = CHAIN_TO_METADATA[Chain.GNOSIS.value]["gasFundReq"]
    if xdai_balance > gas_fund_requirements:
        transferable_amount = xdai_balance - gas_fund_requirements
        spinner = Halo(text=f"Transferring {transferable_amount} xDAI from master EOA to master safe...", spinner="dots").start()
        tx_hash = ocm.ledger_api.api.eth.send_transaction({
            "from": ocm.crypto.address,
            "to": wallet_manager.safes[Chain.GNOSIS],
            "value": transferable_amount
        })
        ocm.ledger_api.api.eth.get_transaction(tx_hash)
        spinner.succeed(f"{transferable_amount} XDAI transferred from master EOA to master safe.")


def main() -> None:
    print_title("Predict Trader Quickstart Migration")
    if not TRADER_RUNNER_PATH.exists():
        print("No .trader_runner file found!")
        sys.exit(1)
    
    trader_data = parse_trader_runner()
    operate = OperateApp(home=OPERATE_HOME)
    service = populate_operate(operate, trader_data)
    migrate_to_master_safe(operate, trader_data, service)
    print_section("Migration complete!")


if __name__ == "__main__":
    main()
