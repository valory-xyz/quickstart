#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------------
#
#   Copyright 2022-2023 Valory AG
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# ------------------------------------------------------------------------------

"""Obtains a report of the current service."""

import json
import math
import sys
import time
import traceback
from argparse import ArgumentParser
from collections import Counter

from enum import Enum
from pathlib import Path
from typing import Any

import docker
import requests
import scripts.predict_trader.trades as trades
from scripts.predict_trader.trades import (
    MarketAttribute,
    MarketState,
    get_balance,
    get_token_balance,
    wei_to_olas,
    wei_to_unit,
    wei_to_wxdai,
    wei_to_xdai,
)
from web3 import HTTPProvider, Web3
from web3.exceptions import ABIFunctionNotFound, ContractLogicError

from operate.constants import (
    OPERATE_HOME,
    STAKING_TOKEN_INSTANCE_ABI_PATH,
    SERVICE_REGISTRY_TOKEN_UTILITY_JSON_URL,
    MECH_ACTIVITY_CHECKER_JSON_URL,
    MECH_CONTRACT_JSON_URL,
)
from operate.cli import OperateApp
from operate.ledger.profiles import get_staking_contract
from operate.operate_types import Chain
from operate.quickstart.run_service import ask_password_if_needed, load_local_config
from operate.quickstart.utils import print_title
from scripts.utils import get_service_from_config

SCRIPT_PATH = Path(__file__).resolve().parent
SAFE_BALANCE_THRESHOLD = 500000000000000000
AGENT_XDAI_BALANCE_THRESHOLD = 50000000000000000
OPERATOR_XDAI_BALANCE_THRESHOLD = 50000000000000000
MECH_REQUESTS_PER_EPOCH_THRESHOLD = 10
TRADES_LOOKBACK_DAYS = 3
MULTI_TRADE_LOOKBACK_DAYS = TRADES_LOOKBACK_DAYS
SECONDS_PER_DAY = 60 * 60 * 24
OUTPUT_WIDTH = 80


class ColorCode:
    """Terminal color codes"""

    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    RESET = "\033[0m"


class StakingState(Enum):
    """Staking state enumeration for the staking."""

    UNSTAKED = 0
    STAKED = 1
    EVICTED = 2


def _color_string(text: str, color_code: str) -> str:
    return f"{color_code}{text}{ColorCode.RESET}"


def _color_bool(
    is_true: bool, true_string: str = "True", false_string: str = "False"
) -> str:
    if is_true:
        return _color_string(true_string, ColorCode.GREEN)
    return _color_string(false_string, ColorCode.RED)


def _color_percent(p: float, multiplier: float = 100, symbol: str = "%") -> str:
    if p >= 0:
        return f"{p*multiplier:.2f} {symbol}"
    return _color_string(f"{p*multiplier:.2f} {symbol}", ColorCode.RED)


def _trades_since_message(trades_json: dict[str, Any], utc_ts: float = 0) -> str:
    filtered_trades = [
        trade
        for trade in trades_json.get("data", {}).get("fpmmTrades", [])
        if float(trade["creationTimestamp"]) >= utc_ts
    ]
    unique_markets = set(trade["fpmm"]["id"] for trade in filtered_trades)
    trades_count = len(filtered_trades)
    markets_count = len(unique_markets)
    return f"{trades_count} trades on {markets_count} markets"


def _calculate_retrades_since(trades_json: dict[str, Any], utc_ts: float = 0) -> tuple[Counter[Any], int, int, int]:
    filtered_trades = Counter((
        trade.get("fpmm", {}).get("id", None)
        for trade in trades_json.get("data", {}).get("fpmmTrades", [])
        if float(trade.get("creationTimestamp", 0)) >= utc_ts
    ))

    if None in filtered_trades:
        raise ValueError(
            f"Unexpected format in trades_json: {filtered_trades[None]} trades have no associated market ID.")

    unique_markets = set(filtered_trades)
    n_unique_markets = len(unique_markets)
    n_trades = sum(filtered_trades.values())
    n_retrades = sum(n_bets - 1 for n_bets in filtered_trades.values() if n_bets > 1)

    return filtered_trades, n_unique_markets, n_trades, n_retrades

def _retrades_since_message(n_unique_markets: int, n_trades: int, n_retrades: int) -> str:
    return f"{n_retrades} re-trades on total {n_trades} trades in {n_unique_markets} markets"

def _average_trades_since_message(n_trades: int, n_markets: int) -> str:
    if not n_markets:
        average_trades = 0
    else:
        average_trades = round(n_trades / n_markets, 2)

    return f"{average_trades} trades per market"

def _max_trades_per_market_since_message(filtered_trades: Counter[Any]) -> str:
    if not filtered_trades:
        max_trades = 0
    else:
        max_trades = max(filtered_trades.values())

    return f"{max_trades} trades per market"


def _get_mech_requests_count(
    mech_requests: dict[str, Any], timestamp: float = 0
) -> int:
    return sum(
        1
        for mech_request in mech_requests.values()
        if mech_request.get("block_timestamp", 0) > timestamp
    )


def _print_section_header(header: str) -> None:
    print("\n\n" + header)
    print("=" * OUTPUT_WIDTH)


def _print_subsection_header(header: str) -> None:
    print("\n" + header)
    print("-" * OUTPUT_WIDTH)


def _print_status(key: str, value: str, message: str = "") -> None:
    print(f"{key:<30}{value:<10} {message or ''}")


def _warning_message(current_value: int, threshold: int = 0, message: str = "") -> str:
    default_message = _color_string(
        f"- Balance too low. Threshold is {wei_to_unit(threshold):.2f}.",
        ColorCode.YELLOW,
    )
    if current_value < threshold:
        return (
            _color_string(f"{message}", ColorCode.YELLOW)
            if message
            else default_message
        )
    return ""


def _get_agent_status() -> str:
    client = docker.from_env()
    trader_abci_container = None
    trader_tm_container = None
    for container in client.containers.list():
        if container.name.startswith("traderpearl") and container.name.endswith("abci_0"):
            trader_abci_container = container
        elif container.name.startswith("traderpearl") and container.name.endswith("tm_0"):
            trader_tm_container = container
        if trader_abci_container and trader_tm_container:
            break

    is_running = trader_abci_container and trader_tm_container
    return _color_bool(is_running, "Running", "Stopped")


def _parse_args() -> Any:
    """Parse the script arguments."""
    parser = ArgumentParser(description="Get a report for a trader service.")
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    user_args = _parse_args()

    operate_wallet_path = OPERATE_HOME / "wallets" / "ethereum.json"
    if not operate_wallet_path.exists():
        print("Operate wallet not found.")
        sys.exit(1)

    with open(operate_wallet_path) as file:
        operator_wallet_data = json.load(file)

    template_path = Path(SCRIPT_PATH.parents[1], "configs", "config_predict_trader.json")
    operate = OperateApp()
    ask_password_if_needed(operate)
    service = get_service_from_config(template_path, operate)
    config = load_local_config(operate=operate, service_name=service.name)
    chain_config = service.chain_configs["gnosis"]
    agent_address = service.keys[0].address
    master_eoa = operator_wallet_data["address"]
    if "safes" in operator_wallet_data and "gnosis" in operator_wallet_data["safes"]:
        operator_address = operator_wallet_data["safes"]["gnosis"]
    else:
        print("Operate wallet not found.")
        sys.exit(1)

    safe_address = chain_config.chain_data.multisig
    service_id = chain_config.chain_data.token
    rpc = chain_config.ledger_config.rpc

    # Prediction market trading
    mech_requests = trades.get_mech_requests(safe_address)
    mech_statistics = trades.get_mech_statistics(mech_requests)
    trades_json = trades._query_omen_xdai_subgraph(safe_address)
    _, statistics_table = trades.parse_user(
        rpc, safe_address, trades_json, mech_statistics
    )

    try:
        w3 = Web3(HTTPProvider(rpc))
        current_block_number = w3.eth.block_number

        print("")
        print_title(f"\nService report on block number {current_block_number}\n")

        # Performance
        _print_section_header(f"Performance")
        _print_subsection_header("Staking")

        staking_token_address = get_staking_contract(
            chain=Chain.GNOSIS.value,
            staking_program_id=config.staking_program_id,
        )
        if staking_token_address is None:
            is_staked = False
            staking_state = StakingState.UNSTAKED
        else:
            staking_token_data = requests.get(STAKING_TOKEN_INSTANCE_ABI_PATH).json()

            staking_token_abi = staking_token_data.get("abi", [])
            staking_token_contract = w3.eth.contract(
                address=staking_token_address, abi=staking_token_abi  # type: ignore
            )

            staking_state = StakingState(
                staking_token_contract.functions.getStakingState(
                    service_id
                ).call(block_identifier=current_block_number)
            )

            is_staked = (
                staking_state == StakingState.STAKED
                or staking_state == StakingState.EVICTED
            )

        _print_status("Is service staked?", _color_bool(is_staked, "Yes", "No"))
        if is_staked:
            _print_status("Staking program", config.staking_program_id)  # type: ignore
        if staking_state == StakingState.STAKED:
            _print_status("Staking state", staking_state.name)
        elif staking_state == StakingState.EVICTED:
            _print_status("Staking state", _color_string(staking_state.name, ColorCode.RED))

        if is_staked:

            activity_checker_address = staking_token_contract.functions.activityChecker().call(block_identifier=current_block_number)
            activity_checker_data = requests.get(MECH_ACTIVITY_CHECKER_JSON_URL).json()

            activity_checker_abi = activity_checker_data.get("abi", [])
            activity_checker_contract = w3.eth.contract(
                address=activity_checker_address, abi=activity_checker_abi  # type: ignore
            )

            service_registry_token_utility_data = requests.get(SERVICE_REGISTRY_TOKEN_UTILITY_JSON_URL).json()
            service_registry_token_utility_contract_address = (
                staking_token_contract.functions.serviceRegistryTokenUtility().call(block_identifier=current_block_number)
            )
            service_registry_token_utility_abi = (
                service_registry_token_utility_data.get("abi", [])
            )
            service_registry_token_utility_contract = w3.eth.contract(
                address=service_registry_token_utility_contract_address,
                abi=service_registry_token_utility_abi,
            )

            try:
                activity_checker_data = requests.get(MECH_CONTRACT_JSON_URL).json()
                activity_checker_abi = activity_checker_data.get("abi", [])
                mm_activity_checker_contract = w3.eth.contract(
                    address=activity_checker_address, abi=activity_checker_abi  # type: ignore
                )
                mech_contract_address = mm_activity_checker_contract.functions.mechMarketplace().call(block_identifier=current_block_number)
            except (ContractLogicError, ValueError):
                mech_contract_address = activity_checker_contract.functions.agentMech().call(block_identifier=current_block_number)

            mech_contract_abi = [
                {
                    "inputs": [
                        {
                        "internalType": "address",
                        "name": "account",
                        "type": "address"
                        }
                    ],
                    "name": function_name,
                    "outputs": [
                        {
                        "internalType": "uint256",
                        "name": "",
                        "type": "uint256"
                        }
                    ],
                    "stateMutability": "view",
                    "type": "function"
                }
                for function_name in ("mapRequestsCounts", "mapRequestCounts")
            ]
            mech_contract = w3.eth.contract(
                address=mech_contract_address, abi=mech_contract_abi   # type: ignore
            )
            try:
                mech_request_count = mech_contract.functions.mapRequestsCounts(safe_address).call(block_identifier=current_block_number)
            except (ContractLogicError, ABIFunctionNotFound, ValueError):
                # Use mapRequestCounts for newer mechs
                mech_request_count = mech_contract.functions.mapRequestCounts(safe_address).call(block_identifier=current_block_number)

            security_deposit = (
                service_registry_token_utility_contract.functions.getOperatorBalance(
                    operator_address, service_id
                ).call(block_identifier=current_block_number)
            )
            agent_id = int(staking_token_contract.functions.getAgentIds().call(block_identifier=current_block_number)[0])
            agent_bond = service_registry_token_utility_contract.functions.getAgentBond(
                service_id, agent_id
            ).call(block_identifier=current_block_number)
            min_staking_deposit = (
                staking_token_contract.functions.minStakingDeposit().call(block_identifier=current_block_number)
            )

            # In the setting 1 agent instance as of now: minOwnerBond = minStakingDeposit
            min_security_deposit = min_staking_deposit
            _print_status(
                "Staked (security deposit)",
                f"{wei_to_olas(security_deposit)} {_warning_message(security_deposit, min_security_deposit)}",
            )
            _print_status(
                "Staked (agent bond)",
                f"{wei_to_olas(agent_bond)} {_warning_message(agent_bond, min_staking_deposit)}",
            )

            service_info = staking_token_contract.functions.mapServiceInfo(
                service_id
            ).call(block_identifier=current_block_number)
            rewards = service_info[3]
            _print_status("Accrued rewards", f"{wei_to_olas(rewards)}")

            liveness_ratio = (
                activity_checker_contract.functions.livenessRatio().call(block_identifier=current_block_number)
            )
            current_timestamp = w3.eth.get_block(current_block_number).timestamp
            last_ts_checkpoint = staking_token_contract.functions.tsCheckpoint().call(block_identifier=current_block_number)
            liveness_period = (
                staking_token_contract.functions.livenessPeriod().call(block_identifier=current_block_number)
            )
            mech_requests_24h_threshold = math.ceil(
                max(liveness_period, (current_timestamp - last_ts_checkpoint))
                * liveness_ratio
                / 10 ** 18
            )

            next_checkpoint_ts = (
                staking_token_contract.functions.getNextRewardCheckpointTimestamp().call(block_identifier=current_block_number)
            )
            last_checkpoint_ts = next_checkpoint_ts - liveness_period

            mech_request_count_on_last_checkpoint = (
                staking_token_contract.functions.getServiceInfo(service_id).call(block_identifier=current_block_number)
            )[2][1]
            mech_requests_since_last_cp = mech_request_count - mech_request_count_on_last_checkpoint
            # mech_requests_current_epoch = _get_mech_requests_count(
            #     mech_requests, last_checkpoint_ts
            # )
            mech_requests_current_epoch = mech_requests_since_last_cp
            _print_status(
                "Num. Mech txs current epoch",
                f"{mech_requests_current_epoch} {_warning_message(mech_requests_current_epoch, mech_requests_24h_threshold, f'- Too low. Threshold is {mech_requests_24h_threshold}.')}",
            )

    except Exception:  # pylint: disable=broad-except
        traceback.print_exc()
        print("An error occurred while interacting with the staking contract.")

    _print_subsection_header("Prediction market trading")
    _print_status(
        "ROI on closed markets",
        _color_percent(statistics_table[MarketAttribute.ROI][MarketState.CLOSED]),
    )

    since_ts = time.time() - SECONDS_PER_DAY * TRADES_LOOKBACK_DAYS
    _print_status(
        f"Trades on last {TRADES_LOOKBACK_DAYS} days",
        _trades_since_message(trades_json, since_ts),
    )

    #Multi trade strategy
    retrades_since_ts = time.time() - SECONDS_PER_DAY * MULTI_TRADE_LOOKBACK_DAYS
    filtered_trades, n_unique_markets, n_trades, n_retrades = _calculate_retrades_since(trades_json, retrades_since_ts)
    _print_subsection_header(f"Multi-trade markets in previous {MULTI_TRADE_LOOKBACK_DAYS} days")
    _print_status(f"Multi-trade markets", _retrades_since_message(n_unique_markets, n_trades, n_retrades))
    _print_status(f"Average trades per market", _average_trades_since_message(n_trades, n_unique_markets))
    _print_status(f"Max trades per market", _max_trades_per_market_since_message(filtered_trades))

    # Service
    _print_section_header("Service")
    _print_status("ID", str(service_id))

    # Agent
    agent_status = _get_agent_status()
    agent_xdai = get_balance(agent_address, rpc, block_identifier=current_block_number)
    _print_subsection_header("Agent")
    _print_status("Status (on this machine)", agent_status)
    _print_status("Address", agent_address)
    _print_status(
        "xDAI Balance",
        f"{wei_to_xdai(agent_xdai)} {_warning_message(agent_xdai, AGENT_XDAI_BALANCE_THRESHOLD)}",
    )

    # Safe
    safe_xdai = get_balance(safe_address, rpc, block_identifier=current_block_number)
    safe_wxdai = get_token_balance(safe_address, trades.WXDAI_CONTRACT_ADDRESS, rpc, block_identifier=current_block_number)
    _print_subsection_header(
        f"Safe {_warning_message(safe_xdai + safe_wxdai, SAFE_BALANCE_THRESHOLD)}"
    )
    _print_status("Address", safe_address)
    _print_status("xDAI Balance", wei_to_xdai(safe_xdai))
    _print_status("WxDAI Balance", wei_to_wxdai(safe_wxdai))

    # Master Safe - Agent Owner/Operator
    operator_xdai = get_balance(operator_address, rpc, block_identifier=current_block_number)
    _print_subsection_header("Master Safe - Agent Owner/Operator")
    _print_status("Address", operator_address)
    _print_status(
        "xDAI Balance",
        f"{wei_to_xdai(operator_xdai)} {_warning_message(operator_xdai, OPERATOR_XDAI_BALANCE_THRESHOLD)}",
    )

    # Master EOA - Master Safe Owner
    master_eoa_xdai = get_balance(master_eoa, rpc, block_identifier=current_block_number)
    _print_subsection_header("Master EOA - Master Safe Owner")
    _print_status("Address", master_eoa)
    _print_status(
        "xDAI Balance",
        f"{wei_to_xdai(master_eoa_xdai)} {_warning_message(master_eoa_xdai, OPERATOR_XDAI_BALANCE_THRESHOLD)}",
    )
    print("")
