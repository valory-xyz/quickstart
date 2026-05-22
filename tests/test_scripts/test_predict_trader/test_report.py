"""Unit tests for predict_trader.report."""

import importlib
import runpy
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def _load_report_module(monkeypatch: pytest.MonkeyPatch):
	"""Import report module with password prompt disabled."""
	import operate.quickstart.run_service as run_service

	monkeypatch.setattr(run_service, "ask_password_if_needed", lambda *_args, **_kwargs: None)
	module = importlib.import_module("scripts.predict_trader.report")
	return importlib.reload(module)


def test_color_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Color helper functions should format as expected."""
	report = _load_report_module(monkeypatch)

	assert report._color_string("ok", report.ColorCode.GREEN).startswith(report.ColorCode.GREEN)
	assert "True" in report._color_bool(True)
	assert "False" in report._color_bool(False)


def test_color_percent_handles_negative(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Negative percentages should be colored as warning text."""
	report = _load_report_module(monkeypatch)

	positive = report._color_percent(0.25)
	negative = report._color_percent(-0.25)

	assert positive == "25.00 %"
	assert report.ColorCode.RED in negative


def test_trades_since_message(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Should count filtered trades and unique markets."""
	report = _load_report_module(monkeypatch)

	trades_json = {
		"data": {
			"fpmmTrades": [
				{"creationTimestamp": "10", "fpmm": {"id": "m1"}},
				{"creationTimestamp": "11", "fpmm": {"id": "m1"}},
				{"creationTimestamp": "9", "fpmm": {"id": "m2"}},
			]
		}
	}

	assert report._trades_since_message(trades_json, utc_ts=10) == "2 trades on 1 markets"


def test_calculate_retrades_since(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Should return per-market counts, totals, and retrades."""
	report = _load_report_module(monkeypatch)

	trades_json = {
		"data": {
			"fpmmTrades": [
				{"creationTimestamp": "10", "fpmm": {"id": "m1"}},
				{"creationTimestamp": "11", "fpmm": {"id": "m1"}},
				{"creationTimestamp": "12", "fpmm": {"id": "m2"}},
			]
		}
	}

	filtered, n_unique, n_trades, n_retrades = report._calculate_retrades_since(trades_json, utc_ts=10)

	assert filtered["m1"] == 2
	assert filtered["m2"] == 1
	assert n_unique == 2
	assert n_trades == 3
	assert n_retrades == 1


def test_calculate_retrades_since_raises_on_missing_market_id(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Trades without fpmm id should raise a ValueError."""
	report = _load_report_module(monkeypatch)

	trades_json = {"data": {"fpmmTrades": [{"creationTimestamp": "10", "fpmm": {}}]}}

	with pytest.raises(ValueError, match="no associated market ID"):
		report._calculate_retrades_since(trades_json, utc_ts=0)


def test_retrade_average_and_max_messages(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Message formatters should return expected values."""
	report = _load_report_module(monkeypatch)

	assert report._retrades_since_message(2, 3, 1) == "1 re-trades on total 3 trades in 2 markets"
	assert report._average_trades_since_message(5, 2) == "2.5 trades per market"
	assert report._average_trades_since_message(5, 0) == "0 trades per market"
	assert report._max_trades_per_market_since_message({"m1": 3, "m2": 1}) == "3 trades per market"
	assert report._max_trades_per_market_since_message({}) == "0 trades per market"


def test_get_mech_requests_count(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Should count only requests above the timestamp threshold."""
	report = _load_report_module(monkeypatch)

	mech_requests = {
		"a": {"block_timestamp": 100},
		"b": {"block_timestamp": 200},
		"c": {"block_timestamp": 50},
	}

	assert report._get_mech_requests_count(mech_requests, timestamp=100) == 1


def test_warning_message(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Should emit warning text only when current value is below threshold."""
	report = _load_report_module(monkeypatch)

	warn = report._warning_message(1, threshold=2)
	ok = report._warning_message(2, threshold=2)

	assert "Balance too low" in warn
	assert ok == ""


def test_print_helpers_emit_expected_text(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
	"""Header/subheader/status print helpers should format output consistently."""
	report = _load_report_module(monkeypatch)

	report._print_section_header("Section")
	report._print_subsection_header("Sub")
	report._print_status("Key", "Val", "Msg")
	output = capsys.readouterr().out

	assert "Section" in output
	assert "Sub" in output
	assert "Key" in output
	assert "Val" in output
	assert "Msg" in output


def test_get_agent_status_container_running(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	"""Should report Running when both trader containers are present."""
	report = _load_report_module(monkeypatch)

	containers = [
		SimpleNamespace(name="traderpearl123_abci_0"),
		SimpleNamespace(name="traderpearl123_tm_0"),
	]

	class _ContainerApi:
		def list(self):
			return containers

	class _DockerClient:
		containers = _ContainerApi()

	monkeypatch.setattr(report.docker, "from_env", lambda: _DockerClient())
	service = SimpleNamespace(path=tmp_path)

	status = report._get_agent_status(service)

	assert "Running" in status


def test_get_agent_status_falls_back_to_pid_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	"""Should use agent.pid + pid_exists when containers are absent."""
	report = _load_report_module(monkeypatch)

	class _ContainerApi:
		def list(self):
			return []

	class _DockerClient:
		containers = _ContainerApi()

	deployment_dir = tmp_path / report.DEPLOYMENT_DIR
	deployment_dir.mkdir(parents=True)
	(deployment_dir / "agent.pid").write_text("123", encoding="utf-8")

	monkeypatch.setattr(report.docker, "from_env", lambda: _DockerClient())
	monkeypatch.setattr(report, "pid_exists", lambda pid: pid == 123)
	service = SimpleNamespace(path=tmp_path)

	status = report._get_agent_status(service)

	assert "Running" in status


def test_parse_args_smoke(monkeypatch: pytest.MonkeyPatch) -> None:
	"""_parse_args should parse empty argv without errors."""
	report = _load_report_module(monkeypatch)
	monkeypatch.setattr(report.sys, "argv", ["report.py"])
	args = report._parse_args()
	assert args is not None


def _run_report_main(
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
	*,
	wallet_data: dict[str, Any],
	staking_token_address: str | None,
	staking_state: int = 1,
	raise_balance_type_error_once: bool = False,
	raise_activity_checker: bool = False,
	raise_mech_marketplace: bool = False,
	raise_map_requests_counts: bool = False,
) -> None:
	"""Execute report.py __main__ with fully mocked external dependencies."""

	import operate.constants as operate_constants
	import operate.ledger.profiles as profiles
	import operate.quickstart.run_service as run_service
	import scripts.predict_trader.trades as trades_module
	import scripts.utils as utils_module
	import docker as docker_module
	import requests as requests_module
	import web3
	from web3.exceptions import ABIFunctionNotFound

	monkeypatch.setattr(run_service, "ask_password_if_needed", lambda *_args, **_kwargs: None)
	monkeypatch.setattr(sys, "argv", ["report.py"])

	operate_home = tmp_path / ".operate"
	(operate_home / "wallets").mkdir(parents=True, exist_ok=True)
	(operate_home / "wallets" / "ethereum.json").write_text(
		importlib.import_module("json").dumps(wallet_data), encoding="utf-8"
	)
	monkeypatch.setattr(operate_constants, "OPERATE_HOME", operate_home)

	service_path = tmp_path / "service"
	service_path.mkdir(parents=True, exist_ok=True)
	chain_config = SimpleNamespace(
		chain_data=SimpleNamespace(multisig="0x" + "1" * 40, token=123),
		ledger_config=SimpleNamespace(rpc="http://rpc"),
	)
	service = SimpleNamespace(
		name="service-name",
		chain_configs={"gnosis": chain_config},
		agent_addresses=["0x" + "2" * 40],
		path=service_path,
	)

	monkeypatch.setattr(utils_module, "get_service_from_config", lambda *_args, **_kwargs: service)
	monkeypatch.setattr(
		run_service,
		"load_local_config",
		lambda **_kwargs: SimpleNamespace(staking_program_id="program"),
	)

	trades_json = {
		"data": {
			"fpmmTrades": [
				{"creationTimestamp": "0", "fpmm": {"id": "m1"}},
				{"creationTimestamp": "0", "fpmm": {"id": "m1"}},
			]
		}
	}
	stats_table = {
		trades_module.MarketAttribute.ROI: {trades_module.MarketState.CLOSED: 0.1}
	}
	monkeypatch.setattr(trades_module, "get_mech_requests", lambda *_args, **_kwargs: {})
	monkeypatch.setattr(trades_module, "get_mech_statistics", lambda *_args, **_kwargs: {})
	monkeypatch.setattr(trades_module, "_query_omen_xdai_subgraph", lambda *_args, **_kwargs: trades_json)
	monkeypatch.setattr(trades_module, "parse_user", lambda *_args, **_kwargs: ("ok", stats_table))

	balance_calls = {"count": 0}

	def _fake_get_balance(*_args, **_kwargs) -> int:
		if raise_balance_type_error_once and balance_calls["count"] == 0:
			balance_calls["count"] += 1
			raise TypeError("unsupported block number")
		balance_calls["count"] += 1
		return 10**18

	monkeypatch.setattr(trades_module, "get_balance", _fake_get_balance)
	monkeypatch.setattr(trades_module, "get_token_balance", lambda *_args, **_kwargs: 2 * 10**18)
	monkeypatch.setattr(profiles, "get_staking_contract", lambda **_kwargs: staking_token_address)

	class _ContainerApi:
		def list(self):
			return [
				SimpleNamespace(name="traderpearl123_abci_0"),
				SimpleNamespace(name="traderpearl123_tm_0"),
			]

	class _DockerClient:
		containers = _ContainerApi()

	monkeypatch.setattr(docker_module, "from_env", lambda: _DockerClient())

	class _Resp:
		def json(self):
			return {"abi": []}

	monkeypatch.setattr(requests_module, "get", lambda *_args, **_kwargs: _Resp())

	class _Call:
		def __init__(self, value: Any = None, exc: Exception | None = None):
			self.value = value
			self.exc = exc

		def call(self, **_kwargs):
			if self.exc:
				raise self.exc
			return self.value

	class _StakingFns:
		def getStakingState(self, _service_id):
			return _Call(staking_state)

		def activityChecker(self):
			if raise_activity_checker:
				return _Call(exc=RuntimeError("activity checker failure"))
			return _Call("0xactivity")

		def serviceRegistryTokenUtility(self):
			return _Call("0xutility")

		def getAgentIds(self):
			return _Call(["1"])

		def minStakingDeposit(self):
			return _Call(100)

		def mapServiceInfo(self, _service_id):
			return _Call([0, 0, 0, 7])

		def tsCheckpoint(self):
			return _Call(900)

		def livenessPeriod(self):
			return _Call(100)

		def getNextRewardCheckpointTimestamp(self):
			return _Call(1200)

		def getServiceInfo(self, _service_id):
			return _Call([0, 0, [0, 2]])

	class _ActivityFns:
		def livenessRatio(self):
			return _Call(10**18)

		def agentMech(self):
			return _Call("0xagentmech")

	class _UtilityFns:
		def getOperatorBalance(self, _operator_address, _service_id):
			return _Call(500)

		def getAgentBond(self, _service_id, _agent_id):
			return _Call(300)

	class _MMActivityFns:
		def mechMarketplace(self):
			if raise_mech_marketplace:
				return _Call(exc=ValueError("no mech marketplace"))
			return _Call("0xmech")

	class _MechFns:
		def mapRequestsCounts(self, _safe_address):
			if raise_map_requests_counts:
				return _Call(exc=ABIFunctionNotFound("not found"))
			return _Call(10)

		def mapRequestCounts(self, _safe_address):
			return _Call(10)

	class _Contract:
		def __init__(self, functions_obj: Any):
			self.functions = functions_obj

	class _Eth:
		block_number = 111

		def __init__(self):
			self._activity_contract_calls = 0

		def contract(self, address=None, abi=None):
			if address == staking_token_address:
				return _Contract(_StakingFns())
			if address == "0xactivity":
				self._activity_contract_calls += 1
				if self._activity_contract_calls == 1:
					return _Contract(_ActivityFns())
				return _Contract(_MMActivityFns())
			if address == "0xutility":
				return _Contract(_UtilityFns())
			if address in ("0xmech", "0xagentmech"):
				return _Contract(_MechFns())
			return _Contract(SimpleNamespace())

		def get_block(self, _block_number):
			return SimpleNamespace(timestamp=1000)

	class _Web3:
		def __init__(self, _provider):
			self.eth = _Eth()

	monkeypatch.setattr(web3, "HTTPProvider", lambda rpc: rpc)
	monkeypatch.setattr(web3, "Web3", _Web3)

	runpy.run_module("scripts.predict_trader.report", run_name="__main__")


def test_report_main_exits_without_wallet(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	"""Main should exit if operate wallet file does not exist."""

	import operate.constants as operate_constants
	import operate.quickstart.run_service as run_service

	monkeypatch.setattr(run_service, "ask_password_if_needed", lambda *_args, **_kwargs: None)
	monkeypatch.setattr(sys, "argv", ["report.py"])
	monkeypatch.setattr(operate_constants, "OPERATE_HOME", tmp_path / "missing-operate")

	with pytest.raises(SystemExit):
		runpy.run_module("scripts.predict_trader.report", run_name="__main__")


def test_report_main_exits_without_gnosis_safe(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	"""Main should exit if wallet json lacks gnosis safe entry."""

	wallet_data = {"address": "0x" + "a" * 40, "safes": {}}
	with pytest.raises(SystemExit):
		_run_report_main(
			monkeypatch,
			tmp_path,
			wallet_data=wallet_data,
			staking_token_address=None,
		)


def test_report_main_runs_non_staked_flow(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	"""Main should complete when staking contract is unavailable."""

	wallet_data = {
		"address": "0x" + "a" * 40,
		"safes": {"gnosis": "0x" + "b" * 40},
	}
	_run_report_main(
		monkeypatch,
		tmp_path,
		wallet_data=wallet_data,
		staking_token_address=None,
		raise_balance_type_error_once=True,
	)


def test_report_main_runs_staked_flow_with_fallbacks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	"""Main should execute staked path and fallback mech calls."""

	wallet_data = {
		"address": "0x" + "a" * 40,
		"safes": {"gnosis": "0x" + "b" * 40},
	}
	_run_report_main(
		monkeypatch,
		tmp_path,
		wallet_data=wallet_data,
		staking_token_address="0xstake",
		staking_state=1,
		raise_mech_marketplace=True,
		raise_map_requests_counts=True,
	)


def test_report_main_evicted_and_staking_try_exception(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	"""Main should hit evicted state branch and outer staking exception handler."""

	wallet_data = {
		"address": "0x" + "a" * 40,
		"safes": {"gnosis": "0x" + "b" * 40},
	}
	_run_report_main(
		monkeypatch,
		tmp_path,
		wallet_data=wallet_data,
		staking_token_address="0xstake",
		staking_state=2,
		raise_activity_checker=True,
	)
