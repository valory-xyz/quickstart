"""Unit tests for predict_trader.trades."""

import datetime
import runpy
import sys
from pathlib import Path
from typing import Any

import pytest

from scripts.predict_trader import trades
from scripts import utils as scripts_utils


class _Response:
	"""Simple fake response for requests.post."""

	def __init__(self, payload: dict[str, Any]):
		self._payload = payload

	def json(self) -> dict[str, Any]:
		"""Return payload as JSON."""
		return self._payload


def test_parse_args_with_creator(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Argument parsing should set UTC timezone and preserve creator."""

	creator = "0x" + "a" * 40
	monkeypatch.setattr(
		trades.sys,
		"argv",
		[
			"trades.py",
			"--creator",
			creator,
			"--from-date",
			"2025-01-01T00:00:00",
			"--to-date",
			"2025-01-02T00:00:00",
		],
	)

	args = trades._parse_args()

	assert args.creator == creator
	assert args.from_date.tzinfo == datetime.timezone.utc
	assert args.to_date.tzinfo == datetime.timezone.utc


def test_parse_args_without_creator_uses_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	"""If creator is omitted, config-derived multisig should be used."""

	script_path = tmp_path / "scripts" / "predict_trader"
	(tmp_path / "configs").mkdir(parents=True)
	(tmp_path / "configs" / "config_predict_trader.json").write_text("{}", encoding="utf-8")
	script_path.mkdir(parents=True)

	class _Cfg:
		chain_configs = {
			"gnosis": type("_X", (), {"chain_data": type("_Y", (), {"multisig": "0x" + "b" * 40})()})()
		}

	monkeypatch.setattr(trades, "SCRIPT_PATH", script_path)
	monkeypatch.setattr(trades, "get_service_from_config", lambda *_args, **_kwargs: _Cfg())
	monkeypatch.setattr(trades.sys, "argv", ["trades.py"])

	args = trades._parse_args()

	assert args.creator == "0x" + "b" * 40


def test_parse_args_without_creator_and_missing_config_exits(
	monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
	"""Missing default config should exit when creator is not provided."""

	script_path = tmp_path / "scripts" / "predict_trader"
	script_path.mkdir(parents=True)
	monkeypatch.setattr(trades, "SCRIPT_PATH", script_path)
	monkeypatch.setattr(trades.sys, "argv", ["trades.py"])

	with pytest.raises(SystemExit):
		trades._parse_args()


def test_parse_args_with_invalid_creator_exits(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Invalid Ethereum creator value should raise parser error."""

	monkeypatch.setattr(trades.sys, "argv", ["trades.py", "--creator", "invalid"])
	with pytest.raises(SystemExit):
		trades._parse_args()


def test_to_content_wraps_query() -> None:
	"""_to_content should match expected request structure."""

	query = "{ q }"
	assert trades._to_content(query) == {
		"query": query,
		"variables": None,
		"extensions": {"headers": None},
	}


def test_query_omen_xdai_subgraph_paginates(
	monkeypatch: pytest.MonkeyPatch, tmp_path: Path, requests_mock
) -> None:
	"""Should paginate by creationTimestamp for each FPMM creator."""

	creator = "0x" + "c" * 40
	calls: list[dict[str, Any]] = []
	responses = [
		{"data": {"fpmmTrades": [{"id": "1", "creationTimestamp": "10", "fpmm": {"id": "m1"}}]}},
		{"data": {"fpmmTrades": []}},
		{"data": {"fpmmTrades": []}},
	]

	operate_home = tmp_path / ".operate"
	operate_home.mkdir(parents=True)
	(operate_home / "subgraph_api_key.txt").write_text("k", encoding="utf-8")
	monkeypatch.setattr(scripts_utils, "OPERATE_HOME", operate_home)

	url = "https://gateway-arbitrum.network.thegraph.com/api/k/subgraphs/id/9fUVQpFwzpdWS9bq5WkAnmKbNNcoBwatMR4yZq81pbbz"
	requests_mock.post(url, [{"json": responses[0]}, {"json": responses[1]}, {"json": responses[2]}])

	original_to_content = trades._to_content

	def _capture(query: str) -> dict[str, Any]:
		payload = original_to_content(query)
		calls.append(payload)
		return payload

	monkeypatch.setattr(trades, "_to_content", _capture)

	result = trades._query_omen_xdai_subgraph(creator, 1, 2, 3, 4)

	assert len(calls) == 3
	assert 'creationTimestamp_gt: "0"' in calls[0]["query"]
	assert 'creationTimestamp_gt: "10"' in calls[1]["query"]
	assert result["data"]["fpmmTrades"][0]["id"] == "1"


def test_query_conditional_tokens_gc_subgraph(
	monkeypatch: pytest.MonkeyPatch, tmp_path: Path, requests_mock
) -> None:
	"""Should paginate user positions until empty page."""

	responses = [
		{"data": {"user": {"userPositions": [{"id": "p1"}, {"id": "p2"}]}}},
		{"data": {"user": {"userPositions": []}}},
	]

	operate_home = tmp_path / ".operate"
	operate_home.mkdir(parents=True)
	(operate_home / "subgraph_api_key.txt").write_text("k", encoding="utf-8")
	monkeypatch.setattr(scripts_utils, "OPERATE_HOME", operate_home)

	url = "https://gateway-arbitrum.network.thegraph.com/api/k/subgraphs/id/7s9rGBffUTL8kDZuxvvpuc46v44iuDarbrADBFw5uVp2"
	requests_mock.post(url, [{"json": responses[0]}, {"json": responses[1]}])

	result = trades._query_conditional_tokens_gc_subgraph("0xabc")

	history = requests_mock.request_history
	assert len(history) == 2
	assert 'id_gt: ""' in history[0].json()["query"]
	assert 'id_gt: "p2"' in history[1].json()["query"]
	assert result["data"]["user"]["userPositions"] == [{"id": "p1"}, {"id": "p2"}]


def test_query_conditional_tokens_gc_subgraph_returns_none_when_empty(
	monkeypatch: pytest.MonkeyPatch, tmp_path: Path, requests_mock
) -> None:
	"""No user data should produce {'data': {'user': None}}."""

	operate_home = tmp_path / ".operate"
	operate_home.mkdir(parents=True)
	(operate_home / "subgraph_api_key.txt").write_text("k", encoding="utf-8")
	monkeypatch.setattr(scripts_utils, "OPERATE_HOME", operate_home)

	url = "https://gateway-arbitrum.network.thegraph.com/api/k/subgraphs/id/7s9rGBffUTL8kDZuxvvpuc46v44iuDarbrADBFw5uVp2"
	requests_mock.post(url, json={"data": {"user": {}}})

	result = trades._query_conditional_tokens_gc_subgraph("0xabc")

	assert result == {"data": {"user": None}}


def test_unit_conversion_helpers() -> None:
	"""Wei conversion helpers should return expected units and strings."""

	assert trades.wei_to_unit(10**18) == 1
	assert trades.wei_to_xdai(2 * 10**18) == "2.00 xDAI"
	assert trades.wei_to_wxdai(3 * 10**18) == "3.00 WxDAI"
	assert trades.wei_to_olas(4 * 10**18) == "4.00 OLAS"


def test_is_redeemed_paths() -> None:
	"""_is_redeemed should detect unredeemed, redeemed, and unknown states."""

	fpmm_trade = {
		"outcomeTokensTraded": "10",
		"fpmm": {"condition": {"id": "cond-1"}},
	}

	not_redeemed = {
		"data": {
			"user": {
				"userPositions": [
					{
						"balance": "10",
						"position": {"conditionIds": ["cond-1"]},
					}
				]
			}
		}
	}
	redeemed = {
		"data": {
			"user": {
				"userPositions": [
					{
						"balance": "0",
						"position": {"conditionIds": ["cond-1"]},
					}
				]
			}
		}
	}
	unknown = {"data": {"user": {"userPositions": []}}}

	assert trades._is_redeemed(not_redeemed, fpmm_trade) is False
	assert trades._is_redeemed(redeemed, fpmm_trade) is True
	assert trades._is_redeemed(unknown, fpmm_trade) is False


def test_compute_roi() -> None:
	"""ROI should be computed safely for zero and non-zero initial value."""

	assert trades._compute_roi(100, 150) == 0.5
	assert trades._compute_roi(0, 999) == 0.0


def test_compute_totals_recomputes_derived_fields() -> None:
	"""_compute_totals should fill TOTAL and recompute net earnings and ROI."""

	table = {row: {col: 0 for col in trades.STATS_TABLE_COLS} for row in trades.STATS_TABLE_ROWS}
	table[trades.MarketAttribute.INVESTMENT][trades.MarketState.CLOSED] = 100
	table[trades.MarketAttribute.FEES][trades.MarketState.CLOSED] = 10
	table[trades.MarketAttribute.EARNINGS][trades.MarketState.CLOSED] = 150
	mech_statistics = {"q1": {"count": 2, "fees": 5}, "q2": {"count": 1, "fees": 10}}

	trades._compute_totals(table, mech_statistics)

	assert table[trades.MarketAttribute.MECH_CALLS]["TOTAL"] == 3
	assert table[trades.MarketAttribute.MECH_FEES]["TOTAL"] == 15
	assert table[trades.MarketAttribute.INVESTMENT][trades.MarketState.CLOSED] == 90
	assert table[trades.MarketAttribute.NET_EARNINGS][trades.MarketState.CLOSED] == 50


def test_get_market_state_paths(monkeypatch: pytest.MonkeyPatch) -> None:
	"""_get_market_state should return expected values across branches."""

	class _FakeDateTime(datetime.datetime):
		@classmethod
		def utcnow(cls):
			return cls(2025, 1, 1, 0, 0, 0)

	monkeypatch.setattr(trades.datetime, "datetime", _FakeDateTime)

	open_market = {"currentAnswer": None, "openingTimestamp": str(1735790400)}  # 2025-01-02
	pending_market = {"currentAnswer": None, "openingTimestamp": str(1735603200)}  # 2024-12-31
	arbitrating_market = {"currentAnswer": "0x0", "isPendingArbitration": True, "answerFinalizedTimestamp": "0"}
	finalizing_market = {"currentAnswer": "0x0", "isPendingArbitration": False, "answerFinalizedTimestamp": str(1735776000)}
	closed_market = {"currentAnswer": "0x0", "isPendingArbitration": False, "answerFinalizedTimestamp": str(1735689600)}

	assert trades._get_market_state(open_market) == trades.MarketState.OPEN
	assert trades._get_market_state(pending_market) == trades.MarketState.PENDING
	assert trades._get_market_state(arbitrating_market) == trades.MarketState.ARBITRATING
	assert trades._get_market_state(finalizing_market) == trades.MarketState.FINALIZING
	assert trades._get_market_state(closed_market) == trades.MarketState.CLOSED


def test_format_table_and_mech_statistics() -> None:
	"""Formatting and mech aggregation helpers should produce expected content."""

	table = {row: {col: 0 for col in trades.STATS_TABLE_COLS} for row in trades.STATS_TABLE_ROWS}
	rendered = trades._format_table(table)
	assert str(trades.MarketAttribute.ROI) in rendered
	assert "TOTAL" in rendered

	mech_requests = {
		"a": {"ipfs_contents": {"tool": "foo", "prompt": 'Ask "What is this?"'}, "fee": 10},
		"b": {"ipfs_contents": {"tool": "foo", "prompt": "Simple prompt"}, "fee": 5},
		"c": {"ipfs_contents": {"tool": trades.IRRELEVANT_TOOLS[0], "prompt": "skip"}, "fee": 99},
	}
	stats = trades.get_mech_statistics(mech_requests)

	assert stats["What is this?"]["count"] == 1
	assert stats["What is this?"]["fees"] == 10
	assert stats["Simple prompt"]["count"] == 1


def test_balance_helpers_use_rpc(monkeypatch: pytest.MonkeyPatch) -> None:
	"""RPC balance helpers should parse hex balances to integers."""

	responses = [_Response({"result": "0xa"}), _Response({"result": "0xb"})]
	idx = {"value": 0}

	def _fake_post(*_args, **_kwargs):
		i = idx["value"]
		idx["value"] += 1
		return responses[i]

	monkeypatch.setattr(trades.requests, "post", _fake_post)

	assert trades.get_balance("0x" + "1" * 40, "http://rpc") == 10
	assert trades.get_token_balance("0x" + "2" * 40, "0x" + "3" * 40, "http://rpc") == 11


def test_market_attribute_repr_and_argparse_error() -> None:
	"""MarketAttribute repr and invalid argparse input should be covered."""

	assert repr(trades.MarketAttribute.ROI) == "ROI"
	with pytest.raises(ValueError, match="Invalid MarketAttribute"):
		trades.MarketAttribute.argparse("not-an-attr")


def test_get_market_state_exception_returns_unknown() -> None:
	"""Malformed market input should be handled and return UNKNOWN."""

	assert trades._get_market_state({}) == trades.MarketState.UNKNOWN


def test_parse_user_covers_status_branches(monkeypatch: pytest.MonkeyPatch) -> None:
	"""parse_user should exercise finalizing/closed branches and type error fallback."""

	def _trade(title: str, fpmm_id: str, current_answer: str, outcome_index: str = "0", traded: str = "100") -> dict[str, Any]:
		return {
			"title": title,
			"collateralAmount": "100",
			"outcomeIndex": outcome_index,
			"feeAmount": "5",
			"outcomeTokensTraded": traded,
			"creationTimestamp": "1",
			"fpmm": {
				"id": fpmm_id,
				"outcomes": ["YES", "NO"],
				"currentAnswer": current_answer,
				"isPendingArbitration": False,
				"answerFinalizedTimestamp": "0",
				"openingTimestamp": "0",
				"condition": {"id": f"cond-{fpmm_id}"},
			},
		}

	trade_type_error = {
		"title": "type-error",
		"collateralAmount": None,
		"outcomeIndex": "0",
		"feeAmount": "5",
		"outcomeTokensTraded": "100",
		"creationTimestamp": "1",
		"fpmm": {
			"id": "m0",
			"outcomes": ["YES", "NO"],
			"currentAnswer": "0x0",
			"isPendingArbitration": False,
			"answerFinalizedTimestamp": "0",
			"openingTimestamp": "0",
			"condition": {"id": "cond-m0"},
		},
	}

	trades_json = {
		"data": {
			"fpmmTrades": [
				trade_type_error,
				_trade("fin-invalid", "m1", hex(trades.INVALID_ANSWER)),
				_trade("fin-winner", "m2", "0x0", outcome_index="0", traded="200"),
				_trade("fin-loser", "m3", "0x1", outcome_index="0"),
				_trade("closed-invalid", "m4", hex(trades.INVALID_ANSWER)),
				_trade("closed-winner", "m5", "0x0", outcome_index="0", traded="1"),
				_trade("closed-loser", "m6", "0x1", outcome_index="0"),
			]
		}
	}

	status_map = {
		"m1": trades.MarketState.FINALIZING,
		"m2": trades.MarketState.FINALIZING,
		"m3": trades.MarketState.FINALIZING,
		"m4": trades.MarketState.CLOSED,
		"m5": trades.MarketState.CLOSED,
		"m6": trades.MarketState.CLOSED,
	}

	monkeypatch.setattr(trades, "_get_market_state", lambda fpmm: status_map[fpmm["id"]])
	monkeypatch.setattr(
		trades,
		"_query_conditional_tokens_gc_subgraph",
		lambda *_args, **_kwargs: {
			"data": {
				"user": {
					"userPositions": [
						{"position": {"conditionIds": ["cond-m4"]}, "balance": "0"},
						{"position": {"conditionIds": ["cond-m5"]}, "balance": "0"},
					]
				}
			}
		},
	)
	monkeypatch.setattr(trades, "get_balance", lambda *_args, **_kwargs: 10**18)
	monkeypatch.setattr(trades, "get_token_balance", lambda *_args, **_kwargs: 2 * 10**18)

	mech_stats = {
		"fin-invalid": {"count": 1, "fees": 1},
		"closed-winner": {"count": 2, "fees": 2},
	}

	output, table = trades.parse_user("http://rpc", "0x" + "a" * 40, trades_json, mech_stats)

	assert "ERROR RETRIEVING TRADE INFORMATION" in output
	assert "Current answer: Market has been declared invalid." in output
	assert "Final answer: Market has been declared invalid." in output
	assert "Congrats! The trade was for the winner answer." in output
	assert "The trade was for the loser answer." in output
	assert "Earnings are dust." in output
	assert table[trades.MarketAttribute.NUM_INVALID_MARKET][trades.MarketState.CLOSED] == 1
	assert table[trades.MarketAttribute.NUM_REDEEMED][trades.MarketState.CLOSED] == 1


def test_get_mech_statistics_skips_missing_ipfs_fields() -> None:
	"""Missing ipfs tool/prompt keys should be ignored."""

	stats = trades.get_mech_statistics({
		"a": {"fee": 1},
		"b": {"ipfs_contents": {"tool": "x"}, "fee": 2},
		"c": {"ipfs_contents": {"prompt": "q"}, "fee": 3},
	})

	assert dict(stats) == {}


def test_main_execution_path(
	monkeypatch: pytest.MonkeyPatch,
	tmp_path: Path,
	requests_mock,
	capsys: pytest.CaptureFixture[str],
) -> None:
	"""Run trades.py as script with mocks to cover __main__ path."""

	import operate.cli as operate_cli
	import operate.quickstart.run_service as run_service
	import scripts.predict_trader.mech_events as mech_events
	import scripts.utils as utils_module
	import requests

	class _DummyOperate:
		pass

	class _Service:
		name = "svc"

	monkeypatch.setattr(operate_cli, "OperateApp", _DummyOperate)
	monkeypatch.setattr(run_service, "ask_password_if_needed", lambda *_args, **_kwargs: None)
	monkeypatch.setattr(run_service, "load_local_config", lambda *_args, **_kwargs: type("_Cfg", (), {"rpc": {trades.Chain.GNOSIS.value: "http://rpc"}})())
	monkeypatch.setattr(utils_module, "get_service_from_config", lambda *_args, **_kwargs: _Service())
	operate_home = tmp_path / ".operate"
	operate_home.mkdir(parents=True)
	(operate_home / "subgraph_api_key.txt").write_text("dummy_key", encoding="utf-8")
	monkeypatch.setattr(utils_module, "OPERATE_HOME", operate_home)
	monkeypatch.setattr(mech_events, "get_mech_requests", lambda *_args, **_kwargs: {})

	subgraph_url_a = "https://gateway-arbitrum.network.thegraph.com/api/dummy_key/subgraphs/id/9fUVQpFwzpdWS9bq5WkAnmKbNNcoBwatMR4yZq81pbbz"
	subgraph_url_b = "https://gateway-arbitrum.network.thegraph.com/api/dummy_key/subgraphs/id/7s9rGBffUTL8kDZuxvvpuc46v44iuDarbrADBFw5uVp2"
	requests_mock.post(subgraph_url_a, [{"json": {"data": {"fpmmTrades": []}}}, {"json": {"data": {"fpmmTrades": []}}}])
	requests_mock.post(subgraph_url_b, json={"data": {"user": {}}})
	requests_mock.post("http://rpc", [{"json": {"result": "0xa"}}, {"json": {"result": "0xb"}}])

	monkeypatch.setattr(
		sys,
		"argv",
		[
			"trades.py",
			"--creator",
			"0x" + "a" * 40,
		],
	)

	runpy.run_module("scripts.predict_trader.trades", run_name="__main__")
	output = capsys.readouterr().out
	assert "Summary (per market state)" in output
	assert "Safe address:" in output
