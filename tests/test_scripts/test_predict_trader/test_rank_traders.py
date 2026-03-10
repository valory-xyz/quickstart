"""Unit tests for predict_trader.rank_traders."""

import datetime
import runpy
from typing import Any

import pytest

from scripts.predict_trader import rank_traders
from scripts import utils as scripts_utils


class _Response:
	"""Simple fake response for requests.post calls."""

	def __init__(self, payload: dict[str, Any]):
		self._payload = payload

	def json(self) -> dict[str, Any]:
		"""Return payload JSON."""
		return self._payload


def _stats_row(roi: float, trades: int) -> dict[rank_traders.MarketAttribute, dict[rank_traders.MarketState, Any]]:
	"""Build a minimal statistics table used by _print_user_summary."""
	state = rank_traders.MarketState.CLOSED
	return {
		rank_traders.MarketAttribute.NUM_TRADES: {state: trades},
		rank_traders.MarketAttribute.WINNER_TRADES: {state: trades - 1},
		rank_traders.MarketAttribute.NUM_REDEEMED: {state: 1},
		rank_traders.MarketAttribute.INVESTMENT: {state: 10**18},
		rank_traders.MarketAttribute.FEES: {state: 10**17},
		rank_traders.MarketAttribute.EARNINGS: {state: 2 * 10**18},
		rank_traders.MarketAttribute.NET_EARNINGS: {state: 9 * 10**17},
		rank_traders.MarketAttribute.REDEMPTIONS: {state: 3 * 10**17},
		rank_traders.MarketAttribute.ROI: {state: roi},
	}


def test_parse_args_sets_utc_timezone(monkeypatch: pytest.MonkeyPatch) -> None:
	"""All parsed datetime arguments should be UTC-aware."""

	monkeypatch.setattr(
		rank_traders.sys,
		"argv",
		[
			"rank_traders.py",
			"--from-date",
			"2025-01-01T00:00:00",
			"--to-date",
			"2025-01-02T00:00:00",
			"--fpmm-created-from-date",
			"2024-12-01T00:00:00",
			"--fpmm-created-to-date",
			"2025-12-01T00:00:00",
			"--sort-by",
			"ROI",
		],
	)

	args = rank_traders._parse_args()

	assert args.from_date.tzinfo == datetime.timezone.utc
	assert args.to_date.tzinfo == datetime.timezone.utc
	assert args.fpmm_created_from_date.tzinfo == datetime.timezone.utc
	assert args.fpmm_created_to_date.tzinfo == datetime.timezone.utc
	assert args.sort_by == rank_traders.MarketAttribute.ROI


def test_to_content_wraps_query() -> None:
	"""_to_content should return expected Graph payload shape."""

	query = "{ fpmmTrades { id } }"
	content = rank_traders._to_content(query)

	assert content == {
		"query": query,
		"variables": None,
		"extensions": {"headers": None},
	}


def test_query_omen_xdai_subgraph_paginates_and_groups(
	monkeypatch: pytest.MonkeyPatch, tmp_path, requests_mock
) -> None:
	"""Subgraph pagination should continue until empty page and aggregate results."""

	calls: list[dict[str, Any]] = []
	responses = [
		{
			"data": {
				"fpmmTrades": [
					{"id": "1", "fpmm": {"id": "fpmm-a"}},
					{"id": "2", "fpmm": {"id": "fpmm-a"}},
				]
			}
		},
		{"data": {"fpmmTrades": []}},
	]

	operate_home = tmp_path / ".operate"
	operate_home.mkdir(parents=True)
	(operate_home / "subgraph_api_key.txt").write_text("dummy_key", encoding="utf-8")
	monkeypatch.setattr(scripts_utils, "OPERATE_HOME", operate_home)

	url = "https://gateway-arbitrum.network.thegraph.com/api/dummy_key/subgraphs/id/9fUVQpFwzpdWS9bq5WkAnmKbNNcoBwatMR4yZq81pbbz"
	requests_mock.post(url, [{"json": responses[0]}, {"json": responses[1]}])

	original_to_content = rank_traders._to_content

	def _capturing_to_content(query: str) -> dict[str, Any]:
		payload = original_to_content(query)
		calls.append(payload)
		return payload

	monkeypatch.setattr(rank_traders, "_to_content", _capturing_to_content)

	result = rank_traders._query_omen_xdai_subgraph(10, 20, 5, 15)

	assert len(calls) == 2
	assert 'id_gt: ""' in calls[0]["query"]
	assert 'id_gt: "2"' in calls[1]["query"]
	assert len(result["data"]["fpmmTrades"]) == 2


def test_group_trades_by_creator() -> None:
	"""Trades should be bucketed by creator id."""

	trades_json = {
		"data": {
			"fpmmTrades": [
				{"id": "1", "creator": {"id": "u1"}},
				{"id": "2", "creator": {"id": "u2"}},
				{"id": "3", "creator": {"id": "u1"}},
			]
		}
	}

	grouped = rank_traders._group_trades_by_creator(trades_json)

	assert set(grouped.keys()) == {"u1", "u2"}
	assert [trade["id"] for trade in grouped["u1"]["data"]["fpmmTrades"]] == ["1", "3"]
	assert [trade["id"] for trade in grouped["u2"]["data"]["fpmmTrades"]] == ["2"]


def test_print_user_summary_sorts_descending(capsys: pytest.CaptureFixture[str]) -> None:
	"""Higher ROI user should appear first in rendered summary."""

	creator_to_statistics = {
		"user-low": _stats_row(roi=0.10, trades=2),
		"user-high": _stats_row(roi=0.50, trades=4),
	}

	rank_traders._print_user_summary(creator_to_statistics)
	output = capsys.readouterr().out

	assert "User summary for Closed markets sorted by ROI:" in output
	assert output.find("user-high") < output.find("user-low")


def test_print_progress_bar_writes_expected_output(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Progress bar should write percent and iteration details to stdout."""

	written: list[str] = []
	flushed = {"called": False}

	class _Stdout:
		def write(self, text: str) -> None:
			written.append(text)

		def flush(self) -> None:
			flushed["called"] = True

	monkeypatch.setattr(rank_traders.sys, "stdout", _Stdout())

	rank_traders._print_progress_bar(iteration=5, total=10, prefix="P", suffix="S", length=10, fill="#")

	assert written
	assert "(5 of 10) - 50.0%" in written[0]
	assert flushed["called"] is True


def test_print_progress_bar_rejects_multi_char_fill() -> None:
	"""fill must be a single character."""

	with pytest.raises(ValueError, match="single character"):
		rank_traders._print_progress_bar(iteration=1, total=2, fill="##")


def test_main_execution_path(
	monkeypatch: pytest.MonkeyPatch,
	tmp_path,
	requests_mock,
	capsys: pytest.CaptureFixture[str],
) -> None:
	"""Execute the module as script and verify the main flow prints expected output."""

	import operate.quickstart.run_service as run_service
	import scripts.predict_trader.trades as trades_module
	import scripts.utils as utils_module

	monkeypatch.setattr(rank_traders.sys, "argv", ["rank_traders.py"])

	class _Config:
		rpc = {rank_traders.Chain.GNOSIS.value: "http://rpc"}

	monkeypatch.setattr(run_service, "load_local_config", lambda: _Config())
	operate_home = tmp_path / ".operate"
	operate_home.mkdir(parents=True)
	(operate_home / "subgraph_api_key.txt").write_text("dummy_key", encoding="utf-8")
	monkeypatch.setattr(utils_module, "OPERATE_HOME", operate_home)
	monkeypatch.setattr(
		trades_module,
		"parse_user",
		lambda _rpc, _creator, _trades_json, _stats: ("ignored", _stats_row(roi=0.25, trades=1)),
	)

	responses = [
		{
			"data": {
				"fpmmTrades": [
					{
						"id": "1",
						"creator": {"id": "user-1"},
						"fpmm": {"id": "market-1"},
					}
				]
			}
		},
		{"data": {"fpmmTrades": []}},
	]

	url = "https://gateway-arbitrum.network.thegraph.com/api/dummy_key/subgraphs/id/9fUVQpFwzpdWS9bq5WkAnmKbNNcoBwatMR4yZq81pbbz"
	requests_mock.post(url, [{"json": responses[0]}, {"json": responses[1]}])

	runpy.run_module("scripts.predict_trader.rank_traders", run_name="__main__")
	output = capsys.readouterr().out

	assert "Starting script" in output
	assert "Total trading transactions: 1" in output
	assert "Total traders: 1" in output
