"""Unit tests for predict_trader.mech_events."""

import builtins
import json
from typing import Any

import pytest

from scripts.predict_trader import mech_events


class _Response:
	"""Simple fake response object for requests.get tests."""

	def __init__(self, payload: Any):
		self._payload = payload

	def raise_for_status(self) -> None:
		"""No-op successful status."""

	def json(self) -> Any:
		"""Return payload or raise it if it is an exception."""
		if isinstance(self._payload, Exception):
			raise self._payload
		return self._payload


class _DummyEvent(mech_events.MechBaseEvent):
	"""Concrete helper event used to test base-class behavior."""

	event_name = "Dummy"
	subgraph_event_name = "dummy"


def _make_subgraph_event(event_id: str) -> dict[str, Any]:
	"""Create a minimal subgraph event payload for MechRequest."""
	return {
		"id": event_id,
		"sender": {"id": "0xabc"},
		"transactionHash": f"0xtx{event_id}",
		"blockNumber": "1",
		"blockTimestamp": "100",
		"mechRequest": {"ipfsHash": "QmHash"},
		"marketplaceRequest": {"ipfsHashBytes": "0x1234"},
	}


def test_populate_ipfs_contents_falls_back_to_non_metadata_url(monkeypatch: pytest.MonkeyPatch) -> None:
	"""When metadata.json is not JSON, fallback URL should be used."""

	calls: list[str] = []

	def _fake_get(url: str) -> _Response:
		calls.append(url)
		if url.endswith("/metadata.json"):
			return _Response(json.JSONDecodeError("bad", "doc", 0))
		return _Response({"ok": True})

	monkeypatch.setattr(mech_events.requests, "get", _fake_get)

	event = mech_events.MechBaseEvent(
		event_id="1",
		sender="0xabc",
		transaction_hash="0xtx",
		block_number=1,
		block_timestamp=100,
		ipfs_hash="QmHash",
	)

	assert calls[0].endswith("/metadata.json")
	assert event.ipfs_contents == {"ok": True}
	assert event.ipfs_link.endswith("QmHash")


def test_read_mech_events_file_not_found_sets_default_version(
	monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
	"""Missing DB file should initialize default DB structure."""

	db_path = tmp_path / "mech_events.json"
	monkeypatch.setattr(mech_events, "MECH_EVENTS_JSON_PATH", db_path)

	data = mech_events._read_mech_events_data_from_file()

	assert data == {"db_version": mech_events.MECH_EVENTS_DB_VERSION}


def test_read_mech_events_old_version_renames_and_resets(
	monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
	"""Old DB version should be moved aside and recreated."""

	db_path = tmp_path / "mech_events.json"
	db_path.write_text(json.dumps({"db_version": 1}), encoding="utf-8")
	monkeypatch.setattr(mech_events, "MECH_EVENTS_JSON_PATH", db_path)
	monkeypatch.setattr(mech_events.time, "strftime", lambda _: "2026-03-10_10-00-00")

	data = mech_events._read_mech_events_data_from_file()

	assert data == {"db_version": mech_events.MECH_EVENTS_DB_VERSION}
	assert (tmp_path / "mech_events.2026-03-10_10-00-00.old.json").exists()


def test_read_mech_events_corrupted_json_exits(
	monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
	"""Corrupted DB JSON should trigger process exit."""

	db_path = tmp_path / "mech_events.json"
	db_path.write_text("{invalid", encoding="utf-8")
	monkeypatch.setattr(mech_events, "MECH_EVENTS_JSON_PATH", db_path)

	with pytest.raises(SystemExit):
		mech_events._read_mech_events_data_from_file()


def test_write_mech_events_data_respects_delay_and_force_write(
	monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
	"""Writes should be delayed unless force_write is enabled."""

	db_path = tmp_path / "mech_events.json"
	monkeypatch.setattr(mech_events, "MECH_EVENTS_JSON_PATH", db_path)
	monkeypatch.setattr(mech_events, "last_write_time", 100.0)
	monkeypatch.setattr(mech_events.time, "time", lambda: 110.0)

	mech_events._write_mech_events_data_to_file({"db_version": 3})
	assert not db_path.exists()

	mech_events._write_mech_events_data_to_file({"db_version": 3}, force_write=True)
	assert db_path.exists()


def test_query_mech_events_subgraph_paginates(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Subgraph query should request all pages until an empty page."""

	calls: list[dict[str, Any]] = []

	class _FakeClient:
		def execute(self, _query: Any, variable_values: dict[str, Any]) -> dict[str, Any]:
			calls.append(variable_values)
			if variable_values["id_gt"] == "":
				return {"requests": [{"id": "1"}, {"id": "2"}]}
			return {"requests": []}

	monkeypatch.setattr(mech_events, "RequestsHTTPTransport", lambda *_args, **_kwargs: object())
	monkeypatch.setattr(mech_events, "Client", lambda **_kwargs: _FakeClient())
	monkeypatch.setattr(mech_events, "gql", lambda q: q)

	result = mech_events._query_mech_events_subgraph("0xabc", mech_events.MechRequest)

	assert result == {"data": {"requests": [{"id": "1"}, {"id": "2"}]}}
	assert calls[0]["id_gt"] == ""
	assert calls[1]["id_gt"] == "2"


def test_update_mech_events_db_updates_missing_and_incomplete_events(
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	"""Only missing events or events without IPFS contents should be refreshed."""

	sender = "0xabc"
	db = {
		"db_version": mech_events.MECH_EVENTS_DB_VERSION,
		sender: {
			"Request": {
				"1": {"event_id": "1", "ipfs_contents": {"done": True}},
				"2": {"event_id": "2", "ipfs_contents": {}},
			}
		},
	}

	query_result = {
		"data": {
			"requests": [
				_make_subgraph_event("1"),
				_make_subgraph_event("2"),
				_make_subgraph_event("3"),
			]
		}
	}

	writes: list[bool] = []

	monkeypatch.setattr(mech_events, "_query_mech_events_subgraph", lambda *_args, **_kwargs: query_result)
	monkeypatch.setattr(mech_events, "_read_mech_events_data_from_file", lambda: db)
	monkeypatch.setattr(
		mech_events,
		"_write_mech_events_data_to_file",
		lambda mech_events_data, force_write=False: writes.append(force_write),
	)
	monkeypatch.setattr(mech_events.MechBaseEvent, "_populate_ipfs_contents", lambda self: None)

	mech_events._update_mech_events_db(sender, mech_events.MechRequest)

	stored = db[sender]["Request"]
	assert "3" in stored
	assert stored["2"]["event_id"] == "2"
	assert stored["1"]["ipfs_contents"] == {"done": True}
	assert writes[-1] is True


def test_get_mech_events_updates_then_returns_sender_events(monkeypatch: pytest.MonkeyPatch) -> None:
	"""_get_mech_events should update database and return sender event bucket."""

	updated: list[tuple[str, type[mech_events.MechBaseEvent]]] = []
	monkeypatch.setattr(
		mech_events,
		"_update_mech_events_db",
		lambda sender, event_cls: updated.append((sender, event_cls)),
	)
	monkeypatch.setattr(
		mech_events,
		"_read_mech_events_data_from_file",
		lambda: {"0xabc": {"Request": {"evt": {"event_id": "evt"}}}},
	)

	result = mech_events._get_mech_events("0xabc", mech_events.MechRequest)

	assert updated == [("0xabc", mech_events.MechRequest)]
	assert result == {"evt": {"event_id": "evt"}}


def test_get_mech_requests_filters_by_timestamp(monkeypatch: pytest.MonkeyPatch) -> None:
	"""get_mech_requests should include events within inclusive bounds only."""

	monkeypatch.setattr(
		mech_events,
		"_get_mech_events",
		lambda *_args, **_kwargs: {
			"a": {"block_timestamp": "9"},
			"b": {"block_timestamp": "10"},
			"c": {"block_timestamp": "20"},
			"d": {"block_timestamp": "21"},
		},
	)

	result = mech_events.get_mech_requests("0xabc", from_timestamp=10, to_timestamp=20)

	assert result == {
		"b": {"block_timestamp": "10"},
		"c": {"block_timestamp": "20"},
	}


def test_populate_ipfs_contents_warns_when_no_hash(capsys: pytest.CaptureFixture[str]) -> None:
	"""No hash values should print warning and keep empty fields."""

	event = _DummyEvent(
		event_id="1",
		sender="0xabc",
		transaction_hash="0xtx",
		block_number=1,
		block_timestamp=100,
	)

	output = capsys.readouterr().out
	assert "WARNING: No IPFS hash found" in output
	assert event.ipfs_contents == {}
	assert event.ipfs_link == ""


def test_populate_ipfs_contents_uses_ipfs_hash_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
	"""When only ipfs_hash_bytes exists, CID-prefixed URL should be used."""

	called: list[str] = []

	def _fake_get(url: str) -> _Response:
		called.append(url)
		return _Response({"ok": True})

	monkeypatch.setattr(mech_events.requests, "get", _fake_get)

	event = mech_events.MechBaseEvent(
		event_id="1",
		sender="0xabc",
		transaction_hash="0xtx",
		block_number=1,
		block_timestamp=100,
		ipfs_hash_bytes="0x1234",
	)

	assert event.ipfs_hash_bytes == "1234"
	assert called[0].endswith(f"{mech_events.CID_PREFIX}1234/metadata.json")
	assert event.ipfs_contents == {"ok": True}


def test_populate_ipfs_contents_handles_generic_exception(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Unexpected request errors should print traceback and continue."""

	class _BadResponse:
		def raise_for_status(self) -> None:
			raise RuntimeError("boom")

		def json(self) -> dict[str, Any]:
			return {"unused": True}

	printed: list[str] = []
	inputs: list[str] = []
	monkeypatch.setattr(mech_events.requests, "get", lambda _url: _BadResponse())
	monkeypatch.setattr(mech_events.traceback, "format_exc", lambda: "traceback-text")
	monkeypatch.setattr(builtins, "print", lambda *args, **kwargs: printed.append(" ".join(str(a) for a in args)))
	monkeypatch.setattr(builtins, "input", lambda prompt: inputs.append(prompt) or "")

	_DummyEvent(
		event_id="1",
		sender="0xabc",
		transaction_hash="0xtx",
		block_number=1,
		block_timestamp=100,
		ipfs_hash="QmHash",
	)

	assert any("traceback-text" in line for line in printed)
	assert len(inputs) == 2


def test_update_mech_events_db_handles_keyboard_interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
	"""KeyboardInterrupt from query should be handled gracefully."""

	messages: list[str] = []
	inputs: list[str] = []
	monkeypatch.setattr(mech_events, "_query_mech_events_subgraph", lambda *_a, **_k: (_ for _ in ()).throw(KeyboardInterrupt()))
	monkeypatch.setattr(builtins, "print", lambda *args, **kwargs: messages.append(" ".join(str(a) for a in args)))
	monkeypatch.setattr(builtins, "input", lambda prompt: inputs.append(prompt) or "")

	mech_events._update_mech_events_db("0xabc", mech_events.MechRequest)

	assert any("was cancelled" in msg for msg in messages)
	assert inputs == ["Press Enter to continue..."]


def test_update_mech_events_db_handles_generic_exception(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Generic exceptions during update should print warning and continue."""

	messages: list[str] = []
	inputs: list[str] = []
	monkeypatch.setattr(mech_events, "_query_mech_events_subgraph", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")))
	monkeypatch.setattr(mech_events.traceback, "format_exc", lambda: "runtime-trace")
	monkeypatch.setattr(builtins, "print", lambda *args, **kwargs: messages.append(" ".join(str(a) for a in args)))
	monkeypatch.setattr(builtins, "input", lambda prompt: inputs.append(prompt) or "")

	mech_events._update_mech_events_db("0xabc", mech_events.MechRequest)

	assert any("runtime-trace" in msg for msg in messages)
	assert any("An error occurred while updating" in msg for msg in messages)
	assert inputs == ["Press Enter to continue..."]
