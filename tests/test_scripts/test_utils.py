"""Unit tests for scripts.utils."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import utils


def test_get_subgraph_api_key_reads_existing_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Existing subgraph key file should be read without prompting."""

    operate_home = tmp_path / ".operate"
    operate_home.mkdir(parents=True)
    key_path = operate_home / "subgraph_api_key.txt"
    key_path.write_text("existing-key", encoding="utf-8")

    monkeypatch.setattr(utils, "OPERATE_HOME", operate_home)

    assert utils.get_subgraph_api_key() == "existing-key"


def test_get_subgraph_api_key_prompts_and_writes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Missing key file should prompt user, persist key, and return it."""

    operate_home = tmp_path / ".operate"
    monkeypatch.setattr(utils, "OPERATE_HOME", operate_home)
    monkeypatch.setattr("builtins.input", lambda _prompt: "prompted-key")

    result = utils.get_subgraph_api_key()

    assert result == "prompted-key"
    assert (operate_home / "subgraph_api_key.txt").read_text(encoding="utf-8") == "prompted-key"


def test_get_service_from_config_missing_file_exits(tmp_path: Path) -> None:
    """Missing config path should exit with error message."""

    with pytest.raises(SystemExit):
        utils.get_service_from_config(tmp_path / "missing.json")


def test_get_service_from_config_creates_operate_when_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When operate is None, helper should create and initialize OperateApp."""

    config_path = tmp_path / "config.json"
    template = {"name": "svc"}
    config_path.write_text(json.dumps(template), encoding="utf-8")

    manager = object()

    class _Operate:
        def service_manager(self):
            return manager

    created = {"count": 0}

    def _make_operate():
        created["count"] += 1
        return _Operate()

    asked = {"count": 0}
    configured = {"args": None}

    monkeypatch.setattr(utils, "OperateApp", _make_operate)
    monkeypatch.setattr(utils, "ask_password_if_needed", lambda _op: asked.__setitem__("count", asked["count"] + 1))
    monkeypatch.setattr(utils, "configure_local_config", lambda t, o: configured.__setitem__("args", (t, o)))
    monkeypatch.setattr(utils, "get_service", lambda m, t: {"manager": m, "template": t})

    result = utils.get_service_from_config(config_path)

    assert created["count"] == 1
    assert asked["count"] == 1
    assert configured["args"][0] == template
    assert result["manager"] is manager


def test_get_service_from_config_uses_provided_operate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When operate is provided, it should not create a new instance or ask password."""

    config_path = tmp_path / "config.json"
    template = {"name": "svc"}
    config_path.write_text(json.dumps(template), encoding="utf-8")

    manager = object()
    operate = SimpleNamespace(service_manager=lambda: manager)

    monkeypatch.setattr(utils, "ask_password_if_needed", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not ask")))
    monkeypatch.setattr(utils, "configure_local_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(utils, "get_service", lambda m, t: {"manager": m, "template": t})

    result = utils.get_service_from_config(config_path, operate=operate)

    assert result["manager"] is manager


def test_validate_config_params_success() -> None:
    """Complete config should pass validation."""

    utils.validate_config_params({"a": "1", "b": "2"}, ["a", "b"])


def test_validate_config_params_raises_for_missing_and_empty() -> None:
    """Missing/empty required config values should raise ValueError."""

    with pytest.raises(ValueError, match="Missing required configuration parameters"):
        utils.validate_config_params({"a": "", "b": "ok"}, ["a", "c"])


def test_handle_missing_rpcs_all_present() -> None:
    """No prompt needed when all required RPC endpoints already exist."""

    config = {
        "optimism_rpc": "http://optimism",
        "base_rpc": "http://base",
        "mode_rpc": "http://mode",
    }

    mapping = utils.handle_missing_rpcs(config)

    assert mapping == {
        utils.Chain.OPTIMISM.value: "http://optimism",
        utils.Chain.BASE.value: "http://base",
        utils.Chain.MODE.value: "http://mode",
    }


def test_handle_missing_rpcs_prompts_until_non_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing RPCs should be requested and empty input retried."""

    config = {"optimism_rpc": "http://optimism"}
    answers = iter(["", "http://base", "http://mode"])

    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))

    mapping = utils.handle_missing_rpcs(config)

    assert mapping[utils.Chain.OPTIMISM.value] == "http://optimism"
    assert mapping[utils.Chain.BASE.value] == "http://base"
    assert mapping[utils.Chain.MODE.value] == "http://mode"
    assert config["base_rpc"] == "http://base"
    assert config["mode_rpc"] == "http://mode"


def test_input_with_default_value(monkeypatch: pytest.MonkeyPatch) -> None:
    """Helper should return user value or fallback default."""

    monkeypatch.setattr("builtins.input", lambda _prompt: "custom")
    assert utils.input_with_default_value("Prompt", "default") == "custom"

    monkeypatch.setattr("builtins.input", lambda _prompt: "")
    assert utils.input_with_default_value("Prompt", "default") == "default"
