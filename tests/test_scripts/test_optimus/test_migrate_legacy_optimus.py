"""Unit tests for optimus.migrate_legacy_optimus."""

import json
import runpy
import sys
from pathlib import Path

import pytest

from scripts.optimus import migrate_legacy_optimus as migrate


def test_parse_optimus_config_happy_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	"""Should parse config and derive staking/principal settings."""

	optimus_path = tmp_path / ".optimus"
	optimus_path.mkdir(parents=True)
	(optimus_path / "local_config.json").write_text(
		json.dumps(
			{
				"tenderly_access_key": "a",
				"tenderly_account_slug": "b",
				"tenderly_project_slug": "c",
				"coingecko_api_key": "d",
				"use_staking": True,
			}
		),
		encoding="utf-8",
	)

	monkeypatch.setattr(migrate, "OPTIMUS_PATH", optimus_path)
	monkeypatch.setattr(
		migrate,
		"handle_missing_rpcs",
		lambda _cfg: {"optimistic": "http://optimism", "base": "http://base"},
	)
	monkeypatch.setattr(migrate, "print_section", lambda *_args, **_kwargs: None)

	parsed = migrate.parse_optimus_config()

	assert parsed.use_staking is True
	assert parsed.staking_program_id == "optimus_alpha"
	assert parsed.principal_chain == "optimism"
	assert parsed.rpc["base"] == "http://base"


def test_parse_optimus_config_missing_required_fields(
	monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
	"""Missing required keys should raise ValueError."""

	optimus_path = tmp_path / ".optimus"
	optimus_path.mkdir(parents=True)
	(optimus_path / "local_config.json").write_text(json.dumps({"use_staking": False}), encoding="utf-8")
	monkeypatch.setattr(migrate, "OPTIMUS_PATH", optimus_path)
	monkeypatch.setattr(migrate, "print_section", lambda *_args, **_kwargs: None)

	with pytest.raises(ValueError, match="Missing required configuration parameters"):
		migrate.parse_optimus_config()


def test_copy_optimus_to_operate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	"""Should copy all files except local_config.json."""

	optimus_path = tmp_path / ".optimus"
	operate_home = tmp_path / ".operate"
	(optimus_path / "nested").mkdir(parents=True)
	(optimus_path / "local_config.json").write_text("{}", encoding="utf-8")
	(optimus_path / "state.json").write_text("{\"x\":1}", encoding="utf-8")
	(optimus_path / "nested" / "a.txt").write_text("a", encoding="utf-8")

	monkeypatch.setattr(migrate, "OPTIMUS_PATH", optimus_path)
	monkeypatch.setattr(migrate, "OPERATE_HOME", operate_home)
	monkeypatch.setattr(migrate, "print_section", lambda *_args, **_kwargs: None)

	migrate.copy_optimus_to_operate()

	assert (operate_home / "state.json").exists()
	assert (operate_home / "nested" / "a.txt").exists()
	assert not (operate_home / "local_config.json").exists()


def test_create_operate_config_updates_service_and_stores_qs(
	monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
	"""Should rename service name and persist QuickstartConfig."""

	operate_home = tmp_path / ".operate"
	services_dir = operate_home / "services" / "svc"
	services_dir.mkdir(parents=True)
	config_path = services_dir / "config.json"
	config_path.write_text(json.dumps({"name": "valory/optimus"}), encoding="utf-8")

	monkeypatch.setattr(migrate, "OPERATE_HOME", operate_home)
	monkeypatch.setattr(migrate, "print_section", lambda *_args, **_kwargs: None)

	stored = {"called": False}

	class _QS:
		def __init__(self, **kwargs):
			self.kwargs = kwargs

		def store(self):
			stored["called"] = True
			stored["kwargs"] = self.kwargs

	monkeypatch.setattr(migrate, "QuickstartConfig", _QS)

	optimus_config = migrate.OptimusConfig(
		rpc={"optimistic": "http://optimism"},
		tenderly_access_key="a",
		tenderly_account_slug="b",
		tenderly_project_slug="c",
		coingecko_api_key="d",
		use_staking=False,
		staking_program_id="no_staking",
		principal_chain="optimistic",
	)

	migrate.create_operate_config(optimus_config, "my/optimus")

	updated_config = json.loads(config_path.read_text(encoding="utf-8"))
	assert updated_config["name"] == "my/optimus"
	assert stored["called"] is True
	assert stored["kwargs"]["staking_program_id"] == "no_staking"


def test_main_returns_when_optimus_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	"""If .optimus is missing, main should return cleanly."""

	monkeypatch.setattr(migrate, "OPTIMUS_PATH", tmp_path / ".optimus")
	monkeypatch.setattr(migrate, "print_title", lambda *_args, **_kwargs: None)

	config = tmp_path / "cfg.json"
	config.write_text(json.dumps({"name": "my/optimus"}), encoding="utf-8")

	# Should not raise.
	migrate.main(config)


def test_create_operate_config_skips_missing_and_non_matching_service_dirs(
	monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
	"""Service dirs without config or non-optimus names should be skipped."""

	operate_home = tmp_path / ".operate"
	services = operate_home / "services"
	(services / "missing_config").mkdir(parents=True)
	non_match = services / "non_matching"
	non_match.mkdir(parents=True)
	(non_match / "config.json").write_text(json.dumps({"name": "other/service"}), encoding="utf-8")

	monkeypatch.setattr(migrate, "OPERATE_HOME", operate_home)
	monkeypatch.setattr(migrate, "print_section", lambda *_args, **_kwargs: None)

	stored = {"called": False}

	class _QS:
		def __init__(self, **kwargs):
			self.kwargs = kwargs

		def store(self):
			stored["called"] = True
			stored["kwargs"] = self.kwargs

	monkeypatch.setattr(migrate, "QuickstartConfig", _QS)

	optimus_config = migrate.OptimusConfig(
		rpc={"optimism": "http://optimism"},
		tenderly_access_key="a",
		tenderly_account_slug="b",
		tenderly_project_slug="c",
		coingecko_api_key="d",
		use_staking=False,
		staking_program_id="no_staking",
		principal_chain="optimism",
	)

	migrate.create_operate_config(optimus_config, "my/optimus")

	assert stored["called"] is True
	updated = json.loads((non_match / "config.json").read_text(encoding="utf-8"))
	assert updated["name"] == "other/service"


def test_main_success_calls_all_steps(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	"""Main should parse config and invoke parse/copy/create sequence."""

	optimus_path = tmp_path / ".optimus"
	optimus_path.mkdir(parents=True)
	monkeypatch.setattr(migrate, "OPTIMUS_PATH", optimus_path)
	monkeypatch.setattr(migrate, "print_title", lambda *_args, **_kwargs: None)

	config_path = tmp_path / "cfg.json"
	config_path.write_text(json.dumps({"name": "my/optimus"}), encoding="utf-8")

	calls: list[str] = []
	optimus_config = migrate.OptimusConfig(
		rpc={"optimism": "http://optimism"},
		tenderly_access_key="a",
		tenderly_account_slug="b",
		tenderly_project_slug="c",
		coingecko_api_key="d",
		use_staking=False,
		staking_program_id="no_staking",
		principal_chain="optimism",
	)

	monkeypatch.setattr(migrate, "parse_optimus_config", lambda: calls.append("parse") or optimus_config)
	monkeypatch.setattr(migrate, "copy_optimus_to_operate", lambda: calls.append("copy"))
	monkeypatch.setattr(migrate, "create_operate_config", lambda cfg, name: calls.append(f"create:{name}:{cfg.principal_chain}"))
	monkeypatch.setattr(migrate, "print_section", lambda msg: calls.append(f"section:{msg}"))

	migrate.main(config_path)

	assert calls[0] == "parse"
	assert "copy" in calls
	assert "create:my/optimus:optimism" in calls
	assert any(c.startswith("section:Migration completed successfully") for c in calls)


def test_main_prints_and_reraises_on_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	"""Main should print error and re-raise exceptions from migration steps."""

	optimus_path = tmp_path / ".optimus"
	optimus_path.mkdir(parents=True)
	monkeypatch.setattr(migrate, "OPTIMUS_PATH", optimus_path)
	monkeypatch.setattr(migrate, "print_title", lambda *_args, **_kwargs: None)

	config_path = tmp_path / "cfg.json"
	config_path.write_text(json.dumps({"name": "my/optimus"}), encoding="utf-8")

	monkeypatch.setattr(migrate, "parse_optimus_config", lambda: (_ for _ in ()).throw(RuntimeError("boom")))

	printed: list[str] = []
	monkeypatch.setattr("builtins.print", lambda *args, **kwargs: printed.append(" ".join(str(a) for a in args)))

	with pytest.raises(RuntimeError, match="boom"):
		migrate.main(config_path)

	assert any("Error during migration: boom" in line for line in printed)


def test_module_main_entrypoint(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
	"""__main__ block should execute argument parsing path without crashing."""

	config_path = tmp_path / "cfg.json"
	config_path.write_text("{}", encoding="utf-8")

	monkeypatch.setattr(sys, "argv", ["migrate_legacy_optimus.py", str(config_path)])

	# Executes parser.add_argument/parse_args and main invocation in module namespace.
	runpy.run_module("scripts.optimus.migrate_legacy_optimus", run_name="__main__")
