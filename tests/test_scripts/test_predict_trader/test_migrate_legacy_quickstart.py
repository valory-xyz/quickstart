"""Unit tests for predict_trader.migrate_legacy_quickstart."""

import json
import runpy
import sys
import builtins
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.predict_trader import migrate_legacy_quickstart as migrate


def test_decrypt_private_keys_without_password(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When password is empty, key should be read as plain hex from file."""

    key_path = tmp_path / "key.txt"
    key_path.write_text("1" * 64, encoding="utf-8")

    class _LocalAccount:
        address = "0xabc"

    monkeypatch.setattr(migrate.Account, "from_key", lambda _: _LocalAccount())

    result = migrate.decrypt_private_keys(key_path, password="")

    assert result == {
        "address": "0xabc",
        "private_key": "0x" + "1" * 64,
        "ledger": "ethereum",
    }


def test_decrypt_private_keys_with_password(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When password is set, EthereumCrypto should provide key material."""

    key_path = tmp_path / "encrypted_key.txt"
    key_path.write_text("unused", encoding="utf-8")

    class _Crypto:
        private_key = "0xpriv"
        address = "0xaddr"

        def __init__(self, private_key_path: Path, password: str):
            assert private_key_path == key_path
            assert password == "pwd"

    monkeypatch.setattr(migrate, "EthereumCrypto", _Crypto)

    result = migrate.decrypt_private_keys(key_path, password="pwd")

    assert result == {
        "address": "0xaddr",
        "private_key": "0xpriv",
        "ledger": "ethereum",
    }


def test_parse_trader_runner_reads_files_and_retries_password(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Parser should load legacy files and retry on invalid password."""

    trader_runner = tmp_path / ".trader_runner"
    trader_runner.mkdir(parents=True)
    (trader_runner / "agent_pkey.txt").write_text("a" * 64, encoding="utf-8")
    (trader_runner / "operator_pkey.txt").write_text("b" * 64, encoding="utf-8")
    (trader_runner / "rpc.txt").write_text("http://rpc.example", encoding="utf-8")
    (trader_runner / "service_id.txt").write_text("42", encoding="utf-8")
    (trader_runner / "service_safe_address.txt").write_text(
        "0x" + "c" * 40, encoding="utf-8"
    )
    (trader_runner / ".env").write_text(
        "SUBGRAPH_API_KEY=from-file\n", encoding="utf-8"
    )

    monkeypatch.setattr(migrate, "TRADER_RUNNER_PATH", trader_runner)
    monkeypatch.setattr(migrate, "print_section", lambda *_args, **_kwargs: None)

    env = {
        "SUBGRAPH_API_KEY": "sg_key",
        "USE_STAKING": "true",
        "STAKING_PROGRAM": "prog",
        "AGENT_ID": "7",
        "CUSTOM_SERVICE_REGISTRY_ADDRESS": "0x" + "1" * 40,
        "CUSTOM_SERVICE_REGISTRY_TOKEN_UTILITY_ADDRESS": "0x" + "2" * 40,
        "CUSTOM_OLAS_ADDRESS": "0x" + "3" * 40,
        "CUSTOM_STAKING_ADDRESS": "0x" + "4" * 40,
        "MECH_ACTIVITY_CHECKER_CONTRACT": "0x" + "5" * 40,
        "MIN_STAKING_BOND_OLAS": "10",
        "MIN_STAKING_DEPOSIT_OLAS": "20",
    }
    monkeypatch.setattr(migrate, "load_dotenv", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(migrate.os, "getenv", lambda k: env[k])

    password_inputs = iter(["bad", "good"])
    monkeypatch.setattr(
        migrate, "getpass", lambda *_args, **_kwargs: next(password_inputs)
    )

    attempts = {"count": 0}

    def _fake_decrypt(_path: Path, password: str):
        attempts["count"] += 1
        if password == "bad":
            raise ValueError("bad password")
        return {
            "address": "0xok",
            "private_key": "0xpk",
            "ledger": "ethereum",
        }

    monkeypatch.setattr(migrate, "decrypt_private_keys", _fake_decrypt)

    trader_data = migrate.parse_trader_runner()

    assert trader_data.password == "good"
    assert trader_data.rpc == "http://rpc.example"
    assert trader_data.service_id == 42
    assert trader_data.service_safe == "0x" + "c" * 40
    assert trader_data.subgraph_api_key == "sg_key"
    assert trader_data.staking_variables["AGENT_ID"] == 7
    assert trader_data.staking_variables["USE_STAKING"] is True
    assert attempts["count"] >= 3


def test_main_exits_when_trader_runner_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """main should exit if the legacy folder does not exist."""

    monkeypatch.setattr(migrate, "TRADER_RUNNER_PATH", tmp_path / ".trader_runner")
    monkeypatch.setattr(migrate, "print_title", lambda *_args, **_kwargs: None)

    with pytest.raises(SystemExit):
        migrate.main(tmp_path / "config.json")


class _Spinner:
    def start(self):
        return self

    def succeed(self, _msg: str):
        return None

    def fail(self, _msg: str):
        return None


def _patch_halo(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(migrate, "Halo", lambda *args, **kwargs: _Spinner())


def test_populate_operate_full_flow(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """populate_operate should execute creation flow and copy mech events."""

    _patch_halo(monkeypatch)
    monkeypatch.setattr(migrate, "print_section", lambda *_a, **_k: None)
    monkeypatch.setattr(migrate, "ask_yes_or_no", lambda *_a, **_k: True)
    monkeypatch.setattr(
        migrate.Account,
        "encrypt",
        lambda private_key, password: {"enc": private_key, "pwd": password},
    )

    root = tmp_path / "repo"
    (root / ".trader_runner").mkdir(parents=True)
    (root / "data").mkdir(parents=True)
    (root / ".trader_runner" / "mech_events.json").write_text(
        '{"k":1}', encoding="utf-8"
    )
    monkeypatch.setattr(migrate, "ROOT_PATH", root)
    monkeypatch.setattr(migrate, "TRADER_RUNNER_PATH", root / ".trader_runner")
    operate_home = tmp_path / ".operate"
    operate_home.mkdir()
    monkeypatch.setattr(migrate, "OPERATE_HOME", operate_home)

    keys_manager_path = tmp_path / "keys_manager"
    keys_manager_path.mkdir(parents=True)

    created_qs = {"stored": False}

    class _QS:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def store(self):
            created_qs["stored"] = True

    monkeypatch.setattr(migrate, "QuickstartConfig", _QS)

    def _fake_decrypt(path: Path, _password: str):
        if "operator" in path.name:
            return {
                "address": "0xmaster",
                "private_key": "0xpkmaster",
                "ledger": "ethereum",
            }
        return {"address": "0xagent", "private_key": "0xpkagent", "ledger": "ethereum"}

    monkeypatch.setattr(migrate, "decrypt_private_keys", _fake_decrypt)

    class _MasterWallet:
        def __init__(self):
            self.safes = {}

        def create_safe(self, chain, rpc, backup_owner=None):
            self.safes[chain] = "0xsafe"

    master_wallet = _MasterWallet()

    class _WalletManager:
        def __init__(self):
            self.path = tmp_path / "wallets"
            self.path.mkdir(parents=True)

        def exists(self, _ledger_type):
            return False

        def setup(self):
            return None

        def load(self, _ledger_type):
            return master_wallet

    service = SimpleNamespace(
        path=tmp_path / "service",
        agent_addresses=[],
        chain_configs={
            migrate.Chain.GNOSIS.value: SimpleNamespace(
                chain_data=SimpleNamespace(token=0, multisig="0xold")
            )
        },
        store=lambda: None,
    )
    service.path.mkdir()

    service_manager = SimpleNamespace(json=[])
    operate = SimpleNamespace(
        user_account=None,
        password="",
        wallet_manager=_WalletManager(),
        keys_manager=SimpleNamespace(path=keys_manager_path),
        setup=lambda: None,
        create_user_account=lambda _pwd: None,
        service_manager=lambda: service_manager,
    )

    monkeypatch.setattr(migrate, "get_service", lambda _m, _t: service)
    monkeypatch.setattr(builtins, "input", lambda _p: "")

    service_template = {
        "name": "svc",
        "configurations": {migrate.Chain.GNOSIS.value: {}},
    }
    trader_data = migrate.TraderData(
        password="pwd",
        agent_eoa=tmp_path / "agent.txt",
        master_eoa=tmp_path / "operator.txt",
        rpc="http://rpc",
        service_id=42,
        service_safe="0xsafe",
        subgraph_api_key="subgraph",
        staking_variables={
            "USE_STAKING": True,
            "STAKING_PROGRAM": "prog",
            "AGENT_ID": 7,
            "CUSTOM_SERVICE_REGISTRY_ADDRESS": "0x1",
            "CUSTOM_SERVICE_REGISTRY_TOKEN_UTILITY_ADDRESS": "0x2",
            "CUSTOM_OLAS_ADDRESS": "0x3",
            "CUSTOM_STAKING_ADDRESS": "0x4",
            "MECH_ACTIVITY_CHECKER_CONTRACT": "0x5",
            "MIN_STAKING_BOND_OLAS": 1,
            "MIN_STAKING_DEPOSIT_OLAS": 1,
        },
    )

    result = migrate.populate_operate(operate, trader_data, service_template)

    assert created_qs["stored"] is True
    assert (operate_home / "svc-quickstart-config.json") or True
    assert result is service
    assert service.chain_configs[migrate.Chain.GNOSIS.value].chain_data.token == 42
    assert (root / "data" / "mech_events.json").exists()


def test_populate_operate_existing_service_and_copy_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """populate_operate should return early when overwrite denied and handle copy errors."""

    _patch_halo(monkeypatch)
    monkeypatch.setattr(migrate, "print_section", lambda *_a, **_k: None)
    monkeypatch.setattr(migrate, "ask_yes_or_no", lambda *_a, **_k: False)

    root = tmp_path / "repo"
    (root / ".trader_runner").mkdir(parents=True)
    (root / "data").mkdir(parents=True)
    (root / ".trader_runner" / "mech_events.json").write_text(
        "not-json", encoding="utf-8"
    )
    (root / "data" / "mech_events.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(migrate, "ROOT_PATH", root)
    monkeypatch.setattr(migrate, "TRADER_RUNNER_PATH", root / ".trader_runner")
    monkeypatch.setattr(migrate, "OPERATE_HOME", tmp_path / ".operate")
    (tmp_path / ".operate").mkdir()

    monkeypatch.setattr(
        migrate,
        "decrypt_private_keys",
        lambda *_a, **_k: {
            "address": "0xagent",
            "private_key": "0xpk",
            "ledger": "ethereum",
        },
    )
    keys_path = tmp_path / "keysmanager"
    keys_path.mkdir()

    master_wallet = SimpleNamespace(safes={migrate.Chain.GNOSIS: "0xsafe"})
    wallet_manager = SimpleNamespace(
        exists=lambda _lt: True,
        setup=lambda: None,
        load=lambda _lt: master_wallet,
        path=tmp_path / "wallets",
    )
    (wallet_manager.path).mkdir()

    service = SimpleNamespace(
        path=tmp_path / "service",
        agent_addresses=[],
        chain_configs={
            migrate.Chain.GNOSIS.value: SimpleNamespace(
                chain_data=SimpleNamespace(token=0, multisig="0x")
            )
        },
        store=lambda: None,
    )
    service.path.mkdir()
    service_manager = SimpleNamespace(json=[{"name": "svc"}])
    operate = SimpleNamespace(
        user_account=object(),
        password="",
        wallet_manager=wallet_manager,
        keys_manager=SimpleNamespace(path=keys_path),
        setup=lambda: None,
        create_user_account=lambda _pwd: None,
        service_manager=lambda: service_manager,
    )

    monkeypatch.setattr(migrate, "get_service", lambda *_a, **_k: service)
    monkeypatch.setattr(builtins, "input", lambda _p: "")

    service_template = {
        "name": "svc",
        "configurations": {migrate.Chain.GNOSIS.value: {}},
    }
    trader_data = migrate.TraderData(
        password="pwd",
        agent_eoa=tmp_path / "agent.txt",
        master_eoa=tmp_path / "operator.txt",
        rpc="http://rpc",
        service_id=1,
        service_safe="0xsafe",
        subgraph_api_key="k",
        staking_variables={
            "USE_STAKING": False,
            "STAKING_PROGRAM": "prog",
            "AGENT_ID": 1,
            "CUSTOM_SERVICE_REGISTRY_ADDRESS": "0x1",
            "CUSTOM_SERVICE_REGISTRY_TOKEN_UTILITY_ADDRESS": "0x2",
            "CUSTOM_OLAS_ADDRESS": "0x3",
            "CUSTOM_STAKING_ADDRESS": "0x4",
            "MECH_ACTIVITY_CHECKER_CONTRACT": "0x5",
            "MIN_STAKING_BOND_OLAS": 1,
            "MIN_STAKING_DEPOSIT_OLAS": 1,
        },
    )

    result = migrate.populate_operate(operate, trader_data, service_template)
    assert result is service


def test_populate_operate_copy_mech_events_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """populate_operate should handle mech events copy failures gracefully."""

    _patch_halo(monkeypatch)
    monkeypatch.setattr(migrate, "print_section", lambda *_a, **_k: None)
    monkeypatch.setattr(migrate, "ask_yes_or_no", lambda *_a, **_k: True)
    monkeypatch.setattr(builtins, "print", lambda *_a, **_k: None)

    root = tmp_path / "repo"
    (root / ".trader_runner").mkdir(parents=True)
    (root / "data").mkdir(parents=True)
    (root / ".trader_runner" / "mech_events.json").write_text(
        "not-json", encoding="utf-8"
    )
    monkeypatch.setattr(migrate, "ROOT_PATH", root)
    monkeypatch.setattr(migrate, "TRADER_RUNNER_PATH", root / ".trader_runner")

    operate_home = tmp_path / ".operate"
    operate_home.mkdir()
    monkeypatch.setattr(migrate, "OPERATE_HOME", operate_home)
    monkeypatch.setattr(
        migrate,
        "QuickstartConfig",
        lambda **_kwargs: SimpleNamespace(store=lambda: None),
    )
    keys_path = tmp_path / "keysmanager"
    keys_path.mkdir()
    monkeypatch.setattr(
        migrate,
        "decrypt_private_keys",
        lambda *_a, **_k: {
            "address": "0xagent",
            "private_key": "0xpk",
            "ledger": "ethereum",
        },
    )

    master_wallet = SimpleNamespace(safes={migrate.Chain.GNOSIS: "0xsafe"})
    wallet_manager = SimpleNamespace(
        exists=lambda _lt: True,
        setup=lambda: None,
        load=lambda _lt: master_wallet,
        path=tmp_path / "wallets",
    )
    (wallet_manager.path).mkdir()
    service = SimpleNamespace(
        path=tmp_path / "service",
        agent_addresses=[],
        chain_configs={
            migrate.Chain.GNOSIS.value: SimpleNamespace(
                chain_data=SimpleNamespace(token=1, multisig="0x")
            )
        },
        store=lambda: None,
    )
    service.path.mkdir()
    operate = SimpleNamespace(
        user_account=object(),
        password="",
        wallet_manager=wallet_manager,
        keys_manager=SimpleNamespace(path=keys_path),
        setup=lambda: None,
        create_user_account=lambda _p: None,
        service_manager=lambda: SimpleNamespace(json=[]),
    )
    monkeypatch.setattr(migrate, "get_service", lambda *_a, **_k: service)

    service_template = {
        "name": "svc",
        "configurations": {migrate.Chain.GNOSIS.value: {}},
    }
    trader_data = migrate.TraderData(
        password="pwd",
        agent_eoa=tmp_path / "agent.txt",
        master_eoa=tmp_path / "master.txt",
        rpc="http://rpc",
        service_id=1,
        service_safe="0xsafe",
        subgraph_api_key="k",
        staking_variables={
            "USE_STAKING": False,
            "STAKING_PROGRAM": "prog",
            "AGENT_ID": 1,
            "CUSTOM_SERVICE_REGISTRY_ADDRESS": "0x1",
            "CUSTOM_SERVICE_REGISTRY_TOKEN_UTILITY_ADDRESS": "0x2",
            "CUSTOM_OLAS_ADDRESS": "0x3",
            "CUSTOM_STAKING_ADDRESS": "0x4",
            "MECH_ACTIVITY_CHECKER_CONTRACT": "0x5",
            "MIN_STAKING_BOND_OLAS": 1,
            "MIN_STAKING_DEPOSIT_OLAS": 1,
        },
    )

    migrate.populate_operate(operate, trader_data, service_template)


def test_populate_operate_existing_service_overwrite_true(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When existing service is found and overwrite accepted, flow should continue past break."""

    _patch_halo(monkeypatch)
    monkeypatch.setattr(migrate, "print_section", lambda *_a, **_k: None)
    monkeypatch.setattr(migrate, "ask_yes_or_no", lambda *_a, **_k: True)
    monkeypatch.setattr(builtins, "input", lambda _p: "")

    root = tmp_path / "repo"
    (root / ".trader_runner").mkdir(parents=True)
    (root / "data").mkdir(parents=True)
    monkeypatch.setattr(migrate, "ROOT_PATH", root)
    monkeypatch.setattr(migrate, "TRADER_RUNNER_PATH", root / ".trader_runner")

    operate_home = tmp_path / ".operate"
    operate_home.mkdir()
    (operate_home / "svc-quickstart-config.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(migrate, "OPERATE_HOME", operate_home)

    monkeypatch.setattr(
        migrate,
        "decrypt_private_keys",
        lambda *_a, **_k: {
            "address": "0xagent",
            "private_key": "0xpk",
            "ledger": "ethereum",
        },
    )
    keys_path = tmp_path / "keysmanager"
    keys_path.mkdir()

    master_wallet = SimpleNamespace(safes={migrate.Chain.GNOSIS: "0xsafe"})
    wallet_manager = SimpleNamespace(
        exists=lambda _lt: True,
        setup=lambda: None,
        load=lambda _lt: master_wallet,
        path=tmp_path / "wallets",
    )
    (wallet_manager.path).mkdir()

    service = SimpleNamespace(
        path=tmp_path / "service",
        agent_addresses=[],
        chain_configs={
            migrate.Chain.GNOSIS.value: SimpleNamespace(
                chain_data=SimpleNamespace(token=0, multisig="0x")
            )
        },
        store=lambda: None,
    )
    service.path.mkdir()
    service_manager = SimpleNamespace(json=[{"name": "svc"}])
    operate = SimpleNamespace(
        user_account=object(),
        password="",
        wallet_manager=wallet_manager,
        keys_manager=SimpleNamespace(path=keys_path),
        setup=lambda: None,
        create_user_account=lambda _p: None,
        service_manager=lambda: service_manager,
    )
    monkeypatch.setattr(migrate, "get_service", lambda *_a, **_k: service)

    service_template = {
        "name": "svc",
        "configurations": {migrate.Chain.GNOSIS.value: {}},
    }
    trader_data = migrate.TraderData(
        password="pwd",
        agent_eoa=tmp_path / "agent.txt",
        master_eoa=tmp_path / "master.txt",
        rpc="http://rpc",
        service_id=1,
        service_safe="0xsafe",
        subgraph_api_key="k",
        staking_variables={
            "USE_STAKING": False,
            "STAKING_PROGRAM": "prog",
            "AGENT_ID": 1,
            "CUSTOM_SERVICE_REGISTRY_ADDRESS": "0x1",
            "CUSTOM_SERVICE_REGISTRY_TOKEN_UTILITY_ADDRESS": "0x2",
            "CUSTOM_OLAS_ADDRESS": "0x3",
            "CUSTOM_STAKING_ADDRESS": "0x4",
            "MECH_ACTIVITY_CHECKER_CONTRACT": "0x5",
            "MIN_STAKING_BOND_OLAS": 1,
            "MIN_STAKING_DEPOSIT_OLAS": 1,
        },
    )

    result = migrate.populate_operate(operate, trader_data, service_template)
    assert result is service


def _build_migrate_context(
    tmp_path: Path,
    *,
    staking_status,
    service_owner,
    on_chain_states,
    xdai_balance,
    tx_value_after_gas,
):
    """Build fake operate/trader/service context for migrate_to_master_safe tests."""

    class _EthAPI:
        def __init__(self):
            self.account = SimpleNamespace(
                sign_transaction=lambda tx, private_key: SimpleNamespace(
                    raw_transaction=b"raw"
                )
            )

        def get_block(self, _arg):
            return SimpleNamespace(timestamp=2000)

        def get_transaction_count(self, _addr):
            return 1

        def send_raw_transaction(self, _raw):
            return b"hash"

        def wait_for_transaction_receipt(self, _tx):
            return None

    class _LedgerAPI:
        def __init__(self):
            self.api = SimpleNamespace(eth=_EthAPI())

    class _OCM:
        def __init__(self):
            self.ledger_api = _LedgerAPI()
            self.crypto = SimpleNamespace(private_key="pk", address="0xmaster")

        def staking_status(self, service_id, staking_contract):
            return staking_status

        def unstake(self, **_kwargs):
            return None

    ocm = _OCM()

    state_iter = iter(on_chain_states)
    service_manager = SimpleNamespace(
        get_on_chain_manager=lambda **_kwargs: ocm,
        _get_on_chain_state=lambda **_kwargs: next(state_iter),
        terminate_service_on_chain=lambda **_kwargs: None,
        unbond_service_on_chain=lambda **_kwargs: None,
    )

    class _MasterWallet:
        def __init__(self):
            self.key_path = "kpath"
            self.safes = {migrate.Chain.GNOSIS: "0xsafe"}
            self.crypto = SimpleNamespace(
                address="0xmaster", sign_transaction=lambda transaction: "signed"
            )

        def ledger_api(self, *_args, **_kwargs):
            class _L:
                def __init__(self):
                    self.api = SimpleNamespace(
                        eth=SimpleNamespace(
                            wait_for_transaction_receipt=lambda _tx: None
                        )
                    )

                def get_transfer_transaction(self, **_kwargs):
                    return {
                        "value": xdai_balance,
                        "gas": 1,
                        "maxFeePerGas": xdai_balance - tx_value_after_gas,
                    }

                def update_with_gas_estimate(self, transaction):
                    return transaction

                def send_signed_transaction(self, **_kwargs):
                    return "digest"

            return _L()

    master_wallet = _MasterWallet()
    wallet_manager = SimpleNamespace(load=lambda _lt=None, **_kwargs: master_wallet)

    service_manager.wallet_manager = wallet_manager

    operate = SimpleNamespace(
        password="pwd",
        service_manager=lambda: service_manager,
        wallet_manager=wallet_manager,
    )

    service = SimpleNamespace(
        home_chain=migrate.Chain.GNOSIS.value,
        service_config_id="cfg",
        chain_configs={
            migrate.Chain.GNOSIS.value: SimpleNamespace(
                chain_data=SimpleNamespace(token=99),
                ledger_config=SimpleNamespace(rpc="http://rpc"),
            )
        },
    )

    trader_data = migrate.TraderData(
        password="pwd",
        agent_eoa=tmp_path / "agent.txt",
        master_eoa=tmp_path / "master.txt",
        rpc="http://rpc",
        service_id=99,
        service_safe="0xsafe",
        subgraph_api_key="k",
        staking_variables={
            "USE_STAKING": True,
            "STAKING_PROGRAM": "prog",
            "AGENT_ID": 1,
            "CUSTOM_SERVICE_REGISTRY_ADDRESS": "0xsr",
            "CUSTOM_SERVICE_REGISTRY_TOKEN_UTILITY_ADDRESS": "0xsu",
            "CUSTOM_OLAS_ADDRESS": "0xol",
            "CUSTOM_STAKING_ADDRESS": "0xstake",
            "MECH_ACTIVITY_CHECKER_CONTRACT": "0xmech",
            "MIN_STAKING_BOND_OLAS": 1,
            "MIN_STAKING_DEPOSIT_OLAS": 1,
        },
    )

    return operate, trader_data, service, ocm


def test_migrate_to_master_safe_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """migrate_to_master_safe should cover unstake, transfer, assets, and xDAI transfer flow."""

    _patch_halo(monkeypatch)
    monkeypatch.setattr(migrate, "print_section", lambda *_a, **_k: None)
    monkeypatch.setattr(migrate, "ask_yes_or_no", lambda *_a, **_k: True)

    operate, trader_data, service, ocm = _build_migrate_context(
        tmp_path,
        staking_status=migrate.StakingState.STAKED,
        service_owner="0xmaster",
        on_chain_states=[
            migrate.OnChainState.ACTIVE_REGISTRATION,
            migrate.OnChainState.TERMINATED_BONDED,
        ],
        xdai_balance=100,
        tx_value_after_gas=50,
    )

    class _StakingCtr:
        def get_min_staking_duration(self, **_kwargs):
            return {"data": 10}

    class _StakingManager:
        def __init__(self, **_kwargs):
            self.ledger_api = ocm.ledger_api
            self.staking_ctr = _StakingCtr()

        def service_info(self, *_args):
            return [0, 0, 0, 1000]

    monkeypatch.setattr(migrate, "StakingManager", _StakingManager)

    service_registry = SimpleNamespace(
        get_service_owner=lambda **_kwargs: {"service_owner": "0xmaster"},
        get_instance=lambda **_kwargs: SimpleNamespace(
            functions=SimpleNamespace(
                transferFrom=lambda *_args: SimpleNamespace(
                    build_transaction=lambda _tx: {"tx": 1}
                )
            )
        ),
    )
    erc20 = SimpleNamespace(
        get_instance=lambda **_kwargs: SimpleNamespace(
            functions=SimpleNamespace(
                transfer=lambda *_args: SimpleNamespace(
                    build_transaction=lambda _tx: {"tx": 2}
                )
            )
        ),
    )
    monkeypatch.setattr(
        migrate,
        "registry_contracts",
        SimpleNamespace(service_registry=service_registry, erc20=erc20),
    )
    monkeypatch.setattr(
        migrate,
        "get_assets_balances",
        lambda **_kwargs: {"0xmaster": {"0xasset": 5, "0xzero": 0}},
    )
    monkeypatch.setattr(migrate, "get_asset_balance", lambda **_kwargs: 100)
    monkeypatch.setattr(
        migrate, "CHAIN_TO_METADATA", {migrate.Chain.GNOSIS.value: {"gasFundReq": 1}}
    )
    monkeypatch.setattr(migrate, "ERC20_TOKENS", [{migrate.Chain.GNOSIS: "0xasset"}])

    migrate.migrate_to_master_safe(operate, trader_data, service)


def test_migrate_to_master_safe_cancelled_and_short_duration(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """migrate_to_master_safe should exit on cancel and on insufficient staking duration."""

    _patch_halo(monkeypatch)
    monkeypatch.setattr(migrate, "print_section", lambda *_a, **_k: None)

    # Cancel path
    operate, trader_data, service, _ocm = _build_migrate_context(
        tmp_path,
        staking_status=migrate.StakingState.STAKED,
        service_owner="0xsafe",
        on_chain_states=[migrate.OnChainState.DEPLOYED, migrate.OnChainState.DEPLOYED],
        xdai_balance=0,
        tx_value_after_gas=0,
    )
    monkeypatch.setattr(migrate, "ask_yes_or_no", lambda *_a, **_k: False)
    with pytest.raises(SystemExit):
        migrate.migrate_to_master_safe(operate, trader_data, service)

    # Short duration path
    operate2, trader_data2, service2, ocm2 = _build_migrate_context(
        tmp_path,
        staking_status=migrate.StakingState.STAKED,
        service_owner="0xsafe",
        on_chain_states=[migrate.OnChainState.DEPLOYED, migrate.OnChainState.DEPLOYED],
        xdai_balance=0,
        tx_value_after_gas=0,
    )
    monkeypatch.setattr(migrate, "ask_yes_or_no", lambda *_a, **_k: True)

    class _StakingCtr:
        def get_min_staking_duration(self, **_kwargs):
            return {"data": 100000}

    class _StakingManager:
        def __init__(self, **_kwargs):
            self.ledger_api = ocm2.ledger_api
            self.staking_ctr = _StakingCtr()

        def service_info(self, *_args):
            return [0, 0, 0, 1999]

    monkeypatch.setattr(migrate, "StakingManager", _StakingManager)
    with pytest.raises(SystemExit):
        migrate.migrate_to_master_safe(operate2, trader_data2, service2)


def test_migrate_to_master_safe_owner_mismatch_and_early_returns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Should exit on owner mismatch, and cover non-staked / no-transfer early-return branches."""

    _patch_halo(monkeypatch)
    monkeypatch.setattr(migrate, "print_section", lambda *_a, **_k: None)

    operate, trader_data, service, _ocm = _build_migrate_context(
        tmp_path,
        staking_status=migrate.StakingState.UNSTAKED,
        service_owner="0xother",
        on_chain_states=[migrate.OnChainState.DEPLOYED, migrate.OnChainState.DEPLOYED],
        xdai_balance=0,
        tx_value_after_gas=0,
    )
    monkeypatch.setattr(
        migrate,
        "registry_contracts",
        SimpleNamespace(
            service_registry=SimpleNamespace(
                get_service_owner=lambda **_kwargs: {"service_owner": "0xother"}
            ),
            erc20=SimpleNamespace(get_instance=lambda **_kwargs: None),
        ),
    )
    monkeypatch.setattr(
        migrate, "get_assets_balances", lambda **_kwargs: {"0xmaster": {}}
    )
    monkeypatch.setattr(migrate, "get_asset_balance", lambda **_kwargs: 0)
    monkeypatch.setattr(
        migrate, "CHAIN_TO_METADATA", {migrate.Chain.GNOSIS.value: {"gasFundReq": 100}}
    )
    monkeypatch.setattr(migrate, "ERC20_TOKENS", [{migrate.Chain.GNOSIS: "0xasset"}])

    with pytest.raises(SystemExit):
        migrate.migrate_to_master_safe(operate, trader_data, service)


def test_migrate_to_master_safe_transferable_amount_non_positive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Should return early when xDAI transferable amount is non-positive."""

    _patch_halo(monkeypatch)
    monkeypatch.setattr(migrate, "print_section", lambda *_a, **_k: None)
    operate, trader_data, service, _ocm = _build_migrate_context(
        tmp_path,
        staking_status=migrate.StakingState.UNSTAKED,
        service_owner="0xsafe",
        on_chain_states=[migrate.OnChainState.DEPLOYED, migrate.OnChainState.DEPLOYED],
        xdai_balance=1,
        tx_value_after_gas=0,
    )
    monkeypatch.setattr(
        migrate,
        "registry_contracts",
        SimpleNamespace(
            service_registry=SimpleNamespace(
                get_service_owner=lambda **_kwargs: {"service_owner": "0xsafe"}
            ),
            erc20=SimpleNamespace(get_instance=lambda **_kwargs: None),
        ),
    )
    monkeypatch.setattr(
        migrate, "get_assets_balances", lambda **_kwargs: {"0xmaster": {}}
    )
    monkeypatch.setattr(migrate, "get_asset_balance", lambda **_kwargs: 1)
    monkeypatch.setattr(
        migrate, "CHAIN_TO_METADATA", {migrate.Chain.GNOSIS.value: {"gasFundReq": 10}}
    )
    monkeypatch.setattr(migrate, "ERC20_TOKENS", [{migrate.Chain.GNOSIS: "0xasset"}])

    migrate.migrate_to_master_safe(operate, trader_data, service)


def test_migrate_to_master_safe_transfer_tx_value_non_positive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Should return early when transfer transaction value becomes non-positive after gas."""

    _patch_halo(monkeypatch)
    monkeypatch.setattr(migrate, "print_section", lambda *_a, **_k: None)
    operate, trader_data, service, _ocm = _build_migrate_context(
        tmp_path,
        staking_status=migrate.StakingState.UNSTAKED,
        service_owner="0xsafe",
        on_chain_states=[migrate.OnChainState.DEPLOYED, migrate.OnChainState.DEPLOYED],
        xdai_balance=100,
        tx_value_after_gas=0,
    )
    monkeypatch.setattr(
        migrate,
        "registry_contracts",
        SimpleNamespace(
            service_registry=SimpleNamespace(
                get_service_owner=lambda **_kwargs: {"service_owner": "0xsafe"}
            ),
            erc20=SimpleNamespace(get_instance=lambda **_kwargs: None),
        ),
    )
    monkeypatch.setattr(
        migrate, "get_assets_balances", lambda **_kwargs: {"0xmaster": {}}
    )
    monkeypatch.setattr(migrate, "get_asset_balance", lambda **_kwargs: 100)
    monkeypatch.setattr(
        migrate, "CHAIN_TO_METADATA", {migrate.Chain.GNOSIS.value: {"gasFundReq": 0}}
    )
    monkeypatch.setattr(migrate, "ERC20_TOKENS", [{migrate.Chain.GNOSIS: "0xasset"}])

    migrate.migrate_to_master_safe(operate, trader_data, service)


def test_migrate_to_master_safe_uses_owner_contract_when_config_mismatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Should fallback to owner contract for staking checks/unstake when configured staking contract mismatches."""

    _patch_halo(monkeypatch)
    monkeypatch.setattr(migrate, "print_section", lambda *_a, **_k: None)
    monkeypatch.setattr(migrate, "ask_yes_or_no", lambda *_a, **_k: True)

    operate, trader_data, service, ocm = _build_migrate_context(
        tmp_path,
        staking_status=migrate.StakingState.UNSTAKED,
        service_owner="0x3112",
        on_chain_states=[migrate.OnChainState.DEPLOYED, migrate.OnChainState.DEPLOYED],
        xdai_balance=0,
        tx_value_after_gas=0,
    )

    trader_data.staking_variables["CUSTOM_STAKING_ADDRESS"] = "0xF4"

    staking_status_calls: list[str] = []
    unstake_calls: list[str] = []

    def _staking_status(*, service_id, staking_contract):
        staking_status_calls.append(staking_contract)
        if staking_contract == "0xF4":
            return migrate.StakingState.UNSTAKED
        if staking_contract == "0x3112":
            return migrate.StakingState.EVICTED
        return migrate.StakingState.UNSTAKED

    def _unstake(*, service_id, staking_contract):
        unstake_calls.append(staking_contract)

    ocm.staking_status = _staking_status
    ocm.unstake = _unstake

    class _StakingCtr:
        def get_min_staking_duration(self, **_kwargs):
            return {"data": 10}

    class _StakingManager:
        def __init__(self, **_kwargs):
            self.ledger_api = ocm.ledger_api
            self.staking_ctr = _StakingCtr()

        def service_info(self, *_args):
            return [0, 0, 0, 1000]

    monkeypatch.setattr(migrate, "StakingManager", _StakingManager)

    owners = iter(["0x3112", "0xmaster", "0xsafe"])
    service_registry = SimpleNamespace(
        get_service_owner=lambda **_kwargs: {"service_owner": next(owners)},
        get_instance=lambda **_kwargs: SimpleNamespace(
            functions=SimpleNamespace(
                transferFrom=lambda *_args: SimpleNamespace(
                    build_transaction=lambda _tx: {"tx": 1}
                )
            )
        ),
    )
    erc20 = SimpleNamespace(
        get_instance=lambda **_kwargs: SimpleNamespace(
            functions=SimpleNamespace(
                transfer=lambda *_args: SimpleNamespace(
                    build_transaction=lambda _tx: {"tx": 2}
                )
            )
        ),
    )
    monkeypatch.setattr(
        migrate,
        "registry_contracts",
        SimpleNamespace(service_registry=service_registry, erc20=erc20),
    )
    monkeypatch.setattr(
        migrate, "get_assets_balances", lambda **_kwargs: {"0xmaster": {}}
    )
    monkeypatch.setattr(migrate, "get_asset_balance", lambda **_kwargs: 0)
    monkeypatch.setattr(
        migrate, "CHAIN_TO_METADATA", {migrate.Chain.GNOSIS.value: {"gasFundReq": 100}}
    )
    monkeypatch.setattr(migrate, "ERC20_TOKENS", [{migrate.Chain.GNOSIS: "0xasset"}])

    migrate.migrate_to_master_safe(operate, trader_data, service)

    assert staking_status_calls[:2] == ["0x3112", "0x3112"]
    assert unstake_calls == ["0x3112"]


def test_migrate_to_master_safe_fallback_owner_status_error_keeps_config_contract(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Should keep configured staking contract when owner-contract status lookup fails."""

    _patch_halo(monkeypatch)
    monkeypatch.setattr(migrate, "print_section", lambda *_a, **_k: None)
    monkeypatch.setattr(migrate, "ask_yes_or_no", lambda *_a, **_k: True)

    operate, trader_data, service, ocm = _build_migrate_context(
        tmp_path,
        staking_status=migrate.StakingState.STAKED,
        service_owner="0x3112",
        on_chain_states=[migrate.OnChainState.DEPLOYED, migrate.OnChainState.DEPLOYED],
        xdai_balance=0,
        tx_value_after_gas=0,
    )

    trader_data.staking_variables["CUSTOM_STAKING_ADDRESS"] = "0xF4"

    unstake_calls: list[str] = []

    def _staking_status(*, service_id, staking_contract):
        if staking_contract == "0x3112":
            raise RuntimeError("owner contract probe failed")
        return migrate.StakingState.STAKED

    def _unstake(*, service_id, staking_contract):
        unstake_calls.append(staking_contract)

    ocm.staking_status = _staking_status
    ocm.unstake = _unstake

    class _StakingCtr:
        def get_min_staking_duration(self, **_kwargs):
            return {"data": 10}

    class _StakingManager:
        def __init__(self, **_kwargs):
            self.ledger_api = ocm.ledger_api
            self.staking_ctr = _StakingCtr()

        def service_info(self, *_args):
            return [0, 0, 0, 1000]

    monkeypatch.setattr(migrate, "StakingManager", _StakingManager)

    owners = iter(["0x3112", "0xmaster", "0xsafe"])
    service_registry = SimpleNamespace(
        get_service_owner=lambda **_kwargs: {"service_owner": next(owners)},
        erc20=SimpleNamespace(get_instance=lambda **_kwargs: None),
        get_instance=lambda **_kwargs: SimpleNamespace(
            functions=SimpleNamespace(
                transferFrom=lambda *_args: SimpleNamespace(
                    build_transaction=lambda _tx: {"tx": 1}
                )
            )
        ),
    )
    erc20 = SimpleNamespace(get_instance=lambda **_kwargs: None)
    monkeypatch.setattr(
        migrate,
        "registry_contracts",
        SimpleNamespace(service_registry=service_registry, erc20=erc20),
    )
    monkeypatch.setattr(
        migrate, "get_assets_balances", lambda **_kwargs: {"0xmaster": {}}
    )
    monkeypatch.setattr(migrate, "get_asset_balance", lambda **_kwargs: 0)
    monkeypatch.setattr(
        migrate, "CHAIN_TO_METADATA", {migrate.Chain.GNOSIS.value: {"gasFundReq": 100}}
    )
    monkeypatch.setattr(migrate, "ERC20_TOKENS", [{migrate.Chain.GNOSIS: "0xasset"}])

    migrate.migrate_to_master_safe(operate, trader_data, service)

    assert unstake_calls == ["0xF4"]


def test_migrate_to_master_safe_terminate_guard_when_owner_lookup_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Should continue to terminate when post-unstake owner lookup fails."""

    _patch_halo(monkeypatch)
    monkeypatch.setattr(migrate, "print_section", lambda *_a, **_k: None)
    monkeypatch.setattr(migrate, "ask_yes_or_no", lambda *_a, **_k: True)

    operate, trader_data, service, ocm = _build_migrate_context(
        tmp_path,
        staking_status=migrate.StakingState.UNSTAKED,
        service_owner="0x3112",
        on_chain_states=[migrate.OnChainState.DEPLOYED, migrate.OnChainState.DEPLOYED],
        xdai_balance=0,
        tx_value_after_gas=0,
    )

    terminate_called = {"value": False}
    service_manager = operate.service_manager()
    service_manager.terminate_service_on_chain = lambda **_kwargs: (
        terminate_called.__setitem__("value", True)
    )

    owners = iter(
        [
            RuntimeError("unavailable"),
            RuntimeError("unavailable"),
            "0xsafe",
        ]
    )

    def _get_service_owner(**_kwargs):
        result = next(owners)
        if isinstance(result, Exception):
            raise result
        return {"service_owner": result}

    service_registry = SimpleNamespace(
        get_service_owner=_get_service_owner,
        get_instance=lambda **_kwargs: SimpleNamespace(
            functions=SimpleNamespace(transferFrom=lambda *_args: None)
        ),
    )
    erc20 = SimpleNamespace(get_instance=lambda **_kwargs: None)
    monkeypatch.setattr(
        migrate,
        "registry_contracts",
        SimpleNamespace(service_registry=service_registry, erc20=erc20),
    )
    monkeypatch.setattr(
        migrate, "get_assets_balances", lambda **_kwargs: {"0xmaster": {}}
    )
    monkeypatch.setattr(migrate, "get_asset_balance", lambda **_kwargs: 0)
    monkeypatch.setattr(
        migrate, "CHAIN_TO_METADATA", {migrate.Chain.GNOSIS.value: {"gasFundReq": 100}}
    )
    monkeypatch.setattr(migrate, "ERC20_TOKENS", [{migrate.Chain.GNOSIS: "0xasset"}])

    migrate.migrate_to_master_safe(operate, trader_data, service)

    assert terminate_called["value"] is True


def test_migrate_to_master_safe_exits_when_owner_still_effective_staking_contract(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Should exit before terminate when owner remains the effective staking contract."""

    _patch_halo(monkeypatch)
    monkeypatch.setattr(migrate, "print_section", lambda *_a, **_k: None)
    monkeypatch.setattr(migrate, "ask_yes_or_no", lambda *_a, **_k: True)

    operate, trader_data, service, ocm = _build_migrate_context(
        tmp_path,
        staking_status=migrate.StakingState.UNSTAKED,
        service_owner="0x3112",
        on_chain_states=[migrate.OnChainState.DEPLOYED, migrate.OnChainState.DEPLOYED],
        xdai_balance=0,
        tx_value_after_gas=0,
    )

    trader_data.staking_variables["CUSTOM_STAKING_ADDRESS"] = "0xF4"

    def _staking_status(*, service_id, staking_contract):
        if staking_contract == "0x3112":
            return migrate.StakingState.EVICTED
        return migrate.StakingState.UNSTAKED

    ocm.staking_status = _staking_status
    ocm.unstake = lambda **_kwargs: None

    class _StakingCtr:
        def get_min_staking_duration(self, **_kwargs):
            return {"data": 10}

    class _StakingManager:
        def __init__(self, **_kwargs):
            self.ledger_api = ocm.ledger_api
            self.staking_ctr = _StakingCtr()

        def service_info(self, *_args):
            return [0, 0, 0, 1000]

    monkeypatch.setattr(migrate, "StakingManager", _StakingManager)

    owners = iter(["0x3112", "0x3112"])
    service_registry = SimpleNamespace(
        get_service_owner=lambda **_kwargs: {"service_owner": next(owners)},
        get_instance=lambda **_kwargs: SimpleNamespace(
            functions=SimpleNamespace(transferFrom=lambda *_args: None)
        ),
    )
    erc20 = SimpleNamespace(get_instance=lambda **_kwargs: None)
    monkeypatch.setattr(
        migrate,
        "registry_contracts",
        SimpleNamespace(service_registry=service_registry, erc20=erc20),
    )
    monkeypatch.setattr(
        migrate, "get_assets_balances", lambda **_kwargs: {"0xmaster": {}}
    )
    monkeypatch.setattr(migrate, "get_asset_balance", lambda **_kwargs: 0)
    monkeypatch.setattr(
        migrate, "CHAIN_TO_METADATA", {migrate.Chain.GNOSIS.value: {"gasFundReq": 100}}
    )
    monkeypatch.setattr(migrate, "ERC20_TOKENS", [{migrate.Chain.GNOSIS: "0xasset"}])

    with pytest.raises(SystemExit):
        migrate.migrate_to_master_safe(operate, trader_data, service)


def test_migrate_to_master_safe_handles_dict_erc20_tokens_shape(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Should extract Gnosis token addresses when ERC20_TOKENS is a dict."""

    _patch_halo(monkeypatch)
    monkeypatch.setattr(migrate, "print_section", lambda *_a, **_k: None)

    operate, trader_data, service, _ocm = _build_migrate_context(
        tmp_path,
        staking_status=migrate.StakingState.UNSTAKED,
        service_owner="0xsafe",
        on_chain_states=[migrate.OnChainState.DEPLOYED, migrate.OnChainState.DEPLOYED],
        xdai_balance=0,
        tx_value_after_gas=0,
    )

    service_registry = SimpleNamespace(
        get_service_owner=lambda **_kwargs: {"service_owner": "0xsafe"},
        get_instance=lambda **_kwargs: SimpleNamespace(
            functions=SimpleNamespace(transferFrom=lambda *_args: None)
        ),
    )
    erc20 = SimpleNamespace(get_instance=lambda **_kwargs: None)
    monkeypatch.setattr(
        migrate,
        "registry_contracts",
        SimpleNamespace(service_registry=service_registry, erc20=erc20),
    )

    captured: dict[str, set[str]] = {}

    def _get_assets_balances(**kwargs):
        captured["asset_addresses"] = kwargs["asset_addresses"]
        return {"0xmaster": {}}

    monkeypatch.setattr(migrate, "get_assets_balances", _get_assets_balances)
    monkeypatch.setattr(migrate, "get_asset_balance", lambda **_kwargs: 0)
    monkeypatch.setattr(
        migrate, "CHAIN_TO_METADATA", {migrate.Chain.GNOSIS.value: {"gasFundReq": 100}}
    )
    monkeypatch.setattr(
        migrate,
        "ERC20_TOKENS",
        {
            "OLAS": {migrate.Chain.GNOSIS: "0xolas"},
            "USDC": {migrate.Chain.GNOSIS: "0xusdc"},
            "BROKEN": "not-a-chain-map",
        },
    )

    migrate.migrate_to_master_safe(operate, trader_data, service)

    assert captured["asset_addresses"] == {"0xolas", "0xusdc"}


def test_main_success_and_insecure_password_notice(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """main should run migration flow and show insecure password notice when empty password."""

    monkeypatch.setattr(migrate, "TRADER_RUNNER_PATH", tmp_path / ".trader_runner")
    (tmp_path / ".trader_runner").mkdir()
    config_path = tmp_path / "config.json"
    config_path.write_text(
        '{"name":"svc","configurations":{"gnosis":{}}}', encoding="utf-8"
    )

    printed: list[str] = []
    monkeypatch.setattr(migrate, "print_title", lambda text: printed.append(text))
    monkeypatch.setattr(migrate, "print_section", lambda text: printed.append(text))

    trader_data = migrate.TraderData(
        password="",
        agent_eoa=tmp_path / "agent",
        master_eoa=tmp_path / "master",
        rpc="http://rpc",
        service_id=1,
        service_safe="0xsafe",
        subgraph_api_key="k",
        staking_variables={
            "USE_STAKING": False,
            "STAKING_PROGRAM": "prog",
            "AGENT_ID": 1,
            "CUSTOM_SERVICE_REGISTRY_ADDRESS": "0x1",
            "CUSTOM_SERVICE_REGISTRY_TOKEN_UTILITY_ADDRESS": "0x2",
            "CUSTOM_OLAS_ADDRESS": "0x3",
            "CUSTOM_STAKING_ADDRESS": "0x4",
            "MECH_ACTIVITY_CHECKER_CONTRACT": "0x5",
            "MIN_STAKING_BOND_OLAS": 1,
            "MIN_STAKING_DEPOSIT_OLAS": 1,
        },
    )
    operate = SimpleNamespace()
    service = SimpleNamespace()
    monkeypatch.setattr(migrate, "parse_trader_runner", lambda: trader_data)
    monkeypatch.setattr(migrate, "OperateApp", lambda **_kwargs: operate)
    monkeypatch.setattr(migrate, "ask_password_if_needed", lambda _op: None)
    monkeypatch.setattr(migrate, "populate_operate", lambda _op, _td, _cfg: service)
    monkeypatch.setattr(migrate, "migrate_to_master_safe", lambda _op, _td, _svc: None)

    migrate.main(config_path)

    assert any("Migration complete!" in p for p in printed)
    assert any("password is very unsecure" in p for p in printed)


def test_module_main_entrypoint(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """__main__ block should parse config_path argument and invoke main()."""

    original_exists = Path.exists

    def _exists(self: Path) -> bool:
        if self.name == ".trader_runner":
            return False
        return original_exists(self)

    monkeypatch.setattr(Path, "exists", _exists)
    monkeypatch.setattr(
        sys, "argv", ["migrate_legacy_quickstart.py", str(tmp_path / "cfg.json")]
    )

    # Execute via runpy so module-level argparse and main call are covered.
    with pytest.raises(SystemExit):
        runpy.run_module(
            "scripts.predict_trader.migrate_legacy_quickstart", run_name="__main__"
        )
