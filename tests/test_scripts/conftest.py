"""Shared pytest fixtures for realistic script tests."""

from __future__ import annotations

import socket
from typing import Any

import pytest


@pytest.fixture(autouse=True)
def block_external_network(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    """Disallow accidental external network access in script tests.

    Tests should mock only true network boundaries (e.g., requests/web3 calls).
    Allow localhost so in-process tooling remains usable.
    """

    # Keep real networking for e2e/integration tests.
    if request.node.get_closest_marker("e2e") or request.node.get_closest_marker(
        "integration"
    ):
        return

    original_connect = socket.socket.connect

    def guarded_connect(sock: socket.socket, address: Any) -> Any:
        host = address[0] if isinstance(address, tuple) and address else None
        if host in {"127.0.0.1", "localhost", "::1"}:
            return original_connect(sock, address)
        raise RuntimeError(
            f"External network call blocked during tests: connect({address!r})"
        )

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
