"""TLS material extraction and configuration stay offline and do not log secrets."""

from __future__ import annotations

import logging

import pytest

import xbbg
from xbbg import _engine

CLIENT = b"\x30\0\xffsynthetic-client-secret"
TRUST = b"\x30\0\xfesynthetic-trust-secret"
PASSWORD = "synthetic-password-secret"


@pytest.mark.parametrize(
    ("credentials", "trust"),
    [("fixtures/client.p12", "fixtures/trust.p7"), (CLIENT, TRUST)],
    ids=["paths", "bytes"],
)
def test_tls_material_round_trips_through_constructor_and_setters(credentials, trust):
    config = xbbg.EngineConfig(
        tls_client_credentials=credentials,
        tls_trust_material=trust,
        tls_client_credentials_password=PASSWORD,
    )
    assert config.tls_client_credentials == credentials
    assert config.tls_trust_material == trust
    assert type(config.tls_client_credentials) is type(credentials)
    assert type(config.tls_trust_material) is type(trust)

    config.tls_client_credentials = None
    config.tls_trust_material = None
    assert config.tls_client_credentials is None
    assert config.tls_trust_material is None
    config.tls_client_credentials = credentials
    config.tls_trust_material = trust
    assert config.tls_client_credentials == credentials
    assert config.tls_trust_material == trust
    assert "synthetic" not in repr(config)


def test_configure_accepts_tls_bytes_and_keeps_logs_redacted(monkeypatch, caplog):
    monkeypatch.setattr(_engine, "_config", None)
    monkeypatch.setattr(_engine, "_engine", None)
    with caplog.at_level(logging.INFO, logger="xbbg._engine"):
        xbbg.configure(
            tls_client_credentials=CLIENT,
            tls_trust_material=TRUST,
            tls_client_credentials_password=PASSWORD,
        )
        config = _engine._config
        assert config is not None
        assert config.tls_client_credentials == CLIENT
        assert config.tls_trust_material == TRUST
        # The config-plus-overrides route uses the native property setters.
        xbbg.configure(config, tls_client_credentials=CLIENT, tls_trust_material=TRUST)
    assert _engine._config is config
    assert _engine._engine is None
    assert "synthetic" not in caplog.text
