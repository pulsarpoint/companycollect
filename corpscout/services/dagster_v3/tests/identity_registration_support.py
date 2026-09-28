"""Disposable PostgreSQL configuration for identity publication tests."""

import pytest


@pytest.fixture
def identity_postgres(processing_postgres_url, monkeypatch):
    monkeypatch.setenv("PROCESSING_PG_URL", processing_postgres_url)
    monkeypatch.setenv("INVENTORY_LOCK_TIMEOUT_MS", "30000")
    return processing_postgres_url
