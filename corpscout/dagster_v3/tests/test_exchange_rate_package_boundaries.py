from __future__ import annotations

import importlib

import pytest


def test_exchange_rate_runtime_package_is_top_level() -> None:
    exchange_rates = importlib.import_module("exchange_rates")
    client_module = importlib.import_module("exchange_rates.client")

    assert hasattr(exchange_rates, "ExchangeRateClient")
    assert hasattr(client_module, "ExchangeRateClient")


def test_exchange_rate_runtime_package_is_not_under_dagster_namespace() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("dagster_v3.exchange_rates")

    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("dagster_v3.exchange_rates.client")
