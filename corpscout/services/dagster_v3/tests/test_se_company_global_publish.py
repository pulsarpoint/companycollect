"""Global publication visits every bucket and retains the existing fold's run identity."""

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from dagster_v3.defs.se_company.basic_info import assets as info
from dagster_v3.defs.se_company.financial import assets as finance
from dagster_v3.defs.se_company.basic_info import llm
from dagster_v3.defs.se_company.address import assets as address


@pytest.mark.parametrize("module,asset,config", [
    (info, info.se_company_basic_info_publish, info.BasicInfoFoldConfig),
    (finance, finance.se_company_financial_publish, finance.FinancialFoldConfig),
])
def test_global_publication_folds_all_buckets_once(module, asset, config, monkeypatch) -> None:
    client = object()
    resource = SimpleNamespace(get_connection=lambda: nullcontext(client))
    context = SimpleNamespace(run_id="global-run", log=Mock())
    exists = Mock()
    fold = Mock(return_value=SimpleNamespace(as_metadata=lambda: {"changed": 2, "unchanged": 3, "fold_version": "v1"}))
    monkeypatch.setattr(module, "assert_clickhouse_tables_exist", exists)
    monkeypatch.setattr(module, "fold_bucket", fold)

    result = asset.op.compute_fn.decorated_fn(context, config(changed_only=True, page_size=123), resource)

    assert [call.args for call in fold.call_args_list] == [(client, bucket) for bucket in range(64)]
    for call in fold.call_args_list:
        assert call.kwargs["source_run_id"] == "global-run"
        assert call.kwargs["changed_only"] is True
        assert call.kwargs["page_size"] == 123
        assert call.kwargs["folded_at"].utcoffset().total_seconds() == 0
    assert result.metadata["buckets"] == 64
    assert result.metadata["changed"] == 128
    assert result.metadata["unchanged"] == 192
    exists.assert_called_once_with(resource, database=module.tables.DATABASE, tables=module._FOLD_TABLES)


def test_global_address_publication_uses_one_osm_connection_for_all_buckets(monkeypatch) -> None:
    client, duckdb = object(), object()
    resource = SimpleNamespace(get_connection=Mock(return_value=nullcontext(client)))
    osm = SimpleNamespace(get_connection=Mock(return_value=nullcontext(duckdb)))
    context = SimpleNamespace(run_id="address-run", log=Mock())
    exists = Mock()
    fold = Mock(return_value=SimpleNamespace(as_metadata=lambda: {"changed": 2, "geocoded": 3}))
    monkeypatch.setattr(address, "assert_clickhouse_tables_exist", exists)
    monkeypatch.setattr(address, "fold_bucket", fold)
    result = address.se_company_address_publish.op.compute_fn.decorated_fn(
        context, address.AddressFoldConfig(changed_only=True, page_size=123), resource, osm,
    )
    assert [call.args for call in fold.call_args_list] == [(client, duckdb, bucket) for bucket in range(64)]
    for call in fold.call_args_list:
        assert call.kwargs["source_run_id"] == "address-run"
        assert call.kwargs["changed_only"] is True
        assert call.kwargs["page_size"] == 123
    osm.get_connection.assert_called_once_with()
    resource.get_connection.assert_called_once_with()
    assert result.metadata["changed"] == 128
    assert result.metadata["geocoded"] == 192
    assert result.metadata["buckets"] == 64
    exists.assert_called_once_with(resource, database=address.tables.DATABASE, tables=(*address._FOLD_TABLES, *address._GEOCODE_TABLES))


def test_info_llm_uses_the_credential_name_from_the_saved_profile(monkeypatch) -> None:
    resource = SimpleNamespace(get_connection=lambda: nullcontext(object()))
    context = SimpleNamespace(run_id="info-run", log=Mock())
    client_factory = Mock(return_value=object())
    extract = Mock(return_value=SimpleNamespace(as_metadata=lambda: {}))
    monkeypatch.setattr(llm, "assert_clickhouse_tables_exist", Mock())
    monkeypatch.setattr(llm, "build_llm_client", client_factory)
    monkeypatch.setattr(llm, "run_llm_extractor", extract)
    config = llm.LlmExtractConfig(execute=True, llm=llm.LlmSuggestionProfile(
        provider="openrouter", model="chosen/model", api_key_environment_variable="WORKER_LLM_KEY",
    ))
    llm.se_basic_info_suggestions_llm.op.compute_fn.decorated_fn(context, config, resource)
    client_factory.assert_called_once_with(config.llm, timeout_seconds=config.timeout_seconds, api_key_environment_variable="WORKER_LLM_KEY")
    assert extract.call_args.kwargs["llm_client"] is client_factory.return_value
    with pytest.raises(ValueError):
        llm.LlmSuggestionProfile(provider="openrouter", model="chosen/model", api_key_environment_variable="not a variable")
