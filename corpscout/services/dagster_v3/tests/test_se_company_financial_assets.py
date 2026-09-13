"""The precedence export (spec section 8): global rows only, idempotent, and registered
without dependencies; the two fold assets (slice 3): 64 static buckets, one partition per run,
the serial pool, the extractor deps, the configs."""

from datetime import UTC, datetime

import dagster as dg
import pytest

from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.assets import (
    EXTRACTOR_ASSET_NAMES,
    FOLD_POOL,
    GROUP_NAME,
    FinancialFoldCompaniesConfig,
    FinancialFoldConfig,
    export_precedence,
    financial_bucket_index,
    se_company_financial_fold,
    se_company_financial_fold_companies,
)
from dagster_v3.defs.se_company.financial.precedence import precedence_rows
from dagster_v3.definitions import defs as load_project_defs

_SELECT_STORED_SQL = (
    f"SELECT field, source, precedence FROM {tables.QUALIFIED_PRECEDENCE_TABLE} "
    "FINAL WHERE company_id = '' AND period_key = '' AND removed = 0"
)
_INSERT_SQL = (
    f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
    f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES"
)


class FakeClient:
    """Dispatches on the whole statement text against the table's own qualified name."""

    def __init__(self, stored: list[tuple] = ()) -> None:
        self.calls: list[tuple[str, object]] = []
        self.stored = list(stored)

    def execute(self, sql, params=None, settings=None):
        self.calls.append((sql, params))
        if sql == _SELECT_STORED_SQL:
            return list(self.stored)
        if sql.startswith(f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE}"):
            return []
        raise AssertionError(f"unexpected statement: {sql!r}")


def test_export_inserts_the_82_global_rows_and_counts_stale_pairs() -> None:
    client = FakeClient(
        stored=[("period_start", "reviewer", 20000), ("legacy_field", "ratsit", 1000)]
    )
    exported_at = datetime(2026, 9, 12, 8, 0, 0, 123000, tzinfo=UTC)
    pairs, stale = export_precedence(client, exported_at)
    assert (pairs, stale) == (82, 1)
    select_sql, _ = client.calls[0]
    assert select_sql == _SELECT_STORED_SQL
    insert_sql, rows = client.calls[1]
    assert insert_sql == _INSERT_SQL
    assert rows[0] == ("", "", "period_start", "reviewer", 20000, 0, "code", "", exported_at)
    assert all(row[0] == "" and row[1] == "" for row in rows)
    assert [row[2:5] for row in rows] == precedence_rows()
    assert [call[0].split(" ", 1)[0] for call in client.calls] == ["SELECT", "INSERT"]


def test_export_against_a_changed_dictionary_inserts_every_row() -> None:
    stored = list(precedence_rows())
    stored[-1] = (stored[-1][0], stored[-1][1], 999)  # one number differs from the dictionary
    client = FakeClient(stored=stored)
    pairs, stale = export_precedence(client, datetime(2026, 9, 12, 9, 0, tzinfo=UTC))
    assert (pairs, stale) == (82, 0)
    assert [call[0].split(" ", 1)[0] for call in client.calls] == ["SELECT", "INSERT"]


def test_export_against_matching_stored_rows_inserts_nothing() -> None:
    client = FakeClient(stored=list(precedence_rows()))
    pairs, stale = export_precedence(client, datetime(2026, 9, 12, 10, 0, tzinfo=UTC))
    assert (pairs, stale) == (0, 0)
    assert [call[0].split(" ", 1)[0] for call in client.calls] == ["SELECT"]


def test_a_pair_the_dictionary_no_longer_names_is_stale_but_never_deleted() -> None:
    stored = [*precedence_rows(), ("revenue", "wikidata", 200)]
    client = FakeClient(stored=stored)
    pairs, stale = export_precedence(client, datetime(2026, 9, 12, 11, 0, tzinfo=UTC))
    assert (pairs, stale) == (82, 1)
    assert [call[0].split(" ", 1)[0] for call in client.calls] == ["SELECT", "INSERT"]


def test_the_export_asset_is_registered_without_dependencies() -> None:
    repository = load_project_defs().get_repository_def()
    node = repository.asset_graph.get(dg.AssetKey("se_company_financial_precedence_clickhouse"))
    assert node.parent_keys == set()
    assert node.partitions_def is None
    assert node.group_name == GROUP_NAME == "se_company_financial"


def test_the_bucket_fold_is_partitioned_pooled_and_downstream_of_the_four_extractors() -> None:
    asset = se_company_financial_fold
    assert asset.key == dg.AssetKey("se_company_financial_fold")
    keys = asset.partitions_def.get_partition_keys()
    assert len(keys) == 64 and keys[0] == "bucket_00" and keys[-1] == "bucket_63"
    assert asset.backfill_policy.max_partitions_per_run == 1
    assert asset.op.pool == FOLD_POOL == "se_company_financial_fold"
    assert asset.dependency_keys == {dg.AssetKey(name) for name in EXTRACTOR_ASSET_NAMES}
    spec = next(iter(asset.specs))
    assert spec.group_name == GROUP_NAME
    assert spec.metadata["table"] == tables.QUALIFIED_MAIN_TABLE
    keys = load_project_defs().get_repository_def().asset_graph.get_all_asset_keys()
    assert dg.AssetKey("se_company_financial_fold") in keys
    assert dg.AssetKey("se_company_financial_fold_companies") in keys


def test_the_targeted_fold_is_unpartitioned_and_unpooled() -> None:
    asset = se_company_financial_fold_companies
    assert asset.partitions_def is None and asset.op.pool is None
    assert asset.dependency_keys == set()
    assert next(iter(asset.specs)).group_name == GROUP_NAME


def test_the_fold_configs_default_to_the_spec_and_validate_ids() -> None:
    assert (FinancialFoldConfig().changed_only, FinancialFoldConfig().page_size) == (True, 5000)
    targeted = FinancialFoldCompaniesConfig(company_ids=[" 5567081699 ", "5567081699"])
    assert (targeted.company_ids, targeted.changed_only, targeted.page_size) == (["5567081699"], False, 5000)
    with pytest.raises(ValueError):
        FinancialFoldCompaniesConfig(company_ids=[])
    with pytest.raises(ValueError, match="10 or 12 digits"):
        FinancialFoldCompaniesConfig(company_ids=["abc"])


def test_bucket_keys_parse_and_bad_ones_are_refused() -> None:
    assert financial_bucket_index("bucket_00") == 0 and financial_bucket_index("bucket_63") == 63
    with pytest.raises(ValueError, match="invalid financial fold partition key"):
        financial_bucket_index("bucket_7")
    with pytest.raises(ValueError, match="out of range"):
        financial_bucket_index("bucket_64")
