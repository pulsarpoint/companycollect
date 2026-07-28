import re
from pathlib import Path

from dagster_v3.defs.france_decp_procurement.assets import (
    defs as france_defs,
)
from dagster_v3.defs.france_decp_procurement import tables as france_tables
from dagster_v3.defs.estonia_rhr_procurement.assets import (
    BACKFILL_POLICY as estonia_backfill_policy,
    defs as estonia_defs,
)
from dagster_v3.defs.estonia_rhr_procurement import tables as estonia_tables
from dagster_v3.defs.latvia_iub_procurement.assets import (
    BACKFILL_POLICY as latvia_backfill_policy,
    defs as latvia_defs,
)
from dagster_v3.defs.latvia_iub_procurement import tables as latvia_tables
from dagster_v3.defs.slovakia_uvo_procurement.assets import (
    BACKFILL_POLICY as slovakia_backfill_policy,
    defs as slovakia_defs,
)
from dagster_v3.defs.slovakia_uvo_procurement import tables as slovakia_tables

MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"


def _asset_names(defs) -> set[str]:
    return {
        asset.key.to_user_string()
        for asset in defs.assets or []
        if len(asset.keys) == 1
    }


def test_france_decp_asset_graph_is_registered() -> None:
    assert _asset_names(france_defs) == {
        "france_decp_procurement_raw_snapshot_s3",
        "france_decp_contract_holders_duckdb",
        "france_decp_contract_holders_usd",
        "france_decp_contract_holders_clickhouse",
    }
    assert {job.name for job in france_defs.jobs or []} == {
        "france_decp_procurement_job"
    }


def test_latvia_iub_asset_graph_is_monthly_and_registered() -> None:
    assert _asset_names(latvia_defs) == {
        "latvia_iub_procurement_daily_json_s3",
        "latvia_iub_procurement_normalized_duckdb",
        "latvia_iub_procurement_winners_usd",
        "latvia_iub_procurement_clickhouse",
    }
    assert all(asset.partitions_def is not None for asset in latvia_defs.assets or [])
    assert {job.name for job in latvia_defs.jobs or []} == {
        "latvia_iub_procurement_backfill_job"
    }
    assert latvia_backfill_policy.max_partitions_per_run == 1


def test_slovakia_uvo_asset_graph_is_monthly_and_registered() -> None:
    assert _asset_names(slovakia_defs) == {
        "slovakia_uvo_procurement_html_s3",
        "slovakia_uvo_procurement_notices_duckdb",
        "slovakia_uvo_procurement_notices_usd",
        "slovakia_uvo_procurement_notices_clickhouse",
    }
    assert all(asset.partitions_def is not None for asset in slovakia_defs.assets or [])
    assert {job.name for job in slovakia_defs.jobs or []} == {
        "slovakia_uvo_procurement_backfill_job"
    }
    assert slovakia_backfill_policy.max_partitions_per_run == 1


def test_estonia_rhr_asset_graph_is_monthly_and_registered() -> None:
    assert _asset_names(estonia_defs) == {
        "estonia_rhr_procurement_xml_s3",
        "estonia_rhr_procurement_normalized_duckdb",
        "estonia_rhr_procurement_winners_usd",
        "estonia_rhr_procurement_clickhouse",
    }
    assert all(asset.partitions_def is not None for asset in estonia_defs.assets or [])
    assert {job.name for job in estonia_defs.jobs or []} == {
        "estonia_rhr_procurement_backfill_job"
    }
    assert estonia_backfill_policy.max_partitions_per_run == 1


def test_national_procurement_migrations_match_exported_column_order() -> None:
    fr_sk_sql = (
        MIGRATIONS / "000201_corpscout_fr_sk_national_procurement.up.sql"
    ).read_text()
    latvia_sql = (
        MIGRATIONS / "000202_corpscout_lv_national_procurement.up.sql"
    ).read_text()
    estonia_sql = (
        MIGRATIONS / "000206_corpscout_ee_national_procurement.up.sql"
    ).read_text()

    for sql, table, columns in (
        (
            fr_sk_sql,
            france_tables.CONTRACT_HOLDERS_TABLE,
            france_tables.CONTRACT_HOLDER_COLUMNS,
        ),
        (
            fr_sk_sql,
            slovakia_tables.NOTICES_TABLE,
            slovakia_tables.NOTICES_COLUMNS,
        ),
        (latvia_sql, latvia_tables.NOTICES_TABLE, latvia_tables.NOTICES_COLUMNS),
        (latvia_sql, latvia_tables.LOTS_TABLE, latvia_tables.LOTS_COLUMNS),
        (latvia_sql, latvia_tables.WINNERS_TABLE, latvia_tables.WINNERS_COLUMNS),
        (
            latvia_sql,
            latvia_tables.EXECUTIONS_TABLE,
            latvia_tables.EXECUTIONS_COLUMNS,
        ),
        (estonia_sql, estonia_tables.NOTICES_TABLE, estonia_tables.NOTICES_COLUMNS),
        (estonia_sql, estonia_tables.LOTS_TABLE, estonia_tables.LOTS_COLUMNS),
        (estonia_sql, estonia_tables.WINNERS_TABLE, estonia_tables.WINNERS_COLUMNS),
    ):
        assert _migration_columns(sql, table) == columns


def _migration_columns(sql: str, table: str) -> tuple[str, ...]:
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS corpscout\.{table}\s*"
        r"\((.*?)\)\s*ENGINE",
        sql,
        re.DOTALL,
    )
    assert match is not None, table
    return tuple(
        line.strip().split()[0].rstrip(",")
        for line in match.group(1).splitlines()
        if line.strip()
    )
