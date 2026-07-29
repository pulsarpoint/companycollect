import uuid

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
)
from dagster_v3.defs.company_financials_latest.sql import (
    SOURCES,
    build_latest_insert_sql,
)
from dagster_v3.defs.company_financials_latest.tables import (
    COMPANY_FINANCIALS_LATEST_COLUMNS,
    COMPANY_FINANCIALS_LATEST_COUNTRIES,
)

GROUP_NAME = "company_financials_latest"

# Every upstream asset key was verified against the real def in its source
# module (2026-07-17): rg for `name="<key>"` / `def <key>` across
# src/dagster_v3/defs turned up an exact match for all 8 -- no brief
# corrections were needed here.
UPSTREAM_KEYS: dict[str, list[str]] = {
    "no": [
        "norway_brreg_financial_statements_snapshot_clickhouse",
        "norway_brreg_financial_statements_updates_clickhouse",
    ],
    "fi": ["fi_financial_metrics_ch"],
    "se": ["sweden_financial_metrics_clickhouse"],
    "ee": ["estonia_ar_clickhouse_financial_metrics"],
    "lv": ["latvia_financial_metrics_clickhouse"],
    "fr": ["france_financial_metrics_clickhouse"],
    "gb": [
        "uk_companies_house_clickhouse_financial_metrics",
        "uk_companies_house_pdf_financial_metrics",
        "uk_companies_house_accounts_incremental",
    ],
    "br": [
        "brazil_fin_cvm_dfp_statement_rows_clickhouse",
        "brazil_fin_cvm_itr_statement_rows_clickhouse",
    ],
    "sk": ["slovakia_financials_metrics_clickhouse"],
}


def _qualified_table(table: str) -> str:
    return f"`{RESOLVED_DATABASE}`.`{table}`"


def _replace_summary_table(client, *, code: str, log) -> int:
    target = f"{code}_company_financials_latest"
    stage = f"_tmp_{target}_{uuid.uuid4().hex}"
    qualified_target = _qualified_table(target)
    qualified_stage = _qualified_table(stage)
    columns = ", ".join(COMPANY_FINANCIALS_LATEST_COLUMNS)
    client.execute(f"CREATE TABLE {qualified_stage} AS {qualified_target}")
    try:
        client.execute(
            f"INSERT INTO {qualified_stage} ({columns}) {build_latest_insert_sql(code)}"
        )
        [(row_count,)] = client.execute(f"SELECT count() FROM {qualified_stage}")
        if row_count == 0:
            raise ValueError(f"{stage} has 0 rows; refusing to replace {target}")
        client.execute(f"EXCHANGE TABLES {qualified_stage} AND {qualified_target}")
        log.info("Replaced %s with %s rows", target, row_count)
        return int(row_count)
    finally:
        client.execute(f"DROP TABLE IF EXISTS {qualified_stage}")


def _build_asset(code: str) -> dg.AssetsDefinition:
    source_table = SOURCES[code]["table"]
    target_table = f"{code}_company_financials_latest"

    @dg.asset(
        name=f"{code}_company_financials_latest_clickhouse",
        group_name=GROUP_NAME,
        deps=[dg.AssetKey(key) for key in UPSTREAM_KEYS[code]],
        # eager() only fires once the default automation-condition sensor is
        # turned on in the Dagster UI -- not enabled by default in this repo.
        # Until then, the RUNNING daily schedule below is the actual refresh
        # trigger; eager() stays declared so it activates for free if/when
        # that sensor is enabled.
        automation_condition=dg.AutomationCondition.eager(),
        kinds={"clickhouse"},
        metadata={"table": f"{RESOLVED_DATABASE}.{target_table}"},
        description=(
            f"Latest fiscal-year financials per {code.upper()} company, selected "
            f"from corpscout.{source_table} and exported to "
            f"corpscout.{target_table} (stage + EXCHANGE TABLES)."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext,
        clickhouse: ClickhouseResource,
    ) -> dg.MaterializeResult:
        assert_clickhouse_tables_exist(
            clickhouse,
            database=RESOLVED_DATABASE,
            tables=[target_table, source_table],
        )
        with clickhouse.get_connection() as client:
            row_count = _replace_summary_table(client, code=code, log=context.log)
        return dg.MaterializeResult(metadata={"row_count": row_count})

    return _asset


company_financials_latest_assets = [
    _build_asset(code) for code in COMPANY_FINANCIALS_LATEST_COUNTRIES
]

company_financials_latest_job = dg.define_asset_job(
    "company_financials_latest_job",
    selection=dg.AssetSelection.assets(
        *(
            f"{code}_company_financials_latest_clickhouse"
            for code in COMPANY_FINANCIALS_LATEST_COUNTRIES
        )
    ),
)

company_financials_latest_schedule = dg.ScheduleDefinition(
    name="company_financials_latest_schedule",
    job=company_financials_latest_job,
    # Daily fallback. The per-asset automation_condition=eager() only fires
    # once the default automation-condition sensor is enabled in the Dagster
    # UI (not enabled by default in this repo) -- until then this RUNNING
    # schedule is the actual refresh trigger.
    cron_schedule="30 6 * * *",
    execution_timezone="Europe/Oslo",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=company_financials_latest_assets,
    jobs=[company_financials_latest_job],
    schedules=[company_financials_latest_schedule],
)
