"""companies_all build asset: per-country INSERT legs + stage/EXCHANGE.

Deps verification (against ``uv run dg list defs`` output, 2026-07-18):

- The 11 companies-export keys and the 8
  ``{code}_company_financials_latest_clickhouse`` keys from the plan's
  Ground truth matched real asset names EXACTLY -- no corrections needed.
- Per the brief, also checked whether any country exports its industries
  table from a SEPARATE asset than its companies export (the per-country
  ``industry_subquery`` in ``sql.py`` joins these tables, so their producers
  belong in deps too). Found and added:
    - se/ee/gb/fr/cz/sk each export ``xx_industries`` from a dedicated
      asset, distinct from their companies-export asset:
      ``sweden_company_industries_clickhouse``,
      ``estonia_ar_clickhouse_industries``,
      ``uk_companies_house_clickhouse_industries``,
      ``france_sirene_clickhouse_industries``,
      ``czech_ares_clickhouse_industries``,
      ``slovakia_rpo_clickhouse_industries``.
    - br's ``industry_subquery`` reads ``br_establishments`` (own asset,
      ``brazil_comp_rfb_clickhouse_establishments``) and ``br_cnae_to_nace``
      (own fixture-driven asset, ``brazil_comp_cnae_to_nace_clickhouse``).
    - ``nace_categories`` -- joined by no/fi/ee/gb/fr/cz/sk's
      ``industry_subquery`` -- is a shared reference table produced by its
      own asset, ``nace_categories_clickhouse``, not part of any country
      module.
    - lv's ``industry_subquery`` reads the ``corpscout.lv_companies_nace``
      VIEW (migration 000085), computed from
      ``corpscout.text_classifications``, which is populated by
      ``latvia_ur_nace_classification`` (GPU-backed embedding+LLM
      classification). This isn't the usual stage+EXCHANGE table export
      shape, but it's the real producer of the view's NACE data, so it's
      added as a dep for lineage correctness.
  - no's and fi's industries tables (``no_industries``/``fi_industries``)
    are exported TOGETHER with their companies tables by the SAME assets
    already in ``COMPANIES_EXPORT_KEYS`` below
    (``norway_brreg_entities_snapshot_clickhouse`` /
    ``norway_brreg_entity_updates_clickhouse``;
    ``finland_ytj_resolved_clickhouse``) -- no separate addition needed for
    those two.
"""

import time
import uuid

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
)
from dagster_v3.defs.companies_all.sql import SOURCES, build_country_insert_select
from dagster_v3.defs.companies_all.tables import (
    COMPANIES_ALL_COLUMNS,
    COMPANIES_ALL_COUNTRIES,
    COMPANIES_ALL_TABLE,
)
from dagster_v3.defs.company_financials_latest.tables import (
    COMPANY_FINANCIALS_LATEST_COUNTRIES,
)

GROUP_NAME = "companies_all"

COMPANIES_EXPORT_KEYS = (
    "norway_brreg_entities_snapshot_clickhouse",
    "norway_brreg_entity_updates_clickhouse",
    "finland_ytj_resolved_clickhouse",
    "sweden_company_companies_clickhouse",
    "estonia_ar_clickhouse_companies",
    "latvia_ur_clickhouse_companies",
    "uk_companies_house_clickhouse_companies",
    "france_sirene_clickhouse_companies",
    "brazil_comp_rfb_clickhouse_companies",
    "czech_ares_clickhouse_companies",
    "slovakia_rpo_clickhouse_companies",
)

INDUSTRIES_EXPORT_KEYS = (
    "sweden_company_industries_clickhouse",
    "estonia_ar_clickhouse_industries",
    "uk_companies_house_clickhouse_industries",
    "france_sirene_clickhouse_industries",
    "czech_ares_clickhouse_industries",
    "slovakia_rpo_clickhouse_industries",
    "brazil_comp_rfb_clickhouse_establishments",
    "brazil_comp_cnae_to_nace_clickhouse",
    "nace_categories_clickhouse",
    "latvia_ur_nace_classification",
)

FINANCIALS_LATEST_EXPORT_KEYS = tuple(
    f"{code}_company_financials_latest_clickhouse"
    for code in COMPANY_FINANCIALS_LATEST_COUNTRIES
)

# One upstream per country: companies_all reads the shared summary table,
# which each country asset replaces its own partition of.
SIGNAL_SUMMARY_EXPORT_KEYS = (
    "se_government_contract_signals_clickhouse",
    "fi_government_contract_signals_clickhouse",
    "no_government_contract_signals_clickhouse",
)

UPSTREAM_KEYS = (
    COMPANIES_EXPORT_KEYS
    + INDUSTRIES_EXPORT_KEYS
    + FINANCIALS_LATEST_EXPORT_KEYS
    + SIGNAL_SUMMARY_EXPORT_KEYS
)


def _qualified_table(table: str) -> str:
    return f"`{RESOLVED_DATABASE}`.`{table}`"


def _replace_companies_all_table(client, *, log) -> dict:
    stage = f"_tmp_{COMPANIES_ALL_TABLE}_{uuid.uuid4().hex}"
    qualified_target = _qualified_table(COMPANIES_ALL_TABLE)
    qualified_stage = _qualified_table(stage)
    columns = ", ".join(COMPANIES_ALL_COLUMNS)

    client.execute(f"CREATE TABLE {qualified_stage} AS {qualified_target}")
    try:
        per_country_rows: dict[str, int] = {}
        for code in COMPANIES_ALL_COUNTRIES:
            start = time.monotonic()
            client.execute(
                f"INSERT INTO {qualified_stage} ({columns}) "
                f"{build_country_insert_select(code)}"
            )
            # Each leg's SELECT literal-aliases 'country_code' to `code`, so
            # filtering the stage by country_code isolates exactly the rows
            # this leg just added -- the "stage-count delta" for the country.
            [(country_rows,)] = client.execute(
                f"SELECT count() FROM {qualified_stage} WHERE country_code = %(code)s",
                {"code": code},
            )
            duration = time.monotonic() - start
            country_rows = int(country_rows)

            companies_table = SOURCES[code]["companies_table"]
            [(source_count,)] = client.execute(
                f"SELECT count() FROM corpscout.{companies_table}"
            )
            source_count = int(source_count)
            if source_count == 0:
                raise ValueError(
                    f"{code}: corpscout.{companies_table} has 0 rows; refusing to "
                    "build companies_all (an empty source register is never a "
                    "legitimate publish -- upstream exports refuse empty "
                    "publishes, so companies_all must too)"
                )
            if country_rows != source_count:
                raise ValueError(
                    f"{code}: staged {country_rows} companies_all rows but "
                    f"corpscout.{companies_table} has {source_count} rows "
                    "(expected exact equality)"
                )

            per_country_rows[code] = country_rows
            log.info(
                "companies_all leg %s: %s rows in %.1fs", code, country_rows, duration
            )

        total_rows = sum(per_country_rows.values())
        if total_rows == 0:
            raise ValueError(
                f"{stage} has 0 rows; refusing to replace {COMPANIES_ALL_TABLE}"
            )

        client.execute(f"EXCHANGE TABLES {qualified_stage} AND {qualified_target}")
        log.info("Replaced %s with %s total rows", COMPANIES_ALL_TABLE, total_rows)
        return {"per_country_rows": per_country_rows, "total_rows": total_rows}
    finally:
        client.execute(f"DROP TABLE IF EXISTS {qualified_stage}")


@dg.asset(
    name="companies_all_clickhouse",
    group_name=GROUP_NAME,
    deps=[dg.AssetKey(key) for key in UPSTREAM_KEYS],
    # eager() only fires once the default automation-condition sensor is
    # turned on in the Dagster UI -- not enabled by default in this repo.
    # Until then, the RUNNING daily schedule below is the actual refresh
    # trigger; eager() stays declared so it activates for free if/when that
    # sensor is enabled.
    automation_condition=dg.AutomationCondition.eager(),
    kinds={"clickhouse"},
    metadata={"table": f"{RESOLVED_DATABASE}.{COMPANIES_ALL_TABLE}"},
    description=(
        "Uniform per-company row across all 10 countries (search/facet/"
        "industry/financial/government-contract columns), built from the per-country companies "
        "exports, industries tables, and company_financials_latest "
        "plus procurement summaries into corpscout.companies_all "
        "(stage + EXCHANGE TABLES)."
    ),
)
def companies_all_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    build_start = time.monotonic()
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=[
            COMPANIES_ALL_TABLE,
            "company_government_contract_summary",
            *(SOURCES[code]["companies_table"] for code in COMPANIES_ALL_COUNTRIES),
        ],
    )
    with clickhouse.get_connection() as client:
        result = _replace_companies_all_table(client, log=context.log)
    build_seconds = time.monotonic() - build_start

    metadata = {
        f"{code}_rows": count for code, count in result["per_country_rows"].items()
    }
    metadata["total_rows"] = result["total_rows"]
    metadata["build_seconds"] = round(build_seconds, 1)
    return dg.MaterializeResult(metadata=metadata)


companies_all_job = dg.define_asset_job(
    "companies_all_job",
    selection=dg.AssetSelection.assets("companies_all_clickhouse"),
)

companies_all_schedule = dg.ScheduleDefinition(
    name="companies_all_schedule",
    job=companies_all_job,
    # Daily, after the 06:30 company_financials_latest run. The per-asset
    # automation_condition=eager() only fires once the default automation-
    # condition sensor is enabled in the Dagster UI (not enabled by default
    # in this repo) -- until then this RUNNING schedule is the actual
    # refresh trigger.
    cron_schedule="15 7 * * *",
    execution_timezone="Europe/Oslo",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[companies_all_clickhouse],
    jobs=[companies_all_job],
    schedules=[companies_all_schedule],
)
