"""ARES pravni forma code list: fetch, land in DuckDB, publish, translate.

Reference data, not a register, so it sits apart from `czech_ares` for the
same reason `france_legal_forms` sits apart from `france_sirene`: its cadence
is the publisher's revision schedule, not the register's daily refresh.
Scheduled yearly -- 151 rows that change when a law does.
"""

from datetime import UTC, date, datetime
from pathlib import Path

import dagster as dg
import pyarrow as pa
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from dlt.sources.helpers import requests

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.legal_form_static import load_curated_legal_forms
from dagster_v3.defs.czech_legal_forms.english import CZ_LEGAL_FORM_EN_BY_CODE
from dagster_v3.defs.czech_legal_forms import tables
from dagster_v3.defs.czech_legal_forms.source import (
    ARES_CISELNIK_BODY,
    ARES_CISELNIK_URL,
    MIN_LEGAL_FORM_ROWS,
    parse_legal_forms,
)
from dagster_v3.defs.translator_load.coverage import (
    TRANSLATION_CHECK_CRON,
    translation_coverage_result,
)
from dagster_v3.defs.translator_load.loader import TranslationField, build_scan_sql
from dagster_v3.defs.translator_load.resource import (
    TranslatorResource,
    translator_queue_health_check,
)

GROUP_NAME = "czech_legal_forms"
CZ_LEGAL_FORMS_DUCKDB_PATH = Path("data/czech_legal_forms_source.duckdb")
# Single-writer pool covering every asset that opens the file, readers
# included: a DuckDB writer excludes readers across processes.
CZ_LEGAL_FORMS_DUCKDB_POOL = "czech_legal_forms_duckdb"

SOURCE_TABLE = f"{RESOLVED_DATABASE}.{tables.CZ_LEGAL_FORMS_TABLE}"
SOURCE_COLUMN = "label_cs"
SOURCE_LANG = "cs"
TARGET_LANG = "en"


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "czech_legal_forms"},
    pool=CZ_LEGAL_FORMS_DUCKDB_POOL,
    description=(
        "Fetches ARES's pravni forma code list and lands it in DuckDB, one row "
        "per code with the name in force."
    ),
)
def czech_legal_forms_duckdb(
    context: dg.AssetExecutionContext,
    czech_legal_forms_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    # dlt's session, not plain requests: it retries connection errors and
    # 429/5xx, which a public government endpoint needs.
    response = requests.post(
        ARES_CISELNIK_URL,
        json=ARES_CISELNIK_BODY,
        headers={"Accept": "application/json"},
        timeout=120,
    )
    response.raise_for_status()

    # The pick is made against the run's own date rather than a constant, so a
    # code renamed with a future effective date starts reading correctly on
    # the day it takes effect.
    rows = parse_legal_forms(response.json(), today=datetime.now(UTC).date())
    if len(rows) < MIN_LEGAL_FORM_ROWS:
        # Refuse to replace on a short read. A partial list would silently
        # unname whole families of legal form rather than fail.
        raise ValueError(
            f"ARES code list yielded {len(rows)} codes, "
            f"below the {MIN_LEGAL_FORM_ROWS} floor"
        )

    duplicates = len(rows) - len({row.code for row in rows})
    if duplicates:
        raise ValueError(
            f"ARES code list returned {duplicates} duplicate codes after the "
            f"validity pick, which should yield exactly one name per code"
        )

    retrieved_at = datetime.now(UTC).replace(tzinfo=None)
    # An Arrow table rather than row-wise executemany: DuckDB reads Arrow
    # directly in C++, and tests/test_duckdb_bulk_loading_contract.py forbids
    # the row-at-a-time path outside one explicitly tracked TED debt.
    batch = pa.table(
        {
            "code": pa.array([row.code for row in rows], pa.string()),
            "label_cs": pa.array([row.label_cs for row in rows], pa.string()),
            "valid_from": pa.array([row.valid_from for row in rows], pa.string()),
            "valid_to": pa.array([row.valid_to for row in rows], pa.string()),
            "source_url": pa.array([ARES_CISELNIK_URL] * len(rows), pa.string()),
            "source_run_id": pa.array([context.run_id] * len(rows), pa.string()),
            "retrieved_at": pa.array([retrieved_at] * len(rows), pa.timestamp("us")),
        }
    )

    with czech_legal_forms_duckdb.get_connection() as connection:
        connection.execute(
            f"CREATE SCHEMA IF NOT EXISTS {tables.CZ_LEGAL_FORMS_DUCKDB_SCHEMA}"
        )
        connection.execute(
            f"""
            CREATE OR REPLACE TABLE
              {tables.CZ_LEGAL_FORMS_DUCKDB_SCHEMA}.{tables.CZ_LEGAL_FORMS_TABLE} (
                code VARCHAR NOT NULL,
                label_cs VARCHAR NOT NULL,
                valid_from VARCHAR NOT NULL,
                valid_to VARCHAR NOT NULL,
                source_url VARCHAR NOT NULL,
                source_run_id VARCHAR NOT NULL,
                retrieved_at TIMESTAMP NOT NULL
            )
            """
        )
        connection.register("czech_legal_forms_batch", batch)
        try:
            connection.execute(
                f"INSERT INTO {tables.CZ_LEGAL_FORMS_DUCKDB_SCHEMA}."
                f"{tables.CZ_LEGAL_FORMS_TABLE} SELECT * FROM czech_legal_forms_batch"
            )
        finally:
            connection.unregister("czech_legal_forms_batch")

    return dg.MaterializeResult(
        metadata={"row_count": len(rows), "source_url": ARES_CISELNIK_URL}
    )


@dg.asset(
    deps=[dg.AssetKey("czech_legal_forms_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "czech_legal_forms"},
    pool=CZ_LEGAL_FORMS_DUCKDB_POOL,
    description="Publishes the ARES pravni forma code list to ClickHouse.",
)
def czech_legal_forms_clickhouse(
    clickhouse: ClickhouseResource,
    czech_legal_forms_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    # The migration owns the schema; this asserts it exists and replaces the
    # contents rather than issuing DDL of its own.
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.CZ_LEGAL_FORMS_TABLES,
    )
    with czech_legal_forms_duckdb.get_connection() as connection:
        with clickhouse.get_connection() as client:
            row_count = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=client,
                duckdb_schema=tables.CZ_LEGAL_FORMS_DUCKDB_SCHEMA,
                duckdb_table=tables.CZ_LEGAL_FORMS_TABLE,
                clickhouse_database=RESOLVED_DATABASE,
                clickhouse_table=tables.CZ_LEGAL_FORMS_TABLE,
                columns=tables.CZ_LEGAL_FORMS_COLUMNS,
                truncate=True,
            )
    return dg.MaterializeResult(metadata={"row_count": row_count})


@dg.asset(
    deps=[dg.AssetKey("czech_legal_forms_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse"},
    description=(
        "Insert the hand-curated English for Czech legal forms straight into "
        "text_translations. No translator involved."
    ),
)
def czech_legal_forms_curated_english(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with clickhouse.get_connection() as client:
        inserted = load_curated_legal_forms(
            client,
            table=SOURCE_TABLE,
            label_column=SOURCE_COLUMN,
            key_column="code",
            source_lang=SOURCE_LANG,
            mapping=CZ_LEGAL_FORM_EN_BY_CODE,
        )
    context.log.info("CZ: %d curated legal forms inserted", inserted)
    return dg.MaterializeResult(
        metadata={"inserted": inserted, "curated_terms": len(CZ_LEGAL_FORM_EN_BY_CODE)}
    )


@dg.asset(
    deps=[dg.AssetKey("czech_legal_forms_curated_english")],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    description=(
        "Scan cz_legal_forms for labels the curated map does not cover, "
        "enqueue them to the translator, and wait."
    ),
)
def czech_legal_forms_translation_load(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    translator: TranslatorResource,
) -> dg.MaterializeResult:
    baseline_failed = translator.queue_stats().failed

    with clickhouse.get_connection() as client:
        # The anti-join inside build_scan_sql is what keeps the curated terms
        # untouched: they already carry a row, so they are not rescanned.
        untranslated_rows = client.execute(build_scan_sql(SOURCE_TABLE, SOURCE_COLUMN))
        context.log.info("scanned %d untranslated labels", len(untranslated_rows))
        enqueue_result = translator.enqueue_translation_rows(
            source_table=SOURCE_TABLE,
            source_column=SOURCE_COLUMN,
            source_lang=SOURCE_LANG,
            target_lang=TARGET_LANG,
            source_language_name="Czech",
            target_language_name="English",
            rows=untranslated_rows,
        )
        for warning in enqueue_result.workflow_start_warnings:
            context.log.warning("translator workflow start warning: %s", warning)

    if enqueue_result.workflow_start_warnings:
        raise dg.Failure(
            description="translator accepted rows but failed to start its workflow",
            metadata={
                "warning_count": len(enqueue_result.workflow_start_warnings),
                "warnings": dg.MetadataValue.json(
                    enqueue_result.workflow_start_warnings
                ),
            },
        )

    if enqueue_result.received > 0:
        completion_stats = translator.wait_for_queue_completion(
            baseline_failed=baseline_failed
        )
        context.log.info(
            "translator queue completed: input=%d output=%d failed=%d",
            completion_stats.input,
            completion_stats.output,
            completion_stats.failed,
        )

    return dg.MaterializeResult(
        metadata={
            "enqueued_received": enqueue_result.received,
            "enqueued_inserted": enqueue_result.inserted,
        }
    )


@dg.asset_check(
    asset=czech_legal_forms_translation_load, name="translator_queue_healthy"
)
def czech_legal_forms_translator_queue_health_check(
    translator: TranslatorResource,
) -> dg.AssetCheckResult:
    return translator_queue_health_check(translator)


@dg.asset_check(
    asset=czech_legal_forms_translation_load, name="translations_present"
)
def czech_legal_forms_translation_coverage(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """How many Czech legal-form labels exist, and how many are translated."""
    return translation_coverage_result(
        clickhouse, (TranslationField(SOURCE_TABLE, SOURCE_COLUMN),)
    )


czech_legal_forms_refresh_job = dg.define_asset_job(
    name="czech_legal_forms_refresh_job",
    selection=dg.AssetSelection.assets(czech_legal_forms_curated_english).upstream(),
)

czech_legal_forms_yearly_schedule = dg.ScheduleDefinition(
    name="czech_legal_forms_yearly",
    job=czech_legal_forms_refresh_job,
    cron_schedule="45 3 1 2 *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

# Every asset that loads translations. Listed by name rather than imported:
# the job lives in one module but covers all of them, and importing each
# would make czech_legal_forms depend on Brazil, Latvia, Norway and Sweden.
# test_translation_coverage_job_covers_every_loader keeps this list honest.
TRANSLATION_LOAD_ASSETS = (
    "brazil_comp_cnae_translation_load",
    "brazil_pncp_translation_load",
    "company_entity_types_translation_load",
    "czech_legal_forms_translation_load",
    "france_legal_forms_translation_load",
    "latvia_ur_translation_load",
    "norway_brreg_translation_load",
    "sweden_company_translation_load",
    "sweden_financial_concepts_translation_load",
)

# Checks only -- no asset is materialised, so this cannot re-download or
# re-publish anything. It re-reads coverage so a queue that drains after the
# load finished is reflected without waiting for the next yearly refresh.
translation_coverage_job = dg.define_asset_job(
    name="translation_coverage_job",
    # Selected by KEY, not by importing each check: the job lives in one
    # module but covers every source, and importing France's check here would
    # make czech_legal_forms depend on france_legal_forms for no reason.
    selection=dg.AssetSelection.checks(
        *(
            dg.AssetCheckKey(dg.AssetKey(asset), "translations_present")
            for asset in TRANSLATION_LOAD_ASSETS
        )
    ),
)

translation_coverage_schedule = dg.ScheduleDefinition(
    name="translation_coverage_schedule",
    job=translation_coverage_job,
    cron_schedule=TRANSLATION_CHECK_CRON,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[
        czech_legal_forms_duckdb,
        czech_legal_forms_clickhouse,
        czech_legal_forms_curated_english,
        czech_legal_forms_translation_load,
    ],
    asset_checks=[
        czech_legal_forms_translator_queue_health_check,
        czech_legal_forms_translation_coverage,
    ],
    jobs=[czech_legal_forms_refresh_job, translation_coverage_job],
    schedules=[czech_legal_forms_yearly_schedule, translation_coverage_schedule],
    resources={"czech_legal_forms_duckdb": duckdb_resource(CZ_LEGAL_FORMS_DUCKDB_PATH)},
)
