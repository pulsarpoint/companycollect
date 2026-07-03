from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dagster as dg
import dlt
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.latvia_ur import resources, tables
from dagster_v3.defs.latvia_ur.classification import latvia_ur_nace_classification
from dagster_v3.defs.latvia_ur.clickhouse import export_latvia_ur_clickhouse_companies
from dagster_v3.defs.latvia_ur.translation import (
    latvia_ur_translation_load,
    latvia_ur_translator_stats_check,
)

GROUP_NAME = "latvia"
LATVIA_UR_DUCKDB_POOL = "latvia_ur_duckdb"
# File stem (catalog) must differ from DLT_DATASET_NAME or DuckDB's binder
# cannot disambiguate "<dataset>.<table>" (mirrors norway_brreg_source.duckdb).
LATVIA_UR_DUCKDB_PATH = Path("data/latvia_ur_source.duckdb")
DLT_DATASET_NAME = tables.DLT_DATASET_NAME
ENTITIES_TABLE = tables.ENTITIES_TABLE
ENTITIES_ASSET_KEY = "latvia_ur_entities_duckdb"
COMPANY_ACTIVITY_ASSET_KEY = "latvia_company_activity_duckdb"
AREA_OF_ACTIVITY_DOWNLOAD_URL = (
    "https://data.gov.lv/dati/dataset/2fcacd83-8b01-4452-a087-b199c72817be/"
    "resource/49bbd751-3fa2-4d78-8c35-ae0e1c5250d6/download/area_of_activity.csv"
)
VZD_ADDRESS_BUILDINGS_DOWNLOAD_URL = (
    "https://data.gov.lv/dati/dataset/6b06a7e8-dedf-4705-a47b-2a7c51177473/"
    "resource/a510737a-18ce-400f-ad4b-04fce5228272/download/aw_eka.csv"
)
VZD_ADDRESS_CITIES_DOWNLOAD_URL = (
    "https://data.gov.lv/dati/dataset/6b06a7e8-dedf-4705-a47b-2a7c51177473/"
    "resource/ee02baa4-2bc3-4f77-a6cb-5427a3e9befe/download/aw_pilseta.csv"
)
VZD_ADDRESS_MUNICIPALITIES_DOWNLOAD_URL = (
    "https://data.gov.lv/dati/dataset/6b06a7e8-dedf-4705-a47b-2a7c51177473/"
    "resource/c62c60bb-58d4-4f26-82c0-5b630769f9d1/download/aw_novads.csv"
)


class LatviaUrDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.name != ENTITIES_TABLE:
            return spec
        return spec.replace_attributes(
            key=dg.AssetKey(ENTITIES_ASSET_KEY),
            deps=[],
            group_name=GROUP_NAME,
            description="Latvia UR register entity bulk data loaded to local DuckDB with dlt.",
            kinds={"python", "dlt", "duckdb"},
        )


def latvia_ur_pipeline(
    database_path: str | Path,
    *,
    pipelines_dir: str | Path | None = None,
) -> Any:
    kwargs: dict[str, Any] = {
        "pipeline_name": "latvia_ur_register",
        "destination": dlt.destinations.duckdb(str(database_path)),
        "dataset_name": DLT_DATASET_NAME,
        "dev_mode": False,
    }
    if pipelines_dir is not None:
        kwargs["pipelines_dir"] = str(pipelines_dir)
    return dlt.pipeline(**kwargs)


def run_latvia_ur_dlt_pipeline(
    *,
    database_path: str | Path,
    run_id: str,
    session: resources.HttpSession | None = None,
    pipelines_dir: str | Path | None = None,
) -> Any:
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    return latvia_ur_pipeline(database_path, pipelines_dir=pipelines_dir).run(
        resources.latvia_ur_source(run_id=run_id, session=session)
    )


def load_latvia_company_activity_csv(
    *,
    duckdb_connection: Any,
    download_url: str = AREA_OF_ACTIVITY_DOWNLOAD_URL,
    session: resources.HttpSession | None = None,
) -> int:
    import tempfile

    with tempfile.TemporaryDirectory(prefix="latvia_activity_") as tmpdir:
        csv_path = Path(tmpdir) / "area_of_activity.csv"
        resources._download_to_path(
            url=download_url,
            dest=csv_path,
            timeout_seconds=resources.DEFAULT_TIMEOUT_SECONDS,
            user_agent=resources.DEFAULT_USER_AGENT,
            session=session,
        )
        duckdb_connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
        duckdb_connection.execute(
            f"""
            create or replace table {DLT_DATASET_NAME}.{tables.COMPANY_ACTIVITY_TABLE} as
            select
                legal_entity_registration_number as regcode,
                name as legal_name,
                legal_form_code,
                legal_form_code_text as legal_form_text,
                nullif(trim(area_of_activity), '') as activity_text_original
            from read_csv(
                ?,
                delim=';',
                header=true,
                all_varchar=true,
                quote='"',
                escape='"'
            )
            """,
            [str(csv_path)],
        )
        rows = duckdb_connection.execute(
            f"select count(*) from {DLT_DATASET_NAME}.{tables.COMPANY_ACTIVITY_TABLE}"
        ).fetchone()[0]
    return int(rows)


def load_latvia_address_csv(
    *,
    duckdb_connection: Any,
    table_name: str,
    download_url: str,
    session: resources.HttpSession | None = None,
) -> int:
    import tempfile

    if table_name not in {
        tables.ADDRESS_BUILDINGS_TABLE,
        tables.ADDRESS_CITIES_TABLE,
        tables.ADDRESS_MUNICIPALITIES_TABLE,
    }:
        raise ValueError(f"Unsupported Latvia VZD address table: {table_name}")

    with tempfile.TemporaryDirectory(prefix="latvia_vzd_address_") as tmpdir:
        raw_csv_path = Path(tmpdir) / f"{table_name}.raw.csv"
        csv_path = Path(tmpdir) / f"{table_name}.csv"
        resources._download_to_path(
            url=download_url,
            dest=raw_csv_path,
            timeout_seconds=resources.DEFAULT_TIMEOUT_SECONDS,
            user_agent=resources.DEFAULT_USER_AGENT,
            session=session,
        )
        _write_utf8_csv(raw_csv_path, csv_path)
        duckdb_connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
        if table_name == tables.ADDRESS_BUILDINGS_TABLE:
            select_sql = """
                select
                    KODS as address_code,
                    TIPS_CD as address_type_code,
                    STATUSS as status,
                    VKUR_CD as parent_address_code,
                    VKUR_TIPS as parent_address_type_code,
                    NOSAUKUMS as name,
                    ATRIB as postal_code,
                    STD as full_address,
                    try_cast(DD_N as double) as latitude,
                    try_cast(DD_E as double) as longitude
            """
        else:
            select_sql = """
                select
                    KODS as address_code,
                    TIPS_CD as address_type_code,
                    STATUSS as status,
                    VKUR_CD as parent_address_code,
                    VKUR_TIPS as parent_address_type_code,
                    NOSAUKUMS as name,
                    ATRIB as atvk_code,
                    STD as full_address,
                    cast(null as double) as latitude,
                    cast(null as double) as longitude
            """
        duckdb_connection.execute(
            f"""
            create or replace table {DLT_DATASET_NAME}.{table_name} as
            {select_sql}
            from read_csv(
                ?,
                delim=',',
                header=true,
                all_varchar=true,
                quote='"',
                escape='"'
            )
            """,
            [str(csv_path)],
        )
        rows = duckdb_connection.execute(
            f"select count(*) from {DLT_DATASET_NAME}.{table_name}"
        ).fetchone()[0]
    return int(rows)


def _write_utf8_csv(source_path: Path, dest_path: Path) -> None:
    raw = source_path.read_bytes()
    for encoding in ("utf-8-sig", "utf-8", "windows-1257", "iso-8859-1"):
        try:
            text = raw.decode(encoding)
        except UnicodeDecodeError:
            continue
        dest_path.write_text(text, encoding="utf-8")
        return
    dest_path.write_bytes(raw)


@dlt_assets(
    dlt_source=resources.latvia_ur_source(),
    dlt_pipeline=latvia_ur_pipeline(LATVIA_UR_DUCKDB_PATH),
    name=ENTITIES_ASSET_KEY,
    dagster_dlt_translator=LatviaUrDltTranslator(),
    pool=LATVIA_UR_DUCKDB_POOL,
)
def latvia_ur_entities_duckdb_asset(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    latvia_ur_duckdb: DuckDBResource,
) -> Iterator[Any]:
    """Load Latvia UR register entity bulk data to local DuckDB with dlt."""
    LATVIA_UR_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    context.log.info(
        "Starting Latvia UR register dlt load: source_url=%s, duckdb_path=%s, "
        "dataset=%s, table=%s",
        resources.REGISTER_DOWNLOAD_URL,
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
    )
    yield from dlt.run(context=context)
    with read_only_duckdb_connection(latvia_ur_duckdb) as connection:
        row_count = _duckdb_table_count(
            duckdb_connection=connection,
            table_name=f"{DLT_DATASET_NAME}.{ENTITIES_TABLE}",
        )
    context.log.info(
        "Completed Latvia UR register dlt load: duckdb_path=%s, dataset=%s, table=%s, rows=%s",
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
        row_count,
    )


@dg.asset(
    name=COMPANY_ACTIVITY_ASSET_KEY,
    group_name=GROUP_NAME,
    kinds={"python", "csv", "duckdb"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description=(
        "Latvia UR company declared activity text loaded from area_of_activity.csv "
        "into DuckDB for enrichment of lv_companies."
    ),
)
def latvia_company_activity_duckdb(
    context: AssetExecutionContext,
    latvia_ur_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Latvia company activity CSV load: source_url=%s duckdb_path=%s table=%s.%s",
        AREA_OF_ACTIVITY_DOWNLOAD_URL,
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.COMPANY_ACTIVITY_TABLE,
    )
    LATVIA_UR_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with latvia_ur_duckdb.get_connection() as connection:
        rows = load_latvia_company_activity_csv(duckdb_connection=connection)
    context.log.info("Completed Latvia company activity CSV load: rows=%s", rows)
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "source_url": AREA_OF_ACTIVITY_DOWNLOAD_URL,
            "table": f"{DLT_DATASET_NAME}.{tables.COMPANY_ACTIVITY_TABLE}",
        }
    )


@dg.asset(
    name="latvia_address_buildings_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "csv", "duckdb", "vzd"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description="VZD AW_EKA.CSV building/buildable-land addresses loaded to DuckDB.",
)
def latvia_address_buildings_duckdb(
    context: AssetExecutionContext,
    latvia_ur_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Latvia VZD building address CSV load: source_url=%s duckdb_path=%s table=%s.%s",
        VZD_ADDRESS_BUILDINGS_DOWNLOAD_URL,
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.ADDRESS_BUILDINGS_TABLE,
    )
    with latvia_ur_duckdb.get_connection() as connection:
        rows = load_latvia_address_csv(
            duckdb_connection=connection,
            table_name=tables.ADDRESS_BUILDINGS_TABLE,
            download_url=VZD_ADDRESS_BUILDINGS_DOWNLOAD_URL,
        )
    context.log.info("Completed Latvia VZD building address CSV load: rows=%s", rows)
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "source_url": VZD_ADDRESS_BUILDINGS_DOWNLOAD_URL,
            "table": f"{DLT_DATASET_NAME}.{tables.ADDRESS_BUILDINGS_TABLE}",
        }
    )


@dg.asset(
    name="latvia_address_cities_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "csv", "duckdb", "vzd"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description="VZD AW_PILSETA.CSV city address objects loaded to DuckDB.",
)
def latvia_address_cities_duckdb(
    context: AssetExecutionContext,
    latvia_ur_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Latvia VZD city address CSV load: source_url=%s duckdb_path=%s table=%s.%s",
        VZD_ADDRESS_CITIES_DOWNLOAD_URL,
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.ADDRESS_CITIES_TABLE,
    )
    with latvia_ur_duckdb.get_connection() as connection:
        rows = load_latvia_address_csv(
            duckdb_connection=connection,
            table_name=tables.ADDRESS_CITIES_TABLE,
            download_url=VZD_ADDRESS_CITIES_DOWNLOAD_URL,
        )
    context.log.info("Completed Latvia VZD city address CSV load: rows=%s", rows)
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "source_url": VZD_ADDRESS_CITIES_DOWNLOAD_URL,
            "table": f"{DLT_DATASET_NAME}.{tables.ADDRESS_CITIES_TABLE}",
        }
    )


@dg.asset(
    name="latvia_address_municipalities_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "csv", "duckdb", "vzd"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description="VZD AW_NOVADS.CSV municipality address objects loaded to DuckDB.",
)
def latvia_address_municipalities_duckdb(
    context: AssetExecutionContext,
    latvia_ur_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Latvia VZD municipality address CSV load: source_url=%s duckdb_path=%s table=%s.%s",
        VZD_ADDRESS_MUNICIPALITIES_DOWNLOAD_URL,
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.ADDRESS_MUNICIPALITIES_TABLE,
    )
    with latvia_ur_duckdb.get_connection() as connection:
        rows = load_latvia_address_csv(
            duckdb_connection=connection,
            table_name=tables.ADDRESS_MUNICIPALITIES_TABLE,
            download_url=VZD_ADDRESS_MUNICIPALITIES_DOWNLOAD_URL,
        )
    context.log.info("Completed Latvia VZD municipality address CSV load: rows=%s", rows)
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "source_url": VZD_ADDRESS_MUNICIPALITIES_DOWNLOAD_URL,
            "table": f"{DLT_DATASET_NAME}.{tables.ADDRESS_MUNICIPALITIES_TABLE}",
        }
    )


@dg.asset(
    deps=[
        dg.AssetKey(ENTITIES_ASSET_KEY),
        dg.AssetKey(COMPANY_ACTIVITY_ASSET_KEY),
        dg.AssetKey("latvia_address_buildings_duckdb"),
        dg.AssetKey("latvia_address_cities_duckdb"),
        dg.AssetKey("latvia_address_municipalities_duckdb"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=LATVIA_UR_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_LV_COMPANIES_TABLE},
    description="Latvia UR register companies exported to ClickHouse corpscout.lv_companies.",
)
def latvia_ur_clickhouse_companies(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    latvia_ur_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Latvia UR companies ClickHouse export: duckdb_path=%s, table=%s",
        LATVIA_UR_DUCKDB_PATH,
        tables.QUALIFIED_LV_COMPANIES_TABLE,
    )
    with latvia_ur_duckdb.get_connection() as connection:
        rows = export_latvia_ur_clickhouse_companies(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    context.log.info("Completed Latvia UR companies ClickHouse export: rows=%s", rows)
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_LV_COMPANIES_TABLE},
    )


def _duckdb_table_count(*, duckdb_connection: Any, table_name: str) -> int:
    value = duckdb_connection.execute(f"select count(*) from {table_name}").fetchone()[0]
    return int(value)


# --- Jobs & schedules (mirrors estonia_ar; see dagster_v3/CLAUDE.md "Scheduling") -------
# Register-only full-refresh chain. Latvia financial assets live in defs/latvia_financial.
# Selecting from both leaves (translation loader + NACE classification, both
# downstream of latvia_ur_clickhouse_companies) pulls the whole register
# chain via upstream() AND runs both after ClickHouse lands, so newly
# ingested texts are enqueued to the translator service AND classified every
# refresh.
latvia_ur_register_job = dg.define_asset_job(
    "latvia_ur_register_job",
    selection=dg.AssetSelection.assets(
        "latvia_ur_translation_load", "latvia_ur_nace_classification"
    ).upstream(),
)
latvia_ur_register_schedule = dg.ScheduleDefinition(
    name="latvia_ur_register_schedule",
    job=latvia_ur_register_job,
    cron_schedule="30 4 * * *",  # daily 04:30 (staggered from estonia's 04:00)
    execution_timezone="Europe/Belgrade",
)


defs = dg.Definitions(
    assets=[
        latvia_ur_entities_duckdb_asset,
        latvia_company_activity_duckdb,
        latvia_address_buildings_duckdb,
        latvia_address_cities_duckdb,
        latvia_address_municipalities_duckdb,
        latvia_ur_clickhouse_companies,
        latvia_ur_translation_load,
        latvia_ur_nace_classification,
    ],
    asset_checks=[
        latvia_ur_translator_stats_check,
    ],
    jobs=[
        latvia_ur_register_job,
    ],
    schedules=[
        latvia_ur_register_schedule,
    ],
    resources={
        "latvia_ur_duckdb": duckdb_resource(LATVIA_UR_DUCKDB_PATH),
    },
)
