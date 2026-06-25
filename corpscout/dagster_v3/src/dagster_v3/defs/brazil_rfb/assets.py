from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from pydantic import Field, field_validator

from dagster_v3.defs.brazil_rfb import contacts, source, staging, tables, transforms
from dagster_v3.defs.brazil_rfb.clickhouse import (
    export_brazil_rfb_clickhouse_contact_info,
    export_brazil_rfb_clickhouse_companies,
    export_brazil_rfb_clickhouse_establishments,
    export_brazil_rfb_clickhouse_websites,
)

GROUP_NAME = "brazil_rfb"
BRAZIL_RFB_DUCKDB_POOL = "brazil_rfb_duckdb"
BRAZIL_RFB_DUCKDB_PATH = Path("data/brazil_rfb_source.duckdb")
BRAZIL_RFB_DOWNLOAD_DIR = Path("data/brazil_rfb_downloads")
SNAPSHOT_FILES_ASSET_KEY = "brazil_rfb_snapshot_files_duckdb"
RAW_FILES_ASSET_KEY = "brazil_rfb_raw_files_duckdb"
COMPANIES_ASSET_KEY = "brazil_rfb_companies_duckdb"
CONTACT_INFO_ASSET_KEY = "brazil_rfb_contact_info_duckdb"
WEBSITES_ASSET_KEY = "brazil_rfb_websites_duckdb"
CLICKHOUSE_COMPANIES_ASSET_KEY = "brazil_rfb_clickhouse_companies"
CLICKHOUSE_ESTABLISHMENTS_ASSET_KEY = "brazil_rfb_clickhouse_establishments"
CLICKHOUSE_CONTACT_INFO_ASSET_KEY = "brazil_rfb_clickhouse_contact_info"
CLICKHOUSE_WEBSITES_ASSET_KEY = "brazil_rfb_clickhouse_websites"


class BrazilRfbConfig(dg.Config):
    snapshot_year_month: str = Field(
        description=(
            "Brazil RFB CNPJ snapshot directory in YYYY-MM format. Examples: "
            "2026-05 or 2026-06. Dagster launch config example: "
            "ops.brazil_rfb_snapshot_files_duckdb.config.snapshot_year_month: "
            "'2026-05'."
        ),
        examples=["2026-05", "2026-06"],
    )
    snapshot_base_url: str = Field(
        default=source.DEFAULT_BASE_URL,
        description=(
            "Base URL for Receita Federal CNPJ open-data snapshots. The asset appends "
            "the snapshot_year_month value to this URL, for example "
            "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2026-05/."
        ),
    )

    @field_validator("snapshot_year_month")
    @classmethod
    def validate_snapshot_year_month(cls, value: str) -> str:
        return source.validate_snapshot_year_month(value)


class BrazilRfbDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        return spec.replace_attributes(
            key=dg.AssetKey(SNAPSHOT_FILES_ASSET_KEY),
            deps=[],
            group_name=GROUP_NAME,
            description=(
                "Brazil RFB CNPJ monthly snapshot ZIP files downloaded, extracted, "
                "and recorded as a dlt manifest table in DuckDB."
            ),
            kinds={"python", "dlt", "duckdb"},
        )


@dlt_assets(
    dlt_source=source.brazil_rfb_source(
        source_run_id="definition",
        manifest_rows=[],
    ),
    dlt_pipeline=source.brazil_rfb_pipeline(BRAZIL_RFB_DUCKDB_PATH),
    name=SNAPSHOT_FILES_ASSET_KEY,
    dagster_dlt_translator=BrazilRfbDltTranslator(),
    pool=BRAZIL_RFB_DUCKDB_POOL,
)
def brazil_rfb_snapshot_files_duckdb(
    context: dg.AssetExecutionContext,
    config: BrazilRfbConfig,
    dlt: DagsterDltResource,
) -> Iterator[Any]:
    yield from dlt.run(
        context=context,
        dlt_source=source.brazil_rfb_source(
            source_run_id=context.run_id,
            snapshot_year_month=config.snapshot_year_month,
            snapshot_base_url=config.snapshot_base_url,
            download_dir=BRAZIL_RFB_DOWNLOAD_DIR / config.snapshot_year_month,
        ),
        dlt_pipeline=source.brazil_rfb_pipeline(BRAZIL_RFB_DUCKDB_PATH),
    )


@dg.asset(
    name=RAW_FILES_ASSET_KEY,
    deps=[dg.AssetKey(SNAPSHOT_FILES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=BRAZIL_RFB_DUCKDB_POOL,
    description="Brazil RFB CNPJ raw CSV file families loaded into DuckDB with read_csv.",
)
def brazil_rfb_raw_files_duckdb(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    counts = staging.load_all_raw_families_from_manifest(
        database_path=BRAZIL_RFB_DUCKDB_PATH,
        source_run_id=context.run_id,
    )
    context.log.info("Loaded Brazil RFB raw CSV families: counts=%s", counts)
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name=COMPANIES_ASSET_KEY,
    deps=[dg.AssetKey(RAW_FILES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=BRAZIL_RFB_DUCKDB_POOL,
    description="Brazil RFB legal entities and establishments normalized in DuckDB.",
)
def brazil_rfb_companies_duckdb(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    counts = transforms.build_brazil_rfb_companies_and_establishments(
        database_path=BRAZIL_RFB_DUCKDB_PATH,
        source_run_id=context.run_id,
    )
    context.log.info("Normalized Brazil RFB companies and establishments: counts=%s", counts)
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name=CONTACT_INFO_ASSET_KEY,
    deps=[dg.AssetKey(COMPANIES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=BRAZIL_RFB_DUCKDB_POOL,
    description=(
        "Brazil RFB establishment contact info normalized in DuckDB "
        "with accepted unique email domains."
    ),
)
def brazil_rfb_contact_info_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    counts = contacts.build_brazil_rfb_contact_info(
        database_path=BRAZIL_RFB_DUCKDB_PATH,
        source_run_id=context.run_id,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name=WEBSITES_ASSET_KEY,
    deps=[dg.AssetKey(CONTACT_INFO_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=BRAZIL_RFB_DUCKDB_POOL,
    description=(
        "Brazil RFB email-derived br_websites feeder table for the "
        "cross-source domain graph."
    ),
)
def brazil_rfb_websites_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    counts = contacts.build_brazil_rfb_websites(
        database_path=BRAZIL_RFB_DUCKDB_PATH,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name=CLICKHOUSE_COMPANIES_ASSET_KEY,
    deps=[dg.AssetKey(COMPANIES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=BRAZIL_RFB_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_BR_COMPANIES_TABLE},
    description="Brazil RFB legal entities exported to ClickHouse corpscout.br_companies.",
)
def brazil_rfb_clickhouse_companies(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    rows = export_brazil_rfb_clickhouse_companies(
        database_path=BRAZIL_RFB_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_BR_COMPANIES_TABLE},
    )


@dg.asset(
    name=CLICKHOUSE_ESTABLISHMENTS_ASSET_KEY,
    deps=[dg.AssetKey(COMPANIES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=BRAZIL_RFB_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_BR_ESTABLISHMENTS_TABLE},
    description=(
        "Brazil RFB establishments exported to ClickHouse "
        "corpscout.br_establishments."
    ),
)
def brazil_rfb_clickhouse_establishments(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    rows = export_brazil_rfb_clickhouse_establishments(
        database_path=BRAZIL_RFB_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_BR_ESTABLISHMENTS_TABLE},
    )


@dg.asset(
    name=CLICKHOUSE_CONTACT_INFO_ASSET_KEY,
    deps=[dg.AssetKey(CONTACT_INFO_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=BRAZIL_RFB_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_BR_COMPANY_CONTACT_INFO_TABLE},
    description=(
        "Brazil RFB company contact info exported to ClickHouse "
        "corpscout.br_company_contact_info."
    ),
)
def brazil_rfb_clickhouse_contact_info(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    rows = export_brazil_rfb_clickhouse_contact_info(
        database_path=BRAZIL_RFB_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_BR_COMPANY_CONTACT_INFO_TABLE},
    )


@dg.asset(
    name=CLICKHOUSE_WEBSITES_ASSET_KEY,
    deps=[dg.AssetKey(WEBSITES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=BRAZIL_RFB_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_BR_WEBSITES_TABLE},
    description=(
        "Brazil RFB email-derived websites exported to ClickHouse "
        "corpscout.br_websites for the domain graph."
    ),
)
def brazil_rfb_clickhouse_websites(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    rows = export_brazil_rfb_clickhouse_websites(
        database_path=BRAZIL_RFB_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_BR_WEBSITES_TABLE},
    )


brazil_rfb_resolve_job = dg.define_asset_job(
    "brazil_rfb_resolve_job",
    selection=dg.AssetSelection.groups(GROUP_NAME)
    | dg.AssetSelection.assets("domains_clickhouse"),
)


defs = dg.Definitions(
    assets=[
        brazil_rfb_snapshot_files_duckdb,
        brazil_rfb_raw_files_duckdb,
        brazil_rfb_companies_duckdb,
        brazil_rfb_contact_info_duckdb,
        brazil_rfb_websites_duckdb,
        brazil_rfb_clickhouse_companies,
        brazil_rfb_clickhouse_establishments,
        brazil_rfb_clickhouse_contact_info,
        brazil_rfb_clickhouse_websites,
    ],
    jobs=[brazil_rfb_resolve_job],
)
