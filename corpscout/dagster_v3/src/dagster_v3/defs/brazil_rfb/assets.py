from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
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
from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)

GROUP_NAME = "brazil_rfb"
BRAZIL_RFB_MANIFEST_DUCKDB_POOL = "brazil_rfb_manifest_duckdb"
BRAZIL_RFB_EMPRESAS_DUCKDB_POOL = "brazil_rfb_empresas_duckdb"
BRAZIL_RFB_ESTABELECIMENTOS_DUCKDB_POOL = "brazil_rfb_estabelecimentos_duckdb"
BRAZIL_RFB_SIMPLES_DUCKDB_POOL = "brazil_rfb_simples_duckdb"
BRAZIL_RFB_REFERENCE_DUCKDB_POOL = "brazil_rfb_reference_duckdb"
BRAZIL_RFB_COMPANIES_DUCKDB_POOL = "brazil_rfb_companies_duckdb"
BRAZIL_RFB_CONTACT_INFO_DUCKDB_POOL = "brazil_rfb_contact_info_duckdb"
BRAZIL_RFB_WEBSITES_DUCKDB_POOL = "brazil_rfb_websites_duckdb"
BRAZIL_RFB_MANIFEST_DUCKDB_PATH = Path("data/brazil_rfb_manifest.duckdb")
BRAZIL_RFB_EMPRESAS_DUCKDB_PATH = Path("data/brazil_rfb_empresas.duckdb")
BRAZIL_RFB_ESTABELECIMENTOS_DUCKDB_PATH = Path(
    "data/brazil_rfb_estabelecimentos.duckdb"
)
BRAZIL_RFB_SIMPLES_DUCKDB_PATH = Path("data/brazil_rfb_simples.duckdb")
BRAZIL_RFB_REFERENCE_DUCKDB_PATH = Path("data/brazil_rfb_reference.duckdb")
BRAZIL_RFB_COMPANIES_DUCKDB_PATH = Path("data/brazil_rfb_companies.duckdb")
BRAZIL_RFB_CONTACT_INFO_DUCKDB_PATH = Path("data/brazil_rfb_contact_info.duckdb")
BRAZIL_RFB_WEBSITES_DUCKDB_PATH = Path("data/brazil_rfb_websites.duckdb")
BRAZIL_RFB_DOWNLOAD_DIR = Path("data/brazil_rfb_downloads")
SNAPSHOT_FILES_ASSET_KEY = "brazil_rfb_snapshot_files_duckdb"
EMPRESAS_ASSET_KEY = "brazil_rfb_empresas_duckdb"
ESTABELECIMENTOS_ASSET_KEY = "brazil_rfb_estabelecimentos_duckdb"
SIMPLES_ASSET_KEY = "brazil_rfb_simples_duckdb"
REFERENCE_ASSET_KEY = "brazil_rfb_reference_duckdb"
COMPANIES_ASSET_KEY = "brazil_rfb_companies_duckdb"
CONTACT_INFO_ASSET_KEY = "brazil_rfb_contact_info_duckdb"
WEBSITES_ASSET_KEY = "brazil_rfb_websites_duckdb"
CLICKHOUSE_COMPANIES_ASSET_KEY = "brazil_rfb_clickhouse_companies"
CLICKHOUSE_ESTABLISHMENTS_ASSET_KEY = "brazil_rfb_clickhouse_establishments"
CLICKHOUSE_CONTACT_INFO_ASSET_KEY = "brazil_rfb_clickhouse_contact_info"
CLICKHOUSE_WEBSITES_ASSET_KEY = "brazil_rfb_clickhouse_websites"
REFERENCE_FAMILIES = (
    "cnaes",
    "naturezas",
    "municipios",
    "paises",
    "qualificacoes",
    "motivos",
)


class BrazilRfbConfig(dg.Config):
    snapshot_year_month: str = Field(
        description=(
            "Receita Federal full CNPJ registry snapshot to load, in YYYY-MM "
            "format. Example: 2026-05."
        ),
        examples=["2026-05"],
    )
    snapshot_base_url: str = Field(
        default=source.DEFAULT_BASE_URL,
        description=(
            "Base URL for Receita Federal CNPJ open-data snapshots. "
            "snapshot_year_month controls the YYYY-MM snapshot, and the resolver "
            "selects a direct YYYY-MM directory or the latest matching YYYY-MM-DD "
            "directory. Default mirror base: "
            "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/."
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
    dlt_pipeline=source.brazil_rfb_pipeline(BRAZIL_RFB_MANIFEST_DUCKDB_PATH),
    name=SNAPSHOT_FILES_ASSET_KEY,
    dagster_dlt_translator=BrazilRfbDltTranslator(),
    pool=BRAZIL_RFB_MANIFEST_DUCKDB_POOL,
)
def brazil_rfb_snapshot_files_duckdb(
    context: dg.AssetExecutionContext,
    config: BrazilRfbConfig,
    dlt: DagsterDltResource,
) -> Iterator[Any]:
    snapshot_year_month = config.snapshot_year_month
    log_info = getattr(getattr(context, "log", None), "info", None)
    if log_info is not None:
        log_info(
            "Starting Brazil RFB snapshot preparation: snapshot_year_month=%s base_url=%s download_dir=%s",
            snapshot_year_month,
            config.snapshot_base_url,
            BRAZIL_RFB_DOWNLOAD_DIR / snapshot_year_month,
        )
    yield from dlt.run(
        context=context,
        dlt_source=source.brazil_rfb_source(
            source_run_id=context.run_id,
            snapshot_year_month=snapshot_year_month,
            snapshot_base_url=config.snapshot_base_url,
            download_dir=BRAZIL_RFB_DOWNLOAD_DIR / snapshot_year_month,
            log=log_info,
        ),
        dlt_pipeline=source.brazil_rfb_pipeline(BRAZIL_RFB_MANIFEST_DUCKDB_PATH),
    )


@dg.asset(
    name=EMPRESAS_ASSET_KEY,
    deps=[dg.AssetKey(SNAPSHOT_FILES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=BRAZIL_RFB_EMPRESAS_DUCKDB_POOL,
    description="Brazil RFB Empresas raw CSV files loaded into a stage DuckDB file.",
)
def brazil_rfb_empresas_duckdb(
    context: dg.AssetExecutionContext,
    brazil_rfb_empresas_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with brazil_rfb_empresas_duckdb.get_connection() as connection:
        rows = staging.load_raw_family_from_manifest(
            connection=connection,
            manifest_database_path=BRAZIL_RFB_MANIFEST_DUCKDB_PATH,
            family="empresas",
            source_run_id=context.run_id,
        )
    context.log.info("Loaded Brazil RFB Empresas raw CSV files: rows=%s", rows)
    return dg.MaterializeResult(metadata={"empresas": rows})


@dg.asset(
    name=ESTABELECIMENTOS_ASSET_KEY,
    deps=[dg.AssetKey(SNAPSHOT_FILES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=BRAZIL_RFB_ESTABELECIMENTOS_DUCKDB_POOL,
    description=(
        "Brazil RFB Estabelecimentos raw CSV files loaded into a stage DuckDB file."
    ),
)
def brazil_rfb_estabelecimentos_duckdb(
    context: dg.AssetExecutionContext,
    brazil_rfb_estabelecimentos_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with brazil_rfb_estabelecimentos_duckdb.get_connection() as connection:
        rows = staging.load_raw_family_from_manifest(
            connection=connection,
            manifest_database_path=BRAZIL_RFB_MANIFEST_DUCKDB_PATH,
            family="estabelecimentos",
            source_run_id=context.run_id,
        )
    context.log.info("Loaded Brazil RFB Estabelecimentos raw CSV files: rows=%s", rows)
    return dg.MaterializeResult(metadata={"estabelecimentos": rows})


@dg.asset(
    name=SIMPLES_ASSET_KEY,
    deps=[dg.AssetKey(SNAPSHOT_FILES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=BRAZIL_RFB_SIMPLES_DUCKDB_POOL,
    description="Brazil RFB Simples raw CSV files loaded into a stage DuckDB file.",
)
def brazil_rfb_simples_duckdb(
    context: dg.AssetExecutionContext,
    brazil_rfb_simples_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with brazil_rfb_simples_duckdb.get_connection() as connection:
        rows = staging.load_raw_family_from_manifest(
            connection=connection,
            manifest_database_path=BRAZIL_RFB_MANIFEST_DUCKDB_PATH,
            family="simples",
            source_run_id=context.run_id,
        )
    context.log.info("Loaded Brazil RFB Simples raw CSV files: rows=%s", rows)
    return dg.MaterializeResult(metadata={"simples": rows})


@dg.asset(
    name=REFERENCE_ASSET_KEY,
    deps=[dg.AssetKey(SNAPSHOT_FILES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=BRAZIL_RFB_REFERENCE_DUCKDB_POOL,
    description="Brazil RFB reference CSV families loaded into a stage DuckDB file.",
)
def brazil_rfb_reference_duckdb(
    context: dg.AssetExecutionContext,
    brazil_rfb_reference_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with brazil_rfb_reference_duckdb.get_connection() as connection:
        counts = staging.load_raw_families_from_manifest(
            connection=connection,
            manifest_database_path=BRAZIL_RFB_MANIFEST_DUCKDB_PATH,
            families=REFERENCE_FAMILIES,
            source_run_id=context.run_id,
        )
    context.log.info("Loaded Brazil RFB reference raw CSV families: counts=%s", counts)
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name=COMPANIES_ASSET_KEY,
    deps=[
        dg.AssetKey(EMPRESAS_ASSET_KEY),
        dg.AssetKey(ESTABELECIMENTOS_ASSET_KEY),
        dg.AssetKey(SIMPLES_ASSET_KEY),
        dg.AssetKey(REFERENCE_ASSET_KEY),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=BRAZIL_RFB_COMPANIES_DUCKDB_POOL,
    description="Brazil RFB legal entities and establishments normalized in DuckDB.",
)
def brazil_rfb_companies_duckdb(
    context: dg.AssetExecutionContext,
    brazil_rfb_companies_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with brazil_rfb_companies_duckdb.get_connection() as connection:
        counts = transforms.build_brazil_rfb_companies_and_establishments(
            connection=connection,
            source_run_id=context.run_id,
            empresas_database_path=BRAZIL_RFB_EMPRESAS_DUCKDB_PATH,
            estabelecimentos_database_path=BRAZIL_RFB_ESTABELECIMENTOS_DUCKDB_PATH,
            simples_database_path=BRAZIL_RFB_SIMPLES_DUCKDB_PATH,
            reference_database_path=BRAZIL_RFB_REFERENCE_DUCKDB_PATH,
        )
    context.log.info("Normalized Brazil RFB companies and establishments: counts=%s", counts)
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name=CONTACT_INFO_ASSET_KEY,
    deps=[dg.AssetKey(COMPANIES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=BRAZIL_RFB_CONTACT_INFO_DUCKDB_POOL,
    description=(
        "Brazil RFB establishment contact info normalized in DuckDB "
        "with accepted unique email domains."
    ),
)
def brazil_rfb_contact_info_duckdb(
    context: dg.AssetExecutionContext,
    brazil_rfb_contact_info_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with brazil_rfb_contact_info_duckdb.get_connection() as connection:
        counts = contacts.build_brazil_rfb_contact_info(
            connection=connection,
            companies_database_path=BRAZIL_RFB_COMPANIES_DUCKDB_PATH,
            source_run_id=context.run_id,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name=WEBSITES_ASSET_KEY,
    deps=[dg.AssetKey(CONTACT_INFO_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=BRAZIL_RFB_WEBSITES_DUCKDB_POOL,
    description=(
        "Brazil RFB email-derived br_websites feeder table for the "
        "cross-source domain graph."
    ),
)
def brazil_rfb_websites_duckdb(
    context: dg.AssetExecutionContext,
    brazil_rfb_websites_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with brazil_rfb_websites_duckdb.get_connection() as connection:
        counts = contacts.build_brazil_rfb_websites(
            connection=connection,
            contact_info_database_path=BRAZIL_RFB_CONTACT_INFO_DUCKDB_PATH,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name=CLICKHOUSE_COMPANIES_ASSET_KEY,
    deps=[dg.AssetKey(COMPANIES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    metadata={"table": tables.QUALIFIED_BR_COMPANIES_TABLE},
    description="Brazil RFB legal entities exported to ClickHouse corpscout.br_companies.",
)
def brazil_rfb_clickhouse_companies(
    context: dg.AssetExecutionContext,
    brazil_rfb_companies_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(brazil_rfb_companies_duckdb) as connection:
        rows = export_brazil_rfb_clickhouse_companies(
            duckdb_connection=connection,
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
    metadata={"table": tables.QUALIFIED_BR_ESTABLISHMENTS_TABLE},
    description=(
        "Brazil RFB establishments exported to ClickHouse "
        "corpscout.br_establishments."
    ),
)
def brazil_rfb_clickhouse_establishments(
    context: dg.AssetExecutionContext,
    brazil_rfb_companies_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(brazil_rfb_companies_duckdb) as connection:
        rows = export_brazil_rfb_clickhouse_establishments(
            duckdb_connection=connection,
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
    metadata={"table": tables.QUALIFIED_BR_COMPANY_CONTACT_INFO_TABLE},
    description=(
        "Brazil RFB company contact info exported to ClickHouse "
        "corpscout.br_company_contact_info."
    ),
)
def brazil_rfb_clickhouse_contact_info(
    context: dg.AssetExecutionContext,
    brazil_rfb_contact_info_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(brazil_rfb_contact_info_duckdb) as connection:
        rows = export_brazil_rfb_clickhouse_contact_info(
            duckdb_connection=connection,
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
    metadata={"table": tables.QUALIFIED_BR_WEBSITES_TABLE},
    description=(
        "Brazil RFB email-derived websites exported to ClickHouse "
        "corpscout.br_websites for the domain graph."
    ),
)
def brazil_rfb_clickhouse_websites(
    context: dg.AssetExecutionContext,
    brazil_rfb_websites_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(brazil_rfb_websites_duckdb) as connection:
        rows = export_brazil_rfb_clickhouse_websites(
            duckdb_connection=connection,
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
        brazil_rfb_empresas_duckdb,
        brazil_rfb_estabelecimentos_duckdb,
        brazil_rfb_simples_duckdb,
        brazil_rfb_reference_duckdb,
        brazil_rfb_companies_duckdb,
        brazil_rfb_contact_info_duckdb,
        brazil_rfb_websites_duckdb,
        brazil_rfb_clickhouse_companies,
        brazil_rfb_clickhouse_establishments,
        brazil_rfb_clickhouse_contact_info,
        brazil_rfb_clickhouse_websites,
    ],
    jobs=[brazil_rfb_resolve_job],
    resources={
        "brazil_rfb_empresas_duckdb": duckdb_resource(BRAZIL_RFB_EMPRESAS_DUCKDB_PATH),
        "brazil_rfb_estabelecimentos_duckdb": duckdb_resource(
            BRAZIL_RFB_ESTABELECIMENTOS_DUCKDB_PATH
        ),
        "brazil_rfb_simples_duckdb": duckdb_resource(BRAZIL_RFB_SIMPLES_DUCKDB_PATH),
        "brazil_rfb_reference_duckdb": duckdb_resource(
            BRAZIL_RFB_REFERENCE_DUCKDB_PATH
        ),
        "brazil_rfb_companies_duckdb": duckdb_resource(
            BRAZIL_RFB_COMPANIES_DUCKDB_PATH
        ),
        "brazil_rfb_contact_info_duckdb": duckdb_resource(
            BRAZIL_RFB_CONTACT_INFO_DUCKDB_PATH
        ),
        "brazil_rfb_websites_duckdb": duckdb_resource(BRAZIL_RFB_WEBSITES_DUCKDB_PATH),
    },
)
