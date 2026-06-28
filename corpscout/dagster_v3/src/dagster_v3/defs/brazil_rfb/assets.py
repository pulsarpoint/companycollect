from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from pydantic import Field

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
BRAZIL_RFB_PARTITIONS = dg.MonthlyPartitionsDefinition(start_date="2024-01-01")
BRAZIL_RFB_DATA_ROOT = Path("data/brazil_rfb")
BRAZIL_RFB_DEFINITION_MANIFEST_DUCKDB_PATH = (
    BRAZIL_RFB_DATA_ROOT / "__definition__" / "manifest.duckdb"
)
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


@dataclass(frozen=True)
class BrazilRfbStagePaths:
    root: Path
    manifest: Path
    empresas: Path
    estabelecimentos: Path
    simples: Path
    reference: Path
    companies: Path
    contact_info: Path
    websites: Path

    def ensure_root(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)


def brazil_rfb_snapshot_year_month(partition_key: str) -> str:
    clean_partition_key = partition_key.strip()
    if len(clean_partition_key) >= 7:
        clean_partition_key = clean_partition_key[:7]
    return source.validate_snapshot_year_month(clean_partition_key)


def brazil_rfb_stage_paths(snapshot_year_month: str) -> BrazilRfbStagePaths:
    clean_year_month = source.validate_snapshot_year_month(snapshot_year_month)
    root = BRAZIL_RFB_DATA_ROOT / clean_year_month
    return BrazilRfbStagePaths(
        root=root,
        manifest=root / "manifest.duckdb",
        empresas=root / "empresas.duckdb",
        estabelecimentos=root / "estabelecimentos.duckdb",
        simples=root / "simples.duckdb",
        reference=root / "reference.duckdb",
        companies=root / "companies.duckdb",
        contact_info=root / "contact_info.duckdb",
        websites=root / "websites.duckdb",
    )


def _stage_paths_for_context(context: dg.AssetExecutionContext) -> BrazilRfbStagePaths:
    return brazil_rfb_stage_paths(brazil_rfb_snapshot_year_month(context.partition_key))


class BrazilRfbConfig(dg.Config):
    snapshot_base_url: str = Field(
        default=source.DEFAULT_BASE_URL,
        description=(
            "Base URL for Receita Federal CNPJ open-data snapshots. "
            "The partition key controls the YYYY-MM snapshot, and the resolver "
            "selects a direct YYYY-MM directory or the latest matching YYYY-MM-DD "
            "directory. Default mirror base: "
            "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/."
        ),
    )


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
    dlt_pipeline=source.brazil_rfb_pipeline(BRAZIL_RFB_DEFINITION_MANIFEST_DUCKDB_PATH),
    name=SNAPSHOT_FILES_ASSET_KEY,
    dagster_dlt_translator=BrazilRfbDltTranslator(),
    partitions_def=BRAZIL_RFB_PARTITIONS,
    pool=BRAZIL_RFB_MANIFEST_DUCKDB_POOL,
)
def brazil_rfb_snapshot_files_duckdb(
    context: dg.AssetExecutionContext,
    config: BrazilRfbConfig,
    dlt: DagsterDltResource,
) -> Iterator[Any]:
    snapshot_year_month = brazil_rfb_snapshot_year_month(context.partition_key)
    stage_paths = brazil_rfb_stage_paths(snapshot_year_month)
    stage_paths.ensure_root()
    log_info = getattr(getattr(context, "log", None), "info", None)
    if log_info is not None:
        log_info(
            "Starting Brazil RFB snapshot preparation: snapshot_year_month=%s "
            "base_url=%s download_dir=%s manifest_db=%s",
            snapshot_year_month,
            config.snapshot_base_url,
            BRAZIL_RFB_DOWNLOAD_DIR / snapshot_year_month,
            stage_paths.manifest,
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
        dlt_pipeline=source.brazil_rfb_pipeline(stage_paths.manifest),
    )


@dg.asset(
    name=EMPRESAS_ASSET_KEY,
    deps=[dg.AssetKey(SNAPSHOT_FILES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    partitions_def=BRAZIL_RFB_PARTITIONS,
    pool=BRAZIL_RFB_EMPRESAS_DUCKDB_POOL,
    description="Brazil RFB Empresas raw CSV files loaded into a stage DuckDB file.",
)
def brazil_rfb_empresas_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    with duckdb_resource(stage_paths.empresas).get_connection() as connection:
        rows = staging.load_raw_family_from_manifest(
            connection=connection,
            manifest_database_path=stage_paths.manifest,
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
    partitions_def=BRAZIL_RFB_PARTITIONS,
    pool=BRAZIL_RFB_ESTABELECIMENTOS_DUCKDB_POOL,
    description=(
        "Brazil RFB Estabelecimentos raw CSV files loaded into a stage DuckDB file."
    ),
)
def brazil_rfb_estabelecimentos_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    with duckdb_resource(stage_paths.estabelecimentos).get_connection() as connection:
        rows = staging.load_raw_family_from_manifest(
            connection=connection,
            manifest_database_path=stage_paths.manifest,
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
    partitions_def=BRAZIL_RFB_PARTITIONS,
    pool=BRAZIL_RFB_SIMPLES_DUCKDB_POOL,
    description="Brazil RFB Simples raw CSV files loaded into a stage DuckDB file.",
)
def brazil_rfb_simples_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    with duckdb_resource(stage_paths.simples).get_connection() as connection:
        rows = staging.load_raw_family_from_manifest(
            connection=connection,
            manifest_database_path=stage_paths.manifest,
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
    partitions_def=BRAZIL_RFB_PARTITIONS,
    pool=BRAZIL_RFB_REFERENCE_DUCKDB_POOL,
    description="Brazil RFB reference CSV families loaded into a stage DuckDB file.",
)
def brazil_rfb_reference_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    with duckdb_resource(stage_paths.reference).get_connection() as connection:
        counts = staging.load_raw_families_from_manifest(
            connection=connection,
            manifest_database_path=stage_paths.manifest,
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
    partitions_def=BRAZIL_RFB_PARTITIONS,
    pool=BRAZIL_RFB_COMPANIES_DUCKDB_POOL,
    description="Brazil RFB legal entities and establishments normalized in DuckDB.",
)
def brazil_rfb_companies_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    with duckdb_resource(stage_paths.companies).get_connection() as connection:
        counts = transforms.build_brazil_rfb_companies_and_establishments(
            connection=connection,
            source_run_id=context.run_id,
            empresas_database_path=stage_paths.empresas,
            estabelecimentos_database_path=stage_paths.estabelecimentos,
            simples_database_path=stage_paths.simples,
            reference_database_path=stage_paths.reference,
        )
    context.log.info(
        "Normalized Brazil RFB companies and establishments: counts=%s", counts
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name=CONTACT_INFO_ASSET_KEY,
    deps=[dg.AssetKey(COMPANIES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    partitions_def=BRAZIL_RFB_PARTITIONS,
    pool=BRAZIL_RFB_CONTACT_INFO_DUCKDB_POOL,
    description=(
        "Brazil RFB establishment contact info normalized in DuckDB "
        "with accepted unique email domains."
    ),
)
def brazil_rfb_contact_info_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    with duckdb_resource(stage_paths.contact_info).get_connection() as connection:
        counts = contacts.build_brazil_rfb_contact_info(
            connection=connection,
            companies_database_path=stage_paths.companies,
            source_run_id=context.run_id,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name=WEBSITES_ASSET_KEY,
    deps=[dg.AssetKey(CONTACT_INFO_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    partitions_def=BRAZIL_RFB_PARTITIONS,
    pool=BRAZIL_RFB_WEBSITES_DUCKDB_POOL,
    description=(
        "Brazil RFB email-derived br_websites feeder table for the "
        "cross-source domain graph."
    ),
)
def brazil_rfb_websites_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    with duckdb_resource(stage_paths.websites).get_connection() as connection:
        counts = contacts.build_brazil_rfb_websites(
            connection=connection,
            contact_info_database_path=stage_paths.contact_info,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name=CLICKHOUSE_COMPANIES_ASSET_KEY,
    deps=[dg.AssetKey(COMPANIES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    partitions_def=BRAZIL_RFB_PARTITIONS,
    metadata={"table": tables.QUALIFIED_BR_COMPANIES_TABLE},
    description="Brazil RFB legal entities exported to ClickHouse corpscout.br_companies.",
)
def brazil_rfb_clickhouse_companies(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    with read_only_duckdb_connection(
        duckdb_resource(stage_paths.companies)
    ) as connection:
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
    partitions_def=BRAZIL_RFB_PARTITIONS,
    metadata={"table": tables.QUALIFIED_BR_ESTABLISHMENTS_TABLE},
    description=(
        "Brazil RFB establishments exported to ClickHouse corpscout.br_establishments."
    ),
)
def brazil_rfb_clickhouse_establishments(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    with read_only_duckdb_connection(
        duckdb_resource(stage_paths.companies)
    ) as connection:
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
    partitions_def=BRAZIL_RFB_PARTITIONS,
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
    stage_paths = _stage_paths_for_context(context)
    with read_only_duckdb_connection(
        duckdb_resource(stage_paths.contact_info)
    ) as connection:
        rows = export_brazil_rfb_clickhouse_contact_info(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "table": tables.QUALIFIED_BR_COMPANY_CONTACT_INFO_TABLE,
        },
    )


@dg.asset(
    name=CLICKHOUSE_WEBSITES_ASSET_KEY,
    deps=[dg.AssetKey(WEBSITES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    partitions_def=BRAZIL_RFB_PARTITIONS,
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
    stage_paths = _stage_paths_for_context(context)
    with read_only_duckdb_connection(
        duckdb_resource(stage_paths.websites)
    ) as connection:
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
    selection=dg.AssetSelection.groups(GROUP_NAME),
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
)
