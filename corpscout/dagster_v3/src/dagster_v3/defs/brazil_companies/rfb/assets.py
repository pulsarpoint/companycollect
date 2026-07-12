from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from pydantic import Field

from dagster_v3.defs.brazil_companies.rfb import (
    cleanup,
    contacts,
    resume,
    source,
    staging,
    tables,
    transforms,
)
from dagster_v3.defs.brazil_companies.rfb.clickhouse import (
    export_brazil_comp_rfb_clickhouse_companies,
    export_brazil_comp_rfb_clickhouse_company_contacts,
    export_brazil_comp_rfb_clickhouse_company_domains,
    export_brazil_comp_rfb_clickhouse_establishments,
    export_brazil_comp_rfb_clickhouse_websites,
)
from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)

GROUP_NAME = "brazil_comp_rfb"
BRAZIL_COMP_RFB_MANIFEST_DUCKDB_POOL = "brazil_comp_rfb_manifest_duckdb"
BRAZIL_COMP_RFB_EMPRESAS_DUCKDB_POOL = "brazil_comp_rfb_empresas_duckdb"
BRAZIL_COMP_RFB_ESTABELECIMENTOS_DUCKDB_POOL = "brazil_comp_rfb_estabelecimentos_duckdb"
BRAZIL_COMP_RFB_SIMPLES_DUCKDB_POOL = "brazil_comp_rfb_simples_duckdb"
BRAZIL_COMP_RFB_REFERENCE_DUCKDB_POOL = "brazil_comp_rfb_reference_duckdb"
BRAZIL_COMP_RFB_COMPANIES_DUCKDB_POOL = "brazil_comp_rfb_companies_duckdb"
BRAZIL_COMP_RFB_CONTACT_INFO_DUCKDB_POOL = "brazil_comp_rfb_contact_info_duckdb"
BRAZIL_COMP_RFB_WEBSITES_DUCKDB_POOL = "brazil_comp_rfb_websites_duckdb"
BRAZIL_COMP_RFB_PARTITIONS = dg.MonthlyPartitionsDefinition(start_date="2026-04-01")
BRAZIL_COMP_RFB_DATA_ROOT = Path("data/brazil_rfb")
BRAZIL_COMP_RFB_DEFINITION_MANIFEST_DUCKDB_PATH = (
    BRAZIL_COMP_RFB_DATA_ROOT / "__definition__" / "manifest.duckdb"
)
BRAZIL_COMP_RFB_DOWNLOAD_DIR = Path("data/brazil_rfb_downloads")
SNAPSHOT_FILES_ASSET_KEY = "brazil_comp_rfb_snapshot_files_duckdb"
EMPRESAS_ASSET_KEY = "brazil_comp_rfb_empresas_duckdb"
ESTABELECIMENTOS_ASSET_KEY = "brazil_comp_rfb_estabelecimentos_duckdb"
SIMPLES_ASSET_KEY = "brazil_comp_rfb_simples_duckdb"
REFERENCE_ASSET_KEY = "brazil_comp_rfb_reference_duckdb"
COMPANIES_ASSET_KEY = "brazil_comp_rfb_companies_duckdb"
CONTACT_INFO_ASSET_KEY = "brazil_comp_rfb_contact_info_duckdb"
WEBSITES_ASSET_KEY = "brazil_comp_rfb_websites_duckdb"
CLICKHOUSE_COMPANIES_ASSET_KEY = "brazil_comp_rfb_clickhouse_companies"
CLICKHOUSE_ESTABLISHMENTS_ASSET_KEY = "brazil_comp_rfb_clickhouse_establishments"
CLICKHOUSE_COMPANY_CONTACTS_ASSET_KEY = "brazil_comp_rfb_clickhouse_company_contacts"
CLICKHOUSE_COMPANY_DOMAINS_ASSET_KEY = "brazil_comp_rfb_clickhouse_company_domains"
CLICKHOUSE_WEBSITES_ASSET_KEY = "brazil_comp_rfb_clickhouse_websites"
PREVIOUS_PARTITION_CLEANUP_ASSET_KEY = "brazil_comp_rfb_previous_partition_cleanup"
REFERENCE_FAMILIES = (
    "cnaes",
    "naturezas",
    "municipios",
    "paises",
    "qualificacoes",
    "motivos",
)


@dataclass(frozen=True)
class BrazilCompRfbStagePaths:
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


def brazil_comp_rfb_snapshot_year_month(partition_key: str) -> str:
    clean_partition_key = partition_key.strip()
    if len(clean_partition_key) >= 7:
        clean_partition_key = clean_partition_key[:7]
    return source.validate_snapshot_year_month(clean_partition_key)


def brazil_comp_rfb_stage_paths(snapshot_year_month: str) -> BrazilCompRfbStagePaths:
    clean_year_month = source.validate_snapshot_year_month(snapshot_year_month)
    root = BRAZIL_COMP_RFB_DATA_ROOT / clean_year_month
    return BrazilCompRfbStagePaths(
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


def _stage_paths_for_context(
    context: dg.AssetExecutionContext,
) -> BrazilCompRfbStagePaths:
    return brazil_comp_rfb_stage_paths(
        brazil_comp_rfb_snapshot_year_month(context.partition_key)
    )


def _metadata_reused(counts: dict[str, int]) -> dict[str, object]:
    return {**counts, "reused_existing_stage": True}


def _log_reused_stage(
    context: dg.AssetExecutionContext,
    stage_name: str,
    counts: dict[str, int],
) -> None:
    context.log.info(
        "Reusing existing Brazil RFB %s DuckDB stage: counts=%s", stage_name, counts
    )


class BrazilCompRfbConfig(dg.Config):
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


class BrazilCompRfbDltTranslator(DagsterDltTranslator):
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
    dlt_pipeline=source.brazil_rfb_pipeline(
        BRAZIL_COMP_RFB_DEFINITION_MANIFEST_DUCKDB_PATH
    ),
    name=SNAPSHOT_FILES_ASSET_KEY,
    dagster_dlt_translator=BrazilCompRfbDltTranslator(),
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=BRAZIL_COMP_RFB_MANIFEST_DUCKDB_POOL,
)
def brazil_comp_rfb_snapshot_files_duckdb(
    context: dg.AssetExecutionContext,
    config: BrazilCompRfbConfig,
    dlt: DagsterDltResource,
) -> Iterator[Any]:
    snapshot_year_month = brazil_comp_rfb_snapshot_year_month(context.partition_key)
    stage_paths = brazil_comp_rfb_stage_paths(snapshot_year_month)
    stage_paths.ensure_root()
    log_info = getattr(getattr(context, "log", None), "info", None)
    if log_info is not None:
        log_info(
            "Starting Brazil RFB snapshot preparation: snapshot_year_month=%s "
            "base_url=%s download_dir=%s manifest_db=%s",
            snapshot_year_month,
            config.snapshot_base_url,
            BRAZIL_COMP_RFB_DOWNLOAD_DIR / snapshot_year_month,
            stage_paths.manifest,
        )
    existing_manifest_rows = resume.existing_snapshot_manifest_rows(
        stage_paths.manifest,
        source_run_id=context.run_id,
        required_families=source.DEFAULT_FAMILIES,
    )
    if existing_manifest_rows is not None:
        if log_info is not None:
            log_info(
                "Reusing existing Brazil RFB snapshot manifest: "
                "snapshot_year_month=%s rows=%s manifest_db=%s",
                snapshot_year_month,
                len(existing_manifest_rows),
                stage_paths.manifest,
            )
        yield from dlt.run(
            context=context,
            dlt_source=source.brazil_rfb_source(
                source_run_id=context.run_id,
                manifest_rows=existing_manifest_rows,
            ),
            dlt_pipeline=source.brazil_rfb_pipeline(stage_paths.manifest),
        )
        return

    yield from dlt.run(
        context=context,
        dlt_source=source.brazil_rfb_source(
            source_run_id=context.run_id,
            snapshot_year_month=snapshot_year_month,
            snapshot_base_url=config.snapshot_base_url,
            download_dir=BRAZIL_COMP_RFB_DOWNLOAD_DIR / snapshot_year_month,
            log=log_info,
        ),
        dlt_pipeline=source.brazil_rfb_pipeline(stage_paths.manifest),
    )


@dg.asset(
    name=EMPRESAS_ASSET_KEY,
    deps=[dg.AssetKey(SNAPSHOT_FILES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=BRAZIL_COMP_RFB_EMPRESAS_DUCKDB_POOL,
    description="Brazil RFB Empresas raw CSV files loaded into a stage DuckDB file.",
)
def brazil_comp_rfb_empresas_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    table_name = tables.RAW_TABLE_BY_FAMILY["empresas"]
    existing_counts = resume.stage_table_counts(stage_paths.empresas, (table_name,))
    if existing_counts is not None:
        counts = {"empresas": existing_counts[table_name]}
        _log_reused_stage(context, "Empresas", counts)
        return dg.MaterializeResult(metadata=_metadata_reused(counts))

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
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=BRAZIL_COMP_RFB_ESTABELECIMENTOS_DUCKDB_POOL,
    description=(
        "Brazil RFB Estabelecimentos raw CSV files loaded into a stage DuckDB file."
    ),
)
def brazil_comp_rfb_estabelecimentos_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    table_name = tables.RAW_TABLE_BY_FAMILY["estabelecimentos"]
    existing_counts = resume.stage_table_counts(
        stage_paths.estabelecimentos, (table_name,)
    )
    if existing_counts is not None:
        counts = {"estabelecimentos": existing_counts[table_name]}
        _log_reused_stage(context, "Estabelecimentos", counts)
        return dg.MaterializeResult(metadata=_metadata_reused(counts))

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
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=BRAZIL_COMP_RFB_SIMPLES_DUCKDB_POOL,
    description="Brazil RFB Simples raw CSV files loaded into a stage DuckDB file.",
)
def brazil_comp_rfb_simples_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    table_name = tables.RAW_TABLE_BY_FAMILY["simples"]
    existing_counts = resume.stage_table_counts(stage_paths.simples, (table_name,))
    if existing_counts is not None:
        counts = {"simples": existing_counts[table_name]}
        _log_reused_stage(context, "Simples", counts)
        return dg.MaterializeResult(metadata=_metadata_reused(counts))

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
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=BRAZIL_COMP_RFB_REFERENCE_DUCKDB_POOL,
    description="Brazil RFB reference CSV families loaded into a stage DuckDB file.",
)
def brazil_comp_rfb_reference_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    table_names = tuple(
        tables.RAW_TABLE_BY_FAMILY[family] for family in REFERENCE_FAMILIES
    )
    existing_counts = resume.stage_table_counts(stage_paths.reference, table_names)
    if existing_counts is not None:
        counts = {
            family: existing_counts[tables.RAW_TABLE_BY_FAMILY[family]]
            for family in REFERENCE_FAMILIES
        }
        _log_reused_stage(context, "reference", counts)
        return dg.MaterializeResult(metadata=_metadata_reused(counts))

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
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=BRAZIL_COMP_RFB_COMPANIES_DUCKDB_POOL,
    description="Brazil RFB legal entities and establishments normalized in DuckDB.",
)
def brazil_comp_rfb_companies_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    existing_counts = resume.existing_companies_counts(stage_paths.companies)
    if existing_counts is not None:
        _log_reused_stage(context, "companies", existing_counts)
        return dg.MaterializeResult(metadata=_metadata_reused(existing_counts))

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
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=BRAZIL_COMP_RFB_CONTACT_INFO_DUCKDB_POOL,
    description=(
        "Brazil RFB establishment contact info normalized in DuckDB "
        "with accepted unique email domains."
    ),
)
def brazil_comp_rfb_contact_info_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    existing_counts = resume.existing_contact_info_counts(stage_paths.contact_info)
    if existing_counts is not None:
        _log_reused_stage(context, "contact info", existing_counts)
        return dg.MaterializeResult(metadata=_metadata_reused(existing_counts))

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
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=BRAZIL_COMP_RFB_WEBSITES_DUCKDB_POOL,
    description=(
        "Brazil RFB email-derived br_websites feeder table for the "
        "cross-source domain graph."
    ),
)
def brazil_comp_rfb_websites_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    existing_counts = resume.existing_websites_counts(stage_paths.websites)
    if existing_counts is not None:
        _log_reused_stage(context, "websites", existing_counts)
        return dg.MaterializeResult(metadata=_metadata_reused(existing_counts))

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
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    metadata={"table": tables.QUALIFIED_BR_COMPANIES_TABLE},
    description="Brazil RFB legal entities exported to ClickHouse corpscout.br_companies.",
)
def brazil_comp_rfb_clickhouse_companies(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    with read_only_duckdb_connection(
        duckdb_resource(stage_paths.companies)
    ) as connection:
        rows = export_brazil_comp_rfb_clickhouse_companies(
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
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    metadata={"table": tables.QUALIFIED_BR_ESTABLISHMENTS_TABLE},
    description=(
        "Brazil RFB establishments exported to ClickHouse corpscout.br_establishments."
    ),
)
def brazil_comp_rfb_clickhouse_establishments(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    with read_only_duckdb_connection(
        duckdb_resource(stage_paths.companies)
    ) as connection:
        rows = export_brazil_comp_rfb_clickhouse_establishments(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_BR_ESTABLISHMENTS_TABLE},
    )


@dg.asset(
    name=CLICKHOUSE_COMPANY_CONTACTS_ASSET_KEY,
    deps=[dg.AssetKey(CONTACT_INFO_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    metadata={"table": tables.QUALIFIED_BR_COMPANY_CONTACTS_TABLE},
    description=(
        "Brazil RFB canonical company contacts exported to ClickHouse "
        "corpscout.br_company_contacts."
    ),
)
def brazil_comp_rfb_clickhouse_company_contacts(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    with read_only_duckdb_connection(
        duckdb_resource(stage_paths.contact_info)
    ) as connection:
        rows = export_brazil_comp_rfb_clickhouse_company_contacts(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "table": tables.QUALIFIED_BR_COMPANY_CONTACTS_TABLE,
        },
    )


@dg.asset(
    name=CLICKHOUSE_COMPANY_DOMAINS_ASSET_KEY,
    deps=[dg.AssetKey(WEBSITES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    metadata={"table": tables.QUALIFIED_BR_COMPANY_DOMAINS_TABLE},
    description=(
        "Brazil RFB canonical company domains exported to ClickHouse "
        "corpscout.br_company_domains."
    ),
)
def brazil_comp_rfb_clickhouse_company_domains(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    with read_only_duckdb_connection(
        duckdb_resource(stage_paths.websites)
    ) as connection:
        rows = export_brazil_comp_rfb_clickhouse_company_domains(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "table": tables.QUALIFIED_BR_COMPANY_DOMAINS_TABLE,
        },
    )


@dg.asset(
    name=CLICKHOUSE_WEBSITES_ASSET_KEY,
    deps=[dg.AssetKey(WEBSITES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    metadata={"table": tables.QUALIFIED_BR_WEBSITES_TABLE},
    description=(
        "Brazil RFB email-derived websites exported to ClickHouse "
        "corpscout.br_websites for the domain graph."
    ),
)
def brazil_comp_rfb_clickhouse_websites(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    with read_only_duckdb_connection(
        duckdb_resource(stage_paths.websites)
    ) as connection:
        rows = export_brazil_comp_rfb_clickhouse_websites(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_BR_WEBSITES_TABLE},
    )


@dg.asset(
    name=PREVIOUS_PARTITION_CLEANUP_ASSET_KEY,
    deps=[
        dg.AssetKey(CLICKHOUSE_COMPANIES_ASSET_KEY),
        dg.AssetKey(CLICKHOUSE_ESTABLISHMENTS_ASSET_KEY),
        dg.AssetKey(CLICKHOUSE_COMPANY_CONTACTS_ASSET_KEY),
        dg.AssetKey(CLICKHOUSE_COMPANY_DOMAINS_ASSET_KEY),
        dg.AssetKey(CLICKHOUSE_WEBSITES_ASSET_KEY),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "filesystem"},
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description=(
        "Remove previous Brazil RFB partition stage/download folders after the "
        "current partition has been exported to ClickHouse."
    ),
)
def brazil_comp_rfb_previous_partition_cleanup(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    result = cleanup.cleanup_previous_partition_files(
        partition_key=context.partition_key,
        data_root=BRAZIL_COMP_RFB_DATA_ROOT,
        download_root=BRAZIL_COMP_RFB_DOWNLOAD_DIR,
    )
    context.log.info(
        "Brazil RFB previous partition cleanup complete: "
        "target_partition=%s removed_partition=%s removed_paths=%s "
        "missing_paths=%s removed_file_count=%s removed_bytes=%s",
        result.target_partition,
        result.removed_partition,
        result.removed_paths,
        result.missing_paths,
        result.removed_file_count,
        result.removed_bytes,
    )
    return dg.MaterializeResult(
        metadata={
            "target_partition": result.target_partition,
            "removed_partition": result.removed_partition or "",
            "removed_paths": list(result.removed_paths),
            "missing_paths": list(result.missing_paths),
            "removed_file_count": result.removed_file_count,
            "removed_bytes": result.removed_bytes,
        }
    )


brazil_comp_rfb_resolve_job = dg.define_asset_job(
    "brazil_comp_rfb_resolve_job",
    selection=dg.AssetSelection.groups(GROUP_NAME),
)


defs = dg.Definitions(
    assets=[
        brazil_comp_rfb_snapshot_files_duckdb,
        brazil_comp_rfb_empresas_duckdb,
        brazil_comp_rfb_estabelecimentos_duckdb,
        brazil_comp_rfb_simples_duckdb,
        brazil_comp_rfb_reference_duckdb,
        brazil_comp_rfb_companies_duckdb,
        brazil_comp_rfb_contact_info_duckdb,
        brazil_comp_rfb_websites_duckdb,
        brazil_comp_rfb_clickhouse_companies,
        brazil_comp_rfb_clickhouse_establishments,
        brazil_comp_rfb_clickhouse_company_contacts,
        brazil_comp_rfb_clickhouse_company_domains,
        brazil_comp_rfb_clickhouse_websites,
        brazil_comp_rfb_previous_partition_cleanup,
    ],
    jobs=[brazil_comp_rfb_resolve_job],
)
