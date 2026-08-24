"""Partition-scoped ClickHouse publication for canonical ESEF parsing outputs.

No ``from __future__ import annotations``: Dagster inspects asset annotations.
"""

import uuid
from dataclasses import dataclass
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.common.partition_duckdb import require_partition_duckdb
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.partitioned_storage import (
    CONCEPT_LABELS_STORAGE,
    CONTACT_CANDIDATES_STORAGE,
    DISCLOSURES_STORAGE,
    FACTS_STORAGE,
    SOURCE_DOCUMENTS_STORAGE,
    require_completed_partition,
)
from dagster_v3.defs.esef_filings.publish import ESEF_FACTS_COLUMN_EXPRESSIONS
from dagster_v3.defs.esef_filings.segment_assets import (
    ESEF_PROCESSED_WEEK_PARTITIONS,
)


GROUP_NAME = "esef_filings"


@dataclass(frozen=True)
class PartitionPublishContract:
    dataset_name: str
    storage_source: str
    duckdb_table: str
    clickhouse_table: str
    columns: tuple[str, ...]
    column_expressions: dict[str, str] | None = None


SOURCE_DOCUMENTS_CONTRACT = PartitionPublishContract(
    dataset_name="source_documents",
    storage_source=SOURCE_DOCUMENTS_STORAGE,
    duckdb_table=tables.ESEF_SOURCE_DOCUMENTS_TABLE,
    clickhouse_table=tables.ESEF_SOURCE_DOCUMENTS_TABLE,
    columns=tables.ESEF_SOURCE_DOCUMENTS_PARTITION_EXPORT_COLUMNS,
)
FACTS_CONTRACT = PartitionPublishContract(
    dataset_name="facts",
    storage_source=FACTS_STORAGE,
    duckdb_table=tables.FACTS_TABLE,
    clickhouse_table=tables.ESEF_FACTS_TABLE,
    columns=tables.ESEF_FACTS_PARTITION_EXPORT_COLUMNS,
    column_expressions=ESEF_FACTS_COLUMN_EXPRESSIONS,
)
CONTACT_CANDIDATES_CONTRACT = PartitionPublishContract(
    dataset_name="contact_candidates",
    storage_source=CONTACT_CANDIDATES_STORAGE,
    duckdb_table=tables.ESEF_DOCUMENT_CONTACT_CANDIDATES_TABLE,
    clickhouse_table=tables.ESEF_DOCUMENT_CONTACT_CANDIDATES_TABLE,
    columns=tables.ESEF_DOCUMENT_CONTACT_CANDIDATES_PARTITION_EXPORT_COLUMNS,
)
CONCEPT_LABELS_CONTRACT = PartitionPublishContract(
    dataset_name="taxonomy_labels",
    storage_source=CONCEPT_LABELS_STORAGE,
    duckdb_table=tables.ESEF_DOCUMENT_CONCEPT_LABELS_TABLE,
    clickhouse_table=tables.ESEF_DOCUMENT_CONCEPT_LABELS_TABLE,
    columns=tables.ESEF_DOCUMENT_CONCEPT_LABELS_PARTITION_EXPORT_COLUMNS,
)
DISCLOSURES_CONTRACT = PartitionPublishContract(
    dataset_name="disclosures",
    storage_source=DISCLOSURES_STORAGE,
    duckdb_table=tables.ESEF_FACT_DISCLOSURES_TABLE,
    clickhouse_table=tables.ESEF_FACT_DISCLOSURES_TABLE,
    columns=tables.ESEF_FACT_DISCLOSURES_PARTITION_EXPORT_COLUMNS,
)


def replace_esef_partition_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    partition_key: str,
    contract: PartitionPublishContract,
    log: Any = None,
) -> dict[str, object]:
    """Publish one validated DuckDB file into exactly one table partition."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESEF_DATABASE,
        tables=(contract.clickhouse_table,),
    )
    with require_partition_duckdb(
        source=contract.storage_source,
        partition=partition_key,
    ) as duckdb_connection:
        expected_row_count = require_completed_partition(
            duckdb_connection,
            dataset_name=contract.dataset_name,
            table=contract.duckdb_table,
            partition_key=partition_key,
        )
        stage_name = f"_tmp_{contract.clickhouse_table}_{uuid.uuid4().hex}"
        target = f"`{tables.ESEF_DATABASE}`.`{contract.clickhouse_table}`"
        stage = f"`{tables.ESEF_DATABASE}`.`{stage_name}`"
        with clickhouse.get_connection() as client:
            client.execute(f"CREATE TABLE {stage} AS {target}")
            try:
                inserted_row_count = int(
                    export_duckdb_connection_table_to_clickhouse(
                        duckdb_connection=duckdb_connection,
                        clickhouse_client=client,
                        duckdb_schema=tables.DLT_DATASET_NAME,
                        duckdb_table=contract.duckdb_table,
                        clickhouse_database=tables.ESEF_DATABASE,
                        clickhouse_table=stage_name,
                        columns=contract.columns,
                        column_expressions=contract.column_expressions,
                        truncate=False,
                        log=log,
                    )
                )
                if inserted_row_count != expected_row_count:
                    raise ValueError(
                        f"ESEF {contract.dataset_name} stage row mismatch: "
                        f"expected={expected_row_count} inserted={inserted_row_count}"
                    )
                client.execute(
                    f"ALTER TABLE {target} REPLACE PARTITION "
                    f"'{partition_key}' FROM {stage}"
                )
                [(published_row_count,)] = client.execute(
                    f"SELECT count() FROM {target} "
                    "WHERE processed_week = %(processed_week)s",
                    {"processed_week": partition_key},
                )
                if int(published_row_count) != expected_row_count:
                    raise ValueError(
                        f"ESEF {contract.dataset_name} published row mismatch: "
                        f"expected={expected_row_count} actual={published_row_count}"
                    )
            finally:
                client.execute(f"DROP TABLE IF EXISTS {stage}")
    return {
        "dataset_name": contract.dataset_name,
        "partition_key": partition_key,
        "row_count": expected_row_count,
        "table": f"{tables.ESEF_DATABASE}.{contract.clickhouse_table}",
    }


def _publish_result(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    contract: PartitionPublishContract,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata=replace_esef_partition_clickhouse(
            clickhouse=clickhouse,
            partition_key=context.partition_key,
            contract=contract,
            log=context.log.info,
        )
    )


@dg.asset(
    name="esef_source_documents_clickhouse",
    deps=[dg.AssetKey("esef_source_documents_duckdb")],
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="esef_source_documents_clickhouse",
)
def esef_source_documents_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _publish_result(context, clickhouse, SOURCE_DOCUMENTS_CONTRACT)


@dg.asset(
    name="esef_facts_clickhouse",
    deps=[dg.AssetKey("esef_filing_facts_duckdb")],
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse", "xbrl"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="esef_facts_clickhouse",
)
def esef_facts_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _publish_result(context, clickhouse, FACTS_CONTRACT)


@dg.asset(
    name="esef_document_contact_candidates_clickhouse",
    deps=[dg.AssetKey("esef_document_contact_candidates_duckdb")],
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="esef_document_contact_candidates_clickhouse",
)
def esef_document_contact_candidates_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _publish_result(context, clickhouse, CONTACT_CANDIDATES_CONTRACT)


@dg.asset(
    name="esef_document_concept_labels_clickhouse",
    deps=[dg.AssetKey("esef_document_concept_labels_duckdb")],
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse", "taxonomy"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="esef_document_concept_labels_clickhouse",
)
def esef_document_concept_labels_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _publish_result(context, clickhouse, CONCEPT_LABELS_CONTRACT)


@dg.asset(
    name="esef_fact_disclosures_clickhouse",
    deps=[dg.AssetKey("esef_fact_disclosures_duckdb")],
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse", "xhtml"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="esef_fact_disclosures_clickhouse",
)
def esef_fact_disclosures_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _publish_result(context, clickhouse, DISCLOSURES_CONTRACT)


ESEF_PARSING_CLICKHOUSE_ASSETS = (
    esef_source_documents_clickhouse,
    esef_facts_clickhouse,
    esef_document_contact_candidates_clickhouse,
    esef_document_concept_labels_clickhouse,
    esef_fact_disclosures_clickhouse,
)

defs = dg.Definitions(
    assets=list(ESEF_PARSING_CLICKHOUSE_ASSETS),
)
