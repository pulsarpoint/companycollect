import json
import time
from collections.abc import Callable
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pyarrow as pa
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    replace_duckdb_connection_tables_in_clickhouse,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.wikidata.registry_seed import (
    WIKIDATA_REGISTRY_SEED_SPINE_ASSET_KEYS,
)
from dagster_v3.defs.wikidata.source import (
    WIKIDATA_DUCKDB_DATASET_NAME,
    WIKIDATA_EXCHANGES_TABLE,
    WIKIDATA_EXCHANGES_COLUMNS,
    WIKIDATA_LISTED_COMPANIES_TABLE,
    WIKIDATA_LISTED_COMPANIES_COLUMNS,
    WIKIDATA_RAW_BUCKET,
    WikidataSnapshotConfig,
    WikidataRawPullConfig,
    WikidataSparqlClient,
    WikidataTransientRequestError,
    active_exchanges_object_key,
    active_listed_exchange_row_from_binding,
    augmentation_object_key,
    binding_value,
    build_active_listed_exchanges_query,
    build_company_identifier_augmentation_query,
    build_company_people_augmentation_query,
    build_company_profile_augmentation_query,
    build_company_relationship_augmentation_query,
    build_listed_company_query,
    build_registry_number_company_query,
    collapse_active_listed_exchange_rows,
    completed_wikidata_raw_manifest_keys,
    manifest_object_key,
    page_object_key,
    query_hash,
    registry_pseudo_exchange_id,
    response_bindings,
    seed_units_object_key,
    snapshot_manifest_object_key,
    stage_manifest_object_key,
    iter_wikidata_exchange_rows,
    iter_wikidata_listed_company_rows,
    wikidata_id_from_url,
)
from dagster_v3.defs.wikidata import tables

GROUP_NAME = "wikidata"
WIKIDATA_DUCKDB_SCHEMA = "wikidata"
WIKIDATA_DUCKDB_DIRECTORY = Path("data/wikidata")
WIKIDATA_CLICKHOUSE_POOL = "wikidata_clickhouse"
WIKIDATA_STEP_MAX_RETRIES = 4
WIKIDATA_STEP_RETRY_BASE_DELAY_SECONDS = 300
WIKIDATA_STEP_RETRY_MAX_DELAY_SECONDS = 2_400
WIKIDATA_DUCKDB_INSERT_BATCH_ROWS = 50_000
WIKIDATA_SPARQL_POOL = "wikidata_sparql"
WIKIDATA_WEEKLY_PARTITIONS = dg.WeeklyPartitionsDefinition(
    start_date="2026-07-20",
    minute_offset=30,
    hour_offset=3,
    day_offset=1,
    timezone="Europe/Belgrade",
    end_offset=1,
)
WIKIDATA_COMPANY_SOURCE_PARTITIONS = dg.DynamicPartitionsDefinition(
    name="wikidata_company_source"
)
WIKIDATA_RAW_PARTITIONS = dg.MultiPartitionsDefinition(
    {
        "company_source": WIKIDATA_COMPANY_SOURCE_PARTITIONS,
        "date": WIKIDATA_WEEKLY_PARTITIONS,
    }
)
WIKIDATA_COMPANY_PAGES_KIND = "company_pages"
WIKIDATA_COMPANY_PROFILE_KIND = "company_profiles"
WIKIDATA_COMPANY_IDENTIFIERS_KIND = "company_identifiers"
WIKIDATA_COMPANY_RELATIONSHIPS_KIND = "company_relationships"
WIKIDATA_COMPANY_PEOPLE_KIND = "company_people"
WIKIDATA_PERSONS_KIND = "persons"
WIKIDATA_AUGMENTATION_KIND_BY_DATA_KIND = {
    WIKIDATA_COMPANY_PROFILE_KIND: "profile",
    WIKIDATA_COMPANY_IDENTIFIERS_KIND: "identifiers",
    WIKIDATA_COMPANY_RELATIONSHIPS_KIND: "relationships",
    WIKIDATA_COMPANY_PEOPLE_KIND: "people",
}
WIKIDATA_AUGMENTATION_QUERY_BY_DATA_KIND = {
    WIKIDATA_COMPANY_PROFILE_KIND: build_company_profile_augmentation_query,
    WIKIDATA_COMPANY_IDENTIFIERS_KIND: build_company_identifier_augmentation_query,
    WIKIDATA_COMPANY_RELATIONSHIPS_KIND: (
        build_company_relationship_augmentation_query
    ),
    WIKIDATA_COMPANY_PEOPLE_KIND: build_company_people_augmentation_query,
}
WIKIDATA_NETWORK_DATA_KINDS = tuple(WIKIDATA_AUGMENTATION_KIND_BY_DATA_KIND)
WIKIDATA_ALL_SEED_UNIT_DATA_KINDS = (
    WIKIDATA_COMPANY_PAGES_KIND,
    *WIKIDATA_NETWORK_DATA_KINDS,
    WIKIDATA_PERSONS_KIND,
)
WIKIDATA_EMPTY_ALLOWED_TABLES = (
    tables.WIKIDATA_COMPANY_PEOPLE_TABLE,
    tables.WIKIDATA_PERSONS_TABLE,
)


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "wikidata", "s3"},
    description=(
        "Downloads or reuses the weekly active-exchange catalog. This asset "
        "contains exchange discovery only; it does not download companies or people."
    ),
    partitions_def=WIKIDATA_WEEKLY_PARTITIONS,
    pool=WIKIDATA_SPARQL_POOL,
)
def wikidata_exchanges_raw(
    context: dg.AssetExecutionContext,
    config: WikidataRawPullConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    client = WikidataSparqlClient(timeout_seconds=config.request_timeout_seconds)
    return pull_wikidata_exchanges_raw(
        client=client,
        object_store=object_store,
        config=config,
        partition_date=context.partition_key,
        source_run_id=context.partition_key,
        retrieved_at=datetime.now(UTC).isoformat(),
    )


@dg.asset(
    deps=[
        dg.AssetDep("wikidata_exchanges_raw"),
        *(
            dg.AssetDep(
                dg.AssetKey(spine_asset_key),
                partition_mapping=dg.AllPartitionMapping(),
            )
            for spine_asset_key in WIKIDATA_REGISTRY_SEED_SPINE_ASSET_KEYS
        ),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "wikidata", "s3"},
    description=(
        "Builds company-source partitions from exchange listings and national "
        "registry identifiers. This asset is company-specific, not the Wikidata root."
    ),
    partitions_def=WIKIDATA_WEEKLY_PARTITIONS,
)
def wikidata_company_source_units(
    context: dg.AssetExecutionContext,
    config: WikidataRawPullConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    partition_date = context.partition_key
    client = WikidataSparqlClient(timeout_seconds=config.request_timeout_seconds)
    retrieved_at = datetime.now(UTC).isoformat()
    return discover_wikidata_company_sources(
        client=client,
        object_store=object_store,
        config=config,
        partition_date=partition_date,
        source_run_id=partition_date,
        retrieved_at=retrieved_at,
        log=context.log.info,
    )


@dg.asset(
    deps=[
        dg.AssetDep(
            "wikidata_company_source_units",
            partition_mapping=dg.MultiToSingleDimensionPartitionMapping("date"),
        )
    ],
    group_name=GROUP_NAME,
    kinds={"python", "wikidata", "s3"},
    description=(
        "Downloads only base company/listing pages for one weekly company source. "
        "Profiles, identifiers, relationships, and people are separate assets."
    ),
    partitions_def=WIKIDATA_RAW_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=WIKIDATA_SPARQL_POOL,
)
def wikidata_company_pages_raw(
    context: dg.AssetExecutionContext,
    config: WikidataRawPullConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    partition_date, seed_unit_id = wikidata_company_source_partition(context)
    seed_unit = read_wikidata_seed_unit(
        object_store=object_store,
        partition_date=partition_date,
        exchange_id=seed_unit_id,
    )
    client = WikidataSparqlClient(timeout_seconds=config.request_timeout_seconds)
    try:
        return pull_wikidata_company_pages_for_seed_unit(
            client=client,
            object_store=object_store,
            config=config,
            partition_date=partition_date,
            seed_unit=seed_unit,
            source_run_id=partition_date,
            retrieved_at=datetime.now(UTC).isoformat(),
            sleep=time.sleep,
            log=context.log.info,
        )
    except WikidataTransientRequestError as exc:
        retry_delay_seconds = wikidata_step_retry_delay_seconds(context.retry_number)
        context.log.warning(
            "Wikidata transient request failure; retrying partition date=%s "
            "seed_unit=%s data_kind=%s in %s seconds (retry=%s/%s): %s",
            partition_date,
            seed_unit_id,
            WIKIDATA_COMPANY_PAGES_KIND,
            retry_delay_seconds,
            context.retry_number + 1,
            WIKIDATA_STEP_MAX_RETRIES,
            exc,
        )
        raise dg.RetryRequested(
            max_retries=WIKIDATA_STEP_MAX_RETRIES,
            seconds_to_wait=retry_delay_seconds,
        ) from exc


def _wikidata_augmentation_asset(
    context: dg.AssetExecutionContext,
    *,
    config: WikidataRawPullConfig,
    object_store: ObjectStoreResource,
    data_kind: str,
) -> dg.MaterializeResult:
    partition_date, seed_unit_id = wikidata_company_source_partition(context)
    client = WikidataSparqlClient(timeout_seconds=config.request_timeout_seconds)
    try:
        return pull_wikidata_augmentation_for_seed_unit(
            client=client,
            object_store=object_store,
            config=config,
            partition_date=partition_date,
            seed_unit_id=seed_unit_id,
            data_kind=data_kind,
            source_run_id=partition_date,
            retrieved_at=datetime.now(UTC).isoformat(),
            sleep=time.sleep,
            log=context.log.info,
        )
    except WikidataTransientRequestError as exc:
        retry_delay_seconds = wikidata_step_retry_delay_seconds(context.retry_number)
        context.log.warning(
            "Wikidata transient request failure; retrying partition date=%s "
            "seed_unit=%s data_kind=%s in %s seconds (retry=%s/%s): %s",
            partition_date,
            seed_unit_id,
            data_kind,
            retry_delay_seconds,
            context.retry_number + 1,
            WIKIDATA_STEP_MAX_RETRIES,
            exc,
        )
        raise dg.RetryRequested(
            max_retries=WIKIDATA_STEP_MAX_RETRIES,
            seconds_to_wait=retry_delay_seconds,
        ) from exc


@dg.asset(
    deps=["wikidata_company_pages_raw"],
    group_name=GROUP_NAME,
    kinds={"python", "wikidata", "s3"},
    description="Downloads only company profile attributes for one company source.",
    partitions_def=WIKIDATA_RAW_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=WIKIDATA_SPARQL_POOL,
)
def wikidata_company_profiles_raw(
    context: dg.AssetExecutionContext,
    config: WikidataRawPullConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _wikidata_augmentation_asset(
        context,
        config=config,
        object_store=object_store,
        data_kind=WIKIDATA_COMPANY_PROFILE_KIND,
    )


@dg.asset(
    deps=["wikidata_company_pages_raw"],
    group_name=GROUP_NAME,
    kinds={"python", "wikidata", "s3"},
    description="Downloads only company identifiers for one company source.",
    partitions_def=WIKIDATA_RAW_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=WIKIDATA_SPARQL_POOL,
)
def wikidata_company_identifiers_raw(
    context: dg.AssetExecutionContext,
    config: WikidataRawPullConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _wikidata_augmentation_asset(
        context,
        config=config,
        object_store=object_store,
        data_kind=WIKIDATA_COMPANY_IDENTIFIERS_KIND,
    )


@dg.asset(
    deps=["wikidata_company_pages_raw"],
    group_name=GROUP_NAME,
    kinds={"python", "wikidata", "s3"},
    description="Downloads company relationships for one weekly company source.",
    partitions_def=WIKIDATA_RAW_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=WIKIDATA_SPARQL_POOL,
)
def wikidata_company_relationships_raw(
    context: dg.AssetExecutionContext,
    config: WikidataRawPullConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _wikidata_augmentation_asset(
        context,
        config=config,
        object_store=object_store,
        data_kind=WIKIDATA_COMPANY_RELATIONSHIPS_KIND,
    )


@dg.asset(
    deps=["wikidata_company_pages_raw"],
    group_name=GROUP_NAME,
    kinds={"python", "wikidata", "s3"},
    description="Downloads company-to-person links for one weekly company source.",
    partitions_def=WIKIDATA_RAW_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=WIKIDATA_SPARQL_POOL,
)
def wikidata_company_people_raw(
    context: dg.AssetExecutionContext,
    config: WikidataRawPullConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _wikidata_augmentation_asset(
        context,
        config=config,
        object_store=object_store,
        data_kind=WIKIDATA_COMPANY_PEOPLE_KIND,
    )


@dg.asset(
    deps=["wikidata_company_people_raw"],
    group_name=GROUP_NAME,
    kinds={"python", "wikidata", "s3"},
    description=(
        "Materializes person records independently from the company-person raw "
        "responses. It performs no additional Wikidata request."
    ),
    partitions_def=WIKIDATA_RAW_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
)
def wikidata_persons_raw(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    partition_date, seed_unit_id = wikidata_company_source_partition(context)
    return materialize_wikidata_persons_for_seed_unit(
        object_store=object_store,
        partition_date=partition_date,
        seed_unit_id=seed_unit_id,
    )


@dg.asset(
    deps=[
        "wikidata_company_pages_raw",
        "wikidata_company_profiles_raw",
        "wikidata_company_identifiers_raw",
        "wikidata_company_relationships_raw",
        "wikidata_company_people_raw",
        "wikidata_persons_raw",
    ],
    group_name=GROUP_NAME,
    kinds={"python", "wikidata", "s3"},
    description="Combines completed domain manifests for one weekly company source.",
    partitions_def=WIKIDATA_RAW_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
)
def wikidata_company_source_snapshot(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    partition_date, seed_unit_id = wikidata_company_source_partition(context)
    return materialize_wikidata_company_source_snapshot(
        object_store=object_store,
        partition_date=partition_date,
        seed_unit_id=seed_unit_id,
    )


@dg.asset(
    deps=[
        dg.AssetDep(
            "wikidata_company_source_snapshot",
            partition_mapping=dg.MultiToSingleDimensionPartitionMapping("date"),
        )
    ],
    group_name=GROUP_NAME,
    kinds={"python", "wikidata", "s3"},
    description=(
        "Verifies every company-source manifest for a weekly partition and "
        "publishes the aggregate Wikidata raw snapshot consumed by DuckDB."
    ),
    partitions_def=WIKIDATA_WEEKLY_PARTITIONS,
)
def wikidata_raw_snapshot(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return finalize_wikidata_raw_snapshot(
        object_store=object_store,
        partition_date=context.partition_key,
        completed_at=datetime.now(UTC).isoformat(),
    )


def wikidata_company_source_partition(
    context: dg.AssetExecutionContext,
) -> tuple[str, str]:
    partition_key = context.partition_key
    if not isinstance(partition_key, dg.MultiPartitionKey):
        raise ValueError(
            "Wikidata company raw asset requires company_source and date partitions"
        )
    return (
        partition_key.keys_by_dimension["date"],
        partition_key.keys_by_dimension["company_source"],
    )


def wikidata_step_retry_delay_seconds(retry_number: int) -> int:
    return min(
        WIKIDATA_STEP_RETRY_BASE_DELAY_SECONDS * 2**retry_number,
        WIKIDATA_STEP_RETRY_MAX_DELAY_SECONDS,
    )


def wikidata_duckdb_path(table_name: str) -> Path:
    if table_name not in tables.WIKIDATA_TABLES:
        raise ValueError(f"Unsupported Wikidata table: {table_name}")
    return WIKIDATA_DUCKDB_DIRECTORY / f"{table_name}.duckdb"


def _materialize_wikidata_duckdb_table(
    context: dg.AssetExecutionContext,
    *,
    table_name: str,
    config: WikidataSnapshotConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    database_path = wikidata_duckdb_path(table_name)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    partition_date = resolve_wikidata_snapshot_partition_date(
        object_store=object_store,
        configured_partition_date=config.partition_date,
    )
    source_run_id = partition_date
    context.log.info(
        "Starting Wikidata DuckDB table: table=%s partition_date=%s database=%s",
        table_name,
        partition_date,
        database_path,
    )
    with duckdb.connect(str(database_path)) as connection:
        if table_name == tables.WIKIDATA_EXCHANGES_TABLE:
            source_row_count = _replace_wikidata_stage_table(
                connection,
                table_name=WIKIDATA_EXCHANGES_TABLE,
                columns=WIKIDATA_EXCHANGES_COLUMNS,
                rows=iter_wikidata_exchange_rows(
                    object_store=object_store,
                    partition_date=partition_date,
                    max_exchanges=config.max_exchanges,
                    source_run_id=source_run_id,
                ),
                log=context.log.info,
            )
        else:
            source_row_count = _replace_wikidata_stage_table(
                connection,
                table_name=WIKIDATA_LISTED_COMPANIES_TABLE,
                columns=WIKIDATA_LISTED_COMPANIES_COLUMNS,
                rows=iter_wikidata_listed_company_rows(
                    object_store=object_store,
                    partition_date=partition_date,
                    max_pages=config.max_pages,
                    max_exchanges=config.max_exchanges,
                    source_run_id=source_run_id,
                ),
                log=context.log.info,
            )
        row_count = normalize_wikidata_table(
            connection,
            table_name=table_name,
            catalog_name=database_path.stem,
        )
    context.log.info(
        "Completed Wikidata DuckDB table: table=%s source_rows=%s rows=%s database=%s",
        table_name,
        source_row_count,
        row_count,
        database_path,
    )
    return dg.MaterializeResult(
        metadata={
            "duckdb_path": str(database_path),
            "duckdb_schema": WIKIDATA_DUCKDB_SCHEMA,
            "duckdb_table": table_name,
            "partition_date": partition_date,
            "source_run_id": source_run_id,
            "source_row_count": source_row_count,
            "row_count": row_count,
        }
    )


def _replace_wikidata_stage_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    columns: dict[str, dict[str, Any]],
    rows: Iterator[dict[str, Any]],
    log: Callable[..., object],
    batch_rows: int = WIKIDATA_DUCKDB_INSERT_BATCH_ROWS,
) -> int:
    if batch_rows < 1:
        raise ValueError("Wikidata DuckDB insert batch size must be greater than zero")
    connection.execute(
        f"create schema if not exists {_quote_duckdb_identifier(WIKIDATA_DUCKDB_DATASET_NAME)}"
    )
    qualified_table = _duckdb_qualified_name(
        WIKIDATA_DUCKDB_DATASET_NAME,
        table_name,
    )
    duckdb_types = {
        "text": "varchar",
        "timestamp": "timestamp",
        "bigint": "bigint",
    }
    column_names = tuple(columns)
    column_definitions = ", ".join(
        f"{_quote_duckdb_identifier(column_name)} "
        f"{duckdb_types[columns[column_name]['data_type']]}"
        for column_name in column_names
    )
    connection.execute(
        f"create or replace table {qualified_table} ({column_definitions})"
    )
    batch: list[tuple[Any, ...]] = []
    row_count = 0
    for row in rows:
        batch.append(tuple(row.get(column_name) for column_name in column_names))
        if len(batch) < batch_rows:
            continue
        _insert_wikidata_stage_batch(
            connection,
            qualified_table=qualified_table,
            columns=columns,
            rows=batch,
        )
        row_count += len(batch)
        batch.clear()
        log(
            "Loaded Wikidata DuckDB stage rows: table=%s rows=%s", table_name, row_count
        )
    if batch:
        _insert_wikidata_stage_batch(
            connection,
            qualified_table=qualified_table,
            columns=columns,
            rows=batch,
        )
        row_count += len(batch)
    log(
        "Completed Wikidata DuckDB stage table: table=%s rows=%s", table_name, row_count
    )
    return row_count


def _insert_wikidata_stage_batch(
    connection: duckdb.DuckDBPyConnection,
    *,
    qualified_table: str,
    columns: dict[str, dict[str, Any]],
    rows: list[tuple[Any, ...]],
) -> None:
    column_names = tuple(columns)
    values_by_column = tuple(zip(*rows, strict=True))
    arrays: list[pa.Array] = []
    for column_name, values in zip(column_names, values_by_column, strict=True):
        data_type = columns[column_name]["data_type"]
        if data_type == "bigint":
            arrays.append(pa.array(values, type=pa.int64()))
        else:
            arrays.append(
                pa.array(
                    [None if value is None else str(value) for value in values],
                    type=pa.string(),
                )
            )
    arrow_table = pa.Table.from_arrays(arrays, names=column_names)
    registered_name = "_wikidata_stage_batch"
    duckdb_types = {
        "text": "varchar",
        "timestamp": "timestamp",
        "bigint": "bigint",
    }
    select_list = ", ".join(
        f"cast({_quote_duckdb_identifier(column_name)} as "
        f"{duckdb_types[columns[column_name]['data_type']]}) as "
        f"{_quote_duckdb_identifier(column_name)}"
        for column_name in column_names
    )
    connection.register(registered_name, arrow_table)
    try:
        connection.execute(
            f"insert into {qualified_table} select {select_list} "
            f"from {_quote_duckdb_identifier(registered_name)}"
        )
    finally:
        connection.unregister(registered_name)


def _export_wikidata_table_to_clickhouse(
    clickhouse: ClickhouseResource,
    *,
    table_name: str,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=(table_name,),
    )
    database_path = wikidata_duckdb_path(table_name)
    with duckdb.connect(str(database_path), read_only=True) as connection:
        with clickhouse.get_connection() as client:
            row_counts = replace_duckdb_connection_tables_in_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=client,
                duckdb_schema=_duckdb_schema_qualifier(
                    database_path,
                    WIKIDATA_DUCKDB_SCHEMA,
                ),
                clickhouse_database=RESOLVED_DATABASE,
                tables=((table_name, tables.WIKIDATA_TABLE_COLUMNS[table_name]),),
                allow_empty_tables=(
                    (table_name,) if table_name in WIKIDATA_EMPTY_ALLOWED_TABLES else ()
                ),
            )
    return dg.MaterializeResult(
        metadata={
            "clickhouse_database": RESOLVED_DATABASE,
            "clickhouse_table": table_name,
            "duckdb_path": str(database_path),
            "row_count": row_counts[table_name],
        }
    )


@dg.asset(
    deps=["wikidata_raw_snapshot"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "wikidata"},
)
def wikidata_companies_duckdb(
    context: dg.AssetExecutionContext,
    config: WikidataSnapshotConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_wikidata_duckdb_table(
        context,
        table_name=tables.WIKIDATA_COMPANIES_TABLE,
        config=config,
        object_store=object_store,
    )


@dg.asset(
    deps=["wikidata_raw_snapshot"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "wikidata"},
)
def wikidata_exchanges_duckdb(
    context: dg.AssetExecutionContext,
    config: WikidataSnapshotConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_wikidata_duckdb_table(
        context,
        table_name=tables.WIKIDATA_EXCHANGES_TABLE,
        config=config,
        object_store=object_store,
    )


@dg.asset(
    deps=["wikidata_raw_snapshot"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "wikidata"},
)
def wikidata_company_listings_duckdb(
    context: dg.AssetExecutionContext,
    config: WikidataSnapshotConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_wikidata_duckdb_table(
        context,
        table_name=tables.WIKIDATA_COMPANY_LISTINGS_TABLE,
        config=config,
        object_store=object_store,
    )


@dg.asset(
    deps=["wikidata_raw_snapshot"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "wikidata"},
)
def wikidata_company_identifiers_duckdb(
    context: dg.AssetExecutionContext,
    config: WikidataSnapshotConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_wikidata_duckdb_table(
        context,
        table_name=tables.WIKIDATA_COMPANY_IDENTIFIERS_TABLE,
        config=config,
        object_store=object_store,
    )


@dg.asset(
    deps=["wikidata_raw_snapshot"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "wikidata"},
)
def wikidata_company_websites_duckdb(
    context: dg.AssetExecutionContext,
    config: WikidataSnapshotConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_wikidata_duckdb_table(
        context,
        table_name=tables.WIKIDATA_COMPANY_WEBSITES_TABLE,
        config=config,
        object_store=object_store,
    )


@dg.asset(
    deps=["wikidata_raw_snapshot"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "wikidata"},
)
def wikidata_company_relationships_duckdb(
    context: dg.AssetExecutionContext,
    config: WikidataSnapshotConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_wikidata_duckdb_table(
        context,
        table_name=tables.WIKIDATA_COMPANY_RELATIONSHIPS_TABLE,
        config=config,
        object_store=object_store,
    )


@dg.asset(
    deps=["wikidata_raw_snapshot"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "wikidata"},
)
def wikidata_company_people_duckdb(
    context: dg.AssetExecutionContext,
    config: WikidataSnapshotConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_wikidata_duckdb_table(
        context,
        table_name=tables.WIKIDATA_COMPANY_PEOPLE_TABLE,
        config=config,
        object_store=object_store,
    )


@dg.asset(
    deps=["wikidata_raw_snapshot"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "wikidata"},
)
def wikidata_persons_duckdb(
    context: dg.AssetExecutionContext,
    config: WikidataSnapshotConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_wikidata_duckdb_table(
        context,
        table_name=tables.WIKIDATA_PERSONS_TABLE,
        config=config,
        object_store=object_store,
    )


@dg.asset(
    deps=["wikidata_raw_snapshot"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "wikidata"},
)
def wikidata_seed_extraction_runs_duckdb(
    context: dg.AssetExecutionContext,
    config: WikidataSnapshotConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_wikidata_duckdb_table(
        context,
        table_name=tables.WIKIDATA_SEED_EXTRACTION_RUNS_TABLE,
        config=config,
        object_store=object_store,
    )


@dg.asset(
    name=tables.WIKIDATA_COMPANIES_TABLE,
    deps=["wikidata_companies_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=WIKIDATA_CLICKHOUSE_POOL,
)
def wikidata_companies_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _export_wikidata_table_to_clickhouse(
        clickhouse, table_name=tables.WIKIDATA_COMPANIES_TABLE
    )


@dg.asset(
    name=tables.WIKIDATA_EXCHANGES_TABLE,
    deps=["wikidata_exchanges_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=WIKIDATA_CLICKHOUSE_POOL,
)
def wikidata_exchanges_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _export_wikidata_table_to_clickhouse(
        clickhouse, table_name=tables.WIKIDATA_EXCHANGES_TABLE
    )


@dg.asset(
    name=tables.WIKIDATA_COMPANY_LISTINGS_TABLE,
    deps=["wikidata_company_listings_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=WIKIDATA_CLICKHOUSE_POOL,
)
def wikidata_company_listings_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _export_wikidata_table_to_clickhouse(
        clickhouse, table_name=tables.WIKIDATA_COMPANY_LISTINGS_TABLE
    )


@dg.asset(
    name=tables.WIKIDATA_COMPANY_IDENTIFIERS_TABLE,
    deps=["wikidata_company_identifiers_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=WIKIDATA_CLICKHOUSE_POOL,
)
def wikidata_company_identifiers_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _export_wikidata_table_to_clickhouse(
        clickhouse, table_name=tables.WIKIDATA_COMPANY_IDENTIFIERS_TABLE
    )


@dg.asset(
    name=tables.WIKIDATA_COMPANY_WEBSITES_TABLE,
    deps=["wikidata_company_websites_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=WIKIDATA_CLICKHOUSE_POOL,
)
def wikidata_company_websites_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _export_wikidata_table_to_clickhouse(
        clickhouse, table_name=tables.WIKIDATA_COMPANY_WEBSITES_TABLE
    )


@dg.asset(
    name=tables.WIKIDATA_COMPANY_RELATIONSHIPS_TABLE,
    deps=["wikidata_company_relationships_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=WIKIDATA_CLICKHOUSE_POOL,
)
def wikidata_company_relationships_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _export_wikidata_table_to_clickhouse(
        clickhouse, table_name=tables.WIKIDATA_COMPANY_RELATIONSHIPS_TABLE
    )


@dg.asset(
    name=tables.WIKIDATA_COMPANY_PEOPLE_TABLE,
    deps=["wikidata_company_people_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=WIKIDATA_CLICKHOUSE_POOL,
)
def wikidata_company_people_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _export_wikidata_table_to_clickhouse(
        clickhouse, table_name=tables.WIKIDATA_COMPANY_PEOPLE_TABLE
    )


@dg.asset(
    name=tables.WIKIDATA_PERSONS_TABLE,
    deps=["wikidata_persons_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=WIKIDATA_CLICKHOUSE_POOL,
)
def wikidata_persons_clickhouse(clickhouse: ClickhouseResource) -> dg.MaterializeResult:
    return _export_wikidata_table_to_clickhouse(
        clickhouse, table_name=tables.WIKIDATA_PERSONS_TABLE
    )


@dg.asset(
    name=tables.WIKIDATA_SEED_EXTRACTION_RUNS_TABLE,
    deps=["wikidata_seed_extraction_runs_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=WIKIDATA_CLICKHOUSE_POOL,
)
def wikidata_seed_extraction_runs_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _export_wikidata_table_to_clickhouse(
        clickhouse, table_name=tables.WIKIDATA_SEED_EXTRACTION_RUNS_TABLE
    )


@dg.asset(
    deps=[dg.AssetKey(table_name) for table_name in tables.WIKIDATA_TABLES],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "wikidata"},
    description="Verifies that every published Wikidata ClickHouse table belongs to one source snapshot.",
)
def wikidata_snapshot_complete(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _validate_wikidata_clickhouse_snapshot(clickhouse)


def _validate_wikidata_clickhouse_snapshot(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    source_run_ids: set[str] = set()
    row_counts: dict[str, int] = {}
    with clickhouse.get_connection() as client:
        for table_name in tables.WIKIDATA_TABLES:
            rows = client.execute(
                f"select source_run_id, count() from {RESOLVED_DATABASE}.{table_name} "
                "group by source_run_id order by source_run_id limit 2"
            )
            row_counts[table_name] = sum(int(row[1]) for row in rows)
            if not rows and table_name in WIKIDATA_EMPTY_ALLOWED_TABLES:
                continue
            if len(rows) != 1:
                raise ValueError(
                    f"ClickHouse table {table_name} does not contain exactly one source_run_id"
                )
            source_run_ids.add(str(rows[0][0]))

        missing_exchange_ids = client.execute(
            f"select distinct listings.exchange_wikidata_id "
            f"from {RESOLVED_DATABASE}.{tables.WIKIDATA_COMPANY_LISTINGS_TABLE} as listings "
            f"left join {RESOLVED_DATABASE}.{tables.WIKIDATA_EXCHANGES_TABLE} as exchanges "
            "on exchanges.exchange_wikidata_id = listings.exchange_wikidata_id "
            "where exchanges.exchange_wikidata_id is null "
            "order by listings.exchange_wikidata_id limit 10"
        )
    if missing_exchange_ids:
        values = ", ".join(str(row[0]) for row in missing_exchange_ids)
        raise ValueError(f"Wikidata listings reference missing exchanges: {values}")
    if len(source_run_ids) != 1:
        raise ValueError(
            "Wikidata ClickHouse tables contain different source_run_id values: "
            f"{sorted(source_run_ids)}"
        )
    source_run_id = next(iter(source_run_ids))
    return dg.MaterializeResult(
        metadata={
            "source_run_id": source_run_id,
            **{
                f"{table_name}_row_count": row_count
                for table_name, row_count in row_counts.items()
            },
        }
    )


def normalize_wikidata_snapshot_tables(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
) -> dict[str, int]:
    source_table = _duckdb_qualified_table_name(
        catalog_name,
        WIKIDATA_DUCKDB_DATASET_NAME,
        WIKIDATA_LISTED_COMPANIES_TABLE,
    )
    exchanges_source_table = _duckdb_qualified_table_name(
        catalog_name,
        WIKIDATA_DUCKDB_DATASET_NAME,
        WIKIDATA_EXCHANGES_TABLE,
    )
    target_schema = _duckdb_qualified_schema_name(catalog_name, WIKIDATA_DUCKDB_SCHEMA)
    _assert_wikidata_listed_companies_table_exists(
        connection, source_table=source_table
    )
    _assert_wikidata_exchanges_table_exists(
        connection,
        source_table=exchanges_source_table,
    )
    _ensure_optional_wikidata_source_columns(connection, source_table=source_table)
    connection.execute(f"create schema if not exists {target_schema}")
    _create_wikidata_companies_table(
        connection,
        source_table=source_table,
        target_schema=target_schema,
    )
    _create_wikidata_exchanges_table(
        connection,
        source_table=exchanges_source_table,
        target_schema=target_schema,
    )
    _create_wikidata_company_listings_table(
        connection,
        source_table=source_table,
        target_schema=target_schema,
    )
    _create_wikidata_company_identifiers_table(
        connection,
        source_table=source_table,
        target_schema=target_schema,
    )
    _create_wikidata_company_websites_table(
        connection,
        source_table=source_table,
        target_schema=target_schema,
    )
    _create_wikidata_company_relationships_table(
        connection,
        source_table=source_table,
        target_schema=target_schema,
    )
    _create_wikidata_company_people_table(
        connection,
        source_table=source_table,
        target_schema=target_schema,
    )
    _create_wikidata_persons_table(
        connection,
        source_table=source_table,
        target_schema=target_schema,
    )
    _create_wikidata_seed_extraction_runs_table(
        connection,
        source_table=source_table,
        target_schema=target_schema,
    )
    _validate_wikidata_exchanges(connection, target_schema=target_schema)
    return {
        table_name: _duckdb_table_count(
            connection,
            _duckdb_qualified_table_name(
                catalog_name,
                WIKIDATA_DUCKDB_SCHEMA,
                table_name,
            ),
        )
        for table_name in tables.WIKIDATA_TABLES
    }


def normalize_wikidata_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    catalog_name: str,
) -> int:
    target_schema = _duckdb_qualified_schema_name(catalog_name, WIKIDATA_DUCKDB_SCHEMA)
    connection.execute(f"create schema if not exists {target_schema}")
    if table_name == tables.WIKIDATA_EXCHANGES_TABLE:
        source_table = _duckdb_qualified_table_name(
            catalog_name,
            WIKIDATA_DUCKDB_DATASET_NAME,
            WIKIDATA_EXCHANGES_TABLE,
        )
        _assert_wikidata_exchanges_table_exists(
            connection,
            source_table=source_table,
        )
        _create_wikidata_exchanges_table(
            connection,
            source_table=source_table,
            target_schema=target_schema,
        )
        _validate_wikidata_exchange_mics(connection, target_schema=target_schema)
    else:
        source_table = _duckdb_qualified_table_name(
            catalog_name,
            WIKIDATA_DUCKDB_DATASET_NAME,
            WIKIDATA_LISTED_COMPANIES_TABLE,
        )
        _assert_wikidata_listed_companies_table_exists(
            connection,
            source_table=source_table,
        )
        _ensure_optional_wikidata_source_columns(connection, source_table=source_table)
        table_builders: dict[str, Callable[..., None]] = {
            tables.WIKIDATA_COMPANIES_TABLE: _create_wikidata_companies_table,
            tables.WIKIDATA_COMPANY_LISTINGS_TABLE: (
                _create_wikidata_company_listings_table
            ),
            tables.WIKIDATA_COMPANY_IDENTIFIERS_TABLE: (
                _create_wikidata_company_identifiers_table
            ),
            tables.WIKIDATA_COMPANY_WEBSITES_TABLE: (
                _create_wikidata_company_websites_table
            ),
            tables.WIKIDATA_COMPANY_RELATIONSHIPS_TABLE: (
                _create_wikidata_company_relationships_table
            ),
            tables.WIKIDATA_COMPANY_PEOPLE_TABLE: _create_wikidata_company_people_table,
            tables.WIKIDATA_PERSONS_TABLE: _create_wikidata_persons_table,
            tables.WIKIDATA_SEED_EXTRACTION_RUNS_TABLE: (
                _create_wikidata_seed_extraction_runs_table
            ),
        }
        if table_name not in table_builders:
            raise ValueError(f"Unsupported Wikidata table: {table_name}")
        table_builders[table_name](
            connection,
            source_table=source_table,
            target_schema=target_schema,
        )
    return _duckdb_table_count(
        connection,
        _duckdb_qualified_table_name(
            catalog_name,
            WIKIDATA_DUCKDB_SCHEMA,
            table_name,
        ),
    )


def _assert_wikidata_listed_companies_table_exists(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
) -> None:
    try:
        connection.execute(f"select 1 from {source_table} limit 0")
    except Exception as exc:
        raise ValueError(
            "Missing DuckDB source table "
            f"{WIKIDATA_DUCKDB_DATASET_NAME}.{WIKIDATA_LISTED_COMPANIES_TABLE}; "
            "load the Wikidata raw snapshot into DuckDB first"
        ) from exc


def _assert_wikidata_exchanges_table_exists(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
) -> None:
    try:
        connection.execute(f"select 1 from {source_table} limit 0")
    except Exception as exc:
        raise ValueError(
            "Missing DuckDB source table "
            f"{WIKIDATA_DUCKDB_DATASET_NAME}.{WIKIDATA_EXCHANGES_TABLE}; "
            "materialize wikidata_exchanges_duckdb first"
        ) from exc


def _create_wikidata_exchanges_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_EXCHANGES_TABLE} as
        with normalized as (
            select
                trim(exchange_wikidata_id) as exchange_wikidata_id,
                trim(exchange_name) as exchange_name,
                nullif(upper(trim(mic)), '') as mic,
                nullif(trim(country_wikidata_id), '') as country_wikidata_id,
                nullif(trim(country_name), '') as country_name,
                nullif(upper(trim(country_iso2)), '') as country_iso2,
                listed_company_count,
                source_run_id,
                source_payload_hash,
                retrieved_at
            from {source_table}
            where nullif(trim(exchange_wikidata_id), '') is not null
        )
        select
            exchange_wikidata_id,
            coalesce(nullif(max(exchange_name), ''), exchange_wikidata_id) as exchange_name,
            mic,
            nullif(max(country_wikidata_id), '') as country_wikidata_id,
            nullif(max(country_name), '') as country_name,
            nullif(max(country_iso2), '') as country_iso2,
            cast(max(listed_company_count) as ubigint) as listed_company_count,
            'wikidata' as source_system,
            max(source_run_id) as source_run_id,
            exchange_wikidata_id || ':' || coalesce(mic, 'no_mic') as source_record_id,
            max(source_payload_hash) as source_payload_hash,
            cast(max(retrieved_at) as timestamp) as retrieved_at,
            cast(current_timestamp as timestamp) as resolved_at
        from normalized
        group by exchange_wikidata_id, mic
        """
    )


def _validate_wikidata_exchanges(
    connection: duckdb.DuckDBPyConnection,
    *,
    target_schema: str,
) -> None:
    _validate_wikidata_exchange_mics(connection, target_schema=target_schema)

    missing_exchange_ids = connection.execute(
        f"""
        select distinct listings.exchange_wikidata_id
        from {target_schema}.{tables.WIKIDATA_COMPANY_LISTINGS_TABLE} as listings
        left join {target_schema}.{tables.WIKIDATA_EXCHANGES_TABLE} as exchanges
          on exchanges.exchange_wikidata_id = listings.exchange_wikidata_id
        where exchanges.exchange_wikidata_id is null
        order by listings.exchange_wikidata_id
        """
    ).fetchall()
    if missing_exchange_ids:
        values = ", ".join(str(row[0]) for row in missing_exchange_ids[:10])
        raise ValueError(f"Wikidata listings reference missing exchanges: {values}")


def _validate_wikidata_exchange_mics(
    connection: duckdb.DuckDBPyConnection,
    *,
    target_schema: str,
) -> None:
    invalid_mics = connection.execute(
        f"""
        select mic
        from {target_schema}.{tables.WIKIDATA_EXCHANGES_TABLE}
        where mic is not null
          and not regexp_full_match(mic, '^[A-Z0-9]{{4}}$')
        order by mic
        """
    ).fetchall()
    if invalid_mics:
        values = ", ".join(str(row[0]) for row in invalid_mics[:10])
        raise ValueError(f"Invalid Wikidata MIC values: {values}")


def _create_wikidata_companies_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_COMPANIES_TABLE} as
        with normalized as (
            select
                *,
                try_cast(
                    nullif(regexp_extract(inception_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0), '')
                    as date
                ) as inception_date_value,
                try_cast(nullif(employee_count, '') as ubigint) as employee_count_value,
                try_cast(
                    nullif(
                        regexp_extract(employee_count_point_in_time, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as employee_count_point_in_time_value
            from {source_table}
        )
        select
            company_wikidata_id as wikidata_id,
            max(company_url) as wikidata_url,
            coalesce(nullif(max(company_label), ''), company_wikidata_id) as name,
            lower(coalesce(nullif(max(company_label), ''), company_wikidata_id)) as name_normalized,
            nullif(max(company_description), '') as company_description,
            nullif(max(official_name), '') as official_name,
            nullif(max(headquarters_wikidata_id), '') as headquarters_wikidata_id,
            nullif(max(headquarters_label), '') as headquarters_label,
            nullif(max(headquarters_country_wikidata_id), '') as headquarters_country_wikidata_id,
            nullif(max(headquarters_country_label), '') as headquarters_country_label,
            upper(nullif(max(headquarters_country_iso2), '')) as headquarters_country_iso2,
            case
                when nullif(max(headquarters_country_iso2), '') is not null
                    then 'wikidata_headquarters_p131_p17'
                else null
            end as country_resolution_method,
            case
                when nullif(max(headquarters_country_iso2), '') is not null
                    then 'high'
                else null
            end as country_resolution_confidence,
            max(inception_date_value) as inception_date,
            nullif(max(legal_form_wikidata_id), '') as legal_form_wikidata_id,
            nullif(max(legal_form_label), '') as legal_form_label,
            arg_max(
                employee_count_value,
                coalesce(employee_count_point_in_time_value, date '1900-01-01')
            ) as employee_count,
            max(employee_count_point_in_time_value) as employee_count_point_in_time,
            nullif(max(logo_image), '') as logo_image,
            nullif(max(logo_image_url), '') as logo_image_url,
            nullif(max(industry_wikidata_id), '') as industry_wikidata_id,
            nullif(max(industry_label), '') as industry_label,
            -- Registry-number pseudo-exchange rows carry no listing_statement_id (see
            -- build_registry_number_company_query); a company discovered only via a
            -- registry property, never on a real exchange, must not claim a current
            -- listing. Was hardcoded to 1 before the registry seed existed, when every
            -- seeded company necessarily came from an exchange listing.
            case
                when count(distinct nullif(listing_statement_id, '')) > 0 then 1
                else 0
            end as has_current_listing,
            count(distinct nullif(listing_statement_id, '')) as listing_count,
            'wikidata' as source_system,
            max(source_run_id) as source_run_id,
            company_wikidata_id as source_record_id,
            max(source_payload_hash) as source_payload_hash,
            cast(max(retrieved_at) as timestamp) as retrieved_at,
            cast(current_timestamp as timestamp) as resolved_at
        from normalized
        where nullif(company_wikidata_id, '') is not null
        group by company_wikidata_id
        """
    )


def _ensure_optional_wikidata_source_columns(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
) -> None:
    for column_name in (
        "company_description",
        "headquarters_wikidata_id",
        "headquarters_country_wikidata_id",
        "headquarters_country_label",
        "headquarters_country_iso2",
        "inception_date",
        "legal_form_wikidata_id",
        "legal_form_label",
        "employee_count",
        "employee_count_point_in_time",
        "logo_image",
        "logo_image_url",
        "industry_wikidata_id",
        "opencorporates_company_id",
        "eu_vat_number",
        "duns_number",
        "permid",
        "bloomberg_company_id",
        "linkedin_company_id",
        "parent_organization_statement_id",
        "parent_organization_wikidata_id",
        "parent_organization_label",
        "parent_organization_start_date",
        "parent_organization_end_date",
        "child_organization_statement_id",
        "child_organization_wikidata_id",
        "child_organization_label",
        "child_organization_start_date",
        "child_organization_end_date",
        "owned_by_statement_id",
        "owned_by_wikidata_id",
        "owned_by_label",
        "owned_by_start_date",
        "owned_by_end_date",
        "owner_of_statement_id",
        "owner_of_wikidata_id",
        "owner_of_label",
        "owner_of_start_date",
        "owner_of_end_date",
        "person_wikidata_id",
        "person_url",
        "person_label",
        "person_description",
        "person_image",
        "person_image_url",
        "person_birth_year",
        "role_property",
        "role_start_date",
        "role_end_date",
    ):
        connection.execute(
            f"alter table {source_table} add column if not exists {column_name} varchar"
        )


def _create_wikidata_company_listings_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_COMPANY_LISTINGS_TABLE} as
        select
            company_wikidata_id as wikidata_id,
            listing_statement_id,
            exchange_wikidata_id,
            max(exchange_name) as exchange_name,
            nullif(max(ticker), '') as ticker,
            nullif(max(isin), '') as isin,
            cast(1 as integer) as is_current,
            'wikidata' as source_system,
            max(source_run_id) as source_run_id,
            max(source_record_id) as source_record_id,
            max(source_payload_hash) as source_payload_hash,
            cast(max(retrieved_at) as timestamp) as retrieved_at,
            cast(current_timestamp as timestamp) as resolved_at
        from {source_table}
        where nullif(company_wikidata_id, '') is not null
          and nullif(listing_statement_id, '') is not null
          and nullif(exchange_wikidata_id, '') is not null
        group by company_wikidata_id, listing_statement_id, exchange_wikidata_id
        """
    )


REGISTRY_NUMBER_IDENTIFIER_COLUMNS = (
    "se_orgnr",
    "no_orgnr",
    "dk_cvr",
    "fi_business_id",
    "uk_company_number",
    "fr_siren",
    "cz_ico",
    "lv_regcode",
    "br_cnpj",
)


def _create_wikidata_company_identifiers_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    # A registry-number column only exists in the raw augmentation table
    # once some seed row carried the property; guard so the pivot below
    # never binds against a missing column.
    for column in REGISTRY_NUMBER_IDENTIFIER_COLUMNS:
        connection.execute(
            f"alter table {source_table} add column if not exists {column} varchar"
        )
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_COMPANY_IDENTIFIERS_TABLE} as
        with identifiers as (
            select
                company_wikidata_id as wikidata_id,
                'cik' as identifier_type,
                'P5531' as wikidata_property_id,
                cik as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(cik, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'lei' as identifier_type,
                'P1278' as wikidata_property_id,
                lei as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(lei, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'isin' as identifier_type,
                'P946' as wikidata_property_id,
                isin as identifier_value,
                exchange_wikidata_id as identifier_scope,
                cast(0 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(isin, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'opencorporates_company_id' as identifier_type,
                'P1320' as wikidata_property_id,
                opencorporates_company_id as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(opencorporates_company_id, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'eu_vat_number' as identifier_type,
                'P3608' as wikidata_property_id,
                eu_vat_number as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(eu_vat_number, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'duns_number' as identifier_type,
                'P2771' as wikidata_property_id,
                duns_number as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(duns_number, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'permid' as identifier_type,
                'P3347' as wikidata_property_id,
                permid as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(permid, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'bloomberg_company_id' as identifier_type,
                'P3377' as wikidata_property_id,
                bloomberg_company_id as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(bloomberg_company_id, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'linkedin_company_id' as identifier_type,
                'P4264' as wikidata_property_id,
                linkedin_company_id as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(linkedin_company_id, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'se_orgnr' as identifier_type,
                'P6460' as wikidata_property_id,
                se_orgnr as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(se_orgnr, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'no_orgnr' as identifier_type,
                'P2333' as wikidata_property_id,
                no_orgnr as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(no_orgnr, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'dk_cvr' as identifier_type,
                'P1059' as wikidata_property_id,
                dk_cvr as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(dk_cvr, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'fi_business_id' as identifier_type,
                'P12980' as wikidata_property_id,
                fi_business_id as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(fi_business_id, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'uk_company_number' as identifier_type,
                'P2622' as wikidata_property_id,
                uk_company_number as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(uk_company_number, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'fr_siren' as identifier_type,
                'P1616' as wikidata_property_id,
                fr_siren as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(fr_siren, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'cz_ico' as identifier_type,
                'P4156' as wikidata_property_id,
                cz_ico as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(cz_ico, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'lv_regcode' as identifier_type,
                'P8053' as wikidata_property_id,
                lv_regcode as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(lv_regcode, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'br_cnpj' as identifier_type,
                'P6204' as wikidata_property_id,
                br_cnpj as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(br_cnpj, '') is not null
        )
        select
            wikidata_id,
            identifier_type,
            wikidata_property_id,
            identifier_value,
            identifier_scope,
            max(is_primary) as is_primary,
            'wikidata' as source_system,
            max(source_run_id) as source_run_id,
            wikidata_id || ':' || wikidata_property_id || ':' || identifier_value as source_record_id,
            max(source_payload_hash) as source_payload_hash,
            cast(max(retrieved_at) as timestamp) as retrieved_at,
            cast(current_timestamp as timestamp) as resolved_at
        from identifiers
        group by wikidata_id, identifier_type, wikidata_property_id, identifier_value, identifier_scope
        """
    )


def _create_wikidata_company_websites_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    # Internal stage: the domain graph no longer reads wikidata_company_websites
    # directly. Consumed only by wikidata/contacts.py's canonical derivation
    # (wikidata_company_contacts / wikidata_company_domains). Do not build new
    # consumers on it.
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_COMPANY_WEBSITES_TABLE} as
        with normalized as (
            select
                company_wikidata_id as wikidata_id,
                website_url,
                lower(trim(website_url)) as website_normalized_url,
                regexp_replace(
                    regexp_replace(
                        regexp_replace(lower(trim(website_url)), '^https?://', ''),
                        '^www\\.',
                        ''
                    ),
                    '/.*$',
                    ''
                ) as website_host,
                source_run_id,
                source_payload_hash,
                retrieved_at
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(website_url, '') is not null
        )
        select
            wikidata_id,
            website_url,
            website_normalized_url,
            website_host,
            website_host as root_domain,
            cast(null as varchar) as website_path,
            'official' as website_kind,
            'wikidata' as confidence,
            'unverified' as validation_status,
            cast(1 as integer) as is_primary_candidate,
            'wikidata' as source_system,
            max(source_run_id) as source_run_id,
            wikidata_id || ':website:' || website_normalized_url as source_record_id,
            max(source_payload_hash) as source_payload_hash,
            cast(max(retrieved_at) as timestamp) as retrieved_at,
            cast(current_timestamp as timestamp) as resolved_at
        from normalized
        group by wikidata_id, website_url, website_normalized_url, website_host
        """
    )


def _create_wikidata_company_relationships_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_COMPANY_RELATIONSHIPS_TABLE} as
        with relationships as (
            select
                company_wikidata_id as subject_wikidata_id,
                parent_organization_wikidata_id as object_wikidata_id,
                'parent_organization' as relationship_type,
                'P749' as wikidata_property_id,
                parent_organization_statement_id as relationship_statement_id,
                nullif(parent_organization_label, '') as object_name,
                try_cast(
                    nullif(
                        regexp_extract(parent_organization_start_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as start_date,
                try_cast(
                    nullif(
                        regexp_extract(parent_organization_end_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as end_date,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(parent_organization_wikidata_id, '') is not null

            union all

            select
                company_wikidata_id as subject_wikidata_id,
                child_organization_wikidata_id as object_wikidata_id,
                'child_organization' as relationship_type,
                'P355' as wikidata_property_id,
                child_organization_statement_id as relationship_statement_id,
                nullif(child_organization_label, '') as object_name,
                try_cast(
                    nullif(
                        regexp_extract(child_organization_start_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as start_date,
                try_cast(
                    nullif(
                        regexp_extract(child_organization_end_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as end_date,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(child_organization_wikidata_id, '') is not null

            union all

            select
                company_wikidata_id as subject_wikidata_id,
                owned_by_wikidata_id as object_wikidata_id,
                'owned_by' as relationship_type,
                'P127' as wikidata_property_id,
                owned_by_statement_id as relationship_statement_id,
                nullif(owned_by_label, '') as object_name,
                try_cast(
                    nullif(
                        regexp_extract(owned_by_start_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as start_date,
                try_cast(
                    nullif(
                        regexp_extract(owned_by_end_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as end_date,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(owned_by_wikidata_id, '') is not null

            union all

            select
                company_wikidata_id as subject_wikidata_id,
                owner_of_wikidata_id as object_wikidata_id,
                'owner_of' as relationship_type,
                'P1830' as wikidata_property_id,
                owner_of_statement_id as relationship_statement_id,
                nullif(owner_of_label, '') as object_name,
                try_cast(
                    nullif(
                        regexp_extract(owner_of_start_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as start_date,
                try_cast(
                    nullif(
                        regexp_extract(owner_of_end_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as end_date,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(owner_of_wikidata_id, '') is not null
        )
        select
            subject_wikidata_id,
            object_wikidata_id,
            relationship_type,
            wikidata_property_id,
            nullif(max(relationship_statement_id), '') as relationship_statement_id,
            max(object_name) as object_name,
            min(start_date) as start_date,
            max(end_date) as end_date,
            case when max(end_date) is null then 1 else 0 end as is_current,
            'wikidata' as source_system,
            max(source_run_id) as source_run_id,
            subject_wikidata_id || ':' || wikidata_property_id || ':' || object_wikidata_id
                as source_record_id,
            max(source_payload_hash) as source_payload_hash,
            cast(max(retrieved_at) as timestamp) as retrieved_at,
            cast(current_timestamp as timestamp) as resolved_at
        from relationships
        group by subject_wikidata_id, object_wikidata_id, relationship_type, wikidata_property_id
        """
    )


# Static role-label map (human strings, not SPARQL labels) for the person-link
# properties build_company_people_augmentation_query pulls. Kept here in code, next to
# the table it feeds, rather than round-tripped through Wikidata's label service.
WIKIDATA_COMPANY_PEOPLE_ROLE_LABEL_SQL_CASE = """
            case role_property
                when 'P169' then 'chief executive officer'
                when 'P112' then 'founder'
                when 'P488' then 'chairperson'
                when 'P3320' then 'board member'
                when 'P127' then 'owned by'
                else role_property
            end"""


def _create_wikidata_company_people_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    # Company anchor only: every row's identity is (company_wikidata_id,
    # role_property, person_wikidata_id) -- both ids are Wikidata QIDs captured
    # directly from the SPARQL binding (build_company_people_augmentation_query),
    # never derived from a person's name/label. No fuzzy matching anywhere.
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_COMPANY_PEOPLE_TABLE} as
        with people as (
            select
                company_wikidata_id,
                person_wikidata_id,
                role_property,
                {WIKIDATA_COMPANY_PEOPLE_ROLE_LABEL_SQL_CASE} as role_label,
                try_cast(
                    nullif(regexp_extract(role_start_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0), '')
                    as date
                ) as start_date,
                try_cast(
                    nullif(regexp_extract(role_end_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0), '')
                    as date
                ) as end_date,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(person_wikidata_id, '') is not null
              and nullif(role_property, '') is not null
        )
        select
            company_wikidata_id,
            person_wikidata_id,
            role_property,
            max(role_label) as role_label,
            min(start_date) as start_date,
            max(end_date) as end_date,
            case when max(end_date) is null then 1 else 0 end as is_current,
            'wikidata' as source_system,
            max(source_run_id) as source_run_id,
            company_wikidata_id || ':' || role_property || ':' || person_wikidata_id
                as source_record_id,
            max(source_payload_hash) as source_payload_hash,
            cast(max(retrieved_at) as timestamp) as retrieved_at,
            cast(current_timestamp as timestamp) as resolved_at
        from people
        group by company_wikidata_id, person_wikidata_id, role_property
        """
    )


def _create_wikidata_persons_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    # One row per person_wikidata_id, deduped across every company that links to them
    # (e.g. a person who is CEO of one company and a board member of another still gets
    # exactly one row here). Keyed on the Wikidata QID only -- never on name/label.
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_PERSONS_TABLE} as
        select
            person_wikidata_id,
            coalesce(nullif(max(person_label), ''), person_wikidata_id) as name,
            lower(coalesce(nullif(max(person_label), ''), person_wikidata_id)) as name_normalized,
            nullif(max(person_description), '') as description,
            try_cast(nullif(max(person_birth_year), '') as usmallint) as birth_year,
            nullif(max(person_image_url), '') as image_url,
            nullif(max(person_url), '') as wikidata_url,
            'wikidata' as source_system,
            max(source_run_id) as source_run_id,
            person_wikidata_id as source_record_id,
            max(source_payload_hash) as source_payload_hash,
            cast(max(retrieved_at) as timestamp) as retrieved_at,
            cast(current_timestamp as timestamp) as resolved_at
        from {source_table}
        where nullif(person_wikidata_id, '') is not null
        group by person_wikidata_id
        """
    )


def _create_wikidata_seed_extraction_runs_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_SEED_EXTRACTION_RUNS_TABLE} as
        select
            source_run_id,
            'active_exchange_listing' as query_mode,
            cast(null as varchar) as query_exchange_id,
            max(query_hash) as query_hash,
            count(*) as row_count,
            count(distinct company_wikidata_id) as distinct_company_count,
            count(distinct listing_statement_id) as distinct_listing_count,
            count(*) filter (where nullif(website_url, '') is not null) as companies_with_website_count,
            count(*) filter (where nullif(cik, '') is not null) as companies_with_cik_count,
            count(*) filter (where nullif(lei, '') is not null) as companies_with_lei_count,
            cast(min(retrieved_at) as timestamp) as started_at,
            cast(max(retrieved_at) as timestamp) as completed_at,
            'wikidata' as source_system
        from {source_table}
        group by source_run_id
        """
    )


def _duckdb_table_count(connection: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(connection.execute(f"select count(*) from {table_name}").fetchone()[0])


def _duckdb_qualified_schema_name(database_path: str | Path, schema_name: str) -> str:
    return _duckdb_qualified_name(_duckdb_catalog_name(database_path), schema_name)


def _duckdb_schema_qualifier(database_path: str | Path, schema_name: str) -> str:
    return f"{_duckdb_catalog_name(database_path)}.{schema_name}"


def _duckdb_qualified_table_name(
    database_path: str | Path,
    schema_name: str,
    table_name: str,
) -> str:
    return _duckdb_qualified_name(
        _duckdb_catalog_name(database_path), schema_name, table_name
    )


def _duckdb_catalog_name(database_path: str | Path) -> str:
    return Path(database_path).stem


def _duckdb_qualified_name(*parts: str) -> str:
    return ".".join(_quote_duckdb_identifier(part) for part in parts)


def _quote_duckdb_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def pull_wikidata_exchanges_raw(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    config: WikidataRawPullConfig,
    partition_date: str,
    source_run_id: str,
    retrieved_at: str,
) -> dg.MaterializeResult:
    object_store.ensure_bucket(WIKIDATA_RAW_BUCKET)
    exchange_rows, active_exchanges_key = wikidata_raw_pull_exchange_rows(
        client=client,
        object_store=object_store,
        config=config,
        partition_date=partition_date,
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
    )
    return dg.MaterializeResult(
        metadata={
            "bucket": WIKIDATA_RAW_BUCKET,
            "partition_date": partition_date,
            "exchange_count": len(exchange_rows),
            "active_exchanges_key": active_exchanges_key or "",
            "exchange_ids": [
                str(exchange_row["exchange_wikidata_id"])
                for exchange_row in exchange_rows
            ],
        }
    )


def discover_wikidata_company_sources(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    config: WikidataRawPullConfig,
    partition_date: str,
    source_run_id: str,
    retrieved_at: str,
    log: Callable[..., object],
) -> dg.MaterializeResult:
    object_store.ensure_bucket(WIKIDATA_RAW_BUCKET)
    catalog_key = seed_units_object_key(partition_date=partition_date)
    if object_store.exists(catalog_key, bucket=WIKIDATA_RAW_BUCKET):
        seed_units = read_wikidata_seed_units(
            object_store=object_store,
            partition_date=partition_date,
        )
        log(
            "Reusing Wikidata seed-unit catalog: partition_date=%s units=%s key=%s",
            partition_date,
            len(seed_units),
            catalog_key,
        )
        return _wikidata_seed_units_result(
            partition_date=partition_date,
            catalog_key=catalog_key,
            seed_units=seed_units,
            reused=True,
        )

    log("Discovering Wikidata exchanges and registry seed properties")
    exchange_rows, active_exchanges_key = wikidata_raw_pull_exchange_rows(
        client=client,
        object_store=object_store,
        config=config,
        partition_date=partition_date,
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
    )
    registry_rows = wikidata_raw_pull_registry_rows(
        config=config,
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
    )
    seed_units = [_seed_unit_from_exchange_row(row) for row in exchange_rows] + [
        _seed_unit_from_registry_row(row) for row in registry_rows
    ]
    object_store.write_json(
        catalog_key,
        json.dumps(
            {
                "source": "wikidata",
                "status": "complete",
                "partition_date": partition_date,
                "source_run_id": source_run_id,
                "retrieved_at": retrieved_at,
                "active_exchanges_key": active_exchanges_key,
                "seed_units": seed_units,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        bucket=WIKIDATA_RAW_BUCKET,
    )
    log(
        "Completed Wikidata seed-unit discovery: partition_date=%s exchanges=%s "
        "registry_properties=%s units=%s key=%s",
        partition_date,
        len(exchange_rows),
        len(registry_rows),
        len(seed_units),
        catalog_key,
    )
    return _wikidata_seed_units_result(
        partition_date=partition_date,
        catalog_key=catalog_key,
        seed_units=seed_units,
        reused=False,
    )


def _wikidata_seed_units_result(
    *,
    partition_date: str,
    catalog_key: str,
    seed_units: list[dict[str, Any]],
    reused: bool,
) -> dg.MaterializeResult:
    exchange_count = sum(unit["query_mode"] == "exchange" for unit in seed_units)
    return dg.MaterializeResult(
        metadata={
            "bucket": WIKIDATA_RAW_BUCKET,
            "partition_date": partition_date,
            "seed_units_key": catalog_key,
            "unit_count": len(seed_units),
            "exchange_count": exchange_count,
            "registry_property_count": len(seed_units) - exchange_count,
            "exchange_ids": [str(unit["exchange_wikidata_id"]) for unit in seed_units],
            "reused": reused,
        }
    )


def read_wikidata_seed_units(
    *,
    object_store: ObjectStoreResource,
    partition_date: str,
) -> list[dict[str, Any]]:
    catalog_key = seed_units_object_key(partition_date=partition_date)
    if not object_store.exists(catalog_key, bucket=WIKIDATA_RAW_BUCKET):
        raise ValueError(
            f"No Wikidata seed-unit catalog exists for partition_date={partition_date}"
        )
    payload = read_wikidata_raw_payload(
        object_store=object_store,
        object_key=catalog_key,
    )
    raw_seed_units = payload.get("seed_units")
    if not isinstance(raw_seed_units, list) or not raw_seed_units:
        raise ValueError(
            "Wikidata seed-unit catalog must contain at least one seed unit: "
            f"{catalog_key}"
        )
    seed_units: list[dict[str, Any]] = []
    for raw_seed_unit in raw_seed_units:
        if not isinstance(raw_seed_unit, dict):
            raise ValueError(f"Wikidata seed unit is not an object: {catalog_key}")
        exchange_id = str(raw_seed_unit.get("exchange_wikidata_id") or "")
        if not exchange_id:
            raise ValueError(f"Wikidata seed unit has no exchange id: {catalog_key}")
        seed_units.append(raw_seed_unit)
    return seed_units


def read_wikidata_seed_unit(
    *,
    object_store: ObjectStoreResource,
    partition_date: str,
    exchange_id: str,
) -> dict[str, Any]:
    matches = [
        seed_unit
        for seed_unit in read_wikidata_seed_units(
            object_store=object_store,
            partition_date=partition_date,
        )
        if seed_unit["exchange_wikidata_id"] == exchange_id
    ]
    if len(matches) != 1:
        raise ValueError(
            "Expected one Wikidata seed unit for "
            f"partition_date={partition_date} exchange_id={exchange_id}; "
            f"found {len(matches)}"
        )
    return matches[0]


def pull_wikidata_company_pages_for_seed_unit(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    config: WikidataRawPullConfig,
    partition_date: str,
    seed_unit: dict[str, Any],
    source_run_id: str,
    retrieved_at: str,
    sleep: Callable[[float], None],
    log: Callable[..., object],
) -> dg.MaterializeResult:
    object_store.ensure_bucket(WIKIDATA_RAW_BUCKET)
    seed_unit_id = str(seed_unit["exchange_wikidata_id"])
    adopted = adopt_existing_wikidata_stage_manifest(
        object_store=object_store,
        partition_date=partition_date,
        seed_unit_id=seed_unit_id,
        data_kind=WIKIDATA_COMPANY_PAGES_KIND,
    )
    if adopted is not None:
        return wikidata_stage_result(adopted, reused=True)

    stage_key = stage_manifest_object_key(
        partition_date=partition_date,
        seed_unit_id=seed_unit_id,
        data_kind=WIKIDATA_COMPANY_PAGES_KIND,
    )
    if object_store.exists(stage_key, bucket=WIKIDATA_RAW_BUCKET):
        return wikidata_stage_result(
            read_completed_wikidata_stage_manifest(
                object_store=object_store,
                partition_date=partition_date,
                seed_unit_id=seed_unit_id,
                data_kind=WIKIDATA_COMPANY_PAGES_KIND,
            ),
            reused=True,
        )

    object_keys: list[str] = []
    row_count = 0
    page_count = 0
    offset = 0
    first_query = _seed_unit_query(seed_unit, limit=config.page_size, offset=0)
    while config.max_pages is None or page_count < config.max_pages:
        page_number = page_count + 1
        object_key = page_object_key(
            partition_date=partition_date,
            exchange_id=seed_unit_id,
            page_number=page_number,
        )
        reused_checkpoint = object_store.exists(
            object_key,
            bucket=WIKIDATA_RAW_BUCKET,
        )
        if reused_checkpoint:
            payload = read_wikidata_raw_payload(
                object_store=object_store,
                object_key=object_key,
            )
        else:
            payload = client.fetch(
                _seed_unit_query(
                    seed_unit,
                    limit=config.page_size,
                    offset=offset,
                ),
                user_agent=config.user_agent,
            )
            object_store.write_json(
                object_key,
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                bucket=WIKIDATA_RAW_BUCKET,
            )
        bindings = response_bindings(payload)
        if not bindings:
            break
        page_count += 1
        row_count += len(bindings)
        offset += len(bindings)
        object_keys.append(object_key)
        log(
            "%s Wikidata company page: partition_date=%s seed_unit=%s "
            "page=%s page_rows=%s total_rows=%s object_key=%s",
            "Reused" if reused_checkpoint else "Downloaded",
            partition_date,
            seed_unit_id,
            page_number,
            len(bindings),
            row_count,
            object_key,
        )
        if len(bindings) < config.page_size:
            break
        if config.request_delay_seconds > 0:
            sleep(config.request_delay_seconds)

    manifest = {
        "source": "wikidata",
        "status": "complete",
        "data_kind": WIKIDATA_COMPANY_PAGES_KIND,
        "partition_date": partition_date,
        "source_run_id": source_run_id,
        "seed_unit_id": seed_unit_id,
        "exchange_id": seed_unit_id,
        "query_mode": seed_unit["query_mode"],
        "exchange_name": seed_unit["exchange_name"],
        "listed_company_count_on_exchange": seed_unit[
            "listed_company_count_on_exchange"
        ],
        "mics": seed_unit["mics"],
        "country_wikidata_id": seed_unit["country_wikidata_id"],
        "country_name": seed_unit["country_name"],
        "country_iso2": seed_unit["country_iso2"],
        "registry_property_id": seed_unit["registry_property_id"],
        "page_size": config.page_size,
        "max_pages": config.max_pages,
        "query_hash": query_hash(first_query),
        "row_count": row_count,
        "page_count": page_count,
        "started_at": retrieved_at,
        "completed_at": datetime.now(UTC).isoformat(),
        "objects": object_keys,
    }
    write_wikidata_stage_manifest(
        object_store=object_store,
        manifest=manifest,
    )
    return wikidata_stage_result(manifest, reused=False)


def pull_wikidata_augmentation_for_seed_unit(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    config: WikidataRawPullConfig,
    partition_date: str,
    seed_unit_id: str,
    data_kind: str,
    source_run_id: str,
    retrieved_at: str,
    sleep: Callable[[float], None],
    log: Callable[..., object],
) -> dg.MaterializeResult:
    if data_kind not in WIKIDATA_AUGMENTATION_KIND_BY_DATA_KIND:
        raise ValueError(f"Unsupported Wikidata data kind: {data_kind}")
    adopted = adopt_existing_wikidata_stage_manifest(
        object_store=object_store,
        partition_date=partition_date,
        seed_unit_id=seed_unit_id,
        data_kind=data_kind,
    )
    if adopted is not None:
        return wikidata_stage_result(adopted, reused=True)

    stage_key = stage_manifest_object_key(
        partition_date=partition_date,
        seed_unit_id=seed_unit_id,
        data_kind=data_kind,
    )
    if object_store.exists(stage_key, bucket=WIKIDATA_RAW_BUCKET):
        return wikidata_stage_result(
            read_completed_wikidata_stage_manifest(
                object_store=object_store,
                partition_date=partition_date,
                seed_unit_id=seed_unit_id,
                data_kind=data_kind,
            ),
            reused=True,
        )

    pages_manifest = read_completed_wikidata_stage_manifest(
        object_store=object_store,
        partition_date=partition_date,
        seed_unit_id=seed_unit_id,
        data_kind=WIKIDATA_COMPANY_PAGES_KIND,
    )
    augmentation_kind = WIKIDATA_AUGMENTATION_KIND_BY_DATA_KIND[data_kind]
    query_builder = WIKIDATA_AUGMENTATION_QUERY_BY_DATA_KIND[data_kind]
    object_keys: list[str] = []
    row_count = 0
    for page_number, page_key in enumerate(pages_manifest["objects"], start=1):
        listed_bindings = response_bindings(
            read_wikidata_raw_payload(
                object_store=object_store,
                object_key=str(page_key),
            )
        )
        company_ids = sorted(
            {
                wikidata_id_from_url(binding_value(binding, "company"))
                for binding in listed_bindings
                if wikidata_id_from_url(binding_value(binding, "company"))
            }
        )
        batches = list(_batched(company_ids, config.augmentation_batch_size))
        for batch_number, company_id_batch in enumerate(batches, start=1):
            object_key = augmentation_object_key(
                partition_date=partition_date,
                exchange_id=seed_unit_id,
                augmentation_kind=augmentation_kind,
                page_number=page_number,
                batch_number=batch_number,
            )
            reused_checkpoint = object_store.exists(
                object_key,
                bucket=WIKIDATA_RAW_BUCKET,
            )
            if reused_checkpoint:
                payload = read_wikidata_raw_payload(
                    object_store=object_store,
                    object_key=object_key,
                )
            else:
                payload = client.fetch(
                    query_builder(tuple(company_id_batch)),
                    user_agent=config.user_agent,
                )
                object_store.write_json(
                    object_key,
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                    bucket=WIKIDATA_RAW_BUCKET,
                )
            object_keys.append(object_key)
            row_count += len(response_bindings(payload))
            log(
                "%s Wikidata data kind: partition_date=%s seed_unit=%s "
                "data_kind=%s page=%s batch=%s object_key=%s",
                "Reused" if reused_checkpoint else "Downloaded",
                partition_date,
                seed_unit_id,
                data_kind,
                page_number,
                batch_number,
                object_key,
            )
            if (
                config.request_delay_seconds > 0
                and not reused_checkpoint
                and (page_number, batch_number)
                != (len(pages_manifest["objects"]), len(batches))
            ):
                sleep(config.request_delay_seconds)

    manifest = {
        "source": "wikidata",
        "status": "complete",
        "data_kind": data_kind,
        "augmentation_kind": augmentation_kind,
        "partition_date": partition_date,
        "source_run_id": source_run_id,
        "seed_unit_id": seed_unit_id,
        "exchange_id": seed_unit_id,
        "augmentation_batch_size": config.augmentation_batch_size,
        "row_count": row_count,
        "started_at": retrieved_at,
        "completed_at": datetime.now(UTC).isoformat(),
        "objects": object_keys,
    }
    write_wikidata_stage_manifest(
        object_store=object_store,
        manifest=manifest,
    )
    return wikidata_stage_result(manifest, reused=False)


def materialize_wikidata_persons_for_seed_unit(
    *,
    object_store: ObjectStoreResource,
    partition_date: str,
    seed_unit_id: str,
) -> dg.MaterializeResult:
    adopted = adopt_existing_wikidata_stage_manifest(
        object_store=object_store,
        partition_date=partition_date,
        seed_unit_id=seed_unit_id,
        data_kind=WIKIDATA_PERSONS_KIND,
    )
    if adopted is not None:
        return wikidata_stage_result(adopted, reused=True)
    stage_key = stage_manifest_object_key(
        partition_date=partition_date,
        seed_unit_id=seed_unit_id,
        data_kind=WIKIDATA_PERSONS_KIND,
    )
    if object_store.exists(stage_key, bucket=WIKIDATA_RAW_BUCKET):
        return wikidata_stage_result(
            read_completed_wikidata_stage_manifest(
                object_store=object_store,
                partition_date=partition_date,
                seed_unit_id=seed_unit_id,
                data_kind=WIKIDATA_PERSONS_KIND,
            ),
            reused=True,
        )
    people_manifest = read_completed_wikidata_stage_manifest(
        object_store=object_store,
        partition_date=partition_date,
        seed_unit_id=seed_unit_id,
        data_kind=WIKIDATA_COMPANY_PEOPLE_KIND,
    )
    manifest = {
        **people_manifest,
        "data_kind": WIKIDATA_PERSONS_KIND,
        "completed_at": datetime.now(UTC).isoformat(),
    }
    write_wikidata_stage_manifest(
        object_store=object_store,
        manifest=manifest,
    )
    return wikidata_stage_result(manifest, reused=False)


def materialize_wikidata_company_source_snapshot(
    *,
    object_store: ObjectStoreResource,
    partition_date: str,
    seed_unit_id: str,
) -> dg.MaterializeResult:
    combined_manifest_key = manifest_object_key(
        partition_date=partition_date,
        exchange_id=seed_unit_id,
    )
    if object_store.exists(combined_manifest_key, bucket=WIKIDATA_RAW_BUCKET):
        manifest = read_wikidata_raw_payload(
            object_store=object_store,
            object_key=combined_manifest_key,
        )
        _validate_completed_wikidata_unit_manifest(
            object_store=object_store,
            manifest=manifest,
            manifest_key=combined_manifest_key,
            partition_date=partition_date,
            exchange_id=seed_unit_id,
        )
        return _wikidata_unit_result(
            partition_date=partition_date,
            exchange_id=seed_unit_id,
            manifest_key=combined_manifest_key,
            manifest=manifest,
            reused=True,
        )

    stage_manifests = {
        data_kind: read_completed_wikidata_stage_manifest(
            object_store=object_store,
            partition_date=partition_date,
            seed_unit_id=seed_unit_id,
            data_kind=data_kind,
        )
        for data_kind in WIKIDATA_ALL_SEED_UNIT_DATA_KINDS
    }
    pages_manifest = stage_manifests[WIKIDATA_COMPANY_PAGES_KIND]
    augmentation_manifests = [
        stage_manifests[data_kind] for data_kind in WIKIDATA_NETWORK_DATA_KINDS
    ]
    manifest = {
        **pages_manifest,
        "status": "complete",
        "augmentation_batch_size": max(
            (
                int(stage_manifest.get("augmentation_batch_size") or 0)
                for stage_manifest in augmentation_manifests
            ),
            default=0,
        ),
        "augmentation_row_count": sum(
            int(stage_manifest.get("row_count") or 0)
            for stage_manifest in augmentation_manifests
        ),
        "augmentation_objects": [
            str(object_key)
            for stage_manifest in augmentation_manifests
            for object_key in stage_manifest["objects"]
        ],
        "completed_at": datetime.now(UTC).isoformat(),
    }
    manifest.pop("data_kind", None)
    object_store.write_json(
        combined_manifest_key,
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
        bucket=WIKIDATA_RAW_BUCKET,
    )
    return _wikidata_unit_result(
        partition_date=partition_date,
        exchange_id=seed_unit_id,
        manifest_key=combined_manifest_key,
        manifest=manifest,
        reused=False,
    )


def adopt_existing_wikidata_stage_manifest(
    *,
    object_store: ObjectStoreResource,
    partition_date: str,
    seed_unit_id: str,
    data_kind: str,
) -> dict[str, Any] | None:
    stage_key = stage_manifest_object_key(
        partition_date=partition_date,
        seed_unit_id=seed_unit_id,
        data_kind=data_kind,
    )
    if object_store.exists(stage_key, bucket=WIKIDATA_RAW_BUCKET):
        return None
    combined_key = manifest_object_key(
        partition_date=partition_date,
        exchange_id=seed_unit_id,
    )
    if not object_store.exists(combined_key, bucket=WIKIDATA_RAW_BUCKET):
        return None
    combined = read_wikidata_raw_payload(
        object_store=object_store,
        object_key=combined_key,
    )
    _validate_completed_wikidata_unit_manifest(
        object_store=object_store,
        manifest=combined,
        manifest_key=combined_key,
        partition_date=partition_date,
        exchange_id=seed_unit_id,
    )
    if data_kind == WIKIDATA_COMPANY_PAGES_KIND:
        object_keys = [str(key) for key in combined.get("objects", [])]
        row_count = int(combined.get("row_count") or 0)
    else:
        source_data_kind = (
            WIKIDATA_COMPANY_PEOPLE_KIND
            if data_kind == WIKIDATA_PERSONS_KIND
            else data_kind
        )
        augmentation_kind = WIKIDATA_AUGMENTATION_KIND_BY_DATA_KIND[source_data_kind]
        marker = f"/augmentation_kind={augmentation_kind}/"
        object_keys = [
            str(key)
            for key in combined.get("augmentation_objects", [])
            if marker in str(key)
        ]
        row_count = sum(
            len(
                response_bindings(
                    read_wikidata_raw_payload(
                        object_store=object_store,
                        object_key=object_key,
                    )
                )
            )
            for object_key in object_keys
        )
    manifest = {
        "source": "wikidata",
        "status": "complete",
        "data_kind": data_kind,
        "partition_date": partition_date,
        "source_run_id": partition_date,
        "seed_unit_id": seed_unit_id,
        "exchange_id": seed_unit_id,
        "row_count": row_count,
        "started_at": combined.get("started_at"),
        "completed_at": combined.get("completed_at"),
        "objects": object_keys,
    }
    if data_kind == WIKIDATA_COMPANY_PAGES_KIND:
        for field_name in (
            "query_mode",
            "exchange_name",
            "listed_company_count_on_exchange",
            "mics",
            "country_wikidata_id",
            "country_name",
            "country_iso2",
            "registry_property_id",
            "page_size",
            "max_pages",
            "query_hash",
            "page_count",
        ):
            manifest[field_name] = combined.get(field_name)
    else:
        manifest["augmentation_kind"] = (
            "people"
            if data_kind == WIKIDATA_PERSONS_KIND
            else WIKIDATA_AUGMENTATION_KIND_BY_DATA_KIND[data_kind]
        )
        manifest["augmentation_batch_size"] = combined.get("augmentation_batch_size")
    write_wikidata_stage_manifest(
        object_store=object_store,
        manifest=manifest,
    )
    return manifest


def read_completed_wikidata_stage_manifest(
    *,
    object_store: ObjectStoreResource,
    partition_date: str,
    seed_unit_id: str,
    data_kind: str,
) -> dict[str, Any]:
    manifest_key = stage_manifest_object_key(
        partition_date=partition_date,
        seed_unit_id=seed_unit_id,
        data_kind=data_kind,
    )
    if not object_store.exists(manifest_key, bucket=WIKIDATA_RAW_BUCKET):
        raise ValueError(f"Missing Wikidata stage manifest: {manifest_key}")
    manifest = read_wikidata_raw_payload(
        object_store=object_store,
        object_key=manifest_key,
    )
    if (
        manifest.get("status") != "complete"
        or manifest.get("partition_date") != partition_date
        or manifest.get("seed_unit_id") != seed_unit_id
        or manifest.get("data_kind") != data_kind
    ):
        raise ValueError(f"Invalid Wikidata stage manifest: {manifest_key}")
    missing_keys = [
        str(object_key)
        for object_key in manifest.get("objects", [])
        if not object_store.exists(str(object_key), bucket=WIKIDATA_RAW_BUCKET)
    ]
    if missing_keys:
        raise ValueError(
            f"Wikidata stage manifest references missing objects: {missing_keys[:5]}"
        )
    return manifest


def write_wikidata_stage_manifest(
    *,
    object_store: ObjectStoreResource,
    manifest: dict[str, Any],
) -> None:
    object_store.write_json(
        stage_manifest_object_key(
            partition_date=str(manifest["partition_date"]),
            seed_unit_id=str(manifest["seed_unit_id"]),
            data_kind=str(manifest["data_kind"]),
        ),
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
        bucket=WIKIDATA_RAW_BUCKET,
    )


def wikidata_stage_result(
    manifest: dict[str, Any],
    *,
    reused: bool,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata={
            "bucket": WIKIDATA_RAW_BUCKET,
            "partition_date": str(manifest["partition_date"]),
            "seed_unit_id": str(manifest["seed_unit_id"]),
            "data_kind": str(manifest["data_kind"]),
            "manifest_key": stage_manifest_object_key(
                partition_date=str(manifest["partition_date"]),
                seed_unit_id=str(manifest["seed_unit_id"]),
                data_kind=str(manifest["data_kind"]),
            ),
            "object_count": len(manifest.get("objects", [])),
            "row_count": int(manifest.get("row_count") or 0),
            "reused": reused,
        }
    )


def _validate_completed_wikidata_unit_manifest(
    *,
    object_store: ObjectStoreResource,
    manifest: dict[str, Any],
    manifest_key: str,
    partition_date: str,
    exchange_id: str,
) -> None:
    if manifest.get("status") != "complete":
        raise ValueError(f"Wikidata unit manifest is not complete: {manifest_key}")
    if manifest.get("partition_date") != partition_date:
        raise ValueError(f"Wikidata unit manifest has wrong partition: {manifest_key}")
    if manifest.get("exchange_id") != exchange_id:
        raise ValueError(f"Wikidata unit manifest has wrong exchange: {manifest_key}")
    raw_object_keys = [
        *manifest.get("objects", []),
        *manifest.get("augmentation_objects", []),
    ]
    missing_keys = [
        str(object_key)
        for object_key in raw_object_keys
        if not object_store.exists(str(object_key), bucket=WIKIDATA_RAW_BUCKET)
    ]
    if missing_keys:
        raise ValueError(
            f"Wikidata unit manifest references missing objects: {missing_keys[:5]}"
        )


def _wikidata_unit_result(
    *,
    partition_date: str,
    exchange_id: str,
    manifest_key: str,
    manifest: dict[str, Any],
    reused: bool,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata={
            "bucket": WIKIDATA_RAW_BUCKET,
            "partition_date": partition_date,
            "exchange_id": exchange_id,
            "page_count": int(manifest.get("page_count") or 0),
            "row_count": int(manifest.get("row_count") or 0),
            "augmentation_object_count": len(manifest.get("augmentation_objects", [])),
            "augmentation_row_count": int(manifest.get("augmentation_row_count") or 0),
            "manifest_key": manifest_key,
            "reused": reused,
        }
    )


def finalize_wikidata_raw_snapshot(
    *,
    object_store: ObjectStoreResource,
    partition_date: str,
    completed_at: str,
) -> dg.MaterializeResult:
    seed_units = read_wikidata_seed_units(
        object_store=object_store,
        partition_date=partition_date,
    )
    manifests: list[dict[str, Any]] = []
    manifest_keys: list[str] = []
    for seed_unit in seed_units:
        exchange_id = str(seed_unit["exchange_wikidata_id"])
        manifest_key = manifest_object_key(
            partition_date=partition_date,
            exchange_id=exchange_id,
        )
        if not object_store.exists(manifest_key, bucket=WIKIDATA_RAW_BUCKET):
            raise ValueError(
                "Wikidata weekly snapshot is incomplete: "
                f"partition_date={partition_date} missing_exchange={exchange_id}"
            )
        manifest = read_wikidata_raw_payload(
            object_store=object_store,
            object_key=manifest_key,
        )
        _validate_completed_wikidata_unit_manifest(
            object_store=object_store,
            manifest=manifest,
            manifest_key=manifest_key,
            partition_date=partition_date,
            exchange_id=exchange_id,
        )
        manifests.append(manifest)
        manifest_keys.append(manifest_key)

    snapshot_manifest_key = snapshot_manifest_object_key(partition_date=partition_date)
    exchange_count = sum(
        manifest.get("query_mode") == "exchange" for manifest in manifests
    )
    row_count = sum(int(manifest.get("row_count") or 0) for manifest in manifests)
    page_count = sum(int(manifest.get("page_count") or 0) for manifest in manifests)
    augmentation_row_count = sum(
        int(manifest.get("augmentation_row_count") or 0) for manifest in manifests
    )
    augmentation_object_count = sum(
        len(manifest.get("augmentation_objects", [])) for manifest in manifests
    )
    object_store.write_json(
        snapshot_manifest_key,
        json.dumps(
            {
                "source": "wikidata",
                "status": "complete",
                "partition_date": partition_date,
                "source_run_id": partition_date,
                "completed_at": completed_at,
                "exchange_count": exchange_count,
                "registry_property_count": len(manifests) - exchange_count,
                "page_count": page_count,
                "row_count": row_count,
                "augmentation_object_count": augmentation_object_count,
                "augmentation_row_count": augmentation_row_count,
                "manifest_keys": manifest_keys,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        bucket=WIKIDATA_RAW_BUCKET,
    )
    return dg.MaterializeResult(
        metadata={
            "bucket": WIKIDATA_RAW_BUCKET,
            "partition_date": partition_date,
            "unit_count": len(manifests),
            "exchange_count": exchange_count,
            "registry_property_count": len(manifests) - exchange_count,
            "page_count": page_count,
            "row_count": row_count,
            "augmentation_object_count": augmentation_object_count,
            "augmentation_row_count": augmentation_row_count,
            "snapshot_manifest_key": snapshot_manifest_key,
        }
    )


def resolve_wikidata_snapshot_partition_date(
    *,
    object_store: ObjectStoreResource,
    configured_partition_date: str | None,
) -> str:
    if configured_partition_date is not None:
        completed_wikidata_raw_manifest_keys(
            object_store=object_store,
            partition_date=configured_partition_date,
        )
        return configured_partition_date

    snapshot_keys = [
        key
        for key in object_store.list_keys(
            "partition_date=",
            bucket=WIKIDATA_RAW_BUCKET,
        )
        if key.endswith("/snapshot_manifest.json")
    ]
    if not snapshot_keys:
        raise ValueError("No completed Wikidata weekly raw snapshot exists")
    return sorted(snapshot_keys)[-1].split("/", 1)[0].removeprefix("partition_date=")


def _seed_unit_from_exchange_row(exchange_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "exchange_wikidata_id": exchange_row["exchange_wikidata_id"],
        "exchange_name": exchange_row["exchange_name"],
        "listed_company_count_on_exchange": exchange_row[
            "listed_company_count_on_exchange"
        ],
        "mics": exchange_row["mics"],
        "country_wikidata_id": exchange_row["country_wikidata_id"],
        "country_name": exchange_row["country_name"],
        "country_iso2": exchange_row["country_iso2"],
        "query_mode": "exchange",
        "registry_property_id": None,
    }


def _seed_unit_from_registry_row(registry_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "exchange_wikidata_id": registry_row["exchange_wikidata_id"],
        "exchange_name": registry_row["exchange_name"],
        "listed_company_count_on_exchange": registry_row[
            "listed_company_count_on_exchange"
        ],
        "mics": [],
        "country_wikidata_id": "",
        "country_name": "",
        "country_iso2": "",
        "query_mode": "registry_number",
        "registry_property_id": registry_row["registry_property_id"],
    }


def _seed_unit_query(seed_unit: dict[str, Any], *, limit: int, offset: int) -> str:
    if seed_unit["query_mode"] == "registry_number":
        return build_registry_number_company_query(
            property_id=seed_unit["registry_property_id"],
            limit=limit,
            offset=offset,
        )
    return build_listed_company_query(
        exchange_id=seed_unit["exchange_wikidata_id"],
        limit=limit,
        offset=offset,
    )


def wikidata_raw_pull_registry_rows(
    *,
    config: WikidataRawPullConfig,
    source_run_id: str,
    retrieved_at: str,
) -> list[dict[str, Any]]:
    if not config.include_registry_seed:
        return []
    property_ids = config.configured_registry_property_ids()
    return [
        {
            "exchange_wikidata_id": registry_pseudo_exchange_id(property_id),
            "exchange_name": f"Wikidata registry-number seed: {property_id}",
            "listed_company_count_on_exchange": 0,
            "registry_property_id": property_id,
            "source_run_id": source_run_id,
            "retrieved_at": retrieved_at,
            "source_row_number": row_number,
        }
        for row_number, property_id in enumerate(property_ids, start=1)
    ]


def read_wikidata_raw_payload(
    *,
    object_store: ObjectStoreResource,
    object_key: str,
) -> dict[str, Any]:
    payload = json.loads(
        object_store.read_bytes(object_key, bucket=WIKIDATA_RAW_BUCKET).decode("utf-8")
    )
    if not isinstance(payload, dict):
        raise ValueError(f"Wikidata checkpoint is not a JSON object: {object_key}")
    return payload


def _batched[T](items: list[T], batch_size: int) -> Iterator[list[T]]:
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


def wikidata_raw_pull_exchange_rows(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    config: WikidataRawPullConfig,
    partition_date: str,
    source_run_id: str,
    retrieved_at: str,
) -> tuple[list[dict[str, Any]], str | None]:
    configured_exchange_ids = config.configured_exchange_ids()
    if configured_exchange_ids is not None:
        return (
            wikidata_raw_pull_configured_exchange_rows(
                configured_exchange_ids,
                source_run_id=source_run_id,
                retrieved_at=retrieved_at,
                max_exchanges=config.max_exchanges,
            ),
            None,
        )

    exchange_list_key = active_exchanges_object_key(
        partition_date=partition_date,
    )
    if object_store.exists(exchange_list_key, bucket=WIKIDATA_RAW_BUCKET):
        payload = read_wikidata_raw_payload(
            object_store=object_store,
            object_key=exchange_list_key,
        )
    else:
        query = build_active_listed_exchanges_query()
        payload = client.fetch(query, user_agent=config.user_agent)
        object_store.write_json(
            exchange_list_key,
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
            bucket=WIKIDATA_RAW_BUCKET,
        )
    binding_rows = [
        active_listed_exchange_row_from_binding(
            binding,
            source_run_id=source_run_id,
            retrieved_at=retrieved_at,
            source_row_number=row_number,
        )
        for row_number, binding in enumerate(response_bindings(payload), start=1)
    ]
    exchange_rows = collapse_active_listed_exchange_rows(binding_rows)
    if config.max_exchanges is not None:
        exchange_rows = exchange_rows[: config.max_exchanges]
    if not exchange_rows:
        raise ValueError("Wikidata active listed-exchange query returned no exchanges")
    return exchange_rows, exchange_list_key


def wikidata_raw_pull_configured_exchange_rows(
    exchange_ids: tuple[str, ...],
    *,
    source_run_id: str,
    retrieved_at: str,
    max_exchanges: int | None,
) -> list[dict[str, Any]]:
    limited_exchange_ids = (
        exchange_ids if max_exchanges is None else exchange_ids[:max_exchanges]
    )
    return [
        {
            "exchange_wikidata_id": exchange_id,
            "exchange_name": "",
            "mics": [],
            "country_wikidata_id": "",
            "country_name": "",
            "country_iso2": "",
            "listed_company_count_on_exchange": 0,
            "source_run_id": source_run_id,
            "retrieved_at": retrieved_at,
            "source_row_number": row_number,
        }
        for row_number, exchange_id in enumerate(limited_exchange_ids, start=1)
    ]


# Company-source discovery declares ordering-only dependencies on every registry
# seed spine so Dagster shows where registry identifiers originate. The publish
# selection must exclude those country pipelines; they are inputs, not work that
# Wikidata should rematerialize.
_wikidata_registry_seed_spine_exclusion = dg.AssetSelection.assets(
    *(
        dg.AssetKey(spine_asset_key)
        for spine_asset_key in WIKIDATA_REGISTRY_SEED_SPINE_ASSET_KEYS
    )
).upstream()

_wikidata_raw_assets = dg.AssetSelection.assets(
    "wikidata_exchanges_raw",
    "wikidata_company_source_units",
    "wikidata_company_pages_raw",
    "wikidata_company_profiles_raw",
    "wikidata_company_identifiers_raw",
    "wikidata_company_relationships_raw",
    "wikidata_company_people_raw",
    "wikidata_persons_raw",
    "wikidata_company_source_snapshot",
    "wikidata_raw_snapshot",
)
wikidata_publish_selection = (
    (
        dg.AssetSelection.assets("wikidata_snapshot_complete").upstream()
        | dg.AssetSelection.assets(
            "wikidata_clickhouse_canonical_contacts",
            "wikidata_company_source_records_clickhouse",
        )
    )
    - _wikidata_registry_seed_spine_exclusion
    - _wikidata_raw_assets
)

wikidata_publish_job = dg.define_asset_job(
    "wikidata_publish_job",
    selection=wikidata_publish_selection,
    run_tags={"dagster/max_runtime": "0"},
)
wikidata_exchange_discovery_job = dg.define_asset_job(
    "wikidata_exchange_discovery_job",
    selection=dg.AssetSelection.assets("wikidata_exchanges_raw"),
)
wikidata_company_source_discovery_job = dg.define_asset_job(
    "wikidata_company_source_discovery_job",
    selection=dg.AssetSelection.assets("wikidata_company_source_units"),
)
wikidata_company_pages_job = dg.define_asset_job(
    "wikidata_company_pages_job",
    selection=dg.AssetSelection.assets("wikidata_company_pages_raw"),
)
wikidata_company_profiles_job = dg.define_asset_job(
    "wikidata_company_profiles_job",
    selection=dg.AssetSelection.assets("wikidata_company_profiles_raw"),
)
wikidata_company_identifiers_job = dg.define_asset_job(
    "wikidata_company_identifiers_job",
    selection=dg.AssetSelection.assets("wikidata_company_identifiers_raw"),
)
wikidata_company_relationships_job = dg.define_asset_job(
    "wikidata_company_relationships_job",
    selection=dg.AssetSelection.assets("wikidata_company_relationships_raw"),
)
wikidata_company_people_job = dg.define_asset_job(
    "wikidata_company_people_job",
    selection=dg.AssetSelection.assets("wikidata_company_people_raw"),
)
wikidata_persons_job = dg.define_asset_job(
    "wikidata_persons_job",
    selection=dg.AssetSelection.assets("wikidata_persons_raw"),
)
wikidata_company_source_snapshot_job = dg.define_asset_job(
    "wikidata_company_source_snapshot_job",
    selection=dg.AssetSelection.assets("wikidata_company_source_snapshot"),
)
wikidata_raw_snapshot_job = dg.define_asset_job(
    "wikidata_raw_snapshot_job",
    selection=dg.AssetSelection.assets("wikidata_raw_snapshot"),
)


@dg.schedule(
    name="wikidata_weekly_schedule",
    cron_schedule="30 3 * * 1",
    execution_timezone="Europe/Belgrade",
    job=wikidata_exchange_discovery_job,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def wikidata_weekly_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.RunRequest:
    if context.scheduled_execution_time is None:
        raise ValueError("Wikidata weekly schedule requires a scheduled execution time")
    return dg.RunRequest(
        partition_key=context.scheduled_execution_time.strftime("%Y-%m-%d")
    )


@dg.asset_sensor(
    asset_key=dg.AssetKey("wikidata_exchanges_raw"),
    job=wikidata_company_source_discovery_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def wikidata_exchange_catalog_sensor(
    _context: dg.SensorEvaluationContext,
    event: dg.EventLogEntry,
) -> dg.RunRequest:
    materialization = event.asset_materialization
    if materialization is None or materialization.partition is None:
        raise ValueError("Wikidata exchange materialization has no partition date")
    partition_date = materialization.partition
    return dg.RunRequest(
        run_key=f"wikidata-company-sources:{partition_date}",
        partition_key=partition_date,
    )


@dg.asset_sensor(
    asset_key=dg.AssetKey("wikidata_company_source_units"),
    job=wikidata_company_pages_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
    required_resource_keys={"object_store"},
)
def wikidata_company_source_sensor(
    context: dg.SensorEvaluationContext,
    event: dg.EventLogEntry,
) -> dg.SensorResult:
    materialization = event.asset_materialization
    if materialization is None or materialization.partition is None:
        raise ValueError(
            "Wikidata company-source materialization has no partition date"
        )
    partition_date = materialization.partition
    object_store = context.resources.object_store
    company_sources = read_wikidata_seed_units(
        object_store=object_store,
        partition_date=partition_date,
    )
    company_source_ids = [
        str(company_source["exchange_wikidata_id"])
        for company_source in company_sources
    ]
    return dg.SensorResult(
        dynamic_partitions_requests=[
            WIKIDATA_COMPANY_SOURCE_PARTITIONS.build_add_request(company_source_ids)
        ],
        run_requests=[
            dg.RunRequest(
                run_key=f"wikidata-company-pages:{partition_date}:{company_source_id}",
                partition_key=str(
                    dg.MultiPartitionKey(
                        {
                            "date": partition_date,
                            "company_source": company_source_id,
                        }
                    )
                ),
            )
            for company_source_id in company_source_ids
        ],
    )


@dg.asset_sensor(
    asset_key=dg.AssetKey("wikidata_company_pages_raw"),
    jobs=[
        wikidata_company_profiles_job,
        wikidata_company_identifiers_job,
        wikidata_company_relationships_job,
        wikidata_company_people_job,
    ],
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def wikidata_company_pages_sensor(
    _context: dg.SensorEvaluationContext,
    event: dg.EventLogEntry,
) -> list[dg.RunRequest]:
    materialization = event.asset_materialization
    if materialization is None or materialization.partition is None:
        raise ValueError("Wikidata company-page materialization has no partition")
    partition_key = materialization.partition
    jobs = (
        wikidata_company_profiles_job,
        wikidata_company_identifiers_job,
        wikidata_company_relationships_job,
        wikidata_company_people_job,
    )
    return [
        dg.RunRequest(
            run_key=f"{job.name}:{partition_key}",
            partition_key=partition_key,
            job_name=job.name,
        )
        for job in jobs
    ]


@dg.asset_sensor(
    asset_key=dg.AssetKey("wikidata_company_people_raw"),
    job=wikidata_persons_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def wikidata_company_people_sensor(
    _context: dg.SensorEvaluationContext,
    event: dg.EventLogEntry,
) -> dg.RunRequest:
    materialization = event.asset_materialization
    if materialization is None or materialization.partition is None:
        raise ValueError("Wikidata company-people materialization has no partition")
    partition_key = materialization.partition
    return dg.RunRequest(
        run_key=f"wikidata-persons:{partition_key}",
        partition_key=partition_key,
    )


@dg.sensor(
    job=wikidata_company_source_snapshot_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60,
    required_resource_keys={"object_store"},
)
def wikidata_company_source_snapshot_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult | dg.SkipReason:
    object_store = context.resources.object_store
    catalog_keys = [
        key
        for key in object_store.list_keys(
            "partition_date=",
            bucket=WIKIDATA_RAW_BUCKET,
        )
        if key.endswith("/seed_units.json")
    ]
    ready_partitions: list[tuple[str, str]] = []
    for catalog_key in sorted(catalog_keys):
        partition_date = catalog_key.split("/", 1)[0].removeprefix("partition_date=")
        company_sources = read_wikidata_seed_units(
            object_store=object_store,
            partition_date=partition_date,
        )
        for company_source in company_sources:
            company_source_id = str(company_source["exchange_wikidata_id"])
            if object_store.exists(
                manifest_object_key(
                    partition_date=partition_date,
                    exchange_id=company_source_id,
                ),
                bucket=WIKIDATA_RAW_BUCKET,
            ):
                continue
            if all(
                object_store.exists(
                    stage_manifest_object_key(
                        partition_date=partition_date,
                        seed_unit_id=company_source_id,
                        data_kind=data_kind,
                    ),
                    bucket=WIKIDATA_RAW_BUCKET,
                )
                for data_kind in WIKIDATA_ALL_SEED_UNIT_DATA_KINDS
            ):
                ready_partitions.append((partition_date, company_source_id))

    if not ready_partitions:
        return dg.SkipReason(
            "No completed Wikidata company-source stages await aggregation"
        )
    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(
                run_key=f"wikidata-source-snapshot:{partition_date}:{company_source_id}",
                partition_key=str(
                    dg.MultiPartitionKey(
                        {
                            "date": partition_date,
                            "company_source": company_source_id,
                        }
                    )
                ),
            )
            for partition_date, company_source_id in ready_partitions
        ]
    )


@dg.sensor(
    job=wikidata_raw_snapshot_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60,
    required_resource_keys={"object_store"},
)
def wikidata_raw_snapshot_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult | dg.SkipReason:
    object_store = context.resources.object_store
    catalog_keys = [
        key
        for key in object_store.list_keys(
            "partition_date=",
            bucket=WIKIDATA_RAW_BUCKET,
        )
        if key.endswith("/seed_units.json")
    ]
    ready_partition_dates: list[str] = []
    for catalog_key in sorted(catalog_keys):
        partition_date = catalog_key.split("/", 1)[0].removeprefix("partition_date=")
        if object_store.exists(
            snapshot_manifest_object_key(partition_date=partition_date),
            bucket=WIKIDATA_RAW_BUCKET,
        ):
            continue
        company_sources = read_wikidata_seed_units(
            object_store=object_store,
            partition_date=partition_date,
        )
        if all(
            object_store.exists(
                manifest_object_key(
                    partition_date=partition_date,
                    exchange_id=str(company_source["exchange_wikidata_id"]),
                ),
                bucket=WIKIDATA_RAW_BUCKET,
            )
            for company_source in company_sources
        ):
            ready_partition_dates.append(partition_date)

    if not ready_partition_dates:
        return dg.SkipReason("No complete Wikidata weekly partition awaits aggregation")
    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(
                run_key=f"wikidata-raw-snapshot:{partition_date}",
                partition_key=partition_date,
            )
            for partition_date in ready_partition_dates
        ]
    )


@dg.asset_sensor(
    asset_key=dg.AssetKey("wikidata_raw_snapshot"),
    job=wikidata_publish_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def wikidata_publish_sensor(
    _context: dg.SensorEvaluationContext,
    event: dg.EventLogEntry,
) -> dg.RunRequest:
    materialization = event.asset_materialization
    if materialization is None or materialization.partition is None:
        raise ValueError("Wikidata raw snapshot materialization has no partition date")
    partition_date = materialization.partition
    duckdb_assets = (
        "wikidata_companies_duckdb",
        "wikidata_exchanges_duckdb",
        "wikidata_company_listings_duckdb",
        "wikidata_company_identifiers_duckdb",
        "wikidata_company_websites_duckdb",
        "wikidata_company_relationships_duckdb",
        "wikidata_company_people_duckdb",
        "wikidata_persons_duckdb",
        "wikidata_seed_extraction_runs_duckdb",
    )
    return dg.RunRequest(
        run_key=f"wikidata-publish:{partition_date}",
        run_config={
            "ops": {
                asset_name: {"config": {"partition_date": partition_date}}
                for asset_name in duckdb_assets
            }
        },
    )


defs = dg.Definitions(
    assets=[
        wikidata_exchanges_raw,
        wikidata_company_source_units,
        wikidata_company_pages_raw,
        wikidata_company_profiles_raw,
        wikidata_company_identifiers_raw,
        wikidata_company_relationships_raw,
        wikidata_company_people_raw,
        wikidata_persons_raw,
        wikidata_company_source_snapshot,
        wikidata_raw_snapshot,
        wikidata_companies_duckdb,
        wikidata_exchanges_duckdb,
        wikidata_company_listings_duckdb,
        wikidata_company_identifiers_duckdb,
        wikidata_company_websites_duckdb,
        wikidata_company_relationships_duckdb,
        wikidata_company_people_duckdb,
        wikidata_persons_duckdb,
        wikidata_seed_extraction_runs_duckdb,
        wikidata_companies_clickhouse,
        wikidata_exchanges_clickhouse,
        wikidata_company_listings_clickhouse,
        wikidata_company_identifiers_clickhouse,
        wikidata_company_websites_clickhouse,
        wikidata_company_relationships_clickhouse,
        wikidata_company_people_clickhouse,
        wikidata_persons_clickhouse,
        wikidata_seed_extraction_runs_clickhouse,
        wikidata_snapshot_complete,
    ],
    jobs=[
        wikidata_publish_job,
        wikidata_exchange_discovery_job,
        wikidata_company_source_discovery_job,
        wikidata_company_pages_job,
        wikidata_company_profiles_job,
        wikidata_company_identifiers_job,
        wikidata_company_relationships_job,
        wikidata_company_people_job,
        wikidata_persons_job,
        wikidata_company_source_snapshot_job,
        wikidata_raw_snapshot_job,
    ],
    schedules=[wikidata_weekly_schedule],
    sensors=[
        wikidata_exchange_catalog_sensor,
        wikidata_company_source_sensor,
        wikidata_company_pages_sensor,
        wikidata_company_people_sensor,
        wikidata_company_source_snapshot_sensor,
        wikidata_raw_snapshot_sensor,
        wikidata_publish_sensor,
    ],
)
