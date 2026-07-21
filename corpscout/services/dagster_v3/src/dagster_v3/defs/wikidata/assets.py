import json
import time
from collections.abc import Callable
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
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
    manifest_object_key,
    page_object_key,
    query_hash,
    registry_pseudo_exchange_id,
    response_bindings,
    snapshot_manifest_object_key,
    iter_wikidata_exchange_rows,
    iter_wikidata_listed_company_rows,
    wikidata_id_from_url,
)
from dagster_v3.defs.wikidata import tables
from dagster_v3.defs.common.tags import HEAVY_BULK_RUN_TAGS

GROUP_NAME = "wikidata"
WIKIDATA_DUCKDB_SCHEMA = "wikidata"
WIKIDATA_DUCKDB_DIRECTORY = Path("data/wikidata")
WIKIDATA_CLICKHOUSE_POOL = "wikidata_clickhouse"
WIKIDATA_EMPTY_ALLOWED_TABLES = (
    tables.WIKIDATA_COMPANY_PEOPLE_TABLE,
    tables.WIKIDATA_PERSONS_TABLE,
)


@dg.asset(
    # Ordering-only deps for discoverability: every country with a national
    # registry-number Wikidata property (see WikidataRegistrySeedSpec) shows up
    # connected to the wikidata seed in the Dagster UI asset graph, and a country
    # lacking the edge is visibly unwired. These deps do NOT force materialization —
    # the seed stays on its own weekly schedule, and wikidata_company_seed_selection
    # explicitly excludes each spine's upstream so the weekly job doesn't balloon into
    # materializing nine full country pipelines (see WIKIDATA_REGISTRY_SEED_SPECS /
    # tests/test_wikidata_assets.py wiring test).
    deps=[
        dg.AssetKey(spine_asset_key)
        for spine_asset_key in WIKIDATA_REGISTRY_SEED_SPINE_ASSET_KEYS
    ],
    group_name=GROUP_NAME,
    kinds={"python", "wikidata", "s3"},
    description=(
        "Pulls paged raw Wikidata company SPARQL responses into object storage: "
        "exchange-listed companies, plus every Wikidata item carrying a national "
        "registry-number property (pulled as pseudo-exchanges)."
    ),
)
def wikidata_company_seed_raw_objects(
    context: dg.AssetExecutionContext,
    config: WikidataRawPullConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    client = WikidataSparqlClient(timeout_seconds=config.request_timeout_seconds)
    return pull_wikidata_company_seed_raw_objects(
        client=client,
        object_store=object_store,
        config=config,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC).isoformat(),
        sleep=time.sleep,
        log=context.log.info,
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
    source_run_id = config.raw_run_id or context.run_id
    context.log.info(
        "Starting Wikidata DuckDB table: table=%s source_run_id=%s database=%s",
        table_name,
        source_run_id,
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
                    raw_run_id=config.raw_run_id,
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
                    raw_run_id=config.raw_run_id,
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
) -> int:
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
    placeholders = ", ".join("?" for _column_name in column_names)
    insert_sql = f"insert into {qualified_table} values ({placeholders})"
    batch: list[tuple[Any, ...]] = []
    row_count = 0
    for row in rows:
        batch.append(tuple(row.get(column_name) for column_name in column_names))
        if len(batch) < 1_000:
            continue
        connection.executemany(insert_sql, batch)
        row_count += len(batch)
        batch.clear()
        log(
            "Loaded Wikidata DuckDB stage rows: table=%s rows=%s", table_name, row_count
        )
    if batch:
        connection.executemany(insert_sql, batch)
        row_count += len(batch)
    log(
        "Completed Wikidata DuckDB stage table: table=%s rows=%s", table_name, row_count
    )
    return row_count


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
    deps=["wikidata_company_seed_raw_objects"],
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
    deps=["wikidata_company_seed_raw_objects"],
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
    deps=["wikidata_company_seed_raw_objects"],
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
    deps=["wikidata_company_seed_raw_objects"],
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
    deps=["wikidata_company_seed_raw_objects"],
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
    deps=["wikidata_company_seed_raw_objects"],
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
    deps=["wikidata_company_seed_raw_objects"],
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
    deps=["wikidata_company_seed_raw_objects"],
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
    deps=["wikidata_company_seed_raw_objects"],
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
def wikidata_company_seed_complete(
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
            result = client.query(
                f"select source_run_id, count() from {RESOLVED_DATABASE}.{table_name} "
                "group by source_run_id order by source_run_id limit 2"
            )
            rows = result.result_rows
            row_counts[table_name] = sum(int(row[1]) for row in rows)
            if not rows and table_name in WIKIDATA_EMPTY_ALLOWED_TABLES:
                continue
            if len(rows) != 1:
                raise ValueError(
                    f"ClickHouse table {table_name} does not contain exactly one source_run_id"
                )
            source_run_ids.add(str(rows[0][0]))

        missing_exchange_ids = client.query(
            f"select distinct listings.exchange_wikidata_id "
            f"from {RESOLVED_DATABASE}.{tables.WIKIDATA_COMPANY_LISTINGS_TABLE} as listings "
            f"left join {RESOLVED_DATABASE}.{tables.WIKIDATA_EXCHANGES_TABLE} as exchanges "
            "on exchanges.exchange_wikidata_id = listings.exchange_wikidata_id "
            "where exchanges.exchange_wikidata_id is null "
            "order by listings.exchange_wikidata_id limit 10"
        ).result_rows
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


def pull_wikidata_company_seed_raw_objects(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    config: WikidataRawPullConfig,
    run_id: str,
    retrieved_at: str,
    sleep: Callable[[float], None],
    log: Callable[..., object],
) -> dg.MaterializeResult:
    log(
        "Starting Wikidata raw download: run_id=%s page_size=%s max_pages=%s "
        "include_registry_seed=%s",
        run_id,
        config.page_size,
        config.max_pages,
        config.include_registry_seed,
    )
    object_store.ensure_bucket(WIKIDATA_RAW_BUCKET)
    retrieved_date = retrieved_at[:10]
    log("Discovering Wikidata exchanges and registry seed properties")
    exchange_rows, active_exchanges_key = wikidata_raw_pull_exchange_rows(
        client=client,
        object_store=object_store,
        config=config,
        run_id=run_id,
        retrieved_date=retrieved_date,
        retrieved_at=retrieved_at,
    )
    registry_rows = wikidata_raw_pull_registry_rows(
        config=config,
        source_run_id=run_id,
        retrieved_at=retrieved_at,
    )
    # Registry properties are pulled as pseudo-exchanges AFTER the real exchanges, in
    # the same loop, so raw-object layout/manifests/page numbering/augmentation
    # batching/provenance keys work unchanged (see build_registry_number_company_query).
    seed_units = [_seed_unit_from_exchange_row(row) for row in exchange_rows] + [
        _seed_unit_from_registry_row(row) for row in registry_rows
    ]
    log(
        "Wikidata seed discovery completed: exchanges=%s registry_properties=%s "
        "download_units=%s",
        len(exchange_rows),
        len(registry_rows),
        len(seed_units),
    )

    total_row_count = 0
    total_page_count = 0
    total_augmentation_row_count = 0
    total_augmentation_object_count = 0
    manifest_keys: list[str] = []

    for unit_index, seed_unit in enumerate(seed_units, start=1):
        exchange_id = seed_unit["exchange_wikidata_id"]
        unit_row_count = 0
        unit_augmentation_row_count = 0
        page_count = 0
        object_keys: list[str] = []
        augmentation_object_keys: list[str] = []
        first_query = _seed_unit_query(seed_unit, limit=config.page_size, offset=0)
        current_query_hash = query_hash(first_query)
        log(
            "Starting Wikidata download unit: unit=%s/%s mode=%s exchange_id=%s "
            "exchange_name=%s expected_listed_companies=%s",
            unit_index,
            len(seed_units),
            seed_unit["query_mode"],
            exchange_id,
            seed_unit["exchange_name"],
            seed_unit["listed_company_count_on_exchange"],
        )

        while config.max_pages is None or page_count < config.max_pages:
            offset = page_count * config.page_size
            query = _seed_unit_query(seed_unit, limit=config.page_size, offset=offset)
            payload = client.fetch(query, user_agent=config.user_agent)
            bindings = response_bindings(payload)
            if not bindings:
                break

            page_count += 1
            unit_row_count += len(bindings)
            object_key = page_object_key(
                retrieved_date=retrieved_date,
                run_id=run_id,
                exchange_id=exchange_id,
                page_number=page_count,
            )
            object_store.write_json(
                object_key,
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                bucket=WIKIDATA_RAW_BUCKET,
            )
            object_keys.append(object_key)
            page_augmentation_object_keys, page_augmentation_row_count = (
                pull_wikidata_company_augmentation_raw_objects_for_page(
                    client=client,
                    object_store=object_store,
                    config=config,
                    run_id=run_id,
                    retrieved_date=retrieved_date,
                    exchange_id=exchange_id,
                    page_number=page_count,
                    listed_company_bindings=bindings,
                    sleep=sleep,
                    log=log,
                )
            )
            augmentation_object_keys.extend(page_augmentation_object_keys)
            unit_augmentation_row_count += page_augmentation_row_count
            log(
                "Downloaded Wikidata company page: unit=%s/%s exchange_id=%s "
                "page=%s page_rows=%s unit_rows=%s augmentation_rows=%s "
                "object_key=%s",
                unit_index,
                len(seed_units),
                exchange_id,
                page_count,
                len(bindings),
                unit_row_count,
                unit_augmentation_row_count,
                object_key,
            )

            if len(bindings) < config.page_size:
                break
            if config.request_delay_seconds > 0:
                sleep(config.request_delay_seconds)

        manifest_key = manifest_object_key(
            retrieved_date=retrieved_date,
            run_id=run_id,
            exchange_id=exchange_id,
        )
        manifest = {
            "source": "wikidata",
            "query_mode": seed_unit["query_mode"],
            "run_id": run_id,
            "retrieved_date": retrieved_date,
            "exchange_id": exchange_id,
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
            "query_hash": current_query_hash,
            "row_count": unit_row_count,
            "augmentation_row_count": unit_augmentation_row_count,
            "page_count": page_count,
            "started_at": retrieved_at,
            "completed_at": datetime.now(UTC).isoformat(),
            "objects": object_keys,
            "augmentation_objects": augmentation_object_keys,
        }
        object_store.write_json(
            manifest_key,
            json.dumps(manifest, sort_keys=True, separators=(",", ":")),
            bucket=WIKIDATA_RAW_BUCKET,
        )
        manifest_keys.append(manifest_key)
        total_row_count += unit_row_count
        total_page_count += page_count
        total_augmentation_row_count += unit_augmentation_row_count
        total_augmentation_object_count += len(augmentation_object_keys)
        log(
            "Completed Wikidata download unit: unit=%s/%s exchange_id=%s "
            "pages=%s company_rows=%s augmentation_objects=%s "
            "augmentation_rows=%s manifest_key=%s",
            unit_index,
            len(seed_units),
            exchange_id,
            page_count,
            unit_row_count,
            len(augmentation_object_keys),
            unit_augmentation_row_count,
            manifest_key,
        )

        if config.request_delay_seconds > 0 and unit_index < len(seed_units):
            sleep(config.request_delay_seconds)

    snapshot_manifest_key = snapshot_manifest_object_key(run_id=run_id)
    completed_at = datetime.now(UTC).isoformat()
    object_store.write_json(
        snapshot_manifest_key,
        json.dumps(
            {
                "source": "wikidata",
                "run_id": run_id,
                "status": "complete",
                "retrieved_date": retrieved_date,
                "started_at": retrieved_at,
                "completed_at": completed_at,
                "exchange_count": len(exchange_rows),
                "registry_property_count": len(registry_rows),
                "page_count": total_page_count,
                "row_count": total_row_count,
                "augmentation_object_count": total_augmentation_object_count,
                "augmentation_row_count": total_augmentation_row_count,
                "manifest_keys": manifest_keys,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        bucket=WIKIDATA_RAW_BUCKET,
    )
    deleted_old_raw_object_count = delete_old_wikidata_raw_snapshot_objects(
        object_store=object_store,
        run_id=run_id,
    )
    log(
        "Completed Wikidata raw download: run_id=%s units=%s pages=%s "
        "company_rows=%s augmentation_objects=%s augmentation_rows=%s "
        "manifests=%s deleted_old_objects=%s snapshot_manifest_key=%s",
        run_id,
        len(seed_units),
        total_page_count,
        total_row_count,
        total_augmentation_object_count,
        total_augmentation_row_count,
        len(manifest_keys),
        deleted_old_raw_object_count,
        snapshot_manifest_key,
    )
    return dg.MaterializeResult(
        metadata={
            "bucket": WIKIDATA_RAW_BUCKET,
            "exchange_count": len(exchange_rows),
            "registry_property_count": len(registry_rows),
            "page_count": total_page_count,
            "row_count": total_row_count,
            "augmentation_object_count": total_augmentation_object_count,
            "augmentation_row_count": total_augmentation_row_count,
            "manifest_count": len(manifest_keys),
            "manifest_keys": manifest_keys,
            "snapshot_manifest_key": snapshot_manifest_key,
            "active_exchanges_key": active_exchanges_key or "",
            "deleted_old_raw_object_count": deleted_old_raw_object_count,
            "retrieved_at": retrieved_at,
        }
    )


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


def pull_wikidata_company_augmentation_raw_objects_for_page(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    config: WikidataRawPullConfig,
    run_id: str,
    retrieved_date: str,
    exchange_id: str,
    page_number: int,
    listed_company_bindings: list[dict[str, Any]],
    sleep: Callable[[float], None],
    log: Callable[..., object],
) -> tuple[list[str], int]:
    company_ids = sorted(
        {
            wikidata_id_from_url(binding_value(binding, "company"))
            for binding in listed_company_bindings
            if wikidata_id_from_url(binding_value(binding, "company"))
        }
    )
    object_keys: list[str] = []
    row_count = 0
    batches = list(_batched(company_ids, config.augmentation_batch_size))
    for batch_number, company_id_batch in enumerate(batches, start=1):
        batch_row_count = 0
        query_builders: tuple[tuple[str, Callable[[tuple[str, ...]], str]], ...] = (
            ("profile", build_company_profile_augmentation_query),
            ("identifiers", build_company_identifier_augmentation_query),
            ("relationships", build_company_relationship_augmentation_query),
            ("people", build_company_people_augmentation_query),
        )
        for query_index, (augmentation_kind, query_builder) in enumerate(
            query_builders,
            start=1,
        ):
            query = query_builder(tuple(company_id_batch))
            payload = client.fetch(query, user_agent=config.user_agent)
            bindings = response_bindings(payload)
            object_key = augmentation_object_key(
                retrieved_date=retrieved_date,
                run_id=run_id,
                exchange_id=exchange_id,
                augmentation_kind=augmentation_kind,
                page_number=page_number,
                batch_number=batch_number,
            )
            object_store.write_json(
                object_key,
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                bucket=WIKIDATA_RAW_BUCKET,
            )
            object_keys.append(object_key)
            row_count += len(bindings)
            batch_row_count += len(bindings)

            is_last_query = batch_number == len(batches) and query_index == len(
                query_builders
            )
            if config.request_delay_seconds > 0 and not is_last_query:
                sleep(config.request_delay_seconds)

        log(
            "Downloaded Wikidata augmentation batch: exchange_id=%s page=%s "
            "batch=%s/%s companies=%s response_objects=%s rows=%s",
            exchange_id,
            page_number,
            batch_number,
            len(batches),
            len(company_id_batch),
            len(query_builders),
            batch_row_count,
        )

    return object_keys, row_count


def _batched[T](items: list[T], batch_size: int) -> Iterator[list[T]]:
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


def wikidata_raw_pull_exchange_rows(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    config: WikidataRawPullConfig,
    run_id: str,
    retrieved_date: str,
    retrieved_at: str,
) -> tuple[list[dict[str, Any]], str | None]:
    configured_exchange_ids = config.configured_exchange_ids()
    if configured_exchange_ids is not None:
        return (
            wikidata_raw_pull_configured_exchange_rows(
                configured_exchange_ids,
                source_run_id=run_id,
                retrieved_at=retrieved_at,
                max_exchanges=config.max_exchanges,
            ),
            None,
        )

    query = build_active_listed_exchanges_query()
    payload = client.fetch(query, user_agent=config.user_agent)
    exchange_list_key = active_exchanges_object_key(
        retrieved_date=retrieved_date,
        run_id=run_id,
    )
    object_store.write_json(
        exchange_list_key,
        json.dumps(payload, sort_keys=True, separators=(",", ":")),
        bucket=WIKIDATA_RAW_BUCKET,
    )
    binding_rows = [
        active_listed_exchange_row_from_binding(
            binding,
            source_run_id=run_id,
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


def delete_old_wikidata_raw_snapshot_objects(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
) -> int:
    raw_prefix = "raw/"
    current_run_prefix = f"{raw_prefix}run_id={run_id}/"
    stale_keys = [
        key
        for key in object_store.list_keys(raw_prefix, bucket=WIKIDATA_RAW_BUCKET)
        if not key.startswith(current_run_prefix)
    ]
    return object_store.delete_keys(stale_keys, bucket=WIKIDATA_RAW_BUCKET)


# wikidata_company_seed_raw_objects declares ordering-only `deps` on every registry
# seed spec's spine asset (see the asset's docstring) purely so the UI shows the
# wikidata seed connected to each country's pipeline. `.upstream()` from the ClickHouse
# export walks that edge too, so without this exclusion the weekly job's selection
# would balloon into materializing nine full country pipelines (Sweden bulk download,
# Norway entities, Denmark CVR, ...) every week. Subtract each spine's own `.upstream()`
# (which includes the spine itself) to drop exactly the country pipelines and nothing
# from the native wikidata chain — no wikidata_* asset is upstream of a country spine.
_wikidata_registry_seed_spine_exclusion = dg.AssetSelection.assets(
    *(
        dg.AssetKey(spine_asset_key)
        for spine_asset_key in WIKIDATA_REGISTRY_SEED_SPINE_ASSET_KEYS
    )
).upstream()

# The canonical-contacts derivation runs only after the completion asset proves that
# all nine ClickHouse tables belong to one source snapshot.
wikidata_company_seed_selection = (
    dg.AssetSelection.assets("wikidata_company_seed_complete").upstream()
    | dg.AssetSelection.assets("wikidata_clickhouse_canonical_contacts")
) - _wikidata_registry_seed_spine_exclusion

wikidata_company_seed_weekly_job = dg.define_asset_job(
    "wikidata_company_seed_weekly_job",
    tags=HEAVY_BULK_RUN_TAGS,
    selection=wikidata_company_seed_selection,
)


@dg.schedule(
    name="wikidata_company_seed_weekly_schedule",
    cron_schedule="30 3 * * 1",
    execution_timezone="Europe/Belgrade",
    job=wikidata_company_seed_weekly_job,
)
def wikidata_company_seed_weekly_schedule() -> dg.RunRequest:
    return dg.RunRequest()


defs = dg.Definitions(
    assets=[
        wikidata_company_seed_raw_objects,
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
        wikidata_company_seed_complete,
    ],
    jobs=[wikidata_company_seed_weekly_job],
    schedules=[wikidata_company_seed_weekly_schedule],
)
