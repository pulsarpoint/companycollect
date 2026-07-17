"""Publish the complete Finland XBRL model and derive metrics in ClickHouse."""

from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import uuid

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
import duckdb

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.assets.common import FINLAND_XBRL_DUCKDB_POOL
from dagster_v3.defs.finland_xbrl.assets.data_daily_xml_duckdb import data_daily_xml_duckdb
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml_duckdb import (
    data_snapshot_xml_duckdb,
    list_xml_parse_duckdb_paths,
)
from dagster_v3.defs.finland_xbrl.assets.financial_metrics import (
    build_finland_financial_metrics_insert_sql,
)
from dagster_v3.defs.finland_xbrl.clickhouse import (
    CLICKHOUSE_DATABASE,
    FINANCIAL_METRICS_CLICKHOUSE_TABLE,
    FINANCIAL_STATEMENTS_CLICKHOUSE_COLUMNS,
    FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE,
    XBRL_CONTEXTS_CLICKHOUSE_COLUMNS,
    XBRL_CONTEXTS_CLICKHOUSE_TABLE,
    XBRL_FACTS_CLICKHOUSE_COLUMNS,
    XBRL_FACTS_CLICKHOUSE_TABLE,
    XBRL_TAXONOMY_CLICKHOUSE_COLUMNS,
    XBRL_TAXONOMY_CLICKHOUSE_TABLE,
    XBRL_UNITS_CLICKHOUSE_COLUMNS,
    XBRL_UNITS_CLICKHOUSE_TABLE,
    clickhouse_context_row,
    clickhouse_fact_row,
    clickhouse_financial_statement_row,
    clickhouse_unit_row,
)
from dagster_v3.defs.finland_xbrl.taxonomy import read_taxonomy_catalog

INSERT_BATCH_SIZE = 5_000

PARSED_TABLES = (
    (
        "statement_documents",
        FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE,
        tables.STATEMENT_DOCUMENTS_COLUMNS,
        FINANCIAL_STATEMENTS_CLICKHOUSE_COLUMNS,
        clickhouse_financial_statement_row,
    ),
    (
        "contexts",
        XBRL_CONTEXTS_CLICKHOUSE_TABLE,
        tables.CONTEXTS_COLUMNS,
        XBRL_CONTEXTS_CLICKHOUSE_COLUMNS,
        clickhouse_context_row,
    ),
    (
        "units",
        XBRL_UNITS_CLICKHOUSE_TABLE,
        tables.UNITS_COLUMNS,
        XBRL_UNITS_CLICKHOUSE_COLUMNS,
        clickhouse_unit_row,
    ),
    (
        "facts",
        XBRL_FACTS_CLICKHOUSE_TABLE,
        tables.FACTS_COLUMNS,
        XBRL_FACTS_CLICKHOUSE_COLUMNS,
        clickhouse_fact_row,
    ),
)


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            "fi_financial_statements_ch",
            deps=[data_snapshot_xml_duckdb, data_daily_xml_duckdb],
        ),
        dg.AssetSpec(
            "fi_xbrl_contexts_ch",
            deps=[data_snapshot_xml_duckdb, data_daily_xml_duckdb],
        ),
        dg.AssetSpec(
            "fi_xbrl_units_ch",
            deps=[data_snapshot_xml_duckdb, data_daily_xml_duckdb],
        ),
        dg.AssetSpec(
            "fi_xbrl_facts_ch",
            deps=[data_snapshot_xml_duckdb, data_daily_xml_duckdb],
        ),
    ],
    group_name="finland_xbrl",
    pool=FINLAND_XBRL_DUCKDB_POOL,
    can_subset=False,
    description=(
        "Streams all parsed Finland XBRL statements, contexts, units, and facts "
        "from partition DuckDB files into validated ClickHouse staging tables."
    ),
)
def fi_xbrl_parsed_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> Iterator[dg.MaterializeResult]:
    paths = list_xml_parse_duckdb_paths()
    if not paths:
        raise FileNotFoundError("No Finland XBRL parsed DuckDB partitions were found")
    counts = publish_parsed_xbrl(clickhouse=clickhouse, paths=paths, log=context.log.info)
    metadata_by_asset = {
        "fi_financial_statements_ch": (
            FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE,
            counts[FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE],
        ),
        "fi_xbrl_contexts_ch": (
            XBRL_CONTEXTS_CLICKHOUSE_TABLE,
            counts[XBRL_CONTEXTS_CLICKHOUSE_TABLE],
        ),
        "fi_xbrl_units_ch": (
            XBRL_UNITS_CLICKHOUSE_TABLE,
            counts[XBRL_UNITS_CLICKHOUSE_TABLE],
        ),
        "fi_xbrl_facts_ch": (
            XBRL_FACTS_CLICKHOUSE_TABLE,
            counts[XBRL_FACTS_CLICKHOUSE_TABLE],
        ),
    }
    for asset_key, (table, row_count) in metadata_by_asset.items():
        yield dg.MaterializeResult(
            asset_key=asset_key,
            metadata={
                "row_count": row_count,
                "parsed_duckdb_count": len(paths),
                "clickhouse_table": f"{CLICKHOUSE_DATABASE}.{table}",
            },
        )


@dg.asset(
    name="fi_xbrl_taxonomy_codes_ch",
    group_name="finland_xbrl",
    kinds={"python", "xbrl", "clickhouse"},
    description="Publishes the bundled official Finnish SBR taxonomy label catalog.",
)
def fi_xbrl_taxonomy_codes_ch(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    loaded_at = datetime.now(UTC)
    rows = [
        {
            "taxonomy_version": item.taxonomy_version,
            "code": item.code,
            "code_kind": item.code_kind,
            "namespace_hint": item.namespace_hint or None,
            "label_fi": item.label_fi,
            "label_en": item.label_en or None,
            "label_sv": item.label_sv or None,
            "metric_name_hint": item.metric_name_hint or None,
            "source_artifact": item.source_artifact,
            "source_url": item.source_url,
            "loaded_at": loaded_at,
        }
        for item in read_taxonomy_catalog()
    ]
    with clickhouse.get_connection() as client:
        _replace_table_with_rows(
            client=client,
            target_table=XBRL_TAXONOMY_CLICKHOUSE_TABLE,
            columns=XBRL_TAXONOMY_CLICKHOUSE_COLUMNS,
            rows=rows,
        )
    context.log.info("Published Finland taxonomy codes: rows=%d", len(rows))
    return dg.MaterializeResult(
        metadata={
            "row_count": len(rows),
            "clickhouse_table": (
                f"{CLICKHOUSE_DATABASE}.{XBRL_TAXONOMY_CLICKHOUSE_TABLE}"
            ),
        }
    )


@dg.asset(
    name="fi_financial_metrics_ch",
    deps=[
        dg.AssetKey("fi_financial_statements_ch"),
        dg.AssetKey("fi_xbrl_contexts_ch"),
        dg.AssetKey("fi_xbrl_facts_ch"),
        dg.AssetKey("exchange_rates_v2_clickhouse"),
    ],
    group_name="finland_xbrl",
    kinds={"sql", "xbrl", "fx", "clickhouse"},
    description=(
        "Builds deterministic current-period Finland metrics directly from raw "
        "ClickHouse facts and converts EUR amounts to USD in the same statement."
    ),
)
def fi_financial_metrics_ch(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    row_count = rebuild_financial_metrics(clickhouse=clickhouse)
    context.log.info("Published Finland financial metrics: rows=%d", row_count)
    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "clickhouse_table": (
                f"{CLICKHOUSE_DATABASE}.{FINANCIAL_METRICS_CLICKHOUSE_TABLE}"
            ),
        }
    )


def publish_parsed_xbrl(
    *,
    clickhouse: Any,
    paths: list[Path],
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    target_tables = tuple(spec[1] for spec in PARSED_TABLES)
    assert_clickhouse_tables_exist(
        clickhouse,
        database=CLICKHOUSE_DATABASE,
        tables=target_tables,
    )
    stage_tables = {table: _stage_table_name(table) for table in target_tables}
    counts = dict.fromkeys(target_tables, 0)
    with clickhouse.get_connection() as client:
        try:
            for target, stage in stage_tables.items():
                client.execute(
                    f"CREATE TABLE {CLICKHOUSE_DATABASE}.{stage} "
                    f"AS {CLICKHOUSE_DATABASE}.{target}"
                )
            for index, path in enumerate(paths, start=1):
                _assert_parsed_duckdb_contract(path)
                for source, target, source_columns, target_columns, converter in PARSED_TABLES:
                    inserted = _stream_duckdb_table(
                        path=path,
                        source_table=source,
                        source_columns=source_columns,
                        client=client,
                        target_table=stage_tables[target],
                        target_columns=target_columns,
                        converter=converter,
                    )
                    counts[target] += inserted
                if log is not None and (index == 1 or index == len(paths) or index % 10 == 0):
                    log("Finland XBRL publish progress: partition=%d/%d path=%s", index, len(paths), path)
            _validate_parsed_stages(client=client, stages=stage_tables, counts=counts)
            for target, stage in stage_tables.items():
                client.execute(
                    f"EXCHANGE TABLES {CLICKHOUSE_DATABASE}.{stage} "
                    f"AND {CLICKHOUSE_DATABASE}.{target}"
                )
        finally:
            for stage in stage_tables.values():
                client.execute(f"DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.{stage}")
    return counts


def rebuild_financial_metrics(*, clickhouse: Any) -> int:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=CLICKHOUSE_DATABASE,
        tables=(
            FINANCIAL_METRICS_CLICKHOUSE_TABLE,
            FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE,
            XBRL_CONTEXTS_CLICKHOUSE_TABLE,
            XBRL_FACTS_CLICKHOUSE_TABLE,
            "exchange_rates",
        ),
    )
    stage = _stage_table_name(FINANCIAL_METRICS_CLICKHOUSE_TABLE)
    with clickhouse.get_connection() as client:
        try:
            client.execute(
                f"CREATE TABLE {CLICKHOUSE_DATABASE}.{stage} "
                f"AS {CLICKHOUSE_DATABASE}.{FINANCIAL_METRICS_CLICKHOUSE_TABLE}"
            )
            client.execute(
                build_finland_financial_metrics_insert_sql(
                    f"{CLICKHOUSE_DATABASE}.{stage}"
                )
            )
            row_count = _scalar(
                client,
                f"SELECT count() FROM {CLICKHOUSE_DATABASE}.{stage}",
            )
            statement_count = _scalar(
                client,
                f"SELECT count() FROM {CLICKHOUSE_DATABASE}.{FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE} FINAL",
            )
            if row_count != statement_count:
                raise ValueError(
                    "Finland metrics coverage mismatch: "
                    f"metrics={row_count} statements={statement_count}"
                )
            missing_fx = _scalar(
                client,
                f"SELECT countIf(fx_rate_to_usd IS NULL) FROM {CLICKHOUSE_DATABASE}.{stage}",
            )
            if missing_fx:
                raise ValueError(f"Finland metrics contain {missing_fx} rows without EUR/USD rates")
            missing_source = _scalar(
                client,
                "SELECT countIf(empty(ifNull(xml_source_uri, '')) OR "
                "empty(ifNull(source_url, ''))) "
                f"FROM {CLICKHOUSE_DATABASE}.{stage}",
            )
            if missing_source:
                raise ValueError(
                    f"Finland metrics contain {missing_source} rows without source links"
                )
            mapped = _scalar(
                client,
                f"SELECT countIf(mapped_fact_count > 0) FROM {CLICKHOUSE_DATABASE}.{stage}",
            )
            if mapped == 0:
                raise ValueError("Finland metrics contain no mapped XBRL facts")
            client.execute(
                f"EXCHANGE TABLES {CLICKHOUSE_DATABASE}.{stage} "
                f"AND {CLICKHOUSE_DATABASE}.{FINANCIAL_METRICS_CLICKHOUSE_TABLE}"
            )
        finally:
            client.execute(f"DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.{stage}")
    return row_count


def _assert_parsed_duckdb_contract(path: Path) -> None:
    with duckdb.connect(str(path), read_only=True) as connection:
        available = {
            str(row[0])
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        expected = {spec[0] for spec in PARSED_TABLES}
        missing = sorted(expected - available)
        if missing:
            raise ValueError(
                f"Finland XBRL parsed DuckDB requires parser v2 reprocessing: "
                f"path={path} missing_tables={missing}"
            )
        for source, _target, columns, _target_columns, _converter in PARSED_TABLES:
            actual_columns = {
                str(row[0])
                for row in connection.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'main' AND table_name = ?",
                    [source],
                ).fetchall()
            }
            missing_columns = sorted(set(columns) - actual_columns)
            if missing_columns:
                raise ValueError(
                    f"Finland XBRL parsed DuckDB has an old schema: "
                    f"path={path} table={source} missing_columns={missing_columns}"
                )


def _stream_duckdb_table(
    *,
    path: Path,
    source_table: str,
    source_columns: list[str],
    client: Any,
    target_table: str,
    target_columns: tuple[str, ...],
    converter: Callable[[dict[str, Any]], dict[str, Any]],
) -> int:
    selected_columns = ", ".join(f'"{column}"' for column in source_columns)
    insert_columns = ", ".join(target_columns)
    insert_sql = (
        f"INSERT INTO {CLICKHOUSE_DATABASE}.{target_table} ({insert_columns}) VALUES"
    )
    inserted = 0
    with duckdb.connect(str(path), read_only=True) as connection:
        cursor = connection.execute(
            f"SELECT {selected_columns} FROM {source_table}"
        )
        while batch := cursor.fetchmany(INSERT_BATCH_SIZE):
            output_rows = []
            for values in batch:
                source_row = dict(zip(source_columns, values, strict=True))
                converted = converter(source_row)
                output_rows.append(tuple(converted[column] for column in target_columns))
            client.execute(insert_sql, output_rows)
            inserted += len(output_rows)
    return inserted


def _validate_parsed_stages(
    *,
    client: Any,
    stages: dict[str, str],
    counts: dict[str, int],
) -> None:
    for target in (
        FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE,
        XBRL_CONTEXTS_CLICKHOUSE_TABLE,
        XBRL_FACTS_CLICKHOUSE_TABLE,
    ):
        if counts[target] == 0:
            raise ValueError(f"Refusing to publish empty Finland XBRL table: {target}")
    reports = stages[FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE]
    contexts = stages[XBRL_CONTEXTS_CLICKHOUSE_TABLE]
    units = stages[XBRL_UNITS_CLICKHOUSE_TABLE]
    facts = stages[XBRL_FACTS_CLICKHOUSE_TABLE]
    declared_facts = _scalar(
        client,
        f"SELECT sum(facts_count) FROM {CLICKHOUSE_DATABASE}.{reports}",
    )
    if declared_facts != counts[XBRL_FACTS_CLICKHOUSE_TABLE]:
        raise ValueError(
            "Finland XBRL fact count mismatch: "
            f"declared={declared_facts} actual={counts[XBRL_FACTS_CLICKHOUSE_TABLE]}"
        )
    orphan_contexts = _scalar(
        client,
        f"SELECT count() FROM {CLICKHOUSE_DATABASE}.{contexts} AS contexts "
        f"LEFT ANTI JOIN {CLICKHOUSE_DATABASE}.{reports} AS reports "
        "ON reports.statement_key = contexts.statement_key",
    )
    orphan_facts = _scalar(
        client,
        f"SELECT count() FROM {CLICKHOUSE_DATABASE}.{facts} AS facts "
        f"LEFT ANTI JOIN {CLICKHOUSE_DATABASE}.{contexts} AS contexts "
        "ON contexts.statement_key = facts.statement_key "
        "AND contexts.context_id = facts.context_id",
    )
    orphan_units = _scalar(
        client,
        f"SELECT count() FROM {CLICKHOUSE_DATABASE}.{units} AS units "
        f"LEFT ANTI JOIN {CLICKHOUSE_DATABASE}.{reports} AS reports "
        "ON reports.statement_key = units.statement_key",
    )
    if orphan_contexts or orphan_facts or orphan_units:
        raise ValueError(
            "Finland XBRL parent integrity failed: "
            f"contexts={orphan_contexts} facts={orphan_facts} units={orphan_units}"
        )
    listing_count = _scalar(
        client,
        "SELECT count() FROM "
        f"{CLICKHOUSE_DATABASE}.fi_xbrl_financial_statement_listings FINAL",
    )
    if listing_count != counts[FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE]:
        raise ValueError(
            "Finland XBRL parsed coverage does not match downloaded listings: "
            f"parsed={counts[FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE]} "
            f"listings={listing_count}"
        )


def _replace_table_with_rows(
    *,
    client: Any,
    target_table: str,
    columns: tuple[str, ...],
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        raise ValueError(f"Refusing to publish empty table: {target_table}")
    stage = _stage_table_name(target_table)
    try:
        client.execute(
            f"CREATE TABLE {CLICKHOUSE_DATABASE}.{stage} "
            f"AS {CLICKHOUSE_DATABASE}.{target_table}"
        )
        insert_columns = ", ".join(columns)
        client.execute(
            f"INSERT INTO {CLICKHOUSE_DATABASE}.{stage} ({insert_columns}) VALUES",
            [tuple(row[column] for column in columns) for row in rows],
        )
        client.execute(
            f"EXCHANGE TABLES {CLICKHOUSE_DATABASE}.{stage} "
            f"AND {CLICKHOUSE_DATABASE}.{target_table}"
        )
    finally:
        client.execute(f"DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.{stage}")


def _stage_table_name(table: str) -> str:
    return f"{table}__stage_{uuid.uuid4().hex}"


def _scalar(client: Any, sql: str) -> int:
    rows = client.execute(sql)
    value = rows[0][0]
    return int(value or 0)
