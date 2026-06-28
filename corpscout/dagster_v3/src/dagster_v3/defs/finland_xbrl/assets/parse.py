import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Callable

import dagster as dg
import dlt
import polars as pl
from dagster_duckdb import DuckDBResource
from dlt.extract.resource import DltResource
from pydantic import ConfigDict, field_validator

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_database_path,
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.arelle_parser import (
    ArelleStatementParser,
    parse_statement_xml_with_arelle,
)
from dagster_v3.defs.finland_xbrl.assets.common import (
    BACKFILL_PARTITIONS,
    DAILY_PARTITIONS,
    FINLAND_XBRL_DUCKDB_POOL,
    XBRL_BUCKET,
    XBRL_DLT_DATASET_NAME,
    _duckdb_table_exists,
)
from dagster_v3.defs.finland_xbrl.assets.raw_xml_documents import (
    finland_xbrl_raw_xml_documents_backfill,
    finland_xbrl_raw_xml_documents_incremental,
    load_xml_document_catalog_frame,
    load_xbrl_document_manifest,
    resolve_xbrl_documents_key,
)

class XbrlParsedConfig(dg.Config):
    model_config = ConfigDict(extra="forbid")

    documents_key: str | None = None

    @field_validator("documents_key", mode="before")
    @classmethod
    def normalize_object_key(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("object keys must be strings")
        stripped = value.strip()
        return stripped or None


@dataclass
class XbrlParseRunResult:
    load_info: Any
    parsed: int
    failed: int


def run_finland_xbrl_arelle_dlt_pipeline(
    *,
    database_path: str | Path,
    object_store: ObjectStoreResource,
    documents: list[dict[str, Any]],
    run_id: str,
    parser: ArelleStatementParser = parse_statement_xml_with_arelle,
    log_info: Callable[[str], None] | None = None,
    progress_interval: int = 25,
) -> XbrlParseRunResult:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    _log_parse_progress(log_info, f"Parsing {len(documents)} XBRL XML documents")
    statement_rows, fact_rows, failed = parse_xbrl_documents(
        documents,
        object_store=object_store,
        run_id=run_id,
        parser=parser,
        log_info=log_info,
        progress_interval=progress_interval,
    )
    pipeline = dlt.pipeline(
        pipeline_name="finland_xbrl_arelle_parsed_tables",
        destination=dlt.destinations.duckdb(str(database_file)),
        dataset_name=XBRL_DLT_DATASET_NAME,
        dev_mode=False,
    )
    load_info = pipeline.run(
        finland_xbrl_arelle_source(
            statement_rows=statement_rows,
            fact_rows=fact_rows,
        )
    )
    with duckdb_resource(database_file).get_connection() as connection:
        _ensure_parsed_duckdb_tables(connection)
    if log_info is not None:
        log_info("dlt loaded parsed XBRL tables into DuckDB")
    return XbrlParseRunResult(
        load_info=load_info,
        parsed=len(statement_rows),
        failed=len(failed),
    )


def _parse_registration_date(raw: object) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def documents_in_registration_window(
    documents: list[dict[str, Any]],
    *,
    window_start: date,
    window_end: date,
) -> list[dict[str, Any]]:
    """Keep docs whose registration_date is in [window_start, window_end)."""
    selected: list[dict[str, Any]] = []
    for document in documents:
        registered = _parse_registration_date(document.get("registration_date"))
        if registered is not None and window_start <= registered < window_end:
            selected.append(document)
    return selected


def documents_missing_registration_date(
    documents: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Docs with no/unparseable registration_date — excluded from every month partition."""
    return [
        document
        for document in documents
        if _parse_registration_date(document.get("registration_date")) is None
    ]


def unparsed_documents(
    documents: list[dict[str, Any]],
    *,
    parsed_object_keys: set[str],
) -> list[dict[str, Any]]:
    """Drop docs whose S3 object key is already present in the parsed output."""
    return [
        document
        for document in documents
        if document["xml_object_key"] not in parsed_object_keys
    ]


def parse_xbrl_documents(
    documents: list[dict[str, Any]],
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    parser: ArelleStatementParser = parse_statement_xml_with_arelle,
    log_info: Callable[[str], None] | None = None,
    progress_interval: int = 25,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    parsed_at = datetime.now(UTC)
    total_documents = len(documents)
    statement_rows: list[dict[str, Any]] = []
    fact_rows: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    warning_count = 0
    started_at = datetime.now(UTC)
    for document_index, document in enumerate(documents, start=1):
        xml_object_key = document["xml_object_key"]
        try:
            body = object_store.read_bytes(xml_object_key, bucket=XBRL_BUCKET)
            parsed = parser(
                business_id=document["business_id"],
                financial_date=document["financial_date"],
                registration_date=document.get("registration_date"),
                source_url=document.get("source_url", ""),
                xml_object_key=xml_object_key,
                source_run_id=run_id,
                body=body,
                parsed_at=parsed_at,
            )
        except Exception as exc:  # noqa: BLE001 - skip and record one bad document
            failed.append(
                {
                    "xml_object_key": xml_object_key,
                    "business_id": document["business_id"],
                    "financial_date": document["financial_date"],
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            _log_parse_progress(
                log_info,
                f"Skipping unparseable XBRL document {xml_object_key}: "
                f"business_id={document['business_id']} "
                f"financial_date={document['financial_date']} "
                f"error={type(exc).__name__}: {exc}",
            )
            continue
        warning_count += len(parsed.warnings)
        statement_rows.append(_statement_document_row(parsed.statement_document))
        fact_rows.extend(_fact_row(fact) for fact in parsed.facts)
        if _should_log_parse_progress(
            document_index=document_index,
            total_documents=total_documents,
            progress_interval=progress_interval,
        ):
            elapsed_seconds = (datetime.now(UTC) - started_at).total_seconds()
            _log_parse_progress(
                log_info,
                "Parsed XBRL XML document "
                f"{document_index}/{total_documents}: "
                f"business_id={document['business_id']} "
                f"financial_date={document['financial_date']} "
                f"facts={len(parsed.facts)} "
                f"warnings={len(parsed.warnings)} "
                f"elapsed_seconds={elapsed_seconds:.1f}",
            )

    elapsed_seconds = (datetime.now(UTC) - started_at).total_seconds()
    _log_parse_progress(
        log_info,
        "Parsed XBRL XML documents complete: "
        f"documents={total_documents} "
        f"statement_rows={len(statement_rows)} "
        f"fact_rows={len(fact_rows)} "
        f"failed={len(failed)} "
        f"parser_warnings={warning_count} "
        f"elapsed_seconds={elapsed_seconds:.1f}",
    )
    return statement_rows, fact_rows, failed


@dlt.source(name="finland_xbrl_arelle")
def finland_xbrl_arelle_source(
    *,
    statement_rows: list[dict[str, Any]],
    fact_rows: list[dict[str, Any]],
) -> list[DltResource]:
    return _finland_xbrl_arelle_resources(
        statement_rows=statement_rows,
        fact_rows=fact_rows,
    )


def _finland_xbrl_arelle_resources(
    *,
    statement_rows: list[dict[str, Any]],
    fact_rows: list[dict[str, Any]],
) -> list[DltResource]:
    return [
        dlt.resource(
            statement_rows,
            name=tables.STATEMENT_DOCUMENTS_TABLE,
            write_disposition="append",
            primary_key="statement_key",
        ),
        dlt.resource(
            fact_rows,
            name=tables.FACTS_TABLE,
            write_disposition="append",
            primary_key=("statement_key", "fact_ordinal"),
        ),
    ]


def _should_log_parse_progress(
    *,
    document_index: int,
    total_documents: int,
    progress_interval: int,
) -> bool:
    if total_documents == 0:
        return False
    if document_index == 1 or document_index == total_documents:
        return True
    return progress_interval > 0 and document_index % progress_interval == 0


def _log_parse_progress(log_info: Callable[[str], None] | None, message: str) -> None:
    if log_info is not None:
        log_info(message)


def _materialize_parse_window(
    context: dg.AssetExecutionContext,
    config: XbrlParsedConfig,
    object_store: ObjectStoreResource,
    source_duckdb: DuckDBResource,
    *,
    window_start: date,
    window_end: date,
) -> dg.MaterializeResult:
    documents_key = resolve_xbrl_documents_key(config=config)

    documents, _meta = load_xbrl_document_manifest(
        object_store=object_store, documents_key=documents_key
    )
    # Docs without a parseable registration_date fall outside every month partition,
    # so they are never parsed. Surface that gap rather than dropping it silently.
    missing_registration = len(documents_missing_registration_date(documents))
    if missing_registration:
        context.log.warning(
            "XBRL parse partition %s: %d catalog docs have no parseable registration_date "
            "and are excluded from all month partitions",
            context.partition_key, missing_registration,
        )
    in_window = documents_in_registration_window(
        documents, window_start=window_start, window_end=window_end
    )
    to_parse = unparsed_documents(
        in_window, parsed_object_keys=load_parsed_object_keys(source_duckdb)
    )
    context.log.info(
        "XBRL parse partition %s: %d catalog docs, %d in window, %d to parse",
        context.partition_key, len(documents), len(in_window), len(to_parse),
    )

    parsed_this_run = 0
    failed_this_run = 0
    if to_parse:
        result = run_finland_xbrl_arelle_dlt_pipeline(
            database_path=duckdb_database_path(source_duckdb),
            object_store=object_store,
            documents=to_parse,
            run_id=context.run_id,
            log_info=context.log.info,
        )
        with source_duckdb.get_connection() as connection:
            _ensure_parsed_duckdb_tables(connection)
        parsed_this_run = result.parsed
        failed_this_run = result.failed
        if failed_this_run:
            # Failed docs are not written, so they are re-attempted on the next run
            # (and keep failing until the source document is fixed/removed).
            context.log.warning(
                "XBRL parse partition %s: %d documents failed to parse and were skipped "
                "(will retry next run)",
                context.partition_key, failed_this_run,
            )

    row_counts = parsed_duckdb_row_counts(source_duckdb)
    return dg.MaterializeResult(
        metadata={
            "partition": context.partition_key,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "documents_in_window": len(in_window),
            "documents_parsed_this_run": parsed_this_run,
            "documents_failed_this_run": failed_this_run,
            "documents_missing_registration_date": missing_registration,
            "statement_documents_total_row_count": row_counts[
                tables.STATEMENT_DOCUMENTS_TABLE
            ],
            "facts_total_row_count": row_counts[tables.FACTS_TABLE],
            "xml_documents_object_key": documents_key,
        }
    )


@dg.asset(
    name="finland_xbrl_parse_backfill",
    group_name="finland_xbrl",
    deps=[finland_xbrl_raw_xml_documents_backfill],
    partitions_def=BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"python", "dlt", "duckdb", "arelle"},
    pool=FINLAND_XBRL_DUCKDB_POOL,
)
def finland_xbrl_parse_backfill(
    context: dg.AssetExecutionContext,
    config: XbrlParsedConfig,
    object_store: ObjectStoreResource,
    source_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    window = context.partition_time_window
    return _materialize_parse_window(
        context,
        config,
        object_store,
        source_duckdb,
        window_start=window.start.date(),
        window_end=window.end.date(),
    )


@dg.asset(
    name="finland_xbrl_parse_incremental",
    group_name="finland_xbrl",
    deps=[finland_xbrl_raw_xml_documents_incremental],
    partitions_def=DAILY_PARTITIONS,
    kinds={"python", "dlt", "duckdb", "arelle"},
    pool=FINLAND_XBRL_DUCKDB_POOL,
)
def finland_xbrl_parse_incremental(
    context: dg.AssetExecutionContext,
    config: XbrlParsedConfig,
    object_store: ObjectStoreResource,
    source_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    window = context.partition_time_window
    return _materialize_parse_window(
        context,
        config,
        object_store,
        source_duckdb,
        window_start=window.start.date(),
        window_end=window.end.date(),
    )


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            table,
            deps=[finland_xbrl_parse_backfill, finland_xbrl_parse_incremental],
            group_name="finland_xbrl",
            kinds={"duckdb"},
            description=f"Catalog marker for parsed Finland PRH XBRL DuckDB table `{table}`.",
        )
        for table in (tables.STATEMENT_DOCUMENTS_TABLE, tables.FACTS_TABLE)
    ],
    pool=FINLAND_XBRL_DUCKDB_POOL,
)
def finland_xbrl_parsed_tables(
    source_duckdb: DuckDBResource,
) -> Iterator[dg.MaterializeResult]:
    row_counts = parsed_duckdb_row_counts(source_duckdb)
    for table in (tables.STATEMENT_DOCUMENTS_TABLE, tables.FACTS_TABLE):
        yield dg.MaterializeResult(
            asset_key=table,
            metadata={
                "duckdb_schema": XBRL_DLT_DATASET_NAME,
                "duckdb_table": table,
                "total_table_row_count": row_counts[table],
            },
        )


def load_parsed_table_frame(source_duckdb: DuckDBResource, *, table: str) -> pl.DataFrame:
    if table not in {tables.STATEMENT_DOCUMENTS_TABLE, tables.FACTS_TABLE}:
        raise ValueError(f"Unsupported parsed XBRL DuckDB table {table!r}")
    columns = _parsed_table_columns(table)
    with read_only_duckdb_connection(source_duckdb) as connection:
        if not _duckdb_table_exists(connection, table=table):
            raise ValueError(
                f"XBRL parsed DuckDB table {XBRL_DLT_DATASET_NAME}.{table} does not exist. "
                "Materialize fi_prh_xbrl_statement_documents and fi_prh_xbrl_facts_raw first."
            )
        arrow_table = connection.execute(
            f"select {', '.join(columns)} from {XBRL_DLT_DATASET_NAME}.{table}"
        ).to_arrow_table()
    return pl.from_arrow(arrow_table)


def parsed_duckdb_row_counts(source_duckdb: DuckDBResource) -> dict[str, int]:
    parsed_tables = (tables.STATEMENT_DOCUMENTS_TABLE, tables.FACTS_TABLE)
    if not duckdb_database_path(source_duckdb).exists():
        return {table: 0 for table in parsed_tables}
    with read_only_duckdb_connection(source_duckdb) as connection:
        return {
            table: (
                int(
                    connection.execute(
                        f"select count(*) from {XBRL_DLT_DATASET_NAME}.{table}"
                    ).fetchone()[0]
                )
                if _duckdb_table_exists(connection, table=table)
                else 0
            )
            for table in parsed_tables
        }


def load_parsed_object_keys(source_duckdb: DuckDBResource) -> set[str]:
    """Set of xml_object_keys already present in the parsed statement table."""
    if not duckdb_database_path(source_duckdb).exists():
        return set()
    with read_only_duckdb_connection(source_duckdb) as connection:
        if not _duckdb_table_exists(connection, table=tables.STATEMENT_DOCUMENTS_TABLE):
            return set()
        rows = connection.execute(
            f"select distinct xml_object_key "
            f"from {XBRL_DLT_DATASET_NAME}.{tables.STATEMENT_DOCUMENTS_TABLE}"
        ).fetchall()
    return {row[0] for row in rows}


def parsed_duckdb_observability_metadata(
    *,
    object_store: ObjectStoreResource,
    source_duckdb: DuckDBResource,
    documents_key: str,
) -> dict[str, Any]:
    xml_documents_frame = load_xml_document_catalog_frame(
        object_store,
        documents_key=documents_key,
    )
    statement_documents_frame = load_parsed_table_frame(
        source_duckdb,
        table=tables.STATEMENT_DOCUMENTS_TABLE,
    )
    facts_frame = load_parsed_table_frame(
        source_duckdb,
        table=tables.FACTS_TABLE,
    )
    quality = build_parse_quality_row(
        xml_documents_frame=xml_documents_frame,
        statement_documents_frame=statement_documents_frame,
        facts_frame=facts_frame,
        generated_at=datetime.now(UTC).isoformat(),
    )
    concept_rows = build_concept_profile_rows(facts_frame, example_limit=3)
    return {
        "xml_documents_count": quality["xml_documents_count"],
        "statement_documents_count": quality["statement_documents_count"],
        "facts_count": quality["facts_count"],
        "statements_with_zero_facts_count": quality["statements_with_zero_facts_count"],
        "duplicate_statement_keys_count": quality["duplicate_statement_keys_count"],
        "parser_warning_statements_count": quality["parser_warning_statements_count"],
        "missing_statement_business_ids_count": quality["missing_statement_business_ids_count"],
        "missing_statement_financial_dates_count": quality[
            "missing_statement_financial_dates_count"
        ],
        "missing_fact_business_ids_count": quality["missing_fact_business_ids_count"],
        "missing_fact_financial_dates_count": quality["missing_fact_financial_dates_count"],
        "facts_per_statement_min": quality["facts_per_statement_min"],
        "facts_per_statement_avg": quality["facts_per_statement_avg"],
        "facts_per_statement_max": quality["facts_per_statement_max"],
        "latest_parsed_at": quality["latest_parsed_at"],
        "top_concept_namespaces": json.loads(quality["top_concept_namespaces"]),
        "top_concepts": json.loads(quality["top_concepts"]),
        "concept_count": len(concept_rows),
    }


def _ensure_parsed_duckdb_tables(connection: Any) -> None:
    connection.execute(f"create schema if not exists {XBRL_DLT_DATASET_NAME}")
    for table, schema in (
        (tables.STATEMENT_DOCUMENTS_TABLE, tables.STATEMENT_DOCUMENTS_DUCKDB_SCHEMA),
        (tables.FACTS_TABLE, tables.FACTS_DUCKDB_SCHEMA),
    ):
        column_definitions = ", ".join(
            f"{column} {duckdb_type}" for column, duckdb_type in schema.items()
        )
        connection.execute(
            f"create table if not exists {XBRL_DLT_DATASET_NAME}.{table} ({column_definitions})"
        )


def _statement_document_row(row: dict[str, Any]) -> dict[str, Any]:
    return {column: row.get(column) for column in tables.STATEMENT_DOCUMENTS_COLUMNS}


def _fact_row(row: dict[str, Any]) -> dict[str, Any]:
    return {column: row.get(column) for column in tables.FACTS_COLUMNS}


def _parsed_table_columns(table: str) -> list[str]:
    if table == tables.STATEMENT_DOCUMENTS_TABLE:
        return tables.STATEMENT_DOCUMENTS_COLUMNS
    if table == tables.FACTS_TABLE:
        return tables.FACTS_COLUMNS
    raise ValueError(f"Unsupported parsed XBRL DuckDB table {table!r}")


def build_parse_quality_row(
    *,
    xml_documents_frame: pl.DataFrame,
    statement_documents_frame: pl.DataFrame,
    facts_frame: pl.DataFrame,
    generated_at: str,
) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "xml_documents_count": xml_documents_frame.height,
        "statement_documents_count": statement_documents_frame.height,
        "facts_count": facts_frame.height,
        "statements_with_zero_facts_count": _zero_facts_statement_count(
            statement_documents_frame
        ),
        "duplicate_statement_keys_count": _duplicate_count(
            statement_documents_frame,
            column="statement_key",
        ),
        "parser_warning_statements_count": _non_empty_string_count(
            statement_documents_frame,
            column="validation_warnings",
            ignore_values={"[]"},
        ),
        "missing_statement_business_ids_count": _missing_string_count(
            statement_documents_frame,
            column="business_id",
        ),
        "missing_statement_financial_dates_count": _missing_string_count(
            statement_documents_frame,
            column="financial_date",
        ),
        "missing_fact_business_ids_count": _missing_string_count(facts_frame, column="business_id"),
        "missing_fact_financial_dates_count": _missing_string_count(
            facts_frame,
            column="financial_date",
        ),
        "facts_per_statement_min": _numeric_column_stat(
            statement_documents_frame,
            column="facts_count",
            statistic="min",
            default=0,
        ),
        "facts_per_statement_avg": _numeric_column_stat(
            statement_documents_frame,
            column="facts_count",
            statistic="mean",
            default=0.0,
        ),
        "facts_per_statement_max": _numeric_column_stat(
            statement_documents_frame,
            column="facts_count",
            statistic="max",
            default=0,
        ),
        "latest_parsed_at": _max_string(statement_documents_frame, column="parsed_at"),
        "top_concept_namespaces": json.dumps(
            _top_counts(facts_frame, column="concept_namespace", count_name="count"),
            ensure_ascii=False,
        ),
        "top_concepts": json.dumps(
            _top_counts(facts_frame, column="concept_qname", count_name="count"),
            ensure_ascii=False,
        ),
    }


def build_concept_profile_rows(
    facts_frame: pl.DataFrame,
    *,
    example_limit: int = 5,
) -> list[dict[str, Any]]:
    if facts_frame.height == 0:
        return []

    concept_columns = ["concept_qname", "concept_namespace", "concept_local_name"]
    concepts = facts_frame.select(concept_columns).unique().sort("concept_qname").to_dicts()
    rows: list[dict[str, Any]] = []
    for concept in concepts:
        concept_facts = facts_frame.filter(
            (pl.col("concept_qname") == concept["concept_qname"])
            & (pl.col("concept_namespace") == concept["concept_namespace"])
            & (pl.col("concept_local_name") == concept["concept_local_name"])
        )
        rows.append(
            {
                "concept_qname": concept["concept_qname"],
                "concept_namespace": concept["concept_namespace"],
                "concept_local_name": concept["concept_local_name"],
                "facts_count": concept_facts.height,
                "statement_count": _unique_non_empty_count(concept_facts, column="statement_key"),
                "business_count": _unique_non_empty_count(concept_facts, column="business_id"),
                "financial_date_count": _unique_non_empty_count(
                    concept_facts,
                    column="financial_date",
                ),
                "numeric_count": _value_kind_count(concept_facts, value_kind="numeric"),
                "date_count": _value_kind_count(concept_facts, value_kind="date"),
                "text_count": _value_kind_count(concept_facts, value_kind="text"),
                "current_period_count": _current_period_count(concept_facts),
                "comparative_count": _comparative_count(concept_facts),
                "example_numeric_values": json.dumps(
                    _example_values(concept_facts, column="numeric_value", limit=example_limit),
                    ensure_ascii=False,
                ),
                "example_date_values": json.dumps(
                    _example_values(concept_facts, column="date_value", limit=example_limit),
                    ensure_ascii=False,
                ),
                "example_text_values": json.dumps(
                    _example_values(concept_facts, column="text_value", limit=example_limit),
                    ensure_ascii=False,
                ),
            }
        )

    return sorted(rows, key=lambda row: (-int(row["facts_count"]), str(row["concept_qname"])))

def _zero_facts_statement_count(frame: pl.DataFrame) -> int:
    if "facts_count" not in frame.columns or frame.height == 0:
        return 0
    return int(frame.select((pl.col("facts_count").fill_null(0) == 0).sum()).item())


def _duplicate_count(frame: pl.DataFrame, *, column: str) -> int:
    if column not in frame.columns or frame.height == 0:
        return 0
    return frame.height - int(frame.select(pl.col(column).n_unique()).item())


def _missing_string_count(frame: pl.DataFrame, *, column: str) -> int:
    if column not in frame.columns or frame.height == 0:
        return 0
    return int(
        frame.select((pl.col(column).fill_null("").str.strip_chars() == "").sum()).item()
    )


def _non_empty_string_count(
    frame: pl.DataFrame,
    *,
    column: str,
    ignore_values: set[str] | None = None,
) -> int:
    if column not in frame.columns or frame.height == 0:
        return 0
    ignored = ignore_values or set()
    value = pl.col(column).fill_null("").str.strip_chars()
    return int(frame.select(((value != "") & value.is_in(list(ignored)).not_()).sum()).item())


def _numeric_column_stat(
    frame: pl.DataFrame,
    *,
    column: str,
    statistic: str,
    default: int | float,
) -> int | float:
    if column not in frame.columns or frame.height == 0:
        return default
    expression = getattr(pl.col(column), statistic)()
    value = frame.select(expression).item()
    return default if value is None else value


def _max_string(frame: pl.DataFrame, *, column: str) -> str:
    if column not in frame.columns or frame.height == 0:
        return ""
    value = frame.select(pl.col(column).fill_null("").max()).item()
    return str(value or "")


def _top_counts(
    frame: pl.DataFrame,
    *,
    column: str,
    count_name: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    if column not in frame.columns or frame.height == 0:
        return []
    return (
        frame.filter(pl.col(column).fill_null("").str.strip_chars() != "")
        .group_by(column)
        .len(count_name)
        .sort([count_name, column], descending=[True, False])
        .head(limit)
        .to_dicts()
    )


def _unique_non_empty_count(frame: pl.DataFrame, *, column: str) -> int:
    if column not in frame.columns or frame.height == 0:
        return 0
    return int(
        frame.filter(pl.col(column).fill_null("").str.strip_chars() != "")
        .select(pl.col(column).n_unique())
        .item()
    )


def _value_kind_count(frame: pl.DataFrame, *, value_kind: str) -> int:
    if "value_kind" not in frame.columns or frame.height == 0:
        return 0
    return int(frame.select((pl.col("value_kind") == value_kind).sum()).item())


def _comparative_count(frame: pl.DataFrame) -> int:
    if "is_comparative" not in frame.columns or frame.height == 0:
        return 0
    return int(frame.select(pl.col("is_comparative").fill_null(False).sum()).item())


def _current_period_count(frame: pl.DataFrame) -> int:
    if "is_comparative" not in frame.columns or frame.height == 0:
        return frame.height
    return int(frame.select(pl.col("is_comparative").fill_null(False).not_().sum()).item())


def _example_values(frame: pl.DataFrame, *, column: str, limit: int) -> list[str]:
    if column not in frame.columns or frame.height == 0:
        return []
    return (
        frame.select(pl.col(column).fill_null("").cast(pl.Utf8).str.strip_chars())
        .filter(pl.col(column) != "")
        .unique(maintain_order=True)
        .head(limit)
        .to_series()
        .to_list()
    )
