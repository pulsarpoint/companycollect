import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Callable

import dagster as dg
import polars as pl
from pydantic import ConfigDict

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.arelle_parser import (
    ArelleStatementParser,
    parse_statement_xml_with_arelle,
)
from dagster_v3.defs.finland_xbrl.assets.common import (
    BACKFILL_PARTITIONS,
    DAILY_PARTITIONS,
    XBRL_BUCKET,
)
from dagster_v3.defs.finland_xbrl.assets.raw_xml_documents import (
    finland_xbrl_raw_xml_documents_backfill,
    finland_xbrl_raw_xml_documents_incremental,
)
from dagster_v3.defs.finland_xbrl.resources import XbrlParquetStorageResource

class XbrlParsedConfig(dg.Config):
    model_config = ConfigDict(extra="forbid")


@dataclass
class XbrlParseRunResult:
    statement_rows: list[dict[str, Any]]
    fact_rows: list[dict[str, Any]]
    failed_rows: list[dict[str, Any]]


def run_finland_xbrl_arelle_parse(
    *,
    object_store: ObjectStoreResource,
    documents: list[dict[str, Any]],
    run_id: str,
    parser: ArelleStatementParser = parse_statement_xml_with_arelle,
    log_info: Callable[[str], None] | None = None,
    progress_interval: int = 25,
) -> XbrlParseRunResult:
    _log_parse_progress(log_info, f"Parsing {len(documents)} XBRL XML documents")
    statement_rows, fact_rows, failed = parse_xbrl_documents(
        documents,
        object_store=object_store,
        run_id=run_id,
        parser=parser,
        log_info=log_info,
        progress_interval=progress_interval,
    )
    if log_info is not None:
        log_info(
            "Parsed XBRL rows ready for parquet: "
            f"statement_rows={len(statement_rows)} fact_rows={len(fact_rows)} "
            f"failed={len(failed)}"
        )
    return XbrlParseRunResult(
        statement_rows=statement_rows,
        fact_rows=fact_rows,
        failed_rows=failed,
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
    object_store: ObjectStoreResource,
    *,
    window_start: date,
    window_end: date,
    documents: list[dict[str, Any]],
    documents_manifest_path: Path,
    run_id: str,
    write_statement_documents: Callable[[str, list[dict[str, Any]]], Path],
    write_facts: Callable[[str, list[dict[str, Any]]], Path],
    parser: ArelleStatementParser = parse_statement_xml_with_arelle,
) -> dg.MaterializeResult:
    context.log.info(
        "XBRL parse partition %s started: window=%s..%s documents_manifest_path=%s",
        context.partition_key,
        window_start.isoformat(),
        window_end.isoformat(),
        documents_manifest_path,
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
    context.log.info(
        "XBRL parse partition %s: %d manifest docs, %d in window",
        context.partition_key,
        len(documents),
        len(in_window),
    )

    result = run_finland_xbrl_arelle_parse(
        object_store=object_store,
        documents=in_window,
        run_id=run_id,
        parser=parser,
        log_info=context.log.info,
    )
    statement_documents_path = write_statement_documents(
        context.partition_key,
        result.statement_rows,
    )
    facts_path = write_facts(context.partition_key, result.fact_rows)
    failed_this_run = len(result.failed_rows)
    if failed_this_run:
        # Failed docs are not written, so they are re-attempted on the next run
        # (and keep failing until the source document is fixed/removed).
        context.log.warning(
            "XBRL parse partition %s: %d documents failed to parse and were skipped "
            "(will retry next run)",
            context.partition_key,
            failed_this_run,
        )
    if not in_window:
        context.log.info(
            "XBRL parse partition %s: no documents in window",
            context.partition_key,
        )

    context.log.info(
        "XBRL parse partition %s complete: parsed=%d failed=%d statement_rows=%d facts=%d statement_path=%s facts_path=%s",
        context.partition_key,
        len(result.statement_rows),
        failed_this_run,
        len(result.statement_rows),
        len(result.fact_rows),
        statement_documents_path,
        facts_path,
    )
    return dg.MaterializeResult(
        metadata={
            "partition": context.partition_key,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "documents_in_window": len(in_window),
            "documents_parsed_this_run": len(result.statement_rows),
            "documents_failed_this_run": failed_this_run,
            "documents_missing_registration_date": missing_registration,
            "statement_documents_row_count": len(result.statement_rows),
            "facts_row_count": len(result.fact_rows),
            "xml_documents_manifest_path": str(documents_manifest_path),
            "statement_documents_parquet_path": str(statement_documents_path),
            "facts_parquet_path": str(facts_path),
        }
    )


@dg.asset(
    name="finland_xbrl_parse_backfill",
    group_name="finland_xbrl",
    deps=[finland_xbrl_raw_xml_documents_backfill],
    partitions_def=BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"python", "parquet", "arelle"},
)
def finland_xbrl_parse_backfill(
    context: dg.AssetExecutionContext,
    config: XbrlParsedConfig,
    xbrl_parquet_storage: XbrlParquetStorageResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    window = context.partition_time_window
    del config
    return _materialize_parse_window(
        context,
        object_store,
        window_start=window.start.date(),
        window_end=window.end.date(),
        documents=xbrl_parquet_storage.read_raw_xml_documents_backfill(
            context.partition_key
        ),
        documents_manifest_path=xbrl_parquet_storage.raw_xml_documents_backfill_path(
            context.partition_key
        ),
        run_id=context.run.run_id,
        write_statement_documents=xbrl_parquet_storage.write_statement_documents_backfill,
        write_facts=xbrl_parquet_storage.write_facts_backfill,
    )


@dg.asset(
    name="finland_xbrl_parse_incremental",
    group_name="finland_xbrl",
    deps=[finland_xbrl_raw_xml_documents_incremental],
    partitions_def=DAILY_PARTITIONS,
    kinds={"python", "parquet", "arelle"},
)
def finland_xbrl_parse_incremental(
    context: dg.AssetExecutionContext,
    config: XbrlParsedConfig,
    xbrl_parquet_storage: XbrlParquetStorageResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    window = context.partition_time_window
    del config
    return _materialize_parse_window(
        context,
        object_store,
        window_start=window.start.date(),
        window_end=window.end.date(),
        documents=xbrl_parquet_storage.read_raw_xml_documents_incremental(
            context.partition_key
        ),
        documents_manifest_path=xbrl_parquet_storage.raw_xml_documents_incremental_path(
            context.partition_key
        ),
        run_id=context.run.run_id,
        write_statement_documents=xbrl_parquet_storage.write_statement_documents_incremental,
        write_facts=xbrl_parquet_storage.write_facts_incremental,
    )


def _statement_document_row(row: dict[str, Any]) -> dict[str, Any]:
    return {column: row.get(column) for column in tables.STATEMENT_DOCUMENTS_COLUMNS}


def _fact_row(row: dict[str, Any]) -> dict[str, Any]:
    return {column: row.get(column) for column in tables.FACTS_COLUMNS}


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
