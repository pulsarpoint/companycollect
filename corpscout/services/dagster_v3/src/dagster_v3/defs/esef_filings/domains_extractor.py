"""The esef_domains extractor: registrable domains per archived ESEF filing.

First extractor of the documents-plus-independent-extractors architecture
(spec 2026-09-13): one asset, one table (corpscout.esef_domains, migration
000405), one version constant. It selects documents whose esef_domains rows
are missing or carry another extractor version, streams each archived package
from the object store into a process pool whose workers run the deterministic
website extraction in a killable child, and replaces the table's rows batch
by batch -- no partitions, no shared artifact, no schema version. A version
bump re-runs this extractor over every document and nothing else; the sensor
in domains_sensor.py launches the runs.

No ``from __future__ import annotations``: Dagster inspects the asset's
``context``/resource annotations directly.
"""

import tempfile
import uuid
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.document_rows import replace_document_rows
from dagster_v3.defs.esef_filings.domains_extraction import (
    ESEF_DOMAINS_EXTRACTOR_VERSION,
    STATUS_EMPTY,
    STATUS_FAILED,
    STATUS_OK,
    STATUS_TIMED_OUT,
    DocumentDomains,
    DomainsDocument,
    domain_rows,
    extract_package_domains_bounded,
)
from dagster_v3.defs.esef_filings.segment_assets import (
    ESEF_DOCUMENT_BUCKET,
    report_package_object_key,
)
from dagster_v3.defs.esef_filings.website_candidates import WEBSITE_FACT_CONCEPTS

GROUP_NAME = "esef"
ESEF_DOMAINS_POOL = "esef_domains_clickhouse"

# Pool workers are recycled after this many documents (lxml memory growth).
_EXTRACTIONS_PER_POOL_CHILD = 64
# Packages downloaded ahead of the extraction, per worker: bounds the temp dir.
_DOWNLOAD_AHEAD_PER_WORKER = 2
# fxo_ids per IN-list: the native driver inlines parameters into the query
# text, and ClickHouse's max_query_size defaults to 256 KiB.
_ID_CHUNK = 500

ExtractFn = Callable[
    [str, tuple[tuple[str, str], ...], tuple[str, ...], int, str], DocumentDomains
]


class EsefDomainsConfig(dg.Config):
    max_documents: int | None = Field(
        default=None,
        ge=1,
        le=100_000,
        description="Upper bound on documents processed this run (newest period_end first).",
    )
    source_document_ids: list[str] = Field(
        default_factory=list,
        description="Re-extract exactly these documents (fxo_id), stale or not.",
    )
    refresh_existing: bool = Field(
        default=False,
        description="Re-extract every available document, not only the stale ones.",
    )
    workers: int = Field(default=4, ge=1, le=8)
    batch_size: int = Field(
        default=250,
        ge=1,
        le=5000,
        description="Documents per ClickHouse replace; progress survives a failure after each batch.",
    )
    parse_timeout_seconds: int = Field(default=300, ge=60, le=3600)


# --- SQL ------------------------------------------------------------------------


def stale_documents_sql(*, all_available: bool, only_listed: bool) -> str:
    """Available documents -- a package archived by the weekly parse and
    facts in esef_facts -- whose esef_domains rows are missing or carry
    another extractor version (a newer one counts as stale too, like the
    artifact reuse rule). Newest period_end first; `period_end <= today()`
    drops the future-dated index rows (owner ruling 2026-09-11).

    Parameters: extractor_version (unless all_available),
    source_document_ids (when only_listed).
    """
    version_predicate = (
        ""
        if all_available
        else (
            " AND (extracted.source_document_id = '' "
            "OR extracted.extractor_version != %(extractor_version)s)"
        )
    )
    listed_predicate = (
        " AND filings.fxo_id IN %(source_document_ids)s" if only_listed else ""
    )
    return (
        "SELECT filings.fxo_id, filings.package_sha256, filings.lei, filings.period_end "
        f"FROM {tables.QUALIFIED_ESEF_FILINGS_TABLE} AS filings FINAL "
        f"INNER JOIN (SELECT DISTINCT fxo_id FROM {tables.QUALIFIED_ESEF_FACTS_TABLE}) "
        "AS parsed ON parsed.fxo_id = filings.fxo_id "
        "LEFT JOIN (SELECT source_document_id, max(extractor_version) AS extractor_version "
        f"FROM {tables.QUALIFIED_ESEF_DOMAINS_TABLE} GROUP BY source_document_id) "
        "AS extracted ON extracted.source_document_id = filings.fxo_id "
        "WHERE filings.package_sha256 != '' AND filings.period_end <= today()"
        f"{version_predicate}{listed_predicate} "
        "ORDER BY filings.period_end DESC, filings.fxo_id"
    )


def stale_document_count_sql() -> str:
    return (
        "SELECT count() FROM ("
        f"{stale_documents_sql(all_available=False, only_listed=False)})"
    )


def tagged_website_facts_sql() -> str:
    return (
        "SELECT fxo_id, concept_local_name, raw_value "
        f"FROM {tables.QUALIFIED_ESEF_FACTS_TABLE} "
        "WHERE fxo_id IN %(source_document_ids)s "
        "AND concept_local_name IN %(concepts)s AND raw_value != ''"
    )


def email_domains_sql() -> str:
    return (
        "SELECT source_document_id, normalized_value "
        f"FROM {tables.QUALIFIED_ESEF_DOCUMENT_CONTACT_CANDIDATES_TABLE} "
        "WHERE source_document_id IN %(source_document_ids)s "
        "AND candidate_kind = 'email'"
    )


# --- selection and inputs ---------------------------------------------------------


def _as_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _chunks(items: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def select_documents(
    clickhouse: ClickhouseResource, config: EsefDomainsConfig
) -> list[DomainsDocument]:
    listed = tuple(sorted(set(config.source_document_ids)))
    sql = stale_documents_sql(
        all_available=config.refresh_existing or bool(listed),
        only_listed=bool(listed),
    )
    parameters: dict[str, object] = {}
    if not (config.refresh_existing or listed):
        parameters["extractor_version"] = ESEF_DOMAINS_EXTRACTOR_VERSION
    if listed:
        parameters["source_document_ids"] = listed
    with clickhouse.get_connection() as client:
        rows = client.execute(sql, parameters)
    documents = []
    for row in rows:
        period_end = _as_date(row[3])
        documents.append(
            DomainsDocument(
                source_document_id=str(row[0]),
                package_sha256=str(row[1]),
                lei=str(row[2]),
                period_end=period_end,
                fiscal_year=period_end.year,
            )
        )
    if config.max_documents is not None:
        documents = documents[: config.max_documents]
    return documents


def load_tagged_website_facts(
    clickhouse: ClickhouseResource, source_document_ids: Sequence[str]
) -> dict[str, tuple[tuple[str, str], ...]]:
    """(concept_local_name, raw_value) per document, deduplicated and sorted."""
    grouped: dict[str, set[tuple[str, str]]] = {}
    concepts = tuple(sorted(WEBSITE_FACT_CONCEPTS))
    with clickhouse.get_connection() as client:
        for chunk in _chunks(list(source_document_ids), _ID_CHUNK):
            rows = client.execute(
                tagged_website_facts_sql(),
                {"source_document_ids": tuple(chunk), "concepts": concepts},
            )
            for document_id, concept, value in rows:
                grouped.setdefault(str(document_id), set()).add(
                    (str(concept), str(value))
                )
    return {
        document_id: tuple(sorted(values)) for document_id, values in grouped.items()
    }


def load_email_domains(
    clickhouse: ClickhouseResource, source_document_ids: Sequence[str]
) -> dict[str, tuple[str, ...]]:
    """The part after '@' of each e-mail candidate, per document."""
    grouped: dict[str, set[str]] = {}
    with clickhouse.get_connection() as client:
        for chunk in _chunks(list(source_document_ids), _ID_CHUNK):
            rows = client.execute(
                email_domains_sql(), {"source_document_ids": tuple(chunk)}
            )
            for document_id, value in rows:
                text = str(value)
                if "@" not in text:
                    continue
                domain = text.rpartition("@")[2].strip().lower()
                if domain:
                    grouped.setdefault(str(document_id), set()).add(domain)
    return {
        document_id: tuple(sorted(domains)) for document_id, domains in grouped.items()
    }


# --- the run ----------------------------------------------------------------------


@dataclass(frozen=True)
class _Outcome:
    document: DomainsDocument
    result: DocumentDomains


def _extract_batch(
    batch: Sequence[DomainsDocument],
    *,
    object_store: ObjectStoreResource,
    executor: ProcessPoolExecutor,
    temp_root: Path,
    tagged: Mapping[str, tuple[tuple[str, str], ...]],
    emails: Mapping[str, tuple[str, ...]],
    timeout_seconds: int,
    max_ahead: int,
    extract: ExtractFn,
) -> list[_Outcome]:
    """Download each package, hand it to the pool, keep at most `max_ahead`
    packages in flight; a download failure is that document's failure."""
    outcomes: list[_Outcome] = []
    pending: deque[tuple[DomainsDocument, Path, Future[DocumentDomains]]] = deque()

    def settle_oldest() -> None:
        document, package_path, future = pending.popleft()
        try:
            result = future.result()
        finally:
            package_path.unlink(missing_ok=True)
        outcomes.append(_Outcome(document, result))

    for document in batch:
        package_path = temp_root / f"{uuid.uuid4().hex}.zip"
        started = perf_counter()
        try:
            body = object_store.read_bytes(
                report_package_object_key(document.package_sha256),
                bucket=ESEF_DOCUMENT_BUCKET,
            )
            digest = sha256(body).hexdigest()
            if digest != document.package_sha256:
                raise ValueError(
                    "package SHA-256 mismatch: "
                    f"expected={document.package_sha256} actual={digest}"
                )
            package_path.write_bytes(body)
        except Exception as exc:  # noqa: BLE001 - a bad or missing package is this document's failure, not the run's
            outcomes.append(
                _Outcome(
                    document,
                    DocumentDomains(
                        STATUS_FAILED,
                        (),
                        frozenset(),
                        f"package download failed: {type(exc).__name__}: {exc}",
                        perf_counter() - started,
                    ),
                )
            )
            continue
        pending.append(
            (
                document,
                package_path,
                executor.submit(
                    extract,
                    str(package_path),
                    tagged.get(document.source_document_id, ()),
                    emails.get(document.source_document_id, ()),
                    timeout_seconds,
                    str(temp_root),
                ),
            )
        )
        while len(pending) >= max_ahead:
            settle_oldest()
    while pending:
        settle_oldest()
    return outcomes


def run_esef_domains_extraction(
    *,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
    config: EsefDomainsConfig,
    source_run_id: str,
    log: Any,
    extract: ExtractFn = extract_package_domains_bounded,
) -> dict[str, object]:
    wall_started = perf_counter()
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESEF_DATABASE,
        tables=(tables.ESEF_DOMAINS_TABLE,),
    )
    documents = select_documents(clickhouse, config)
    document_ids = [document.source_document_id for document in documents]
    tagged = load_tagged_website_facts(clickhouse, document_ids)
    emails = load_email_domains(clickhouse, document_ids)
    log.info(
        "esef_domains: %d documents to extract (%d with tagged website facts, "
        "%d with e-mail domains), version %s, %d workers",
        len(documents),
        len(tagged),
        len(emails),
        ESEF_DOMAINS_EXTRACTOR_VERSION,
        config.workers,
    )

    counters = {
        status: 0
        for status in (STATUS_OK, STATUS_EMPTY, STATUS_FAILED, STATUS_TIMED_OUT)
    }
    attempted = 0
    rows_written = 0
    extract_seconds = 0.0
    if documents:
        with (
            tempfile.TemporaryDirectory(prefix="esef-domains-") as temp_root_name,
            ProcessPoolExecutor(
                max_workers=config.workers,
                max_tasks_per_child=_EXTRACTIONS_PER_POOL_CHILD,
            ) as executor,
        ):
            temp_root = Path(temp_root_name)
            for batch_index, batch in enumerate(
                _chunks(documents, config.batch_size), start=1
            ):
                outcomes = _extract_batch(
                    batch,
                    object_store=object_store,
                    executor=executor,
                    temp_root=temp_root,
                    tagged=tagged,
                    emails=emails,
                    timeout_seconds=config.parse_timeout_seconds,
                    max_ahead=config.workers * _DOWNLOAD_AHEAD_PER_WORKER,
                    extract=extract,
                )
                extracted_at = datetime.now(UTC)
                rows = [
                    row
                    for outcome in outcomes
                    for row in domain_rows(
                        outcome.document,
                        outcome.result,
                        extractor_version=ESEF_DOMAINS_EXTRACTOR_VERSION,
                        source_run_id=source_run_id,
                        extracted_at=extracted_at,
                    )
                ]
                replace_document_rows(
                    clickhouse,
                    table=tables.ESEF_DOMAINS_TABLE,
                    columns=tables.ESEF_DOMAINS_EXPORT_COLUMNS,
                    source_document_ids=[
                        outcome.document.source_document_id for outcome in outcomes
                    ],
                    rows=rows,
                )
                for outcome in outcomes:
                    counters[outcome.result.status] += 1
                    extract_seconds += outcome.result.seconds
                    if outcome.result.status in (STATUS_FAILED, STATUS_TIMED_OUT):
                        log.warning(
                            "esef_domains: %s %s: %s",
                            outcome.document.source_document_id,
                            outcome.result.status,
                            outcome.result.error_message,
                        )
                attempted += len(outcomes)
                rows_written += len(rows)
                log.info(
                    "esef_domains: batch %d written (%d documents, %d rows; "
                    "%d/%d documents attempted so far)",
                    batch_index,
                    len(outcomes),
                    len(rows),
                    attempted,
                    len(documents),
                )

    return {
        "candidate_document_count": len(documents),
        "attempted_document_count": attempted,
        "processed_document_count": counters[STATUS_OK] + counters[STATUS_EMPTY],
        "documents_without_domains": counters[STATUS_EMPTY],
        "failed_document_count": counters[STATUS_FAILED],
        "timed_out_document_count": counters[STATUS_TIMED_OUT],
        "row_count": rows_written,
        "extractor_version": ESEF_DOMAINS_EXTRACTOR_VERSION,
        "workers": config.workers,
        "wall_seconds": round(perf_counter() - wall_started, 3),
        "extract_seconds": round(extract_seconds, 3),
        "table": tables.QUALIFIED_ESEF_DOMAINS_TABLE,
    }


# --- definitions ------------------------------------------------------------------


@dg.asset(
    name="esef_domains_clickhouse",
    deps=[
        dg.AssetKey("esef_filings_clickhouse"),
        dg.AssetDep(
            dg.AssetKey("esef_facts_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        ),
        dg.AssetDep(
            dg.AssetKey("esef_document_contact_candidates_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        ),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "clickhouse", "xbrl"},
    pool=ESEF_DOMAINS_POOL,
    retry_policy=dg.RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=dg.Backoff.EXPONENTIAL,
    ),
    metadata={"table": tables.QUALIFIED_ESEF_DOMAINS_TABLE},
    description=(
        "Extracts registrable domains from every archived ESEF report package "
        "whose corpscout.esef_domains rows are missing or predate "
        f"{ESEF_DOMAINS_EXTRACTOR_VERSION}, and replaces those documents' rows "
        "batch by batch (spec 2026-09-13: one independent extractor per product)."
    ),
)
def esef_domains_clickhouse(
    context: dg.AssetExecutionContext,
    config: EsefDomainsConfig,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    summary = run_esef_domains_extraction(
        clickhouse=clickhouse,
        object_store=object_store,
        config=config,
        source_run_id=context.run_id,
        log=context.log,
    )
    return dg.MaterializeResult(metadata=summary)


esef_domains_job = dg.define_asset_job(
    name="esef_domains_job",
    selection=dg.AssetSelection.assets(esef_domains_clickhouse),
    description="One esef_domains extractor run (the stale-documents sensor's job).",
)

defs = dg.Definitions(assets=[esef_domains_clickhouse], jobs=[esef_domains_job])
