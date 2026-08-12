from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from dagster_v3.defs.company_domain_suggestions import tables

_NORMALIZED_VALUE_SQL = "lowerUTF8(replaceRegexpAll(raw_value, '[^\\p{L}\\p{N}]', ''))"
_LEGAL_IDENTIFIER_TYPES_SQL = (
    "'vat', 'lei', 'organization_number', 'orgnr', 'registration_number', "
    "'company_number', 'tax_id', 'tax_number'"
)
DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS = 30.0


def replace_web_domain_identity_features(
    clickhouse_client: Any,
    *,
    indexed_at: datetime | None = None,
    include_jsonld: bool = True,
    progress_log_interval_seconds: float = DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS,
    log: Callable[..., object] | None = None,
) -> dict[str, int | float]:
    if progress_log_interval_seconds <= 0:
        raise ValueError("progress_log_interval_seconds must be greater than zero")
    timestamp = indexed_at or datetime.now(UTC)
    stage = f"_tmp_{tables.FEATURES_TABLE}_{uuid.uuid4().hex}"
    qualified_stage = f"{tables.CLICKHOUSE_DATABASE}.{stage}"
    source_queries = [
        ("domain_labels", _domain_label_select_sql()),
        ("identifiers", _identifier_select_sql()),
    ]
    if include_jsonld:
        source_queries.append(("jsonld_names", _jsonld_name_select_sql()))

    build_started_at = time.monotonic()
    build_stats: dict[str, int | float] = {}
    primary_error: BaseException | None = None
    try:
        _log(log, "Web identity index build started: phases=%d", len(source_queries))
        clickhouse_client.execute(
            f"CREATE TABLE {qualified_stage} AS {tables.QUALIFIED_FEATURES_TABLE}"
        )
        columns = ", ".join(tables.FEATURE_COLUMNS)
        stage_rows = 0
        for phase_number, (source_name, select_sql) in enumerate(
            source_queries, start=1
        ):
            phase_stats = _execute_insert_select_with_progress(
                clickhouse_client,
                query=f"INSERT INTO {qualified_stage} ({columns}) {select_sql}",
                params={"indexed_at": timestamp},
                source_name=source_name,
                phase_number=phase_number,
                phase_count=len(source_queries),
                progress_log_interval_seconds=progress_log_interval_seconds,
                log=log,
            )
            current_stage_rows = _scalar(
                clickhouse_client,
                f"SELECT count() FROM {qualified_stage}",
            )
            phase_stats["output_rows"] = current_stage_rows - stage_rows
            stage_rows = current_stage_rows
            build_stats.update(
                {
                    f"{source_name}_{stat_name}": value
                    for stat_name, value in phase_stats.items()
                }
            )
            _log(
                log,
                "Web identity index phase staged: phase=%s output_rows=%d "
                "stage_rows=%d",
                source_name,
                phase_stats["output_rows"],
                stage_rows,
            )

        _log(log, "Web identity index validation started: stage_rows=%d", stage_rows)
        duplicate_count = _scalar(
            clickhouse_client,
            f"""
            SELECT count()
            FROM
            (
                SELECT
                    feature_type,
                    normalized_value,
                    root_domain,
                    source_field,
                    crawl_id
                FROM {qualified_stage}
                GROUP BY
                    feature_type,
                    normalized_value,
                    root_domain,
                    source_field,
                    crawl_id
                HAVING count() > 1
            )
            """,
        )
        if duplicate_count:
            raise ValueError(
                "Web-domain identity feature stage contains duplicate keys: "
                f"duplicates={duplicate_count}"
            )
        _log(log, "Web identity index validation completed: duplicate_keys=0")
        _log(log, "Web identity index publish started: stage_rows=%d", stage_rows)
        clickhouse_client.execute(
            f"EXCHANGE TABLES {qualified_stage} AND {tables.QUALIFIED_FEATURES_TABLE}"
        )
        counts = {
            str(feature_type): int(count)
            for feature_type, count in clickhouse_client.execute(
                f"""
                SELECT feature_type, count()
                FROM {tables.QUALIFIED_FEATURES_TABLE}
                GROUP BY feature_type
                ORDER BY feature_type
                """
            )
        }
        counts["total"] = sum(counts.values())
        build_stats["build_elapsed_seconds"] = round(
            time.monotonic() - build_started_at,
            3,
        )
        _log(
            log,
            "Web identity index build completed: rows=%d elapsed_seconds=%.1f "
            "counts=%s",
            counts["total"],
            build_stats["build_elapsed_seconds"],
            counts,
        )
        return {**counts, **build_stats}
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        try:
            clickhouse_client.execute(f"DROP TABLE IF EXISTS {qualified_stage}")
        except Exception:
            if primary_error is None:
                raise


def _execute_insert_select_with_progress(
    clickhouse_client: Any,
    *,
    query: str,
    params: dict[str, object],
    source_name: str,
    phase_number: int,
    phase_count: int,
    progress_log_interval_seconds: float,
    log: Callable[..., object] | None,
) -> dict[str, int | float]:
    phase_started_at = time.monotonic()
    last_logged_at = phase_started_at - progress_log_interval_seconds
    query_id = f"web-domain-identity-{source_name}-{uuid.uuid4().hex}"
    _log(
        log,
        "Web identity index phase started: phase=%s position=%d/%d query_id=%s",
        source_name,
        phase_number,
        phase_count,
        query_id,
    )
    progress_result = clickhouse_client.execute_with_progress(
        query,
        params,
        query_id=query_id,
    )
    for _ in progress_result:
        progress_logged_at = time.monotonic()
        if progress_logged_at - last_logged_at < progress_log_interval_seconds:
            continue
        _log_clickhouse_progress(
            log,
            source_name=source_name,
            elapsed_seconds=progress_logged_at - phase_started_at,
            progress=progress_result.progress_totals,
        )
        last_logged_at = progress_logged_at

    elapsed_seconds = time.monotonic() - phase_started_at
    progress = progress_result.progress_totals
    _log_clickhouse_progress(
        log,
        source_name=source_name,
        elapsed_seconds=elapsed_seconds,
        progress=progress,
        completed=True,
    )
    return {
        "elapsed_seconds": round(elapsed_seconds, 3),
        "read_rows": int(progress.rows),
        "read_bytes": int(progress.bytes),
        "total_rows_to_read": int(progress.total_rows),
        "total_bytes_to_read": int(progress.total_bytes),
        "written_rows": int(progress.written_rows),
        "written_bytes": int(progress.written_bytes),
    }


def _log_clickhouse_progress(
    log: Callable[..., object] | None,
    *,
    source_name: str,
    elapsed_seconds: float,
    progress: Any,
    completed: bool = False,
) -> None:
    read_rows = int(progress.rows)
    total_rows = int(progress.total_rows)
    rate = read_rows / elapsed_seconds if elapsed_seconds > 0 else 0.0
    _log(
        log,
        "Web identity index phase %s: phase=%s elapsed_seconds=%.1f "
        "read_rows=%d total_rows_to_read=%d progress=%s read_bytes=%s "
        "written_rows=%d written_bytes=%s rows_per_second=%.0f",
        "completed" if completed else "progress",
        source_name,
        elapsed_seconds,
        read_rows,
        total_rows,
        _progress_percentage(read_rows, total_rows),
        _format_bytes(int(progress.bytes)),
        int(progress.written_rows),
        _format_bytes(int(progress.written_bytes)),
        rate,
    )


def _progress_percentage(read_rows: int, total_rows: int) -> str:
    if total_rows <= 0:
        return "unknown"
    return f"{min(100.0, read_rows / total_rows * 100):.1f}%"


def _format_bytes(byte_count: int) -> str:
    value = float(byte_count)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f}{unit}"
        value /= 1024
    raise AssertionError("unreachable")


def _log(
    log: Callable[..., object] | None,
    message: str,
    *args: object,
) -> None:
    if log is not None:
        log(message, *args)


def _domain_label_select_sql() -> str:
    return f"""
    SELECT
        'domain_label' AS feature_type,
        normalized_value,
        root_domain,
        argMax(raw_value, observed_at) AS raw_value,
        'root_domain_label' AS source_field,
        argMax(source_url, observed_at) AS source_url,
        argMax(crawl_id, observed_at) AS crawl_id,
        max(observed_at) AS source_resolved_at,
        %(indexed_at)s AS indexed_at
    FROM
    (
        SELECT
            root_domain,
            arrayElement(splitByChar('.', root_domain), 1) AS raw_value,
            {_NORMALIZED_VALUE_SQL} AS normalized_value,
            source_url,
            crawl_id,
            resolved_at AS observed_at
        FROM corpscout.commoncrawl_domains
        WHERE root_domain != ''
    )
    WHERE length(normalized_value) >= 3
    GROUP BY normalized_value, root_domain
    """


def _identifier_select_sql() -> str:
    return f"""
    WITH latest_domain_crawls AS
    (
        SELECT
            domains.root_domain,
            max(domains.crawl_id) AS crawl_id
        FROM corpscout.commoncrawl_domains AS domains
        WHERE domains.root_domain != ''
          AND domains.crawl_id != ''
        GROUP BY domains.root_domain
    )
    SELECT
        'identifier' AS feature_type,
        normalized_value,
        root_domain,
        argMax(raw_value, observed_at) AS raw_value,
        argMax(source_field, observed_at) AS source_field,
        argMax(source_url, observed_at) AS source_url,
        argMax(crawl_id, observed_at) AS crawl_id,
        max(observed_at) AS source_resolved_at,
        %(indexed_at)s AS indexed_at
    FROM
    (
        SELECT
            root_domain,
            id_value AS raw_value,
            id_type AS source_field,
            {_NORMALIZED_VALUE_SQL} AS normalized_value,
            source_url,
            crawl_id,
            resolved_at AS observed_at
        FROM corpscout.commoncrawl_domain_identifiers AS identifiers
        INNER JOIN latest_domain_crawls USING (root_domain, crawl_id)
        WHERE valid = 1
          AND root_domain != ''
          AND id_value != ''
          AND lowerUTF8(id_type) IN ({_LEGAL_IDENTIFIER_TYPES_SQL})
    )
    WHERE length(normalized_value) >= 3
    GROUP BY normalized_value, root_domain
    """


def _jsonld_name_select_sql() -> str:
    return f"""
    SELECT
        feature_type,
        normalized_value,
        root_domain,
        argMax(raw_value, observed_at) AS raw_value,
        argMax(source_field, observed_at) AS source_field,
        argMax(page_url, observed_at) AS source_url,
        argMax(crawl_id, observed_at) AS crawl_id,
        max(observed_at) AS source_resolved_at,
        %(indexed_at)s AS indexed_at
    FROM
    (
        SELECT
            root_domain,
            page_url,
            crawl_id,
            resolved_at AS observed_at,
            tupleElement(feature, 1) AS feature_type,
            tupleElement(feature, 2) AS source_field,
            tupleElement(feature, 3) AS raw_value,
            {_NORMALIZED_VALUE_SQL} AS normalized_value
        FROM corpscout.commoncrawl_page_jsonld
        ARRAY JOIN
        [
            tuple(
                if(
                    arrayExists(type -> lowerUTF8(type) = 'person', entity_types),
                    'person_name',
                    'organization_name'
                ),
                'jsonld_name',
                name
            ),
            tuple('organization_name', 'jsonld_legal_name', if(is_organization = 1, legal_name, ''))
        ] AS feature
        WHERE root_domain != ''
          AND (
              is_organization = 1
              OR arrayExists(type -> lowerUTF8(type) = 'person', entity_types)
          )
          AND crawl_id = (
              SELECT max(crawl_id)
              FROM corpscout.commoncrawl_page_jsonld
          )
    )
    WHERE length(normalized_value) >= 3
    GROUP BY feature_type, normalized_value, root_domain
    """


def _scalar(clickhouse_client: Any, sql: str) -> int:
    rows = clickhouse_client.execute(sql)
    return int(rows[0][0]) if rows else 0
