from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from typing import Any

import pyarrow as pa

from dagster_v3.defs.company_domain_suggestions import scoring, tables

DEFAULT_QUERY_BATCH_SIZE = 10_000
DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS = 30.0
DUCKDB_INSERT_BATCH_SIZE = 25_000
_DUCKDB_BATCH_RELATION = "company_domain_suggestion_batch"

_COMPANY_COLUMNS = ("company_id", "legal_name")
_COMPANY_FEATURE_COLUMNS = (
    "company_id",
    "feature_type",
    "normalized_value",
    "raw_value",
    "source_field",
    "trigger_score",
)
_COMPANY_PERSON_COLUMNS = (
    "company_id",
    "normalized_value",
    "raw_value",
    "source_field",
)
_COMPANY_INDUSTRY_COLUMNS = ("company_id", "nace_code")
_DOMAIN_FEATURE_COLUMNS = (
    "feature_type",
    "normalized_value",
    "root_domain",
    "raw_value",
    "source_field",
    "source_url",
    "crawl_id",
    "source_resolved_at",
)
_DOMAIN_SUPPORT_COLUMNS = (
    "root_domain",
    "country_match",
    "country_value",
    "crawl_id",
    "source_url",
)
_DOMAIN_INDUSTRY_COLUMNS = ("root_domain", "nace_code", "crawl_id", "source_url")
_DOMAIN_IDENTIFIER_COLUMNS = (
    "root_domain",
    "normalized_value",
    "raw_value",
    "source_field",
    "crawl_id",
    "source_url",
)
_DOMAIN_FEATURE_KEYS_EXTERNAL_TABLE = "company_domain_feature_keys"
_DOMAIN_FEATURE_KEYS_STRUCTURE = [("normalized_value", "String")]

COMPANIES_SQL = """
SELECT company_id, ifNull(legal_name, '')
FROM corpscout.se_companies FINAL
WHERE company_id != ''
ORDER BY company_id
"""

LEIS_SQL = """
SELECT registered_as, lei
FROM corpscout.gleif_lei_records FINAL
WHERE registered_as IS NOT NULL
  AND registered_as != ''
  AND lei != ''
  AND (
      upperUTF8(ifNull(primary_country_iso2, '')) = 'SE'
      OR startsWith(upperUTF8(ifNull(jurisdiction, '')), 'SE')
  )
"""

OFFICERS_SQL = """
SELECT company_id, first_name, last_name, role_kind
FROM corpscout.se_financial_report_signatories
WHERE company_id != ''
  AND first_name != ''
  AND last_name != ''
"""

COMPANY_INDUSTRIES_SQL = """
SELECT company_id, nace_rev2_class_code
FROM corpscout.se_industries FINAL
WHERE company_id != ''
  AND nace_rev2_class_code != ''
"""

DOMAIN_SUPPORT_SQL = """
SELECT
    jsonld.root_domain,
    argMaxIf(
        jsonld.country,
        jsonld.resolved_at,
        jsonld.country != ''
    ) AS country,
    argMaxIf(
        jsonld.crawl_id,
        jsonld.resolved_at,
        jsonld.country != ''
    ) AS crawl_id,
    argMaxIf(
        jsonld.page_url,
        jsonld.resolved_at,
        jsonld.country != ''
    ) AS source_url
FROM corpscout.commoncrawl_page_jsonld AS jsonld
WHERE jsonld.root_domain IN %(roots)s
  AND jsonld.is_organization = 1
GROUP BY jsonld.root_domain
"""

DOMAIN_FEATURE_MATCH_SQL = f"""
SELECT
    feature_type,
    normalized_value,
    root_domain,
    raw_value,
    source_field,
    source_url,
    crawl_id,
    source_resolved_at
FROM {tables.QUALIFIED_FEATURES_TABLE} FINAL
WHERE feature_type = %(feature_type)s
  AND normalized_value IN (
      SELECT normalized_value
      FROM {_DOMAIN_FEATURE_KEYS_EXTERNAL_TABLE}
  )
"""


def replace_sweden_suggestion_inputs(
    connection: Any,
    clickhouse_client: Any,
    *,
    query_batch_size: int = DEFAULT_QUERY_BATCH_SIZE,
    progress_log_interval_seconds: float = DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS,
    log: Callable[..., object] | None = None,
) -> dict[str, int | float]:
    if query_batch_size <= 0:
        raise ValueError("query_batch_size must be greater than zero")
    if progress_log_interval_seconds <= 0:
        raise ValueError("progress_log_interval_seconds must be greater than zero")
    input_started_at = time.monotonic()
    _log(log, "Sweden domain suggestion input preparation started")
    phase_started_at = time.monotonic()
    scoring.prepare_staging_tables(connection)
    _log(
        log,
        "Sweden domain suggestion phase completed: phase=prepare_staging_tables "
        "elapsed_seconds=%.1f",
        time.monotonic() - phase_started_at,
    )
    schema = tables.DUCKDB_SCHEMA

    phase_started_at = time.monotonic()
    last_progress_at = phase_started_at
    _log(log, "Sweden domain suggestion phase started: phase=companies")
    company_rows = 0
    company_batch: list[tuple[str, str]] = []
    feature_batch: list[tuple[object, ...]] = []
    for company_id_raw, legal_name_raw in _iter_rows(clickhouse_client, COMPANIES_SQL):
        company_id = str(company_id_raw)
        legal_name = str(legal_name_raw)
        company_batch.append((company_id, legal_name))
        features = (
            *scoring.company_name_features(legal_name),
            *scoring.company_identifier_features(company_id),
        )
        feature_batch.extend(
            (
                company_id,
                feature.feature_type,
                feature.normalized_value,
                feature.raw_value,
                feature.source_field,
                feature.trigger_score,
            )
            for feature in features
        )
        if len(company_batch) >= DUCKDB_INSERT_BATCH_SIZE:
            _insert_rows(
                connection,
                qualified_table=f"{schema}.companies",
                columns=_COMPANY_COLUMNS,
                rows=company_batch,
            )
            _insert_rows(
                connection,
                qualified_table=f"{schema}.company_features",
                columns=_COMPANY_FEATURE_COLUMNS,
                rows=feature_batch,
            )
            company_rows += len(company_batch)
            company_batch.clear()
            feature_batch.clear()
            last_progress_at = _log_if_due(
                log,
                last_logged_at=last_progress_at,
                interval_seconds=progress_log_interval_seconds,
                message=(
                    "Sweden domain suggestion phase progress: phase=companies "
                    "companies=%d"
                ),
                args=(company_rows,),
            )
    _insert_rows(
        connection,
        qualified_table=f"{schema}.companies",
        columns=_COMPANY_COLUMNS,
        rows=company_batch,
    )
    _insert_rows(
        connection,
        qualified_table=f"{schema}.company_features",
        columns=_COMPANY_FEATURE_COLUMNS,
        rows=feature_batch,
    )
    company_rows += len(company_batch)
    _log(
        log,
        "Sweden domain suggestion phase completed: phase=companies companies=%d "
        "elapsed_seconds=%.1f",
        company_rows,
        time.monotonic() - phase_started_at,
    )

    lei_features = _load_lei_features(
        connection,
        clickhouse_client,
        log=log,
        progress_log_interval_seconds=progress_log_interval_seconds,
    )
    officer_rows = _load_officers(
        connection,
        clickhouse_client,
        log=log,
        progress_log_interval_seconds=progress_log_interval_seconds,
    )
    phase_started_at = time.monotonic()
    _log(log, "Sweden domain suggestion phase started: phase=people_features")
    people_features = scoring.add_distinctive_people_features(connection)
    _log(
        log,
        "Sweden domain suggestion phase completed: phase=people_features rows=%d "
        "elapsed_seconds=%.1f",
        people_features,
        time.monotonic() - phase_started_at,
    )
    phase_started_at = time.monotonic()
    _log(log, "Sweden domain suggestion phase started: phase=ambiguity_filter")
    ambiguous_features_removed = scoring.remove_ambiguous_name_features(connection)
    _log(
        log,
        "Sweden domain suggestion phase completed: phase=ambiguity_filter "
        "removed_rows=%d elapsed_seconds=%.1f",
        ambiguous_features_removed,
        time.monotonic() - phase_started_at,
    )
    company_industries = _load_company_industries(
        connection,
        clickhouse_client,
        log=log,
        progress_log_interval_seconds=progress_log_interval_seconds,
    )
    matched_domain_features = _load_domain_feature_matches(
        connection,
        clickhouse_client,
        query_batch_size=query_batch_size,
        log=log,
        progress_log_interval_seconds=progress_log_interval_seconds,
    )
    candidate_domains = _load_domain_supporting_evidence(
        connection,
        clickhouse_client,
        query_batch_size=query_batch_size,
        log=log,
        progress_log_interval_seconds=progress_log_interval_seconds,
    )

    counts: dict[str, int | float] = {
        "companies": company_rows,
        "company_features": _count(connection, f"{schema}.company_features"),
        "lei_features": lei_features,
        "officer_rows": officer_rows,
        "people_features": people_features,
        "ambiguous_features_removed": ambiguous_features_removed,
        "company_industries": company_industries,
        "matched_domain_features": matched_domain_features,
        "candidate_domains": candidate_domains,
        "domain_industries": _count(connection, f"{schema}.domain_industries"),
        "domain_identifiers": _count(connection, f"{schema}.domain_identifiers"),
        "input_elapsed_seconds": round(time.monotonic() - input_started_at, 3),
    }
    _log(log, "Prepared Sweden company-domain suggestion inputs: counts=%s", counts)
    return counts


def _load_lei_features(
    connection: Any,
    clickhouse_client: Any,
    *,
    log: Callable[..., object] | None,
    progress_log_interval_seconds: float,
) -> int:
    schema = tables.DUCKDB_SCHEMA
    phase_started_at = time.monotonic()
    last_progress_at = phase_started_at
    _log(log, "Sweden domain suggestion phase started: phase=lei_features")
    batch: list[tuple[object, ...]] = []
    written = 0
    for company_id_raw, lei_raw in _iter_rows(clickhouse_client, LEIS_SQL):
        company_id = str(company_id_raw)
        for feature in scoring.company_identifier_features(
            company_id, lei=str(lei_raw)
        ):
            if feature.source_field != "lei":
                continue
            batch.append(
                (
                    company_id,
                    feature.feature_type,
                    feature.normalized_value,
                    feature.raw_value,
                    feature.source_field,
                    feature.trigger_score,
                )
            )
        if len(batch) >= DUCKDB_INSERT_BATCH_SIZE:
            _insert_rows(
                connection,
                qualified_table=f"{schema}.company_features",
                columns=_COMPANY_FEATURE_COLUMNS,
                rows=batch,
            )
            written += len(batch)
            batch.clear()
            last_progress_at = _log_if_due(
                log,
                last_logged_at=last_progress_at,
                interval_seconds=progress_log_interval_seconds,
                message=(
                    "Sweden domain suggestion phase progress: phase=lei_features "
                    "features=%d"
                ),
                args=(written,),
            )
    _insert_rows(
        connection,
        qualified_table=f"{schema}.company_features",
        columns=_COMPANY_FEATURE_COLUMNS,
        rows=batch,
    )
    total = written + len(batch)
    _log(
        log,
        "Sweden domain suggestion phase completed: phase=lei_features features=%d "
        "elapsed_seconds=%.1f",
        total,
        time.monotonic() - phase_started_at,
    )
    return total


def _load_officers(
    connection: Any,
    clickhouse_client: Any,
    *,
    log: Callable[..., object] | None,
    progress_log_interval_seconds: float,
) -> int:
    schema = tables.DUCKDB_SCHEMA
    phase_started_at = time.monotonic()
    last_progress_at = phase_started_at
    _log(log, "Sweden domain suggestion phase started: phase=officers")
    batch: list[tuple[str, str, str, str]] = []
    written = 0
    for company_id, first_name, last_name, role_kind in _iter_rows(
        clickhouse_client, OFFICERS_SQL
    ):
        raw_name = f"{first_name} {last_name}".strip()
        normalized_name = scoring.normalize_match_value(raw_name)
        if normalized_name:
            batch.append(
                (
                    str(company_id),
                    normalized_name,
                    raw_name,
                    f"officer:{role_kind}",
                )
            )
        if len(batch) >= DUCKDB_INSERT_BATCH_SIZE:
            _insert_rows(
                connection,
                qualified_table=f"{schema}.company_people_raw",
                columns=_COMPANY_PERSON_COLUMNS,
                rows=batch,
            )
            written += len(batch)
            batch.clear()
            last_progress_at = _log_if_due(
                log,
                last_logged_at=last_progress_at,
                interval_seconds=progress_log_interval_seconds,
                message=(
                    "Sweden domain suggestion phase progress: phase=officers "
                    "officers=%d"
                ),
                args=(written,),
            )
    _insert_rows(
        connection,
        qualified_table=f"{schema}.company_people_raw",
        columns=_COMPANY_PERSON_COLUMNS,
        rows=batch,
    )
    total = written + len(batch)
    _log(
        log,
        "Sweden domain suggestion phase completed: phase=officers officers=%d "
        "elapsed_seconds=%.1f",
        total,
        time.monotonic() - phase_started_at,
    )
    return total


def _load_company_industries(
    connection: Any,
    clickhouse_client: Any,
    *,
    log: Callable[..., object] | None,
    progress_log_interval_seconds: float,
) -> int:
    schema = tables.DUCKDB_SCHEMA
    phase_started_at = time.monotonic()
    last_progress_at = phase_started_at
    _log(log, "Sweden domain suggestion phase started: phase=company_industries")
    batch: list[tuple[str, str]] = []
    written = 0
    for company_id, nace_code in _iter_rows(clickhouse_client, COMPANY_INDUSTRIES_SQL):
        batch.append((str(company_id), str(nace_code)))
        if len(batch) >= DUCKDB_INSERT_BATCH_SIZE:
            _insert_rows(
                connection,
                qualified_table=f"{schema}.company_industries",
                columns=_COMPANY_INDUSTRY_COLUMNS,
                rows=batch,
            )
            written += len(batch)
            batch.clear()
            last_progress_at = _log_if_due(
                log,
                last_logged_at=last_progress_at,
                interval_seconds=progress_log_interval_seconds,
                message=(
                    "Sweden domain suggestion phase progress: "
                    "phase=company_industries rows=%d"
                ),
                args=(written,),
            )
    _insert_rows(
        connection,
        qualified_table=f"{schema}.company_industries",
        columns=_COMPANY_INDUSTRY_COLUMNS,
        rows=batch,
    )
    total = written + len(batch)
    _log(
        log,
        "Sweden domain suggestion phase completed: phase=company_industries rows=%d "
        "elapsed_seconds=%.1f",
        total,
        time.monotonic() - phase_started_at,
    )
    return total


def _load_domain_feature_matches(
    connection: Any,
    clickhouse_client: Any,
    *,
    query_batch_size: int,
    log: Callable[..., object] | None,
    progress_log_interval_seconds: float,
) -> int:
    schema = tables.DUCKDB_SCHEMA
    feature_key_count = _count(
        connection,
        f"""
        (
            select feature_type, normalized_value
            from {schema}.company_features
            group by feature_type, normalized_value
        ) as feature_keys
        """,
    )
    phase_started_at = time.monotonic()
    last_progress_at = phase_started_at
    _log(
        log,
        "Sweden domain suggestion phase started: phase=domain_feature_matches "
        "feature_keys=%d query_batch_size=%d",
        feature_key_count,
        query_batch_size,
    )
    cursor = connection.cursor().execute(
        f"""
        select feature_type, normalized_value
        from {schema}.company_features
        group by feature_type, normalized_value
        order by feature_type, normalized_value
        """
    )
    written = 0
    processed_keys = 0
    query_count = 0
    for key_batch in _fetchmany(cursor, query_batch_size):
        processed_keys += len(key_batch)
        values_by_type: dict[str, list[str]] = {}
        for feature_type, normalized_value in key_batch:
            values_by_type.setdefault(str(feature_type), []).append(
                str(normalized_value)
            )
        for feature_type, values in values_by_type.items():
            query_count += 1
            rows = _query_domain_feature_matches(
                clickhouse_client,
                feature_type=feature_type,
                normalized_values=values,
            )
            _insert_rows(
                connection,
                qualified_table=f"{schema}.domain_features",
                columns=_DOMAIN_FEATURE_COLUMNS,
                rows=rows,
            )
            written += len(rows)
        last_progress_at = _log_if_due(
            log,
            last_logged_at=last_progress_at,
            interval_seconds=progress_log_interval_seconds,
            message=(
                "Sweden domain suggestion phase progress: "
                "phase=domain_feature_matches feature_keys=%d/%d "
                "clickhouse_queries=%d matched_rows=%d"
            ),
            args=(processed_keys, feature_key_count, query_count, written),
        )
    _log(
        log,
        "Sweden domain suggestion phase completed: phase=domain_feature_matches "
        "feature_keys=%d clickhouse_queries=%d matched_rows=%d elapsed_seconds=%.1f",
        processed_keys,
        query_count,
        written,
        time.monotonic() - phase_started_at,
    )
    return written


def _query_domain_feature_matches(
    clickhouse_client: Any,
    *,
    feature_type: str,
    normalized_values: Sequence[str],
) -> list[Sequence[object]]:
    return clickhouse_client.execute(
        DOMAIN_FEATURE_MATCH_SQL,
        {"feature_type": feature_type},
        external_tables=[
            {
                "name": _DOMAIN_FEATURE_KEYS_EXTERNAL_TABLE,
                "structure": _DOMAIN_FEATURE_KEYS_STRUCTURE,
                "data": [(value,) for value in normalized_values],
            }
        ],
    )


def _load_domain_supporting_evidence(
    connection: Any,
    clickhouse_client: Any,
    *,
    query_batch_size: int,
    log: Callable[..., object] | None,
    progress_log_interval_seconds: float,
) -> int:
    schema = tables.DUCKDB_SCHEMA
    candidate_domain_total = _count(
        connection,
        f"""
        (
            select distinct root_domain
            from {schema}.domain_features
        ) as candidate_domains
        """,
    )
    phase_started_at = time.monotonic()
    last_progress_at = phase_started_at
    _log(
        log,
        "Sweden domain suggestion phase started: phase=domain_support "
        "candidate_domains=%d query_batch_size=%d",
        candidate_domain_total,
        query_batch_size,
    )
    root_cursor = connection.cursor().execute(
        f"select distinct root_domain from {schema}.domain_features order by root_domain"
    )
    candidate_domain_count = 0
    industry_row_count = 0
    identifier_row_count = 0
    query_batch_count = 0
    for root_rows in _fetchmany(root_cursor, query_batch_size):
        query_batch_count += 1
        root_batch = [str(row[0]) for row in root_rows]
        candidate_domain_count += len(root_batch)
        jsonld_rows = clickhouse_client.execute(
            DOMAIN_SUPPORT_SQL,
            {"roots": tuple(root_batch)},
        )
        jsonld_by_root = {str(row[0]): row[1:] for row in jsonld_rows}
        fallback_by_root = {
            str(root): (str(crawl_id), str(source_url))
            for root, crawl_id, source_url in connection.execute(
                f"""
                select
                    root_domain,
                    arg_max(crawl_id, source_resolved_at),
                    arg_max(source_url, source_resolved_at)
                from {schema}.domain_features
                where root_domain in (select unnest(?::varchar[]))
                group by root_domain
                """,
                [root_batch],
            ).fetchall()
        }
        support_rows = []
        for root in root_batch:
            country, crawl_id, source_url = jsonld_by_root.get(root, ("", "", ""))
            fallback_crawl, fallback_url = fallback_by_root.get(root, ("", ""))
            country_value = str(country or "")
            country_match = root.endswith(".se") or country_value.strip().lower() in {
                "se",
                "swe",
                "sweden",
                "sverige",
            }
            support_rows.append(
                (
                    root,
                    country_match,
                    country_value,
                    str(crawl_id or fallback_crawl),
                    str(source_url or fallback_url),
                )
            )
        _insert_rows(
            connection,
            qualified_table=f"{schema}.domain_support",
            columns=_DOMAIN_SUPPORT_COLUMNS,
            rows=support_rows,
        )

        industry_rows = clickhouse_client.execute(
            """
            SELECT root_domain, nace_code, crawl_id, source_url
            FROM corpscout.commoncrawl_industries FINAL
            WHERE root_domain IN %(roots)s
              AND nace_code != ''
            """,
            {"roots": tuple(root_batch)},
        )
        _insert_rows(
            connection,
            qualified_table=f"{schema}.domain_industries",
            columns=_DOMAIN_INDUSTRY_COLUMNS,
            rows=industry_rows,
        )
        industry_row_count += len(industry_rows)

        identifier_rows = clickhouse_client.execute(
            """
            SELECT
                root_domain,
                lowerUTF8(replaceRegexpAll(id_value, '[^\\p{L}\\p{N}]', '')),
                id_value,
                id_type,
                crawl_id,
                source_url
            FROM corpscout.commoncrawl_domain_identifiers FINAL
            WHERE root_domain IN %(roots)s
              AND valid = 1
              AND id_value != ''
              AND lowerUTF8(id_type) IN (
                  'vat',
                  'lei',
                  'organization_number',
                  'orgnr',
                  'registration_number',
                  'company_number',
                  'tax_id',
                  'tax_number'
              )
              AND (root_domain, crawl_id) IN (
                  SELECT
                      domains.root_domain,
                      max(domains.crawl_id)
                  FROM corpscout.commoncrawl_domains AS domains
                  WHERE domains.root_domain IN %(roots)s
                    AND domains.root_domain != ''
                    AND domains.crawl_id != ''
                  GROUP BY domains.root_domain
              )
            """,
            {"roots": tuple(root_batch)},
        )
        _insert_rows(
            connection,
            qualified_table=f"{schema}.domain_identifiers",
            columns=_DOMAIN_IDENTIFIER_COLUMNS,
            rows=identifier_rows,
        )
        identifier_row_count += len(identifier_rows)
        last_progress_at = _log_if_due(
            log,
            last_logged_at=last_progress_at,
            interval_seconds=progress_log_interval_seconds,
            message=(
                "Sweden domain suggestion phase progress: phase=domain_support "
                "candidate_domains=%d/%d batches=%d industry_rows=%d "
                "identifier_rows=%d"
            ),
            args=(
                candidate_domain_count,
                candidate_domain_total,
                query_batch_count,
                industry_row_count,
                identifier_row_count,
            ),
        )
    _log(
        log,
        "Sweden domain suggestion phase completed: phase=domain_support "
        "candidate_domains=%d batches=%d industry_rows=%d identifier_rows=%d "
        "elapsed_seconds=%.1f",
        candidate_domain_count,
        query_batch_count,
        industry_row_count,
        identifier_row_count,
        time.monotonic() - phase_started_at,
    )
    return candidate_domain_count


def _iter_rows(clickhouse_client: Any, sql: str) -> Iterator[Sequence[object]]:
    execute_iter = getattr(clickhouse_client, "execute_iter", None)
    if callable(execute_iter):
        yield from execute_iter(
            sql, settings={"max_block_size": DUCKDB_INSERT_BATCH_SIZE}
        )
        return
    yield from clickhouse_client.execute(sql)


def _insert_rows(
    connection: Any,
    *,
    qualified_table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[object]],
) -> None:
    materialized = list(rows)
    if not materialized:
        return
    arrow_table = pa.Table.from_arrays(
        [
            pa.array([row[index] for row in materialized])
            for index in range(len(columns))
        ],
        names=columns,
    )
    connection.register(_DUCKDB_BATCH_RELATION, arrow_table)
    try:
        column_list = ", ".join(columns)
        connection.execute(
            f"insert into {qualified_table} ({column_list}) "
            f"select {column_list} from {_DUCKDB_BATCH_RELATION}"
        )
    finally:
        connection.unregister(_DUCKDB_BATCH_RELATION)


def _fetchmany(cursor: Any, batch_size: int) -> Iterator[list[Sequence[object]]]:
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            return
        yield rows


def _count(connection: Any, table: str) -> int:
    row = connection.execute(f"select count(*) from {table}").fetchone()
    return int(row[0]) if row is not None else 0


def _log_if_due(
    log: Callable[..., object] | None,
    *,
    last_logged_at: float,
    interval_seconds: float,
    message: str,
    args: tuple[object, ...],
) -> float:
    progress_at = time.monotonic()
    if progress_at - last_logged_at < interval_seconds:
        return last_logged_at
    _log(log, message, *args)
    return progress_at


def _log(
    log: Callable[..., object] | None,
    message: str,
    *args: object,
) -> None:
    if log is not None:
        log(message, *args)
