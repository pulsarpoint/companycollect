import uuid
from collections.abc import Callable, Iterable
from datetime import date
from typing import Any

import pyarrow as pa

from dagster_v3.defs.esma_firds import tables
from dagster_v3.defs.esma_firds.parser import FirdsRecord

INSERT_BATCH_SIZE = 2_000
INSERT_LOG_INTERVAL_ROWS = 100_000

_RAW_COLUMN_TYPES = {
    "source_record_id": "varchar",
    "source_file_id": "varchar",
    "source_file_name": "varchar",
    "source_file_type": "varchar",
    "source_file_checksum": "varchar",
    "source_publication_date": "varchar",
    "source_row_number": "ubigint",
    "event_type": "varchar",
    "isin": "varchar",
    "mic": "varchar",
    "issuer_lei": "varchar",
    "full_name": "varchar",
    "short_name": "varchar",
    "cfi_code": "varchar",
    "notional_currency": "varchar",
    "commodity_derivative": "boolean",
    "issuer_request": "boolean",
    "admission_approval_at": "varchar",
    "request_admission_at": "varchar",
    "first_trade_at": "varchar",
    "termination_at": "varchar",
    "competent_authority_country": "varchar",
    "relevant_venue_mic": "varchar",
    "valid_from": "varchar",
    "source_download_url": "varchar",
    "source_object_key": "varchar",
    "source_run_id": "varchar",
    "source_retrieved_at": "varchar",
    "source_payload_hash": "varchar",
    "resolved_at": "varchar",
}
assert tuple(_RAW_COLUMN_TYPES) == tables.RAW_RECORD_COLUMNS

_RAW_ARROW_TYPES = tuple(
    {
        "varchar": pa.string(),
        "ubigint": pa.uint64(),
        "boolean": pa.bool_(),
    }[column_type]
    for column_type in _RAW_COLUMN_TYPES.values()
)

_RAW_TABLES = frozenset(
    {
        tables.FULL_RECORDS_RAW_TABLE,
        tables.DELTA_EVENTS_RAW_TABLE,
        tables.CANCELLATIONS_RAW_TABLE,
    }
)


def ensure_duckdb_tables(connection: Any) -> None:
    connection.execute(f"create schema if not exists {tables.DUCKDB_SCHEMA}")
    raw_columns_sql = ", ".join(
        f"{name} {column_type}" for name, column_type in _RAW_COLUMN_TYPES.items()
    )
    for table_name in sorted(_RAW_TABLES):
        qualified_table = f"{tables.DUCKDB_SCHEMA}.{table_name}"
        connection.execute(
            f"""
            create table if not exists {qualified_table}
            ({raw_columns_sql})
            """
        )
        # Early FIRDS bootstrap attempts duplicated the full XML element in
        # DuckDB. The immutable ZIP plus source row and payload hash are the
        # audit contract, so remove that obsolete high-volume column in place.
        connection.execute(
            f"alter table {qualified_table} drop column if exists raw_record_xml"
        )
    connection.execute(
        f"""
        create table if not exists {tables.DUCKDB_SCHEMA}.{tables.RAW_FILES_TABLE}
        (
            source_file_id varchar primary key,
            source_file_name varchar not null,
            source_file_type varchar not null,
            source_publication_date date not null,
            source_file_checksum varchar not null,
            source_object_key varchar not null,
            record_count ubigint not null,
            source_run_id varchar not null,
            ingested_at timestamp with time zone not null
        )
        """
    )
    connection.execute(
        f"""
        create table if not exists {tables.DUCKDB_SCHEMA}.{tables.SNAPSHOT_SETS_TABLE}
        (
            file_type varchar not null,
            publication_date date not null,
            file_count uinteger not null,
            source_run_id varchar not null,
            completed_at timestamp with time zone not null,
            primary key (file_type, publication_date)
        )
        """
    )


def replace_source_file_records(
    connection: Any,
    *,
    table: str,
    source_file_id: str,
    records: Iterable[FirdsRecord],
    allow_empty: bool = False,
    log_info: Callable[..., object] | None = None,
) -> int:
    if table not in _RAW_TABLES:
        raise ValueError(f"Unsupported FIRDS raw table: {table}")
    qualified_table = f"{tables.DUCKDB_SCHEMA}.{table}"
    connection.execute("set preserve_insertion_order = false")
    connection.execute("begin transaction")
    try:
        connection.execute(
            f"delete from {qualified_table} where source_file_id = ?",
            [source_file_id],
        )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise

    batch: list[tuple[object, ...]] = []
    row_count = 0
    next_log_row = INSERT_LOG_INTERVAL_ROWS
    for record in records:
        if record.source_file_id != source_file_id:
            raise ValueError(
                "FIRDS record source file id does not match replacement target"
            )
        batch.append(record.as_tuple())
        if len(batch) >= INSERT_BATCH_SIZE:
            _insert_raw_record_batch(
                connection,
                qualified_table=qualified_table,
                rows=batch,
            )
            row_count += len(batch)
            batch.clear()
            if log_info is not None and row_count >= next_log_row:
                log_info(
                    "Inserted FIRDS raw records: source_file_id=%s rows=%s",
                    source_file_id,
                    row_count,
                )
                next_log_row += INSERT_LOG_INTERVAL_ROWS
    if batch:
        _insert_raw_record_batch(
            connection,
            qualified_table=qualified_table,
            rows=batch,
        )
        row_count += len(batch)
    if row_count == 0 and not allow_empty:
        raise ValueError(
            f"FIRDS source file {source_file_id} produced zero records"
        )
    if log_info is not None:
        log_info(
            "Finished FIRDS raw records: source_file_id=%s rows=%s",
            source_file_id,
            row_count,
        )
    return row_count


def _insert_raw_record_batch(
    connection: Any,
    *,
    qualified_table: str,
    rows: list[tuple[object, ...]],
) -> None:
    values_by_column = tuple(zip(*rows, strict=True))
    arrow_table = pa.Table.from_arrays(
        [
            pa.array(values, type=arrow_type)
            for values, arrow_type in zip(
                values_by_column,
                _RAW_ARROW_TYPES,
                strict=True,
            )
        ],
        names=tables.RAW_RECORD_COLUMNS,
    )
    registered_name = f"_esma_firds_batch_{uuid.uuid4().hex}"
    connection.register(registered_name, arrow_table)
    try:
        connection.execute("begin transaction")
        connection.execute(
            f"insert into {qualified_table} select * from {registered_name}"
        )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise
    finally:
        connection.unregister(registered_name)


def raw_file_is_ingested(
    connection: Any,
    *,
    source_file_id: str,
    source_file_checksum: str,
) -> bool:
    ensure_duckdb_tables(connection)
    row = connection.execute(
        f"""
        select source_file_checksum
        from {tables.DUCKDB_SCHEMA}.{tables.RAW_FILES_TABLE}
        where source_file_id = ?
        """,
        [source_file_id],
    ).fetchone()
    return row is not None and str(row[0]) == source_file_checksum


def record_raw_file_ingestion(
    connection: Any,
    *,
    source_file_id: str,
    source_file_name: str,
    source_file_type: str,
    source_publication_date: str,
    source_file_checksum: str,
    source_object_key: str,
    record_count: int,
    source_run_id: str,
) -> None:
    connection.execute(
        f"""
        delete from {tables.DUCKDB_SCHEMA}.{tables.RAW_FILES_TABLE}
        where source_file_id = ?
        """,
        [source_file_id],
    )
    connection.execute(
        f"""
        insert into {tables.DUCKDB_SCHEMA}.{tables.RAW_FILES_TABLE}
        values (?, ?, ?, cast(? as date), ?, ?, ?, ?, current_timestamp)
        """,
        [
            source_file_id,
            source_file_name,
            source_file_type,
            source_publication_date,
            source_file_checksum,
            source_object_key,
            record_count,
            source_run_id,
        ],
    )


def mark_snapshot_set_complete(
    connection: Any,
    *,
    file_type: str,
    publication_date: str,
    file_count: int,
    source_run_id: str,
) -> None:
    if file_count <= 0:
        raise ValueError("FIRDS snapshot set must contain at least one file")
    connection.execute(
        f"""
        delete from {tables.DUCKDB_SCHEMA}.{tables.SNAPSHOT_SETS_TABLE}
        where file_type = ? and publication_date = cast(? as date)
        """,
        [file_type, publication_date],
    )
    connection.execute(
        f"""
        insert into {tables.DUCKDB_SCHEMA}.{tables.SNAPSHOT_SETS_TABLE}
        values (?, cast(? as date), ?, ?, current_timestamp)
        """,
        [file_type, publication_date, file_count, source_run_id],
    )


def invalidate_snapshot_set(
    connection: Any,
    *,
    file_type: str,
    publication_date: str,
) -> None:
    connection.execute(
        f"""
        delete from {tables.DUCKDB_SCHEMA}.{tables.SNAPSHOT_SETS_TABLE}
        where file_type = ? and publication_date = cast(? as date)
        """,
        [file_type, publication_date],
    )


def rebuild_derived_tables(
    connection: Any,
    *,
    as_of_date: str | date,
) -> dict[str, object]:
    event_counts = rebuild_event_history(connection)
    current_counts = rebuild_current_state(
        connection,
        as_of_date=as_of_date,
    )
    return {**event_counts, **current_counts}


def rebuild_event_history(connection: Any) -> dict[str, object]:
    ensure_duckdb_tables(connection)
    baseline_dates = _baseline_dates(connection)
    if not baseline_dates:
        raise ValueError("No complete FIRDS FULINS baseline has been ingested")
    earliest_baseline = baseline_dates[0]
    connection.execute("begin transaction")
    try:
        _build_event_history(connection, earliest_baseline=earliest_baseline)
        event_rows = _count_table(connection, tables.EVENTS_EXPORT_TABLE)
        if event_rows == 0:
            raise ValueError("FIRDS event-history rebuild produced zero rows")
        connection.execute("commit")
        return {
            "earliest_baseline_date": earliest_baseline.isoformat(),
            "event_rows": event_rows,
        }
    except Exception:
        connection.execute("rollback")
        raise


def rebuild_current_state(
    connection: Any,
    *,
    as_of_date: str | date,
    minimum_current_rows: int = 1,
    minimum_country_count: int = 1,
    minimum_mic_count: int = 1,
) -> dict[str, object]:
    if minimum_current_rows <= 0:
        raise ValueError("minimum_current_rows must be positive")
    if minimum_country_count <= 0:
        raise ValueError("minimum_country_count must be positive")
    if minimum_mic_count <= 0:
        raise ValueError("minimum_mic_count must be positive")
    ensure_duckdb_tables(connection)
    as_of = date.fromisoformat(str(as_of_date))
    baseline_dates = _baseline_dates(connection)
    if not baseline_dates:
        raise ValueError("No complete FIRDS FULINS baseline has been ingested")
    earliest_baseline = baseline_dates[0]
    current_baseline = baseline_dates[-1]
    connection.execute("begin transaction")
    try:
        _build_current_state(
            connection,
            current_baseline=current_baseline,
            as_of_date=as_of,
        )
        _cleanup_snapshot_staging(
            connection,
            earliest_baseline=earliest_baseline,
            current_baseline=current_baseline,
        )
        current_rows = _count_table(connection, tables.CURRENT_EXPORT_TABLE)
        country_count, mic_count, distinct_isins = connection.execute(
            f"""
            select
                count(distinct competent_authority_country)
                    filter (where competent_authority_country <> ''),
                count(distinct mic) filter (where mic <> ''),
                count(distinct isin) filter (where isin <> '')
            from {tables.DUCKDB_SCHEMA}.{tables.CURRENT_EXPORT_TABLE}
            """
        ).fetchone()
        validation_failures = []
        if current_rows < minimum_current_rows:
            validation_failures.append(
                f"rows={current_rows} below {minimum_current_rows}"
            )
        if country_count < minimum_country_count:
            validation_failures.append(
                f"countries={country_count} below {minimum_country_count}"
            )
        if mic_count < minimum_mic_count:
            validation_failures.append(
                f"MICs={mic_count} below {minimum_mic_count}"
            )
        if validation_failures:
            raise ValueError(
                "FIRDS current-state coverage guard failed: "
                + "; ".join(validation_failures)
            )
        connection.execute("commit")
        return {
            "current_baseline_date": current_baseline.isoformat(),
            "current_rows": current_rows,
            "distinct_isins": distinct_isins,
            "country_count": country_count,
            "mic_count": mic_count,
        }
    except Exception:
        connection.execute("rollback")
        raise


def current_rows_by_country(connection: Any) -> dict[str, int]:
    ensure_duckdb_tables(connection)
    return {
        str(country): int(row_count)
        for country, row_count in connection.execute(
            f"""
            select competent_authority_country, count(*)
            from {tables.DUCKDB_SCHEMA}.{tables.CURRENT_EXPORT_TABLE}
            where competent_authority_country <> ''
            group by competent_authority_country
            order by competent_authority_country
            """
        ).fetchall()
    }


def current_rows_by_mic(connection: Any) -> dict[str, int]:
    ensure_duckdb_tables(connection)
    return {
        str(mic): int(row_count)
        for mic, row_count in connection.execute(
            f"""
            select mic, count(*)
            from {tables.DUCKDB_SCHEMA}.{tables.CURRENT_EXPORT_TABLE}
            where mic <> ''
            group by mic
            order by count(*) desc, mic
            limit 250
            """
        ).fetchall()
    }


def event_coverage_metadata(connection: Any) -> dict[str, object]:
    ensure_duckdb_tables(connection)
    event_table = f"{tables.DUCKDB_SCHEMA}.{tables.EVENTS_EXPORT_TABLE}"
    event_type_counts = {
        str(event_type): int(row_count)
        for event_type, row_count in connection.execute(
            f"""
            select event_type, count(*)
            from {event_table}
            group by event_type
            order by event_type
            """
        ).fetchall()
    }
    cfi_category_counts = {
        str(category): int(row_count)
        for category, row_count in connection.execute(
            f"""
            select left(cfi_code, 1) as category, count(*)
            from {event_table}
            where cfi_code <> ''
            group by category
            order by category
            """
        ).fetchall()
    }
    source_date_counts = {
        publication_date.isoformat(): int(row_count)
        for publication_date, row_count in connection.execute(
            f"""
            select source_publication_date, count(*)
            from {event_table}
            group by source_publication_date
            order by source_publication_date
            """
        ).fetchall()
    }
    with_issuer_lei, without_issuer_lei, distinct_issuer_leis = (
        connection.execute(
            f"""
            select
                count(*) filter (where issuer_lei <> ''),
                count(*) filter (where issuer_lei = ''),
                count(distinct issuer_lei) filter (where issuer_lei <> '')
            from {event_table}
            """
        ).fetchone()
    )
    return {
        "event_type_counts": event_type_counts,
        "cfi_category_counts": cfi_category_counts,
        "source_publication_date_counts": source_date_counts,
        "issuer_lei_coverage": {
            "with_issuer_lei": int(with_issuer_lei),
            "without_issuer_lei": int(without_issuer_lei),
            "distinct_issuer_leis": int(distinct_issuer_leis),
        },
    }


def _baseline_dates(connection: Any) -> list[date]:
    return [
        row[0]
        for row in connection.execute(
            f"""
            select publication_date
            from {tables.DUCKDB_SCHEMA}.{tables.SNAPSHOT_SETS_TABLE}
            where file_type = 'FULINS'
            order by publication_date
            """
        ).fetchall()
    ]


def _build_event_history(connection: Any, *, earliest_baseline: date) -> None:
    normalized = _normalized_record_select()
    connection.execute(
        f"""
        create or replace table
            {tables.DUCKDB_SCHEMA}.{tables.EVENTS_EXPORT_TABLE}
        as
        with combined as (
            select {normalized}
            from {tables.DUCKDB_SCHEMA}.{tables.FULL_RECORDS_RAW_TABLE}
            where try_cast(source_publication_date as date) = cast(? as date)

            union all

            select {normalized}
            from {tables.DUCKDB_SCHEMA}.{tables.DELTA_EVENTS_RAW_TABLE}
            where try_cast(source_publication_date as date) >= cast(? as date)
              and exists (
                  select 1
                  from {tables.DUCKDB_SCHEMA}.{tables.SNAPSHOT_SETS_TABLE}
                      completed_set
                  where completed_set.file_type = 'DLTINS'
                    and completed_set.publication_date =
                        try_cast(source_publication_date as date)
              )
        ),
        sequenced as (
            select
                *,
                lead(valid_from) over (
                    partition by isin, mic
                    order by
                        valid_from,
                        source_publication_date,
                        source_file_name,
                        source_row_number
                ) as next_valid_from,
                row_number() over (
                    partition by isin, mic
                    order by
                        source_publication_date desc,
                        source_file_name desc,
                        source_row_number desc
                ) = 1 as latest_value
            from combined
        ),
        versioned as (
            select
                * exclude (next_valid_from),
                case
                    when next_valid_from > valid_from
                    then next_valid_from - interval 1 day
                    when next_valid_from = valid_from
                    then valid_from
                    else null
                end as valid_to_value
            from sequenced
        )
        select
            source_record_id,
            source_file_id,
            source_file_name,
            source_file_type,
            source_file_checksum,
            source_publication_date,
            source_row_number,
            event_type,
            isin,
            mic,
            issuer_lei,
            full_name,
            short_name,
            cfi_code,
            notional_currency,
            commodity_derivative,
            issuer_request,
            admission_approval_at,
            request_admission_at,
            first_trade_at,
            termination_at,
            competent_authority_country,
            relevant_venue_mic,
            valid_from,
            cast(valid_to_value as date) as valid_to,
            latest_value as is_latest,
            source_download_url,
            source_object_key,
            source_run_id,
            source_retrieved_at,
            resolved_at
        from versioned
        """,
        [earliest_baseline, earliest_baseline],
    )


def _build_current_state(
    connection: Any,
    *,
    current_baseline: date,
    as_of_date: date,
) -> None:
    normalized = _normalized_record_select()
    connection.execute(
        f"""
        create or replace table
            {tables.DUCKDB_SCHEMA}.{tables.CURRENT_EXPORT_TABLE}
        as
        with combined as (
            select {normalized}
            from {tables.DUCKDB_SCHEMA}.{tables.FULL_RECORDS_RAW_TABLE}
            where try_cast(source_publication_date as date) = cast(? as date)

            union all

            select {normalized}
            from {tables.DUCKDB_SCHEMA}.{tables.DELTA_EVENTS_RAW_TABLE}
            where try_cast(source_publication_date as date) > cast(? as date)
              and exists (
                  select 1
                  from {tables.DUCKDB_SCHEMA}.{tables.SNAPSHOT_SETS_TABLE}
                      completed_set
                  where completed_set.file_type = 'DLTINS'
                    and completed_set.publication_date =
                        try_cast(source_publication_date as date)
              )
        ),
        latest as (
            select * exclude (state_rank)
            from (
                select
                    *,
                    row_number() over (
                        partition by isin, mic
                        order by
                            source_publication_date desc,
                            source_file_name desc,
                            source_row_number desc
                    ) as state_rank
                from combined
            )
            where state_rank = 1
        )
        select
            isin,
            mic,
            issuer_lei,
            full_name,
            short_name,
            cfi_code,
            notional_currency,
            commodity_derivative,
            issuer_request,
            admission_approval_at,
            request_admission_at,
            first_trade_at,
            termination_at,
            competent_authority_country,
            relevant_venue_mic,
            valid_from,
            source_record_id,
            source_file_id,
            source_file_name,
            source_file_type,
            source_file_checksum,
            source_publication_date,
            event_type as latest_event_type,
            source_download_url,
            source_object_key,
            source_run_id,
            source_retrieved_at,
            resolved_at
        from latest
        where event_type not in ('TERMINATED', 'CANCELLED')
          and (
              termination_at is null
              or cast(termination_at as date) >= cast(? as date)
          )
        """,
        [current_baseline, current_baseline, as_of_date],
    )


def _normalized_record_select() -> str:
    return """
        coalesce(source_record_id, '') as source_record_id,
        coalesce(source_file_id, '') as source_file_id,
        coalesce(source_file_name, '') as source_file_name,
        coalesce(source_file_type, '') as source_file_type,
        coalesce(source_file_checksum, '') as source_file_checksum,
        cast(source_publication_date as date) as source_publication_date,
        cast(source_row_number as ubigint) as source_row_number,
        coalesce(event_type, '') as event_type,
        coalesce(isin, '') as isin,
        coalesce(mic, '') as mic,
        coalesce(issuer_lei, '') as issuer_lei,
        coalesce(full_name, '') as full_name,
        coalesce(short_name, '') as short_name,
        coalesce(cfi_code, '') as cfi_code,
        coalesce(notional_currency, '') as notional_currency,
        commodity_derivative,
        issuer_request,
        try_cast(nullif(admission_approval_at, '') as timestamptz)
            as admission_approval_at,
        try_cast(nullif(request_admission_at, '') as timestamptz)
            as request_admission_at,
        try_cast(nullif(first_trade_at, '') as timestamptz) as first_trade_at,
        try_cast(nullif(termination_at, '') as timestamptz) as termination_at,
        coalesce(competent_authority_country, '')
            as competent_authority_country,
        coalesce(relevant_venue_mic, '') as relevant_venue_mic,
        cast(valid_from as date) as valid_from,
        coalesce(source_download_url, '') as source_download_url,
        coalesce(source_object_key, '') as source_object_key,
        coalesce(source_run_id, '') as source_run_id,
        cast(source_retrieved_at as timestamptz) as source_retrieved_at,
        cast(resolved_at as timestamptz) as resolved_at
    """


def _cleanup_snapshot_staging(
    connection: Any,
    *,
    earliest_baseline: date,
    current_baseline: date,
) -> None:
    connection.execute(
        f"""
        delete from {tables.DUCKDB_SCHEMA}.{tables.FULL_RECORDS_RAW_TABLE}
        where try_cast(source_publication_date as date) not in (
            cast(? as date),
            cast(? as date)
        )
        """,
        [earliest_baseline, current_baseline],
    )
    latest_cancellation = connection.execute(
        f"""
        select max(publication_date)
        from {tables.DUCKDB_SCHEMA}.{tables.SNAPSHOT_SETS_TABLE}
        where file_type = 'FULCAN'
        """
    ).fetchone()[0]
    if latest_cancellation is not None:
        connection.execute(
            f"""
            delete from {tables.DUCKDB_SCHEMA}.{tables.CANCELLATIONS_RAW_TABLE}
            where try_cast(source_publication_date as date) <> cast(? as date)
            """,
            [latest_cancellation],
        )


def _count_table(connection: Any, table_name: str) -> int:
    return int(
        connection.execute(
            f"select count(*) from {tables.DUCKDB_SCHEMA}.{table_name}"
        ).fetchone()[0]
    )
