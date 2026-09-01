import uuid
from collections.abc import Callable, Sequence
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.sweden_jobtech_links import tables


def append_stage_insert_sql(
    *,
    target: str,
    stage: str,
    uid_column: str,
    columns: Sequence[str],
) -> str:
    selected_columns = ", ".join(f"incoming.`{column}`" for column in columns)
    inserted_columns = ", ".join(f"`{column}`" for column in columns)
    return f"""
    INSERT INTO {target} ({inserted_columns})
    SELECT {selected_columns}
    FROM {stage} AS incoming
    LEFT ANTI JOIN {target} AS existing FINAL
        ON existing.{uid_column} = incoming.{uid_column}
    """


def append_normalized_partition_to_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Append one DuckDB partition without duplicating deterministic row UIDs."""
    target_tables = tuple(
        target_table for _, target_table, _, _ in tables.CLICKHOUSE_APPEND_TABLES
    )
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=target_tables,
    )

    counts: dict[str, int] = {}
    with clickhouse.get_connection() as client:
        for (
            duckdb_table,
            target_table,
            columns,
            uid_column,
        ) in tables.CLICKHOUSE_APPEND_TABLES:
            stage_table = f"_tmp_{target_table}_{uuid.uuid4().hex}"
            qualified_target = _qualified(target_table)
            qualified_stage = _qualified(stage_table)
            client.execute(f"CREATE TABLE {qualified_stage} AS {qualified_target}")
            try:
                staged_rows = export_duckdb_connection_table_to_clickhouse(
                    duckdb_connection=duckdb_connection,
                    clickhouse_client=client,
                    duckdb_schema=tables.DUCKDB_SCHEMA,
                    duckdb_table=duckdb_table,
                    clickhouse_database=tables.CLICKHOUSE_DATABASE,
                    clickhouse_table=stage_table,
                    columns=columns,
                    truncate=False,
                    log=log,
                )
                [(inserted_rows,)] = client.execute(
                    f"""
                    SELECT count()
                    FROM {qualified_stage} AS incoming
                    LEFT ANTI JOIN {qualified_target} AS existing FINAL
                        ON existing.{uid_column} = incoming.{uid_column}
                    """
                )
                client.execute(
                    append_stage_insert_sql(
                        target=qualified_target,
                        stage=qualified_stage,
                        uid_column=uid_column,
                        columns=columns,
                    )
                )
            finally:
                client.execute(f"DROP TABLE IF EXISTS {qualified_stage}")
            counts[f"{target_table}_staged"] = int(staged_rows)
            counts[f"{target_table}_inserted"] = int(inserted_rows)
    return counts


def active_intervals_insert_sql(*, intervals_stage: str) -> str:
    """Resolve advertisement presence into intervals across canonical snapshots."""
    return f"""
    INSERT INTO {intervals_stage}
    WITH canonical_snapshots AS (
        SELECT
            snapshot_date,
            argMax(snapshot_uid, tuple(retrieved_at, snapshot_uid)) AS snapshot_uid,
            row_number() OVER (ORDER BY snapshot_date) AS snapshot_number
        FROM {_qualified(tables.CLICKHOUSE_SNAPSHOTS_TABLE)} FINAL
        GROUP BY snapshot_date
    ), ordered AS (
        SELECT
            observation.*,
            snapshot.snapshot_number,
            lagInFrame(
                toUInt64(snapshot.snapshot_number),
                1,
                toUInt64(0)
            ) OVER (
                PARTITION BY observation.source_job_ad_uid
                ORDER BY snapshot.snapshot_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS previous_snapshot_number
        FROM {_qualified(tables.CLICKHOUSE_OBSERVATIONS_TABLE)} AS observation FINAL
        INNER JOIN canonical_snapshots AS snapshot
            ON snapshot.snapshot_uid = observation.snapshot_uid
    ), marked AS (
        SELECT
            *,
            if(
                previous_snapshot_number = 0
                OR snapshot_number != previous_snapshot_number + 1,
                1,
                0
            ) AS starts_new_interval
        FROM ordered
    ), grouped AS (
        SELECT
            *,
            sum(starts_new_interval) OVER (
                PARTITION BY source_job_ad_uid
                ORDER BY snapshot_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS interval_group
        FROM marked
    ), interval_groups AS (
        SELECT
            source_job_ad_uid,
            argMin(provider, tuple(snapshot_number, source_line_number)) AS provider,
            argMin(
                source_identifier,
                tuple(snapshot_number, source_line_number)
            ) AS source_identifier,
            interval_group,
            min(observed_at) AS active_from,
            min(snapshot_number) AS first_snapshot_number,
            max(snapshot_number) AS last_snapshot_number,
            min(snapshot_date) AS first_snapshot_date,
            max(snapshot_date) AS last_snapshot_date,
            min(observed_at) AS first_observed_at,
            max(observed_at) AS last_observed_at
        FROM grouped
        GROUP BY source_job_ad_uid, interval_group
    ), numbered AS (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY source_job_ad_uid
                ORDER BY first_snapshot_number
            ) AS interval_number
        FROM interval_groups
    ), latest_snapshot AS (
        SELECT max(snapshot_number) AS snapshot_number
        FROM canonical_snapshots
    )
    SELECT
        interval.source_job_ad_uid,
        interval.provider,
        interval.source_identifier,
        toUInt16(interval.interval_number) AS interval_number,
        interval.active_from,
        if(
            interval.last_snapshot_number < latest_snapshot.snapshot_number,
            toDateTime64(next_snapshot.snapshot_date, 3, 'UTC'),
            CAST(NULL AS Nullable(DateTime64(3, 'UTC')))
        ) AS active_to,
        if(
            interval.last_snapshot_number < latest_snapshot.snapshot_number,
            'first_absent_snapshot',
            ''
        ) AS active_to_basis,
        if(interval.last_snapshot_number < latest_snapshot.snapshot_number, 1, 0)
            AS is_end_estimated,
        interval.first_snapshot_date,
        interval.last_snapshot_date,
        interval.first_observed_at,
        interval.last_observed_at,
        now64(3, 'UTC') AS resolved_at
    FROM numbered AS interval
    CROSS JOIN latest_snapshot
    LEFT JOIN canonical_snapshots AS next_snapshot
        ON next_snapshot.snapshot_number = interval.last_snapshot_number + 1
    """


def job_ads_insert_sql(*, intervals_stage: str, job_ads_stage: str) -> str:
    """Build one serving row per ad, including its derived lifecycle status."""
    inserted_columns = ", ".join(tables.JOB_AD_COLUMNS)
    return f"""
    INSERT INTO {job_ads_stage} ({inserted_columns})
    WITH canonical_snapshots AS (
        SELECT
            snapshot_date,
            argMax(snapshot_uid, tuple(retrieved_at, snapshot_uid)) AS snapshot_uid,
            row_number() OVER (ORDER BY snapshot_date) AS snapshot_number
        FROM {_qualified(tables.CLICKHOUSE_SNAPSHOTS_TABLE)} FINAL
        GROUP BY snapshot_date
    ), latest_snapshot AS (
        SELECT max(snapshot_date) AS snapshot_date
        FROM canonical_snapshots
    ), latest_intervals AS (
        SELECT *
        FROM {intervals_stage}
        QUALIFY row_number() OVER (
            PARTITION BY source_job_ad_uid
            ORDER BY interval_number DESC
        ) = 1
    ), latest_observations AS (
        SELECT
            observation.source_job_ad_uid,
            argMax(
                observation.version_uid,
                tuple(
                    snapshot.snapshot_number,
                    observation.observed_at,
                    observation.ingested_at,
                    observation.observation_uid
                )
            ) AS version_uid,
            argMax(
                observation.snapshot_uid,
                tuple(
                    snapshot.snapshot_number,
                    observation.observed_at,
                    observation.ingested_at,
                    observation.observation_uid
                )
            ) AS snapshot_uid,
            argMax(
                observation.snapshot_date,
                tuple(
                    snapshot.snapshot_number,
                    observation.observed_at,
                    observation.ingested_at,
                    observation.observation_uid
                )
            ) AS snapshot_date,
            argMax(
                observation.observed_at,
                tuple(
                    snapshot.snapshot_number,
                    observation.observed_at,
                    observation.ingested_at,
                    observation.observation_uid
                )
            ) AS observed_at,
            argMax(
                observation.source_run_id,
                tuple(
                    snapshot.snapshot_number,
                    observation.observed_at,
                    observation.ingested_at,
                    observation.observation_uid
                )
            ) AS source_run_id
        FROM {_qualified(tables.CLICKHOUSE_OBSERVATIONS_TABLE)} AS observation FINAL
        INNER JOIN canonical_snapshots AS snapshot
            ON snapshot.snapshot_uid = observation.snapshot_uid
        GROUP BY observation.source_job_ad_uid
    )
    SELECT
        version.source_job_ad_uid,
        version.version_uid,
        version.provider,
        version.source_identifier,
        version.jobtech_links_id,
        version.source_hashsum,
        version.version_at,
        interval.interval_number,
        if(interval.active_to IS NULL, 'active', 'expired') AS status,
        interval.active_from,
        interval.active_to,
        interval.active_to_basis,
        interval.is_end_estimated,
        version.source_first_seen_at,
        version.publication_at,
        version.display_publication_at,
        version.application_deadline,
        version.is_valid,
        version.canonical_url,
        version.headline_original,
        version.brief_description_original,
        version.detected_language,
        version.employer_name,
        version.employer_url,
        version.employer_logo_url,
        version.employment_types,
        version.workplace_type,
        version.number_of_vacancies,
        version.occupation_concept_id,
        version.occupation_label_original,
        version.ssyk_level4_code,
        observation.snapshot_uid,
        observation.snapshot_date,
        observation.observed_at,
        observation.source_run_id,
        latest_snapshot.snapshot_date AS resolved_against_snapshot_date,
        now64(3, 'UTC') AS resolved_at
    FROM latest_intervals AS interval
    INNER JOIN latest_observations AS observation
        ON observation.source_job_ad_uid = interval.source_job_ad_uid
    INNER JOIN {_qualified(tables.CLICKHOUSE_VERSIONS_TABLE)} AS version FINAL
        ON version.version_uid = observation.version_uid
    CROSS JOIN latest_snapshot
    """


def replace_job_ads_from_observations(
    *,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int | str]:
    """Atomically publish intervals and one unified active/expired ad table."""
    required_tables = (
        tables.CLICKHOUSE_SNAPSHOTS_TABLE,
        tables.CLICKHOUSE_VERSIONS_TABLE,
        tables.CLICKHOUSE_OBSERVATIONS_TABLE,
        tables.CLICKHOUSE_INTERVALS_TABLE,
        tables.CLICKHOUSE_JOB_ADS_TABLE,
    )
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=required_tables,
    )

    suffix = uuid.uuid4().hex
    intervals_target = _qualified(tables.CLICKHOUSE_INTERVALS_TABLE)
    job_ads_target = _qualified(tables.CLICKHOUSE_JOB_ADS_TABLE)
    intervals_stage = _qualified(f"_tmp_{tables.CLICKHOUSE_INTERVALS_TABLE}_{suffix}")
    job_ads_stage = _qualified(f"_tmp_{tables.CLICKHOUSE_JOB_ADS_TABLE}_{suffix}")

    with clickhouse.get_connection() as client:
        [(snapshot_count, latest_snapshot_date)] = client.execute(
            f"""
            SELECT count(), max(snapshot_date)
            FROM
            (
                SELECT snapshot_date
                FROM {_qualified(tables.CLICKHOUSE_SNAPSHOTS_TABLE)} FINAL
                GROUP BY snapshot_date
            )
            """
        )
        if int(snapshot_count) == 0:
            raise ValueError(
                "No JobTech Links snapshots exist in ClickHouse; refusing to "
                "publish an empty serving table"
            )

        [(source_job_ads,)] = client.execute(
            f"""
            SELECT uniqExact(observation.source_job_ad_uid)
            FROM {_qualified(tables.CLICKHOUSE_OBSERVATIONS_TABLE)} AS observation FINAL
            INNER JOIN
            (
                SELECT
                    snapshot_date,
                    argMax(snapshot_uid, tuple(retrieved_at, snapshot_uid)) AS snapshot_uid
                FROM {_qualified(tables.CLICKHOUSE_SNAPSHOTS_TABLE)} FINAL
                GROUP BY snapshot_date
            ) AS snapshot
                ON snapshot.snapshot_uid = observation.snapshot_uid
            """
        )
        if int(source_job_ads) == 0:
            raise ValueError(
                "No JobTech Links observations exist for canonical snapshots; "
                "refusing to publish an empty serving table"
            )

        client.execute(f"CREATE TABLE {intervals_stage} AS {intervals_target}")
        client.execute(f"CREATE TABLE {job_ads_stage} AS {job_ads_target}")
        try:
            client.execute(active_intervals_insert_sql(intervals_stage=intervals_stage))
            client.execute(
                job_ads_insert_sql(
                    intervals_stage=intervals_stage,
                    job_ads_stage=job_ads_stage,
                )
            )
            [(intervals,)] = client.execute(f"SELECT count() FROM {intervals_stage}")
            [(job_ads, unique_job_ads, active_job_ads, expired_job_ads)] = (
                client.execute(
                    f"""
                    SELECT
                        count(),
                        uniqExact(source_job_ad_uid),
                        countIf(status = 'active'),
                        countIf(status = 'expired')
                    FROM {job_ads_stage}
                    """
                )
            )
            if int(job_ads) != int(source_job_ads):
                raise ValueError(
                    "JobTech Links serving row mismatch: "
                    f"source_ads={source_job_ads} serving_ads={job_ads}"
                )
            if int(unique_job_ads) != int(job_ads):
                raise ValueError(
                    "JobTech Links serving table contains duplicate advertisement "
                    f"identities: rows={job_ads} unique={unique_job_ads}"
                )
            if int(intervals) < int(job_ads):
                raise ValueError(
                    "JobTech Links interval count cannot be smaller than the "
                    f"serving job count: intervals={intervals} jobs={job_ads}"
                )

            client.execute(f"EXCHANGE TABLES {intervals_stage} AND {intervals_target}")
            try:
                client.execute(f"EXCHANGE TABLES {job_ads_stage} AND {job_ads_target}")
            except BaseException:
                client.execute(
                    f"EXCHANGE TABLES {intervals_stage} AND {intervals_target}"
                )
                raise
        finally:
            client.execute(f"DROP TABLE IF EXISTS {job_ads_stage}")
            client.execute(f"DROP TABLE IF EXISTS {intervals_stage}")

    counts: dict[str, int | str] = {
        "snapshots": int(snapshot_count),
        "source_job_ads": int(source_job_ads),
        "active_intervals": int(intervals),
        "job_ads": int(job_ads),
        "active_job_ads": int(active_job_ads),
        "expired_job_ads": int(expired_job_ads),
        "resolved_against_snapshot_date": str(latest_snapshot_date),
    }
    if log is not None:
        log("Published unified JobTech Links job ads: %s", counts)
    return counts


def _qualified(table: str) -> str:
    return f"{tables.CLICKHOUSE_DATABASE}.{table}"
