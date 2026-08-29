import uuid
from collections.abc import Callable, Sequence
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.sweden_platsbanken import tables


COMPANY_JOB_COLUMNS = (
    "country_code",
    "company_id",
    "source_system",
    "source_job_ad_id",
    "interval_number",
    "active_from",
    "active_to",
    "active_to_basis",
    "is_end_estimated",
    "publication_at",
    "application_deadline",
    "removed_at",
    "employer_name",
    "headline_original",
    "description_text_original",
    "detected_language",
    "number_of_vacancies",
    "occupation_concept_id",
    "occupation_label_original",
    "occupation_group_concept_id",
    "occupation_group_label_original",
    "occupation_field_concept_id",
    "occupation_field_label_original",
    "employment_type_concept_id",
    "employment_type_label_original",
    "duration_concept_id",
    "duration_label_original",
    "working_hours_concept_id",
    "working_hours_label_original",
    "municipality_code",
    "municipality_name_original",
    "region_code",
    "region_name_original",
    "workplace_country_code",
    "workplace_country_name_original",
    "webpage_url",
    "source_type",
    "resolved_at",
)


def _append_stage_anti_join_sql(*, target: str, stage: str, uid_column: str) -> str:
    return f"""
    FROM {stage} AS incoming
    LEFT ANTI JOIN {target} AS existing FINAL
        ON existing.{uid_column} = incoming.{uid_column}
    """


def append_stage_insert_sql(
    *,
    target: str,
    stage: str,
    uid_column: str,
    columns: Sequence[str],
) -> str:
    selected = ", ".join(f"incoming.`{column}`" for column in columns)
    inserted = ", ".join(f"`{column}`" for column in columns)
    return f"""
    INSERT INTO {target} ({inserted})
    SELECT {selected}
    {_append_stage_anti_join_sql(target=target, stage=stage, uid_column=uid_column)}
    """


def append_job_history_batch(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    versions_table: str,
    events_table: str,
    requirements_table: str,
    contacts_table: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Append one normalized raw-object batch without duplicating stable UIDs."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(
            tables.VERSIONS_TABLE,
            tables.EVENTS_TABLE,
            tables.REQUIREMENTS_TABLE,
            tables.CONTACTS_TABLE,
        ),
    )
    source_tables = {
        tables.VERSIONS_TABLE: versions_table,
        tables.EVENTS_TABLE: events_table,
        tables.REQUIREMENTS_TABLE: requirements_table,
        tables.CONTACTS_TABLE: contacts_table,
    }
    counts: dict[str, int] = {}
    with clickhouse.get_connection() as client:
        for target_table, columns, uid_column in tables.APPEND_TABLES:
            stage_table = f"_tmp_{target_table}_{uuid.uuid4().hex}"
            qualified_target = _qualified(target_table)
            qualified_stage = _qualified(stage_table)
            client.execute(f"CREATE TABLE {qualified_stage} AS {qualified_target}")
            try:
                staged_rows = export_duckdb_connection_table_to_clickhouse(
                    duckdb_connection=duckdb_connection,
                    clickhouse_client=client,
                    duckdb_schema=tables.DUCKDB_SCHEMA,
                    duckdb_table=source_tables[target_table],
                    clickhouse_database=tables.CLICKHOUSE_DATABASE,
                    clickhouse_table=stage_table,
                    columns=columns,
                    truncate=False,
                    log=log,
                )
                anti_join_sql = _append_stage_anti_join_sql(
                    target=qualified_target,
                    stage=qualified_stage,
                    uid_column=uid_column,
                )
                [(new_rows,)] = client.execute(f"SELECT count()\n{anti_join_sql}")
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
            counts[f"{target_table}_inserted"] = int(new_rows)
    return counts


def intervals_insert_sql(intervals_stage: str) -> str:
    return f"""
    INSERT INTO {intervals_stage}
    WITH ordered AS (
        SELECT
            *,
            lagInFrame(
                toUInt16(is_active),
                1,
                toUInt16(2)
            ) OVER (
                PARTITION BY source_job_ad_id
                ORDER BY effective_at, event_at, event_uid
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS previous_state
        FROM corpscout.se_platsbanken_job_ad_events FINAL
    ), marked AS (
        SELECT
            *,
            if(previous_state = 2 OR previous_state != is_active, 1, 0)
                AS state_changed
        FROM ordered
    ), grouped AS (
        SELECT
            *,
            sum(state_changed) OVER (
                PARTITION BY source_job_ad_id
                ORDER BY effective_at, event_at, event_uid
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS state_group
        FROM marked
    ), state_groups AS (
        SELECT
            source_job_ad_id,
            state_group,
            argMin(is_active, tuple(effective_at, event_at, event_uid)) AS is_active,
            min(effective_at) AS state_from,
            argMaxIf(
                employer_org_number,
                tuple(effective_at, event_at, event_uid),
                employer_org_number != ''
            ) AS employer_org_number,
            min(event_at) AS first_event_at,
            max(event_at) AS last_event_at,
            argMin(active_to_basis, tuple(effective_at, event_at, event_uid))
                AS active_to_basis,
            max(is_estimated) AS is_estimated
        FROM grouped
        GROUP BY source_job_ad_id, state_group
    ), active_groups AS (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY source_job_ad_id ORDER BY state_group
            ) AS interval_number
        FROM state_groups
        WHERE is_active = 1
    )
    SELECT
        active_group.source_job_ad_id,
        toUInt16(active_group.interval_number) AS interval_number,
        active_group.employer_org_number,
        active_group.state_from AS active_from,
        if(
            end_group.state_group = 0,
            CAST(NULL AS Nullable(DateTime64(3, 'UTC'))),
            end_group.state_from
        ) AS active_to,
        if(end_group.state_group = 0, '', end_group.active_to_basis)
            AS active_to_basis,
        if(end_group.state_group = 0, 0, end_group.is_estimated)
            AS is_end_estimated,
        active_group.first_event_at,
        if(
            end_group.state_group = 0,
            active_group.last_event_at,
            end_group.last_event_at
        ) AS last_event_at,
        now64(3, 'UTC') AS resolved_at
    FROM active_groups AS active_group
    LEFT JOIN state_groups AS end_group
        ON end_group.source_job_ad_id = active_group.source_job_ad_id
       AND end_group.state_group = active_group.state_group + 1
       AND end_group.is_active = 0
    """


def company_history_insert_sql(
    *,
    intervals_stage: str,
    history_stage: str,
) -> str:
    latest_columns = (
        "employer_org_number",
        "publication_at",
        "application_deadline",
        "removed_at",
        "employer_name",
        "headline_original",
        "description_text_original",
        "detected_language",
        "number_of_vacancies",
        "occupation_concept_id",
        "occupation_label_original",
        "occupation_group_concept_id",
        "occupation_group_label_original",
        "occupation_field_concept_id",
        "occupation_field_label_original",
        "employment_type_concept_id",
        "employment_type_label_original",
        "duration_concept_id",
        "duration_label_original",
        "working_hours_concept_id",
        "working_hours_label_original",
        "municipality_code",
        "municipality_name_original",
        "region_code",
        "region_name_original",
        "country_code",
        "country_name_original",
        "webpage_url",
        "source_type",
    )
    latest_projection = ",\n            ".join(
        f"argMax(version.{column}, "
        "tuple(version.version_at, version.ingested_at, version.version_uid)) "
        f"AS {column}"
        for column in latest_columns
    )
    return f"""
    INSERT INTO {history_stage} ({", ".join(COMPANY_JOB_COLUMNS)})
    WITH interval_latest AS (
        SELECT
            interval.source_job_ad_id,
            interval.interval_number,
            interval.active_from,
            interval.active_to,
            interval.active_to_basis,
            interval.is_end_estimated,
            {latest_projection}
        FROM {intervals_stage} AS interval
        INNER JOIN corpscout.se_platsbanken_job_ad_versions AS version FINAL
            ON version.source_job_ad_id = interval.source_job_ad_id
        WHERE (
            version.publication_at IS NOT NULL
            AND version.publication_at >= interval.active_from
            AND (
                interval.active_to IS NULL
                OR version.publication_at < interval.active_to
            )
        ) OR (
            version.version_at >= interval.active_from
            AND (
                interval.active_to IS NULL
                OR version.version_at < interval.active_to
            )
        )
        GROUP BY
            interval.source_job_ad_id,
            interval.interval_number,
            interval.active_from,
            interval.active_to,
            interval.active_to_basis,
            interval.is_end_estimated
    ), company AS (
        SELECT company_id
        FROM corpscout.se_companies FINAL
        WHERE length(company_id) = 10
        GROUP BY company_id
    )
    SELECT
        'SE' AS country_code,
        company.company_id,
        'platsbanken' AS source_system,
        latest.source_job_ad_id,
        latest.interval_number,
        latest.active_from,
        latest.active_to,
        latest.active_to_basis,
        latest.is_end_estimated,
        latest.publication_at,
        latest.application_deadline,
        latest.removed_at,
        latest.employer_name,
        latest.headline_original,
        latest.description_text_original,
        latest.detected_language,
        latest.number_of_vacancies,
        latest.occupation_concept_id,
        latest.occupation_label_original,
        latest.occupation_group_concept_id,
        latest.occupation_group_label_original,
        latest.occupation_field_concept_id,
        latest.occupation_field_label_original,
        latest.employment_type_concept_id,
        latest.employment_type_label_original,
        latest.duration_concept_id,
        latest.duration_label_original,
        latest.working_hours_concept_id,
        latest.working_hours_label_original,
        latest.municipality_code,
        latest.municipality_name_original,
        latest.region_code,
        latest.region_name_original,
        latest.country_code AS workplace_country_code,
        latest.country_name_original AS workplace_country_name_original,
        latest.webpage_url,
        latest.source_type,
        now64(3, 'UTC') AS resolved_at
    FROM interval_latest AS latest
    INNER ANY JOIN company
        ON company.company_id = latest.employer_org_number
    """


def company_current_insert_sql(*, history_stage: str, current_stage: str) -> str:
    return f"""
    INSERT INTO {current_stage} ({", ".join(COMPANY_JOB_COLUMNS)})
    SELECT {", ".join(COMPANY_JOB_COLUMNS)}
    FROM {history_stage}
    WHERE active_to IS NULL
    """


def monthly_insert_sql(*, history_stage: str, monthly_stage: str) -> str:
    return f"""
    INSERT INTO {monthly_stage}
    WITH expanded AS (
        SELECT
            *,
            addMonths(toStartOfMonth(active_from), month_offset) AS month_start
        FROM (
            SELECT
                *,
                arrayJoin(range(toUInt32(greatest(
                    dateDiff(
                        'month',
                        toStartOfMonth(active_from),
                        toStartOfMonth(coalesce(active_to, now64(3, 'UTC')))
                    ) + 1,
                    1
                )))) AS month_offset
            FROM {history_stage}
        )
    )
    SELECT
        country_code,
        company_id,
        toDate(month_start) AS month_start,
        countIf(toStartOfMonth(publication_at) = month_start) AS ads_published,
        sumIf(
            coalesce(number_of_vacancies, 0),
            toStartOfMonth(publication_at) = month_start
        ) AS advertised_positions,
        countIf(
            toStartOfMonth(publication_at) = month_start
            AND number_of_vacancies IS NOT NULL
        ) AS ads_with_known_vacancies,
        countIf(
            active_to IS NOT NULL AND toStartOfMonth(active_to) = month_start
        ) AS ads_closed,
        countIf(
            active_from < addMonths(month_start, 1)
            AND (active_to IS NULL OR active_to >= addMonths(month_start, 1))
        ) AS active_ads_end_of_month,
        sumIf(
            coalesce(number_of_vacancies, 0),
            active_from < addMonths(month_start, 1)
            AND (active_to IS NULL OR active_to >= addMonths(month_start, 1))
        ) AS active_positions_end_of_month,
        countIf(
            number_of_vacancies IS NOT NULL
            AND active_from < addMonths(month_start, 1)
            AND (active_to IS NULL OR active_to >= addMonths(month_start, 1))
        ) AS active_ads_with_known_vacancies,
        if(
            countIf(
                active_to IS NOT NULL AND toStartOfMonth(active_to) = month_start
            ) = 0,
            CAST(NULL AS Nullable(Float64)),
            quantileTDigestIf(0.5)(
                toFloat64(dateDiff('second', active_from, active_to)) / 86400.0,
                active_to IS NOT NULL AND toStartOfMonth(active_to) = month_start
            )
        ) AS median_open_days,
        uniqExactIf(
            occupation_group_concept_id,
            occupation_group_concept_id != ''
        ) AS distinct_occupation_groups,
        now64(3, 'UTC') AS resolved_at
    FROM expanded
    GROUP BY country_code, company_id, month_start
    """


def publish_company_job_projections(
    *,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Atomically republish intervals, company history/current, and trends."""
    targets = (
        tables.INTERVALS_TABLE,
        tables.COMPANY_HISTORY_TABLE,
        tables.COMPANY_CURRENT_TABLE,
        tables.COMPANY_MONTHLY_TABLE,
    )
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(
            tables.VERSIONS_TABLE,
            tables.EVENTS_TABLE,
            *targets,
            "se_companies",
        ),
    )
    stages = {target: f"_tmp_{target}_{uuid.uuid4().hex}" for target in targets}
    exchanged: list[str] = []
    with clickhouse.get_connection() as client:
        try:
            for target in targets:
                client.execute(
                    f"CREATE TABLE {_qualified(stages[target])} AS {_qualified(target)}"
                )
            client.execute(
                intervals_insert_sql(_qualified(stages[tables.INTERVALS_TABLE]))
            )
            client.execute(
                company_history_insert_sql(
                    intervals_stage=_qualified(stages[tables.INTERVALS_TABLE]),
                    history_stage=_qualified(stages[tables.COMPANY_HISTORY_TABLE]),
                )
            )
            client.execute(
                company_current_insert_sql(
                    history_stage=_qualified(stages[tables.COMPANY_HISTORY_TABLE]),
                    current_stage=_qualified(stages[tables.COMPANY_CURRENT_TABLE]),
                )
            )
            client.execute(
                monthly_insert_sql(
                    history_stage=_qualified(stages[tables.COMPANY_HISTORY_TABLE]),
                    monthly_stage=_qualified(stages[tables.COMPANY_MONTHLY_TABLE]),
                )
            )
            counts = {
                target: int(
                    client.execute(f"SELECT count() FROM {_qualified(stages[target])}")[
                        0
                    ][0]
                )
                for target in targets
            }
            for required in (tables.INTERVALS_TABLE, tables.COMPANY_HISTORY_TABLE):
                if counts[required] == 0:
                    raise ValueError(
                        f"Platsbanken projection {required} has zero rows; "
                        "refusing to replace published history"
                    )
            for target in targets:
                client.execute(
                    f"EXCHANGE TABLES {_qualified(stages[target])} AND {_qualified(target)}"
                )
                exchanged.append(target)
        except BaseException as exc:
            rollback_failures: list[str] = []
            for target in reversed(exchanged):
                try:
                    client.execute(
                        f"EXCHANGE TABLES {_qualified(stages[target])} "
                        f"AND {_qualified(target)}"
                    )
                except Exception:
                    rollback_failures.append(target)
            if rollback_failures:
                raise RuntimeError(
                    "Platsbanken projection rollback failed for: "
                    + ", ".join(rollback_failures)
                ) from exc
            raise
        finally:
            for target in reversed(targets):
                client.execute(f"DROP TABLE IF EXISTS {_qualified(stages[target])}")
    if log is not None:
        log("Published Platsbanken company projections: %s", counts)
    return counts


def _qualified(table: str) -> str:
    return f"`{tables.CLICKHOUSE_DATABASE}`.`{table}`"
