import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.sweden_company import tables


def export_sweden_company_clickhouse_companies(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace the Sweden company register ClickHouse table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.SWEDEN_DATABASE,
        tables=(tables.COMPANIES_TABLE_CH,),
    )

    with clickhouse.get_connection() as client:
        if log is not None:
            log(
                "Exporting Sweden company table to ClickHouse: table=%s",
                tables.QUALIFIED_COMPANIES_TABLE,
            )
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.DLT_DATASET_NAME,
            duckdb_table="companies",
            clickhouse_database=tables.SWEDEN_DATABASE,
            clickhouse_table=tables.COMPANIES_TABLE_CH,
            columns=tables.SE_COMPANIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log(
            "Finished Sweden company ClickHouse export: table=%s rows=%s",
            tables.QUALIFIED_COMPANIES_TABLE,
            rows,
        )
    return rows


@dataclass(frozen=True, slots=True)
class SwedenAddressPublishResult:
    address_candidates: int
    address_observations_inserted: int
    first_address_observations: int
    address_changes: int
    address_removals: int


@dataclass(frozen=True, slots=True)
class SnapshotPublishResult:
    candidates: int
    observations_inserted: int
    first_observations: int
    changes: int
    removals: int


def publish_sweden_company_profile_history(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> SnapshotPublishResult:
    """Publish typed Bolagsverket proceedings when they change.

    The source registry states this used to publish beside them
    (se_company_registry_observations / se_company_registry_current) retired with the
    2026-09-03 SE basic-info design: each register source now has its own source table,
    se_scb_companies and se_bolagsverket_companies. Proceedings keep the observation +
    physical-current shape until the proceedings entity's own slice.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.SWEDEN_DATABASE,
        tables=(
            tables.COMPANY_PROCEEDING_OBSERVATIONS_TABLE_CH,
            tables.COMPANY_PROCEEDINGS_CURRENT_TABLE_CH,
        ),
    )
    with clickhouse.get_connection() as client:
        return _publish_changed_snapshot(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_table="company_proceedings",
            history_table=tables.QUALIFIED_COMPANY_PROCEEDING_OBSERVATIONS_TABLE,
            current_table=tables.QUALIFIED_COMPANY_PROCEEDINGS_CURRENT_TABLE,
            current_table_name=tables.COMPANY_PROCEEDINGS_CURRENT_TABLE_CH,
            columns=tables.SE_COMPANY_PROCEEDING_OBSERVATION_COLUMNS,
            key_columns=("company_id", "source", "proceeding_identity"),
            state_fingerprint_column="proceeding_fingerprint",
            presence_column="has_proceeding",
            log=log,
        )


def publish_sweden_company_industry_history(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> SnapshotPublishResult:
    """Publish the complete SCB SNI state when it changes."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.SWEDEN_DATABASE,
        tables=(
            tables.COMPANY_INDUSTRY_OBSERVATIONS_TABLE_CH,
            tables.COMPANY_INDUSTRY_CURRENT_TABLE_CH,
        ),
    )
    with clickhouse.get_connection() as client:
        return _publish_changed_snapshot(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_table="company_industry_states",
            history_table=tables.QUALIFIED_COMPANY_INDUSTRY_OBSERVATIONS_TABLE,
            current_table=tables.QUALIFIED_COMPANY_INDUSTRY_CURRENT_TABLE,
            current_table_name=tables.COMPANY_INDUSTRY_CURRENT_TABLE_CH,
            columns=tables.SE_COMPANY_INDUSTRY_OBSERVATION_COLUMNS,
            key_columns=("company_id", "source"),
            state_fingerprint_column="state_fingerprint",
            presence_column="has_industry",
            log=log,
        )


@dataclass(frozen=True, slots=True)
class SwedenSourceTablePublishResult:
    candidates: int
    inserted: int
    removed: int


_SOURCE_TABLE_TOMBSTONE_COLUMNS = (
    "company_id",
    "company_id_raw",
    "has_company",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "observed_at",
)


def sweden_company_source_anti_join_sql(
    *,
    qualified_stage: str,
    qualified_target: str,
) -> str:
    """The left-anti-join that keeps only the rows whose payload hash differs from the
    company's CURRENT published row.

    Exposed as a function so the clickhouse-local harness executes this exact text rather
    than a paraphrase of it.
    """
    return (
        f"FROM {qualified_stage} AS candidate\n"
        "LEFT ANTI JOIN (\n"
        "    SELECT company_id, argMax(source_payload_hash, observed_at) "
        "AS source_payload_hash\n"
        f"    FROM {qualified_target}\n"
        "    GROUP BY company_id\n"
        ") AS current\n"
        "ON current.company_id = candidate.company_id\n"
        "AND current.source_payload_hash = candidate.source_payload_hash"
    )


def sweden_company_source_removal_sql(
    *,
    qualified_stage: str,
    qualified_target: str,
) -> str:
    """The companies whose CURRENT published row is delivered (has_company = 1) and that
    the staged delivery no longer contains.

    The left side is the current row per company, exactly like the anti-join's right
    side, so a company already tombstoned is not tombstoned again. Exposed as a function
    so the clickhouse-local harness executes this exact text.
    """
    return (
        "FROM (\n"
        "    SELECT company_id, argMax(has_company, observed_at) AS has_company\n"
        f"    FROM {qualified_target}\n"
        "    GROUP BY company_id\n"
        ") AS current\n"
        f"LEFT ANTI JOIN {qualified_stage} AS candidate\n"
        "ON candidate.company_id = current.company_id\n"
        "WHERE current.has_company = 1"
    )


def sweden_company_source_tombstone_insert_sql(
    *,
    qualified_stage: str,
    qualified_target: str,
) -> str:
    """The tombstone row per removed company: has_company 0, every value NULL, an empty
    record id and hash (so a returning company always differs from its tombstone and is
    inserted again), stamped with the staged delivery's source_run_id and observed_at."""
    column_list = ", ".join(_SOURCE_TABLE_TOMBSTONE_COLUMNS)
    removal = sweden_company_source_removal_sql(
        qualified_stage=qualified_stage,
        qualified_target=qualified_target,
    )
    return (
        f"INSERT INTO {qualified_target} ({column_list})\n"
        "SELECT current.company_id, '', 0, "
        f"(SELECT any(source_run_id) FROM {qualified_stage}), '', '', "
        f"(SELECT any(observed_at) FROM {qualified_stage}) {removal}"
    )


def publish_sweden_company_source_table(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    duckdb_table: str,
    clickhouse_table: str,
    columns: tuple[str, ...],
    log: Callable[..., object] | None = None,
) -> SwedenSourceTablePublishResult:
    """Insert only the register rows whose source payload hash changed.

    The target is ``ReplacingMergeTree(observed_at) ORDER BY company_id``: one row per
    company survives a merge, so the anti-join key has to be the CURRENT row per company
    and not every version ever written. ``se_company/common.publish_with_stage`` can
    anti-join its target directly (``new_versions_only=True``) because those targets are
    ordered by ``(company_id, source_record_uid)``, where every version is its own key.
    Here a company that went A -> B -> A would have its third state suppressed by a
    whole-table anti-join and the stale B row would stay current.

    ``_publish_changed_snapshot`` is not reused either: it needs a physical ``*_current``
    twin to diff against and swaps it by rename. The source layer deliberately has no twin
    -- one table per source, and S3 is the archive (2026-09-03 basic-info design, 3.1).

    A company the delivery no longer contains gets a tombstone row (``has_company = 0``,
    NULL values, empty record id and hash) stamped with this delivery's ``source_run_id``
    and ``observed_at``, so ``FINAL ... WHERE has_company = 1`` is the delivered set and a
    returning company is inserted again because its hash differs from the tombstone's
    empty one. The empty-stage refusal above is what keeps a failed load from tombstoning
    the whole register. A partial delivery is not detected: the next full delivery inserts
    again whatever it removed.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.SWEDEN_DATABASE,
        tables=(clickhouse_table,),
    )
    qualified_target = f"{tables.SWEDEN_DATABASE}.{clickhouse_table}"
    stage_table = f"_tmp_{clickhouse_table}_{uuid.uuid4().hex}"
    qualified_stage = f"{tables.SWEDEN_DATABASE}.{stage_table}"
    with clickhouse.get_connection() as client:
        primary_error: BaseException | None = None
        try:
            client.execute(f"CREATE TABLE {qualified_stage} AS {qualified_target}")
            candidates = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=duckdb_connection,
                clickhouse_client=client,
                duckdb_schema=tables.DLT_DATASET_NAME,
                duckdb_table=duckdb_table,
                clickhouse_database=tables.SWEDEN_DATABASE,
                clickhouse_table=stage_table,
                columns=columns,
                truncate=False,
                log=log,
            )
            if candidates == 0:
                raise ValueError(
                    f"DuckDB table {tables.DLT_DATASET_NAME}.{duckdb_table} has 0 rows; "
                    f"refusing to publish {qualified_target}."
                )
            anti_join = sweden_company_source_anti_join_sql(
                qualified_stage=qualified_stage,
                qualified_target=qualified_target,
            )
            inserted = int(
                client.execute(f"SELECT count() AS new_versions {anti_join}")[0][0]
            )
            if inserted > 0:
                column_list = ", ".join(columns)
                selected = ", ".join(f"candidate.{column}" for column in columns)
                client.execute(
                    f"INSERT INTO {qualified_target} ({column_list})\n"
                    f"SELECT {selected} {anti_join}"
                )
            removal = sweden_company_source_removal_sql(
                qualified_stage=qualified_stage,
                qualified_target=qualified_target,
            )
            removed = int(client.execute(f"SELECT count() AS removals {removal}")[0][0])
            if removed > 0:
                client.execute(
                    sweden_company_source_tombstone_insert_sql(
                        qualified_stage=qualified_stage,
                        qualified_target=qualified_target,
                    )
                )
            if log is not None:
                log(
                    "Published Sweden register source table: table=%s candidates=%d "
                    "inserted=%d removed=%d",
                    qualified_target,
                    candidates,
                    inserted,
                    removed,
                )
            return SwedenSourceTablePublishResult(
                candidates=candidates,
                inserted=inserted,
                removed=removed,
            )
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {qualified_stage}")
            except Exception as cleanup_error:
                if primary_error is not None:
                    primary_error.add_note(
                        f"Failed to drop temporary table {qualified_stage}: "
                        f"{cleanup_error}"
                    )
                else:
                    raise


def _publish_changed_snapshot(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    duckdb_table: str,
    history_table: str,
    current_table: str,
    current_table_name: str,
    columns: tuple[str, ...],
    key_columns: tuple[str, ...],
    state_fingerprint_column: str,
    presence_column: str,
    log: Callable[..., object] | None,
) -> SnapshotPublishResult:
    stage_table = f"_tmp_{current_table_name}_{uuid.uuid4().hex}"
    qualified_stage = f"{tables.SWEDEN_DATABASE}.{stage_table}"
    previous_table = f"_tmp_{current_table_name}_previous_{uuid.uuid4().hex}"
    qualified_previous = f"{tables.SWEDEN_DATABASE}.{previous_table}"
    primary_error: BaseException | None = None
    try:
        clickhouse_client.execute(f"CREATE TABLE {qualified_stage} AS {current_table}")
        candidates = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=tables.DLT_DATASET_NAME,
            duckdb_table=duckdb_table,
            clickhouse_database=tables.SWEDEN_DATABASE,
            clickhouse_table=stage_table,
            columns=columns,
            truncate=False,
            log=log,
        )
        first, changes = _changed_snapshot_counts(
            clickhouse_client=clickhouse_client,
            qualified_stage=qualified_stage,
            current_table=current_table,
            key_columns=key_columns,
        )
        removals = _snapshot_removal_count(
            clickhouse_client=clickhouse_client,
            qualified_stage=qualified_stage,
            current_table=current_table,
            key_columns=key_columns,
        )
        if first + changes > 0:
            _insert_changed_snapshot_candidates(
                clickhouse_client=clickhouse_client,
                qualified_stage=qualified_stage,
                history_table=history_table,
                current_table=current_table,
                columns=columns,
                key_columns=key_columns,
            )
        if removals > 0:
            _insert_removed_snapshot_rows(
                clickhouse_client=clickhouse_client,
                qualified_stage=qualified_stage,
                history_table=history_table,
                current_table=current_table,
                columns=columns,
                key_columns=key_columns,
                state_fingerprint_column=state_fingerprint_column,
                presence_column=presence_column,
            )
        _replace_current_snapshot(
            clickhouse_client=clickhouse_client,
            qualified_stage=qualified_stage,
            current_table=current_table,
            qualified_previous=qualified_previous,
        )
        return SnapshotPublishResult(
            candidates=candidates,
            observations_inserted=first + changes + removals,
            first_observations=first,
            changes=changes,
            removals=removals,
        )
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_errors: list[Exception] = []
        for temporary_table in (qualified_stage, qualified_previous):
            try:
                clickhouse_client.execute(f"DROP TABLE IF EXISTS {temporary_table}")
            except Exception as cleanup_error:
                cleanup_errors.append(cleanup_error)
                if primary_error is not None:
                    primary_error.add_note(
                        f"Failed to drop temporary table {temporary_table}: "
                        f"{cleanup_error}"
                    )
        if primary_error is None and cleanup_errors:
            raise cleanup_errors[0]


def _changed_snapshot_counts(
    *,
    clickhouse_client: Any,
    qualified_stage: str,
    current_table: str,
    key_columns: tuple[str, ...],
) -> tuple[int, int]:
    join = _snapshot_join(key_columns)
    rows = clickhouse_client.execute(
        f"""
        SELECT
            countIf(current.has_observation = 0) AS first_observations,
            countIf(
                current.has_observation = 1
                AND current.observation_fingerprint != candidate.observation_fingerprint
            ) AS changed_observations
        FROM {qualified_stage} AS candidate
        LEFT JOIN {current_table} AS current ON {join}
        """
    )
    if not rows:
        return 0, 0
    return int(rows[0][0]), int(rows[0][1])


def _snapshot_removal_count(
    *,
    clickhouse_client: Any,
    qualified_stage: str,
    current_table: str,
    key_columns: tuple[str, ...],
) -> int:
    join = _snapshot_join(key_columns)
    rows = clickhouse_client.execute(
        f"""
        SELECT countIf(candidate.has_observation = 0) AS snapshot_removals
        FROM {current_table} AS current
        LEFT JOIN {qualified_stage} AS candidate ON {join}
        """
    )
    return int(rows[0][0]) if rows else 0


def _insert_changed_snapshot_candidates(
    *,
    clickhouse_client: Any,
    qualified_stage: str,
    history_table: str,
    current_table: str,
    columns: tuple[str, ...],
    key_columns: tuple[str, ...],
) -> None:
    column_list = ", ".join(columns)
    selected = ", ".join(f"candidate.{column}" for column in columns)
    join = _snapshot_join(key_columns)
    clickhouse_client.execute(
        f"""
        INSERT INTO {history_table} ({column_list})
        SELECT {selected}
        FROM {qualified_stage} AS candidate
        LEFT JOIN {current_table} AS current ON {join}
        WHERE current.has_observation = 0
           OR current.observation_fingerprint != candidate.observation_fingerprint
        """
    )


def _insert_removed_snapshot_rows(
    *,
    clickhouse_client: Any,
    qualified_stage: str,
    history_table: str,
    current_table: str,
    columns: tuple[str, ...],
    key_columns: tuple[str, ...],
    state_fingerprint_column: str,
    presence_column: str,
) -> None:
    removed_fingerprint = (
        f"lower(hex(SHA256(concat('removed\\n', current.{state_fingerprint_column}))))"
    )
    selected: list[str] = []
    for column in columns:
        expression = f"current.{column}"
        if column == "updated_from_raw_at" or column == "observed_at":
            expression = "now64(3, 'UTC')"
        elif column == presence_column:
            expression = "toUInt8(0)"
        elif column in (state_fingerprint_column, "observation_fingerprint"):
            expression = removed_fingerprint
        selected.append(expression)
    join = _snapshot_join(key_columns)
    clickhouse_client.execute(
        f"""
        INSERT INTO {history_table} ({", ".join(columns)})
        SELECT {", ".join(selected)}
        FROM {current_table} AS current
        LEFT JOIN {qualified_stage} AS candidate ON {join}
        WHERE candidate.has_observation = 0
        """
    )


def _replace_current_snapshot(
    *,
    clickhouse_client: Any,
    qualified_stage: str,
    current_table: str,
    qualified_previous: str,
) -> None:
    clickhouse_client.execute(
        f"""
        RENAME TABLE
            {current_table} TO {qualified_previous},
            {qualified_stage} TO {current_table}
        """
    )


def _snapshot_join(key_columns: tuple[str, ...]) -> str:
    return " AND ".join(
        f"current.{column} = candidate.{column}" for column in key_columns
    )


def publish_sweden_company_clickhouse_addresses(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> SwedenAddressPublishResult:
    """Append address states that differ from each source's current observation."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.SWEDEN_DATABASE,
        tables=(
            tables.COMPANY_ADDRESSES_TABLE_CH,
            tables.COMPANY_ADDRESSES_CURRENT_TABLE_CH,
        ),
    )

    with clickhouse.get_connection() as client:
        if log is not None:
            log(
                "Publishing Sweden address observations to ClickHouse: table=%s",
                tables.QUALIFIED_COMPANY_ADDRESSES_TABLE,
            )
        counts = _append_changed_address_observations(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            log=log,
        )
    result = SwedenAddressPublishResult(
        address_candidates=counts[0],
        address_observations_inserted=counts[1],
        first_address_observations=counts[2],
        address_changes=counts[3],
        address_removals=counts[4],
    )
    if log is not None:
        log(
            "Finished Sweden address publish: table=%s candidates=%d inserted=%d "
            "first=%d changes=%d removals=%d",
            tables.QUALIFIED_COMPANY_ADDRESSES_TABLE,
            result.address_candidates,
            result.address_observations_inserted,
            result.first_address_observations,
            result.address_changes,
            result.address_removals,
        )
    return result


def _append_changed_address_observations(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    log: Callable[..., object] | None,
) -> tuple[int, int, int, int, int]:
    stage_table = f"_tmp_{tables.COMPANY_ADDRESSES_TABLE_CH}_{uuid.uuid4().hex}"
    qualified_stage = f"{tables.SWEDEN_DATABASE}.{stage_table}"
    previous_current_table = (
        f"_tmp_{tables.COMPANY_ADDRESSES_CURRENT_TABLE_CH}_previous_{uuid.uuid4().hex}"
    )
    qualified_previous_current = f"{tables.SWEDEN_DATABASE}.{previous_current_table}"
    primary_error: BaseException | None = None
    try:
        clickhouse_client.execute(
            f"CREATE TABLE {qualified_stage} AS "
            f"{tables.QUALIFIED_COMPANY_ADDRESSES_CURRENT_TABLE}"
        )
        candidates = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=tables.DLT_DATASET_NAME,
            duckdb_table="company_addresses",
            clickhouse_database=tables.SWEDEN_DATABASE,
            clickhouse_table=stage_table,
            columns=tables.SE_COMPANY_ADDRESS_OBSERVATION_COLUMNS,
            truncate=False,
            log=log,
        )
        inserted, first, changes, removals = _address_change_counts(
            clickhouse_client=clickhouse_client,
            qualified_stage=qualified_stage,
        )
        if inserted > 0:
            _insert_address_changes(
                clickhouse_client=clickhouse_client,
                qualified_stage=qualified_stage,
            )
        _replace_current_address_snapshot(
            clickhouse_client=clickhouse_client,
            qualified_stage=qualified_stage,
            qualified_previous_current=qualified_previous_current,
        )
        return candidates, inserted, first, changes, removals
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_errors: list[Exception] = []
        for temporary_table in (qualified_stage, qualified_previous_current):
            try:
                clickhouse_client.execute(f"DROP TABLE IF EXISTS {temporary_table}")
            except Exception as cleanup_error:
                cleanup_errors.append(cleanup_error)
                if primary_error is not None:
                    primary_error.add_note(
                        "Failed to drop Sweden address temporary table "
                        f"{temporary_table}: {cleanup_error}"
                    )
        if primary_error is None and cleanup_errors:
            raise cleanup_errors[0]


def _address_change_counts(
    *,
    clickhouse_client: Any,
    qualified_stage: str,
) -> tuple[int, int, int, int]:
    rows = clickhouse_client.execute(
        f"""
        SELECT
            countIf(
                current.has_observation = 0
                OR current.observation_fingerprint != candidate.observation_fingerprint
            ) AS address_observations_inserted,
            countIf(current.has_observation = 0) AS first_address_observations,
            countIf(
                current.has_observation = 1
                AND current.observation_fingerprint != candidate.observation_fingerprint
            ) AS address_changes,
            countIf(
                current.has_observation = 1
                AND current.has_address = 1
                AND candidate.has_address = 0
            ) AS address_removals
        FROM {qualified_stage} AS candidate
        LEFT JOIN {tables.QUALIFIED_COMPANY_ADDRESSES_CURRENT_TABLE} AS current
            ON current.company_id = candidate.company_id
           AND current.address_type = candidate.address_type
           AND current.source = candidate.source
        """
    )
    if not rows:
        return 0, 0, 0, 0
    return tuple(int(value) for value in rows[0])


def _insert_address_changes(
    *,
    clickhouse_client: Any,
    qualified_stage: str,
) -> None:
    columns = ", ".join(tables.SE_COMPANY_ADDRESS_OBSERVATION_COLUMNS)
    selected_columns = ", ".join(
        f"candidate.{column}"
        for column in tables.SE_COMPANY_ADDRESS_OBSERVATION_COLUMNS
    )
    clickhouse_client.execute(
        f"""
        INSERT INTO {tables.QUALIFIED_COMPANY_ADDRESSES_TABLE} ({columns})
        SELECT {selected_columns}
        FROM {qualified_stage} AS candidate
        LEFT JOIN {tables.QUALIFIED_COMPANY_ADDRESSES_CURRENT_TABLE} AS current
            ON current.company_id = candidate.company_id
           AND current.address_type = candidate.address_type
           AND current.source = candidate.source
        WHERE current.has_observation = 0
           OR current.observation_fingerprint != candidate.observation_fingerprint
        """
    )


def _replace_current_address_snapshot(
    *,
    clickhouse_client: Any,
    qualified_stage: str,
    qualified_previous_current: str,
) -> None:
    clickhouse_client.execute(
        f"""
        RENAME TABLE
            {tables.QUALIFIED_COMPANY_ADDRESSES_CURRENT_TABLE}
                TO {qualified_previous_current},
            {qualified_stage}
                TO {tables.QUALIFIED_COMPANY_ADDRESSES_CURRENT_TABLE}
        """
    )


def export_sweden_company_clickhouse_industries(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace the Sweden company industries ClickHouse table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.SWEDEN_DATABASE,
        tables=(tables.INDUSTRIES_TABLE_CH,),
    )

    with clickhouse.get_connection() as client:
        if log is not None:
            log(
                "Exporting Sweden company table to ClickHouse: table=%s",
                tables.QUALIFIED_INDUSTRIES_TABLE,
            )
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.DLT_DATASET_NAME,
            duckdb_table="company_industry_codes",
            clickhouse_database=tables.SWEDEN_DATABASE,
            clickhouse_table=tables.INDUSTRIES_TABLE_CH,
            columns=tables.SE_INDUSTRIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log(
            "Finished Sweden company ClickHouse export: table=%s rows=%s",
            tables.QUALIFIED_INDUSTRIES_TABLE,
            rows,
        )
    return rows
