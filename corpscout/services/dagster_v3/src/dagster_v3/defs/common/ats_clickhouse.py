from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)


@dataclass(frozen=True)
class AtsClickhouseTables:
    database: str
    duckdb_schema: str
    boards: str
    board_company_links: str
    board_snapshots: str
    versions: str
    events: str
    current: str
    locations: str
    compensations: str
    columns: Mapping[str, Sequence[str]]

    @property
    def all_tables(self) -> tuple[str, ...]:
        return (
            self.boards,
            self.board_company_links,
            self.board_snapshots,
            self.versions,
            self.events,
            self.current,
            self.locations,
            self.compensations,
        )


def publish_ats_snapshot(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    tables: AtsClickhouseTables,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.database,
        tables=tables.all_tables,
    )
    staged_targets = (
        tables.boards,
        tables.board_company_links,
        tables.board_snapshots,
        tables.versions,
        tables.current,
        tables.locations,
        tables.compensations,
    )
    stage_names = {
        target: f"_tmp_{target}_{uuid.uuid4().hex}" for target in staged_targets
    }
    counts: dict[str, int] = {}
    exchanged: list[str] = []
    event_stage = f"_tmp_{tables.events}_{uuid.uuid4().hex}"

    with clickhouse.get_connection() as client:
        try:
            for target in staged_targets:
                client.execute(
                    f"CREATE TABLE {_qualified(tables.database, stage_names[target])} "
                    f"AS {_qualified(tables.database, target)}"
                )
                counts[f"{target}_staged"] = (
                    export_duckdb_connection_table_to_clickhouse(
                        duckdb_connection=duckdb_connection,
                        clickhouse_client=client,
                        duckdb_schema=tables.duckdb_schema,
                        duckdb_table=target,
                        clickhouse_database=tables.database,
                        clickhouse_table=stage_names[target],
                        columns=tables.columns[target],
                        truncate=False,
                        log=log,
                    )
                )

            _assert_company_links_exist(client, tables=tables, stage_names=stage_names)
            for target, uid_column in (
                (tables.board_snapshots, "snapshot_uid"),
                (tables.versions, "version_uid"),
                (tables.locations, "location_uid"),
                (tables.compensations, "compensation_uid"),
            ):
                counts[f"{target}_inserted"] = _append_new_rows(
                    client,
                    database=tables.database,
                    target=target,
                    stage=stage_names[target],
                    uid_column=uid_column,
                    columns=tables.columns[target],
                )

            client.execute(
                f"CREATE TABLE {_qualified(tables.database, event_stage)} "
                f"AS {_qualified(tables.database, tables.events)}"
            )
            _insert_lifecycle_events(
                client,
                tables=tables,
                stage_names=stage_names,
                event_stage=event_stage,
            )
            counts[f"{tables.events}_inserted"] = _append_new_rows(
                client,
                database=tables.database,
                target=tables.events,
                stage=event_stage,
                uid_column="event_uid",
                columns=tables.columns[tables.events],
            )

            for target in (
                tables.boards,
                tables.board_company_links,
                tables.current,
            ):
                client.execute(
                    f"EXCHANGE TABLES {_qualified(tables.database, stage_names[target])} "
                    f"AND {_qualified(tables.database, target)}"
                )
                exchanged.append(target)
        except BaseException:
            for target in reversed(exchanged):
                client.execute(
                    f"EXCHANGE TABLES {_qualified(tables.database, stage_names[target])} "
                    f"AND {_qualified(tables.database, target)}"
                )
            raise
        finally:
            for stage in (*stage_names.values(), event_stage):
                client.execute(
                    f"DROP TABLE IF EXISTS {_qualified(tables.database, stage)}"
                )
    return counts


def _assert_company_links_exist(
    client: Any,
    *,
    tables: AtsClickhouseTables,
    stage_names: Mapping[str, str],
) -> None:
    qualified_links = _qualified(
        tables.database, stage_names[tables.board_company_links]
    )
    rows = client.execute(
        f"""
        SELECT groupUniqArray(incoming.company_id)
        FROM {qualified_links} AS incoming
        LEFT ANTI JOIN corpscout.se_companies AS company FINAL
            ON company.company_id = incoming.company_id
        """
    )
    missing = tuple(str(value) for value in rows[0][0]) if rows else ()
    if missing:
        raise ValueError(
            "Reviewed ATS board links reference missing Sweden companies: "
            + ", ".join(sorted(missing))
        )


def _append_new_rows(
    client: Any,
    *,
    database: str,
    target: str,
    stage: str,
    uid_column: str,
    columns: Sequence[str],
) -> int:
    qualified_target = _qualified(database, target)
    qualified_stage = _qualified(database, stage)
    join = f"""
        FROM {qualified_stage} AS incoming
        LEFT ANTI JOIN {qualified_target} AS existing FINAL
            ON existing.`{uid_column}` = incoming.`{uid_column}`
    """
    [(row_count,)] = client.execute(f"SELECT count() {join}")
    selected = ", ".join(f"incoming.`{column}`" for column in columns)
    inserted = ", ".join(f"`{column}`" for column in columns)
    client.execute(
        f"INSERT INTO {qualified_target} ({inserted}) SELECT {selected} {join}"
    )
    return int(row_count)


def _insert_lifecycle_events(
    client: Any,
    *,
    tables: AtsClickhouseTables,
    stage_names: Mapping[str, str],
    event_stage: str,
) -> None:
    incoming = _qualified(tables.database, stage_names[tables.current])
    snapshots = _qualified(tables.database, stage_names[tables.board_snapshots])
    current = _qualified(tables.database, tables.current)
    events = _qualified(tables.database, tables.events)
    stage = _qualified(tables.database, event_stage)
    client.execute(
        f"""
        INSERT INTO {stage}
        WITH closed_before AS (
            SELECT DISTINCT provider_board_id, source_job_ad_id
            FROM {events} FINAL
            WHERE event_type = 'closed_by_absence'
        )
        SELECT
            hex(SHA256(concat_ws('|', incoming.provider_board_id,
                incoming.source_job_ad_id,
                if(closed_before.source_job_ad_id = '', 'first_seen', 'reopened'),
                snapshot.snapshot_uid))) AS event_uid,
            incoming.provider_board_id,
            incoming.source_job_ad_id,
            incoming.company_id,
            snapshot.retrieved_at AS event_at,
            snapshot.retrieved_at AS effective_at,
            if(closed_before.source_job_ad_id = '', 'first_seen', 'reopened')
                AS event_type,
            toUInt8(1) AS is_active,
            toUInt8(0) AS is_estimated,
            snapshot.source_run_id,
            snapshot.retrieved_at
        FROM {incoming} AS incoming
        LEFT ANTI JOIN {current} AS previous
            ON previous.provider_board_id = incoming.provider_board_id
           AND previous.source_job_ad_id = incoming.source_job_ad_id
        LEFT JOIN closed_before
            ON closed_before.provider_board_id = incoming.provider_board_id
           AND closed_before.source_job_ad_id = incoming.source_job_ad_id
        INNER JOIN {snapshots} AS snapshot
            ON snapshot.provider_board_id = incoming.provider_board_id

        UNION ALL

        SELECT
            hex(SHA256(concat_ws('|', incoming.provider_board_id,
                incoming.source_job_ad_id, 'content_changed', snapshot.snapshot_uid)))
                AS event_uid,
            incoming.provider_board_id,
            incoming.source_job_ad_id,
            incoming.company_id,
            snapshot.retrieved_at AS event_at,
            snapshot.retrieved_at AS effective_at,
            'content_changed' AS event_type,
            toUInt8(1) AS is_active,
            toUInt8(0) AS is_estimated,
            snapshot.source_run_id,
            snapshot.retrieved_at
        FROM {incoming} AS incoming
        INNER JOIN {current} AS previous
            ON previous.provider_board_id = incoming.provider_board_id
           AND previous.source_job_ad_id = incoming.source_job_ad_id
           AND previous.content_hash != incoming.content_hash
        INNER JOIN {snapshots} AS snapshot
            ON snapshot.provider_board_id = incoming.provider_board_id

        UNION ALL

        SELECT
            hex(SHA256(concat_ws('|', previous.provider_board_id,
                previous.source_job_ad_id, 'closed_by_absence',
                snapshot.snapshot_uid)))
                AS event_uid,
            previous.provider_board_id,
            previous.source_job_ad_id,
            previous.company_id,
            snapshot.retrieved_at AS event_at,
            snapshot.retrieved_at AS effective_at,
            'closed_by_absence' AS event_type,
            toUInt8(0) AS is_active,
            toUInt8(1) AS is_estimated,
            snapshot.source_run_id,
            snapshot.retrieved_at
        FROM {current} AS previous
        LEFT ANTI JOIN {incoming} AS incoming
            ON incoming.provider_board_id = previous.provider_board_id
           AND incoming.source_job_ad_id = previous.source_job_ad_id
        INNER JOIN {snapshots} AS snapshot
            ON snapshot.provider_board_id = previous.provider_board_id
        """
    )


def _qualified(database: str, table: str) -> str:
    return f"`{database}`.`{table}`"
