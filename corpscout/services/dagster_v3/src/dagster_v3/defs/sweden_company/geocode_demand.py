"""What the weekly Sweden resolver run is allowed to match -- and nothing else.

The resolver used to score every one of the ~2.09M address identities every Tuesday. An
address_id is a fingerprint of normalized address text, so the text cannot change under it:
for an unchanged matcher and an unchanged OSM snapshot that work reproduces answers the
store already holds. This module computes the set that is genuinely due.

THE RULE, over each identity's CURRENT RESOLVER OUTCOME (geocode_store's stage 1 restricted
to the resolver family -- an imported legacy_adopted_v1 row is not a resolver answer and
never enters here):

  rematch_all         the operator asked for it, explicitly, in the run config
  no_outcome          the identity has no resolver outcome at all -- register churn
  policy_changed      the stored outcome was produced by a different policy version.
                      A policy bump therefore IS a full rematch, and it routes through the
                      golden corpus gate that already sits upstream of the shadow.
  reference_changed   the stored outcome did not geocode AND was computed against a
                      different OSM snapshot than the one this run holds -- the retry pool.

A GEOCODED outcome at a stale reference is deliberately NOT retried. That is where the whole
saving lives: a Geofabrik refresh costs the non-geocoded population (hundreds of thousands),
not the whole universe. An operator who wants the geocoded population re-examined against a
new snapshot passes rematch_all, which is loud and deliberate.

WHY THE FRESH MD5 COMES OUT OF THE DUCKDB REFERENCE TABLE and not from the OSM asset's
Dagster metadata: the promotion stamps reference_md5 from exactly this expression
(`first(source_md5 order by source_record_id)` over sweden_address_osm.address_points), so
reading it the same way here makes it impossible for the demand scan and the stamp to
disagree about which snapshot this run is matching against.
"""

import time
from collections.abc import Callable, Iterator, Sequence
from typing import Any

from dagster_v3.defs.sweden_address_osm import tables as osm_tables
from dagster_v3.defs.sweden_company import shared_addresses
from dagster_v3.defs.sweden_company.geocode_store import (
    ENRICHMENT_SCHEMA,
    GEOCODED_STATUSES,
    QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE,
    StoredOutcome,
    build_current_resolver_geocodes_sql,
    is_geocoded,
)

PENDING_IDENTITIES_TABLE = "se_address_pending_identities"
QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE = (
    f"{ENRICHMENT_SCHEMA}.{PENDING_IDENTITIES_TABLE}"
)
PENDING_REASONS = (
    "rematch_all",
    "no_outcome",
    "policy_changed",
    "reference_changed",
)
# Everything the demand rule and the two repointed readers (the shadow's comparison and the
# promotion's postcode-conflict gate) need from a previous outcome. Deliberately narrow: the
# whole store's 28 columns for 2.09M identities is not worth streaming.
PREVIOUS_OUTCOME_COLUMNS = (
    "address_id",
    "policy_version",
    "reference_md5",
    "match_status",
    "match_method",
    "match_confidence",
    "candidate_record_ids",
    "matched_at",
)
QUERY_BATCH_SIZE = 100_000
PROGRESS_LOG_ROW_INTERVAL = 500_000

PREVIOUS_OUTCOMES_SQL = build_current_resolver_geocodes_sql(
    columns=PREVIOUS_OUTCOME_COLUMNS
)


def pending_reason(
    outcome: StoredOutcome | None,
    *,
    policy_version: str,
    reference_md5: str,
    rematch_all: bool,
) -> str:
    """The Python twin of the CASE in replace_pending_address_identities."""
    if rematch_all:
        return "rematch_all"
    if outcome is None:
        return "no_outcome"
    if outcome.policy_version != policy_version:
        return "policy_changed"
    if not is_geocoded(outcome.match_status) and outcome.reference_md5 != reference_md5:
        return "reference_changed"
    return ""


def fresh_reference_md5(connection: Any) -> str:
    """The OSM snapshot identity this run holds, read exactly as the promotion stamps it."""
    [(reference_md5,)] = connection.execute(
        f"""
        select coalesce(first(source_md5 order by source_record_id), '')
        from {osm_tables.QUALIFIED_ADDRESS_TABLE}
        """
    ).fetchall()
    if not str(reference_md5):
        raise ValueError(
            "The Sweden OSM reference table carries no snapshot MD5 -- refusing to "
            "compute matching demand against an unidentifiable reference"
        )
    return str(reference_md5)


def load_current_resolver_outcomes(
    *,
    connection: Any,
    clickhouse_client: Any,
    log: Callable[..., object] | None = None,
) -> int:
    """Stream the store's current resolver outcome per identity into DuckDB."""
    connection.execute(f"create schema if not exists {ENRICHMENT_SCHEMA}")
    connection.execute(
        f"""
        create or replace table {QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE} (
            address_id varchar,
            policy_version varchar,
            reference_md5 varchar,
            match_status varchar,
            match_method varchar,
            match_confidence double,
            candidate_record_ids varchar[],
            matched_at timestamptz
        )
        """
    )
    started_at = time.monotonic()
    loaded_rows = 0
    batch: list[Sequence[object]] = []
    for row in _iter_clickhouse_rows(clickhouse_client):
        batch.append(row)
        if len(batch) < QUERY_BATCH_SIZE:
            continue
        _insert_previous_outcome_batch(connection, batch)
        loaded_rows += len(batch)
        batch.clear()
        if loaded_rows % PROGRESS_LOG_ROW_INTERVAL == 0:
            _log(
                log,
                "Loading current Sweden resolver outcomes: rows=%d elapsed_seconds=%.1f",
                loaded_rows,
                time.monotonic() - started_at,
            )
    _insert_previous_outcome_batch(connection, batch)
    loaded_rows += len(batch)
    _log(
        log,
        "Loaded current Sweden resolver outcomes: rows=%d elapsed_seconds=%.1f",
        loaded_rows,
        time.monotonic() - started_at,
    )
    return loaded_rows


def replace_pending_address_identities(
    *,
    connection: Any,
    policy_version: str,
    reference_md5: str,
    rematch_all: bool,
    log: Callable[..., object] | None = None,
) -> dict[str, object]:
    """Build the identity set this run will match, with the reason for each."""
    started_at = time.monotonic()
    connection.execute(
        f"""
        create or replace table {QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE} as
        select address_id, pending_reason
        from (
            select
                cast(address.address_id as varchar) as address_id,
                case
                    when ?::boolean then 'rematch_all'
                    when previous.address_id is null then 'no_outcome'
                    when previous.policy_version != ?::varchar then 'policy_changed'
                    when previous.match_status not in ({_quoted(GEOCODED_STATUSES)})
                     and previous.reference_md5 != ?::varchar then 'reference_changed'
                    else ''
                end as pending_reason
            from {shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE} address
            left join {QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE} previous
                on previous.address_id = cast(address.address_id as varchar)
        ) candidates
        where pending_reason != ''
        """,
        [rematch_all, policy_version, reference_md5],
    )
    reason_counts = {
        str(reason): int(count)
        for reason, count in connection.execute(
            f"""
            select pending_reason, count(*)
            from {QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE}
            group by pending_reason
            order by pending_reason
            """
        ).fetchall()
    }
    pending = pending_identity_count(connection)
    _log(
        log,
        "Sweden geocoding demand: pending=%d policy=%s reference=%s elapsed_seconds=%.1f",
        pending,
        policy_version,
        reference_md5,
        time.monotonic() - started_at,
    )
    return {
        "pending_identities": pending,
        "reason_counts": reason_counts,
        "policy_version": policy_version,
        "reference_md5": reference_md5,
        "rematch_all": rematch_all,
        "short_circuit": pending == 0,
    }


def pending_identity_count(connection: Any) -> int:
    [(count,)] = connection.execute(
        f"select count(*) from {QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE}"
    ).fetchall()
    return int(count)


def _iter_clickhouse_rows(clickhouse_client: Any) -> Iterator[Sequence[object]]:
    execute_iter = getattr(clickhouse_client, "execute_iter", None)
    if callable(execute_iter):
        yield from execute_iter(
            PREVIOUS_OUTCOMES_SQL,
            settings={"max_block_size": QUERY_BATCH_SIZE},
        )
        return
    yield from clickhouse_client.execute(PREVIOUS_OUTCOMES_SQL)


def _insert_previous_outcome_batch(
    connection: Any,
    rows: Sequence[Sequence[object]],
) -> None:
    if not rows:
        return
    connection.executemany(
        f"insert into {QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE} "
        "values (?, ?, ?, ?, ?, ?, ?, ?)",
        [list(row) for row in rows],
    )


def _quoted(values: Sequence[str]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _log(log: Callable[..., object] | None, message: str, *args: object) -> None:
    if log is not None:
        log(message, *args)
