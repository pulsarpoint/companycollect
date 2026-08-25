r"""Standalone Sweden address-matcher experiment harness (no Dagster).

Runs the REAL Sweden address resolver (``replace_sweden_address_resolution_shadow``
and the shared ``address_resolution`` engine) against a LOCAL persistent DuckDB
workbench pulled read-only from prod ClickHouse, so a candidate matcher policy can be
measured against the current v6 baseline on the currently-unmatched pool in minutes --
with no Dagster run, no writes to the geocode store, and no reimplementation of the
matching logic.

What it does
------------
1. Builds/refreshes a local DuckDB workbench (``data/geocode_workbench_local.duckdb``,
   gitignored) by SELECT-only pulls from ClickHouse ``corpscout``:
     - ``sweden_company_enrichment.se_addresses_current``  <- the currently-UNMATCHED
       pool of ``corpscout.se_addresses_current`` rows (the identities whose latest
       servable ``se_address_geocodes`` outcome is NOT in ``GEOCODED_STATUSES``).
     - ``sweden_address_osm.address_points``   <- ``corpscout.se_osm_address_points``.
     - ``sweden_address_osm.street_segments``  <- ``corpscout.se_osm_street_segments``.
     - ``sweden_company_enrichment.se_address_geocodes_previous`` <- empty (we measure
       fresh matches, not diffs).
   The pull is cached; re-runs skip it unless ``--refresh`` is passed.
2. Runs the matcher TWICE against the workbench, snapshotting the shadow result table
   each time:
     - BASELINE: current policy + v6 glued street-suffix expansions.
     - CANDIDATE: same policy + a candidate street-suffix map (default v7, which adds
       PUNCTUATED trailing suffixes ``STAVSTENSV.`` -> ``STAVSTENSVÄGEN`` on top of v6's
       glued ``STAVSTENSV`` -> ``STAVSTENSVÄGEN``).
   The candidate variant map is a PARAMETER of the harness, injected by rebinding the
   module global the shadow reads -- the production resolver file is never edited and
   stays on v6.
3. Reports yield: N pending, baseline-matched vs candidate-matched, the DELTA
   (newly-matched under the candidate), a breakdown by resolution_status, and sample
   newly-matched address_ids with their street and the OSM object they matched.

Run::

    uv run python scripts/geocode_workbench_experiment.py --help
    uv run python scripts/geocode_workbench_experiment.py            # full pool
    uv run python scripts/geocode_workbench_experiment.py --limit 40000
    uv run python scripts/geocode_workbench_experiment.py --street-regex '(?i)(v|g|gr)\.$'

Credentials come from ``.env`` (``CLICKHOUSE_HOST`` / ``_HTTP_PORT`` / ``_USER`` /
``_PASSWORD`` / ``_DATABASE``); ClickHouse access is SELECT-only.
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_SERVICE_ROOT = Path(__file__).resolve().parents[1]
if str(_SERVICE_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_SERVICE_ROOT / "src"))

import duckdb
from dotenv import load_dotenv

from dagster_v3.defs.sweden_company import (
    address_resolution_shadow as shadow_mod,
)
from dagster_v3.defs.sweden_company import geocode_demand, shared_addresses
from dagster_v3.defs.sweden_company.address_canonicalization import ENRICHMENT_SCHEMA
from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
    SWEDEN_STREET_SUFFIX_EXPANSIONS,
)
from dagster_v3.defs.sweden_company.address_resolution_shadow import (
    QUALIFIED_SHADOW_RESULTS_TABLE,
    replace_sweden_address_resolution_shadow,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    GEOCODED_STATUSES,
    build_current_geocodes_sql,
)
from dagster_v3.defs.sweden_address_osm import tables as osm_tables

WORKBENCH_PATH = _SERVICE_ROOT / "data" / "geocode_workbench_local.duckdb"

# The candidate (v7) street-suffix map: v6's glued abbreviations PLUS their punctuated
# trailing forms. Because the last-token expansion already keys on ``endswith(suffix)``
# and strips the abbreviation off the stem, carrying the trailing ``.`` in the key is
# all that is needed for ``STAVSTENSV.`` -> ``STAVSTENSVÄGEN`` -- no resolver code change.
CANDIDATE_V7_PUNCTUATED_SUFFIX_EXPANSIONS: dict[str, dict[str, str]] = {
    country: {
        **glued,
        **{f"{abbrev}.": expansion for abbrev, expansion in glued.items()},
    }
    for country, glued in SWEDEN_STREET_SUFFIX_EXPANSIONS.items()
}

_QUALIFIED_SHARED = shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE
_QUALIFIED_PENDING = geocode_demand.QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE
_QUALIFIED_ADDRESS_POINTS = osm_tables.QUALIFIED_ADDRESS_TABLE
_QUALIFIED_STREET_SEGMENTS = osm_tables.QUALIFIED_STREET_SEGMENT_TABLE

_GEOCODED_SET_SQL = ", ".join(f"'{status}'" for status in GEOCODED_STATUSES)


def _log(message: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)


# --------------------------------------------------------------------------- #
# ClickHouse -> DuckDB workbench load                                          #
# --------------------------------------------------------------------------- #


def _clickhouse_client() -> Any:
    import os

    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ.get("CLICKHOUSE_DATABASE", "corpscout"),
    )


def _load_table(
    *,
    connection: Any,
    clickhouse_client: Any,
    select_sql: str,
    target_table: str,
) -> int:
    started = time.monotonic()
    arrow_table = clickhouse_client.query_arrow(select_sql)
    view = "_ch_pull_arrow"
    connection.register(view, arrow_table)
    try:
        connection.execute(
            f"create or replace table {target_table} as select * from {view}"
        )
    finally:
        connection.unregister(view)
    [(count,)] = connection.execute(
        f"select count(*) from {target_table}"
    ).fetchall()
    _log(
        f"loaded {target_table}: rows={int(count)} "
        f"elapsed={time.monotonic() - started:.1f}s"
    )
    return int(count)


def _unmatched_pool_addresses_sql() -> str:
    """Pool rows of se_addresses_current whose current servable outcome is not geocoded.

    ``build_current_geocodes_sql`` is geocode_store's own two-stage servable read; we
    keep the identities whose chosen outcome falls outside ``GEOCODED_STATUSES`` and
    inner-join back to ``se_addresses_current`` so the pool is always a subset of the
    canonical identities the resolver can build a query document for.
    """
    servable = build_current_geocodes_sql(columns=["address_id", "match_status"])
    return f"""
        SELECT
            toString(a.address_id) AS address_id,
            a.canonical_display_address AS canonical_display_address,
            a.street_address AS street_address,
            a.street_name AS street_name,
            a.house_number AS house_number,
            a.unit AS unit,
            a.postal_code AS postal_code,
            a.post_town AS post_town,
            toString(a.country_code) AS country_code,
            toString(a.address_kind) AS address_kind
        FROM (
            SELECT address_id
            FROM (
{servable}
            ) AS servable
            WHERE match_status NOT IN ({_GEOCODED_SET_SQL})
        ) AS unmatched
        INNER JOIN corpscout.se_addresses_current a
            ON a.address_id = unmatched.address_id
    """


def _address_points_sql() -> str:
    return """
        SELECT
            toString(source_record_id) AS source_record_id,
            toString(country_code) AS country_code,
            street,
            house_number,
            unit,
            postcode,
            city,
            place,
            full_address,
            longitude,
            latitude,
            toString(source_record_url) AS source_record_url
        FROM corpscout.se_osm_address_points
    """


def _street_segments_sql() -> str:
    return """
        SELECT
            toString(source_record_id) AS source_record_id,
            street,
            latitude,
            longitude,
            toString(source_record_url) AS source_record_url
        FROM corpscout.se_osm_street_segments
    """


def _workbench_is_populated(connection: Any) -> bool:
    for table in (
        _QUALIFIED_SHARED,
        _QUALIFIED_ADDRESS_POINTS,
        _QUALIFIED_STREET_SEGMENTS,
    ):
        try:
            [(count,)] = connection.execute(
                f"select count(*) from {table}"
            ).fetchall()
        except duckdb.CatalogException:
            return False
        if int(count) == 0:
            return False
    return True


def refresh_workbench(connection: Any, *, refresh: bool) -> None:
    connection.execute(f"create schema if not exists {ENRICHMENT_SCHEMA}")
    connection.execute(
        f"create schema if not exists {osm_tables.DUCKDB_SCHEMA}"
    )
    if not refresh and _workbench_is_populated(connection):
        [(pool,)] = connection.execute(
            f"select count(*) from {_QUALIFIED_SHARED}"
        ).fetchall()
        _log(
            f"workbench already populated (unmatched pool={int(pool)}); "
            "skipping ClickHouse pull (use --refresh to re-pull)"
        )
        return
    _log("pulling workbench tables from ClickHouse (SELECT only)")
    clickhouse_client = _clickhouse_client()
    _load_table(
        connection=connection,
        clickhouse_client=clickhouse_client,
        select_sql=_unmatched_pool_addresses_sql(),
        target_table=_QUALIFIED_SHARED,
    )
    _load_table(
        connection=connection,
        clickhouse_client=clickhouse_client,
        select_sql=_address_points_sql(),
        target_table=_QUALIFIED_ADDRESS_POINTS,
    )
    _load_table(
        connection=connection,
        clickhouse_client=clickhouse_client,
        select_sql=_street_segments_sql(),
        target_table=_QUALIFIED_STREET_SEGMENTS,
    )


# --------------------------------------------------------------------------- #
# Pending-pool selection + running the matcher                                #
# --------------------------------------------------------------------------- #


def build_pending_identities(
    connection: Any,
    *,
    street_regex: str | None,
    limit: int | None,
) -> int:
    """The identities this experiment matches: the unmatched pool, optionally scoped."""
    connection.execute(f"create schema if not exists {ENRICHMENT_SCHEMA}")
    filters = ["street_name is not null"]
    if street_regex is not None:
        escaped = street_regex.replace("'", "''")
        filters.append(f"regexp_matches(street_name, '{escaped}')")
    where = " and ".join(filters)
    limit_sql = f"order by address_id limit {int(limit)}" if limit else ""
    connection.execute(
        f"""
        create or replace table {_QUALIFIED_PENDING} as
        select
            cast(address_id as varchar) as address_id,
            'unmatched_pool'::varchar as pending_reason
        from {_QUALIFIED_SHARED}
        where {where}
        {limit_sql}
        """
    )
    return geocode_demand.pending_identity_count(connection)


def _snapshot_results(connection: Any, *, label: str) -> str:
    target = f"{ENRICHMENT_SCHEMA}.se_experiment_results_{label}"
    connection.execute(
        f"create or replace table {target} as "
        f"select * from {QUALIFIED_SHADOW_RESULTS_TABLE}"
    )
    return target


def run_matcher(
    connection: Any,
    *,
    label: str,
    suffix_expansions: Mapping[str, Mapping[str, str]],
    policy: Any = SWEDEN_ADDRESS_RESOLUTION_POLICY,
) -> tuple[str, dict[str, object]]:
    """Run the REAL shadow resolver once, with the given street-suffix map injected.

    The map (and policy) are injected by rebinding the module globals the shadow reads
    -- the production module file is never modified, so it stays on v6.
    """
    geocode_demand.create_empty_previous_outcomes_table(connection)
    original_suffix = shadow_mod.SWEDEN_STREET_SUFFIX_EXPANSIONS
    original_policy = shadow_mod.SWEDEN_ADDRESS_RESOLUTION_POLICY
    shadow_mod.SWEDEN_STREET_SUFFIX_EXPANSIONS = suffix_expansions
    shadow_mod.SWEDEN_ADDRESS_RESOLUTION_POLICY = policy
    started = time.monotonic()
    try:
        summary = replace_sweden_address_resolution_shadow(
            connection=connection,
            evaluation_run_id=f"experiment-{label}-{uuid.uuid4().hex[:8]}",
            evaluated_at=datetime.now(timezone.utc),
            log=_log,
        )
    finally:
        shadow_mod.SWEDEN_STREET_SUFFIX_EXPANSIONS = original_suffix
        shadow_mod.SWEDEN_ADDRESS_RESOLUTION_POLICY = original_policy
    snapshot = _snapshot_results(connection, label=label)
    _log(
        f"run '{label}' finished in {time.monotonic() - started:.1f}s "
        f"-> {snapshot}"
    )
    return snapshot, summary


# --------------------------------------------------------------------------- #
# Reporting                                                                    #
# --------------------------------------------------------------------------- #


def _status_counts(connection: Any, table: str) -> list[tuple[str, int]]:
    rows = connection.execute(
        f"""
        select resolution_status, count(*)
        from {table}
        group by resolution_status
        order by count(*) desc
        """
    ).fetchall()
    return [(str(status), int(count)) for status, count in rows]


def _matched_count(connection: Any, table: str) -> int:
    [(count,)] = connection.execute(
        f"""
        select count(*)
        from {table}
        where resolution_status in ({_GEOCODED_SET_SQL})
        """
    ).fetchall()
    return int(count)


def _print_table(title: str, header: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    widths = [len(str(h)) for h in header]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(str(cell)))
    print(f"\n{title}")
    print("  " + "  ".join(str(h).ljust(widths[index]) for index, h in enumerate(header)))
    print("  " + "  ".join("-" * widths[index] for index in range(len(header))))
    for row in rows:
        print(
            "  "
            + "  ".join(str(cell).ljust(widths[index]) for index, cell in enumerate(row))
        )


def report(
    connection: Any,
    *,
    baseline_table: str,
    candidate_table: str,
    pending: int,
) -> dict[str, object]:
    baseline_matched = _matched_count(connection, baseline_table)
    candidate_matched = _matched_count(connection, candidate_table)

    newly_matched = connection.execute(
        f"""
        create or replace table {ENRICHMENT_SCHEMA}.se_experiment_newly_matched as
        select
            candidate.query_document_id as address_id,
            candidate.resolution_status as candidate_status,
            candidate.geocode_precision as candidate_precision,
            candidate.match_strategy as candidate_strategy,
            candidate.match_confidence as candidate_confidence,
            candidate.corrections as candidate_corrections,
            candidate.matched_street_name,
            candidate.matched_house_number,
            candidate.matched_postal_code,
            candidate.matched_locality,
            candidate.candidate_record_ids,
            baseline.resolution_status as baseline_status
        from {candidate_table} candidate
        inner join {baseline_table} baseline
            on baseline.query_document_id = candidate.query_document_id
        where candidate.resolution_status in ({_GEOCODED_SET_SQL})
          and baseline.resolution_status not in ({_GEOCODED_SET_SQL})
        """
    )
    [(newly,)] = connection.execute(
        f"select count(*) from {ENRICHMENT_SCHEMA}.se_experiment_newly_matched"
    ).fetchall()
    newly = int(newly)

    # Any regressions (candidate loses a match the baseline had) -- expected zero for a
    # pure superset of variants, but verify rather than assume.
    [(regressed,)] = connection.execute(
        f"""
        select count(*)
        from {candidate_table} candidate
        inner join {baseline_table} baseline
            on baseline.query_document_id = candidate.query_document_id
        where baseline.resolution_status in ({_GEOCODED_SET_SQL})
          and candidate.resolution_status not in ({_GEOCODED_SET_SQL})
        """
    ).fetchall()
    regressed = int(regressed)

    print("\n" + "=" * 72)
    print("YIELD SUMMARY (baseline v6 vs candidate, on the unmatched pool)")
    print("=" * 72)
    _print_table(
        "Headline",
        ["metric", "count", "% of pending"],
        [
            ["pending identities (N)", pending, "100.0%"],
            [
                "baseline v6 matched",
                baseline_matched,
                f"{100.0 * baseline_matched / pending:.2f}%" if pending else "-",
            ],
            [
                "candidate matched",
                candidate_matched,
                f"{100.0 * candidate_matched / pending:.2f}%" if pending else "-",
            ],
            [
                "DELTA newly matched",
                f"+{newly}",
                f"+{100.0 * newly / pending:.3f}%" if pending else "-",
            ],
            ["regressions (matched->unmatched)", regressed, "-"],
        ],
    )

    _print_table(
        "Baseline v6 resolution_status",
        ["resolution_status", "count"],
        [[status, count] for status, count in _status_counts(connection, baseline_table)],
    )
    _print_table(
        "Candidate resolution_status",
        ["resolution_status", "count"],
        [[status, count] for status, count in _status_counts(connection, candidate_table)],
    )

    newly_by_status = connection.execute(
        f"""
        select candidate_status, count(*)
        from {ENRICHMENT_SCHEMA}.se_experiment_newly_matched
        group by candidate_status
        order by count(*) desc
        """
    ).fetchall()
    _print_table(
        "Newly-matched breakdown (candidate resolution_status)",
        ["candidate_status", "count"],
        [[str(status), int(count)] for status, count in newly_by_status],
    )

    samples = connection.execute(
        f"""
        select
            substr(matched.address_id, 1, 12) as address_id,
            shared.street_name as query_street,
            shared.postal_code as query_postcode,
            matched.candidate_status,
            matched.matched_street_name as osm_street,
            matched.matched_postal_code as osm_postcode,
            coalesce(matched.candidate_record_ids[1], '') as osm_record
        from {ENRICHMENT_SCHEMA}.se_experiment_newly_matched matched
        inner join {_QUALIFIED_SHARED} shared
            on cast(shared.address_id as varchar) = matched.address_id
        where regexp_matches(shared.street_name, '(?i)(v|g|gr)\\.\\s*$')
        order by matched.address_id
        limit 20
        """
    ).fetchall()
    if not samples:
        samples = connection.execute(
            f"""
            select
                substr(matched.address_id, 1, 12) as address_id,
                shared.street_name as query_street,
                shared.postal_code as query_postcode,
                matched.candidate_status,
                matched.matched_street_name as osm_street,
                matched.matched_postal_code as osm_postcode,
                coalesce(matched.candidate_record_ids[1], '') as osm_record
            from {ENRICHMENT_SCHEMA}.se_experiment_newly_matched matched
            inner join {_QUALIFIED_SHARED} shared
                on cast(shared.address_id as varchar) = matched.address_id
            order by matched.address_id
            limit 20
            """
        ).fetchall()
    _print_table(
        "Sample newly-matched addresses (query street -> OSM object)",
        [
            "address_id",
            "query_street",
            "postcode",
            "status",
            "osm_street",
            "osm_postcode",
            "osm_record",
        ],
        [[str(cell) for cell in row] for row in samples],
    )

    return {
        "pending": pending,
        "baseline_matched": baseline_matched,
        "candidate_matched": candidate_matched,
        "newly_matched": newly,
        "regressions": regressed,
        "samples": samples,
    }


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the real Sweden address matcher against a local DuckDB workbench and "
            "diff a candidate street-suffix policy (default v7-punctuated) against the "
            "v6 baseline on the currently-unmatched pool."
        )
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-pull the workbench tables from ClickHouse even if cached locally.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap the pending pool to the first N identities (by address_id).",
    )
    parser.add_argument(
        "--street-regex",
        type=str,
        default=None,
        help=(
            "Only match pending identities whose street_name matches this DuckDB "
            "regexp (e.g. '(?i)(v|g|gr)\\.\\s*$' for punctuated suffixes)."
        ),
    )
    parser.add_argument(
        "--workbench",
        type=Path,
        default=WORKBENCH_PATH,
        help=f"Path to the local DuckDB workbench (default {WORKBENCH_PATH}).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    load_dotenv(_SERVICE_ROOT / ".env")
    load_dotenv(_SERVICE_ROOT.parent / "backoffice" / ".env")

    args.workbench.parent.mkdir(parents=True, exist_ok=True)
    _log(f"workbench: {args.workbench}")
    connection = duckdb.connect(str(args.workbench))
    try:
        refresh_workbench(connection, refresh=args.refresh)
        pending = build_pending_identities(
            connection,
            street_regex=args.street_regex,
            limit=args.limit,
        )
        _log(f"pending identities to match: {pending}")
        if pending == 0:
            _log("no pending identities selected -- nothing to match")
            return 1

        baseline_table, baseline_summary = run_matcher(
            connection,
            label="baseline_v6",
            suffix_expansions=SWEDEN_STREET_SUFFIX_EXPANSIONS,
        )
        candidate_table, candidate_summary = run_matcher(
            connection,
            label="candidate_v7_punctuated",
            suffix_expansions=CANDIDATE_V7_PUNCTUATED_SUFFIX_EXPANSIONS,
        )
        _log(
            "shadow street variants: "
            f"baseline={baseline_summary.get('query_street_variants')} "
            f"candidate={candidate_summary.get('query_street_variants')}"
        )
        report(
            connection,
            baseline_table=baseline_table,
            candidate_table=candidate_table,
            pending=pending,
        )
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
