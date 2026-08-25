"""What each check would have to stop seeing before it stopped failing.

Every SQL constant here is also executed by the harness (Task 11). These tests pin the
predicates, because a check whose predicate silently narrows keeps passing forever.
"""

import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Iterator

import dagster as dg

# Not exported from the top-level package; there is no public way to write the event a
# failed run leaves behind, and this test is about surviving exactly that event.
from dagster._core.definitions.asset_checks.asset_check_evaluation import (
    AssetCheckEvaluationPlanned,
)

from dagster_v3.defs.sweden_company.address_geocoding_assets import (
    ADOPTION_DEMOTION_SQL,
    EXACT_MATCH_RATE_CHECK_KEY,
    EXACT_MATCH_RATE_SQL,
    GEOCODE_STORE_ASSET_KEY,
    SHARED_ADDRESSES_CLICKHOUSE_ASSET_KEY,
    MAX_SERVING_VIEW_REFRESH_AGE,
    SERVING_VIEW_REFRESH_SQL,
    SNAPSHOT_FRESHNESS_SQL,
    STORE_COVERAGE_SQL,
    STORE_INVARIANTS_SQL,
    fetch_sweden_geocode_exact_match_stats,
    fetch_sweden_geocode_snapshot_freshness,
    previous_exact_match_rate_percent,
    sweden_address_geocode_store_complete_check,
    sweden_address_geocodes_serving_view_refresh_check,
    sweden_company_address_exact_match_rate_check,
    serving_view_refresh_is_healthy,
    sweden_company_address_osm_snapshot_freshness_check,
    sweden_shared_addresses_complete_check,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    GEOCODED_STATUSES,
    LEGACY_ADOPTED_POLICY_VERSION,
    QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
    VALID_STATUSES,
)


def test_the_store_invariants_check_pins_the_new_grain() -> None:
    """Uniqueness is per (identity, matcher, reference) now, not per identity: two rows for
    one identity is the store working, and a check that still demanded one row per identity
    would fail every week from the first policy bump onwards."""
    assert "uniqExact(tuple(address_id, policy_version, reference_md5))" in (
        STORE_INVARIANTS_SQL
    )
    assert f"FROM {QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}" in STORE_INVARIANTS_SQL
    # The full 11-status allowlist, and the two key columns are never empty.
    for status in VALID_STATUSES:
        assert f"'{status}'" in STORE_INVARIANTS_SQL
    assert "reference_md5 = ''" in STORE_INVARIANTS_SQL
    assert "policy_version = ''" in STORE_INVARIANTS_SQL
    # Status/coordinate and status/precision agreement survive the grain change unchanged.
    for status in GEOCODED_STATUSES:
        assert f"'{status}'" in STORE_INVARIANTS_SQL
    assert "geocode_precision != 'building'" in STORE_INVARIANTS_SQL
    assert "isNull(source_md5)" in STORE_INVARIANTS_SQL


def test_the_rewrite_drops_every_term_the_store_made_false() -> None:
    """The four terms the old check carried that the store contradicts by design.

    A demand-driven store spans many geocode runs and many identity runs, holds several
    rows per identity, and legitimately retains identities the register has since dropped.
    Each of those makes one of the old terms permanently red, so any of them surviving into
    the rewrite would put the new check into the same state the old one is heading for --
    red every week, for a reason nobody has to act on, which is how a check stops being
    read at all.
    """
    for stale in (
        "uniqExact(geocode_run_id)",
        "address_identity_run_id",
    ):
        assert stale not in STORE_INVARIANTS_SQL
        assert stale not in STORE_COVERAGE_SQL
    # `count() == uniqExact(address_id)` -- one row per identity -- is the term the key
    # triple replaced. The projection still REPORTS the identity count; what must not come
    # back is a per-identity uniqueness demand.
    assert "uniqExact(tuple(address_id, policy_version, reference_md5))" in (
        STORE_INVARIANTS_SQL
    )


def test_the_coverage_check_asks_the_identity_side() -> None:
    """After a demand-driven run every identity must have an outcome -- a new identity is
    matched the week it appears. Anti-joining from the identity table is the only direction
    that can see a MISSING outcome."""
    assert "corpscout.se_addresses_current" in STORE_COVERAGE_SQL
    assert "LEFT ANTI JOIN" in STORE_COVERAGE_SQL
    # The direction is load-bearing: the store keeps outcomes for identities the register
    # has retired, so anti-joining the other way would count those as failures.
    identity_side, store_side = STORE_COVERAGE_SQL.split("LEFT ANTI JOIN")
    assert "se_addresses_current" in identity_side
    assert QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE in store_side


def test_the_demotion_counter_reads_the_served_answer() -> None:
    """The one number that makes the adoption's terminal demotion visible.

    An identity holding an imported `matched_exact` serves that coordinate until a resolver
    outcome that geocodes lands on it. When the resolver's answer is a street or area
    fallback the served precision DROPS, and under the demand rule (spec section 4.2) that
    identity is never selected again -- the demotion is terminal. Both rows stay in the
    store, so nothing is lost and nothing is broken; this is the only place the trade is
    counted, which is why it is reported and not asserted.
    """
    assert LEGACY_ADOPTED_POLICY_VERSION in ADOPTION_DEMOTION_SQL
    # It ranks -- it is the SERVED outcome that matters, not the presence of a resolver row.
    assert "LIMIT 1 BY address_id" in ADOPTION_DEMOTION_SQL
    assert (
        f"served.policy_version != '{LEGACY_ADOPTED_POLICY_VERSION}'"
        in ADOPTION_DEMOTION_SQL
    )
    # matched_corrected is `building` precision too, so it is not a demotion.
    assert (
        "served.match_status NOT IN ('matched_exact', 'matched_corrected')"
        in ADOPTION_DEMOTION_SQL
    )


def test_the_exact_match_rate_counts_links_against_the_versioned_read() -> None:
    """Denominator and numerator must share a grain. The store is per identity and the
    denominator is per company-address link, so the rate is computed by joining -- counting
    identities over a link denominator would report a number that means nothing."""
    assert "corpscout.se_company_address_links_current" in EXACT_MATCH_RATE_SQL
    assert "LEFT JOIN" in EXACT_MATCH_RATE_SQL
    # The joined column is non-Nullable in the store, so the miss is read through ifNull --
    # a bare comparison is NULL under join_use_nulls = 1 and '' under 0.
    assert "ifNull(geocode.match_status, '') = 'matched_exact'" in EXACT_MATCH_RATE_SQL
    # No legacy table, no canonical table.
    assert "se_company_address_geocode" not in EXACT_MATCH_RATE_SQL
    assert "canonical" not in EXACT_MATCH_RATE_SQL


def test_the_freshness_query_reads_the_store_and_nothing_else() -> None:
    assert SNAPSHOT_FRESHNESS_SQL.strip().startswith("SELECT max(source_snapshot_at)")
    assert QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE in SNAPSHOT_FRESHNESS_SQL
    assert "canonical" not in SNAPSHOT_FRESHNESS_SQL
    assert "se_company_address_geocodes" not in SNAPSHOT_FRESHNESS_SQL


def test_the_baseline_check_is_gone() -> None:
    """It joined canonical to the legacy pair on a key that exists only on canonical, and
    its purpose -- parity between the two matchers -- ended when one of them did."""
    from dagster_v3.defs.sweden_company import address_geocoding_assets

    assert not hasattr(
        address_geocoding_assets, "sweden_shared_address_geocodes_baseline_check"
    )
    assert not hasattr(address_geocoding_assets, "fetch_sweden_address_geocode_stats")
    assert not hasattr(address_geocoding_assets, "SwedenAddressGeocodeStats")
    assert not hasattr(
        address_geocoding_assets, "fetch_sweden_address_geocode_result_counts"
    )


def test_the_shared_check_stopped_reading_the_retiring_canonical_table() -> None:
    """Check 2's canonical denominator moved into the DuckDB build, where the table it
    reads is the one the build just wrote. What is left here is shared-vs-links -- and
    what matters is not that the term moved but that the QUERY did: this check has to keep
    running after the canonical ClickHouse table is dropped.
    """
    client = _FakeClickhouseClient(
        {
            "se_company_address_links_current": [(9, 9, 21, 1, 0)],
            "se_addresses_current": [(4, 4, 21, 9, 1)],
        }
    )

    result = sweden_shared_addresses_complete_check.node_def.compute_fn.decorated_fn(
        _FakeResource(client)
    )

    assert result.passed
    assert len(client.executed) == 2
    for sql in client.executed:
        assert "canonical" not in sql


def test_the_store_hosts_the_three_checks_that_read_it() -> None:
    """A check defined and never re-hosted keeps running with an asset that is retiring,
    which is a check that quietly stops running at all.

    The last assertion also pins EXACT_MATCH_RATE_CHECK_KEY to the key this check is
    actually registered under: that constant is how the rate check finds its own history,
    so a rename on one side and not the other would leave the +/-2pp comparison reading an
    empty series forever.
    """
    store = dg.AssetKey(GEOCODE_STORE_ASSET_KEY)
    for check in (
        sweden_address_geocode_store_complete_check,
        sweden_company_address_exact_match_rate_check,
        sweden_company_address_osm_snapshot_freshness_check,
        sweden_address_geocodes_serving_view_refresh_check,
    ):
        assert {key.asset_key for key in check.check_keys} == {store}
    assert {
        key.asset_key for key in sweden_shared_addresses_complete_check.check_keys
    } == {dg.AssetKey(SHARED_ADDRESSES_CLICKHOUSE_ASSET_KEY)}
    assert EXACT_MATCH_RATE_CHECK_KEY in (
        sweden_company_address_exact_match_rate_check.check_keys
    )


def test_the_previous_rate_comes_from_this_checks_own_history() -> None:
    """The comparison series used to be the legacy publish asset's materialization
    metadata. That asset retires in Task 8, and the store asset this check now hangs off
    publishes no rate at all -- it appends outcomes and reports what it appended. Reading
    materializations of the new host would therefore answer None forever and retire the
    +/-2pp term without anyone noticing. The check's own evaluations are the series.
    """
    instance = dg.DagsterInstance.ephemeral()
    assert previous_exact_match_rate_percent(instance, current_run_id="run-2") is None

    instance.report_runless_asset_event(
        dg.AssetCheckEvaluation(
            asset_key=EXACT_MATCH_RATE_CHECK_KEY.asset_key,
            check_name=EXACT_MATCH_RATE_CHECK_KEY.name,
            passed=True,
            metadata={"exact_match_rate_percent": 11.6},
        )
    )

    assert previous_exact_match_rate_percent(instance, current_run_id="run-2") == 11.6
    # A retry inside the same run must not compare the run against itself.
    [record] = instance.event_log_storage.get_asset_check_execution_history(
        EXACT_MATCH_RATE_CHECK_KEY, limit=10
    )
    assert (
        previous_exact_match_rate_percent(instance, current_run_id=record.run_id) is None
    )


def _store_planned_check_event(instance: dg.DagsterInstance, *, run_id: str) -> None:
    """The record a run leaves when it plans this check and dies before evaluating it."""
    instance.event_log_storage.store_event(
        dg.EventLogEntry(
            error_info=None,
            level="debug",
            user_message="",
            run_id=run_id,
            timestamp=time.time(),
            dagster_event=dg.DagsterEvent(
                event_type_value=(
                    dg.DagsterEventType.ASSET_CHECK_EVALUATION_PLANNED.value
                ),
                job_name="sweden_company_address_geocoding_weekly_job",
                event_specific_data=AssetCheckEvaluationPlanned(
                    asset_key=EXACT_MATCH_RATE_CHECK_KEY.asset_key,
                    check_name=EXACT_MATCH_RATE_CHECK_KEY.name,
                ),
            ),
        )
    )


def test_a_planned_record_is_stepped_over_rather_than_read() -> None:
    """One failed week must not end the series permanently.

    `AssetCheckExecutionRecord.evaluation` is an unchecked cast: a PLANNED row hands back
    an `AssetCheckEvaluationPlanned`, which is NOT None and has no `.metadata`. PLANNED
    rows are written at run creation, so a week that dies before this check evaluates --
    the store asset has five raise sites, and runs get terminated -- leaves one on top of
    the history. Reading it raises AttributeError, THIS run is then recorded PLANNED too,
    and every later week raises on its own predecessor: the +/-2pp comparison would never
    come back without someone deleting event-log rows.
    """
    instance = dg.DagsterInstance.ephemeral()
    instance.report_runless_asset_event(
        dg.AssetCheckEvaluation(
            asset_key=EXACT_MATCH_RATE_CHECK_KEY.asset_key,
            check_name=EXACT_MATCH_RATE_CHECK_KEY.name,
            passed=True,
            metadata={"exact_match_rate_percent": 11.6},
        )
    )
    _store_planned_check_event(instance, run_id="the-week-that-failed")

    # The PLANNED row really is newest -- otherwise this test would pass without the guard.
    records = instance.event_log_storage.get_asset_check_execution_history(
        EXACT_MATCH_RATE_CHECK_KEY, limit=10
    )
    assert [record.status.value for record in records] == ["PLANNED", "SUCCEEDED"]
    assert records[0].evaluation is not None
    assert not isinstance(records[0].evaluation, dg.AssetCheckEvaluation)

    assert previous_exact_match_rate_percent(instance, current_run_id="this-week") == 11.6


class _FakeClickhouseClient:
    """Answers a statement by whichever key it contains, and keeps every statement it saw.

    Keys are either a whole SQL constant (which contains itself) or the table name that
    tells two statements apart. The single-element unpacking is deliberate: a fixture whose
    keys match a statement twice, or not at all, fails loudly instead of answering the
    wrong query.
    """

    def __init__(self, answers: dict[str, list[tuple[Any, ...]]]) -> None:
        self.answers = answers
        self.executed: list[str] = []

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        self.executed.append(sql)
        [answer] = [rows for key, rows in self.answers.items() if key in sql]
        return answer


class _FakeResource:
    def __init__(self, client: Any) -> None:
        self._client = client

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        yield self._client


_CLEAN_INVARIANTS = (12, 12, 8, 0, 0, 0, 0, 0, 0, 0)


def _run_store_check(
    *,
    invariants: tuple[int, ...] = _CLEAN_INVARIANTS,
    uncovered: int = 0,
    demoted: int = 0,
) -> dict[str, Any]:
    client = _FakeClickhouseClient(
        {
            STORE_INVARIANTS_SQL: [invariants],
            STORE_COVERAGE_SQL: [(uncovered,)],
            ADOPTION_DEMOTION_SQL: [(demoted,)],
        }
    )
    result = sweden_address_geocode_store_complete_check.node_def.compute_fn.decorated_fn(
        _FakeResource(client)
    )
    return {
        "passed": result.passed,
        **{key: value.value for key, value in result.metadata.items()},
    }


def test_the_store_check_passes_on_many_rows_for_one_identity() -> None:
    """Twelve rows over eight identities is the store working, not a duplicate."""
    result = _run_store_check()

    assert result["passed"]
    assert result["store_rows"] == 12
    assert result["identities"] == 8


def test_the_store_check_fails_on_a_repeated_key_triple() -> None:
    result = _run_store_check(invariants=(13, 12, 8, 0, 0, 0, 0, 0, 0, 0))

    assert not result["passed"]
    assert result["unique_version_keys"] == 12


def test_the_store_check_fails_on_an_identity_with_no_outcome() -> None:
    result = _run_store_check(uncovered=3)

    assert not result["passed"]
    assert result["identities_without_outcome"] == 3


def test_a_demoted_adopted_identity_is_reported_and_never_gates() -> None:
    """The demotion is a design consequence, not a defect: it is counted so somebody can
    watch it, and it must not turn a green pipeline red."""
    result = _run_store_check(demoted=41)

    assert result["passed"]
    assert result["adopted_exact_identities_demoted"] == 41


def test_the_freshness_check_reports_the_age_of_the_newest_stored_snapshot() -> None:
    client = _FakeClickhouseClient(
        {SNAPSHOT_FRESHNESS_SQL: [(datetime(1999, 1, 1, tzinfo=UTC),)]}
    )

    result = sweden_company_address_osm_snapshot_freshness_check.node_def.compute_fn.decorated_fn(
        _FakeResource(client)
    )

    assert not result.passed
    assert result.severity == dg.AssetCheckSeverity.WARN
    assert client.executed == [SNAPSHOT_FRESHNESS_SQL]


def test_an_empty_store_reports_no_snapshot_rather_than_raising() -> None:
    client = _FakeClickhouseClient({SNAPSHOT_FRESHNESS_SQL: [(None,)]})

    assert fetch_sweden_geocode_snapshot_freshness(client) is None


def test_the_rate_is_matched_exact_links_over_all_links() -> None:
    client = _FakeClickhouseClient({EXACT_MATCH_RATE_SQL: [(1000, 116, 480)]})

    stats = fetch_sweden_geocode_exact_match_stats(client)

    assert stats.company_address_links == 1000
    assert stats.matched_exact_links == 116
    assert stats.geocoded_links == 480
    assert stats.exact_match_rate_percent == 11.6


def test_a_register_with_no_links_reports_zero_instead_of_dividing() -> None:
    client = _FakeClickhouseClient({EXACT_MATCH_RATE_SQL: [(0, 0, 0)]})

    assert fetch_sweden_geocode_exact_match_stats(client).exact_match_rate_percent == 0.0


def test_check_one_left_clickhouse_for_the_duckdb_build_it_can_still_read() -> None:
    """The canonical ClickHouse table is dropped, so a check reading it could only fail.

    Its arithmetic did not evaporate: address_canonicalization._assert_canonical_address_invariants
    runs the same source-observation/member/canonical comparison against the DuckDB
    canonical table one step earlier, where it aborts the build instead of reporting on an
    already-published snapshot. What is NOT relocated is the informational
    upstream_observations_since_snapshot term, which `passed` never depended on.
    """
    from dagster_v3.defs.sweden_company import address_canonicalization
    from dagster_v3.defs.sweden_company import address_geocoding_assets

    assert not hasattr(
        address_geocoding_assets, "sweden_company_canonical_addresses_complete_check"
    )
    assert not any(
        "all_source_observations_have_one_canonical_address" in key.name
        for check in address_geocoding_assets.defs.asset_checks or ()
        for key in check.check_keys
    )
    assert hasattr(address_canonicalization, "_assert_canonical_address_invariants")


def test_only_the_clickhouse_canonical_constants_retire() -> None:
    """The DuckDB canonical build stays -- the members bridge derives from it -- so the
    two names that spell the ClickHouse copy go and the two that spell the DuckDB one
    must not follow them out.
    """
    from dagster_v3.defs.sweden_company import address_canonicalization

    assert not hasattr(
        address_canonicalization, "QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE"
    )
    assert not hasattr(address_canonicalization, "CANONICAL_ADDRESS_COLUMNS")
    assert (
        address_canonicalization.QUALIFIED_CANONICAL_ADDRESSES_TABLE
        == "sweden_company_enrichment.se_company_addresses_canonical_current"
    )
    assert address_canonicalization.QUALIFIED_CLICKHOUSE_ADDRESS_MEMBERS_TABLE == (
        "corpscout.se_company_address_members_current"
    )


# ---------------------------------------------------------------------------------------
# The serving view's refresh state. Migration 000320 turned
# corpscout.se_address_geocodes_current into a refreshable materialized view, which took
# its publish asset, its shrink guard, its two parity checks and its row-count leaf with
# it. This check is what replaced all of them, so its predicate is the whole safety net
# and every branch of it is pinned here.


# A fixed instant for the PREDICATE tests, which take `now` as an argument. The check
# body calls datetime.now(UTC) itself and cannot be handed one, so its fixtures are
# built relative to the real clock instead -- a fixed instant there would drift into the
# future or the past depending on the day the suite runs.
_NOW = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)


def _epoch(hours_ago: float, *, before: datetime = _NOW) -> int:
    return int((before - timedelta(hours=hours_ago)).timestamp())


def _ago(hours: float) -> int:
    """An epoch that is `hours` old when the check body reads the real clock."""
    return _epoch(hours, before=datetime.now(UTC))


def _run_view_refresh_check(rows: list[tuple[Any, ...]]) -> dict[str, Any]:
    client = _FakeClickhouseClient({SERVING_VIEW_REFRESH_SQL: rows})
    result = (
        sweden_address_geocodes_serving_view_refresh_check.node_def.compute_fn.decorated_fn(
            _FakeResource(client)
        )
    )
    [executed] = client.executed
    assert executed == SERVING_VIEW_REFRESH_SQL
    return {
        "passed": result.passed,
        **{key: value.value for key, value in result.metadata.items()},
    }


def test_the_view_refresh_check_reads_the_one_table_that_still_knows() -> None:
    """Pinned as SQL because there is no second source for this.

    The view is never materialized, so no Dagster freshness signal exists for it, and the
    only record that it is still being rebuilt lives in system.view_refreshes. A query that
    drifted off that table, or off this exact view, would leave the serving name unwatched
    while the check kept passing.
    """
    assert "FROM system.view_refreshes" in SERVING_VIEW_REFRESH_SQL
    assert "WHERE database = 'corpscout'" in SERVING_VIEW_REFRESH_SQL
    assert "AND view = 'se_address_geocodes_current'" in SERVING_VIEW_REFRESH_SQL
    assert "exception" in SERVING_VIEW_REFRESH_SQL
    # The instant crosses as an epoch integer, never as a datetime: last_success_time is
    # Nullable(DateTime) with no timezone in its type, so a datetime would arrive naive in
    # the SERVER's timezone and be compared against a UTC now.
    assert "toUnixTimestamp(last_success_time)" in SERVING_VIEW_REFRESH_SQL
    assert MAX_SERVING_VIEW_REFRESH_AGE == timedelta(hours=3)


def test_the_view_refresh_check_passes_on_a_recently_refreshed_view() -> None:
    result = _run_view_refresh_check([("Scheduled", "", _ago(0.5))])

    assert result["passed"]
    assert result["refresh_row_found"] is True
    assert result["status"] == "Scheduled"
    assert 0.49 < result["refresh_age_hours"] < 0.51
    assert result["maximum_refresh_age_hours"] == 3.0


def test_the_view_refresh_check_fails_on_an_exception_however_recent_the_success() -> None:
    """The definer-privilege failure, and the one the age term alone would sit on.

    A refresh that throws leaves the view serving its last good contents at full speed, so
    for three hours nothing about the READ looks wrong. Gating on the exception makes the
    failure visible at the first refresh rather than the fourth.
    """
    result = _run_view_refresh_check(
        [("Scheduled", "Code: 497. DB::Exception: Not enough privileges", _ago(0.1))]
    )

    assert not result["passed"]
    assert "Not enough privileges" in result["exception"]


def test_the_view_refresh_check_fails_once_two_whole_intervals_have_been_missed() -> None:
    """Three hours is 3x the REFRESH EVERY 1 HOUR interval, so one long refresh is not a
    failure and two skipped ones are."""
    assert _run_view_refresh_check([("Scheduled", "", _ago(2.9))])["passed"]
    assert not _run_view_refresh_check([("Scheduled", "", _ago(3.1))])["passed"]


def test_the_view_refresh_check_fails_a_view_that_has_never_succeeded() -> None:
    """NULL last_success_time is the worst state, not the least informative one.

    ClickHouse knows the view and has never finished refreshing it, which means every
    backoffice read of the serving name is returning nothing and raising nothing. Tolerating
    the NULL as 'no evidence yet' would keep this check green through exactly that.
    """
    result = _run_view_refresh_check([("Scheduled", "", None)])

    assert not result["passed"]
    assert result["last_success_at"] is None
    assert result["refresh_age_hours"] is None


def test_the_view_refresh_check_fails_when_the_view_is_not_refreshable_at_all() -> None:
    """No row means the name is not a refreshable view here -- 000320 unapplied, or
    something put a plain table back over it. Both are failures, and an empty result set is
    the only way either one shows up."""
    result = _run_view_refresh_check([])

    assert not result["passed"]
    assert result["refresh_row_found"] is False
    assert "not a refreshable materialized view" in result["detail"]


def test_the_view_refresh_predicate_gates_on_exception_before_age() -> None:
    """The predicate alone, away from the check body: both terms fail independently and a
    clean recent refresh is the only shape that passes."""
    recent = _epoch(hours_ago=0.25)
    assert serving_view_refresh_is_healthy(
        exception="", last_success_epoch_seconds=recent, now=_NOW
    )
    assert not serving_view_refresh_is_healthy(
        exception="boom", last_success_epoch_seconds=recent, now=_NOW
    )
    assert not serving_view_refresh_is_healthy(
        exception="", last_success_epoch_seconds=_epoch(4), now=_NOW
    )
    assert not serving_view_refresh_is_healthy(
        exception="", last_success_epoch_seconds=None, now=_NOW
    )
    # A `now` in another timezone is the same instant and must answer identically -- the
    # epoch is absolute and the comparison normalizes to UTC.
    assert serving_view_refresh_is_healthy(
        exception="",
        last_success_epoch_seconds=recent,
        now=_NOW.astimezone(timezone(timedelta(hours=2))),
    )
