"""The se_company_address final asset: change detection, the geocode read, set
replacement and the wiring (jobs, sensor, schedule, freshness leaves).

The ClickHouse-facing helpers are asserted as SQL text (this repo has no live
ClickHouse in CI); the resolution loop is exercised end-to-end through
``materialize_se_company_address`` with the scripted fake client from
``test_se_company_common``.
"""

import json
import re
import uuid
from datetime import UTC, datetime

import dagster as dg
import pytest

from dagster_v3.defs.se_company.address import (
    GEOCODE_COLUMNS,
    GEOCODE_PROJECTION,
    INSERT_COLUMNS,
    PUBLISHED_COLUMNS,
    PUBLISHED_PROJECTION,
    SELECTION_REASONS,
    build_artifact_rows_sql,
    build_changed_companies_sql,
    build_geocodes_sql,
    build_published_rows_sql,
    materialize_se_company_address,
    normalized_se_company_ids,
)
from dagster_v3.defs.se_company.address_rules import address_components, address_key_for
from tests.se_company_ddl import declared_columns

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)
GEOCODED_AT = datetime(2026, 8, 20, 3, 15, tzinfo=UTC)
COMPANY = "5565200028"
SOLE_TRADER = "196408233412"

# assert_clickhouse_tables_exist runs its own SELECT against system.tables first, so every
# scripted answer list starts with the tables it asks about.
EXISTING_TABLES = [
    (table,)
    for table in (
        "se_company_address_bolagsverket",
        "se_company_address_scb",
        "se_company_address",
        "se_company_address_correction",
        "se_company_address_members_current",
        "se_company_address_links_current",
        "se_address_geocodes_current",
    )
]

BOLAGSVERKET_VALUES = {
    "address_type": "postal",
    "address_fingerprint": "fingerprint-postal",
    "care_of": "",
    "street_address": "Storgatan 1",
    "normalized_address": "storgatan 1, 111 22 stockholm, se",
    "postal_code": "111 22",
    "city": "Stockholm",
    "country_code": "SE",
}
SCB_VALUES = {
    **BOLAGSVERKET_VALUES,
    "address_type": "visiting_or_postal",
    "address_fingerprint": "fingerprint-visiting",
    "city": "STOCKHOLM",
}
POSTAL_KEY = address_key_for(address_components(BOLAGSVERKET_VALUES))
VISITING_KEY = address_key_for(address_components(SCB_VALUES))
VANISHED_KEY = "d" * 64


def _artifact_row(source: str, uid: str, evidence_hash: str, values: dict) -> tuple:
    """One row of build_artifact_rows_sql: every payload value arrives as text."""
    return (source, COMPANY, uid, evidence_hash, NOW, json.dumps(values))


ARTIFACT_ROWS = [
    _artifact_row("bolagsverket", "bv:1", "a" * 64, BOLAGSVERKET_VALUES),
    _artifact_row("scb", "scb:1", "b" * 64, SCB_VALUES),
]
# build_geocodes_sql projects every column as text; only the Bolagsverket observation
# ever reached the shared-identity chain.
GEOCODE_ROWS = [
    (COMPANY, "fingerprint-postal", "c" * 64, 1, "59.3326", "18.0649", "exact",
     "2026-08-20 03:15:00.000"),
]
# One published row for a key neither source produces any more -- the tombstone case.
PUBLISHED_ROWS = [
    (COMPANY, VANISHED_KEY, "postal", None, "Gamla vagen 9", "gamla vagen 9, 222 22 lund, se",
     "222 22", "Lund", "SE", True, ["bolagsverket"], ["bv:0"], ["e" * 64]),
]


def _selected(company_id: str, **flags: int) -> tuple:
    """One change-scan row: the company id followed by its reason flags, in the order
    build_changed_companies_sql projects them. Written positionally from SELECTION_REASONS
    rather than by hand, so a new reason shifts every scripted row at once instead of
    silently misaligning the metadata counts the loop reads by position."""
    unknown = set(flags) - set(SELECTION_REASONS)
    assert not unknown, f"not scan reasons: {sorted(unknown)}"
    return (company_id, *(int(flags.get(name, 0)) for name in SELECTION_REASONS))


def _final_rows(client) -> list[tuple]:
    """Every row staged for the final table, in insert order."""
    rows = []
    for sql, params in client.executed:
        if re.match(r"^INSERT INTO `corpscout`\.`_tmp_se_company_address_[0-9a-f]{32}`", sql):
            rows.extend(params)
    return rows


def _published(rows: list[tuple]) -> dict[str, dict]:
    """Staged final rows keyed by address_key, each as a column-name -> value map."""
    return {
        str(row[INSERT_COLUMNS.index("address_key")]): dict(zip(INSERT_COLUMNS, row, strict=True))
        for row in rows
    }


def test_insert_columns_are_the_final_ddl_minus_the_materialized_hash() -> None:
    assert list(INSERT_COLUMNS) == [
        column for column in declared_columns("se_company_address") if column != "evidence_set_hash"
    ]


def test_the_change_scan_reads_every_left_join_miss_through_ifnull() -> None:
    """Bare comparisons work only while join_use_nulls = 0; under 1 a miss is NULL, the
    WHERE is NULL for every never-published company and the scan returns nothing."""
    sql = build_changed_companies_sql()
    assert "ifNull(published.company_id, '') = '' AS never_published" in sql
    assert "ifNull(published.resolved_at, toDateTime64('1970-01-01 00:00:00', 3, 'UTC'))" in sql
    assert "ifNull(ledger.latest_correction_at," in sql
    assert "ifNull(geocodes.latest_geocoded_at," in sql


def test_the_change_scan_has_one_term_per_reason_including_the_geocode_one() -> None:
    sql = build_changed_companies_sql()
    assert SELECTION_REASONS == ("never_published", "new_evidence_bolagsverket",
                                 "new_evidence_scb", "new_geocode", "ledger_pending")
    for reason in SELECTION_REASONS:
        assert f" AS {reason}" in sql
    assert "%(resolve_all)s = 1" in sql
    assert "parseDateTime64BestEffort(%(resolve_all_before)s, 3, 'UTC')" in sql
    assert "artifacts.company_id > %(after_company_id)s" in sql and "LIMIT %(page_size)s" in sql


def test_the_change_scan_needs_final_only_on_the_final_table() -> None:
    """observed_at and matched_at ARE the version columns, so max() over the raw parts is
    already the newest; the final is keyed by (company_id, address_key) and appends a row
    per resolution, but max(resolved_at) per company is likewise version-safe."""
    sql = build_changed_companies_sql()
    assert "FROM corpscout.se_company_address_bolagsverket GROUP BY company_id" in sql
    assert "FINAL" not in sql


def test_the_geocode_cte_does_not_alias_a_table_with_its_own_name() -> None:
    """Self-shadowing a WITH name inside its own body is analyzer-dependent: the outer
    ``geocodes.latest_geocoded_at`` reads the CTE, so the joined table must be called
    something else or the two names are one identifier with two meanings."""
    sql = build_changed_companies_sql()
    assert "corpscout.se_address_geocodes_current AS geocodes" not in sql
    assert "corpscout.se_address_geocodes_current AS points" in sql
    assert "max(points.matched_at) AS latest_geocoded_at" in sql


def test_the_geocode_query_gates_every_non_nullable_joined_column() -> None:
    sql = build_geocodes_sql()
    assert "toUInt8(ifNull(geocodes.geocode_run_id, '') != '') AS has_geocode" in sql
    assert "INNER JOIN corpscout.se_company_address_links_current AS links" in sql
    assert "LEFT JOIN corpscout.se_address_geocodes_current AS geocodes" in sql
    assert "toString(members.address_key) AS address_fingerprint" in sql
    # Nullable source columns are ifNull'd, never gated; joined non-Nullable ones are gated.
    assert "ifNull(toString(geocodes.latitude), '') AS latitude" in sql
    assert "toString(geocodes.match_status), '') AS geocode_status" in sql
    assert re.findall(r"AS (\w+)", sql[: sql.index("\nFROM ")]) == list(GEOCODE_COLUMNS)
    # An alias fed by the wrong source column would transpose the fact silently.
    for column, expression in GEOCODE_PROJECTION:
        if column in ("latitude", "longitude", "geocode_status"):
            assert re.search(rf"geocodes\.{'match_status' if column == 'geocode_status' else column}\b",
                             expression), (column, expression)


def test_the_artifact_read_contract_names_its_columns() -> None:
    sql = build_artifact_rows_sql()
    for source in ("bolagsverket", "scb"):
        assert f"'{source}' AS source" in sql
        assert f"FROM corpscout.se_company_address_{source} FINAL" in sql
    assert "'address_fingerprint', ifNull(toString(address_fingerprint), '')" in sql
    assert "*" not in sql


def test_published_rows_are_read_final_and_carry_the_tombstone_flag() -> None:
    sql = build_published_rows_sql()
    assert "FROM corpscout.se_company_address AS published FINAL" in sql
    assert "published.is_current AS is_current" in sql
    assert "WHERE published.company_id IN %(company_ids)s" in sql
    # The projection IS PUBLISHED_COLUMNS, in order. Reordering the pair list moves both
    # the SQL and the mapper together and is therefore harmless; what is NOT harmless is
    # an alias fed by the wrong column, so each expression must name its own.
    assert re.findall(r"AS (\w+)", sql[: sql.index("\nFROM ")]) == list(PUBLISHED_COLUMNS)
    for column, expression in PUBLISHED_PROJECTION:
        assert re.search(rf"\bpublished\.{column}\b", expression), (column, expression)
    # The geocode columns are deliberately absent: a tombstone republishes the address,
    # not a coordinate this resolution did not verify.
    for column in ("latitude", "longitude", "geocode_status", "geocoded_at", "address_id"):
        assert column not in PUBLISHED_COLUMNS


def test_a_preview_writes_nothing_and_reads_nothing_but_the_scan() -> None:
    """A bare Materialize click in the Dagster UI carries no config at all."""
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    client = FakeClient(answers=[EXISTING_TABLES, []])
    metadata = materialize_se_company_address(
        clickhouse=FakeClickhouse(client), source_run_id="run-1", resolved_at=NOW,
        company_ids=[], max_companies=10, company_batch_size=10, execute=False, log=None)

    assert metadata["preview"] is True and metadata["selected_company_count"] == 0
    assert all("INSERT" not in sql for sql, _ in client.executed)
    for reason in SELECTION_REASONS:
        assert metadata[reason] == 0


def test_company_ids_accept_sole_traders() -> None:
    """se_companies carries 12-digit personnummer-based ids for enskild firma, and the
    final's has_company CHECK admits them -- so a scoped run must too."""
    assert normalized_se_company_ids([SOLE_TRADER, COMPANY]) == (SOLE_TRADER, COMPANY)
    with pytest.raises(ValueError):
        normalized_se_company_ids(["55652000"])


def test_normalized_se_company_ids_sorts_dedupes_and_rejects_non_ids() -> None:
    assert normalized_se_company_ids([f" {COMPANY} ", COMPANY, SOLE_TRADER]) == (
        SOLE_TRADER, COMPANY)
    with pytest.raises(ValueError, match="10 or 12 digits"):
        normalized_se_company_ids(["not-an-id"])


def test_a_resolution_publishes_the_whole_set_the_geocode_and_the_tombstone() -> None:
    """One company, two sources, one geocoded observation and one address that left.

    Every position of every published tuple is asserted by NAME: AddressOutcome's
    dataclass field order is not the DDL's, so a positional _final_row would transpose
    same-typed columns with an otherwise-green suite.
    """
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    client = FakeClient(answers=[
        EXISTING_TABLES,                    # assert_clickhouse_tables_exist
        [_selected(COMPANY, never_published=1, new_evidence_bolagsverket=1)],
        ARTIFACT_ROWS,                      # artifact rows
        GEOCODE_ROWS,                       # geocodes
        PUBLISHED_ROWS,                     # already-published rows
        [],                                 # ledger
        [(3, 0)],                           # final stage validation
        [(1,)],                             # target row count before the insert
        [(4,)],                             # target row count after the insert
    ])
    metadata = materialize_se_company_address(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[COMPANY], max_companies=10, company_batch_size=10, execute=True, log=None)

    assert metadata["selected_company_count"] == 1
    assert metadata["never_published"] == 1 and metadata["new_evidence_bolagsverket"] == 1
    assert metadata["address_count"] == 2 and metadata["tombstone_count"] == 1
    assert metadata["geocoded_count"] == 1 and metadata["inserted_count"] == 3

    rows = _published(_final_rows(client))
    assert set(rows) == {POSTAL_KEY, VISITING_KEY, VANISHED_KEY}
    assert rows[POSTAL_KEY] == {
        "company_id": COMPANY, "address_key": POSTAL_KEY, "address_type": "postal",
        "care_of": None, "street_address": "Storgatan 1",
        "normalized_address": "storgatan 1, 111 22 stockholm, se",
        "postal_code": "111 22", "city": "Stockholm", "country_code": "SE",
        # The augmentation, looked up by this observation's own address_fingerprint.
        "address_id": "c" * 64, "latitude": 59.3326, "longitude": 18.0649,
        "geocode_status": "exact", "geocoded_at": GEOCODED_AT, "is_current": True,
        "sources": ["bolagsverket"], "source_record_uids": ["bv:1"],
        "evidence_hashes": ["a" * 64], "correction_ids": [],
        "source_run_id": "run", "resolved_at": NOW,
    }
    # SCB's observation never reached the address identity chain, so it publishes with no
    # coordinate rather than borrowing the other address's.
    assert rows[VISITING_KEY] | {
        "address_type": "visiting_or_postal", "city": "STOCKHOLM",
        "sources": ["scb"], "source_record_uids": ["scb:1"], "evidence_hashes": ["b" * 64],
        "address_id": None, "latitude": None, "longitude": None,
        "geocode_status": "", "geocoded_at": None, "is_current": True,
    } == rows[VISITING_KEY]
    # The tombstone republishes the vanished row's own provenance with is_current false,
    # and claims no geocode this resolution did not look up.
    assert rows[VANISHED_KEY] | {
        "is_current": False, "sources": ["bolagsverket"], "source_record_uids": ["bv:0"],
        "evidence_hashes": ["e" * 64], "correction_ids": [], "street_address": "Gamla vagen 9",
        "address_id": None, "latitude": None, "longitude": None, "geocoded_at": None,
        "resolved_at": NOW,
    } == rows[VANISHED_KEY]


@pytest.mark.parametrize("ungeocoded_first", [True, False])
def test_the_coordinate_survives_a_duplicate_row_for_the_same_fingerprint(ungeocoded_first) -> None:
    """One fingerprint can reach the chain under several canonical addresses (the member
    bridge keys on source and type too), so this page can hand the same observation two
    facts. Whichever order they arrive in, the one that says most has to win -- keeping the
    last would drop the coordinate half the time, and nothing downstream would look wrong.
    """
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    linked_only = (COMPANY, "fingerprint-postal", "f" * 64, 0, "", "", "", "")
    duplicates = [linked_only, *GEOCODE_ROWS] if ungeocoded_first else [*GEOCODE_ROWS, linked_only]
    client = FakeClient(answers=[
        EXISTING_TABLES, [_selected(COMPANY, new_geocode=1)],
        ARTIFACT_ROWS, duplicates, [], [], [(2, 0)], [(0,)], [(2,)],
    ])
    materialize_se_company_address(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[COMPANY], max_companies=10, company_batch_size=10, execute=True, log=None)

    postal = _published(_final_rows(client))[POSTAL_KEY]
    assert (postal["latitude"], postal["longitude"]) == (59.3326, 18.0649)
    assert postal["geocode_status"] == "exact" and postal["address_id"] == "c" * 64


def test_a_reject_correction_publishes_the_address_as_not_current() -> None:
    """The ledger runs before the set replacement, so a rejected key is its own tombstone:
    one row per key per resolution, carrying the correction that decided it."""
    from dagster_v3.defs.se_company.address_rules import ZERO_HASH
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    correction_id = uuid.UUID(int=7)
    client = FakeClient(answers=[
        EXISTING_TABLES,
        [_selected(COMPANY, ledger_pending=1)],
        ARTIFACT_ROWS,
        [],   # no geocodes
        [],   # nothing published yet
        [(correction_id, COMPANY, "reject_address", json.dumps({"address_key": POSTAL_KEY}),
          ZERO_HASH, None, NOW)],
        [(2, 0)], [(0,)], [(2,)],
    ])
    metadata = materialize_se_company_address(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[COMPANY], max_companies=10, company_batch_size=10, execute=True, log=None)

    assert metadata["applied_correction_count"] == 1 and metadata["stale_correction_count"] == 0
    assert metadata["address_count"] == 1 and metadata["tombstone_count"] == 1
    rows = _published(_final_rows(client))
    assert rows[POSTAL_KEY]["is_current"] is False
    assert rows[POSTAL_KEY]["correction_ids"] == [correction_id]
    assert rows[VISITING_KEY]["is_current"] is True


def test_an_explicit_scope_is_chunked_so_the_rendered_query_stays_under_max_query_size() -> None:
    """The scan embeds %(company_ids)s four times and clickhouse-driver substitutes them
    client-side, so a scope larger than company_batch_size is paged chunk by chunk."""
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    scope = [str(5560000000 + index) for index in range(5)]
    client = FakeClient(answers=[EXISTING_TABLES, [], [], []])
    materialize_se_company_address(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=scope, max_companies=10, company_batch_size=2, execute=False, log=None)

    scans = [params for sql, params in client.executed if sql.startswith("WITH artifacts AS (")]
    assert [len(params["company_ids"]) for params in scans] == [2, 2, 1]
    assert all(params["all_companies"] == 0 for params in scans)


def test_the_cap_rather_than_exhaustion_is_reported_when_a_full_page_uses_it_up() -> None:
    from tests.test_se_company_common import FakeClickhouse, FakeClient

    client = FakeClient(answers=[EXISTING_TABLES, [_selected(COMPANY, never_published=1)]])
    metadata = materialize_se_company_address(
        clickhouse=FakeClickhouse(client), source_run_id="run", resolved_at=NOW,
        company_ids=[], max_companies=1, company_batch_size=1, execute=False, log=None)

    assert metadata["stopped_at_cap"] is True and metadata["selected_company_count"] == 1


def test_the_asset_the_jobs_the_sensor_and_the_schedule_are_wired() -> None:
    from dagster_v3.definitions import defs as load_defs
    from dagster_v3.defs.common.clickhouse_checks import CLICKHOUSE_LEAVES

    repository = load_defs().get_repository_def()
    asset = repository.asset_graph.get(dg.AssetKey("se_company_address_clickhouse"))
    assert asset.parent_keys == {dg.AssetKey("se_company_address_bolagsverket_clickhouse"),
                                 dg.AssetKey("se_company_address_scb_clickhouse")}
    assert asset.group_name == "se_company"
    assert asset.metadata["table"] == "corpscout.se_company_address"

    job_keys = {key.path[-1]
                for key in repository.get_job("se_company_address_job").asset_layer.executable_asset_keys}
    assert job_keys == {"se_company_address_bolagsverket_clickhouse",
                        "se_company_address_scb_clickhouse", "se_company_address_clickhouse"}
    review_keys = {key.path[-1]
                   for key in repository.get_job("se_company_address_review_job").asset_layer.executable_asset_keys}
    assert review_keys == {"se_company_address_clickhouse"}

    sensor = repository.get_sensor_def("se_company_address_correction_sensor")
    assert sensor.job_name == "se_company_address_review_job"
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED

    schedule = repository.get_schedule_def("se_company_address_weekly")
    assert schedule.cron_schedule == "55 6 * * 1"  # offset from se_company_info_weekly's 50 6
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    # An automated run must never fall back to the asset's own defaults, which are a
    # preview. Read off an evaluated tick -- the config the daemon would actually submit.
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2026, 8, 24, 6, 55, tzinfo=UTC))
    run_requests = schedule.evaluate_tick(context).run_requests
    assert run_requests is not None and run_requests[0].run_config == {
        "ops": {"se_company_address_clickhouse": {"config": {"execute": True}}}}

    # Ruling A10: all three leaves registered, freshness deliberately off until the weekly
    # schedule is switched on -- a freshness check on a STOPPED schedule is permanent noise.
    leaves = {leaf.asset_key: leaf for leaf in CLICKHOUSE_LEAVES}
    assert leaves["se_company_address_clickhouse"].tables == ("se_company_address",)
    assert leaves["se_company_address_bolagsverket_clickhouse"].tables == (
        "se_company_address_bolagsverket",)
    assert leaves["se_company_address_scb_clickhouse"].tables == ("se_company_address_scb",)
    assert all(leaves[key].max_age is None for key in (
        "se_company_address_clickhouse", "se_company_address_bolagsverket_clickhouse",
        "se_company_address_scb_clickhouse"))


def test_the_correction_sensor_launches_a_real_run_not_a_preview(monkeypatch) -> None:
    """A ledger row must actually re-resolve its company. Without execute in the sensor's
    run config the review job would run the scan and write nothing -- the reviewer's
    correction would sit unapplied forever, and nothing would look broken."""
    from contextlib import contextmanager

    from dagster_clickhouse import ClickhouseResource

    from dagster_v3.defs.se_company.address import se_company_address_correction_sensor
    from tests.test_se_company_common import _FakeLedgerClient

    ledger = _FakeLedgerClient()
    ledger.append(COMPANY, str(uuid.UUID(int=7)), "2026-08-24 09:00:00.000")
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self):
        yield ledger

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    context = dg.build_sensor_context(cursor=None, resources={"clickhouse": resource})
    execution_data = se_company_address_correction_sensor.evaluate_tick(context)

    assert execution_data.run_requests is not None
    assert execution_data.run_requests[0].run_config == {
        "ops": {"se_company_address_clickhouse": {
            "config": {"execute": True, "company_ids": [COMPANY]}}}}


def test_the_module_documents_the_stale_address_property_and_its_mitigation() -> None:
    """Ruling A12: a source-vanished address appends no artifact row, so nothing re-triggers
    the change scan for it and the published row stays current until a resolve_all pass.
    That is a known property, and the module has to say so where a reader will find it."""
    from dagster_v3.defs.se_company import address

    assert "resolve_all" in address.__doc__
    assert re.search(r"vanish|disappear|stops? carrying", address.__doc__, re.IGNORECASE)
