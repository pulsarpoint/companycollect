"""The one-off adoption asset (spec 2026-09-06 section 6, slice 2a, Task 4): copies each old
address identity's CURRENT geocode outcome onto its slice-2a location key, so the address
entity's first fold does not re-match a population the store already decided.

What this file pins, against a scripted ClickHouse `FakeClient`:

1. The collapse rule -- several old identities normalizing to the SAME location key keep
   only the first in `address_id` order; the rest are `collapsed` and never looked up.
2. The normalizer skip -- an identity that does not parse to a usable location (`no_address`)
   is never a candidate at all.
3. The 2026-09-06 sizing amendment's adopt rule, one test per branch:
     - a GEOCODED outcome on an OLD (policy_version, reference_md5) pair is still adopted,
     - a non-geocoded (`unmatched`) outcome on the CURRENT pair is adopted,
     - a non-geocoded outcome on an OLD pair is neither -- `skipped_stale`.
4. The existing-key skip -- a location key the store already holds a row for is never
   re-inserted, and is counted `existing`.
5. `execute=False` (the default) counts everything above and inserts nothing.
6. The adopted tuples, column by column in STORE_COLUMNS order: `address_id` is the NEW
   key, `address_identity_run_id` is `adopted:<old id>`, `geocode_run_id` is this run's id,
   and every other column -- `policy_version`, `reference_md5`, `matched_at` included -- is
   the ORIGINAL outcome's, unchanged.
7. `resolve_current_reference_md5` derives the run's default reference_md5 from the store's
   newest row for the current policy, and raises when there is none.
8. The four SQL texts.
"""

from datetime import UTC, datetime
from typing import Any

import pytest

from dagster_v3.defs.se_company.address import adoption
from dagster_v3.defs.se_company.address.geocode import ADOPTED_RUN_ID_PREFIX, CACHE_LOOKUP_CHUNK
from dagster_v3.defs.se_company.address.normalize_se import (
    RawAddress,
    location_key,
    normalize_se_address,
)
from dagster_v3.defs.sweden_company.geocode_store import GEOCODED_STATUSES, STORE_COLUMNS

POLICY = "se-address-resolution-policy-v7"
STALE_POLICY = "se-address-resolution-policy-v6"
REFERENCE = "current-ref-md5"
STALE_REFERENCE = "an-older-extract"
RUN_ID = "0f3d9c1a-2b4e-4f6a-8c0d-1e2f3a4b5c6d"
MATCHED_AT = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)

ADDRESS_ID_INDEX = STORE_COLUMNS.index("address_id")


def _normalized(**fields: str) -> Any:
    return normalize_se_address(RawAddress(**fields))


def _key(**fields: str) -> str:
    return location_key(_normalized(**fields))


# Seven old identities, address_id ascending. A1/A2 collapse onto one key; B has no usable
# location; C/D/E/F each have their own key.
ID_A1 = "0" * 63 + "1"
ID_A2 = "0" * 63 + "2"
ID_B = "0" * 63 + "3"
ID_C = "0" * 63 + "4"
ID_D = "0" * 63 + "5"
ID_E = "0" * 63 + "6"
ID_F = "0" * 63 + "7"

A1_FIELDS = dict(street_address="Storgatan 5", postal_code="11122", post_town="Stockholm")
A2_FIELDS = dict(street_address="STORGATAN  5", postal_code="11122", post_town="STOCKHOLM")
B_FIELDS: dict[str, str] = {}
C_FIELDS = dict(street_address="Kungsgatan 10", postal_code="11143", post_town="Stockholm")
D_FIELDS = dict(street_address="Sveavägen 20", postal_code="11134", post_town="Stockholm")
E_FIELDS = dict(street_address="Vasagatan 8", postal_code="11120", post_town="Stockholm")
F_FIELDS = dict(street_address="Odengatan 30", postal_code="11324", post_town="Stockholm")

KEY_A = _key(**A1_FIELDS)
KEY_C = _key(**C_FIELDS)
KEY_D = _key(**D_FIELDS)
KEY_E = _key(**E_FIELDS)
KEY_F = _key(**F_FIELDS)

assert _key(**A2_FIELDS) == KEY_A  # the collapse fixture actually collapses
assert len({KEY_A, KEY_C, KEY_D, KEY_E, KEY_F}) == 5  # ... and the rest are distinct

PAGE_1 = [
    (ID_A1, A1_FIELDS["street_address"], A1_FIELDS["postal_code"], A1_FIELDS["post_town"], None),
    (ID_A2, A2_FIELDS["street_address"], A2_FIELDS["postal_code"], A2_FIELDS["post_town"], None),
    (ID_B, None, None, None, None),
    (ID_C, C_FIELDS["street_address"], C_FIELDS["postal_code"], C_FIELDS["post_town"], None),
]
PAGE_2 = [
    (ID_D, D_FIELDS["street_address"], D_FIELDS["postal_code"], D_FIELDS["post_town"], None),
    (ID_E, E_FIELDS["street_address"], E_FIELDS["postal_code"], E_FIELDS["post_town"], None),
    (ID_F, F_FIELDS["street_address"], F_FIELDS["postal_code"], F_FIELDS["post_town"], None),
]


def _outcome_row(address_id: str, **overrides: Any) -> tuple[Any, ...]:
    """One current-outcome row in STORE_COLUMNS order, defaulted to a plausible geocoded
    resolver row on the CURRENT pair."""
    values: dict[str, Any] = {
        "address_id": address_id,
        "policy_version": POLICY,
        "reference_md5": REFERENCE,
        "address_identity_run_id": "address-entity",
        "normalized_match_key": "Some Street 1, 111 22 Stockholm",
        "match_status": "matched_exact",
        "candidate_count": 1,
        "candidate_record_ids": ["osm/1"],
        "candidate_record_urls": ["https://www.openstreetmap.org/node/1"],
        "match_method": "street_house_postcode",
        "match_confidence": 0.97,
        "latitude": 59.3,
        "longitude": 18.0,
        "geocode_provider": "osm",
        "geocode_precision": "building",
        "coordinate_method": "resolver",
        "coordinate_locality": "Stockholm",
        "coordinate_supporting_point_count": 1,
        "coordinate_spread_meters": 0.0,
        "source_record_id": None,
        "source_record_url": None,
        "source_url": None,
        "source_object_key": None,
        "source_md5": None,
        "source_snapshot_at": None,
        "source_retrieved_at": None,
        "geocode_run_id": "an-earlier-run",
        "matched_at": MATCHED_AT,
    }
    values.update(overrides)
    return tuple(values[column] for column in STORE_COLUMNS)


# A1: geocoded, current pair -- the ordinary case.
OUTCOME_A1 = _outcome_row(ID_A1)
# C: geocoded, but on an OLD pair -- still adopted (geocoded on any version).
OUTCOME_C = _outcome_row(
    ID_C, policy_version=STALE_POLICY, reference_md5=STALE_REFERENCE, match_status="matched_corrected"
)
# D: NOT geocoded, but the CURRENT pair -- adopted (any status on this run's own pair).
OUTCOME_D = _outcome_row(
    ID_D,
    match_status="unmatched",
    latitude=None,
    longitude=None,
    geocode_provider="",
    geocode_precision="",
    coordinate_method=None,
)
# E: NOT geocoded, on an OLD pair -- neither condition holds: skipped_stale.
OUTCOME_E = _outcome_row(
    ID_E,
    policy_version=STALE_POLICY,
    reference_md5=STALE_REFERENCE,
    match_status="unmatched",
    latitude=None,
    longitude=None,
    geocode_provider="",
    geocode_precision="",
    coordinate_method=None,
)
# F: geocoded, adoptable -- but its NEW key already holds a store row (scripted below).
OUTCOME_F = _outcome_row(ID_F)

assert all(status in GEOCODED_STATUSES for status in ("matched_exact", "matched_corrected"))


class FakeClient:
    """Answers the four statements adoption.py sends, and records what it was sent."""

    def __init__(
        self,
        *,
        pages: list[list[tuple[Any, ...]]],
        outcomes: list[tuple[Any, ...]] = (),
        existing_keys: frozenset[str] = frozenset(),
        reference_md5_rows: list[tuple[Any, ...]] = (),
    ) -> None:
        self.pages = [list(page) for page in pages]
        self.outcomes = list(outcomes)
        self.existing_keys = set(existing_keys)
        self.reference_md5_rows = list(reference_md5_rows)
        self.calls: list[tuple[str, Any]] = []
        self.inserted: list[tuple[Any, ...]] = []

    def execute(self, sql: str, params: Any = None, settings: Any = None) -> list[tuple[Any, ...]]:
        self.calls.append((sql, params))
        if sql.startswith("INSERT INTO"):
            self.inserted.extend(params or [])
            return []
        if f"FROM {adoption.QUALIFIED_IDENTITIES_TABLE}" in sql:
            return self.pages.pop(0) if self.pages else []
        if sql.startswith("SELECT DISTINCT address_id"):
            requested = set(params["keys"])
            return [(key,) for key in self.existing_keys if key in requested]
        if "%(ids)s" in sql:
            requested = set(params["ids"])
            return [row for row in self.outcomes if row[ADDRESS_ID_INDEX] in requested]
        if "%(policy_version)s" in sql:
            return list(self.reference_md5_rows)
        raise AssertionError(f"unexpected statement: {sql}")

    def statements(self, needle: str) -> list[tuple[str, Any]]:
        return [call for call in self.calls if needle in call[0]]


def _run(
    pages: list[list[tuple[Any, ...]]],
    *,
    outcomes: list[tuple[Any, ...]],
    existing_keys: frozenset[str] = frozenset(),
    execute: bool = False,
    page_size: int = 4,
) -> tuple[FakeClient, adoption.AdoptCounts]:
    client = FakeClient(pages=pages, outcomes=outcomes, existing_keys=existing_keys)
    counts = adoption.adopt_geocode_keys(
        client,
        policy_version=POLICY,
        reference_md5=REFERENCE,
        run_id=RUN_ID,
        page_size=page_size,
        execute=execute,
    )
    return client, counts


def _inserted_by_key(client: FakeClient, key: str) -> dict[str, Any]:
    [row] = [row for row in client.inserted if row[ADDRESS_ID_INDEX] == key]
    return dict(zip(STORE_COLUMNS, row, strict=True))


ALL_OUTCOMES = [OUTCOME_A1, OUTCOME_C, OUTCOME_D, OUTCOME_E, OUTCOME_F]


def test_the_full_scenario_counts_every_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    client, counts = _run(
        [list(PAGE_1), list(PAGE_2)], outcomes=ALL_OUTCOMES, existing_keys=frozenset({KEY_F})
    )

    assert counts == adoption.AdoptCounts(
        identities=7,
        normalized=6,  # everyone but B
        collapsed=1,  # A2, onto A1's key
        geocoded=4,  # A1, C, D, F pass the adopt rule
        skipped_stale=1,  # E
        existing=1,  # F's key already has a store row
        adopted=3,  # A1, C, D actually inserted
        execute=False,
    )
    assert counts.as_metadata() == {
        "identities": 7,
        "normalized": 6,
        "collapsed": 1,
        "geocoded": 4,
        "skipped_stale": 1,
        "existing": 1,
        "adopted": 3,
        "execute": False,
    }


def test_execute_false_inserts_nothing() -> None:
    client, counts = _run(
        [list(PAGE_1), list(PAGE_2)], outcomes=ALL_OUTCOMES, existing_keys=frozenset({KEY_F}), execute=False
    )

    assert client.inserted == []
    assert not any(sql.startswith("INSERT INTO") for sql, _ in client.calls)
    assert counts.adopted == 3  # still counted, just not written


def test_the_collapse_rule_keeps_the_first_address_id_and_never_looks_up_the_rest() -> None:
    client, counts = _run([list(PAGE_1), list(PAGE_2)], outcomes=ALL_OUTCOMES, execute=True)

    assert counts.collapsed == 1
    lookups = client.statements("%(ids)s")
    requested_ids = {row_id for _, params in lookups for row_id in params["ids"]}
    assert ID_A1 in requested_ids
    assert ID_A2 not in requested_ids  # collapsed: never a lookup candidate


def test_a_no_address_identity_is_never_a_candidate() -> None:
    client, counts = _run([list(PAGE_1), list(PAGE_2)], outcomes=ALL_OUTCOMES, execute=True)

    assert counts.normalized == 6  # not 7: B is excluded
    lookups = client.statements("%(ids)s")
    requested_ids = {row_id for _, params in lookups for row_id in params["ids"]}
    assert ID_B not in requested_ids


def test_a_geocoded_outcome_on_an_old_pair_is_still_adopted() -> None:
    client, _ = _run([list(PAGE_1), list(PAGE_2)], outcomes=ALL_OUTCOMES, execute=True)

    row = _inserted_by_key(client, KEY_C)
    assert row["match_status"] == "matched_corrected"
    assert row["policy_version"] == STALE_POLICY  # copied unchanged, not overridden
    assert row["reference_md5"] == STALE_REFERENCE


def test_a_non_geocoded_outcome_on_the_current_pair_is_adopted() -> None:
    client, _ = _run([list(PAGE_1), list(PAGE_2)], outcomes=ALL_OUTCOMES, execute=True)

    row = _inserted_by_key(client, KEY_D)
    assert row["match_status"] == "unmatched"
    assert (row["policy_version"], row["reference_md5"]) == (POLICY, REFERENCE)


def test_a_non_geocoded_outcome_on_an_old_pair_is_skipped_as_stale() -> None:
    client, counts = _run([list(PAGE_1), list(PAGE_2)], outcomes=ALL_OUTCOMES, execute=True)

    assert counts.skipped_stale == 1
    assert all(row[ADDRESS_ID_INDEX] != KEY_E for row in client.inserted)


def test_an_existing_key_is_skipped_and_counted_not_reinserted() -> None:
    client, counts = _run(
        [list(PAGE_1), list(PAGE_2)], outcomes=ALL_OUTCOMES, existing_keys=frozenset({KEY_F}), execute=True
    )

    assert counts.existing == 1
    assert all(row[ADDRESS_ID_INDEX] != KEY_F for row in client.inserted)


def test_the_adopted_tuple_overrides_exactly_three_columns() -> None:
    client, _ = _run([list(PAGE_1), list(PAGE_2)], outcomes=ALL_OUTCOMES, execute=True)

    row = _inserted_by_key(client, KEY_A)
    original = dict(zip(STORE_COLUMNS, OUTCOME_A1, strict=True))
    assert row["address_id"] == KEY_A  # overridden: the NEW location key
    assert row["address_identity_run_id"] == f"{ADOPTED_RUN_ID_PREFIX}{ID_A1}"  # overridden
    assert row["geocode_run_id"] == RUN_ID  # overridden
    unchanged = [c for c in STORE_COLUMNS if c not in ("address_id", "address_identity_run_id", "geocode_run_id")]
    for column in unchanged:
        assert row[column] == original[column], column
    assert row["matched_at"] == MATCHED_AT
    assert row["policy_version"] == POLICY
    assert row["reference_md5"] == REFERENCE


def test_adopted_row_helper_directly() -> None:
    outcome = dict(zip(STORE_COLUMNS, OUTCOME_A1, strict=True))
    row = adoption.adopted_row(outcome, new_key=KEY_A, old_id=ID_A1, run_id=RUN_ID)

    assert len(row) == len(STORE_COLUMNS) == 28
    by_name = dict(zip(STORE_COLUMNS, row, strict=True))
    assert by_name["address_id"] == KEY_A
    assert by_name["address_identity_run_id"] == f"adopted:{ID_A1}"
    assert by_name["geocode_run_id"] == RUN_ID
    assert by_name["matched_at"] == MATCHED_AT


def test_no_representatives_on_a_page_still_pages_on() -> None:
    """A page whose every identity is `no_address` (a corner case: no outcomes to look up
    and nothing to insert) must not stop the walk before the next page is read."""
    client, counts = _run(
        [[(ID_B, None, None, None, None)], [list(PAGE_2)[0]]],
        outcomes=[OUTCOME_D],
        page_size=1,
        execute=True,
    )

    assert counts.identities == 2
    assert counts.normalized == 1
    assert counts.geocoded == 1


def test_current_reference_md5_is_derived_when_not_configured() -> None:
    client = FakeClient(pages=[[]], reference_md5_rows=[(REFERENCE,)])

    assert adoption.resolve_current_reference_md5(client, policy_version=POLICY) == REFERENCE
    [(sql, params)] = client.statements("%(policy_version)s")
    assert params == {"policy_version": POLICY}


def test_current_reference_md5_raises_when_the_store_has_no_row_for_the_policy() -> None:
    client = FakeClient(pages=[[]], reference_md5_rows=[])

    with pytest.raises(ValueError, match=POLICY):
        adoption.resolve_current_reference_md5(client, policy_version=POLICY)


def test_sql_texts() -> None:
    identities = adoption.identities_page_sql()
    assert f"FROM {adoption.QUALIFIED_IDENTITIES_TABLE}" in identities
    assert "address_id > %(after)s" in identities
    assert "ORDER BY address_id" in identities
    assert "LIMIT %(page_size)s" in identities

    outcomes = adoption.current_outcomes_sql()
    from dagster_v3.defs.sweden_company.geocode_store import build_current_geocodes_sql

    assert outcomes == build_current_geocodes_sql(
        columns=STORE_COLUMNS, address_filter_sql="address_id IN %(ids)s"
    )

    existing = adoption.existing_keys_sql()
    assert existing == (
        f"SELECT DISTINCT address_id FROM {adoption.QUALIFIED_STORE_TABLE}"
        " WHERE address_id IN %(keys)s"
    )

    reference = adoption.current_reference_md5_sql()
    assert "SELECT reference_md5" in reference
    assert f"FROM {adoption.QUALIFIED_STORE_TABLE}" in reference
    assert "policy_version = %(policy_version)s" in reference
    assert "ORDER BY matched_at DESC" in reference
    assert "LIMIT 1" in reference


def test_the_lookup_and_existing_checks_are_chunked_at_the_geocode_module_constant() -> None:
    """Reuses geocode.py's own CACHE_LOOKUP_CHUNK rather than restating the number, so the
    two modules cannot drift apart on ClickHouse's max_query_size headroom. One more
    identity than the chunk, each on its own distinct street so none collapses: the
    outcome lookup AND the existing-key check must each split into two statements."""
    assert adoption.CACHE_LOOKUP_CHUNK is CACHE_LOOKUP_CHUNK

    keys = [f"{index:064x}" for index in range(CACHE_LOOKUP_CHUNK + 1)]
    page = [(key, key, "11122", "Stockholm", None) for key in keys]  # street_address = key: distinct locations
    outcomes = [_outcome_row(key) for key in keys]  # every outcome geocoded -> every key adoptable
    client = FakeClient(pages=[page], outcomes=outcomes)

    counts = adoption.adopt_geocode_keys(
        client,
        policy_version=POLICY,
        reference_md5=REFERENCE,
        run_id=RUN_ID,
        page_size=len(page),
        execute=False,
    )

    assert counts.identities == CACHE_LOOKUP_CHUNK + 1
    assert counts.collapsed == 0
    assert counts.geocoded == CACHE_LOOKUP_CHUNK + 1
    lookups = client.statements("%(ids)s")
    assert sorted(len(params["ids"]) for _, params in lookups) == [1, CACHE_LOOKUP_CHUNK]
    existing_checks = client.statements("SELECT DISTINCT address_id")
    assert sorted(len(params["keys"]) for _, params in existing_checks) == [1, CACHE_LOOKUP_CHUNK]
