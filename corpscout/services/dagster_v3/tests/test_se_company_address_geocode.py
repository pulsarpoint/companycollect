"""The address entity's geocode function: the store as cache, the OSM workbench as engine.

Slice 2a, spec sections 3.7 (as amended for the location key) and 6. What this file pins:

1. A key whose current store row is on THIS policy/reference pair is not matched again --
   no per-run table is even created for it, and nothing is inserted.
2. A miss goes through the real matcher (real DuckDB, real reference documents, real
   libpostal variants -- nothing about the engine is mocked here) and is written back to the
   store as the matcher's RAW outcome.
3. The centroid fallback is a READ-time overlay: a `postal_box`/`unmatched` outcome is
   SERVED as `matched_area`/`centroid_fallback` while the row the store keeps still says
   `postal_box`/`unmatched`. That asymmetry is the whole point of the cache design, so it is
   asserted on both sides of the same call.
4. A row on another policy version is a miss and is re-matched -- an `adopted:` row
   included: only the imported `legacy_adopted_v1` family is an unconditional hit.
5. The per-run tables are dropped even when the engine raises.
6. Every written row carries the OSM extract's five provenance columns (the live store
   check `missing_provenance` gates on them) and a `candidate_count` clamped to `UInt16`.
7. Both id-bound reads render past ClickHouse's default `max_query_size` at a full chunk,
   and both pass the raised `GEOCODE_QUERY_SETTINGS`.
8. The two shared matcher inputs are per-EXTRACT caches, not per-call work: one
   `ensure_reference_postings` per call, and the candidate step handed the cached postings by
   name instead of rebuilding them (2026-09-07 -- the fold calls this function once per
   20,000-company page and paid the whole-reference rebuild per page).

The ClickHouse side is a `FakeClient` scripted per key: the three SQL texts it answers are
pinned separately here (shape) and against a real ClickHouse in
tests/test_se_company_address_geocode_clickhouse_local.py (behaviour).
"""

from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from typing import Any

import duckdb
import pytest

from dagster_v3.defs.address_resolution.search_documents import (
    SEARCH_DOCUMENT_INPUT_COLUMNS,
    replace_address_search_document_input_table,
    replace_address_search_documents,
)
from dagster_v3.defs.se_company.address import geocode
from dagster_v3.defs.se_company.address.normalize_se import (
    NormalizedAddress,
    RawAddress,
    location_key,
    normalize_se_address,
)
from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
)
from dagster_v3.defs.sweden_company.address_resolution_shadow import (
    INDEX_SCOPE as REFERENCE_INDEX_SCOPE,
)
from dagster_v3.defs.sweden_company.address_resolution_shadow import (
    QUALIFIED_REFERENCE_MANIFEST_TABLE,
    QUALIFIED_REFERENCE_POSTINGS_TABLE,
    QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE,
    ensure_reference_postings,
)
from dagster_v3.defs.sweden_company.geocode_store import STORE_COLUMNS

POLICY = SWEDEN_ADDRESS_RESOLUTION_POLICY.version
STALE_POLICY = "se-address-resolution-policy-v6"
REFERENCE = "ref-1"
RUN_ID = "0f3d9c1a-2b4e-4f6a-8c0d-1e2f3a4b5c6d"
STAMP = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
# The stamp a cached row already carries -- distinct from STAMP (this run's own matched_at)
# so a hit's matched_at and a miss's are never accidentally equal in the test that checks
# which one a served outcome carries.
CACHED_AT = datetime(2026, 8, 20, 9, 0, tzinfo=UTC)

# The OSM extract's provenance, carried on every `address_points` row (000275) and copied
# onto every store row this module writes -- the live store check `missing_provenance`
# (sweden_company/address_geocoding_assets.py) gates on all five being non-NULL.
SOURCE_URL = "https://download.geofabrik.de/europe/sweden-latest.osm.pbf"
SOURCE_OBJECT_KEY = "raw/sweden-test.osm.pbf"
SNAPSHOT_AT = datetime(2026, 8, 16, tzinfo=UTC)
RETRIEVED_AT = datetime(2026, 8, 16, 1, 0, tzinfo=UTC)


def _normalized(**fields: str) -> NormalizedAddress:
    return normalize_se_address(RawAddress(**fields))


CACHED = _normalized(
    street_address="Drottninggatan 1", postal_code="11151", post_town="Stockholm"
)
STREET = _normalized(
    street_address="Storgatan 5", postal_code="11122", post_town="Stockholm"
)
BOX = _normalized(street_address="Box 5305", postal_code="10246", post_town="Stockholm")
MISSING = _normalized(
    street_address="Björkstigen 12", postal_code="11144", post_town="Stockholm"
)
DESIGNATION = _normalized(
    street_address="Bergshamra 2:14", postal_code="11122", post_town="Stockholm"
)
WITH_CARE_OF = _normalized(
    care_of="Anna Svensson",
    street_address="Storgatan 5",
    postal_code="11122",
    post_town="Stockholm",
)

CACHED_KEY = location_key(CACHED)
STREET_KEY = location_key(STREET)
BOX_KEY = location_key(BOX)
MISSING_KEY = location_key(MISSING)
DESIGNATION_KEY = location_key(DESIGNATION)

PROVENANCE = geocode.ExtractProvenance(
    source_url=SOURCE_URL,
    source_object_key=SOURCE_OBJECT_KEY,
    source_md5=REFERENCE,
    source_snapshot_at=SNAPSHOT_AT,
    source_retrieved_at=RETRIEVED_AT,
)


def _cache_row(key: str, **overrides: Any) -> tuple[Any, ...]:
    """One scripted current-outcome row in geocode.CACHE_COLUMNS order."""
    values: dict[str, Any] = {
        "address_id": key,
        "policy_version": POLICY,
        "reference_md5": REFERENCE,
        "address_identity_run_id": geocode.ADDRESS_ENTITY_RUN_ID,
        "match_status": "matched_exact",
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
        "matched_at": CACHED_AT,
    }
    values.update(overrides)
    return tuple(values[column] for column in geocode.CACHE_COLUMNS)


def _result_row(**overrides: Any) -> dict[str, Any]:
    """One engine result row, in geocode.RESULT_COLUMNS shape, for the `store_row` unit
    tests that drive the writer directly rather than through the matcher."""
    values: dict[str, Any] = {
        "query_document_id": STREET_KEY,
        "resolution_status": "matched_exact",
        "geocode_precision": "building",
        "match_confidence": 0.97,
        "match_strategy": "street_house_postcode",
        "latitude": 59.33,
        "longitude": 18.06,
        "coordinate_spread_meters": 0.0,
        "supporting_record_count": 1,
        "matched_locality": "Stockholm",
        "candidate_record_ids": ["osm/1"],
        "candidate_record_urls": ["https://www.openstreetmap.org/node/5"],
        "candidate_record_count": 1,
    }
    values.update(overrides)
    return values


class FakeClient:
    """Answers the three statements geocode.py sends, and records what it was sent."""

    def __init__(
        self,
        *,
        cache_rows: Sequence[tuple[Any, ...]] = (),
        fallback_rows: Sequence[tuple[Any, ...]] = (),
    ) -> None:
        self.cache_rows = list(cache_rows)
        self.fallback_rows = list(fallback_rows)
        self.calls: list[tuple[str, Any]] = []
        # Parallel to `calls`: what each statement was sent as `settings`. The two id-bound
        # reads must raise `max_query_size` (C1), and a fake that swallowed the keyword
        # could not tell a passed setting from a forgotten one.
        self.settings_calls: list[Any] = []
        self.inserted: list[tuple[Any, ...]] = []

    def execute(
        self, sql: str, params: Any = None, settings: Any = None
    ) -> list[tuple[Any, ...]]:
        self.calls.append((sql, params))
        self.settings_calls.append(settings)
        if sql.startswith("INSERT"):
            self.inserted.extend(params or [])
            return []
        if "se_postcode_centroids" in sql:
            requested = {row[0] for row in params["rows"]}
            return [row for row in self.fallback_rows if row[0] in requested]
        if geocode.geocode_store.GEOCODE_STORE_TABLE in sql:
            requested = set(params["keys"])
            return [row for row in self.cache_rows if row[0] in requested]
        raise AssertionError(f"unexpected statement: {sql}")

    def statements(self, needle: str) -> list[tuple[str, Any]]:
        return [call for call in self.calls if needle in call[0]]


REFERENCE_POINTS = (
    ("osm/1/house/5", "Storgatan", "5", 59.33, 18.06),
    ("osm/2/house/7", "Storgatan", "7", 59.331, 18.061),
)


@pytest.fixture()
def workbench() -> Iterator[duckdb.DuckDBPyConnection]:
    """An OSM workbench whose reference documents are already built for extract 'ref-1'.

    Built the way tests/test_address_resolution.py builds its fixtures -- the engine's own
    input-table DDL, then the real `replace_address_search_documents` into the table the
    shadow and the geocode function share. The manifest row plus the one `address_points`
    row make `ensure_reference_documents` a no-op, so what the matcher sees is exactly these
    two building points.

    `address_points` carries the extract's five provenance columns as the real table does
    (tests/test_sweden_address_reference_documents.py has the full shape): they are what
    `extract_provenance` reads and `store_row` stamps onto every row it writes.
    """
    connection = duckdb.connect()
    connection.execute("create schema sweden_address_osm")
    connection.execute(
        """
        create table sweden_address_osm.address_points (
            source_record_id varchar,
            source_url varchar,
            source_object_key varchar,
            source_md5 varchar,
            source_snapshot_at timestamptz,
            source_retrieved_at timestamptz
        )
        """
    )
    connection.execute(
        "insert into sweden_address_osm.address_points values (?, ?, ?, ?, ?, ?)",
        ["osm/1", SOURCE_URL, SOURCE_OBJECT_KEY, REFERENCE, SNAPSHOT_AT, RETRIEVED_AT],
    )
    connection.execute(f"create schema if not exists {geocode.ENRICHMENT_SCHEMA}")
    replace_address_search_document_input_table(connection, table_name="reference_input")
    placeholders = ", ".join("?" for _ in SEARCH_DOCUMENT_INPUT_COLUMNS)
    connection.executemany(
        f"insert into reference_input values ({placeholders})",
        [
            (
                REFERENCE_INDEX_SCOPE,
                document_id,
                "SE",
                f"{street} {house}, 11122 Stockholm",
                f"{street} {house}, 11122 Stockholm",
                street,
                house,
                "",
                "11122",
                "Stockholm",
                "physical",
                "building",
                latitude,
                longitude,
                0.0,
                1,
                document_id.split("/house/")[0],
                f"https://www.openstreetmap.org/node/{house}",
            )
            for document_id, street, house, latitude, longitude in REFERENCE_POINTS
        ],
    )
    replace_address_search_documents(
        connection,
        source_sql="select * from reference_input",
        table_name=QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE,
    )
    connection.execute(
        f"""
        create or replace table {QUALIFIED_REFERENCE_MANIFEST_TABLE} as
        select
            '{REFERENCE}'::varchar as reference_md5,
            '{POLICY}'::varchar as policy_version,
            now()::timestamp as built_at
        """
    )
    yield connection
    connection.close()


def _fold_tables(connection: duckdb.DuckDBPyConnection) -> list[str]:
    return [
        row[0]
        for row in connection.execute(
            "select table_name from information_schema.tables"
            " where table_name like '%address_fold%'"
        ).fetchall()
    ]


def _run(
    workbench: duckdb.DuckDBPyConnection,
    client: FakeClient,
    addresses: dict[str, NormalizedAddress],
) -> dict[str, geocode.GeocodeOutcome]:
    return geocode.geocode_addresses(
        addresses, clickhouse=client, duckdb=workbench, run_id=RUN_ID, matched_at=STAMP
    )


def _inserted(client: FakeClient, key: str) -> dict[str, Any]:
    address_id = STORE_COLUMNS.index("address_id")
    [row] = [row for row in client.inserted if row[address_id] == key]
    return dict(zip(STORE_COLUMNS, row, strict=True))


def test_a_cached_key_is_not_matched_again(workbench: duckdb.DuckDBPyConnection) -> None:
    client = FakeClient(cache_rows=[_cache_row(CACHED_KEY)])

    outcomes = _run(workbench, client, {CACHED_KEY: CACHED})

    outcome = outcomes[CACHED_KEY]
    assert outcome.from_cache is True
    assert (outcome.match_status, outcome.latitude, outcome.longitude) == (
        "matched_exact",
        59.3,
        18.0,
    )
    assert (outcome.policy_version, outcome.reference_md5) == (POLICY, REFERENCE)
    assert client.inserted == []
    assert _fold_tables(workbench) == []


def test_an_adopted_row_is_a_hit_whatever_its_versions(
    workbench: duckdb.DuckDBPyConnection,
) -> None:
    """`legacy_adopted_v1` is the imported family: it is not on any resolver version and
    must not be re-matched merely for that (geocode_store's stage-2 rank, `is_adopted`)."""
    client = FakeClient(
        cache_rows=[
            _cache_row(
                CACHED_KEY,
                policy_version=geocode.geocode_store.LEGACY_ADOPTED_POLICY_VERSION,
                reference_md5="some-older-extract",
                address_identity_run_id="adopted:0123",
                match_method="legacy_adopted",
            )
        ]
    )

    outcomes = _run(workbench, client, {CACHED_KEY: CACHED})

    assert outcomes[CACHED_KEY].from_cache is True
    assert client.inserted == []


def test_a_cache_hit_carries_the_stores_matched_at_and_a_fresh_outcome_the_runs(
    workbench: duckdb.DuckDBPyConnection,
) -> None:
    """geocoded_at on the published row must say when the outcome was computed: the
    cached row's matched_at for a hit, this run's matched_at for a miss."""
    client = FakeClient(cache_rows=[_cache_row(CACHED_KEY)])

    outcomes = _run(workbench, client, {CACHED_KEY: CACHED, STREET_KEY: STREET})

    assert outcomes[CACHED_KEY].from_cache is True
    assert outcomes[CACHED_KEY].matched_at == CACHED_AT
    assert outcomes[STREET_KEY].from_cache is False
    assert outcomes[STREET_KEY].matched_at == STAMP


def test_a_miss_is_matched_and_cached(workbench: duckdb.DuckDBPyConnection) -> None:
    client = FakeClient()

    outcomes = _run(workbench, client, {STREET_KEY: STREET})

    outcome = outcomes[STREET_KEY]
    assert outcome.from_cache is False
    assert outcome.match_status == "matched_exact"
    assert (outcome.latitude, outcome.longitude) == (59.33, 18.06)
    assert outcome.geocode_provider == "osm"
    assert outcome.coordinate_method == "resolver"
    assert (outcome.policy_version, outcome.reference_md5) == (POLICY, REFERENCE)

    row = _inserted(client, STREET_KEY)
    assert row["policy_version"] == POLICY
    assert row["reference_md5"] == REFERENCE
    assert row["address_identity_run_id"] == geocode.ADDRESS_ENTITY_RUN_ID
    assert row["normalized_match_key"] == STREET.normalized_address
    assert row["match_status"] == "matched_exact"
    assert row["geocode_run_id"] == RUN_ID
    assert row["matched_at"] == STAMP
    assert row["candidate_record_ids"] == ["osm/1"]
    assert row["source_record_id"] is None
    assert _fold_tables(workbench) == []


def test_the_reference_postings_are_a_per_extract_cache_the_engine_is_handed(
    workbench: duckdb.DuckDBPyConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One `ensure_reference_postings` per call, and the candidate step takes its table.

    The postings are the reference side of the matcher's fuzzy retrieval -- every reference
    street crossed with its deletion signatures. `replace_address_resolution_candidates`
    rebuilt them on every call over the WHOLE reference table, which the fold, calling this
    function once per 20,000-company page, paid per page (prod 2026-09-07: page 1 of
    `bucket_00` sat in the candidates step past 20 minutes). Both halves of the fix are
    pinned here: the cache is ensured (once), and the engine is told to use it.
    """
    ensured: list[object] = []
    candidate_calls: list[dict[str, Any]] = []

    def _counting_ensure(connection: Any, *, log: Any = None) -> str:
        ensured.append(connection)
        return ensure_reference_postings(connection, log=log)

    real_candidates = geocode.replace_address_resolution_candidates

    def _recording_candidates(connection: Any, **kwargs: Any) -> None:
        candidate_calls.append(kwargs)
        real_candidates(connection, **kwargs)

    monkeypatch.setattr(geocode, "ensure_reference_postings", _counting_ensure)
    monkeypatch.setattr(
        geocode, "replace_address_resolution_candidates", _recording_candidates
    )

    outcomes = _run(workbench, FakeClient(), {STREET_KEY: STREET})

    assert outcomes[STREET_KEY].match_status == "matched_exact"
    assert ensured == [workbench]
    assert [call["reference_postings_table"] for call in candidate_calls] == [
        QUALIFIED_REFERENCE_POSTINGS_TABLE
    ]
    # The cache is a real table left behind for the next page, and the engine's own
    # temporary reference postings were never built.
    assert (
        workbench.execute(
            f"select count(*) from {QUALIFIED_REFERENCE_POSTINGS_TABLE}"
        ).fetchone()[0]
        > 0
    )
    assert (
        workbench.execute(
            "select count(*) from duckdb_tables() where table_name = ?",
            ["_address_resolution_reference_street_postings"],
        ).fetchone()[0]
        == 0
    )


def test_the_query_index_scope_is_the_reference_scope() -> None:
    """Every retrieval strategy joins `query.index_scope = reference.index_scope`
    (address_resolution/resolution.py), so a scope of this module's own invention would
    make every candidate join miss and every address `unmatched`."""
    assert geocode.INDEX_SCOPE == REFERENCE_INDEX_SCOPE


def test_a_box_is_postal_box_and_gets_the_town_centroid(
    workbench: duckdb.DuckDBPyConnection,
) -> None:
    client = FakeClient(
        fallback_rows=[(BOX_KEY, "city", 59.32, 18.07, "STOCKHOLM", 4211, 9100.0)]
    )

    outcomes = _run(workbench, client, {BOX_KEY: BOX})

    outcome = outcomes[BOX_KEY]
    assert outcome.match_status == "matched_area"
    assert outcome.geocode_provider == "centroid_fallback"
    assert outcome.geocode_precision == "city"
    assert outcome.coordinate_method == "centroid_median"
    assert (outcome.latitude, outcome.longitude) == (59.32, 18.07)
    assert outcome.coordinate_locality == "STOCKHOLM"
    assert outcome.coordinate_supporting_point_count == 4211
    assert outcome.coordinate_spread_meters == 9100.0

    # The STORED row is the matcher's raw outcome: the overlay is applied on read only.
    row = _inserted(client, BOX_KEY)
    assert row["match_status"] == "postal_box"
    assert row["latitude"] is None
    assert row["geocode_provider"] == ""
    assert row["coordinate_method"] is None

    [(_, params)] = client.statements("se_postcode_centroids")
    assert params["rows"] == [(BOX_KEY, "10246", "stockholm")]


def test_an_unmatched_street_gets_the_postcode_centroid_when_tight(
    workbench: duckdb.DuckDBPyConnection,
) -> None:
    client = FakeClient(
        fallback_rows=[(MISSING_KEY, "postcode", 59.34, 18.09, "11144", 37, 1200.0)]
    )

    outcomes = _run(workbench, client, {MISSING_KEY: MISSING})

    outcome = outcomes[MISSING_KEY]
    assert outcome.geocode_precision == "postcode"
    assert outcome.match_status == "matched_area"
    assert outcome.coordinate_spread_meters == 1200.0
    assert _inserted(client, MISSING_KEY)["match_status"] == "unmatched"


def test_a_cache_hit_is_overlaid_too(workbench: duckdb.DuckDBPyConnection) -> None:
    """The fallback covers cache hits as well: nothing is stored for it, so an identity
    cached as `unmatched` must be filled on every read, not only the run that matched it."""
    client = FakeClient(
        cache_rows=[
            _cache_row(
                MISSING_KEY,
                match_status="unmatched",
                latitude=None,
                longitude=None,
                geocode_provider="",
                geocode_precision="",
                coordinate_method=None,
            )
        ],
        fallback_rows=[(MISSING_KEY, "city", 59.32, 18.07, "STOCKHOLM", 4211, 9100.0)],
    )

    outcome = _run(workbench, client, {MISSING_KEY: MISSING})[MISSING_KEY]

    assert outcome.from_cache is True
    assert (outcome.match_status, outcome.geocode_precision) == ("matched_area", "city")
    assert client.inserted == []


def test_no_centroid_leaves_the_raw_outcome_alone(
    workbench: duckdb.DuckDBPyConnection,
) -> None:
    client = FakeClient(fallback_rows=[])

    outcome = _run(workbench, client, {MISSING_KEY: MISSING})[MISSING_KEY]

    assert outcome.match_status == "unmatched"
    assert (outcome.latitude, outcome.longitude) == (None, None)
    assert outcome.geocode_provider == ""


def test_a_stale_cache_row_is_a_miss(workbench: duckdb.DuckDBPyConnection) -> None:
    client = FakeClient(
        cache_rows=[_cache_row(STREET_KEY, policy_version=STALE_POLICY)]
    )

    outcome = _run(workbench, client, {STREET_KEY: STREET})[STREET_KEY]

    assert outcome.from_cache is False
    assert outcome.policy_version == POLICY
    assert _inserted(client, STREET_KEY)["policy_version"] == POLICY


def test_another_reference_extract_is_a_miss(
    workbench: duckdb.DuckDBPyConnection,
) -> None:
    client = FakeClient(
        cache_rows=[_cache_row(STREET_KEY, reference_md5="an-older-extract")]
    )

    outcome = _run(workbench, client, {STREET_KEY: STREET})[STREET_KEY]

    assert outcome.from_cache is False
    assert _inserted(client, STREET_KEY)["reference_md5"] == REFERENCE


def test_the_search_text_is_composed_from_the_components() -> None:
    """A care-of may itself contain a comma. Carving the display line's first
    comma-separated part off would leave `Dept 4` in the matched text, so two identities
    that hash to the SAME location key would be scored against different text."""
    comma_care_of = _normalized(
        care_of="Firm AB, Dept 4",
        street_address="Storgatan 5",
        postal_code="11122",
        post_town="Stockholm",
    )
    assert location_key(comma_care_of) == STREET_KEY
    assert geocode.search_text(comma_care_of) == geocode.search_text(STREET)
    assert "Dept" not in geocode.search_text(comma_care_of)
    assert "Dept" not in geocode.search_text(STREET)
    # ... and the display line it is NOT carved out of still carries the whole care-of.
    assert "Dept 4" in comma_care_of.normalized_address

    # The postcode is UNSPACED: the shadow's query documents carry the register's own
    # unspaced postcode, and `raw_full_exact` compares normalized full text.
    assert geocode.search_text(STREET) == "storgatan 5, 11122 stockholm"
    assert geocode.search_text(BOX) == "Box 5305, 10246 stockholm"
    assert geocode.search_text(_normalized(street_address="Storgatan 5")) == "storgatan 5"


def test_a_property_designation_is_not_fallback_eligible(
    workbench: duckdb.DuckDBPyConnection,
) -> None:
    """`Bergshamra 2:14` is a cadastral unit, not a street address. The shadow's query
    projection refines it to `property_identifier`; so does this module, which keeps it out
    of FALLBACK_ELIGIBLE_STATUSES -- a designation is never dressed up with a centroid."""
    kind = SEARCH_DOCUMENT_INPUT_COLUMNS.index("address_kind")
    assert geocode.address_kind(DESIGNATION) == "property_identifier"
    assert geocode._input_row(DESIGNATION_KEY, DESIGNATION)[kind] == "property_identifier"
    assert geocode.address_kind(STREET) == "physical"
    assert geocode.address_kind(BOX) == "postal_box"

    # A centroid IS scripted for it: the assertion is that it is never asked for.
    client = FakeClient(
        fallback_rows=[(DESIGNATION_KEY, "city", 59.32, 18.07, "STOCKHOLM", 4211, 9100.0)]
    )

    outcome = _run(workbench, client, {DESIGNATION_KEY: DESIGNATION})[DESIGNATION_KEY]

    assert outcome.match_status == "property_identifier"
    assert (outcome.latitude, outcome.longitude) == (None, None)
    assert outcome.geocode_provider == ""
    assert client.statements("se_postcode_centroids") == []
    assert _inserted(client, DESIGNATION_KEY)["match_status"] == "property_identifier"


@pytest.mark.parametrize(
    ("policy_version", "reference_md5"),
    [
        (STALE_POLICY, REFERENCE),  # an old policy
        (POLICY, "an-older-extract"),  # an old reference extract
        (STALE_POLICY, "an-older-extract"),  # both
    ],
    ids=("old_policy", "old_reference", "both_old"),
)
def test_an_adopted_row_on_an_old_pair_is_a_miss(
    workbench: duckdb.DuckDBPyConnection, policy_version: str, reference_md5: str
) -> None:
    """An `adopted:` run id must not pin a cache hit forever (the 2026-09-06 review).

    The adoption step copies an old identity's outcome with its ORIGINAL versions, so an
    adopted row is exactly as stale as the outcome it copied. Only the imported
    `legacy_adopted_v1` family -- which is on no resolver version at all -- is an
    unconditional hit; an adopted row that names a real policy and a real extract is
    re-matched after the next extract like any other row.
    """
    client = FakeClient(
        cache_rows=[
            _cache_row(
                STREET_KEY,
                policy_version=policy_version,
                reference_md5=reference_md5,
                address_identity_run_id="adopted:abc",
            )
        ]
    )

    outcome = _run(workbench, client, {STREET_KEY: STREET})[STREET_KEY]

    assert outcome.from_cache is False
    assert (outcome.policy_version, outcome.reference_md5) == (POLICY, REFERENCE)
    assert _inserted(client, STREET_KEY)["policy_version"] == POLICY


def test_an_adopted_row_on_the_current_pair_is_a_hit(
    workbench: duckdb.DuckDBPyConnection,
) -> None:
    """The other side of the same rule: an adopted row whose copied versions ARE this run's
    pair is a hit, on the pair and not on the run-id prefix."""
    client = FakeClient(
        cache_rows=[_cache_row(CACHED_KEY, address_identity_run_id="adopted:abc")]
    )

    outcome = _run(workbench, client, {CACHED_KEY: CACHED})[CACHED_KEY]

    assert outcome.from_cache is True
    assert (outcome.policy_version, outcome.reference_md5) == (POLICY, REFERENCE)
    assert client.inserted == []


def test_the_cache_lookup_and_fallback_are_chunked(
    workbench: duckdb.DuckDBPyConnection,
) -> None:
    """Both bindings are chunked at CACHE_LOOKUP_CHUNK: one more key than the chunk means
    two statements each, never one oversized one (ClickHouse's `max_query_size`)."""
    keys = [f"{index:064x}" for index in range(geocode.CACHE_LOOKUP_CHUNK + 1)]
    client = FakeClient(
        cache_rows=[
            _cache_row(
                key,
                match_status="unmatched",
                latitude=None,
                longitude=None,
                geocode_provider="",
                geocode_precision="",
                coordinate_method=None,
            )
            for key in keys
        ]
    )

    outcomes = _run(workbench, client, dict.fromkeys(keys, MISSING))

    assert len(outcomes) == len(keys)
    assert all(outcome.from_cache for outcome in outcomes.values())
    assert client.inserted == []
    lookups = client.statements(geocode.geocode_store.GEOCODE_STORE_TABLE)
    assert [len(params["keys"]) for _, params in lookups] == [
        geocode.CACHE_LOOKUP_CHUNK,
        1,
    ]
    fallbacks = client.statements("se_postcode_centroids")
    assert [len(params["rows"]) for _, params in fallbacks] == [
        geocode.CACHE_LOOKUP_CHUNK,
        1,
    ]


def test_the_care_of_is_not_matched(workbench: duckdb.DuckDBPyConnection) -> None:
    """The location key ignores care-of, and so must the text handed to the matcher:
    `search_text` is COMPOSED from the location components, so the care-of never reaches
    it -- it is not the display line with a part carved off."""
    key = location_key(WITH_CARE_OF)
    assert key == STREET_KEY
    client = FakeClient()

    outcome = _run(workbench, client, {key: WITH_CARE_OF})[key]

    assert outcome.match_status == "matched_exact"
    row = _inserted(client, key)
    assert row["normalized_match_key"] == WITH_CARE_OF.normalized_address
    assert WITH_CARE_OF.normalized_address.startswith("c/o ")


def test_everything_at_once(workbench: duckdb.DuckDBPyConnection) -> None:
    """One call, one cache lookup, one insert and one fallback query for four keys."""
    client = FakeClient(
        cache_rows=[_cache_row(CACHED_KEY)],
        fallback_rows=[
            (BOX_KEY, "city", 59.32, 18.07, "STOCKHOLM", 4211, 9100.0),
            (MISSING_KEY, "postcode", 59.34, 18.09, "11144", 37, 1200.0),
        ],
    )

    outcomes = _run(
        workbench,
        client,
        {CACHED_KEY: CACHED, STREET_KEY: STREET, BOX_KEY: BOX, MISSING_KEY: MISSING},
    )

    assert {key: outcome.match_status for key, outcome in outcomes.items()} == {
        CACHED_KEY: "matched_exact",
        STREET_KEY: "matched_exact",
        BOX_KEY: "matched_area",
        MISSING_KEY: "matched_area",
    }
    assert [outcome.from_cache for outcome in outcomes.values()].count(True) == 1
    # one lookup + one insert
    assert len(client.statements(geocode.geocode_store.GEOCODE_STORE_TABLE)) == 2
    assert len(client.statements("se_postcode_centroids")) == 1
    assert len(client.inserted) == 3
    assert _fold_tables(workbench) == []


def test_per_run_tables_are_dropped_even_on_failure(
    workbench: duckdb.DuckDBPyConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("engine exploded")

    monkeypatch.setattr(geocode, "replace_address_resolution_results", boom)

    with pytest.raises(RuntimeError, match="engine exploded"):
        _run(workbench, FakeClient(), {STREET_KEY: STREET})

    assert _fold_tables(workbench) == []


def test_no_addresses_touches_nothing(workbench: duckdb.DuckDBPyConnection) -> None:
    client = FakeClient()

    assert _run(workbench, client, {}) == {}

    assert client.calls == []
    assert _fold_tables(workbench) == []


def test_a_foreign_or_unusable_address_never_reaches_the_matcher(
    workbench: duckdb.DuckDBPyConnection,
) -> None:
    """Spec section 6 step 3: the fold filters `foreign` and `no_address` rows out (slice
    2b) and this function refuses one if it is ever handed it -- the key is named so the
    caller can find the row, rather than a silent `unmatched` written into the cache."""
    foreign = _normalized(street_address="Hauptstrasse 1", post_town="Utlandet")
    assert foreign.parse_status == "foreign"
    empty = _normalized()
    assert empty.parse_status == "no_address"
    client = FakeClient()

    for address in (foreign, empty):
        key = location_key(address)
        with pytest.raises(ValueError, match=key):
            _run(workbench, client, {key: address})

    assert client.calls == []
    assert _fold_tables(workbench) == []


def test_the_id_bound_reads_render_under_the_raised_query_size_setting() -> None:
    """C1: clickhouse-driver substitutes `%(keys)s`/`%(rows)s` CLIENT-side, so a full
    CACHE_LOOKUP_CHUNK of 64-hex location keys lands in the statement TEXT the server has to
    parse -- past ClickHouse's 262,144-byte default `max_query_size` (Code: 62, "Max query
    size exceeded"), which is why GEOCODE_QUERY_SETTINGS raises it.

    Rendered exactly as the driver renders it, the technique
    tests/test_se_company_address_normalize.py::
    test_a_full_page_renders_under_the_query_size_setting uses. No server needed.
    """
    from types import SimpleNamespace

    from clickhouse_driver.util.escape import escape_params

    DEFAULT_MAX_QUERY_SIZE = 262_144
    context = SimpleNamespace(
        server_info=SimpleNamespace(get_timezone=lambda: "UTC"),
        client_settings={"server_side_params": False},
    )
    keys = [f"{index:064x}" for index in range(geocode.CACHE_LOOKUP_CHUNK)]
    rows = [(key, "11122", "stockholm") for key in keys]
    rendered = {
        "cache_lookup": geocode.cache_lookup_sql()
        % escape_params({"keys": keys}, context),
        "fallback": geocode.fallback_sql() % escape_params({"rows": rows}, context),
    }

    for name, statement in rendered.items():
        size = len(statement.encode("utf-8"))
        # Half one: the default really would reject this render, so the setting is not
        # decoration -- the comment this replaces claimed a 1 MiB default and no need.
        assert size > DEFAULT_MAX_QUERY_SIZE, name
        # Half two: the raised setting covers the worst case with real margin.
        assert size < geocode.GEOCODE_QUERY_SETTINGS["max_query_size"], name


def test_the_id_bound_reads_pass_the_raised_query_size_setting(
    workbench: duckdb.DuckDBPyConnection,
) -> None:
    """Both id-bound reads -- the cache lookup and the centroid fallback -- carry
    GEOCODE_QUERY_SETTINGS, and so does the INSERT since the warm step pushes up to
    500,000 rows through it per call: its values go over the wire as a block, so the
    size limit is moot for it, but max_execution_time bounds a stalled insert."""
    client = FakeClient(
        fallback_rows=[(BOX_KEY, "city", 59.32, 18.07, "STOCKHOLM", 4211, 9100.0)]
    )

    _run(workbench, client, {BOX_KEY: BOX})

    kinds = [
        "insert"
        if sql.startswith("INSERT")
        else "fallback"
        if "se_postcode_centroids" in sql
        else "lookup"
        for sql, _ in client.calls
    ]
    assert sorted(kinds) == ["fallback", "insert", "lookup"]
    assert dict(zip(kinds, client.settings_calls, strict=True)) == {
        "lookup": geocode.GEOCODE_QUERY_SETTINGS,
        "insert": geocode.GEOCODE_QUERY_SETTINGS,
        "fallback": geocode.GEOCODE_QUERY_SETTINGS,
    }
    assert geocode.GEOCODE_QUERY_SETTINGS == {
        "max_query_size": 1_048_576,
        "max_execution_time": 1800,
    }


def test_the_extract_provenance_is_stamped_on_every_written_row(
    workbench: duckdb.DuckDBPyConnection,
) -> None:
    """I3: the live store check `missing_provenance`
    (sweden_company/address_geocoding_assets.py::STORE_INVARIANTS_SQL) fails any store row
    with a NULL in one of these five, so the entity's own rows carry the OSM extract's
    provenance -- read from the workbench exactly as
    address_resolution_promotion.py reads it. `source_record_id`/`source_record_url` stay
    NULL: the check does not count them."""
    client = FakeClient()

    _run(workbench, client, {STREET_KEY: STREET})

    row = _inserted(client, STREET_KEY)
    assert (
        row["source_url"],
        row["source_object_key"],
        row["source_md5"],
        row["source_snapshot_at"],
        row["source_retrieved_at"],
    ) == (SOURCE_URL, SOURCE_OBJECT_KEY, REFERENCE, SNAPSHOT_AT, RETRIEVED_AT)
    # The extract identity is the reference identity: both are
    # `first(source_md5 order by source_record_id)` off the same table.
    assert row["source_md5"] == row["reference_md5"]
    assert row["source_record_id"] is None
    assert row["source_record_url"] is None


def test_the_provenance_read_is_the_promotions_own(
    workbench: duckdb.DuckDBPyConnection,
) -> None:
    provenance = geocode.extract_provenance(workbench)

    assert provenance == geocode.ExtractProvenance(
        source_url=SOURCE_URL,
        source_object_key=SOURCE_OBJECT_KEY,
        source_md5=REFERENCE,
        source_snapshot_at=SNAPSHOT_AT,
        source_retrieved_at=RETRIEVED_AT,
    )


def test_an_empty_workbench_has_no_provenance_to_stamp(
    workbench: duckdb.DuckDBPyConnection,
) -> None:
    workbench.execute("delete from sweden_address_osm.address_points")

    with pytest.raises(ValueError, match="address_points"):
        geocode.extract_provenance(workbench)


def test_a_store_row_refuses_a_provenance_from_another_extract() -> None:
    """`source_md5` and `reference_md5` are the same value off the same table. A row that
    stamped one extract's md5 as its provenance and another's as its cache key would be
    indistinguishable from a correct one on read."""
    with pytest.raises(ValueError, match="reference_md5"):
        geocode.store_row(
            STREET,
            _result_row(),
            address_id=STREET_KEY,
            policy_version=POLICY,
            reference_md5="a-different-extract",
            run_id=RUN_ID,
            matched_at=STAMP,
            provenance=PROVENANCE,
        )


def test_the_candidate_count_is_clamped_to_the_uint16_column() -> None:
    """`candidate_count` is `UInt16` (migration 000317). A common street in a big city can
    return more candidates than 65,535, and clickhouse-driver would reject the whole block.
    Clamped, exactly as address_resolution_promotion.py's `least(65535, ...)` does it."""
    row = dict(
        zip(
            STORE_COLUMNS,
            geocode.store_row(
                STREET,
                _result_row(candidate_record_count=70_000),
                address_id=STREET_KEY,
                policy_version=POLICY,
                reference_md5=REFERENCE,
                run_id=RUN_ID,
                matched_at=STAMP,
                provenance=PROVENANCE,
            ),
            strict=True,
        )
    )

    assert row["candidate_count"] == 65_535


def test_sql_texts() -> None:
    lookup = geocode.cache_lookup_sql()
    assert "address_id IN %(keys)s" in lookup
    assert lookup == geocode.geocode_store.build_current_geocodes_sql(
        columns=geocode.CACHE_COLUMNS, address_filter_sql="address_id IN %(keys)s"
    )
    assert geocode.CACHE_COLUMNS[0] == "address_id"
    assert set(geocode.CACHE_COLUMNS) <= set(STORE_COLUMNS)

    insert = geocode.cache_insert_sql()
    assert insert == (
        "INSERT INTO corpscout.se_address_geocodes"
        f" ({', '.join(STORE_COLUMNS)}) VALUES"
    )

    fallback = geocode.fallback_sql()
    assert "arrayJoin(%(rows)s)" in fallback
    assert "corpscout.se_postcode_centroids" in fallback
    assert "corpscout.se_city_centroids" in fallback
    assert "regexp_replace" in fallback  # centroid_keys.postcode_key_sql
    assert "replace(upper(trim(" in fallback.replace("\n", "")  # centroid_keys.city_key_sql
    assert "3000.0" in fallback

    documents = geocode.query_documents_sql("_the_input_table")
    assert "from _the_input_table" in documents
    projected = [
        line.strip().rstrip(",")
        for line in documents.splitlines()
        if line.startswith("    ")
    ]
    assert tuple(projected) == SEARCH_DOCUMENT_INPUT_COLUMNS
