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
4. A row on another policy version is a miss and is re-matched.
5. The per-run tables are dropped even when the engine raises.

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
    QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE,
)
from dagster_v3.defs.sweden_company.geocode_store import STORE_COLUMNS

POLICY = SWEDEN_ADDRESS_RESOLUTION_POLICY.version
STALE_POLICY = "se-address-resolution-policy-v6"
REFERENCE = "ref-1"
RUN_ID = "0f3d9c1a-2b4e-4f6a-8c0d-1e2f3a4b5c6d"
STAMP = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)


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
    }
    values.update(overrides)
    return tuple(values[column] for column in geocode.CACHE_COLUMNS)


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
        self.inserted: list[tuple[Any, ...]] = []

    def execute(
        self, sql: str, params: Any = None, settings: Any = None
    ) -> list[tuple[Any, ...]]:
        self.calls.append((sql, params))
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
    """
    connection = duckdb.connect()
    connection.execute("create schema sweden_address_osm")
    connection.execute(
        "create table sweden_address_osm.address_points"
        " (source_record_id varchar, source_md5 varchar)"
    )
    connection.execute(
        f"insert into sweden_address_osm.address_points values ('osm/1', '{REFERENCE}')"
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


def test_the_care_of_is_not_matched(workbench: duckdb.DuckDBPyConnection) -> None:
    """The location key ignores care-of, and so must the text handed to the matcher:
    `search_text` is the display line without its `c/o ...` part."""
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
