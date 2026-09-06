"""geocode.py's three ClickHouse statements against a real ClickHouse (clickhouse-local).

Claims a fake client cannot settle:

1. `cache_lookup_sql()` really is the store's two-stage read rule, bound by key. A v6 and a
   v7 row for one identity resolve to the v7 one; an ADOPTED row and a NEWER resolver
   `ambiguous` row for another resolve to the ADOPTED one -- which is the case
   `geocode._is_hit` exists for, and it can only be observed on an engine that actually
   ranks (`LIMIT 1 BY` twice, no `FINAL`).
2. `cache_insert_sql()` accepts the exact 28-value tuple `geocode.store_row()` builds --
   including the `''`-for-non-nullable/NULL-for-nullable split migration 000317 declares,
   the two `Array(String)` columns and the Float32/Float64 mix -- and reads back unchanged.
3. `fallback_sql()` binds `%(rows)s` as an array of tuples, keys it through
   `centroid_keys.postcode_key_sql`/`city_key_sql` (`'111 22'` -> `'11122'`, `'göteborg'` ->
   `'GÖTEBORG'` -- the Swedish letters survive ClickHouse's ASCII-only `upper`), and walks
   the ladder: a tight postcode centroid wins, a postcode centroid past the 3,000 m cap is
   demoted to the city, and an address with neither is simply absent from the answer.

Run under both `join_use_nulls` settings: the fragment guards every LEFT JOIN column that
gates the tier with `ifNull`, and this is what proves it.
"""

import subprocess
from dataclasses import replace as dataclass_replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from dagster_v3.defs.se_company.address import geocode
from dagster_v3.defs.se_company.address.normalize_se import (
    RawAddress,
    normalize_se_address,
)
from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    LEGACY_ADOPTED_POLICY_VERSION,
    STORE_COLUMNS,
)
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
# The store and the two centroid reference tables. All three are self-contained
# (`CREATE DATABASE` + one `CREATE TABLE`); 000320's `se_address_geocodes_current` MV and
# 000327's served view are downstream of the store, not depended on by it, and nothing here
# reads either -- `cache_lookup_sql` ranks the raw store itself.
MIGRATIONS = (
    "000317_corpscout_se_address_geocodes_store.up.sql",
    "000323_corpscout_se_postcode_centroids.up.sql",
    "000324_corpscout_se_city_centroids.up.sql",
)

POLICY = SWEDEN_ADDRESS_RESOLUTION_POLICY.version
STALE_POLICY = "se-address-resolution-policy-v6"
REFERENCE = "ref-1"
RUN_ID = "0f3d9c1a2b4e4f6a8c0d1e2f3a4b5c6d"

RANKED_KEY = "a" * 64
ADOPTED_KEY = "b" * 64
WRITTEN_KEY = "c" * 64
POSTCODE_KEY = "d" * 64
CITY_KEY = "e" * 64
NOWHERE_KEY = "f" * 64

OLDER = datetime(2026, 6, 1, 9, 0, tzinfo=UTC)
NEWER = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)

ADDRESS = normalize_se_address(
    RawAddress(street_address="Storgatan 5", postal_code="11122", post_town="Stockholm")
)

# The OSM extract's provenance, as `extract_provenance` reads it off the workbench. Its
# `source_md5` IS the reference md5 -- both are `first(source_md5 order by
# source_record_id)` over `sweden_address_osm.address_points` -- so `store_row` refuses any
# other pairing.
PROVENANCE = geocode.ExtractProvenance(
    source_url="https://download.geofabrik.de/europe/sweden-latest.osm.pbf",
    source_object_key="raw/sweden-test.osm.pbf",
    source_md5=REFERENCE,
    source_snapshot_at=datetime(2026, 8, 16, tzinfo=UTC),
    source_retrieved_at=datetime(2026, 8, 16, 1, 0, tzinfo=UTC),
)


def _literal(value: Any) -> str:
    """A parameter value rendered the way clickhouse-driver renders it: a `list` becomes an
    array literal and a `tuple` a parenthesized one -- which is exactly the difference
    `fallback_sql`'s `arrayJoin(%(rows)s)` depends on (an array OF tuples)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, datetime):
        stamp = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return f"toDateTime64('{stamp}', 3, 'UTC')"
    if isinstance(value, list):
        return "[" + ", ".join(_literal(item) for item in value) + "]"
    if isinstance(value, tuple):
        return "(" + ", ".join(_literal(item) for item in value) + ")"
    if isinstance(value, (int, float)):
        return repr(value)
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _bind(sql: str, **params: Any) -> str:
    rendered = sql
    for name, value in params.items():
        rendered = rendered.replace(f"%({name})s", _literal(value))
    assert "%(" not in rendered, rendered
    return rendered


def _schema_statements() -> list[str]:
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(("CREATE DATABASE", "CREATE TABLE")):
                statements.append(statement)
    return statements


def _result(**overrides: Any) -> dict[str, Any]:
    """One engine result row, in geocode.RESULT_COLUMNS shape."""
    values: dict[str, Any] = {
        "query_document_id": WRITTEN_KEY,
        "resolution_status": "matched_exact",
        "geocode_precision": "building",
        "match_confidence": 0.5,
        "match_strategy": "street_house_postcode",
        "latitude": 59.25,
        "longitude": 18.5,
        "coordinate_spread_meters": 0.0,
        "supporting_record_count": 1,
        "matched_locality": "Stockholm",
        "candidate_record_ids": ["osm/1", "osm/2"],
        "candidate_record_urls": ["https://www.openstreetmap.org/node/1"],
        "candidate_record_count": 2,
    }
    values.update(overrides)
    return values


def _store_insert(row: tuple[Any, ...]) -> str:
    return f"{geocode.cache_insert_sql()} {_literal(tuple(row))}"


def _resolver_insert(
    key: str,
    result: dict[str, Any],
    *,
    policy_version: str = POLICY,
    reference_md5: str = REFERENCE,
    matched_at: datetime = NEWER,
) -> str:
    """A row through geocode.store_row -- the module's own tuple, not a hand-built one."""
    return _store_insert(
        geocode.store_row(
            ADDRESS,
            result,
            address_id=key,
            policy_version=policy_version,
            reference_md5=reference_md5,
            run_id=RUN_ID,
            matched_at=matched_at,
            provenance=(
                PROVENANCE
                if reference_md5 == PROVENANCE.source_md5
                else dataclass_replace(PROVENANCE, source_md5=reference_md5)
            ),
        )
    )


def _adopted_insert(key: str) -> str:
    """The imported family: on no resolver version, stamped `adopted:<old address_id>`.

    Hand-built rather than through `store_row`, because `store_row` is deliberately the
    resolver's writer -- the adoption step (Task 4) is what writes this shape.
    """
    values: dict[str, Any] = dict.fromkeys(STORE_COLUMNS)
    values.update(
        address_id=key,
        policy_version=LEGACY_ADOPTED_POLICY_VERSION,
        reference_md5="",
        address_identity_run_id="adopted:0123456789",
        normalized_match_key=ADDRESS.normalized_address,
        match_status="matched_exact",
        candidate_count=1,
        candidate_record_ids=["legacy/1"],
        candidate_record_urls=[],
        match_method="legacy_adopted",
        match_confidence=1.0,
        latitude=57.5,
        longitude=12.0,
        geocode_provider="osm",
        geocode_precision="building",
        coordinate_method="legacy",
        coordinate_locality="Stockholm",
        coordinate_supporting_point_count=1,
        coordinate_spread_meters=0.0,
        geocode_run_id="legacy",
        matched_at=OLDER,
    )
    return _store_insert(tuple(values[column] for column in STORE_COLUMNS))


READ_BACK_COLUMNS = (
    "policy_version",
    "reference_md5",
    "address_identity_run_id",
    "normalized_match_key",
    "match_status",
    "candidate_count",
    "candidate_record_ids",
    "match_method",
    "toString(match_confidence)",
    "latitude",
    "geocode_provider",
    "geocode_precision",
    "coordinate_method",
    "coordinate_supporting_point_count",
    "source_record_id",
    "source_record_url",
    "source_url",
    "source_object_key",
    "source_md5",
    "source_snapshot_at",
    "source_retrieved_at",
    "geocode_run_id",
    "matched_at",
)

CENTROID_STAMP = datetime(2026, 8, 16, tzinfo=UTC)


def _statements() -> list[str]:
    lookup = _bind(
        geocode.cache_lookup_sql(), keys=[RANKED_KEY, ADOPTED_KEY, WRITTEN_KEY]
    )
    fallback = _bind(
        geocode.fallback_sql(),
        rows=[
            (POSTCODE_KEY, "111 22", "Stockholm"),
            (CITY_KEY, "41103", "göteborg"),
            (NOWHERE_KEY, "99999", "Okänd"),
        ],
    )
    return [
        *_schema_statements(),
        # One identity matched under v6 and again under v7: stage 1 keeps the newest per
        # family, stage 2 prefers the geocoded newest -- the v7 row.
        _resolver_insert(
            RANKED_KEY,
            _result(latitude=58.0, longitude=17.0),
            policy_version=STALE_POLICY,
            reference_md5="ref-0",
            matched_at=OLDER,
        ),
        _resolver_insert(RANKED_KEY, _result(latitude=59.25, longitude=18.5)),
        # One identity the resolver refuses (newer) and the import placed (older): the
        # adopted row survives stage 2 -- a resolver `ambiguous` never takes a coordinate
        # away from an import.
        _adopted_insert(ADOPTED_KEY),
        _resolver_insert(
            ADOPTED_KEY,
            _result(
                resolution_status="ambiguous",
                geocode_precision="",
                latitude=None,
                longitude=None,
                coordinate_spread_meters=None,
                matched_locality="",
                match_strategy="",
                match_confidence=0.0,
            ),
        ),
        # The exact tuple store_row builds, read back column by column.
        _resolver_insert(WRITTEN_KEY, _result()),
        "SELECT '@@lookup'",
        f"SELECT address_id, policy_version, reference_md5, address_identity_run_id,"
        f" match_status, latitude FROM ({lookup}) AS ranked ORDER BY address_id",
        "SELECT '@@read_back'",
        f"SELECT {', '.join(READ_BACK_COLUMNS)}"
        f" FROM {geocode.QUALIFIED_STORE_TABLE}"
        f" WHERE address_id = {_literal(WRITTEN_KEY)}",
        f"INSERT INTO corpscout.se_postcode_centroids VALUES"
        f" {_literal(('11122', 59.34, 18.09, 37, 1200.0, CENTROID_STAMP))},"
        f" {_literal(('41103', 57.7, 11.97, 12, 5000.0, CENTROID_STAMP))}",
        f"INSERT INTO corpscout.se_city_centroids VALUES"
        f" {_literal(('GÖTEBORG', 57.71, 11.96, 3000, 9000.0, CENTROID_STAMP))}",
        "SELECT '@@fallback'",
        f"SELECT * FROM ({fallback}) AS filled ORDER BY key",
    ]


def _run(statements: list[str], *, join_use_nulls: int) -> list[str]:
    script = f"SET join_use_nulls = {join_use_nulls};\n" + ";\n".join(statements) + ";\n"
    completed = subprocess.run(
        _clickhouse_local_command(),
        input=script,
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _sections(lines: list[str]) -> dict[str, list[list[str]]]:
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in lines:
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        else:
            result[current].append(line.split("\t"))
    return result


@pytest.fixture(
    scope="module",
    params=(0, 1),
    ids=("join_use_nulls_off", "join_use_nulls_on"),
)
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    return _sections(_run(_statements(), join_use_nulls=request.param))


def test_the_lookup_ranks_the_store(sections: dict[str, list[list[str]]]) -> None:
    assert sections["lookup"] == [
        [RANKED_KEY, POLICY, REFERENCE, geocode.ADDRESS_ENTITY_RUN_ID, "matched_exact", "59.25"],
        [
            ADOPTED_KEY,
            LEGACY_ADOPTED_POLICY_VERSION,
            "",
            "adopted:0123456789",
            "matched_exact",
            "57.5",
        ],
        [WRITTEN_KEY, POLICY, REFERENCE, geocode.ADDRESS_ENTITY_RUN_ID, "matched_exact", "59.25"],
    ]


def test_the_insert_tuple_round_trips(sections: dict[str, list[list[str]]]) -> None:
    [row] = sections["read_back"]
    assert dict(zip(READ_BACK_COLUMNS, row, strict=True)) == {
        "policy_version": POLICY,
        "reference_md5": REFERENCE,
        "address_identity_run_id": geocode.ADDRESS_ENTITY_RUN_ID,
        "normalized_match_key": ADDRESS.normalized_address,
        "match_status": "matched_exact",
        "candidate_count": "2",
        "candidate_record_ids": "['osm/1','osm/2']",
        "match_method": "street_house_postcode",
        "toString(match_confidence)": "0.5",
        "latitude": "59.25",
        "geocode_provider": "osm",
        "geocode_precision": "building",
        "coordinate_method": "resolver",
        "coordinate_supporting_point_count": "1",
        # The two per-RECORD columns stay NULL: the resolver's answer names candidates, not
        # one imported source record, and `missing_provenance` does not count them.
        "source_record_id": "\\N",
        "source_record_url": "\\N",
        # The five per-EXTRACT columns the live store check `missing_provenance` gates on
        # (sweden_company/address_geocoding_assets.py) come back non-NULL, and `source_md5`
        # is the row's own `reference_md5`.
        "source_url": PROVENANCE.source_url,
        "source_object_key": PROVENANCE.source_object_key,
        "source_md5": REFERENCE,
        "source_snapshot_at": "2026-08-16 00:00:00.000",
        "source_retrieved_at": "2026-08-16 01:00:00.000",
        "geocode_run_id": RUN_ID,
        "matched_at": "2026-09-06 12:00:00.000",
    }
    assert "\\N" not in [
        row[READ_BACK_COLUMNS.index(column)]
        for column in (
            "source_url",
            "source_object_key",
            "source_md5",
            "source_snapshot_at",
            "source_retrieved_at",
        )
    ]


def test_the_fallback_walks_the_ladder(sections: dict[str, list[list[str]]]) -> None:
    """A tight postcode centroid wins; a postcode centroid past the 3,000 m cap is demoted
    to the city (keyed accent-preserving); an address with neither is absent."""
    assert sections["fallback"] == [
        [POSTCODE_KEY, "postcode", "59.34", "18.09", "11122", "37", "1200"],
        [CITY_KEY, "city", "57.71", "11.96", "GÖTEBORG", "3000", "9000"],
    ]
