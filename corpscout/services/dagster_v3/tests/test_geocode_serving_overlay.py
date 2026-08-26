"""Executes the SE geocode SERVING overlay against the deployed ClickHouse version in a
disposable clickhouse-local, over every shape the three overlay rules have to get right.

The overlay is the crux of the coarse-centroid feature and the one place a subtly wrong
rule would leak a coordinate no downstream failure would ever reveal: it fills an
`unmatched`/`ambiguous` identity with a coarse postcode-or-city centroid while a precise
outcome, present or later, must ALWAYS win. Substring tests over the generated SQL cannot
prove that; a real engine ranking the real rows can.

The fixture is ten identities, each a different way of getting the rules wrong. `postal_box`
joined `unmatched`/`ambiguous` as fallback-eligible 2026-08 (owner-approved: a box postcode is
a dedicated range tied to a postal town, so the coarse centroid is as honest for a box as for
an unmatched street) -- POSTAL_BOX_PC and POSTAL_BOX_CITY prove it climbs the SAME ladder,
and INVALID_ADDRESS_WINS proves the widening did not go further than that one status:

  GEOCODED_WINS        a resolver matched_exact whose postcode AND city both HAVE centroids.
                       Serves its precise coordinate anyway -- rule 1, precise always wins
                       even when a centroid is sitting right there.
  FOREIGN_WINS         a `foreign_address` whose postcode/city both have centroids. Not
                       eligible: passes through UNCHANGED.
  INVALID_ADDRESS_WINS an `invalid_address` whose post_town HAS a city centroid. Not
                       eligible either: proves postal_box's addition didn't widen the gate
                       to every non-geocoded status, only to postal_box itself.
  POSTAL_BOX_PC        a `postal_box` whose postcode is in se_postcode_centroids (tight
                       spread) -> POSTCODE-precision centroid, city centroid also present so
                       this proves the ladder still prefers the finer postcode rung for a box.
  POSTAL_BOX_CITY      a `postal_box` whose postcode is ABSENT from se_postcode_centroids but
                       whose post_town IS in se_city_centroids -> CITY-precision centroid.
  UNMATCHED_PC         the (b) case: unmatched, postcode in se_postcode_centroids (tight
                       spread) -> POSTCODE-precision centroid. City centroid also present, so
                       this also proves the ladder prefers the finer postcode rung.
  AMBIGUOUS_PC         `ambiguous` (not just unmatched) is eligible too -> postcode centroid.
  UNMATCHED_CITY       the (c) STAVSTENSV/Trelleborg case: unmatched, postcode ABSENT from
                       se_postcode_centroids but post_town in se_city_centroids -> CITY
                       centroid.
  LOOSE_POSTCODE       unmatched, postcode centroid present but its spread exceeds
                       POSTCODE_SPREAD_MAX_METERS -> demoted to the CITY centroid. Also
                       exercises the accent-preserving city key (post_town "Umeå" -> "UMEÅ").
  BARE_UNMATCHED       unmatched with NEITHER centroid -> the original unmatched row,
                       unchanged.
"""

import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.sweden_company import shared_addresses
from dagster_v3.defs.sweden_company.geocode_serving_overlay import (
    CITY_PRECISION,
    GEOCODE_FALLBACK_PROVIDER,
    GEOCODE_FALLBACK_STATUS,
    POSTCODE_PRECISION,
    build_served_geocodes_sql,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
    STORE_COLUMNS,
)
from tests.test_se_company_person_clickhouse_local import (
    _clickhouse_local_command,
    _literal,
)

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
# 000274 creates se_addresses_current, 000278 adds the street_name/house_number/unit
# columns SHARED_ADDRESS_COLUMNS carries, 000317 creates the store, 000323/000324 create
# the two centroid reference tables the overlay joins.
MIGRATIONS = (
    "000274_corpscout_se_shared_addresses.up.sql",
    "000278_corpscout_se_address_components.up.sql",
    "000317_corpscout_se_address_geocodes_store.up.sql",
    "000323_corpscout_se_postcode_centroids.up.sql",
    "000324_corpscout_se_city_centroids.up.sql",
)
NEEDED_TABLES = frozenset({
    "se_addresses_current",
    "se_address_geocodes",
    "se_postcode_centroids",
    "se_city_centroids",
})

STORE = QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE
ADDRESSES = shared_addresses.QUALIFIED_CLICKHOUSE_SHARED_ADDRESSES_TABLE
POSTCODE_CENTROIDS = "corpscout.se_postcode_centroids"
CITY_CENTROIDS = "corpscout.se_city_centroids"
POLICY = "se-address-resolution-policy-v5"

(
    GEOCODED_WINS,
    FOREIGN_WINS,
    INVALID_ADDRESS_WINS,
    POSTAL_BOX_PC,
    POSTAL_BOX_CITY,
    UNMATCHED_PC,
    AMBIGUOUS_PC,
    UNMATCHED_CITY,
    LOOSE_POSTCODE,
    BARE_UNMATCHED,
) = (character * 64 for character in "1234567890")

IDENTITIES = (
    GEOCODED_WINS,
    FOREIGN_WINS,
    INVALID_ADDRESS_WINS,
    POSTAL_BOX_PC,
    POSTAL_BOX_CITY,
    UNMATCHED_PC,
    AMBIGUOUS_PC,
    UNMATCHED_CITY,
    LOOSE_POSTCODE,
    BARE_UNMATCHED,
)

SNAPSHOT = datetime(2026, 8, 1, 1, tzinfo=UTC)
MATCHED_AT = datetime(2026, 8, 1, 3, tzinfo=UTC)

# The precise coordinate GEOCODED_WINS serves. Deliberately far from the 11122 postcode
# centroid below, so "precise wins" is visible in the coordinate, not just the label.
PRECISE_LAT, PRECISE_LON = 59.9990, 17.9990

# --- centroid reference rows (what the derivation asset would publish) --------------------
# key -> (latitude, longitude, point_count, spread_meters)
POSTCODE_CENTROID_ROWS = {
    "11122": (59.3300, 18.0600, 40, 800.0),  # Stockholm; GEOCODED_WINS must ignore it
    "23139": (55.3770, 13.1520, 12, 500.0),  # Trelleborg tight -> served for PC cases
    "90325": (63.8300, 20.2600, 8, 9000.0),  # Umea LOOSE: spread > 3000 -> demoted
}
CITY_CENTROID_ROWS = {
    "TRELLEBORG": (55.3750, 13.1500, 300, 2500.0),
    "UMEÅ": (63.8250, 20.2630, 220, 2800.0),  # accent-preserving key: post_town "Umeå"
    "STOCKHOLM": (59.3290, 18.0620, 900, 2900.0),
}

# --- per-identity address (postcode/post_town feed the join keys) -------------------------
# identity -> (postal_code, post_town)
ADDRESS_BY_IDENTITY = {
    GEOCODED_WINS: ("111 22", "Stockholm"),
    FOREIGN_WINS: ("231 39", "Trelleborg"),
    INVALID_ADDRESS_WINS: ("231 39", "Trelleborg"),  # both centroids present, still ineligible
    POSTAL_BOX_PC: ("231 39", "Trelleborg"),  # tight postcode centroid -> postcode tier
    POSTAL_BOX_CITY: ("999 99", "Trelleborg"),  # 99999 has no postcode centroid -> city tier
    UNMATCHED_PC: ("231 39", "Trelleborg"),
    AMBIGUOUS_PC: ("231 39", "Trelleborg"),
    UNMATCHED_CITY: ("999 99", "Trelleborg"),  # 99999 has no postcode centroid
    LOOSE_POSTCODE: ("903 25", "Umeå"),
    BARE_UNMATCHED: ("000 00", "Nowhere"),  # neither centroid exists
}

# identity -> store match_status
STATUS_BY_IDENTITY = {
    GEOCODED_WINS: "matched_exact",
    FOREIGN_WINS: "foreign_address",
    INVALID_ADDRESS_WINS: "invalid_address",
    POSTAL_BOX_PC: "postal_box",
    POSTAL_BOX_CITY: "postal_box",
    UNMATCHED_PC: "unmatched",
    AMBIGUOUS_PC: "ambiguous",
    UNMATCHED_CITY: "unmatched",
    LOOSE_POSTCODE: "unmatched",
    BARE_UNMATCHED: "unmatched",
}

_STORE_DEFAULTS: dict[str, str] = {
    "address_id": "",
    "policy_version": f"'{POLICY}'",
    "reference_md5": "'md5-1'",
    "address_identity_run_id": "'identity-run-1'",
    "normalized_match_key": "'se|storgatan 1|11122|stockholm'",
    "match_status": "'unmatched'",
    "candidate_count": "0",
    "candidate_record_ids": "[]",
    "candidate_record_urls": "[]",
    "match_method": "''",
    "match_confidence": "0.0",
    "latitude": "NULL",
    "longitude": "NULL",
    "geocode_provider": "'openstreetmap'",
    "geocode_precision": "''",
    "coordinate_method": "NULL",
    "coordinate_locality": "NULL",
    "coordinate_supporting_point_count": "0",
    "coordinate_spread_meters": "NULL",
    "source_record_id": "NULL",
    "source_record_url": "NULL",
    "source_url": "'https://download.geofabrik.de/sweden-latest.osm.pbf'",
    "source_object_key": "'osm/sweden-latest.osm.pbf'",
    "source_md5": "'md5-1'",
    "source_snapshot_at": _literal(SNAPSHOT),
    "source_retrieved_at": _literal(SNAPSHOT),
    "geocode_run_id": "'run-1'",
    "matched_at": _literal(MATCHED_AT),
}

_ADDRESS_DEFAULTS: dict[str, str] = {
    "address_id": "",
    "canonical_display_address": "'Storgatan 1'",
    "representative_address_source": "'bolagsverket'",
    "street_address": "'Storgatan 1'",
    "street_name": "'Storgatan'",
    "house_number": "'1'",
    "unit": "''",
    "postal_code": "''",
    "post_town": "''",
    "country_code": "'SE'",
    "address_kind": "'street'",
    "normalized_street": "'storgatan 1'",
    "normalized_postal_code": "''",
    "normalized_post_town": "''",
    "address_types": "['postal']",
    "address_sources": "['bolagsverket']",
    "company_count": "1",
    "evidence_count": "1",
    "first_observed_at": _literal(MATCHED_AT),
    "last_observed_at": _literal(MATCHED_AT),
    "address_identity_run_id": "'identity-run-1'",
    "address_identity_built_at": _literal(MATCHED_AT),
}


def _row(defaults: dict[str, str], columns: tuple[str, ...], **overrides: str) -> str:
    row = {**defaults, **overrides}
    assert set(row) == set(columns), set(row) ^ set(columns)
    return "(" + ", ".join(row[column] for column in columns) + ")"


def _store_row(identity: str) -> str:
    status = STATUS_BY_IDENTITY[identity]
    overrides: dict[str, str] = {
        "address_id": f"'{identity}'",
        "match_status": f"'{status}'",
    }
    if status == "matched_exact":
        overrides |= {
            "latitude": str(PRECISE_LAT),
            "longitude": str(PRECISE_LON),
            "geocode_precision": "'building'",
            "geocode_provider": "'openstreetmap'",
            "candidate_count": "1",
            "match_method": "'country_street_house_exact_unique'",
            "match_confidence": "1.0",
        }
    return _row(_STORE_DEFAULTS, STORE_COLUMNS, **overrides)


def _address_row(identity: str) -> str:
    postal_code, post_town = ADDRESS_BY_IDENTITY[identity]
    return _row(
        _ADDRESS_DEFAULTS,
        shared_addresses.SHARED_ADDRESS_COLUMNS,
        address_id=f"'{identity}'",
        postal_code=f"'{postal_code}'",
        post_town=f"'{post_town}'",
    )


def _centroid_rows(rows: dict[str, tuple[float, float, int, float]]) -> str:
    return ",\n".join(
        f"('{key}', {lat}, {lon}, {n}, {spread}, {_literal(SNAPSHOT)})"
        for key, (lat, lon, n, spread) in rows.items()
    )


def _insert(table: str, columns: tuple[str, ...], rows: str) -> str:
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n{rows};"


SERVED_COLUMNS = (
    "address_id",
    "match_status",
    "latitude",
    "longitude",
    "geocode_precision",
    "geocode_provider",
)


def _schema_statements() -> list[str]:
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if not statement:
                continue
            if statement.upper().startswith("CREATE DATABASE"):
                statements.append(statement)
                continue
            head = statement.upper()
            if head.startswith(("CREATE TABLE", "ALTER TABLE")):
                if any(f"corpscout.{table}" in statement for table in NEEDED_TABLES):
                    statements.append(statement)
    return statements


def _script(*, join_use_nulls: int) -> str:
    parts = [f"SET join_use_nulls = {join_use_nulls};"]
    parts.extend(f"{statement};" for statement in _schema_statements())
    parts.append(
        _insert(
            ADDRESSES,
            shared_addresses.SHARED_ADDRESS_COLUMNS,
            ",\n".join(_address_row(identity) for identity in IDENTITIES),
        )
    )
    parts.append(
        _insert(
            STORE,
            STORE_COLUMNS,
            ",\n".join(_store_row(identity) for identity in IDENTITIES),
        )
    )
    parts.append(
        _insert(
            POSTCODE_CENTROIDS,
            ("key", "latitude", "longitude", "point_count", "spread_meters",
             "source_snapshot_at"),
            _centroid_rows(POSTCODE_CENTROID_ROWS),
        )
    )
    parts.append(
        _insert(
            CITY_CENTROIDS,
            ("key", "latitude", "longitude", "point_count", "spread_meters",
             "source_snapshot_at"),
            _centroid_rows(CITY_CENTROID_ROWS),
        )
    )
    served = build_served_geocodes_sql(columns=SERVED_COLUMNS)
    parts.append(
        f"SELECT {', '.join(SERVED_COLUMNS)} FROM (\n{served}\n)"
        " ORDER BY address_id FORMAT TSV;"
    )
    return "\n".join(parts) + "\n"


@pytest.fixture(
    scope="module",
    params=(0, 1),
    ids=("join_use_nulls_off", "join_use_nulls_on"),
)
def served(request: pytest.FixtureRequest) -> dict[str, tuple[str, ...]]:
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(
            command,
            input=_script(join_use_nulls=request.param),
            capture_output=True,
            text=True,
            timeout=900,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    rows: dict[str, tuple[str, ...]] = {}
    for line in completed.stdout.splitlines():
        if not line.strip():
            continue
        fields = line.split("\t")
        rows[fields[0]] = tuple(fields[1:])
    return rows


def _coord(value: str) -> float:
    return float(value)


def test_every_identity_is_served_exactly_once(
    served: dict[str, tuple[str, ...]],
) -> None:
    assert set(served) == set(IDENTITIES)


def test_a_precise_match_always_wins_over_an_available_centroid(
    served: dict[str, tuple[str, ...]],
) -> None:
    """GEOCODED_WINS has a postcode centroid (11122) AND a city centroid (STOCKHOLM) sitting
    right there, and still serves its precise coordinate. Break the eligibility gate so the
    overlay fires on geocoded rows and THIS is what changes -- the precise coordinate is
    overwritten by the 11122 centroid, 400+ km of nonsense with the label to match."""
    status, lat, lon, precision, provider = served[GEOCODED_WINS]
    assert status == "matched_exact"
    assert _coord(lat) == pytest.approx(PRECISE_LAT)
    assert _coord(lon) == pytest.approx(PRECISE_LON)
    assert precision == "building"
    assert provider == "openstreetmap"


def test_non_eligible_statuses_are_never_overlaid(
    served: dict[str, tuple[str, ...]],
) -> None:
    """foreign_address and invalid_address both have centroids available and both pass
    through untouched: the overlay fires ONLY for unmatched/ambiguous/postal_box. Proves the
    postal_box widening did not spill over to the other non-geocoded statuses."""
    for identity, expected_status in (
        (FOREIGN_WINS, "foreign_address"),
        (INVALID_ADDRESS_WINS, "invalid_address"),
    ):
        status, lat, lon, precision, provider = served[identity]
        assert status == expected_status
        assert lat == "\\N" and lon == "\\N"  # never geocoded, no coordinate
        assert precision == ""
        assert provider == "openstreetmap"


def test_postal_box_with_a_tight_postcode_gets_the_postcode_centroid(
    served: dict[str, tuple[str, ...]],
) -> None:
    """The (b) case for postal_box: eligible now, and the ladder still prefers the finer
    postcode rung over the also-available city centroid."""
    lat, lon, expected = _centroid_lat_lon_precision(POSTAL_BOX_PC)
    assert expected == POSTCODE_PRECISION
    status, got_lat, got_lon, precision, provider = served[POSTAL_BOX_PC]
    assert status == GEOCODE_FALLBACK_STATUS
    assert precision == POSTCODE_PRECISION
    assert provider == GEOCODE_FALLBACK_PROVIDER
    assert _coord(got_lat) == pytest.approx(lat)
    assert _coord(got_lon) == pytest.approx(lon)


def test_postal_box_with_only_a_city_centroid_gets_the_city_centroid(
    served: dict[str, tuple[str, ...]],
) -> None:
    """The (a) case for postal_box: post_town Trelleborg has a city centroid, and the
    99999 postcode has none, so the box is filled by the CITY rung."""
    status, got_lat, got_lon, precision, provider = served[POSTAL_BOX_CITY]
    assert status == GEOCODE_FALLBACK_STATUS
    assert precision == CITY_PRECISION
    assert provider == GEOCODE_FALLBACK_PROVIDER
    assert _coord(got_lat) == pytest.approx(CITY_CENTROID_ROWS["TRELLEBORG"][0])
    assert _coord(got_lon) == pytest.approx(CITY_CENTROID_ROWS["TRELLEBORG"][1])


def test_unmatched_with_a_tight_postcode_gets_the_postcode_centroid(
    served: dict[str, tuple[str, ...]],
) -> None:
    """The (b) case. The city centroid for Trelleborg is ALSO present, so this proves the
    ladder takes the finer postcode rung when both exist."""
    lat, lon, expected = _centroid_lat_lon_precision(UNMATCHED_PC)
    assert expected == POSTCODE_PRECISION
    status, got_lat, got_lon, precision, provider = served[UNMATCHED_PC]
    assert status == GEOCODE_FALLBACK_STATUS
    assert precision == POSTCODE_PRECISION
    assert provider == GEOCODE_FALLBACK_PROVIDER
    assert _coord(got_lat) == pytest.approx(lat)
    assert _coord(got_lon) == pytest.approx(lon)


def test_ambiguous_is_eligible_too(served: dict[str, tuple[str, ...]]) -> None:
    """Not just `unmatched`: an `ambiguous` precise outcome is filled the same way."""
    status, got_lat, _got_lon, precision, provider = served[AMBIGUOUS_PC]
    assert status == GEOCODE_FALLBACK_STATUS
    assert precision == POSTCODE_PRECISION
    assert provider == GEOCODE_FALLBACK_PROVIDER
    assert _coord(got_lat) == pytest.approx(POSTCODE_CENTROID_ROWS["23139"][0])


def test_no_postcode_centroid_falls_back_to_the_city_centroid(
    served: dict[str, tuple[str, ...]],
) -> None:
    """The (c) STAVSTENSV/Trelleborg case: postcode 99999 is absent from the postcode
    centroids, post_town Trelleborg is present in the city centroids."""
    status, got_lat, got_lon, precision, provider = served[UNMATCHED_CITY]
    assert status == GEOCODE_FALLBACK_STATUS
    assert precision == CITY_PRECISION
    assert provider == GEOCODE_FALLBACK_PROVIDER
    assert _coord(got_lat) == pytest.approx(CITY_CENTROID_ROWS["TRELLEBORG"][0])
    assert _coord(got_lon) == pytest.approx(CITY_CENTROID_ROWS["TRELLEBORG"][1])


def test_a_too_loose_postcode_centroid_is_demoted_to_the_city(
    served: dict[str, tuple[str, ...]],
) -> None:
    """LOOSE_POSTCODE's 90325 centroid exists but its spread exceeds
    POSTCODE_SPREAD_MAX_METERS, so the finer rung is refused and the city centroid serves.
    The accent-preserving city key is exercised here: post_town "Umeå" -> key "UMEÅ"."""
    status, got_lat, got_lon, precision, provider = served[LOOSE_POSTCODE]
    assert status == GEOCODE_FALLBACK_STATUS
    assert precision == CITY_PRECISION
    assert provider == GEOCODE_FALLBACK_PROVIDER
    assert _coord(got_lat) == pytest.approx(CITY_CENTROID_ROWS["UMEÅ"][0])
    assert _coord(got_lon) == pytest.approx(CITY_CENTROID_ROWS["UMEÅ"][1])


def test_unmatched_with_no_centroid_at_all_is_left_unchanged(
    served: dict[str, tuple[str, ...]],
) -> None:
    """Eligible but neither centroid exists: the original unmatched row survives verbatim,
    no coordinate manufactured."""
    status, lat, lon, precision, provider = served[BARE_UNMATCHED]
    assert status == "unmatched"
    assert lat == "\\N" and lon == "\\N"
    assert precision == ""
    assert provider == "openstreetmap"


def _centroid_lat_lon_precision(identity: str) -> tuple[float, float, str]:
    postal_code, _post_town = ADDRESS_BY_IDENTITY[identity]
    key = postal_code.replace(" ", "")
    lat, lon, _n, _spread = POSTCODE_CENTROID_ROWS[key]
    return lat, lon, POSTCODE_PRECISION
