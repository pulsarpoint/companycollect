"""Execute build_se_companies_serving_sql() against a real ClickHouse engine.

The SQL-text is generated, so the risk this test covers is behavioural, not spelling: that the
FINAL merges, the served-overlay LEFT JOIN, the per-company JSON aggregation and the
coarse-aware primary-address class all produce the row the companies/geocoding surfaces expect.
A substring test over the builder output cannot prove any of that; a real engine ranking real
rows can.

Runs through clickhouse-local (a local binary, else the pinned server image under Docker, else
the module skips), twice -- once per `join_use_nulls` setting -- and must answer the same both
times, because every served-overlay miss this SELECT reads is guarded by `ifNull`.

The fixture is four companies, each a different shape of the primary-class rule:

  COARSE      one address, its stored geocode_status 'unmatched', but a served-overlay row
              stamps provider='centroid_fallback'/precision='city'. primary_geocode_class must
              be 'coarse' -- the coarse-before-geocoded check firing on provider, NOT the base
              status. Its JSON element carries the overlay's precision/provider.
  POSTAL_BOX  one address, its stored geocode_status 'postal_box' (fallback-eligible since
              2026-08 -- geocode_serving_overlay.py Rule 1), served-overlay row stamps
              provider='centroid_fallback'/precision='postcode'. Same coarse-before-geocoded
              proof as COARSE, but from a box rather than an unmatched street: the class expr
              does not care WHICH non-geocoded status produced the served row.
  PRECISE     two addresses. The primary (visiting_or_postal) has a served PRECISE row
              (geocoded); the secondary (postal) is ambiguous with no served row. The primary
              pick must take the visiting_or_postal row -> class 'geocoded', proving the ranking
              (had it taken the postal row the class would be 'ambiguous'). address_count == 2.
  NOSERVED    one address, stored geocode_status 'unmatched', NO served-overlay row at all.
              primary_geocode_class classifies from the base status -> 'unmatched'. Paired with
              COARSE (same base status, opposite class) this is the coarse-awareness proof.
"""

import json
import subprocess
from datetime import UTC, datetime

import pytest

from dagster_v3.defs.sweden_company.companies_current import (
    build_se_companies_serving_sql,
)
from tests.se_company_ddl import table_block
from tests.test_se_company_person_clickhouse_local import (
    _clickhouse_local_command,
    _literal,
)

pytestmark = pytest.mark.integration

NOW = datetime(2026, 8, 26, 9, tzinfo=UTC)

COARSE = "5560000011"
PRECISE = "5560000022"
NOSERVED = "5560000033"
POSTAL_BOX = "5560000044"
NOADDRESS = "5560000055"

# address_id -> the served-overlay row (precise or coarse). Absent ids have no served row.
PRECISE_LAT, PRECISE_LON = 59.3300, 18.0600
COARSE_LAT, COARSE_LON = 55.6050, 13.0000
POSTAL_BOX_LAT, POSTAL_BOX_LON = 55.3770, 13.1520
COARSE_ADDR = "a" * 64
PRECISE_PRIMARY_ADDR = "b" * 64
PRECISE_SECONDARY_ADDR = "c" * 64
NOSERVED_ADDR = "d" * 64
POSTAL_BOX_ADDR = "e" * 64

# (address_id, geocode_precision, geocode_provider, latitude, longitude)
SERVED_ROWS = (
    (COARSE_ADDR, "city", "centroid_fallback", COARSE_LAT, COARSE_LON),
    (PRECISE_PRIMARY_ADDR, "building", "openstreetmap", PRECISE_LAT, PRECISE_LON),
    (POSTAL_BOX_ADDR, "postcode", "centroid_fallback", POSTAL_BOX_LAT, POSTAL_BOX_LON),
)


def _info_row(company_id: str, legal_name: str) -> str:
    return (
        f"('{company_id}', '{legal_name}', NULL, 'active', NULL, NULL, 'sv', 'scb', "
        "[], [], 0, '', '', NULL, NULL, ['uid-1'], ['e-1'], [], NULL, 'deterministic', "
        f"'copy', 'v1', 'run', {_literal(NOW)})"
    )


INFO_COLUMNS = (
    "company_id, legal_name, legal_form_code, status, incorporation_date, description, "
    "description_language, description_source, description_sources, "
    "description_source_record_uids, description_source_count, primary_nace_code, "
    "primary_sni_code, wikidata_id, lei, source_record_uids, evidence_hashes, "
    "correction_ids, suggestion_id, model_provider, model_name, prompt_version, "
    "source_run_id, resolved_at"
)

ADDRESS_COLUMNS = (
    "company_id, address_key, address_type, street_address, postal_code, city, "
    "address_id, geocode_status, is_current, sources, source_record_uids, "
    "evidence_hashes, source_run_id, resolved_at"
)


def _address_row(
    *,
    company_id: str,
    address_key: str,
    address_type: str,
    street: str,
    postal_code: str,
    city: str,
    address_id: str,
    geocode_status: str,
) -> str:
    return (
        f"('{company_id}', '{address_key}', '{address_type}', '{street}', "
        f"'{postal_code}', '{city}', '{address_id}', '{geocode_status}', true, "
        f"['bolagsverket'], ['uid-1'], ['e-1'], 'run', {_literal(NOW)})"
    )


ADDRESS_ROWS = (
    _address_row(
        company_id=COARSE,
        address_key="k" + "1" * 63,
        address_type="visiting_or_postal",
        street="Storgatan 1",
        postal_code="231 39",
        city="Trelleborg",
        address_id=COARSE_ADDR,
        geocode_status="unmatched",
    ),
    # PRECISE: the postal (secondary) row sorts AFTER the visiting_or_postal (primary) one.
    _address_row(
        company_id=PRECISE,
        address_key="k" + "2" * 63,
        address_type="visiting_or_postal",
        street="Kungsgatan 2",
        postal_code="111 22",
        city="Stockholm",
        address_id=PRECISE_PRIMARY_ADDR,
        geocode_status="matched_exact",
    ),
    _address_row(
        company_id=PRECISE,
        address_key="k" + "3" * 63,
        address_type="postal",
        street="Box 9",
        postal_code="111 00",
        city="Stockholm",
        address_id=PRECISE_SECONDARY_ADDR,
        geocode_status="ambiguous",
    ),
    _address_row(
        company_id=NOSERVED,
        address_key="k" + "4" * 63,
        address_type="visiting_or_postal",
        street="Nygatan 4",
        postal_code="903 25",
        city="Umeå",
        address_id=NOSERVED_ADDR,
        geocode_status="unmatched",
    ),
    _address_row(
        company_id=POSTAL_BOX,
        address_key="k" + "5" * 63,
        address_type="postal",
        street="Box 5305",
        postal_code="102 47",
        city="Stockholm",
        address_id=POSTAL_BOX_ADDR,
        geocode_status="postal_box",
    ),
)


def _served_row(row: tuple[str, str, str, float, float]) -> str:
    address_id, precision, provider, lat, lon = row
    return f"('{address_id}', '{precision}', '{provider}', {lat}, {lon})"


def _served_table_ddl() -> str:
    """A stand-in for corpscout.se_address_geocodes_served (a VIEW in prod, migration 000325).

    The builder reads only these columns off it, and address_id is FixedString(64) exactly as
    the real view exposes -- so the Nullable-vs-non-null join this test exercises is the real
    one. The overlay's own correctness is proven by test_geocode_serving_overlay.py; here it is
    a fixture of precise/coarse rows.
    """
    return (
        "CREATE TABLE corpscout.se_address_geocodes_served (\n"
        "  address_id FixedString(64),\n"
        "  geocode_precision String,\n"
        "  geocode_provider String,\n"
        "  latitude Nullable(Float64),\n"
        "  longitude Nullable(Float64)\n"
        ") ENGINE = MergeTree ORDER BY address_id"
    )


def _script(*, join_use_nulls: int) -> str:
    parts = [
        f"SET join_use_nulls = {join_use_nulls};",
        "CREATE DATABASE IF NOT EXISTS corpscout;",
        table_block("se_company_info"),
        # 000306's label columns, replayed the way prod got them (table_block renders only
        # the CREATE migration).
        "ALTER TABLE corpscout.se_company_info "
        "ADD COLUMN IF NOT EXISTS legal_form_label_en String DEFAULT '' AFTER legal_form_code, "
        "ADD COLUMN IF NOT EXISTS legal_form_label_sv String DEFAULT '' AFTER legal_form_label_en;",
        table_block("se_company_address"),
        _served_table_ddl() + ";",
        # Stubs for the presence-set reads: only the columns the serving SELECT's
        # IN-subqueries touch. Seeds prove each arm independently.
        "CREATE TABLE corpscout.se_companies (company_id String, activity_description Nullable(String), status_reason Nullable(String), bolagsverket_source_record_uid String, updated_from_raw_at DateTime64(3, 'UTC')) ENGINE = ReplacingMergeTree(updated_from_raw_at) ORDER BY company_id;",
        "CREATE TABLE corpscout.text_translations (source_table String, source_column String, source_lang String, target_lang String, source_text_hash UInt64, translated_text String, version UInt32) ENGINE = MergeTree ORDER BY source_text_hash;",
        "CREATE TABLE corpscout.se_code_labels (code_type String, code String, label_en String, version UInt32) ENGINE = MergeTree ORDER BY code;",
        "CREATE TABLE corpscout.se_bolagsverket_financial_metrics (company_id String) ENGINE = MergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.company_identifier (company_id String, issuer_scheme String, country_code String, is_current UInt8, issuer_id String) ENGINE = MergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.esef_financial_metrics (lei String) ENGINE = MergeTree ORDER BY lei;",
        "CREATE TABLE corpscout.se_financial_reports (company_id String) ENGINE = MergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.se_company_person (company_id String) ENGINE = MergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.se_company_person_role (company_id String, sources Array(String)) ENGINE = MergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.company_domains (company_id String, country_code String) ENGINE = MergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.company_traded_symbols (country_code String, company_id String) ENGINE = MergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.se_government_contracts (company_id String) ENGINE = MergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.company_job_history (company_id String, country_code String) ENGINE = MergeTree ORDER BY company_id;",
        # Spine rows: COARSE has a translated activity + a labeled status reason; PRECISE has
        # an activity with NO translation row (text_en must stay ''); NOSERVED has no spine
        # row at all (every spine-derived field folds to '').
        f"INSERT INTO corpscout.se_companies VALUES ('{COARSE}', 'Bygghandel med trävaror', 'konkurs avslutad', 'blv-uid-coarse', {_literal(NOW)}), ('{PRECISE}', 'Handel med maskiner', NULL, 'blv-uid-precise', {_literal(NOW)});",
        "INSERT INTO corpscout.text_translations VALUES ('corpscout.se_companies', 'activity_description', 'sv', 'en', cityHash64('Bygghandel med trävaror'), 'Building trade with timber', 2), ('corpscout.se_companies', 'activity_description', 'sv', 'en', cityHash64('Bygghandel med trävaror'), 'Timber trade (older render)', 1);",
        "INSERT INTO corpscout.se_code_labels VALUES ('status_reason', 'konkurs avslutad', 'Bankruptcy concluded', 1);",
        f"INSERT INTO corpscout.se_bolagsverket_financial_metrics VALUES ('{PRECISE}');",
        f"INSERT INTO corpscout.se_financial_reports VALUES ('{COARSE}');",
        f"INSERT INTO corpscout.se_company_person VALUES ('{PRECISE}');",
        f"INSERT INTO corpscout.se_company_person_role VALUES ('{PRECISE}', ['esef']);",
        # The SE filter must hold: NOSERVED's domain is Norwegian and must not count.
        f"INSERT INTO corpscout.company_domains VALUES ('{COARSE}', 'SE'), ('{NOSERVED}', 'NO');",
        # Market flags: PRECISE is listed (EODHD listings resolve); COARSE won a government
        # contract; POSTAL_BOX has job-ad history, and NOSERVED's job rows are Norwegian
        # so the SE filter must exclude them.
        # PRECISE has an EODHD listing resolve; NOSERVED's is Norwegian and must not count.
        f"INSERT INTO corpscout.company_traded_symbols VALUES ('SE', '{PRECISE}'), ('NO', '{NOSERVED}');",
        f"INSERT INTO corpscout.se_government_contracts VALUES ('{COARSE}');",
        f"INSERT INTO corpscout.company_job_history VALUES ('{POSTAL_BOX}', 'SE'), ('{NOSERVED}', 'NO');",
        f"INSERT INTO corpscout.se_company_info ({INFO_COLUMNS}) VALUES\n"
        + ",\n".join(
            (
                _info_row(COARSE, "Coarse AB"),
                _info_row(PRECISE, "Precise AB"),
                _info_row(NOSERVED, "Noserved AB"),
                _info_row(POSTAL_BOX, "Postal Box AB"),
                _info_row(NOADDRESS, "Addressless AB"),
            )
        )
        + ";",
        f"INSERT INTO corpscout.se_company_address ({ADDRESS_COLUMNS}) VALUES\n"
        + ",\n".join(ADDRESS_ROWS)
        + ";",
        "INSERT INTO corpscout.se_address_geocodes_served "
        "(address_id, geocode_precision, geocode_provider, latitude, longitude) VALUES\n"
        + ",\n".join(_served_row(row) for row in SERVED_ROWS)
        + ";",
        f"SELECT * FROM (\n{build_se_companies_serving_sql()}\n) FORMAT JSONEachRow;",
    ]
    return "\n".join(parts) + "\n"


@pytest.fixture(
    scope="module",
    params=(0, 1),
    ids=("join_use_nulls_off", "join_use_nulls_on"),
)
def rows(request: pytest.FixtureRequest) -> dict[str, dict]:
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
    parsed: dict[str, dict] = {}
    for line in completed.stdout.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        parsed[row["company_id"]] = row
    return parsed


def _addresses(row: dict) -> dict[str, dict]:
    return {element["address_id"]: element for element in json.loads(row["addresses"])}


def test_one_row_per_company_including_the_addressless(rows: dict[str, dict]) -> None:
    # The widened base: a published company with NO current address still gets a row.
    assert set(rows) == {COARSE, PRECISE, NOSERVED, POSTAL_BOX, NOADDRESS}


def test_an_addressless_company_serves_an_empty_address_summary(
    rows: dict[str, dict],
) -> None:
    row = rows[NOADDRESS]
    assert row["has_address"] == 0
    assert row["address_count"] == 0
    assert json.loads(row["addresses"]) == []
    assert row["primary_street_address"] == ""
    assert row["primary_geocode_class"] == ""
    assert row["primary_latitude"] is None
    assert row["primary_longitude"] is None


def test_presence_flags_come_from_the_child_tables(rows: dict[str, dict]) -> None:
    # has_financial: PRECISE via extracted metrics, COARSE via a filed report --
    # the owner's 2026-08-25 widening -- and nothing else.
    assert rows[PRECISE]["has_financial"] == 1
    assert rows[COARSE]["has_financial"] == 1
    assert rows[NOSERVED]["has_financial"] == 0
    assert rows[POSTAL_BOX]["has_financial"] == 0
    # has_domains honors the SE filter: NOSERVED's Norwegian domain must not count.
    assert rows[COARSE]["has_domains"] == 1
    assert rows[NOSERVED]["has_domains"] == 0
    assert rows[PRECISE]["has_people"] == 1
    assert rows[COARSE]["has_people"] == 0
    for company in (COARSE, PRECISE, NOSERVED, POSTAL_BOX):
        assert rows[company]["has_address"] == 1
        assert rows[company]["has_description"] == 0


def test_translations_are_absorbed_from_the_spine_join(rows: dict[str, dict]) -> None:
    # COARSE: activity translated (argMax picks version 2), status reason labeled.
    assert rows[COARSE]["activity_description"] == "Bygghandel med trävaror"
    assert rows[COARSE]["activity_description_en"] == "Building trade with timber"
    assert rows[COARSE]["status_reason"] == "konkurs avslutad"
    assert rows[COARSE]["status_reason_label_en"] == "Bankruptcy concluded"
    assert rows[COARSE]["bolagsverket_source_record_uid"] == "blv-uid-coarse"
    # PRECISE: activity present, no translation row -> '' (never invented).
    assert rows[PRECISE]["activity_description"] == "Handel med maskiner"
    assert rows[PRECISE]["activity_description_en"] == ""
    assert rows[PRECISE]["status_reason"] == ""
    # NOSERVED: no spine row at all -> every spine-derived field folds to ''.
    assert rows[NOSERVED]["activity_description"] == ""
    assert rows[NOSERVED]["activity_description_en"] == ""
    assert rows[NOSERVED]["bolagsverket_source_record_uid"] == ""


def test_market_flags_come_from_their_own_tables(rows: dict[str, dict]) -> None:
    # is_publicly_traded: an EODHD listings-resolve row, SE only.
    assert rows[PRECISE]["is_publicly_traded"] == 1
    assert rows[COARSE]["is_publicly_traded"] == 0
    assert rows[NOSERVED]["is_publicly_traded"] == 0
    # has_government_contracts: exact-matched winner rows only.
    assert rows[COARSE]["has_government_contracts"] == 1
    assert rows[PRECISE]["has_government_contracts"] == 0
    # has_job_ads honors the SE filter: NOSERVED's Norwegian ads must not count.
    assert rows[POSTAL_BOX]["has_job_ads"] == 1
    assert rows[NOSERVED]["has_job_ads"] == 0
    assert rows[NOADDRESS]["has_job_ads"] == 0


def test_source_flags_or_their_arms_together(rows: dict[str, dict]) -> None:
    # Every addressed fixture row's address is sourced from bolagsverket -> B; PRECISE
    # also earns B via its metrics arm. The addressless company has no B arm at all.
    for company in (COARSE, PRECISE, NOSERVED, POSTAL_BOX):
        assert rows[company]["source_bolagsverket"] == 1
    assert rows[NOADDRESS]["source_bolagsverket"] == 0
    # E: PRECISE via its esef role evidence; nothing else has an arm.
    assert rows[PRECISE]["source_esef"] == 1
    assert rows[COARSE]["source_esef"] == 0
    # W: no fixture row carries wikidata evidence.
    for company in rows:
        assert rows[company]["source_wikidata"] == 0


def test_legal_name_comes_from_company_info(rows: dict[str, dict]) -> None:
    assert rows[COARSE]["legal_name"] == "Coarse AB"
    assert rows[PRECISE]["legal_name"] == "Precise AB"
    assert rows[NOSERVED]["legal_name"] == "Noserved AB"
    assert rows[POSTAL_BOX]["legal_name"] == "Postal Box AB"
    assert rows[NOADDRESS]["legal_name"] == "Addressless AB"


def test_address_count_matches_current_addresses(rows: dict[str, dict]) -> None:
    assert rows[COARSE]["address_count"] == 1
    assert rows[PRECISE]["address_count"] == 2
    assert rows[NOSERVED]["address_count"] == 1
    assert rows[POSTAL_BOX]["address_count"] == 1


def test_addresses_json_carries_the_coarse_overlay_precision_and_provider(
    rows: dict[str, dict],
) -> None:
    """The COARSE company's single address element is enriched from the served overlay:
    precision 'city', provider 'centroid_fallback', the centroid coordinate -- while its stored
    geocode_status stays the precise matcher's own 'unmatched'. Accented city preserved."""
    element = _addresses(rows[COARSE])[COARSE_ADDR]
    assert element["geocode_precision"] == "city"
    assert element["geocode_provider"] == "centroid_fallback"
    assert element["geocode_status"] == "unmatched"
    assert element["city"] == "Trelleborg"
    assert float(element["latitude"]) == pytest.approx(COARSE_LAT)
    assert float(element["longitude"]) == pytest.approx(COARSE_LON)


def test_addresses_json_has_both_elements_for_a_multi_address_company(
    rows: dict[str, dict],
) -> None:
    elements = _addresses(rows[PRECISE])
    assert set(elements) == {PRECISE_PRIMARY_ADDR, PRECISE_SECONDARY_ADDR}
    primary = elements[PRECISE_PRIMARY_ADDR]
    assert primary["geocode_precision"] == "building"
    assert primary["geocode_provider"] == "openstreetmap"
    # The secondary carries no served row: every overlay field folds to '' under both settings.
    secondary = elements[PRECISE_SECONDARY_ADDR]
    assert secondary["geocode_precision"] == ""
    assert secondary["geocode_provider"] == ""
    assert secondary["latitude"] == ""


def test_accented_city_survives_json_roundtrip(rows: dict[str, dict]) -> None:
    assert _addresses(rows[NOSERVED])[NOSERVED_ADDR]["city"] == "Umeå"


def test_primary_class_is_coarse_aware_for_a_centroid_fallback_primary(
    rows: dict[str, dict],
) -> None:
    """COARSE's primary is a centroid_fallback row whose base status is 'unmatched'. The class
    must be 'coarse' -- the provider check running BEFORE the geocoded-status check -- not
    'unmatched' (which the base status alone would give) and not 'geocoded'."""
    row = rows[COARSE]
    assert row["primary_geocode_class"] == "coarse"
    assert row["primary_geocode_precision"] == "city"
    assert row["primary_geocode_provider"] == "centroid_fallback"
    assert float(row["primary_latitude"]) == pytest.approx(COARSE_LAT)
    # The primary row's own display fields are carried out beside the geocode summary, for the
    # backoffice list's Company/Address columns and badge tooltip.
    assert row["primary_street_address"] == "Storgatan 1"
    assert row["primary_postal_code"] == "231 39"
    assert row["primary_city"] == "Trelleborg"
    assert row["primary_geocode_status"] == "unmatched"


def test_primary_class_is_coarse_for_a_centroid_fallback_postal_box(
    rows: dict[str, dict],
) -> None:
    """POSTAL_BOX's primary is a centroid_fallback row whose base status is 'postal_box'
    (fallback-eligible since 2026-08). The class must be 'coarse', same as COARSE -- the
    provider check does not care which non-geocoded status produced the served row."""
    row = rows[POSTAL_BOX]
    assert row["primary_geocode_class"] == "coarse"
    assert row["primary_geocode_precision"] == "postcode"
    assert row["primary_geocode_provider"] == "centroid_fallback"
    assert float(row["primary_latitude"]) == pytest.approx(POSTAL_BOX_LAT)
    assert row["primary_street_address"] == "Box 5305"
    assert row["primary_postal_code"] == "102 47"
    assert row["primary_city"] == "Stockholm"
    assert row["primary_geocode_status"] == "postal_box"


def test_primary_pick_takes_the_visiting_or_postal_row(rows: dict[str, dict]) -> None:
    """PRECISE's primary must be the visiting_or_postal (geocoded) row, not the postal
    (ambiguous) one -- had the pick ranked wrong the class would be 'ambiguous'."""
    row = rows[PRECISE]
    assert row["primary_geocode_class"] == "geocoded"
    assert row["primary_geocode_provider"] == "openstreetmap"
    assert float(row["primary_latitude"]) == pytest.approx(PRECISE_LAT)
    # The display fields come from the SAME primary row -- the visiting_or_postal one --
    # not the postal secondary (whose street is "Box 9").
    assert row["primary_street_address"] == "Kungsgatan 2"
    assert row["primary_city"] == "Stockholm"
    assert row["primary_geocode_status"] == "matched_exact"


def test_primary_class_falls_back_to_base_status_with_no_served_row(
    rows: dict[str, dict],
) -> None:
    """NOSERVED's primary has no served-overlay row: the class comes from its stored
    geocode_status ('unmatched'). Same base status as COARSE, opposite class -- the overlay is
    what separates them."""
    row = rows[NOSERVED]
    assert row["primary_geocode_class"] == "unmatched"
    assert row["primary_geocode_provider"] == ""
    assert row["primary_latitude"] is None
