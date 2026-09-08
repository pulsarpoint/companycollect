"""Execute build_se_companies_serving_sql() against a real ClickHouse engine.

The SQL-text is generated, so the risk this test covers is behavioural, not spelling: that the
FINAL merges, the `active = 1` filter, the per-company JSON aggregation and the coarse-aware
primary-address class all produce the row the companies/geocoding surfaces expect. A substring
test over the builder output cannot prove any of that; a real engine ranking real rows can.

Runs through clickhouse-local (a local binary, else the pinned server image under Docker, else
the module skips), twice -- once per `join_use_nulls` setting -- and must answer the same both
times, because every LEFT JOIN this SELECT still makes (the spine, the translations, the
aggregation) is guarded by `ifNull`/`coalesce`.

SINCE SLICE 4a the address half reads the ADDRESS ENTITY, `corpscout.se_company_address_v2`
(migration 000384): one row per company and published address, `active = 1` for the published
ones, `kinds` an array, and the geocode outcome -- status, precision, coordinate -- on the row
itself. There is no served-overlay join any more; the `centroid_fallback` provider the overlay
used to stamp is DERIVED from `geocode_status = 'matched_area'`, which is what the centroid
overlay writes.

The fixture is seven companies, each a different shape of the primary-class or ranking rule:

  COARSE      one address, geocode_status 'matched_area' with a city-precision centroid.
              primary_geocode_class must be 'coarse' -- the derived provider firing the
              coarse-before-geocoded check, NOT 'geocoded' (matched_area is inside the
              GEOCODED vocabulary and the status alone would say so).
  POSTAL_BOX  one box address, also 'matched_area' but at postcode precision. Same
              coarse-before-geocoded proof from a box rather than a street.
  PRECISE     two addresses. The primary (visiting_or_postal) is matched_exact with a
              building coordinate; the secondary (postal) is ambiguous with no coordinate.
              The primary pick must take the visiting_or_postal row -> class 'geocoded'
              (had it taken the postal row the class would be 'ambiguous'). address_count == 2.
  VISITING    two addresses, NEITHER visiting_or_postal: a 'visiting' one that is matched_exact
              and a 'postal' one that is unmatched and whose key sorts FIRST. The pick must
              take the visiting row -> 'geocoded', proving rank 2 beats rank 3 and that the
              address_key tiebreak does not override the kind ranks.
  UNGEOCODED  one address, geocode_status 'unmatched', no coordinate. The derived provider is
              '' and the class comes from the base status -> 'unmatched'. Paired with COARSE
              (both ungeocoded by the precise matcher, opposite classes) this is the
              coarse-awareness proof.
  HIDDEN      one ACTIVE postal address (matched_exact, with a care-of prefix on its line) and
              one INACTIVE visiting_or_postal address that outranks it. Only the active row may
              reach the serving row: address_count == 1, class 'geocoded', and the JSON element
              carries the active row's key as its address_id.
  NOADDRESS   no address row at all -- still one serving row, with an empty address summary.
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
UNGEOCODED = "5560000033"
POSTAL_BOX = "5560000044"
NOADDRESS = "5560000055"
HIDDEN = "5560000066"
VISITING = "5560000077"

ADDRESSED = (COARSE, PRECISE, UNGEOCODED, POSTAL_BOX, HIDDEN, VISITING)

PRECISE_LAT, PRECISE_LON = 59.3300, 18.0600
COARSE_LAT, COARSE_LON = 55.6050, 13.0000
POSTAL_BOX_LAT, POSTAL_BOX_LON = 55.3770, 13.1520
HIDDEN_LAT, HIDDEN_LON = 57.7080, 11.9740
VISITING_LAT, VISITING_LON = 63.8250, 20.2630

# The entity's own keys ARE the serving JSON's address_id since slice 4a. Each is 64 chars,
# the FixedString(64) width, and the leading letter fixes its sort position for the tiebreak.
COARSE_KEY = "c" + "1" * 63
PRECISE_PRIMARY_KEY = "b" + "2" * 63
PRECISE_SECONDARY_KEY = "d" + "3" * 63
UNGEOCODED_KEY = "n" + "4" * 63
POSTAL_BOX_KEY = "p" + "5" * 63
# The inactive row sorts BEFORE the active one and carries the higher kind rank, so a missing
# `active = 1` filter would visibly hand it the primary pick.
HIDDEN_ACTIVE_KEY = "h" + "6" * 63
HIDDEN_INACTIVE_KEY = "a" + "6" * 63
# The postal row sorts BEFORE the visiting one: the kind rank, not the key, must decide.
VISITING_KEY = "v" + "7" * 63
VISITING_POSTAL_KEY = "a" + "7" * 63


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

# The entity columns the serving SELECT reads, plus the provenance ones a published row always
# carries. Everything omitted takes its type default -- the SELECT never touches it.
ADDRESS_COLUMNS = (
    "company_id, address_key, box, postal_code, city, country_code, normalized_address, "
    "kinds, sources, slots, text_source, active, inactive_reason, latitude, longitude, "
    "geocode_status, geocode_precision, folded_at, fold_version, source_run_id"
)


def _address_row(
    *,
    company_id: str,
    address_key: str,
    kinds: tuple[str, ...],
    line: str,
    postal_code: str,
    city: str,
    geocode_status: str,
    geocode_precision: str = "",
    latitude: float | None = None,
    longitude: float | None = None,
    box: str | None = None,
    active: int = 1,
    inactive_reason: str = "",
) -> str:
    kinds_sql = "[" + ", ".join(f"'{kind}'" for kind in kinds) + "]"
    return (
        f"('{company_id}', '{address_key}', {_literal(box) if box else 'NULL'}, "
        f"'{postal_code}', '{city}', 'se', '{line}', {kinds_sql}, ['bolagsverket'], ['0'], "
        f"'bolagsverket', {active}, '{inactive_reason}', "
        f"{'NULL' if latitude is None else latitude}, "
        f"{'NULL' if longitude is None else longitude}, "
        f"'{geocode_status}', '{geocode_precision}', {_literal(NOW)}, 'v1', 'run')"
    )


ADDRESS_ROWS = (
    # COARSE: the centroid overlay's own outcome -- matched_area at city precision.
    _address_row(
        company_id=COARSE,
        address_key=COARSE_KEY,
        kinds=("visiting_or_postal",),
        line="Storgatan 1, 231 39 Trelleborg",
        postal_code="231 39",
        city="Trelleborg",
        geocode_status="matched_area",
        geocode_precision="city",
        latitude=COARSE_LAT,
        longitude=COARSE_LON,
    ),
    # PRECISE: the postal (secondary) row must lose to the visiting_or_postal (primary) one.
    _address_row(
        company_id=PRECISE,
        address_key=PRECISE_PRIMARY_KEY,
        kinds=("visiting_or_postal",),
        line="Kungsgatan 2, 111 22 Stockholm",
        postal_code="111 22",
        city="Stockholm",
        geocode_status="matched_exact",
        geocode_precision="building",
        latitude=PRECISE_LAT,
        longitude=PRECISE_LON,
    ),
    _address_row(
        company_id=PRECISE,
        address_key=PRECISE_SECONDARY_KEY,
        kinds=("postal",),
        line="Box 9, 111 00 Stockholm",
        postal_code="111 00",
        city="Stockholm",
        geocode_status="ambiguous",
        box="Box 9",
    ),
    _address_row(
        company_id=UNGEOCODED,
        address_key=UNGEOCODED_KEY,
        kinds=("visiting_or_postal",),
        line="Nygatan 4, 903 25 Umeå",
        postal_code="903 25",
        city="Umeå",
        geocode_status="unmatched",
    ),
    _address_row(
        company_id=POSTAL_BOX,
        address_key=POSTAL_BOX_KEY,
        kinds=("postal",),
        line="Box 5305, 102 47 Stockholm",
        postal_code="102 47",
        city="Stockholm",
        geocode_status="matched_area",
        geocode_precision="postcode",
        latitude=POSTAL_BOX_LAT,
        longitude=POSTAL_BOX_LON,
        box="Box 5305",
    ),
    # HIDDEN: the published row carries a care-of prefix -- the street expression strips the
    # trailing postcode and town off the line and keeps everything before it.
    _address_row(
        company_id=HIDDEN,
        address_key=HIDDEN_ACTIVE_KEY,
        kinds=("postal",),
        line="c/o Axfast AB, Vasagatan 7, 411 24 Göteborg",
        postal_code="411 24",
        city="Göteborg",
        geocode_status="matched_exact",
        geocode_precision="building",
        latitude=HIDDEN_LAT,
        longitude=HIDDEN_LON,
    ),
    _address_row(
        company_id=HIDDEN,
        address_key=HIDDEN_INACTIVE_KEY,
        kinds=("visiting_or_postal",),
        line="Withdrawn Gatan 9, 411 25 Göteborg",
        postal_code="411 25",
        city="Göteborg",
        geocode_status="unmatched",
        active=0,
        inactive_reason="withdrawn",
    ),
    # VISITING: neither row is visiting_or_postal, and the postal one sorts first by key.
    _address_row(
        company_id=VISITING,
        address_key=VISITING_KEY,
        kinds=("visiting",),
        line="Rådhusesplanaden 8, 903 28 Umeå",
        postal_code="903 28",
        city="Umeå",
        geocode_status="matched_exact",
        geocode_precision="building",
        latitude=VISITING_LAT,
        longitude=VISITING_LON,
    ),
    _address_row(
        company_id=VISITING,
        address_key=VISITING_POSTAL_KEY,
        kinds=("postal",),
        line="Box 1, 903 01 Umeå",
        postal_code="903 01",
        city="Umeå",
        geocode_status="unmatched",
        box="Box 1",
    ),
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
        # The address entity itself (migration 000384) -- read FINAL, active rows only.
        table_block("se_company_address_v2"),
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
        # an activity with NO translation row (text_en must stay ''); UNGEOCODED has no spine
        # row at all (every spine-derived field folds to '').
        f"INSERT INTO corpscout.se_companies VALUES ('{COARSE}', 'Bygghandel med trävaror', 'konkurs avslutad', 'blv-uid-coarse', {_literal(NOW)}), ('{PRECISE}', 'Handel med maskiner', NULL, 'blv-uid-precise', {_literal(NOW)});",
        "INSERT INTO corpscout.text_translations VALUES ('corpscout.se_companies', 'activity_description', 'sv', 'en', cityHash64('Bygghandel med trävaror'), 'Building trade with timber', 2), ('corpscout.se_companies', 'activity_description', 'sv', 'en', cityHash64('Bygghandel med trävaror'), 'Timber trade (older render)', 1);",
        "INSERT INTO corpscout.se_code_labels VALUES ('status_reason', 'konkurs avslutad', 'Bankruptcy concluded', 1);",
        f"INSERT INTO corpscout.se_bolagsverket_financial_metrics VALUES ('{PRECISE}');",
        f"INSERT INTO corpscout.se_financial_reports VALUES ('{COARSE}');",
        f"INSERT INTO corpscout.se_company_person VALUES ('{PRECISE}');",
        f"INSERT INTO corpscout.se_company_person_role VALUES ('{PRECISE}', ['esef']);",
        # The SE filter must hold: UNGEOCODED's domain is Norwegian and must not count.
        f"INSERT INTO corpscout.company_domains VALUES ('{COARSE}', 'SE'), ('{UNGEOCODED}', 'NO');",
        # Market flags: PRECISE is listed (EODHD listings resolve); COARSE won a government
        # contract; POSTAL_BOX has job-ad history, and UNGEOCODED's job rows are Norwegian
        # so the SE filter must exclude them.
        f"INSERT INTO corpscout.company_traded_symbols VALUES ('SE', '{PRECISE}'), ('NO', '{UNGEOCODED}');",
        f"INSERT INTO corpscout.se_government_contracts VALUES ('{COARSE}');",
        f"INSERT INTO corpscout.company_job_history VALUES ('{POSTAL_BOX}', 'SE'), ('{UNGEOCODED}', 'NO');",
        f"INSERT INTO corpscout.se_company_info ({INFO_COLUMNS}) VALUES\n"
        + ",\n".join(
            (
                _info_row(COARSE, "Coarse AB"),
                _info_row(PRECISE, "Precise AB"),
                _info_row(UNGEOCODED, "Ungeocoded AB"),
                _info_row(POSTAL_BOX, "Postal Box AB"),
                _info_row(NOADDRESS, "Addressless AB"),
                _info_row(HIDDEN, "Hidden Row AB"),
                _info_row(VISITING, "Visiting AB"),
            )
        )
        + ";",
        f"INSERT INTO corpscout.se_company_address_v2 ({ADDRESS_COLUMNS}) VALUES\n"
        + ",\n".join(ADDRESS_ROWS)
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
    # The widened base: a published company with NO published address still gets a row.
    assert set(rows) == {*ADDRESSED, NOADDRESS}


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
    assert rows[UNGEOCODED]["has_financial"] == 0
    assert rows[POSTAL_BOX]["has_financial"] == 0
    # has_domains honors the SE filter: UNGEOCODED's Norwegian domain must not count.
    assert rows[COARSE]["has_domains"] == 1
    assert rows[UNGEOCODED]["has_domains"] == 0
    assert rows[PRECISE]["has_people"] == 1
    assert rows[COARSE]["has_people"] == 0
    for company in ADDRESSED:
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
    # UNGEOCODED: no spine row at all -> every spine-derived field folds to ''.
    assert rows[UNGEOCODED]["activity_description"] == ""
    assert rows[UNGEOCODED]["activity_description_en"] == ""
    assert rows[UNGEOCODED]["bolagsverket_source_record_uid"] == ""


def test_market_flags_come_from_their_own_tables(rows: dict[str, dict]) -> None:
    # is_publicly_traded: an EODHD listings-resolve row, SE only.
    assert rows[PRECISE]["is_publicly_traded"] == 1
    assert rows[COARSE]["is_publicly_traded"] == 0
    assert rows[UNGEOCODED]["is_publicly_traded"] == 0
    # has_government_contracts: exact-matched winner rows only.
    assert rows[COARSE]["has_government_contracts"] == 1
    assert rows[PRECISE]["has_government_contracts"] == 0
    # has_job_ads honors the SE filter: UNGEOCODED's Norwegian ads must not count.
    assert rows[POSTAL_BOX]["has_job_ads"] == 1
    assert rows[UNGEOCODED]["has_job_ads"] == 0
    assert rows[NOADDRESS]["has_job_ads"] == 0


def test_source_flags_or_their_arms_together(rows: dict[str, dict]) -> None:
    # Every addressed fixture row's address is sourced from bolagsverket -> B; PRECISE
    # also earns B via its metrics arm. The addressless company has no B arm at all.
    for company in ADDRESSED:
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
    assert rows[UNGEOCODED]["legal_name"] == "Ungeocoded AB"
    assert rows[POSTAL_BOX]["legal_name"] == "Postal Box AB"
    assert rows[NOADDRESS]["legal_name"] == "Addressless AB"
    assert rows[HIDDEN]["legal_name"] == "Hidden Row AB"
    assert rows[VISITING]["legal_name"] == "Visiting AB"


def test_address_count_matches_the_published_addresses(rows: dict[str, dict]) -> None:
    assert rows[COARSE]["address_count"] == 1
    assert rows[PRECISE]["address_count"] == 2
    assert rows[UNGEOCODED]["address_count"] == 1
    assert rows[POSTAL_BOX]["address_count"] == 1
    assert rows[VISITING]["address_count"] == 2


def test_an_inactive_row_never_reaches_the_serving_row(rows: dict[str, dict]) -> None:
    """HIDDEN carries one active postal row and one INACTIVE visiting_or_postal row whose key
    sorts first. Only the active one may be served -- had the `active = 1` filter gone the
    count would be 2 and the withdrawn row would have won the primary pick outright."""
    row = rows[HIDDEN]
    assert row["address_count"] == 1
    assert set(_addresses(row)) == {HIDDEN_ACTIVE_KEY}
    assert row["primary_geocode_class"] == "geocoded"
    assert row["primary_city"] == "Göteborg"
    assert row["primary_postal_code"] == "411 24"


def test_the_json_address_id_is_the_entity_key(rows: dict[str, dict]) -> None:
    """Since slice 4a `address_id` IS the address entity's key -- the backoffice geocoding
    list reads the JSON by that name and links the detail page by that value."""
    assert set(_addresses(rows[COARSE])) == {COARSE_KEY}
    assert set(_addresses(rows[PRECISE])) == {
        PRECISE_PRIMARY_KEY,
        PRECISE_SECONDARY_KEY,
    }


def test_the_street_element_is_the_line_without_its_postcode_and_town(
    rows: dict[str, dict],
) -> None:
    """`street_address` is the published line with the trailing `, NNN NN Town` stripped --
    and nothing else: HIDDEN's care-of prefix survives ahead of the street."""
    assert _addresses(rows[COARSE])[COARSE_KEY]["street_address"] == "Storgatan 1"
    assert (
        _addresses(rows[HIDDEN])[HIDDEN_ACTIVE_KEY]["street_address"]
        == "c/o Axfast AB, Vasagatan 7"
    )
    assert rows[HIDDEN]["primary_street_address"] == "c/o Axfast AB, Vasagatan 7"


def test_addresses_json_carries_the_rows_own_geocode_block(
    rows: dict[str, dict],
) -> None:
    """The COARSE company's single address element carries the geocode outcome stored ON the
    entity row: status 'matched_area', precision 'city', the centroid coordinate, and the
    provider DERIVED from that status. Accented city preserved."""
    element = _addresses(rows[COARSE])[COARSE_KEY]
    assert element["geocode_status"] == "matched_area"
    assert element["geocode_precision"] == "city"
    assert element["geocode_provider"] == "centroid_fallback"
    assert element["address_type"] == "visiting_or_postal"
    assert element["city"] == "Trelleborg"
    assert float(element["latitude"]) == pytest.approx(COARSE_LAT)
    assert float(element["longitude"]) == pytest.approx(COARSE_LON)


def test_addresses_json_has_both_elements_for_a_multi_address_company(
    rows: dict[str, dict],
) -> None:
    elements = _addresses(rows[PRECISE])
    assert set(elements) == {PRECISE_PRIMARY_KEY, PRECISE_SECONDARY_KEY}
    primary = elements[PRECISE_PRIMARY_KEY]
    assert primary["geocode_precision"] == "building"
    assert primary["geocode_provider"] == "osm"
    # The secondary was never geocoded: no coordinate, so no provider either.
    secondary = elements[PRECISE_SECONDARY_KEY]
    assert secondary["address_type"] == "postal"
    assert secondary["geocode_precision"] == ""
    assert secondary["geocode_provider"] == ""
    assert secondary["latitude"] == ""


def test_accented_city_survives_json_roundtrip(rows: dict[str, dict]) -> None:
    assert _addresses(rows[UNGEOCODED])[UNGEOCODED_KEY]["city"] == "Umeå"


def test_primary_class_is_coarse_aware_for_a_matched_area_primary(
    rows: dict[str, dict],
) -> None:
    """COARSE's primary is a centroid row: `matched_area`, which lives INSIDE the geocoded
    status vocabulary. The class must be 'coarse' -- the derived-provider check running BEFORE
    the geocoded-status check -- not 'geocoded' (which the status alone would give)."""
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
    assert row["primary_geocode_status"] == "matched_area"


def test_primary_class_is_coarse_for_a_postcode_centroid_box(
    rows: dict[str, dict],
) -> None:
    """POSTAL_BOX's primary is a box the centroid overlay placed at postcode precision. The
    class must be 'coarse', same as COARSE -- the check does not care which address shape the
    centroid landed on."""
    row = rows[POSTAL_BOX]
    assert row["primary_geocode_class"] == "coarse"
    assert row["primary_geocode_precision"] == "postcode"
    assert row["primary_geocode_provider"] == "centroid_fallback"
    assert float(row["primary_latitude"]) == pytest.approx(POSTAL_BOX_LAT)
    assert row["primary_street_address"] == "Box 5305"
    assert row["primary_postal_code"] == "102 47"
    assert row["primary_city"] == "Stockholm"
    assert row["primary_geocode_status"] == "matched_area"


def test_primary_pick_takes_the_visiting_or_postal_row(rows: dict[str, dict]) -> None:
    """PRECISE's primary must be the visiting_or_postal (geocoded) row, not the postal
    (ambiguous) one -- had the pick ranked wrong the class would be 'ambiguous'."""
    row = rows[PRECISE]
    assert row["primary_geocode_class"] == "geocoded"
    assert row["primary_geocode_provider"] == "osm"
    assert float(row["primary_latitude"]) == pytest.approx(PRECISE_LAT)
    # The display fields come from the SAME primary row -- the visiting_or_postal one --
    # not the postal secondary (whose street is "Box 9").
    assert row["primary_street_address"] == "Kungsgatan 2"
    assert row["primary_city"] == "Stockholm"
    assert row["primary_geocode_status"] == "matched_exact"


def test_primary_pick_ranks_visiting_above_postal(rows: dict[str, dict]) -> None:
    """VISITING has no visiting_or_postal row: rank 2 (`visiting`) must beat rank 3
    (`postal`), and the key tiebreak must not override it -- the postal row's key sorts
    first, so a pick that fell through to `address_key ASC` would serve 'unmatched'."""
    row = rows[VISITING]
    assert row["primary_geocode_class"] == "geocoded"
    assert row["primary_street_address"] == "Rådhusesplanaden 8"
    assert row["primary_postal_code"] == "903 28"
    assert float(row["primary_latitude"]) == pytest.approx(VISITING_LAT)


def test_primary_class_falls_back_to_the_base_status_without_a_coordinate(
    rows: dict[str, dict],
) -> None:
    """UNGEOCODED's primary has no coordinate: the derived provider is '' and the class comes
    from its stored geocode_status ('unmatched'). Paired with COARSE -- both rows the precise
    matcher failed on, opposite classes -- the centroid outcome is what separates them."""
    row = rows[UNGEOCODED]
    assert row["primary_geocode_class"] == "unmatched"
    assert row["primary_geocode_provider"] == ""
    assert row["primary_latitude"] is None
