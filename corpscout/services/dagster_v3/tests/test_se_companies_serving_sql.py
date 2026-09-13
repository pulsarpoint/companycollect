"""Execute build_se_companies_serving_sql() against a real ClickHouse engine.

The SQL-text is generated, so the risk this test covers is behavioural, not spelling: that the
FINAL merges, the `active = 1` filter, the per-company JSON aggregation and the coarse-aware
primary-address class all produce the row the companies/geocoding surfaces expect. A substring
test over the builder output cannot prove any of that; a real engine ranking real rows can.

Runs through clickhouse-local (a local binary, else the pinned server image under Docker, else
the module skips), twice -- once per `join_use_nulls` setting -- and must answer the same both
times, because every LEFT JOIN this SELECT still makes (the register row, the two label
dictionaries, the aggregation and the primary pick) is guarded by `ifNull`/`coalesce`.

SINCE SLICE 4a the address half reads the ADDRESS ENTITY, `corpscout.se_company_address`
(migration 000384; renamed from `_v2` by 000393): one row per company and published address,
`active = 1` for the published ones, `kinds` an array, and the geocode outcome -- status,
precision, coordinate -- on the row
itself. There is no served-overlay join any more; the `centroid_fallback` provider the overlay
used to stamp is DERIVED from `geocode_status = 'matched_area'`, which is what the centroid
overlay writes.

The fixture is nine companies, each a different shape of the primary-class or ranking rule:

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
  NOADDRESS   no address row at all -- still one serving row, with an empty address summary. Its one entity period is ESEF-sourced.
  POSTCODE    one POSTCODE-ONLY address: normalizer v3 publishes `100 11 Stockholm` -- a
              valid postcode and a town, no street and no box -- and the line therefore has
              no comma for the street expression to cut at. `street_address` must come out
              EMPTY (the strip alone would hand back the whole line as a street), while the
              postcode and city columns carry the location. Its centroid makes it 'coarse'.
  LOCATIONLESS  two addresses: SCB's postcode-only `visiting_or_postal` row (no street, no
              box) and Bolagsverket's `postal` row with a real street. The kind ranks alone
              would serve the postcode-only row and print an EMPTY street; the has_location
              rank runs first, so the street row is the primary.
  WORKPLACE   three active rows in the shape Ratsit slice 3 produces: the company's own
              `postal` address, an establishment the fold MERGED into a published address
              (`['postal', 'workplace']`) and a standalone establishment whose ONLY kind is
              `workplace`. Migration 000403 keeps that last one out of the array, out of
              address_count and out of the primary pick -- and every rank above the key ties
              across the three, with the workplace-only key sorting FIRST, so without the
              exclusion it would be the primary and the count would read 3.
  WORKPLACE_ONLY  one active row, workplace-only. 355 companies look like this after slice 3
              (establishments but no visiting_or_postal row), and they must serve the same
              empty address summary an addressless company does. Its one entity period is sourced from the restated Bolagsverket column.
"""

import json
import subprocess
from datetime import UTC, datetime

import pytest

from dagster_v3.defs.sweden_company.companies_current import (
    build_se_companies_serving_sql,
)
from tests.clickhouse_local import clickhouse_local_command, literal
from tests.se_company_ddl import table_block

pytestmark = pytest.mark.integration

NOW = datetime(2026, 8, 26, 9, tzinfo=UTC)

COARSE = "5560000011"
PRECISE = "5560000022"
UNGEOCODED = "5560000033"
POSTAL_BOX = "5560000044"
NOADDRESS = "5560000055"
HIDDEN = "5560000066"
VISITING = "5560000077"
POSTCODE = "5560000088"
LOCATIONLESS = "5560000099"
WORKPLACE = "5560000111"
WORKPLACE_ONLY = "5560000122"

ADDRESSED = (
    COARSE,
    PRECISE,
    UNGEOCODED,
    POSTAL_BOX,
    HIDDEN,
    VISITING,
    POSTCODE,
    LOCATIONLESS,
    WORKPLACE,
)
# Published companies whose serving row carries NO address: one with no address row at all,
# one whose only address row is workplace-only and therefore not served (migration 000403).
UNSERVED_ADDRESS = (NOADDRESS, WORKPLACE_ONLY)

PRECISE_LAT, PRECISE_LON = 59.3300, 18.0600
COARSE_LAT, COARSE_LON = 55.6050, 13.0000
POSTAL_BOX_LAT, POSTAL_BOX_LON = 55.3770, 13.1520
HIDDEN_LAT, HIDDEN_LON = 57.7080, 11.9740
VISITING_LAT, VISITING_LON = 63.8250, 20.2630
POSTCODE_LAT, POSTCODE_LON = 59.3320, 18.0640
LOCATIONLESS_STREET_LAT, LOCATIONLESS_STREET_LON = 59.3390, 18.0580
WORKPLACE_ONLY_LAT, WORKPLACE_ONLY_LON = 57.5060, 12.6930
WORKPLACE_MERGED_LAT, WORKPLACE_MERGED_LON = 57.5080, 12.6950

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
POSTCODE_KEY = "k" + "8" * 63
# The location-less row outranks the street row on BOTH kind ranks and on the key, so only
# the has_location rank can keep the street row primary.
LOCATIONLESS_EMPTY_KEY = "a" + "9" * 63
LOCATIONLESS_STREET_KEY = "s" + "9" * 63
# All three WORKPLACE rows carry a street and none is visiting_or_postal or visiting, so
# every rank above the key ties and the KEY decides the primary. The workplace-only row's
# key sorts first of the three: without 000403's exclusion it would take the primary pick.
WORKPLACE_EXCLUDED_KEY = "a" + "0" * 63
WORKPLACE_MERGED_KEY = "m" + "0" * 63
WORKPLACE_POSTAL_KEY = "p" + "0" * 63
WORKPLACE_ONLY_KEY = "w" + "0" * 63


BASIC_INFO_COLUMNS = (
    "company_id, legal_name, legal_name_source, legal_form_code, legal_form_code_source, "
    "status, status_source, incorporation_date, incorporation_date_source, lei, lei_source, "
    "wikidata_id, wikidata_id_source, description, description_source, description_language, "
    "description_sv, description_sv_source, folded_at, fold_version, source_run_id"
)


def _basic_info_row(
    company_id: str,
    legal_name: str,
    *,
    legal_form_code: str = "NULL",
    description: str = "NULL",
    description_language: str = "NULL",
    description_sv: str = "NULL",
) -> str:
    source = "'bolagsverket'" if description != "NULL" else "''"
    return (
        f"('{company_id}', '{legal_name}', 'scb', {legal_form_code}, 'scb', 'active', 'bolagsverket', "
        f"NULL, '', NULL, '', NULL, '', {description}, {source}, {description_language}, "
        f"{description_sv}, {source}, {literal(NOW)}, 'fold-v1', 'run')"
    )


BOLAGSVERKET_COLUMNS = (
    "company_id, company_id_raw, legal_name, deregistration_reason, source_run_id, "
    "source_record_id, source_payload_hash, observed_at"
)


def _bolagsverket_row(company_id: str, *, reason: str = "NULL") -> str:
    return (
        f"('{company_id}', '{company_id}$X', 'Register AB', {reason}, 'run', "
        f"'rec-{company_id}', 'HASH-{company_id}', {literal(NOW)})"
    )


def _record_uid(company_id: str) -> str:
    """What the view must render for bolagsverket_source_record_uid: the extractor's
    company-source-record hash over the register row's id and (lower-cased) payload hash."""
    import hashlib

    text = (
        "company-source-record-v1\nstructured\nsweden_bolagsverket\nregistry_company\n"
        f"rec-{company_id}\nhash-{company_id}"
    )
    return hashlib.sha256(text.encode()).hexdigest()


# The entity columns the serving SELECT reads, plus the provenance ones a published row always
# carries. Everything omitted takes its type default -- the SELECT never touches it.
# `street_name` and `box` are read ONLY by the has_location rank, but a fixture that left
# them NULL on a street row would make that rank vacuous, so every row carries the components
# its display line was built from.
ADDRESS_COLUMNS = (
    "company_id, address_key, box, street_name, house_number, unit, postal_code, city, "
    "country_code, normalized_address, "
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
    street_name: str | None = None,
    house_number: str | None = None,
    unit: str | None = None,
    source: str = "bolagsverket",
    active: int = 1,
    inactive_reason: str = "",
) -> str:
    kinds_sql = "[" + ", ".join(f"'{kind}'" for kind in kinds) + "]"
    return (
        f"('{company_id}', '{address_key}', {literal(box) if box else 'NULL'}, "
        f"{literal(street_name) if street_name else 'NULL'}, "
        f"{literal(house_number) if house_number else 'NULL'}, "
        f"{literal(unit) if unit else 'NULL'}, "
        f"'{postal_code}', '{city}', 'se', '{line}', {kinds_sql}, ['{source}'], ['0'], "
        f"'{source}', {active}, '{inactive_reason}', "
        f"{'NULL' if latitude is None else latitude}, "
        f"{'NULL' if longitude is None else longitude}, "
        f"'{geocode_status}', '{geocode_precision}', {literal(NOW)}, 'v1', 'run')"
    )


ADDRESS_ROWS = (
    # COARSE: the centroid overlay's own outcome -- matched_area at city precision.
    _address_row(
        company_id=COARSE,
        address_key=COARSE_KEY,
        kinds=("visiting_or_postal",),
        line="Storgatan 1, 231 39 Trelleborg",
        street_name="Storgatan",
        house_number="1",
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
        street_name="Kungsgatan",
        house_number="2",
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
        box="9",
    ),
    _address_row(
        company_id=UNGEOCODED,
        address_key=UNGEOCODED_KEY,
        kinds=("visiting_or_postal",),
        line="Nygatan 4, 903 25 Umeå",
        street_name="Nygatan",
        house_number="4",
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
        box="5305",
    ),
    # HIDDEN: the published row carries a care-of prefix -- the street expression strips the
    # trailing postcode and town off the line and keeps everything before it.
    _address_row(
        company_id=HIDDEN,
        address_key=HIDDEN_ACTIVE_KEY,
        kinds=("postal",),
        line="c/o Axfast AB, Vasagatan 7, 411 24 Göteborg",
        street_name="Vasagatan",
        house_number="7",
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
        street_name="Withdrawn Gatan",
        house_number="9",
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
        street_name="Rådhusesplanaden",
        house_number="8",
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
        box="1",
    ),
    # POSTCODE: normalizer v3's postcode-only shape -- the whole line IS the postal part,
    # with no comma in front of it, geocoded to the postcode centroid.
    _address_row(
        company_id=POSTCODE,
        address_key=POSTCODE_KEY,
        kinds=("postal",),
        line="100 11 Stockholm",
        postal_code="100 11",
        city="Stockholm",
        geocode_status="matched_area",
        geocode_precision="postcode",
        latitude=POSTCODE_LAT,
        longitude=POSTCODE_LON,
    ),
    # LOCATIONLESS: the real shape behind the has_location rank -- SCB delivers the company's
    # visiting_or_postal address as a postcode-only line (no street, no box), Bolagsverket
    # delivers a postal address with the actual street. The location-less row wins BOTH kind
    # ranks and the key tiebreak, so only has_location can keep the street row primary.
    _address_row(
        company_id=LOCATIONLESS,
        address_key=LOCATIONLESS_EMPTY_KEY,
        kinds=("visiting_or_postal",),
        line="111 60 Stockholm",
        postal_code="111 60",
        city="Stockholm",
        geocode_status="matched_area",
        geocode_precision="postcode",
        latitude=POSTCODE_LAT,
        longitude=POSTCODE_LON,
        source="scb",
    ),
    _address_row(
        company_id=LOCATIONLESS,
        address_key=LOCATIONLESS_STREET_KEY,
        kinds=("postal",),
        line="Drottninggatan 5, 111 51 Stockholm",
        street_name="Drottninggatan",
        house_number="5",
        postal_code="111 51",
        city="Stockholm",
        geocode_status="matched_exact",
        geocode_precision="building",
        latitude=LOCATIONLESS_STREET_LAT,
        longitude=LOCATIONLESS_STREET_LON,
    ),
    # WORKPLACE: the shape Ratsit slice 3 folds. The standalone establishment is a real,
    # geocoded, street-carrying row -- nothing about the row itself disqualifies it, only
    # the fact that `workplace` is its ONLY kind -- and its key sorts ahead of both served
    # rows, so it would be the primary if 000403's exclusion were missing.
    _address_row(
        company_id=WORKPLACE,
        address_key=WORKPLACE_EXCLUDED_KEY,
        kinds=("workplace",),
        line="Fabriksgatan 3, 511 54 Kinna",
        street_name="Fabriksgatan",
        house_number="3",
        postal_code="511 54",
        city="Kinna",
        geocode_status="matched_exact",
        geocode_precision="building",
        latitude=WORKPLACE_ONLY_LAT,
        longitude=WORKPLACE_ONLY_LON,
        source="ratsit",
    ),
    # The establishment the fold MERGED into the register's published address: it keeps BOTH
    # kinds, it IS the company's own address, and it must stay.
    _address_row(
        company_id=WORKPLACE,
        address_key=WORKPLACE_MERGED_KEY,
        kinds=("postal", "workplace"),
        line="Verkstadsgatan 8, 511 55 Kinna",
        street_name="Verkstadsgatan",
        house_number="8",
        postal_code="511 55",
        city="Kinna",
        geocode_status="matched_exact",
        geocode_precision="building",
        latitude=WORKPLACE_MERGED_LAT,
        longitude=WORKPLACE_MERGED_LON,
    ),
    _address_row(
        company_id=WORKPLACE,
        address_key=WORKPLACE_POSTAL_KEY,
        kinds=("postal",),
        line="Box 12, 511 01 Kinna",
        postal_code="511 01",
        city="Kinna",
        geocode_status="unmatched",
        box="12",
    ),
    # WORKPLACE_ONLY: establishments and nothing else -- every published row is excluded, so
    # the company serves the empty address summary an addressless company serves.
    _address_row(
        company_id=WORKPLACE_ONLY,
        address_key=WORKPLACE_ONLY_KEY,
        kinds=("workplace",),
        line="Industrivägen 2, 511 56 Kinna",
        street_name="Industrivägen",
        house_number="2",
        postal_code="511 56",
        city="Kinna",
        geocode_status="matched_exact",
        geocode_precision="building",
        latitude=WORKPLACE_ONLY_LAT,
        longitude=WORKPLACE_ONLY_LON,
        source="ratsit",
    ),
)


def _script(*, join_use_nulls: int) -> str:
    parts = [
        f"SET join_use_nulls = {join_use_nulls};",
        "CREATE DATABASE IF NOT EXISTS corpscout;",
        table_block("se_company_basic_info"),
        table_block("se_bolagsverket_companies"),
        # The address entity itself (migration 000384) -- read FINAL, active rows only.
        # Migration 000384 declares the entity under its build name; 000393 renames the
        # deployed table and never edits that file, so the local schema renames it here.
        table_block("se_company_address_v2").replace(
            "corpscout.se_company_address_v2", "corpscout.se_company_address"
        ),
        # Stubs for the presence-set reads: only the columns the serving SELECT's
        # IN-subqueries touch. Seeds prove each arm independently.
        "CREATE TABLE corpscout.se_code_labels (code_type String, code String, label_en String, label_sv String, version UInt32) ENGINE = MergeTree ORDER BY code;",
        # The financial entity's main table (migration 000401) -- read FINAL, active rows only;
        # only the columns the three financial IN-subqueries touch. ReplacingMergeTree so the
        # stub accepts the FINAL modifier the real engine does.
        "CREATE TABLE corpscout.se_company_financial (company_id String, sources Array(String), active UInt8, scope String) ENGINE = ReplacingMergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.se_financial_reports (company_id String) ENGINE = MergeTree ORDER BY company_id;",
        # The person entity's main table (migration 000396, renamed by 000398) -- read
        # FINAL, active rows only. Only the three columns the serving SELECT's IN-subqueries
        # touch. ReplacingMergeTree (not plain MergeTree, which this ClickHouse rejects with
        # ILLEGAL_FINAL) so the stub accepts the same FINAL modifier the real table's engine
        # does.
        "CREATE TABLE corpscout.se_company_person (company_id String, sources Array(String), active UInt8) ENGINE = ReplacingMergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.company_domains (company_id String, country_code String) ENGINE = MergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.company_traded_symbols (country_code String, company_id String) ENGINE = MergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.se_government_contracts (company_id String) ENGINE = MergeTree ORDER BY company_id;",
        "CREATE TABLE corpscout.company_job_history (company_id String, country_code String) ENGINE = MergeTree ORDER BY company_id;",
        # Register rows: COARSE is deregistered with a labeled reason; PRECISE has no reason;
        # UNGEOCODED has no register row at all (every register-derived field folds to '').
        f"INSERT INTO corpscout.se_bolagsverket_companies ({BOLAGSVERKET_COLUMNS}) VALUES "
        + ", ".join((_bolagsverket_row(COARSE, reason="'konkurs avslutad'"), _bolagsverket_row(PRECISE)))
        + ";",
        "INSERT INTO corpscout.se_code_labels VALUES ('status_reason', 'konkurs avslutad', 'Bankruptcy concluded', '', 1), ('legal_form', '49', 'Limited company (aktiebolag)', 'Aktiebolag', 1);",
        # PRECISE has an active folded period from Bolagsverket and Ratsit; NOADDRESS's only
        # period is ESEF-sourced and WORKPLACE_ONLY's is the restated Bolagsverket column, so
        # each register-specific arm is proven on its own; UNGEOCODED's only period is
        # hidden (active 0) and carries esef, which must light neither has_financial nor E.
        f"INSERT INTO corpscout.se_company_financial VALUES "
        f"('{PRECISE}', ['bolagsverket', 'ratsit'], 1, 'standalone'), "
        f"('{NOADDRESS}', ['esef'], 1, 'standalone'), "
        f"('{WORKPLACE_ONLY}', ['bolagsverket_comparative'], 1, 'standalone'), "
        f"('{UNGEOCODED}', ['esef', 'ratsit'], 0, 'standalone');",
        f"INSERT INTO corpscout.se_financial_reports VALUES ('{COARSE}');",
        f"INSERT INTO corpscout.se_company_person VALUES ('{PRECISE}', ['esef'], 1);",
        # The SE filter must hold: UNGEOCODED's domain is Norwegian and must not count.
        f"INSERT INTO corpscout.company_domains VALUES ('{COARSE}', 'SE'), ('{UNGEOCODED}', 'NO');",
        # Market flags: PRECISE is listed (EODHD listings resolve); COARSE won a government
        # contract; POSTAL_BOX has job-ad history, and UNGEOCODED's job rows are Norwegian
        # so the SE filter must exclude them.
        f"INSERT INTO corpscout.company_traded_symbols VALUES ('SE', '{PRECISE}'), ('NO', '{UNGEOCODED}');",
        f"INSERT INTO corpscout.se_government_contracts VALUES ('{COARSE}');",
        f"INSERT INTO corpscout.company_job_history VALUES ('{POSTAL_BOX}', 'SE'), ('{UNGEOCODED}', 'NO');",
        f"INSERT INTO corpscout.se_company_basic_info ({BASIC_INFO_COLUMNS}) VALUES\n"
        + ",\n".join(
            (
                # COARSE: translated activity text -- English in description, Swedish beside it.
                _basic_info_row(COARSE, "Coarse AB", legal_form_code="'49'",
                                description="'Building trade with timber'", description_language="'en'",
                                description_sv="'Bygghandel med trävaror'"),
                # PRECISE: untranslated -- the Swedish text is the description, language sv.
                _basic_info_row(PRECISE, "Precise AB", description="'Handel med maskiner'",
                                description_language="'sv'", description_sv="'Handel med maskiner'"),
                _basic_info_row(UNGEOCODED, "Ungeocoded AB"),
                _basic_info_row(POSTAL_BOX, "Postal Box AB"),
                _basic_info_row(NOADDRESS, "Addressless AB"),
                _basic_info_row(HIDDEN, "Hidden Row AB"),
                _basic_info_row(VISITING, "Visiting AB"),
                _basic_info_row(POSTCODE, "Postcode Only AB"),
                _basic_info_row(LOCATIONLESS, "Locationless AB"),
                _basic_info_row(WORKPLACE, "Workplace Rows AB"),
                _basic_info_row(WORKPLACE_ONLY, "Workplace Only AB"),
            )
        )
        + ";",
        f"INSERT INTO corpscout.se_company_address ({ADDRESS_COLUMNS}) VALUES\n"
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
    command = clickhouse_local_command()
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
    # The widened base: a published company with NO published address still gets a row --
    # and so does one whose only address row the serving view does not publish.
    assert set(rows) == {*ADDRESSED, *UNSERVED_ADDRESS}


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
    # has_financial: PRECISE, NOADDRESS and WORKPLACE_ONLY via an active financial-entity
    # row, COARSE via a filed report -- the owner's 2026-08-25 widening -- and nothing
    # else; UNGEOCODED's hidden period does not count.
    assert rows[PRECISE]["has_financial"] == 1
    assert rows[NOADDRESS]["has_financial"] == 1
    assert rows[WORKPLACE_ONLY]["has_financial"] == 1
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
    # has_description is the folded description, whatever its language.
    assert rows[COARSE]["has_description"] == 1
    assert rows[PRECISE]["has_description"] == 1
    for company in (UNGEOCODED, POSTAL_BOX, NOADDRESS):
        assert rows[company]["has_description"] == 0


def test_descriptions_come_from_the_main_row_and_register_fields_from_bolagsverket(rows: dict[str, dict]) -> None:
    # COARSE: the folded row carries English + Swedish; the register row is deregistered
    # with a labeled reason; the record uid is the extractor's hash over the register row.
    assert rows[COARSE]["activity_description"] == "Bygghandel med trävaror"
    assert rows[COARSE]["activity_description_en"] == "Building trade with timber"
    assert rows[COARSE]["status_reason"] == "konkurs avslutad"
    assert rows[COARSE]["status_reason_label_en"] == "Bankruptcy concluded"
    assert rows[COARSE]["bolagsverket_source_record_uid"] == _record_uid(COARSE)
    assert rows[COARSE]["legal_form_code"] == "49"
    assert rows[COARSE]["legal_form_label_en"] == "Limited company (aktiebolag)"
    assert rows[COARSE]["legal_form_label_sv"] == "Aktiebolag"
    # PRECISE: untranslated -> the English column stays '' (never the Swedish text).
    assert rows[PRECISE]["activity_description"] == "Handel med maskiner"
    assert rows[PRECISE]["activity_description_en"] == ""
    assert rows[PRECISE]["status_reason"] == ""
    assert rows[PRECISE]["bolagsverket_source_record_uid"] == _record_uid(PRECISE)
    # UNGEOCODED: no register row -> every register-derived field folds to ''.
    assert rows[UNGEOCODED]["activity_description"] == ""
    assert rows[UNGEOCODED]["activity_description_en"] == ""
    assert rows[UNGEOCODED]["bolagsverket_source_record_uid"] == ""
    assert rows[UNGEOCODED]["legal_form_label_en"] == ""


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
    # also earns B via its entity arm. The addressless company has no B arm at all: its
    # only entity period is ESEF-sourced.
    for company in ADDRESSED:
        assert rows[company]["source_bolagsverket"] == 1
    assert rows[NOADDRESS]["source_bolagsverket"] == 0
    # B via the entity arm ALONE: WORKPLACE_ONLY has no published address and no person,
    # and its one active period is sourced from the restated column, which lights the
    # Bolagsverket flag (ruling in the slice 4a plan: bolagsverket_comparative counts).
    assert rows[WORKPLACE_ONLY]["has_address"] == 0
    assert rows[WORKPLACE_ONLY]["has_people"] == 0
    assert rows[WORKPLACE_ONLY]["source_bolagsverket"] == 1
    # E: PRECISE via its esef role evidence; NOADDRESS via the entity arm ALONE (no
    # description, no LEI, no person); a HIDDEN esef period (UNGEOCODED) lights nothing.
    assert rows[PRECISE]["source_esef"] == 1
    assert rows[NOADDRESS]["source_esef"] == 1
    assert rows[NOADDRESS]["has_people"] == 0
    assert rows[UNGEOCODED]["source_esef"] == 0
    assert rows[COARSE]["source_esef"] == 0
    # W: no fixture row carries wikidata evidence.
    for company in rows:
        assert rows[company]["source_wikidata"] == 0


def test_legal_name_comes_from_the_main_row(rows: dict[str, dict]) -> None:
    assert rows[COARSE]["legal_name"] == "Coarse AB"
    assert rows[PRECISE]["legal_name"] == "Precise AB"
    assert rows[UNGEOCODED]["legal_name"] == "Ungeocoded AB"
    assert rows[POSTAL_BOX]["legal_name"] == "Postal Box AB"
    assert rows[NOADDRESS]["legal_name"] == "Addressless AB"
    assert rows[HIDDEN]["legal_name"] == "Hidden Row AB"
    assert rows[VISITING]["legal_name"] == "Visiting AB"
    assert rows[POSTCODE]["legal_name"] == "Postcode Only AB"


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


def test_a_postcode_only_line_has_an_empty_street_and_keeps_its_location(
    rows: dict[str, dict],
) -> None:
    """POSTCODE's published line is `100 11 Stockholm` -- normalizer v3's postcode-only
    shape, no street and no box, and so no comma for the strip to cut at. The street part
    must be EMPTY: the bare `replaceRegexpOne` matched nothing on such a line and handed
    back the whole thing, which put `100 11 Stockholm` in the companies list's street
    column and in the address JSON's `street_address`. The postcode and town still travel
    in their own columns, and the centroid still classifies the row 'coarse'."""
    element = _addresses(rows[POSTCODE])[POSTCODE_KEY]
    assert element["street_address"] == ""
    assert (element["postal_code"], element["city"]) == ("100 11", "Stockholm")
    row = rows[POSTCODE]
    assert row["primary_street_address"] == ""
    assert (row["primary_postal_code"], row["primary_city"]) == ("100 11", "Stockholm")
    assert row["primary_geocode_class"] == "coarse"
    assert row["address_count"] == 1


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


def test_primary_pick_prefers_a_row_that_has_a_location(rows: dict[str, dict]) -> None:
    """LOCATIONLESS's `visiting_or_postal` row is SCB's postcode-only line: no street, no
    box, and it beats the Bolagsverket street row on BOTH kind ranks and on the key. The
    has_location rank runs first, so the STREET row is the primary -- otherwise the
    companies and geocoding lists print an empty street for a company that has one, and
    the badge reports the postcode centroid instead of the building match."""
    row = rows[LOCATIONLESS]
    assert row["address_count"] == 2
    assert row["primary_street_address"] == "Drottninggatan 5"
    assert row["primary_postal_code"] == "111 51"
    assert row["primary_geocode_class"] == "geocoded"
    assert row["primary_geocode_status"] == "matched_exact"
    assert float(row["primary_latitude"]) == pytest.approx(LOCATIONLESS_STREET_LAT)
    # Both rows still travel in the JSON -- the rank decides the primary, not the population.
    assert set(_addresses(row)) == {LOCATIONLESS_EMPTY_KEY, LOCATIONLESS_STREET_KEY}
    assert _addresses(row)[LOCATIONLESS_EMPTY_KEY]["street_address"] == ""


def test_a_workplace_only_row_is_never_published_or_made_primary(
    rows: dict[str, dict],
) -> None:
    """Migration 000403, the Ratsit slice-3 ruling. WORKPLACE carries three ACTIVE rows: its
    own postal address, an establishment the fold merged into a published address (kinds
    `['postal', 'workplace']`) and a standalone establishment whose ONLY kind is `workplace`.
    Just the last one leaves -- the merged row IS the company's address. The exclusion sits on
    the one CTE the array, the count and the primary pick all read, so this proves all three
    at once: every rank above the key ties across the three rows and the workplace-only key
    sorts FIRST, so without the exclusion the count would read 3 and that row would be the
    primary, printing a branch office as the company's own address."""
    row = rows[WORKPLACE]
    assert row["has_address"] == 1
    assert row["address_count"] == 2
    assert set(_addresses(row)) == {WORKPLACE_MERGED_KEY, WORKPLACE_POSTAL_KEY}
    assert WORKPLACE_EXCLUDED_KEY not in _addresses(row)
    # The primary is the merged row -- the key tiebreak among what is left -- and never the
    # workplace-only row, whose key would otherwise have won it.
    assert row["primary_street_address"] == "Verkstadsgatan 8"
    assert row["primary_postal_code"] == "511 55"
    assert row["primary_geocode_class"] == "geocoded"
    assert float(row["primary_latitude"]) == pytest.approx(WORKPLACE_MERGED_LAT)
    assert row["primary_street_address"] != "Fabriksgatan 3"
    assert float(row["primary_latitude"]) != pytest.approx(WORKPLACE_ONLY_LAT)


def test_a_company_whose_only_rows_are_workplaces_serves_no_address(
    rows: dict[str, dict],
) -> None:
    """After slice 3, 355 companies have establishments and no `visiting_or_postal` row at
    all. When EVERY published row is workplace-only the company still gets its serving row,
    with the same empty address summary an addressless company gets -- no array, no count,
    no primary -- rather than a branch office standing in for the company's address."""
    row = rows[WORKPLACE_ONLY]
    assert row["has_address"] == 0
    assert row["address_count"] == 0
    assert json.loads(row["addresses"]) == []
    assert row["primary_street_address"] == ""
    assert row["primary_city"] == ""
    assert row["primary_geocode_class"] == ""
    assert row["primary_latitude"] is None
    assert row["primary_longitude"] is None


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
