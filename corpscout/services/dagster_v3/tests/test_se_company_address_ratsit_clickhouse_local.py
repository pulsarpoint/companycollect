"""ratsit-address-v2 on a real ClickHouse (spec 2026-09-11 sections 5.1-5.3).

The register dictionary decides the postal town (frequency, then alphabetical, `has_company
= 1` only, unknown postcode falls back to Ratsit's locality); the current report yields the
company row plus one `workplace` row per establishment with a street and a postcode, with the
establishment index appended when the report repeats an identifier; an establishment of a
superseded report never appears; and a second scan that drops an establishment tombstones its
slot with the new report's observed_at, after which the scope converges. Runs under
join_use_nulls 0 and 1."""

import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.address import ratsit, tables
from dagster_v3.defs.se_company.address.normalize import changed_rows_sql, normalized_row
from dagster_v3.defs.se_company.address.normalize_se import NORMALIZER_VERSION
from dagster_v3.defs.se_company.address.suggestions import ADDRESS_TARGET
from dagster_v3.defs.se_company.basic_info.extract import changed_scope_sql, insert_page_sql
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
from tests.clickhouse_local import clickhouse_local_command
from tests.test_se_company_basic_info_clickhouse_local import _bind

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATIONS = (
    "000373_corpscout_se_scb_companies.up.sql",
    "000382_corpscout_se_company_address_suggestion.up.sql",
    "000383_corpscout_se_company_address_normalized.up.sql",
)
FIXTURES = (
    Path(__file__).resolve().parent / "fixtures" / "se_basic_info_source_tables.sql",
    Path(__file__).resolve().parent / "fixtures" / "se_company_address_source_tables.sql",
)

# The company whose Ratsit locality is the municipality (Malmö for Oxie) and whose current
# report carries four establishments, one of them streetless and two sharing an identifier.
COMPANY_TOWN_FIX = "5560001112"
# A postcode the register does not know: the row keeps Ratsit's own locality.
COMPANY_UNKNOWN_CODE = "5560002223"
# A postcode two register spellings tie on: the alphabetically first wins.
COMPANY_TIE = "5560003334"

OLD_SHA = "a" * 64
CURRENT_SHA = "b" * 64
UNKNOWN_SHA = "d" * 64
TIE_SHA = "e" * 64
RESCAN_SHA = "f" * 64

OLD_AT = "2026-09-01 00:00:00"
CURRENT_AT = "2026-09-02 00:00:00"
RESCAN_AT = "2026-09-06 00:00:00"
STAMP = datetime(2026, 9, 12, 12, 0, 0, tzinfo=UTC)

ROW_COLUMNS = (
    "company_id", "slot", "kind", "source_record_uid", "street_address", "postal_code",
    "post_town", "county", "raw_address", "care_of", "country_code", "observed_at",
    "extractor_version",
)
# ifNull(..., 'NULL') rather than the TSV \N marker: every address column is Nullable(String)
# and this prints the same text under join_use_nulls 0 and 1.
ROWS_SQL = (
    "SELECT company_id, slot, kind, source_record_uid, "
    "ifNull(street_address, 'NULL'), ifNull(postal_code, 'NULL'), ifNull(post_town, 'NULL'), "
    "ifNull(county, 'NULL'), ifNull(raw_address, 'NULL'), ifNull(care_of, 'NULL'), "
    "ifNull(country_code, 'NULL'), toString(observed_at), extractor_version "
    f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL WHERE source = 'ratsit' "
    "ORDER BY company_id, slot"
)


def _schema() -> list[str]:
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(("CREATE DATABASE", "CREATE TABLE")):
                statements.append(statement)
    for path in FIXTURES:
        statements.extend(s.strip() for s in path.read_text(encoding="utf-8").split(";") if s.strip())
    return statements


def _run(statements: list[str], *, join_use_nulls: int) -> list[str]:
    script = f"SET join_use_nulls = {join_use_nulls};\n" + ";\n".join(statements) + ";\n"
    completed = subprocess.run(
        clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
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


def _scope() -> str:
    sql = _bind(
        changed_scope_sql(current_sql=ratsit.ratsit_current_sql(), target=ADDRESS_TARGET),
        source="ratsit",
        **ratsit.RATSIT_ADDRESS_SELECT_PARAMS,
    )
    return f"{sql}\nORDER BY company_id"


def _insert(ids: list[str]) -> str:
    return _bind(
        insert_page_sql(select_sql=ratsit.ratsit_select_sql(), target=ADDRESS_TARGET),
        company_ids=ids,
        source_run_id="run-1",
        extractor_version=ratsit.RATSIT_ADDRESS_EXTRACTOR_VERSION,
        **ratsit.RATSIT_ADDRESS_SELECT_PARAMS,
    )


def _scb(company_id: str, postal_code: str, post_town: str, has_company: int) -> str:
    return (
        "INSERT INTO corpscout.se_scb_companies (company_id, company_id_raw, postal_code, post_town, "
        "has_company, observed_at, source_run_id, source_record_id, source_payload_hash) VALUES "
        f"('{company_id}', '{company_id}', '{postal_code}', '{post_town}', {has_company}, "
        "toDateTime64('2026-09-01 00:00:00', 3, 'UTC'), '', 's', 'h')"
    )


def _report(company_id: str, sha: str, normalized_at: str, street: str, code: str, locality: str, county: str) -> str:
    return (
        "INSERT INTO corpscout.se_ratsit_company (company_id, result_sha256, normalizer_version, schema_version, "
        "parser_version, requested_url, source_url, result_bucket, result_object_key, name, organization_number, "
        "address_street, address_postal_code, address_locality, address_county, normalized_at) VALUES "
        f"('{company_id}', '{sha}', '{RATSIT_NORMALIZER_VERSION}', 1, 'p', 'u', 'u', 'b', 'k', 'Test AB', "
        f"'{company_id[-10:]}', '{street}', '{code}', '{locality}', '{county}', "
        f"toDateTime64('{normalized_at}', 6, 'UTC'))"
    )


def _establishment(
    company_id: str, sha: str, normalized_at: str, index: int, identifier: str,
    street: str, code: str, locality: str, county: str,
) -> str:
    return (
        "INSERT INTO corpscout.se_ratsit_establishments (company_id, result_sha256, normalizer_version, "
        "establishment_index, name, identifier, address_street, address_postal_code, address_locality, "
        "address_county, normalized_at) VALUES "
        f"('{company_id}', '{sha}', '{RATSIT_NORMALIZER_VERSION}', {index}, 'Site {index}', '{identifier}', "
        f"'{street}', '{code}', '{locality}', '{county}', toDateTime64('{normalized_at}', 6, 'UTC'))"
    )


def _register_rows() -> list[str]:
    """238 40 -> OXIE (two spellings against one, and three has_company = 0 rows that would
    otherwise win with FELSTAD), 11122/111 43 -> STOCKHOLM, 11255 -> a two-way tie."""
    return [
        _scb("5570000001", "238 40", "OXIE", 1),
        _scb("5570000002", "23840", "OXIE", 1),
        _scb("5570000003", "238 40", " Oxie ", 1),
        _scb("5570000004", "11122", "STOCKHOLM", 1),
        _scb("5570000005", "111 43", "STOCKHOLM", 1),
        _scb("5570000006", "11255", "BETA", 1),
        _scb("5570000007", "11255", "ALFA", 1),
        _scb("5570000008", "23840", "FELSTAD", 0),
        _scb("5570000009", "23840", "FELSTAD", 0),
        _scb("5570000010", "23840", "FELSTAD", 0),
    ]


def _ratsit_rows() -> list[str]:
    return [
        # The superseded report and its establishment: neither may reach the suggestions.
        _report(COMPANY_TOWN_FIX, OLD_SHA, OLD_AT, "Gamlagatan 9", "23840", "Malmö", "Skåne län"),
        _establishment(COMPANY_TOWN_FIX, OLD_SHA, OLD_AT, 0, "EST-OLD", "Gamlagatan 9", "23840", "Oxie", "Skåne län"),
        # The current report: locality is the MUNICIPALITY, which the dictionary corrects.
        _report(COMPANY_TOWN_FIX, CURRENT_SHA, CURRENT_AT, "Oxievägen 12", "238 40", "Malmö", "Skåne län"),
        # 0: shares the company's postal street and postcode (the fold merges it).
        _establishment(COMPANY_TOWN_FIX, CURRENT_SHA, CURRENT_AT, 0, "EST-1", "Oxievägen 12", "238 40", "Oxie", "Skåne län"),
        # 1 and 3 repeat the identifier EST-2, so BOTH get the index appended.
        _establishment(COMPANY_TOWN_FIX, CURRENT_SHA, CURRENT_AT, 1, "EST-2", "Storgatan 1", "11122", "Stockholm", "Stockholms län"),
        # 2 has no street: skipped entirely, no slot, no row.
        _establishment(COMPANY_TOWN_FIX, CURRENT_SHA, CURRENT_AT, 2, "EST-3", "", "11122", "Stockholm", "Stockholms län"),
        _establishment(COMPANY_TOWN_FIX, CURRENT_SHA, CURRENT_AT, 3, "EST-2", "Kungsgatan 5", "111 43", "Stockholm", "Stockholms län"),
        _report(COMPANY_UNKNOWN_CODE, UNKNOWN_SHA, CURRENT_AT, "Ödevägen 3", "98765", "Ödeby", "Ödeby län"),
        _report(COMPANY_TIE, TIE_SHA, CURRENT_AT, "Tievägen 4", "11255", "Tievik", "Tie län"),
    ]


def _rescan_rows() -> list[str]:
    """A newer report for COMPANY_TOWN_FIX that keeps only EST-1: the two EST-2 slots must be
    tombstoned with the NEW report's observed_at."""
    return [
        _report(COMPANY_TOWN_FIX, RESCAN_SHA, RESCAN_AT, "Oxievägen 12", "238 40", "Malmö", "Skåne län"),
        _establishment(COMPANY_TOWN_FIX, RESCAN_SHA, RESCAN_AT, 0, "EST-1", "Oxievägen 12", "238 40", "Oxie", "Skåne län"),
    ]


def _statements() -> list[str]:
    return [
        *_schema(),
        *_register_rows(),
        *_ratsit_rows(),
        "SELECT '@@scope_1'",
        _scope(),
        _insert([COMPANY_TOWN_FIX, COMPANY_UNKNOWN_CODE, COMPANY_TIE]),
        "SELECT '@@rows_1'",
        ROWS_SQL,
        "SELECT '@@scope_after_1'",
        _scope(),
        # now64(3) resolves to milliseconds, so the second page must not tie the first page's
        # suggested_at -- the ReplacingMergeTree version column is suggested_at.
        "SELECT sleep(0.01) FORMAT Null",
        *_rescan_rows(),
        "SELECT '@@scope_2'",
        _scope(),
        _insert([COMPANY_TOWN_FIX]),
        "SELECT '@@rows_2'",
        ROWS_SQL,
        "SELECT '@@scope_after_2'",
        _scope(),
        "SELECT '@@changed_rows'",
        "SELECT * FROM (\n"
        + _bind(changed_rows_sql(), company_ids=[COMPANY_TOWN_FIX], normalizer_version=NORMALIZER_VERSION)
        + "\n) AS ordered ORDER BY slot",
    ]


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    return _sections(_run(_statements(), join_use_nulls=request.param))


def _by_slot(rows: list[list[str]]) -> dict[tuple[str, str], dict[str, str]]:
    return {
        (fields[0], fields[1]): dict(zip(ROW_COLUMNS, fields, strict=True))
        for fields in rows
    }


def test_the_scope_selects_every_company_with_a_report_then_converges(
    sections: dict[str, list[list[str]]],
) -> None:
    assert sections["scope_1"] == [[COMPANY_TOWN_FIX], [COMPANY_UNKNOWN_CODE], [COMPANY_TIE]]
    assert sections["scope_after_1"] == []


def test_the_company_row_takes_the_registers_postal_town(sections: dict[str, list[list[str]]]) -> None:
    """Spec 5.1. `23840` has OXIE twice and ` Oxie ` once among the delivering register rows,
    so OXIE wins on frequency after the trim; the three FELSTAD rows carry `has_company = 0`
    and would win if the filter were missing. Ratsit's own locality here is the MUNICIPALITY
    (Malmö), which is what the entity published before this slice."""
    rows = _by_slot(sections["rows_1"])
    company = rows[(COMPANY_TOWN_FIX, "company")]
    assert company["kind"] == "postal"
    assert company["source_record_uid"] == f"ratsit:{CURRENT_SHA}"
    assert company["street_address"] == "Oxievägen 12"
    assert company["postal_code"] == "238 40"          # as delivered; only the join key is digits
    assert company["post_town"] == "OXIE"              # not "Malmö"
    assert company["county"] == "Skåne län"
    assert company["raw_address"] == company["care_of"] == company["country_code"] == "NULL"
    assert company["observed_at"] == "2026-09-02 00:00:00.000"
    assert company["extractor_version"] == "ratsit-address-v2"


def test_an_unknown_postcode_keeps_ratsits_locality_and_a_tie_takes_the_first_spelling(
    sections: dict[str, list[list[str]]],
) -> None:
    """Spec 5.1: a postcode the register does not know -- about 1,000 of the delivered ones
    today -- keeps Ratsit's locality, and `ties broken by the alphabetically first
    spelling`."""
    rows = _by_slot(sections["rows_1"])
    assert rows[(COMPANY_UNKNOWN_CODE, "company")]["post_town"] == "Ödeby"
    assert rows[(COMPANY_TIE, "company")]["post_town"] == "ALFA"   # ALFA and BETA tie 1-1


def test_each_establishment_with_a_street_and_a_postcode_becomes_a_workplace_row(
    sections: dict[str, list[list[str]]],
) -> None:
    """Spec 5.2. Four establishments on the current report produce three rows: the streetless
    one is skipped, and the two that share the identifier EST-2 BOTH take the index suffix --
    a bare `est:EST-2` on either would collapse the pair in the ReplacingMergeTree."""
    rows = _by_slot(sections["rows_1"])
    assert sorted(slot for company_id, slot in rows if company_id == COMPANY_TOWN_FIX) == [
        "company", "est:EST-1", "est:EST-2:1", "est:EST-2:3",
    ]
    shared = rows[(COMPANY_TOWN_FIX, "est:EST-1")]
    assert shared["kind"] == "workplace"
    assert shared["source_record_uid"] == f"ratsit:{CURRENT_SHA}:est:0"
    # Same street and postcode as the company row: the fold merges the two (spec 5.4).
    assert (shared["street_address"], shared["postal_code"], shared["post_town"]) == (
        "Oxievägen 12", "238 40", "OXIE",
    )
    elsewhere = rows[(COMPANY_TOWN_FIX, "est:EST-2:1")]
    assert elsewhere["source_record_uid"] == f"ratsit:{CURRENT_SHA}:est:1"
    assert (elsewhere["street_address"], elsewhere["postal_code"], elsewhere["post_town"]) == (
        "Storgatan 1", "11122", "STOCKHOLM",
    )
    repeated = rows[(COMPANY_TOWN_FIX, "est:EST-2:3")]
    assert repeated["source_record_uid"] == f"ratsit:{CURRENT_SHA}:est:3"
    assert (repeated["street_address"], repeated["postal_code"], repeated["post_town"]) == (
        "Kungsgatan 5", "111 43", "STOCKHOLM",
    )
    # `EST-3` carried no street, and `EST-OLD` belongs to the superseded report.
    assert (COMPANY_TOWN_FIX, "est:EST-3") not in rows
    assert (COMPANY_TOWN_FIX, "est:EST-OLD") not in rows
    # Every row of the company carries the current report's stamp and hash.
    for company_id, slot in rows:
        if company_id == COMPANY_TOWN_FIX:
            assert rows[(company_id, slot)]["observed_at"] == "2026-09-02 00:00:00.000", slot
            assert OLD_SHA not in rows[(company_id, slot)]["source_record_uid"], slot


def test_a_rescan_that_drops_an_establishment_tombstones_its_slot(
    sections: dict[str, list[list[str]]],
) -> None:
    """Spec 5.3. The newer report keeps only EST-1, so both EST-2 slots get a NULL row that
    keeps the slot and the kind and carries the NEW report's observed_at -- if it carried the
    stored row's, argMax(observed_at, suggested_at) could pick the stale stamp out of the
    page's tie and re-select the company on every run for ever."""
    assert sections["scope_2"] == [[COMPANY_TOWN_FIX]]
    rows = _by_slot(sections["rows_2"])
    for slot in ("est:EST-2:1", "est:EST-2:3"):
        tombstone = rows[(COMPANY_TOWN_FIX, slot)]
        assert tombstone["kind"] == "workplace", slot
        assert tombstone["source_record_uid"] == "", slot
        assert tombstone["observed_at"] == "2026-09-06 00:00:00.000", slot
        for column in ("street_address", "postal_code", "post_town", "county",
                       "raw_address", "care_of", "country_code"):
            assert tombstone[column] == "NULL", (slot, column)
    survivor = rows[(COMPANY_TOWN_FIX, "est:EST-1")]
    assert survivor["source_record_uid"] == f"ratsit:{RESCAN_SHA}:est:0"
    assert survivor["observed_at"] == "2026-09-06 00:00:00.000"
    assert survivor["street_address"] == "Oxievägen 12"
    # The other two companies did not move.
    assert rows[(COMPANY_UNKNOWN_CODE, "company")]["observed_at"] == "2026-09-02 00:00:00.000"
    assert rows[(COMPANY_TIE, "company")]["observed_at"] == "2026-09-02 00:00:00.000"
    # And the scan converges: writing the tombstone takes the slot out of the live set.
    assert sections["scope_after_2"] == []


def test_the_normalize_hand_off_files_the_tombstones_as_no_address(
    sections: dict[str, list[list[str]]],
) -> None:
    """The normalizer is untouched by this slice; this proves the rows it now receives parse
    the way the fold needs. The corrected town reaches `city`, and a tombstone becomes
    `no_address`, which is what makes the fold drop that slot from the set (spec 5.3)."""
    rows = [
        tuple(None if field == "\\N" else field for field in fields)
        for fields in sections["changed_rows"]
    ]
    assert len(rows) == 4
    assert [row[2] for row in rows] == ["company", "est:EST-1", "est:EST-2:1", "est:EST-2:3"]
    by_slot = {row[2]: row for row in rows}
    for slot in ("company", "est:EST-1"):
        result = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(by_slot[slot], STAMP), strict=True))
        assert result["parse_status"] == "ok", slot
        assert result["street_name"] == "oxievägen", slot
        assert result["house_number"] == "12", slot
        assert result["postal_code"] == "23840", slot
        assert result["city"] == "oxie", slot      # the register town, folded -- not "malmö"
        assert result["normalized_address"] == "Oxievägen 12, 238 40 Oxie", slot
    assert by_slot["company"][5] == "postal"
    assert by_slot["est:EST-1"][5] == "workplace"
    for slot in ("est:EST-2:1", "est:EST-2:3"):
        result = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(by_slot[slot], STAMP), strict=True))
        assert result["parse_status"] == "no_address", slot
        assert result["kind"] == "workplace", slot
        assert result["city"] is None and result["street_name"] is None, slot
