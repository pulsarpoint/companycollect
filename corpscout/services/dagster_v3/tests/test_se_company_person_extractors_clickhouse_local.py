"""The person extractors' SQL on a real ClickHouse (spec 2026-09-09 sections 3.1 and 6).

Claims a fake client cannot settle:
1. Each page select really produces the sixteen suggestion columns, in a UNION ALL whose
   live and tombstone branches agree on every column type.
2. `data` is a JSON object on every row -- the table's CONSTRAINT valid_data (Code: 469) is
   the judge, and the tombstone branch's literal '{}' has to pass it too.
3. suggestion_id equals sha256(company_id, source, slot, suggested_at) for every row, so
   the WITH-bound stamp really is the instant the id hashed.
4. The state-hash scope selects a company before anything is suggested, selects nothing
   after the page is written (it converges), selects it again when the rebuilt source drops
   one of its rows, and converges again once the tombstone is written -- under
   join_use_nulls 0 and 1, which the scope's two aggregations exist to be immune to.
5. A company the register flags has_company = 0 has every remaining slot tombstoned even
   though the signatory table still holds its rows (spec section 6), and converges too.
6. A company outside se_company_basic_info never reaches the suggestion table.
7. The normalize hand-off (`changed_rows_sql()` + `normalized_row()`) reads what these
   extractors wrote and gives the expected parse statuses, the tombstone included.
"""

import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.basic_info.extract import insert_page_sql
from dagster_v3.defs.se_company.person import bolagsverket, tables
from dagster_v3.defs.se_company.person.normalize import (
    RAW_ROW_COLUMNS,
    changed_rows_sql,
    normalized_row,
)
from dagster_v3.defs.se_company.person.normalize_se import NORMALIZER_VERSION
from dagster_v3.defs.se_company.person.suggestions import PERSON_SELECT_COLUMNS, PERSON_TARGET
from tests.clickhouse_local import clickhouse_local_command, render

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
# (file, the exact CREATE TABLE header line to keep). The trailing newline anchors the whole
# name: `corpscout.se_company_person_` would also be a prefix of five dropped tables, and
# `corpscout.esef_document_people` is a prefix of esef_document_people_legacy.
WANTED_CREATES = (
    ("000396_corpscout_se_company_person_entity.up.sql", "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_suggestion\n"),
    ("000396_corpscout_se_company_person_entity.up.sql", "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_normalized\n"),
    ("000377_corpscout_se_company_basic_info.up.sql", "CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info\n"),
    ("000374_corpscout_se_bolagsverket_companies.up.sql", "CREATE TABLE IF NOT EXISTS corpscout.se_bolagsverket_companies\n"),
    ("000395_corpscout_esef_country_agnostic_products.up.sql", "CREATE TABLE IF NOT EXISTS corpscout.esef_document_people\n"),
)

COMPANY_BV = "5561552760"
COMPANY_OUTSIDE = "5569999999"
STATEMENT_KEY = "st1"
BOARD_DATA = '{"signatory_kind":"board_signature","statement_key":"st1","person_seq":"1"}'
CERT_DATA = '{"signatory_kind":"certification","statement_key":"st1","person_seq":"1"}'

RAW_ROW_CHECK_COLUMNS = (*PERSON_SELECT_COLUMNS, "suggested_at")
RAW_ROWS_SQL = (
    f"SELECT {', '.join(PERSON_SELECT_COLUMNS)}, toString(suggested_at) "
    f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL ORDER BY company_id, source, slot"
)
# The same hash formula as PERSON_TRAILING_SELECT_SQL: '\\n' in the Python source is a
# literal backslash-n in the SQL text, which ClickHouse's string literal turns into a real
# newline when concat() builds the hashed string.
IDENTITY_CHECK_SQL = (
    f"SELECT count() FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL "
    "WHERE suggestion_id != lower(hex(SHA256(concat(company_id, '\\n', toString(source), '\\n', "
    "slot, '\\n', toString(suggested_at)))))"
)
DATA_CHECK_SQL = (
    f"SELECT countIf(JSONType(data) != 'Object') FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL"
)
OUTSIDE_CHECK_SQL = (
    f"SELECT count() FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL "
    f"WHERE company_id = '{COMPANY_OUTSIDE}'"
)


def _statements(path: Path, keep) -> list[str]:
    out: list[str] = []
    for raw in path.read_text(encoding="utf-8").split(";"):
        statement = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if statement and keep(statement):
            out.append(statement)
    return out


def _schema() -> list[str]:
    schema = ["CREATE DATABASE IF NOT EXISTS corpscout"]
    for name, header in WANTED_CREATES:
        found = _statements(MIGRATIONS_DIR / name, lambda s, h=header: s.startswith(h.rstrip("\n")) and h in s + "\n")
        assert len(found) == 1, (name, header, len(found))
        schema += found
    for fixture in ("se_basic_info_source_tables.sql", "se_company_person_source_tables.sql"):
        schema += [s.strip() for s in (FIXTURES_DIR / fixture).read_text(encoding="utf-8").split(";") if s.strip()]
    return schema


def _insert(select_sql: str, ids: list[str], *, extractor_version: str) -> str:
    return render(
        insert_page_sql(select_sql=select_sql, target=PERSON_TARGET),
        {"company_ids": ids, "source_run_id": "run-1", "extractor_version": extractor_version},
    )


def _scope(scope_sql: str, source: str) -> str:
    return render(scope_sql, {"source": source}) + "\nORDER BY company_id"


def _ordered(sql: str, order_by: str) -> str:
    return f"SELECT * FROM ({sql}) AS ordered ORDER BY {order_by}"


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


def _as_row(fields: list[str]) -> tuple[str | None, ...]:
    assert len(fields) == len(RAW_ROW_COLUMNS), fields
    return tuple(None if field == "\\N" else field for field in fields)


SIGNATORY_COLUMNS = (
    "company_id, fiscal_year, statement_key, signatory_kind, person_seq, "
    "first_name, last_name, role_original, role_kind, resolved_at"
)
BOARD_ROW = (
    f"('{COMPANY_BV}', 2024, '{STATEMENT_KEY}', 'board_signature', 1, 'Anna', 'Svensson', "
    "'Styrelseledamot', 'board_member', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'))"
)
CERT_ROW = (
    f"('{COMPANY_BV}', 2024, '{STATEMENT_KEY}', 'certification', 1, 'Anna', 'Svensson', "
    "'Styrelseledamot', 'board_member', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'))"
)
OUTSIDE_ROW = (
    f"('{COMPANY_OUTSIDE}', 2024, 'st9', 'board_signature', 1, 'Nils', 'Utanfor', "
    "'', 'unknown', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'))"
)


def _signatory_insert(rows: tuple[str, ...]) -> str:
    return (
        f"INSERT INTO corpscout.se_financial_report_signatories ({SIGNATORY_COLUMNS}) VALUES "
        + ", ".join(rows)
    )


def _register_row(company_id: str, observed_at: str, has_company: int) -> str:
    """One se_bolagsverket_companies row. The table is ReplacingMergeTree(observed_at) ordered
    by company_id, so a later row with has_company = 0 is the register's own tombstone."""
    return (
        "INSERT INTO corpscout.se_bolagsverket_companies (company_id, observed_at, has_company) "
        f"VALUES ('{company_id}', toDateTime64('{observed_at}', 3, 'UTC'), {has_company})"
    )


def _script_statements() -> list[str]:
    bv_scope = _scope(bolagsverket.bolagsverket_changed_scope_sql(), "bolagsverket")
    bv_insert = _insert(
        bolagsverket.bolagsverket_select_sql(), [COMPANY_BV],
        extractor_version=bolagsverket.BOLAGSVERKET_PERSON_EXTRACTOR_VERSION,
    )
    changed_rows = _ordered(
        render(changed_rows_sql(), {"company_ids": [COMPANY_BV], "normalizer_version": NORMALIZER_VERSION}),
        "company_id, source, slot",
    )
    return [
        *_schema(),
        # The universe: COMPANY_OUTSIDE deliberately has no basic-info row.
        f"INSERT INTO corpscout.se_company_basic_info (company_id) VALUES ('{COMPANY_BV}')",
        _register_row(COMPANY_BV, "2026-09-01 00:00:00", 1),
        _signatory_insert((BOARD_ROW, CERT_ROW, OUTSIDE_ROW)),
        "SELECT '@@bv_scope_1'",
        bv_scope,
        bv_insert,
        "SELECT '@@bv_rows_1'",
        RAW_ROWS_SQL,
        "SELECT '@@identity_check'",
        IDENTITY_CHECK_SQL,
        "SELECT '@@data_check'",
        DATA_CHECK_SQL,
        "SELECT '@@outside_check'",
        OUTSIDE_CHECK_SQL,
        "SELECT '@@bv_scope_2'",
        bv_scope,
        # The source is rebuilt whole (stage + EXCHANGE TABLES on prod) and this time the
        # certification signature line is gone.
        "SELECT sleep(0.01) FORMAT Null",
        "TRUNCATE TABLE corpscout.se_financial_report_signatories",
        _signatory_insert((BOARD_ROW, OUTSIDE_ROW)),
        "SELECT '@@bv_scope_3'",
        bv_scope,
        bv_insert,
        "SELECT '@@bv_rows_2'",
        RAW_ROWS_SQL,
        "SELECT '@@bv_scope_4'",
        bv_scope,
        "SELECT '@@data_check_2'",
        DATA_CHECK_SQL,
        "SELECT '@@changed_rows'",
        changed_rows,
        # The register deregisters the company (spec section 6). The signatory table still
        # holds its board signature line -- reports are never deleted -- so only the
        # has_company flag can retire the slot.
        "SELECT sleep(0.01) FORMAT Null",
        _register_row(COMPANY_BV, "2026-09-02 00:00:00", 0),
        "SELECT '@@bv_scope_5'",
        bv_scope,
        bv_insert,
        "SELECT '@@bv_rows_3'",
        RAW_ROWS_SQL,
        "SELECT '@@bv_scope_6'",
        bv_scope,
        "SELECT '@@data_check_3'",
        DATA_CHECK_SQL,
    ]


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    script = f"SET join_use_nulls = {request.param};\n" + ";\n".join(_script_statements()) + ";\n"
    try:
        completed = subprocess.run(
            clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return _sections([line for line in completed.stdout.splitlines() if line.strip()])


def _rows(sections: dict[str, list[list[str]]], name: str) -> list[dict[str, str]]:
    return [dict(zip(RAW_ROW_CHECK_COLUMNS, fields, strict=True)) for fields in sections[name]]


def test_the_scope_selects_the_company_then_converges(sections) -> None:
    assert sections["bv_scope_1"] == [[COMPANY_BV]]
    assert sections["bv_scope_2"] == []


def test_both_signature_lines_land_as_their_own_slot(sections) -> None:
    rows = _rows(sections, "bv_rows_1")
    assert len(rows) == 2
    assert {row["data"] for row in rows} == {BOARD_DATA, CERT_DATA}
    assert len({row["slot"] for row in rows}) == 2
    for row in rows:
        assert row["source"] == "bolagsverket"
        assert row["first_name"] == "Anna" and row["last_name"] == "Svensson"
        assert row["full_name"] == "\\N"
        assert row["role_original"] == "Styrelseledamot"
        assert row["role_key"] == "board_member"
        assert row["fiscal_year"] == "2024"
        assert row["document_ref"] == STATEMENT_KEY
        assert row["birth_year"] == row["wikidata_id"] == "\\N"
        assert row["role_from"] == row["role_to"] == "\\N"
        # slot = report record uid + signatory uid, both 64 hex characters.
        record_uid, signatory_uid = row["slot"].split(":")
        assert len(record_uid) == len(signatory_uid) == 64
        assert row["source_record_id"] == record_uid


def test_suggestion_id_matches_the_stamp_hash_and_data_is_always_an_object(sections) -> None:
    assert sections["identity_check"] == [["0"]]
    assert sections["data_check"] == [["0"]]
    assert sections["data_check_2"] == [["0"]]


def test_a_company_outside_the_basic_info_universe_is_never_written(sections) -> None:
    assert sections["outside_check"] == [["0"]]


def test_a_vanished_signature_line_is_tombstoned_and_the_scope_reconverges(sections) -> None:
    assert sections["bv_scope_3"] == [[COMPANY_BV]]
    before = {row["slot"]: row for row in _rows(sections, "bv_rows_1")}
    rows = _rows(sections, "bv_rows_2")
    assert len(rows) == 2
    tombstones = [row for row in rows if row["data"] == "{}"]
    assert len(tombstones) == 1
    [tombstone] = tombstones
    for column in ("full_name", "first_name", "last_name", "birth_year", "wikidata_id",
                   "role_original", "role_key", "fiscal_year", "role_from", "role_to",
                   "document_ref"):
        assert tombstone[column] == "\\N", column
    assert tombstone["source_record_id"] == ""
    assert before[tombstone["slot"]]["data"] == CERT_DATA
    assert tombstone["suggested_at"] > before[tombstone["slot"]]["suggested_at"]
    [survivor] = [row for row in rows if row["data"] != "{}"]
    assert survivor["data"] == BOARD_DATA
    assert survivor["suggested_at"] > before[survivor["slot"]]["suggested_at"]
    assert sections["bv_scope_4"] == []


def test_a_deregistered_company_has_its_remaining_slots_tombstoned(sections) -> None:
    """Spec section 6: tombstones on has_company = 0. The register flips, the company's
    signature lines leave the live branch, and the slot still live is retired -- while the slot
    already tombstoned is left exactly as it was, which is what makes the scope converge."""
    assert sections["bv_scope_5"] == [[COMPANY_BV]]
    before = {row["slot"]: row for row in _rows(sections, "bv_rows_2")}
    board_slot = next(slot for slot, row in before.items() if row["data"] != "{}")
    cert_slot = next(slot for slot, row in before.items() if row["data"] == "{}")
    rows = {row["slot"]: row for row in _rows(sections, "bv_rows_3")}
    assert set(rows) == {board_slot, cert_slot}
    for slot, row in rows.items():
        assert row["data"] == "{}", slot
        assert row["first_name"] == row["last_name"] == row["role_key"] == "\\N", slot
        assert row["source_record_id"] == "", slot
    assert rows[board_slot]["suggested_at"] > before[board_slot]["suggested_at"]
    # Already a tombstone, so the page did not rewrite it.
    assert rows[cert_slot]["suggested_at"] == before[cert_slot]["suggested_at"]
    assert sections["bv_scope_6"] == []
    assert sections["data_check_3"] == [["0"]]


def test_the_normalize_hand_off_gives_the_expected_parse_statuses(sections) -> None:
    rows = [normalized_row(_as_row(fields), None) for fields in sections["changed_rows"]]
    status = tables.NORMALIZED_COLUMNS.index("parse_status")
    notes = tables.NORMALIZED_COLUMNS.index("parse_notes")
    code = tables.NORMALIZED_COLUMNS.index("role_code")
    name = tables.NORMALIZED_COLUMNS.index("display_name")
    by_status = sorted((row[status], row[name], row[code], tuple(row[notes])) for row in rows)
    assert by_status == [
        ("no_person", "", None, ("empty name",)),
        ("ok", "Anna Svensson", "board_member", ()),
    ]
