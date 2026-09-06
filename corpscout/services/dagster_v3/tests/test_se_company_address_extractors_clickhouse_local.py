"""The three address extractors' SQL on a real ClickHouse (spec 2026-09-06 section 7): each
source's scope converges, its SELECT produces the expected wide raw row, the suggestion_id
stamp matches the hash formula, a tombstoned register row writes a NULL raw row and
re-selects, and the normalize hand-off (`changed_rows_sql()`/`normalized_row()`) turns the
three raw rows -- one of them freshly tombstoned -- into the expected parse statuses. Runs
under join_use_nulls 0 and 1."""

import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.address import bolagsverket, ratsit, scb, tables
from dagster_v3.defs.se_company.address.normalize import RAW_ROW_COLUMNS, changed_rows_sql, normalized_row
from dagster_v3.defs.se_company.address.normalize_se import NORMALIZER_VERSION
from dagster_v3.defs.se_company.address.suggestions import ADDRESS_TARGET
from dagster_v3.defs.se_company.basic_info.extract import changed_scope_sql, insert_page_sql
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
from tests.test_se_company_basic_info_clickhouse_local import _bind
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATIONS = (
    "000373_corpscout_se_scb_companies.up.sql",
    "000374_corpscout_se_bolagsverket_companies.up.sql",
    "000382_corpscout_se_company_address_suggestion.up.sql",
    "000383_corpscout_se_company_address_normalized.up.sql",
)
# se_ratsit_company is not created by any of the four migrations above (its own migration,
# 000343, is out of scope here), so the fixture supplies it with no risk of colliding with a
# CREATE TABLE the migrations already issued for one of these four.
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "se_basic_info_source_tables.sql"

COMPANY_SCB_BV = "5561552760"
COMPANY_RATSIT = "5560125220"
RATSIT_RESULT_SHA256 = "d" * 64
STAMP = datetime(2026, 9, 6, 12, 0, 0, tzinfo=UTC)

RAW_ROW_CHECK_COLUMNS = (
    "company_id", "source", "slot", "kind", "raw_address", "care_of", "street_address",
    "postal_code", "post_town", "county", "extractor_version", "decided_by", "note",
    "replaces_key", "suggested_at",
)
RAW_ROWS_SQL = (
    "SELECT company_id, source, slot, kind, raw_address, care_of, street_address, postal_code, "
    "post_town, county, extractor_version, decided_by, note, replaces_key, toString(suggested_at) "
    f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL ORDER BY company_id, source"
)
# The same hash formula as ADDRESS_TRAILING_SELECT_SQL (suggestions.py): '\\n' in the Python
# source is a literal backslash-n in the rendered SQL text, which ClickHouse's string literal
# turns into an actual newline when concat() builds the hashed string -- so this reproduces
# exactly what stamped suggestion_id.
IDENTITY_CHECK_SQL = (
    f"SELECT count() FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL "
    "WHERE suggestion_id != lower(hex(SHA256(concat(company_id, '\\n', toString(source), '\\n', "
    "slot, '\\n', toString(suggested_at)))))"
)
SCB_AFTER_TOMBSTONE_SQL = (
    "SELECT street_address, postal_code, post_town, care_of, toString(suggested_at) "
    f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL "
    f"WHERE company_id = '{COMPANY_SCB_BV}' AND source = 'scb' AND slot = ''"
)


def _schema() -> list[str]:
    statements = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(("CREATE DATABASE", "CREATE TABLE")):
                statements.append(statement)
    statements += [s.strip() for s in FIXTURE.read_text(encoding="utf-8").split(";") if s.strip()]
    return statements


def _run(statements: list[str], *, join_use_nulls: int) -> list[str]:
    script = f"SET join_use_nulls = {join_use_nulls};\n" + ";\n".join(statements) + ";\n"
    completed = subprocess.run(_clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900)
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _scope_sql(current_sql: str, source: str, **extra) -> str:
    return _bind(changed_scope_sql(current_sql=current_sql, target=ADDRESS_TARGET), source=source, **extra)


def _scope(current_sql: str, source: str, **extra) -> str:
    # The scope SQL is unpaged now (`scope_pages` runs it into a scratch table and pages
    # that), so the harness renders it alone and sorts for a stable printed order.
    return _scope_sql(current_sql, source, **extra) + "\nORDER BY company_id"


def _insert(select_sql: str, ids: list[str], *, extractor_version: str, **extra) -> str:
    return _bind(
        insert_page_sql(select_sql=select_sql, target=ADDRESS_TARGET),
        company_ids=ids, source_run_id="run-1", extractor_version=extractor_version, **extra,
    )


def _labelled(sql: str, label: str) -> str:
    """Tag a scope query's rows so several scopes combined in one UNION ALL stay tellable
    apart."""
    return f"SELECT '{label}', company_id FROM ({sql}) ORDER BY company_id"


def _ordered(sql: str, order_by: str) -> str:
    """Wrap a (possibly UNION ALL) statement so an ORDER BY unambiguously applies to it."""
    return f"SELECT * FROM ({sql}) AS ordered ORDER BY {order_by}"


def _as_row(fields: list[str]) -> tuple[str | None, ...]:
    """One changed_rows_sql() TSV line -> the tuple shape normalized_row() expects: \\N ->
    None, every other field kept as the string ClickHouse printed (including suggested_at)."""
    assert len(fields) == len(RAW_ROW_COLUMNS), fields
    return tuple(None if field == "\\N" else field for field in fields)


def _sections(lines: list[str]) -> dict[str, list[list[str]]]:
    """Split _run()'s flat line list on the `SELECT '@@name'` markers in the script."""
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in lines:
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        else:
            result[current].append(line.split("\t"))
    return result


def _statements() -> list[str]:
    scb_insert_1 = (
        "INSERT INTO corpscout.se_scb_companies (company_id, company_id_raw, street_address, postal_code, "
        "post_town, has_company, observed_at, source_run_id, source_record_id, source_payload_hash) VALUES "
        f"('{COMPANY_SCB_BV}', '{COMPANY_SCB_BV}', 'SICKLA INDUSTRIVÄG 19', '13134', 'NACKA', 1, "
        "toDateTime64('2026-09-01 00:00:00', 3, 'UTC'), '', 's1', 'h1')"
    )
    bv_insert = (
        "INSERT INTO corpscout.se_bolagsverket_companies (company_id, company_id_raw, postal_address, "
        "has_company, observed_at, source_run_id, source_record_id, source_payload_hash) VALUES "
        f"('{COMPANY_SCB_BV}', '{COMPANY_SCB_BV}', 'Box 5305$$STOCKHOLM$10247$SE-LAND', 1, "
        "toDateTime64('2026-09-02 00:00:00', 3, 'UTC'), '', 's2', 'h2')"
    )
    ratsit_insert = (
        "INSERT INTO corpscout.se_ratsit_company (company_id, result_sha256, normalizer_version, schema_version, "
        "parser_version, requested_url, source_url, result_bucket, result_object_key, name, organization_number, "
        "address_street, address_postal_code, address_locality, address_county, normalized_at) VALUES "
        f"('{COMPANY_RATSIT}', '{RATSIT_RESULT_SHA256}', '{RATSIT_NORMALIZER_VERSION}', 1, 'p', 'u', 'u', 'b', "
        "'k', 'Ratsit Company AB', '556012-5220', 'c/o Anna Svensson Storgatan 5', '11122', 'Stockholm', "
        "'Stockholms län', toDateTime64('2026-09-03 00:00:00', 6, 'UTC'))"
    )
    scb_tombstone = (
        "INSERT INTO corpscout.se_scb_companies (company_id, company_id_raw, has_company, observed_at, care_of, "
        "street_address, postal_code, post_town, source_run_id, source_record_id, source_payload_hash) VALUES "
        f"('{COMPANY_SCB_BV}', '{COMPANY_SCB_BV}', 0, toDateTime64('2026-09-05 00:00:00', 3, 'UTC'), "
        "NULL, NULL, NULL, NULL, '', '', '')"
    )
    reconverged = " UNION ALL ".join(
        f"({query})"
        for query in (
            _labelled(_scope_sql(scb.scb_current_sql(), "scb"), "scb"),
            _labelled(_scope_sql(bolagsverket.bolagsverket_current_sql(), "bolagsverket"), "bolagsverket"),
            _labelled(
                _scope_sql(ratsit.ratsit_current_sql(), "ratsit", **ratsit.RATSIT_ADDRESS_SELECT_PARAMS), "ratsit"
            ),
        )
    )
    changed_rows = _ordered(
        _bind(changed_rows_sql(), company_ids=[COMPANY_SCB_BV, COMPANY_RATSIT], normalizer_version=NORMALIZER_VERSION),
        "company_id, source, slot",
    )
    return [
        *_schema(),
        scb_insert_1,
        bv_insert,
        ratsit_insert,
        "SELECT '@@scb_scope_1'",
        _scope(scb.scb_current_sql(), "scb"),
        "SELECT '@@bv_scope_1'",
        _scope(bolagsverket.bolagsverket_current_sql(), "bolagsverket"),
        "SELECT '@@ratsit_scope_1'",
        _scope(ratsit.ratsit_current_sql(), "ratsit", **ratsit.RATSIT_ADDRESS_SELECT_PARAMS),
        _insert(scb.scb_select_sql(), [COMPANY_SCB_BV], extractor_version=scb.SCB_ADDRESS_EXTRACTOR_VERSION),
        _insert(
            bolagsverket.bolagsverket_select_sql(), [COMPANY_SCB_BV],
            extractor_version=bolagsverket.BOLAGSVERKET_ADDRESS_EXTRACTOR_VERSION,
        ),
        _insert(
            ratsit.ratsit_select_sql(), [COMPANY_RATSIT],
            extractor_version=ratsit.RATSIT_ADDRESS_EXTRACTOR_VERSION, **ratsit.RATSIT_ADDRESS_SELECT_PARAMS,
        ),
        "SELECT '@@raw_rows'",
        RAW_ROWS_SQL,
        "SELECT '@@identity_check'",
        IDENTITY_CHECK_SQL,
        "SELECT '@@reconverged'",
        reconverged,
        # Force the gap past now64(3)'s millisecond resolution so this suggested_at cannot tie the earlier SCB insert's (same hazard test_se_company_basic_info_extractors_clickhouse_local.py notes).
        "SELECT sleep(0.01) FORMAT Null",
        scb_tombstone,
        "SELECT '@@scb_scope_3'",
        _scope(scb.scb_current_sql(), "scb"),
        _insert(scb.scb_select_sql(), [COMPANY_SCB_BV], extractor_version=scb.SCB_ADDRESS_EXTRACTOR_VERSION),
        "SELECT '@@scb_after_tombstone'",
        SCB_AFTER_TOMBSTONE_SQL,
        "SELECT '@@changed_rows'",
        changed_rows,
    ]


@pytest.fixture(
    scope="module",
    params=(0, 1),
    ids=("join_use_nulls_off", "join_use_nulls_on"),
)
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    """Runs the whole script once per join_use_nulls setting (one clickhouse-local invocation
    each), shared across the test functions below rather than re-run per test."""
    return _sections(_run(_statements(), join_use_nulls=request.param))


def test_each_sources_scope_selects_its_company_before_anything_is_suggested(
    sections: dict[str, list[list[str]]],
) -> None:
    assert sections["scb_scope_1"] == [[COMPANY_SCB_BV]]
    assert sections["bv_scope_1"] == [[COMPANY_SCB_BV]]
    assert sections["ratsit_scope_1"] == [[COMPANY_RATSIT]]


def test_raw_rows_hold_each_sources_delivered_columns(sections: dict[str, list[list[str]]]) -> None:
    rows = [dict(zip(RAW_ROW_CHECK_COLUMNS, fields, strict=True)) for fields in sections["raw_rows"]]
    assert len(rows) == 3
    by_key = {(row["company_id"], row["source"]): row for row in rows}

    scb_row = by_key[(COMPANY_SCB_BV, "scb")]
    assert scb_row["slot"] == ""
    assert scb_row["kind"] == "visiting_or_postal"
    assert scb_row["raw_address"] == "\\N"
    assert scb_row["street_address"] == "SICKLA INDUSTRIVÄG 19"
    assert scb_row["postal_code"] == "13134"
    assert scb_row["post_town"] == "NACKA"
    assert scb_row["extractor_version"] == scb.SCB_ADDRESS_EXTRACTOR_VERSION
    assert scb_row["decided_by"] == scb_row["note"] == scb_row["replaces_key"] == "\\N"

    bv_row = by_key[(COMPANY_SCB_BV, "bolagsverket")]
    assert bv_row["slot"] == ""
    assert bv_row["kind"] == "postal"
    assert bv_row["raw_address"] == "Box 5305$$STOCKHOLM$10247$SE-LAND"
    assert bv_row["street_address"] == "\\N"
    assert bv_row["extractor_version"] == bolagsverket.BOLAGSVERKET_ADDRESS_EXTRACTOR_VERSION
    assert bv_row["decided_by"] == bv_row["note"] == bv_row["replaces_key"] == "\\N"

    ratsit_row = by_key[(COMPANY_RATSIT, "ratsit")]
    assert ratsit_row["slot"] == "company"
    assert ratsit_row["street_address"] == "c/o Anna Svensson Storgatan 5"
    assert ratsit_row["county"] == "Stockholms län"
    assert ratsit_row["extractor_version"] == ratsit.RATSIT_ADDRESS_EXTRACTOR_VERSION
    assert ratsit_row["decided_by"] == ratsit_row["note"] == ratsit_row["replaces_key"] == "\\N"


def test_suggestion_id_matches_the_stamp_hash_for_every_row(sections: dict[str, list[list[str]]]) -> None:
    assert sections["identity_check"] == [["0"]]


def test_all_three_scopes_converge_after_insert(sections: dict[str, list[list[str]]]) -> None:
    assert sections["reconverged"] == []


def test_scb_tombstone_reselects_and_writes_a_null_raw_row(sections: dict[str, list[list[str]]]) -> None:
    first = dict(
        zip(
            RAW_ROW_CHECK_COLUMNS,
            next(fields for fields in sections["raw_rows"] if fields[0] == COMPANY_SCB_BV and fields[1] == "scb"),
            strict=True,
        )
    )
    assert sections["scb_scope_3"] == [[COMPANY_SCB_BV]]
    after = dict(
        zip(("street_address", "postal_code", "post_town", "care_of", "suggested_at"),
            sections["scb_after_tombstone"][0], strict=True)
    )
    assert after["street_address"] == after["postal_code"] == after["post_town"] == after["care_of"] == "\\N"
    assert after["suggested_at"] > first["suggested_at"]


def test_normalize_hand_off_gives_the_expected_parse_statuses(sections: dict[str, list[list[str]]]) -> None:
    rows = [_as_row(fields) for fields in sections["changed_rows"]]
    assert len(rows) == 3
    by_source = {row[1]: row for row in rows}

    scb_result = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(by_source["scb"], STAMP), strict=True))
    assert scb_result["parse_status"] == "no_address"

    bv_result = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(by_source["bolagsverket"], STAMP), strict=True))
    assert bv_result["parse_status"] == "ok"
    assert bv_result["box"] == "5305"

    ratsit_result = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(by_source["ratsit"], STAMP), strict=True))
    assert ratsit_result["parse_status"] == "ok"
    assert ratsit_result["care_of"] == "anna svensson"
    assert ratsit_result["street_name"] == "storgatan"
    assert ratsit_result["house_number"] == "5"
