"""The normalize asset's scan, row shaping and writes (spec section 4), against a scripted
ClickHouse client. The SQL texts run for real in
tests/test_se_company_person_normalize_clickhouse_local.py."""

import hashlib
from datetime import UTC, datetime

from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.normalize import (
    NORMALIZE_ID_BOUND_QUERY_SETTINGS,
    PAGE_SIZE,
    NormalizeCounts,
    all_rows_sql,
    all_scope_sql,
    changed_rows_sql,
    changed_scope_sql,
    normalize_all,
    normalize_companies,
    normalized_insert_sql,
    normalized_row,
)
from dagster_v3.defs.se_company.person.normalize_se import NORMALIZER_VERSION

STAMP = datetime(2026, 9, 9, 12, 0, 0, 123000, tzinfo=UTC)

# (company_id, source, slot, suggestion_id, full_name, first_name, last_name, birth_year,
#  wikidata_id, role_original, role_key, fiscal_year, role_from, role_to, data)
RAW_BV = ("5561552760", "bolagsverket", "rec1:sig1", "a" * 64, None, "Anna", "Svensson",
          None, None, "Styrelseledamot", "board_member", 2024, None, None, '{"signatory_kind":"board"}')
RAW_ESEF = ("5561552760", "esef", "doc7:2", "b" * 64, "Öberg, Håkan", None, None,
            None, None, "VD", "chief_executive", 2023, None, None, '{"section":"signatures"}')
RAW_TOMBSTONE = ("5560125220", "wikidata", "Q9:l1", "c" * 64, None, None, None,
                 None, None, None, None, None, None, None, "{}")


class FakeClient:
    def __init__(self, *, scope_ids, rows):
        self.scope_pages = [list(scope_ids)]
        self.rows = list(rows)
        self.statements: list[tuple[str, object, object]] = []

    def execute(self, sql, params=None, settings=None):
        self.statements.append((sql, params, settings))
        if sql.startswith(("CREATE TABLE", "DROP TABLE", "INSERT INTO")):
            return []
        if sql.startswith(f"SELECT company_id FROM {tables.SCRATCH_SCOPE_PREFIX}"):
            return [(company_id,) for company_id in (self.scope_pages.pop(0) if self.scope_pages else [])]
        if f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL" in sql:
            wanted = set(params["company_ids"])
            return [row for row in self.rows if row[0] in wanted]
        raise AssertionError(sql)

    def inserts(self):
        prefix = f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE}"
        return [(sql, list(params)) for sql, params, _ in self.statements if sql.startswith(prefix)]


def test_normalized_row_has_the_23_values_in_column_order() -> None:
    row = normalized_row(RAW_BV, STAMP)
    assert len(row) == len(tables.NORMALIZED_COLUMNS) == 23
    by_name = dict(zip(tables.NORMALIZED_COLUMNS, row, strict=True))
    assert by_name["company_id"] == "5561552760" and by_name["source"] == "bolagsverket"
    assert by_name["slot"] == "rec1:sig1" and by_name["suggestion_id"] == "a" * 64
    assert by_name["normalizer_version"] == NORMALIZER_VERSION
    assert by_name["parse_status"] == "ok" and by_name["parse_notes"] == []
    assert by_name["first_tokens"] == ["anna"] and by_name["last_tokens"] == ["svensson"]
    assert by_name["display_name"] == "Anna Svensson"
    # role_key is the source's own code, passed through and used for the mapping; the map is
    # keyed on it, not on the label in role_original.
    assert by_name["role_key"] == "board_member"
    assert by_name["role_code"] == "board_member"
    assert by_name["role_year"] == 2024
    assert by_name["data"] == RAW_BV[14]
    assert by_name["normalized_at"] == STAMP


def test_normalized_id_is_the_suggestion_id_and_the_version() -> None:
    """Spec 3.2 -- NOT a stamp, so re-normalizing an unchanged row writes the identical id
    and the ReplacingMergeTree keeps one version instead of growing one per run."""
    row = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(RAW_ESEF, STAMP), strict=True))
    assert row["normalized_id"] == hashlib.sha256(f"{'b' * 64}\n{NORMALIZER_VERSION}".encode()).hexdigest()
    assert row["parse_status"] == "ok" and row["parse_notes"] == ["comma form"]
    assert row["display_name"] == "Håkan Öberg" and row["role_code"] == "chief_executive_officer"


def test_a_tombstone_row_normalizes_to_no_person() -> None:
    row = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(RAW_TOMBSTONE, STAMP), strict=True))
    assert row["parse_status"] == "no_person" and row["parse_notes"] == ["empty name"]
    assert row["display_name"] == "" and row["role_code"] is None and row["role_key"] is None
    assert row["first_tokens"] == [] and row["last_tokens"] == []
    assert row["data"] == "{}"


def test_the_read_sql_coerces_data_to_a_json_object() -> None:
    """`data` is a String holding a JSON object and the normalized table constrains it to
    one, so the read is what guarantees the write can land: a row whose text is an array, a
    scalar or nothing at all comes back as the empty object rather than violating the
    constraint 20,000 rows into a page."""
    for sql in (changed_rows_sql(), all_rows_sql()):
        assert "if(JSONType(r.data) = 'Object', r.data, '{}') AS data" in sql
        assert "toJSONString" not in sql


def test_sql_texts_read_final_rows_and_bind_ids_and_version() -> None:
    assert f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL" in changed_rows_sql()
    assert "LEFT ANTI JOIN" in changed_rows_sql() and "UNION ALL" in changed_rows_sql()
    assert "%(company_ids)s" in changed_rows_sql() and "%(normalizer_version)s" in changed_rows_sql()
    # The normalized table has no suggested_at: a changed observation changes suggestion_id.
    assert "n.suggestion_id != r.suggestion_id" in changed_rows_sql()
    assert "suggested_at" not in changed_rows_sql()
    assert "n.normalizer_version != %(normalizer_version)s" in changed_rows_sql()
    assert f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL" in changed_rows_sql()
    assert "JOIN" not in all_rows_sql()
    assert changed_scope_sql().startswith("SELECT DISTINCT company_id FROM (")
    assert all_scope_sql() == f"SELECT DISTINCT company_id FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL"


def test_the_insert_is_the_ordinary_block_statement() -> None:
    """Every column is a type clickhouse-driver knows -- `data` is a String, not the native
    JSON type -- so the insert is the same one-line VALUES header the address entity uses and
    the rows travel as a native block."""
    assert normalized_insert_sql() == (
        f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} "
        f"({', '.join(tables.NORMALIZED_COLUMNS)}) VALUES"
    )


def test_normalize_companies_reads_the_page_and_inserts_one_row_per_raw_row() -> None:
    client = FakeClient(scope_ids=[], rows=[RAW_BV, RAW_ESEF, RAW_TOMBSTONE])
    counts = normalize_companies(
        client, ["5561552760", "5560125220"], changed_only=True, normalized_at=STAMP, page_size=PAGE_SIZE
    )
    assert counts == NormalizeCounts(companies=2, pages=1, rows=3, ok=2, partial=0, no_person=1)
    [(sql, rows)] = client.inserts()
    assert sql == normalized_insert_sql()
    assert [row[1] for row in rows] == ["bolagsverket", "esef", "wikidata"]
    read = [sql for sql, _, _ in client.statements if "AS r FINAL" in sql]
    assert read == [changed_rows_sql()]
    assert client.statements[0][1] == {
        "company_ids": ["5560125220", "5561552760"], "normalizer_version": NORMALIZER_VERSION
    }
    assert client.statements[0][2] == NORMALIZE_ID_BOUND_QUERY_SETTINGS
    # No scratch table for a targeted call: the ids are paged in memory.
    assert not any(sql.startswith("CREATE TABLE") for sql, _, _ in client.statements)


def test_normalize_companies_with_changed_only_false_reads_every_raw_row() -> None:
    client = FakeClient(scope_ids=[], rows=[RAW_BV])
    normalize_companies(client, ["5561552760"], changed_only=False, normalized_at=STAMP, page_size=PAGE_SIZE)
    assert [sql for sql, _, _ in client.statements if "AS r FINAL" in sql] == [all_rows_sql()]


def test_normalize_all_scans_into_a_scratch_table_and_pages_it() -> None:
    client = FakeClient(scope_ids=["5560125220", "5561552760"], rows=[RAW_BV, RAW_ESEF, RAW_TOMBSTONE])
    counts = normalize_all(client, changed_only=True, normalized_at=STAMP, page_size=PAGE_SIZE)
    assert counts.companies == 2 and counts.pages == 1 and counts.rows == 3
    created = [sql for sql, _, _ in client.statements if sql.startswith("CREATE TABLE")]
    assert len(created) == 1 and created[0].split()[2].startswith(tables.SCRATCH_SCOPE_PREFIX)
    scope_insert = next(sql for sql, _, _ in client.statements if sql.startswith(f"INSERT INTO {tables.SCRATCH_SCOPE_PREFIX}"))
    assert changed_scope_sql() in scope_insert
    assert any(sql.startswith("DROP TABLE IF EXISTS") for sql, _, _ in client.statements)


def test_counts_metadata_keys() -> None:
    assert set(NormalizeCounts(1, 1, 1, 1, 0, 0).as_metadata()) == {
        "companies", "pages", "rows", "ok", "partial", "no_person", "normalizer_version",
    }


def test_a_full_page_renders_under_the_query_size_setting() -> None:
    """changed_rows_sql() binds %(company_ids)s four times (two UNION ALL branches, each with
    the outer WHERE plus the nested key select), so a full page of 12-digit ids renders far
    past basic-info's ID_BOUND_QUERY_SETTINGS -- which is why this module carries its own,
    wider setting. Modelled on the address entity's identical test. No server needed."""
    from types import SimpleNamespace

    from clickhouse_driver.util.escape import escape_params

    from dagster_v3.defs.se_company.basic_info.batch import ID_BOUND_QUERY_SETTINGS

    context = SimpleNamespace(
        server_info=SimpleNamespace(get_timezone=lambda: "UTC"),
        client_settings={"server_side_params": False},
    )
    for count in (PAGE_SIZE, 50_000):
        ids = [str(556_000_000_000 + index) for index in range(count)]
        rendered = changed_rows_sql() % escape_params(
            {"company_ids": ids, "normalizer_version": NORMALIZER_VERSION}, context
        )
        rendered_size = len(rendered.encode("utf-8"))
        if count == PAGE_SIZE:
            assert rendered_size > ID_BOUND_QUERY_SETTINGS["max_query_size"]
        assert rendered_size < NORMALIZE_ID_BOUND_QUERY_SETTINGS["max_query_size"]
