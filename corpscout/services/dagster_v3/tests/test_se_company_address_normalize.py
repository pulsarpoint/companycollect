"""The normalize asset's scan, row shaping and writes (spec section 4), against a scripted
ClickHouse client. The SQL texts run for real in test_se_company_address_clickhouse_local.py."""

from datetime import UTC, datetime

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.normalize import (
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
from dagster_v3.defs.se_company.address.normalize_se import NORMALIZER_VERSION
from dagster_v3.defs.se_company.basic_info.extract import SCRATCH_SCOPE_PREFIX

STAMP = datetime(2026, 9, 6, 12, 0, 0, 123000, tzinfo=UTC)

# (company_id, source, slot, suggestion_id, suggested_at, kind, raw_address, care_of,
#  street_address, postal_code, post_town, county, country_code)
RAW_SCB = ("5561552760", "scb", "", "a" * 64, datetime(2026, 9, 1, tzinfo=UTC), "visiting_or_postal",
           None, None, "SICKLA INDUSTRIVÄG 19", "13134", "NACKA", None, None)
RAW_BV = ("5561552760", "bolagsverket", "", "b" * 64, datetime(2026, 9, 2, tzinfo=UTC), "postal",
          "Box 5305$$STOCKHOLM$10247$SE-LAND", None, None, None, None, None, None)
RAW_EMPTY = ("5560125220", "ratsit", "company", "c" * 64, datetime(2026, 9, 3, tzinfo=UTC), "postal",
             None, None, None, None, None, None, None)


class FakeClient:
    def __init__(self, *, scope_ids, rows):
        self.scope_pages = [list(scope_ids)]
        self.rows = list(rows)
        self.statements: list[tuple[str, object, object]] = []

    def execute(self, sql, params=None, settings=None):
        self.statements.append((sql, params, settings))
        if sql.startswith(("CREATE TABLE", "DROP TABLE")):
            return []
        if sql.startswith("INSERT INTO"):
            return []
        if sql.startswith(f"SELECT company_id FROM {SCRATCH_SCOPE_PREFIX}"):
            return [(i,) for i in (self.scope_pages.pop(0) if self.scope_pages else [])]
        if f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL" in sql:
            ids = set(params["company_ids"])
            return [r for r in self.rows if r[0] in ids]
        raise AssertionError(sql)

    def inserts(self):
        return [(s, list(p)) for s, p, _ in self.statements if s.startswith(f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE}")]


def test_normalized_row_has_the_21_values_in_column_order() -> None:
    row = normalized_row(RAW_SCB, STAMP)
    assert len(row) == len(tables.NORMALIZED_COLUMNS) == 21
    by_name = dict(zip(tables.NORMALIZED_COLUMNS, row, strict=True))
    assert by_name["company_id"] == "5561552760" and by_name["source"] == "scb" and by_name["slot"] == ""
    assert by_name["suggestion_id"] == "a" * 64
    assert by_name["suggested_at"] == RAW_SCB[4]
    assert by_name["kind"] == "visiting_or_postal"
    assert by_name["street_name"] == "sickla industriväg" and by_name["house_number"] == "19"
    assert by_name["postal_code"] == "13134" and by_name["city"] == "nacka" and by_name["country_code"] == "SE"
    assert by_name["normalized_address"] == "Sickla Industriväg 19, 131 34 Nacka"
    assert by_name["parse_status"] == "ok" and by_name["parse_notes"] == ""
    assert by_name["normalizer_version"] == NORMALIZER_VERSION
    assert by_name["normalized_at"] == STAMP
    assert len(by_name["address_key"]) == 64 and len(by_name["normalized_id"]) == 64


def test_normalized_id_is_the_key_plus_the_stamp() -> None:
    import hashlib
    row = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(RAW_BV, STAMP), strict=True))
    expected = hashlib.sha256("5561552760\nbolagsverket\n\n2026-09-06 12:00:00.123".encode()).hexdigest()
    assert row["normalized_id"] == expected
    assert row["box"] == "5305" and row["street_name"] is None
    assert row["normalized_address"] == "Box 5305, 102 47 Stockholm"


def test_an_empty_raw_row_normalizes_to_no_address_with_the_empty_key() -> None:
    row = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(RAW_EMPTY, STAMP), strict=True))
    assert row["parse_status"] == "no_address"
    assert row["normalized_address"] == ""
    assert all(row[c] is None for c in tables.COMPONENT_COLUMNS)


def test_sql_texts_read_final_rows_and_bind_ids_and_version() -> None:
    assert f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL" in changed_rows_sql()
    assert "LEFT ANTI JOIN" in changed_rows_sql() and "UNION ALL" in changed_rows_sql()
    assert "%(company_ids)s" in changed_rows_sql() and "%(normalizer_version)s" in changed_rows_sql()
    assert "r.suggested_at > n.suggested_at" in changed_rows_sql()
    assert "n.normalizer_version != %(normalizer_version)s" in changed_rows_sql()
    assert f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL" in changed_rows_sql()
    assert f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL" in all_rows_sql()
    assert "JOIN" not in all_rows_sql()
    assert changed_scope_sql().startswith("SELECT DISTINCT company_id FROM (")
    assert all_scope_sql() == f"SELECT DISTINCT company_id FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL"
    assert normalized_insert_sql() == (
        f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} ({', '.join(tables.NORMALIZED_COLUMNS)}) VALUES"
    )


def test_normalize_companies_reads_the_page_and_inserts_one_row_per_raw_row() -> None:
    client = FakeClient(scope_ids=[], rows=[RAW_SCB, RAW_BV, RAW_EMPTY])
    counts = normalize_companies(client, ["5561552760", "5560125220"], changed_only=True, normalized_at=STAMP, page_size=20_000)
    assert counts == NormalizeCounts(companies=2, pages=1, rows=3, ok=2, partial=0, no_address=1, foreign=0)
    [(sql, rows)] = client.inserts()
    assert sql == normalized_insert_sql()
    assert [r[1] for r in rows] == ["scb", "bolagsverket", "ratsit"]
    read = [s for s, _, _ in client.statements if "AS r FINAL" in s]
    assert read == [changed_rows_sql()]
    assert client.statements[0][1] == {"company_ids": ["5560125220", "5561552760"], "normalizer_version": NORMALIZER_VERSION}
    # No scratch table for a targeted call: the ids are paged in memory.
    assert not any(s.startswith("CREATE TABLE") for s, _, _ in client.statements)


def test_normalize_companies_with_changed_only_false_reads_every_raw_row() -> None:
    client = FakeClient(scope_ids=[], rows=[RAW_SCB])
    normalize_companies(client, ["5561552760"], changed_only=False, normalized_at=STAMP, page_size=20_000)
    read = [s for s, _, _ in client.statements if "AS r FINAL" in s]
    assert read == [all_rows_sql()]


def test_normalize_all_scans_into_a_scratch_table_and_pages_it() -> None:
    client = FakeClient(scope_ids=["5560125220", "5561552760"], rows=[RAW_SCB, RAW_BV, RAW_EMPTY])
    counts = normalize_all(client, changed_only=True, normalized_at=STAMP, page_size=20_000)
    assert counts.companies == 2 and counts.pages == 1 and counts.rows == 3
    created = [s for s, _, _ in client.statements if s.startswith("CREATE TABLE")]
    assert len(created) == 1 and created[0].split()[2].startswith(SCRATCH_SCOPE_PREFIX)
    scope_insert = next(s for s, _, _ in client.statements if s.startswith(f"INSERT INTO {SCRATCH_SCOPE_PREFIX}"))
    assert changed_scope_sql() in scope_insert
    assert any(s.startswith("DROP TABLE IF EXISTS") for s, _, _ in client.statements)


def test_counts_metadata_keys() -> None:
    assert set(NormalizeCounts(1, 1, 1, 1, 0, 0, 0).as_metadata()) == {
        "companies", "pages", "rows", "ok", "partial", "no_address", "foreign", "normalizer_version",
    }
