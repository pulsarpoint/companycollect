"""Spec 3.2 change rule and the shared page loop, against a scripted fake client."""

import pytest

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.batch import ID_BOUND_QUERY_SETTINGS
from dagster_v3.defs.se_company.basic_info.extract import (
    SCAN_QUERY_SETTINGS,
    SCRATCH_SCOPE_PREFIX,
    SUGGESTION_SELECT_COLUMNS,
    ExtractConfig,
    ExtractCounts,
    changed_scope_sql,
    count_page_sql,
    insert_page_sql,
    run_extractor,
    since_scope_sql,
)

CURRENT = "SELECT company_id, observed_at FROM corpscout.se_scb_companies FINAL WHERE has_company = 1"
SELECT = "SELECT company_id, 'scb' AS source FROM corpscout.se_scb_companies WHERE company_id IN %(company_ids)s"
SCOPE_INSERT = f"INSERT INTO {SCRATCH_SCOPE_PREFIX}"
SUGGESTION_INSERT = f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE}"


class FakeClient:
    """The scan writes its id set into a scratch table and pages that table: CREATE, the
    scope INSERT and DROP answer `[]`; each scratch page read serves the next list from
    `scope_pages`. Candidate counts come from `candidates`; every statement is recorded."""

    def __init__(self, *, scope_pages, candidates=0):
        self.scope_pages = list(scope_pages)
        self.candidates = candidates
        self.statements: list[tuple[str, object, object]] = []

    def execute(self, sql, params=None, settings=None):
        self.statements.append((sql, params, settings))
        if sql.startswith(("CREATE TABLE", "DROP TABLE", "INSERT INTO")):
            return []
        if "AS candidates" in sql:
            return [(self.candidates,)]
        if sql.startswith(f"SELECT company_id FROM {SCRATCH_SCOPE_PREFIX}"):
            return [(i,) for i in (self.scope_pages.pop(0) if self.scope_pages else [])]
        raise AssertionError(sql)

    def scope_inserts(self):
        return [(s, p, st) for s, p, st in self.statements if s.startswith(SCOPE_INSERT)]

    def scratch_reads(self):
        return [
            (s, p, st)
            for s, p, st in self.statements
            if s.startswith(f"SELECT company_id FROM {SCRATCH_SCOPE_PREFIX}")
        ]

    def page_reads(self):
        return [(s, p, st) for s, p, st in self.statements if "AS candidates" in s]

    def suggestion_inserts(self):
        return [(s, p, st) for s, p, st in self.statements if s.startswith(SUGGESTION_INSERT)]

    def kinds(self, prefix):
        return [s for s, _, _ in self.statements if s.startswith(prefix)]


def test_select_columns_are_the_insert_columns_minus_the_publisher_stamps() -> None:
    assert SUGGESTION_SELECT_COLUMNS == (
        "company_id", "source", "source_record_uid", "observed_at", *tables.VALUE_COLUMNS,
    )
    assert tables.SUGGESTION_INSERT_COLUMNS == (
        *SUGGESTION_SELECT_COLUMNS[:4], *tables.VALUE_COLUMNS, "decided_by", "note",
        "suggested_at", "source_run_id", "extractor_version",
    )


def test_changed_scope_unions_the_never_suggested_and_the_newer_than_suggested() -> None:
    sql = changed_scope_sql(current_sql=CURRENT)
    assert sql.count(CURRENT) == 2
    assert "UNION ALL" in sql
    assert "LEFT ANTI JOIN" in sql and "WHERE source = %(source)s" in sql
    assert "argMax(observed_at, suggested_at) AS observed_at" in sql
    assert "WHERE candidate.observed_at > current.observed_at" in sql
    # No keyset tail: the scope is the whole id set, run once into the scratch table that
    # `scope_pages` then keyset-pages. Paging this text itself re-read both tables' whole
    # remaining tail per page.
    assert sql.rstrip().endswith(")")
    assert "%(after_company_id)s" not in sql and "LIMIT" not in sql and "OFFSET" not in sql
    since = since_scope_sql(current_sql=CURRENT)
    assert "candidate.observed_at > parseDateTime64BestEffort(%(since)s, 3, 'UTC')" in since
    assert since.rstrip().endswith(")")
    assert "%(after_company_id)s" not in since and "LIMIT" not in since


def test_insert_and_count_page_sql_wrap_the_source_select() -> None:
    insert = insert_page_sql(select_sql=SELECT)
    assert insert.startswith(
        f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE} ({', '.join(tables.SUGGESTION_INSERT_COLUMNS)})\n"
    )
    assert "CAST(NULL AS Nullable(String)) AS decided_by" in insert
    assert "now64(3, 'UTC') AS suggested_at" in insert
    assert "%(source_run_id)s AS source_run_id" in insert and "%(extractor_version)s AS extractor_version" in insert
    assert f"FROM ({SELECT}) AS candidate" in insert
    assert count_page_sql(select_sql=SELECT) == f"SELECT count() AS candidates FROM ({SELECT}) AS candidate"


def test_preview_scans_pages_counts_and_writes_nothing() -> None:
    client = FakeClient(scope_pages=[["5560000000", "5561111111"], ["5562222222"]], candidates=2)
    counts = run_extractor(
        client, source="scb", extractor_version="scb-v1", current_sql=CURRENT, select_sql=SELECT,
        select_params={}, source_run_id="run-1", config=ExtractConfig(page_size=2),
    )
    assert counts == ExtractCounts(companies=3, pages=2, candidates=4, inserted=0, execute=False, stopped_at_cap=False)
    # The heavy scope query runs exactly once per run, into the scratch table.
    scope_inserts = client.scope_inserts()
    assert len(scope_inserts) == 1
    scope_sql, scope_params, scope_settings = scope_inserts[0]
    assert changed_scope_sql(current_sql=CURRENT) in scope_sql
    assert scope_params == {"source": "scb"} and scope_settings == SCAN_QUERY_SETTINGS
    # The pages are keyset reads of the scratch table; a short page ends the scan, so no
    # third read.
    scratch = client.scratch_reads()
    assert [p["after_company_id"] for _, p, _ in scratch] == ["", "5561111111"]
    assert all(p["page_size"] == 2 and st == SCAN_QUERY_SETTINGS for _, p, st in scratch)
    assert len(client.kinds("CREATE TABLE")) == 1 and len(client.kinds("DROP TABLE")) == 1
    assert client.suggestion_inserts() == []
    page_reads = client.page_reads()
    assert [p["company_ids"] for _, p, _ in page_reads] == [["5560000000", "5561111111"], ["5562222222"]]
    assert all(st == ID_BOUND_QUERY_SETTINGS for _, _, st in page_reads)


def test_the_scan_drops_its_scratch_table_even_when_a_page_read_fails() -> None:
    class Exploding(FakeClient):
        def execute(self, sql, params=None, settings=None):
            if sql.startswith(f"SELECT company_id FROM {SCRATCH_SCOPE_PREFIX}"):
                self.statements.append((sql, params, settings))
                raise RuntimeError("page read failed")
            return super().execute(sql, params, settings)

    client = Exploding(scope_pages=[["5560000000"]])
    with pytest.raises(RuntimeError, match="page read failed"):
        run_extractor(
            client, source="scb", extractor_version="scb-v1", current_sql=CURRENT, select_sql=SELECT,
            select_params={}, source_run_id="r", config=ExtractConfig(),
        )
    dropped = client.kinds("DROP TABLE")
    assert len(dropped) == 1
    created = client.kinds("CREATE TABLE")[0]
    scratch = created.split()[2]
    assert scratch.startswith(SCRATCH_SCOPE_PREFIX)
    assert dropped[0] == f"DROP TABLE IF EXISTS {scratch}"


def test_execute_inserts_each_page_after_counting_it() -> None:
    client = FakeClient(scope_pages=[["5560000000"]], candidates=1)
    counts = run_extractor(
        client, source="scb", extractor_version="scb-v1", current_sql=CURRENT, select_sql=SELECT,
        select_params={}, source_run_id="run-1", config=ExtractConfig(execute=True, page_size=5),
    )
    assert counts.inserted == 1 and counts.execute is True
    inserts = client.suggestion_inserts()
    assert len(inserts) == 1
    sql, params, settings = inserts[0]
    assert sql == insert_page_sql(select_sql=SELECT)
    assert params["company_ids"] == ["5560000000"]
    assert params["source_run_id"] == "run-1" and params["extractor_version"] == "scb-v1"
    assert settings == ID_BOUND_QUERY_SETTINGS
    count_index = next(i for i, (s, _, _) in enumerate(client.statements) if "AS candidates" in s)
    insert_index = next(i for i, (s, _, _) in enumerate(client.statements) if s.startswith(SUGGESTION_INSERT))
    assert count_index < insert_index


def test_explicit_company_ids_skip_the_scan_and_are_normalized() -> None:
    client = FakeClient(scope_pages=[], candidates=2)
    counts = run_extractor(
        client, source="scb", extractor_version="scb-v1", current_sql=CURRENT, select_sql=SELECT,
        select_params={}, source_run_id="r",
        config=ExtractConfig(company_ids=["5561111111", "5560000000", "5560000000"], page_size=10),
    )
    assert counts.companies == 2 and counts.pages == 1
    # No scan at all: no scratch table is created and the scope query never runs.
    assert client.kinds("CREATE TABLE") == [] and client.scope_inserts() == []
    reads = [p for _, p, _ in client.page_reads()]
    assert reads[0]["company_ids"] == ["5560000000", "5561111111"]


def test_since_replaces_the_per_company_comparison() -> None:
    client = FakeClient(scope_pages=[["5560000000"]], candidates=1)
    run_extractor(
        client, source="scb", extractor_version="scb-v1", current_sql=CURRENT, select_sql=SELECT,
        select_params={}, source_run_id="r", config=ExtractConfig(since="2026-09-01T00:00:00Z", page_size=5),
    )
    # The scope SQL now lives inside the scratch INSERT.
    scope_sql, scope_params, _ = client.scope_inserts()[0]
    assert "parseDateTime64BestEffort(%(since)s, 3, 'UTC')" in scope_sql
    assert scope_params["since"] == "2026-09-01T00:00:00Z" and scope_params["source"] == "scb"
    assert "LEFT ANTI JOIN" not in scope_sql


def test_max_companies_caps_the_scan_and_reports_it() -> None:
    client = FakeClient(scope_pages=[["5560000000", "5561111111"], ["5562222222", "5563333333"]], candidates=2)
    counts = run_extractor(
        client, source="scb", extractor_version="scb-v1", current_sql=CURRENT, select_sql=SELECT,
        select_params={}, source_run_id="r", config=ExtractConfig(page_size=2, max_companies=3),
    )
    assert counts.companies == 3 and counts.stopped_at_cap is True
    reads = [p["company_ids"] for _, p, _ in client.page_reads()]
    assert reads == [["5560000000", "5561111111"], ["5562222222"]]
    # Breaking out at the cap still drops the scratch table.
    assert len(client.kinds("DROP TABLE")) == 1


def test_config_defaults_and_invalid_config_is_refused() -> None:
    config = ExtractConfig()
    # The cap is above every register's size (bolagsverket is 2.86 M companies), so a
    # single materialization can converge instead of needing a documented re-run loop.
    assert config.max_companies == 5_000_000
    assert config.page_size == 5_000 and config.execute is False and config.since == ""
    assert ExtractConfig(max_companies=5_000_000).max_companies == 5_000_000
    with pytest.raises(ValueError):
        ExtractConfig(max_companies=5_000_001)
    with pytest.raises(ValueError):
        ExtractConfig(max_companies=0)
    with pytest.raises(ValueError):
        ExtractConfig(company_ids=["nope"])
    with pytest.raises(ValueError):
        ExtractConfig(since="yesterday")
    with pytest.raises(ValueError):
        ExtractConfig(page_size=20_001)
