"""Spec 3.2 change rule and the shared page loop, against a scripted fake client."""

import pytest

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.batch import ID_BOUND_QUERY_SETTINGS
from dagster_v3.defs.se_company.basic_info.extract import (
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


class FakeClient:
    """Scope pages come from `scope_pages` (one list per call, in order); candidate counts
    from `candidates`; every INSERT is recorded."""

    def __init__(self, *, scope_pages, candidates=0):
        self.scope_pages = list(scope_pages)
        self.candidates = candidates
        self.statements: list[tuple[str, object, object]] = []

    def execute(self, sql, params=None, settings=None):
        self.statements.append((sql, params, settings))
        if sql.startswith("INSERT INTO"):
            return []
        if "AS candidates" in sql:
            return [(self.candidates,)]
        if "LIMIT %(page_size)s" in sql or "ORDER BY company_id" in sql:
            return [(i,) for i in (self.scope_pages.pop(0) if self.scope_pages else [])]
        raise AssertionError(sql)


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
    assert "LEFT ANTI JOIN" in sql and "WHERE source = %(source)s" in sql
    assert "argMax(observed_at, suggested_at) AS observed_at" in sql
    assert "WHERE candidate.observed_at > current.observed_at" in sql
    assert sql.rstrip().endswith("WHERE company_id > %(after_company_id)s\nORDER BY company_id\nLIMIT %(page_size)s")
    assert "OFFSET" not in sql
    since = since_scope_sql(current_sql=CURRENT)
    assert "candidate.observed_at > parseDateTime64BestEffort(%(since)s, 3, 'UTC')" in since
    assert since.rstrip().endswith("LIMIT %(page_size)s")


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
    client = FakeClient(scope_pages=[["5560000000", "5561111111"], ["5562222222"], []], candidates=2)
    counts = run_extractor(
        client, source="scb", extractor_version="scb-v1", current_sql=CURRENT, select_sql=SELECT,
        select_params={}, source_run_id="run-1", config=ExtractConfig(page_size=2),
    )
    assert counts == ExtractCounts(companies=3, pages=2, candidates=4, inserted=0, execute=False, stopped_at_cap=False)
    scans = [(s, p) for s, p, _ in client.statements if "LIMIT %(page_size)s" in s]
    # A page shorter than page_size ends the scan, so no third query.
    assert [p["after_company_id"] for _, p in scans] == ["", "5561111111"]
    assert all(p["source"] == "scb" and p["page_size"] == 2 for _, p in scans)
    assert not any(s.startswith("INSERT INTO") for s, _, _ in client.statements)
    page_reads = [(p, st) for s, p, st in client.statements if "AS candidates" in s]
    assert [p["company_ids"] for p, _ in page_reads] == [["5560000000", "5561111111"], ["5562222222"]]
    assert all(st == ID_BOUND_QUERY_SETTINGS for _, st in page_reads)


def test_execute_inserts_each_page_after_counting_it() -> None:
    client = FakeClient(scope_pages=[["5560000000"], []], candidates=1)
    counts = run_extractor(
        client, source="scb", extractor_version="scb-v1", current_sql=CURRENT, select_sql=SELECT,
        select_params={}, source_run_id="run-1", config=ExtractConfig(execute=True, page_size=5),
    )
    assert counts.inserted == 1 and counts.execute is True
    inserts = [(s, p, st) for s, p, st in client.statements if s.startswith("INSERT INTO")]
    assert len(inserts) == 1
    sql, params, settings = inserts[0]
    assert sql == insert_page_sql(select_sql=SELECT)
    assert params["company_ids"] == ["5560000000"]
    assert params["source_run_id"] == "run-1" and params["extractor_version"] == "scb-v1"
    assert settings == ID_BOUND_QUERY_SETTINGS


def test_explicit_company_ids_skip_the_scan_and_are_normalized() -> None:
    client = FakeClient(scope_pages=[], candidates=2)
    counts = run_extractor(
        client, source="scb", extractor_version="scb-v1", current_sql=CURRENT, select_sql=SELECT,
        select_params={}, source_run_id="r",
        config=ExtractConfig(company_ids=["5561111111", "5560000000", "5560000000"], page_size=10),
    )
    assert counts.companies == 2 and counts.pages == 1
    assert not any("LIMIT %(page_size)s" in s for s, _, _ in client.statements)
    reads = [p for s, p, _ in client.statements if "AS candidates" in s]
    assert reads[0]["company_ids"] == ["5560000000", "5561111111"]


def test_since_replaces_the_per_company_comparison() -> None:
    client = FakeClient(scope_pages=[["5560000000"], []], candidates=1)
    run_extractor(
        client, source="scb", extractor_version="scb-v1", current_sql=CURRENT, select_sql=SELECT,
        select_params={}, source_run_id="r", config=ExtractConfig(since="2026-09-01T00:00:00Z", page_size=5),
    )
    scan_sql, scan_params, _ = next(x for x in client.statements if "LIMIT %(page_size)s" in x[0])
    assert "parseDateTime64BestEffort(%(since)s, 3, 'UTC')" in scan_sql
    assert scan_params["since"] == "2026-09-01T00:00:00Z"
    assert "LEFT ANTI JOIN" not in scan_sql


def test_max_companies_caps_the_scan_and_reports_it() -> None:
    client = FakeClient(scope_pages=[["5560000000", "5561111111"], ["5562222222", "5563333333"]], candidates=2)
    counts = run_extractor(
        client, source="scb", extractor_version="scb-v1", current_sql=CURRENT, select_sql=SELECT,
        select_params={}, source_run_id="r", config=ExtractConfig(page_size=2, max_companies=3),
    )
    assert counts.companies == 3 and counts.stopped_at_cap is True
    reads = [p["company_ids"] for s, p, _ in client.statements if "AS candidates" in s]
    assert reads == [["5560000000", "5561111111"], ["5562222222"]]


def test_invalid_config_is_refused() -> None:
    with pytest.raises(ValueError):
        ExtractConfig(company_ids=["nope"])
    with pytest.raises(ValueError):
        ExtractConfig(since="yesterday")
    with pytest.raises(ValueError):
        ExtractConfig(page_size=20_001)
