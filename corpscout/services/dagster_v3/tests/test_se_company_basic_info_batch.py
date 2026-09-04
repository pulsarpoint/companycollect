"""Spec section 5, batch layer: changed-only selection, diff-only writes, history rows."""

from datetime import UTC, datetime

import pytest

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.batch import (
    ID_BOUND_QUERY_SETTINGS,
    PAGE_SIZE,
    FoldCounts,
    bucket_company_ids_sql,
    current_main_rows_sql,
    current_suggestions_sql,
    fold_bucket,
    fold_companies,
    history_insert_sql,
    main_insert_sql,
    main_watermarks_sql,
    suggestion_watermarks_sql,
)
from dagster_v3.defs.se_company.basic_info.fold import FOLD_VERSION

T0 = datetime(2026, 9, 1, tzinfo=UTC)
T1 = datetime(2026, 9, 2, tzinfo=UTC)
FOLDED_AT = datetime(2026, 9, 3, 12, tzinfo=UTC)


def suggestion_row(company_id: str, source: str, observed_at: datetime = T1, **values) -> tuple:
    """A row in the SELECT order of current_suggestions_sql."""
    base = {field: None for field in tables.VALUE_COLUMNS}
    base.update(values)
    return (company_id, source, f"{source}-uid", observed_at, *[base[c] for c in tables.VALUE_COLUMNS])


def main_row(company_id: str, **overrides) -> tuple:
    """A row in the SELECT order of current_main_rows_sql (tables.MAIN_COLUMNS minus
    folded_at, fold_version and source_run_id, which the fold does not compare)."""
    base = {
        "company_id": company_id, "legal_name": "", "legal_name_source": "",
        "legal_form_code": None, "legal_form_code_source": "", "status": "", "status_source": "",
        "incorporation_date": None, "incorporation_date_source": "", "lei": None, "lei_source": "",
        "wikidata_id": None, "wikidata_id_source": "", "description": None, "description_source": "",
        "description_language": None, "description_sv": None, "description_sv_source": "",
    }
    base.update(overrides)
    return tuple(base[c] for c in tables.MAIN_COLUMNS if c not in ("folded_at", "fold_version", "source_run_id"))


class FakeClient:
    """Answers the batch layer's SELECTs from scripted rows and records every INSERT."""

    def __init__(self, *, suggestions, mains=(), suggestion_marks=(), main_marks=(), bucket_ids=()):
        self.suggestions = list(suggestions)
        self.mains = list(mains)
        self.suggestion_marks = list(suggestion_marks)
        self.main_marks = list(main_marks)
        self.bucket_ids = list(bucket_ids)
        self.statements: list[tuple[str, object]] = []
        self.inserts: list[tuple[str, list[tuple]]] = []
        # Parallel to self.statements: the settings kwarg each execute carried, or None.
        self.settings_calls: list[object] = []

    def execute(self, sql: str, params=None, settings=None):
        self.statements.append((sql, params))
        self.settings_calls.append(settings)
        if sql.startswith("INSERT INTO"):
            self.inserts.append((sql, list(params)))
            return []
        ids = set(params["company_ids"]) if params and "company_ids" in params else None
        if "max(suggested_at)" in sql:
            return [r for r in self.suggestion_marks if r[0] in ids]
        if "max(folded_at)" in sql:
            return [r for r in self.main_marks if r[0] in ids]
        if f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL" in sql:
            return [r for r in self.suggestions if r[0] in ids]
        if f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL" in sql:
            return [r for r in self.mains if r[0] in ids]
        if "modulo(cityHash64(company_id), 64)" in sql:
            return [(i,) for i in self.bucket_ids]
        raise AssertionError(sql)


def test_sql_texts_bind_company_ids_and_read_final_rows() -> None:
    assert "%(company_ids)s" in current_suggestions_sql()
    assert f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL" in current_suggestions_sql()
    assert "ORDER BY company_id, source" in current_suggestions_sql()
    assert f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL" in current_main_rows_sql()
    assert "max(suggested_at)" in suggestion_watermarks_sql()
    assert "max(folded_at)" in main_watermarks_sql()
    assert "modulo(cityHash64(company_id), 64) = %(bucket)s" in bucket_company_ids_sql()
    assert "%" not in bucket_company_ids_sql().replace("%(bucket)s", "")
    assert main_insert_sql() == (
        f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} ({', '.join(tables.MAIN_COLUMNS)}) VALUES"
    )
    assert history_insert_sql() == (
        f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} ({', '.join(tables.HISTORY_COLUMNS)}) VALUES"
    )


def test_first_publish_writes_main_and_history_with_every_non_null_field() -> None:
    client = FakeClient(
        suggestions=[
            suggestion_row("5560000000", "scb", legal_name="SCB AB", status="active"),
            suggestion_row("5560000000", "wikidata", wikidata_id="Q1"),
        ],
    )
    counts = fold_companies(
        client, ["5560000000"], changed_only=False, source_run_id="run-1", folded_at=FOLDED_AT
    )
    assert counts == FoldCounts(companies=1, considered=1, folded=1, changed=1, unchanged=0, unpublished=0)
    (history_sql, history_rows), (main_sql, main_rows) = client.inserts
    assert history_sql == history_insert_sql() and main_sql == main_insert_sql()
    assert len(main_rows) == 1 and len(history_rows) == 1
    row = dict(zip(tables.MAIN_COLUMNS, main_rows[0]))
    assert row["legal_name"] == "SCB AB" and row["legal_name_source"] == "scb"
    assert row["wikidata_id"] == "Q1" and row["wikidata_id_source"] == "wikidata"
    assert row["folded_at"] == FOLDED_AT and row["fold_version"] == FOLD_VERSION
    assert row["source_run_id"] == "run-1"
    history = dict(zip(tables.HISTORY_COLUMNS, history_rows[0]))
    assert history["changed_fields"] == ["legal_name", "status", "wikidata_id"]
    assert history["legal_name"] == "SCB AB"


def test_an_unchanged_company_writes_nothing() -> None:
    client = FakeClient(
        suggestions=[suggestion_row("5560000000", "scb", legal_name="SCB AB", status="active")],
        mains=[main_row("5560000000", legal_name="SCB AB", legal_name_source="scb", status="active", status_source="scb")],
    )
    counts = fold_companies(client, ["5560000000"], changed_only=False, source_run_id="r", folded_at=FOLDED_AT)
    assert counts.unchanged == 1 and counts.changed == 0
    assert client.inserts == []


def test_a_changed_source_alone_is_a_change_and_names_the_field() -> None:
    client = FakeClient(
        suggestions=[
            suggestion_row("5560000000", "scb", legal_name="SCB AB"),
            suggestion_row("5560000000", "bolagsverket", legal_name="SCB AB", status="active"),
        ],
        mains=[main_row("5560000000", legal_name="SCB AB", legal_name_source="scb", status="active", status_source="scb")],
    )
    counts = fold_companies(client, ["5560000000"], changed_only=False, source_run_id="r", folded_at=FOLDED_AT)
    assert counts.changed == 1
    history = dict(zip(tables.HISTORY_COLUMNS, client.inserts[0][1][0]))
    assert history["changed_fields"] == ["status"]
    assert history["status_source"] == "bolagsverket"


def test_changed_only_keeps_new_companies_and_those_with_newer_suggestions() -> None:
    client = FakeClient(
        suggestions=[
            suggestion_row("5560000000", "scb", legal_name="A AB"),
            suggestion_row("5561111111", "scb", legal_name="B AB"),
            suggestion_row("5562222222", "scb", legal_name="C AB"),
        ],
        mains=[
            main_row("5561111111", legal_name="B AB", legal_name_source="scb"),
            main_row("5562222222", legal_name="C old", legal_name_source="scb"),
        ],
        suggestion_marks=[("5560000000", T1), ("5561111111", T0), ("5562222222", T1)],
        main_marks=[("5561111111", T1), ("5562222222", T0)],
    )
    counts = fold_companies(
        client, ["5560000000", "5561111111", "5562222222"], changed_only=True, source_run_id="r", folded_at=FOLDED_AT
    )
    # 5561111111 was folded after its newest suggestion: skipped before any fold.
    assert counts == FoldCounts(companies=3, considered=2, folded=2, changed=2, unchanged=0, unpublished=0)
    read = [p["company_ids"] for s, p in client.statements if s == current_suggestions_sql()]
    assert read == [["5560000000", "5562222222"]]
    # Every SELECT that binds company_ids raises max_query_size: at PAGE_SIZE the driver's
    # client-side substitution renders past ClickHouse's 262,144-byte default (Code: 62),
    # measured in test_a_full_page_renders_under_the_query_size_setting below. This run
    # exercises all four of them -- the two watermark reads and the two FINAL reads.
    id_bound = [
        settings
        for (sql, params), settings in zip(client.statements, client.settings_calls)
        if not sql.startswith("INSERT INTO") and "%(company_ids)s" in sql
    ]
    assert len(id_bound) == 4
    assert all(settings == ID_BOUND_QUERY_SETTINGS for settings in id_bound)
    # The INSERTs bind no ids and must not carry it, or this test would pass for the
    # wrong statements.
    assert [
        settings
        for (sql, _params), settings in zip(client.statements, client.settings_calls)
        if sql.startswith("INSERT INTO")
    ] == [None, None]


def test_a_company_without_register_legal_name_is_unpublished_and_untouched() -> None:
    client = FakeClient(suggestions=[suggestion_row("5560000000", "wikidata", legal_name="Wiki AB")])
    counts = fold_companies(client, ["5560000000"], changed_only=False, source_run_id="r", folded_at=FOLDED_AT)
    assert counts.unpublished == 1 and counts.folded == 0
    assert client.inserts == []


def test_pages_bound_each_read_and_write() -> None:
    ids = [f"556{i:07d}" for i in range(5)]
    client = FakeClient(suggestions=[suggestion_row(i, "scb", legal_name=f"{i} AB") for i in ids])
    counts = fold_companies(client, ids, changed_only=False, source_run_id="r", folded_at=FOLDED_AT, page_size=2)
    assert counts.companies == 5 and counts.changed == 5
    read_sizes = [len(p["company_ids"]) for s, p in client.statements if s == current_suggestions_sql()]
    assert read_sizes == [2, 2, 1]
    assert [len(rows) for sql, rows in client.inserts if sql == main_insert_sql()] == [2, 2, 1]


def test_a_full_page_renders_under_the_query_size_setting() -> None:
    """The failure ID_BOUND_QUERY_SETTINGS exists for: a full PAGE_SIZE page of ids does
    NOT fit in ClickHouse's 262,144-byte default max_query_size.

    clickhouse-driver substitutes %(company_ids)s client-side, so the ids land in the query
    TEXT. Rendered exactly as the driver renders it -- escape_params against a
    SimpleNamespace context, the technique tests/test_se_company_address.py already uses to
    prove a driver-rendering claim offline -- with 12-digit ids, the wider of the two widths
    normalized_se_company_ids admits (10-digit organisationsnummer or 12-digit
    personnummer-based sole-trader ids). No server needed.
    """
    from types import SimpleNamespace

    from clickhouse_driver.util.escape import escape_params

    DEFAULT_MAX_QUERY_SIZE = 262_144
    context = SimpleNamespace(
        server_info=SimpleNamespace(get_timezone=lambda: "UTC"),
        client_settings={"server_side_params": False},
    )
    ids = [str(556_000_000_000 + index) for index in range(PAGE_SIZE)]
    assert len(ids) == PAGE_SIZE == 20_000
    assert all(len(company_id) == 12 for company_id in ids)
    rendered = current_suggestions_sql() % escape_params({"company_ids": ids}, context)
    rendered_size = len(rendered.encode("utf-8"))

    # Half one: the default really would reject this render (Code: 62, "Max query size
    # exceeded"), so the raised setting is not decoration.
    assert rendered_size > DEFAULT_MAX_QUERY_SIZE
    # Half two: the raised setting covers the worst case with real margin, not a razor's
    # edge that the next column added to the SELECT erodes back to zero.
    assert rendered_size < ID_BOUND_QUERY_SETTINGS["max_query_size"]
    assert ID_BOUND_QUERY_SETTINGS["max_query_size"] - rendered_size > DEFAULT_MAX_QUERY_SIZE


def test_fold_bucket_reads_the_partition_ids_then_folds_them() -> None:
    client = FakeClient(
        suggestions=[suggestion_row("5560000000", "scb", legal_name="A AB")],
        bucket_ids=["5560000000"],
    )
    counts = fold_bucket(client, 7, changed_only=False, source_run_id="r", folded_at=FOLDED_AT)
    assert counts.companies == 1 and counts.changed == 1
    assert client.statements[0][1] == {"bucket": 7}


def test_fold_bucket_forwards_page_size() -> None:
    ids = [f"556{i:07d}" for i in range(5)]
    client = FakeClient(
        suggestions=[suggestion_row(i, "scb", legal_name=f"{i} AB") for i in ids],
        bucket_ids=ids,
    )
    fold_bucket(client, 7, changed_only=False, source_run_id="r", folded_at=FOLDED_AT, page_size=2)
    read_sizes = [len(p["company_ids"]) for s, p in client.statements if s == current_suggestions_sql()]
    assert read_sizes == [2, 2, 1]


def test_fold_bucket_refuses_an_out_of_range_bucket() -> None:
    client = FakeClient(suggestions=[])
    for bucket in (64, -1):
        with pytest.raises(ValueError, match="bucket"):
            fold_bucket(client, bucket, changed_only=False, source_run_id="r", folded_at=FOLDED_AT)
    assert client.statements == []


def test_fold_counts_as_metadata_names_every_counter() -> None:
    assert FoldCounts(1, 2, 3, 4, 5, 6).as_metadata() == {
        "companies": 1,
        "considered": 2,
        "folded": 3,
        "changed": 4,
        "unchanged": 5,
        "unpublished": 6,
    }


def test_invalid_company_ids_are_refused_before_any_query() -> None:
    client = FakeClient(suggestions=[])
    with pytest.raises(ValueError, match="company id"):
        fold_companies(client, ["not-an-id"], changed_only=False, source_run_id="r", folded_at=FOLDED_AT)
    assert client.statements == []
