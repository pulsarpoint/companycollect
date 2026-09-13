"""The batch around the pure financial fold (spec 2026-09-11 section 6, batch layer): the
selection watermarks, paging, history before main, withdrawal, the SQL texts and the rendered
size of the id-bound statements. A fake client answers each SELECT by its SQL-text function
name and records every statement."""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from dagster_v3.defs.se_company.financial import batch, tables
from dagster_v3.defs.se_company.financial.fold import FOLD_VERSION
from dagster_v3.defs.se_company.financial.tables import LIVE_ROW_PREDICATE

T0 = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
T1 = datetime(2026, 9, 2, 8, 0, tzinfo=UTC)
T2 = datetime(2026, 9, 3, 8, 0, tzinfo=UTC)
FOLDED_AT = datetime(2026, 9, 13, 9, 0, tzinfo=UTC)
A, B, C = "5560000001", "5560000002", "5560000003"
P23, P22 = "standalone:2023-12-31", "standalone:2022-12-31"


def suggestion_row(company_id: str, source: str, period_key: str = P23, *, currency: str | None = "SEK",
                   employees: int | None = None, revenue: tuple[str, str] | None = ("100", "10"),
                   equity: tuple[str, str] | None = None) -> tuple:
    """One row in SUGGESTION_SELECT_COLUMNS order, the shape current_suggestions_sql returns."""
    scope, end = period_key.split(":")
    values: dict[str, Any] = {
        "company_id": company_id, "source": source, "period_key": period_key, "scope": scope,
        "period_end": date.fromisoformat(end), "source_record_uid": f"{source}-{period_key}",
        "suggested_at": T0, "period_start": date(int(end[:4]), 1, 1), "fiscal_year": int(end[:4]),
        "period_months": 12, "currency": currency, "employees": employees,
    }
    for column in tables.MONETARY_SUGGESTION_COLUMNS:
        values[column] = None
    for field, pair in (("revenue", revenue), ("equity", equity)):
        if pair is not None:
            values[tables.original_column(field)] = Decimal(pair[0])
            values[tables.usd_column(field)] = Decimal(pair[1])
    return tuple(values[column] for column in batch.SUGGESTION_SELECT_COLUMNS)


def main_row(company_id: str, period_key: str = P23, *, revenue: tuple[str, str, str] = ("100", "10", "ratsit"),
             active: int = 1, inactive_reason: str = "", sources: tuple[str, ...] = ("ratsit",)) -> tuple:
    """One row in MAIN_COMPARE_COLUMNS order, the shape current_main_rows_sql returns."""
    scope, end = period_key.split(":")
    values: dict[str, Any] = {
        "company_id": company_id, "scope": scope, "period_end": date.fromisoformat(end), "period_key": period_key,
        "period_start": date(int(end[:4]), 1, 1), "period_start_source": "ratsit",
        "fiscal_year": int(end[:4]), "fiscal_year_source": "ratsit", "period_months": 12, "period_months_source": "ratsit",
        "currency": "SEK", "currency_source": "ratsit", "employees": None, "employees_source": "",
        "sources": list(sources), "active": active, "inactive_reason": inactive_reason,
    }
    for field in tables.MONETARY_FIELDS:
        values[tables.original_column(field)] = None
        values[tables.usd_column(field)] = None
        values[tables.source_column(field)] = ""
    values["revenue_amount_original"], values["revenue_amount_usd"], values["revenue_source"] = Decimal(revenue[0]), Decimal(revenue[1]), revenue[2]
    return tuple(values[column] for column in batch.MAIN_COMPARE_COLUMNS)


class FakeClient:
    def __init__(self, answers: dict[str, list]) -> None:
        self.answers = answers
        self.calls: list[tuple[str, Any, Any]] = []
        self.inserts: list[tuple[str, list]] = []

    def execute(self, sql, params=None, settings=None):
        self.calls.append((sql, params, settings))
        if sql.startswith("INSERT"):
            self.inserts.append((sql, list(params)))
            return []
        for name, rows in self.answers.items():
            if sql == getattr(batch, name)():
                return rows
        raise AssertionError(f"unexpected SQL: {sql[:90]}")


def answers(**extra) -> dict[str, list]:
    base = {
        "suggestion_watermarks_sql": [], "main_watermarks_sql": [], "rule_watermarks_sql": [],
        "hide_watermarks_sql": [], "current_suggestions_sql": [], "current_main_rows_sql": [],
        "company_rules_sql": [], "hidden_periods_sql": [],
    }
    base.update(extra)
    return base


def run(client, ids, *, changed_only=True, page_size=batch.PAGE_SIZE):
    return batch.fold_companies(client, ids, changed_only=changed_only, source_run_id="run-1", folded_at=FOLDED_AT, page_size=page_size)


def inserted(client, sql_name: str) -> list[dict]:
    columns = tables.MAIN_COLUMNS if sql_name == "main_insert_sql" else tables.HISTORY_COLUMNS
    statement = getattr(batch, sql_name)()
    return [dict(zip(columns, values, strict=True)) for sql, rows in client.inserts if sql == statement for values in rows]


def test_the_sql_texts_pin_final_the_live_predicate_the_draft_exclusion_and_the_hash() -> None:
    assert batch.PAGE_SIZE == 5_000 and batch.BUCKET_COUNT == 64
    assert batch.FOLD_ID_BOUND_QUERY_SETTINGS == {"max_query_size": 1_048_576, "max_execution_time": 1800}
    assert batch.bucket_company_ids_sql() == (
        f"SELECT DISTINCT company_id\nFROM {tables.QUALIFIED_SUGGESTION_TABLE}\n"
        "WHERE modulo(cityHash64(company_id), 64) = %(bucket)s\nORDER BY company_id"
    )
    live = batch.current_suggestions_sql()
    assert live.startswith(f"SELECT {', '.join(batch.SUGGESTION_SELECT_COLUMNS)}\nFROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL\n")
    assert "source != 'reviewer_draft'" in live and f"AND {LIVE_ROW_PREDICATE}\n" in live
    assert live.endswith("ORDER BY company_id, period_key, source")
    marks = batch.suggestion_watermarks_sql()
    assert f"countIf({LIVE_ROW_PREDICATE}) AS live" in marks and " FINAL\n" in marks and "source != 'reviewer_draft'" in marks
    assert batch.main_watermarks_sql() == (
        f"SELECT company_id, max(folded_at) AS folded_at\nFROM {tables.QUALIFIED_MAIN_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\nGROUP BY company_id"
    )
    assert "FINAL" not in batch.rule_watermarks_sql() and "FINAL" not in batch.hide_watermarks_sql()
    assert batch.company_rules_sql().endswith("WHERE company_id IN %(company_ids)s AND removed = 0")
    assert batch.hidden_periods_sql().endswith("WHERE company_id IN %(company_ids)s AND action = 'hide' AND removed = 0")
    assert batch.current_main_rows_sql() == (
        f"SELECT {', '.join(batch.MAIN_COMPARE_COLUMNS)}\nFROM {tables.QUALIFIED_MAIN_TABLE} FINAL\nWHERE company_id IN %(company_ids)s"
    )
    assert batch.main_insert_sql() == f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} ({', '.join(tables.MAIN_COLUMNS)}) VALUES"
    assert batch.history_insert_sql() == f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} ({', '.join(tables.HISTORY_COLUMNS)}) VALUES"
    assert len(batch.SUGGESTION_SELECT_COLUMNS) == 52 and len(batch.MAIN_COMPARE_COLUMNS) == 77
    for sql_name in ("suggestion_watermarks_sql", "main_watermarks_sql", "rule_watermarks_sql", "hide_watermarks_sql",
                     "current_suggestions_sql", "current_main_rows_sql", "company_rules_sql", "hidden_periods_sql"):
        assert getattr(batch, sql_name)().count("%(company_ids)s") == 1, sql_name


def test_a_full_page_of_twelve_digit_ids_renders_well_under_max_query_size() -> None:
    """clickhouse-driver inlines the id list into the statement text; the widest statement of
    the page at PAGE_SIZE must stay under the 1 MiB the settings raise max_query_size to."""
    ids = [f"{199001010000 + index:012d}" for index in range(batch.PAGE_SIZE)]
    rendered_list = "(" + ", ".join(f"'{company_id}'" for company_id in ids) + ")"
    widest = max(
        len(getattr(batch, sql_name)().replace("%(company_ids)s", rendered_list))
        for sql_name in ("suggestion_watermarks_sql", "current_suggestions_sql", "current_main_rows_sql", "company_rules_sql")
    )
    assert 60_000 < widest < batch.FOLD_ID_BOUND_QUERY_SETTINGS["max_query_size"] // 8


def test_selection_by_watermarks() -> None:
    """A: never folded with a live row -> in. B: folded after its newest suggestion -> out.
    C: folded, but a hide decision is newer -> in. D: only tombstones and never folded -> out.
    E: folded, then a precedence decision -> in. F: no suggestion at all -> out."""
    d, e, f = "5560000004", "5560000005", "5560000006"
    client = FakeClient(answers(
        suggestion_watermarks_sql=[(A, T0, 2), (B, T0, 1), (C, T0, 1), (d, T1, 0), (e, T0, 1)],
        main_watermarks_sql=[(B, T1), (C, T1), (e, T1)],
        hide_watermarks_sql=[(C, T2)],
        rule_watermarks_sql=[(e, T2), (B, T0)],
    ))
    assert batch._changed_company_ids(client, [A, B, C, d, e, f]) == [A, C, e]
    assert all(settings == batch.FOLD_ID_BOUND_QUERY_SETTINGS for _, _, settings in client.calls)


def test_a_first_fold_writes_history_then_main_and_rewrites_unchanged_rows() -> None:
    client = FakeClient(answers(
        suggestion_watermarks_sql=[(A, T0, 2), (B, T0, 1)],
        current_suggestions_sql=[
            suggestion_row(A, "bolagsverket", employees=2100, revenue=("59016040", "5877138")),
            suggestion_row(A, "ratsit", revenue=("60300000", "6005001")),
            suggestion_row(A, "ratsit", P22, revenue=("57100000", "5475989")),
            suggestion_row(B, "ratsit", revenue=("1", "1")),
        ],
        current_main_rows_sql=[main_row(B, revenue=("1", "1", "ratsit"))],
    ))
    counts = run(client, [B, A])
    assert (counts.companies, counts.considered, counts.pages) == (2, 2, 1)
    assert (counts.periods, counts.published, counts.created, counts.unchanged, counts.unpublished) == (3, 3, 2, 1, 0)
    statements = [sql.split(" ")[0] + " " + sql.split(" ")[2] for sql, _ in client.inserts]
    assert statements == [f"INSERT {tables.QUALIFIED_HISTORY_TABLE}", f"INSERT {tables.QUALIFIED_MAIN_TABLE}"]
    main = {(row["company_id"], row["period_key"]): row for row in inserted(client, "main_insert_sql")}
    assert set(main) == {(A, P22), (A, P23), (B, P23)}
    a23 = main[(A, P23)]
    assert (a23["revenue_amount_original"], a23["revenue_amount_usd"], a23["revenue_source"]) == (Decimal("60300000"), Decimal("6005001"), "ratsit")
    assert (a23["employees"], a23["employees_source"], a23["sources"]) == (2100, "bolagsverket", ["bolagsverket", "ratsit"])
    assert (a23["folded_at"], a23["fold_version"], a23["source_run_id"], a23["active"]) == (FOLDED_AT, FOLD_VERSION, "run-1", 1)
    assert main[(B, P23)]["folded_at"] == FOLDED_AT     # unchanged, still rewritten
    history = inserted(client, "history_insert_sql")
    assert [(row["company_id"], row["period_key"], row["change_kind"]) for row in history] == [(A, P22, "created"), (A, P23, "created")]
    assert history[1]["changed_fields"] == ["period_start", "fiscal_year", "period_months", "currency", "revenue", "employees"]
    assert history[1]["changed_at"] == FOLDED_AT and history[1]["fold_run_id"] == "run-1"


def test_a_period_the_sources_dropped_is_withdrawn_and_a_hidden_one_lands_inactive() -> None:
    client = FakeClient(answers(
        suggestion_watermarks_sql=[(A, T2, 1)],
        main_watermarks_sql=[(A, T1)],
        current_suggestions_sql=[suggestion_row(A, "ratsit", revenue=("100", "10"))],
        current_main_rows_sql=[main_row(A, P23), main_row(A, P22, revenue=("50", "5", "ratsit"))],
        hidden_periods_sql=[(A, P23)],
    ))
    counts = run(client, [A])
    assert (counts.periods, counts.published, counts.withdrawn, counts.hidden, counts.unchanged) == (2, 0, 1, 1, 0)
    main = {row["period_key"]: row for row in inserted(client, "main_insert_sql")}
    assert (main[P22]["active"], main[P22]["inactive_reason"], main[P22]["revenue_amount_original"]) == (0, "withdrawn", Decimal("50"))
    assert (main[P22]["fold_version"], main[P22]["source_run_id"]) == (FOLD_VERSION, "run-1")
    assert (main[P23]["active"], main[P23]["inactive_reason"]) == (0, "hidden")
    history = {row["period_key"]: row for row in inserted(client, "history_insert_sql")}
    assert (history[P22]["change_kind"], history[P22]["changed_fields"]) == ("withdrawn", [])
    assert (history[P23]["change_kind"], history[P23]["changed_fields"]) == ("hidden", [])


def test_company_rules_reach_the_fold_per_period() -> None:
    """A company-wide rule demotes Ratsit for revenue; a period rule for 2022 restores it."""
    client = FakeClient(answers(
        suggestion_watermarks_sql=[(A, T0, 4)],
        current_suggestions_sql=[
            suggestion_row(A, "ratsit", revenue=("1", "1")), suggestion_row(A, "bolagsverket", revenue=("2", "2")),
            suggestion_row(A, "ratsit", P22, revenue=("3", "3")), suggestion_row(A, "bolagsverket", P22, revenue=("4", "4")),
        ],
        company_rules_sql=[(A, "", "revenue", "ratsit", 100), (A, P22, "revenue", "ratsit", 5000)],
    ))
    run(client, [A])
    main = {row["period_key"]: row for row in inserted(client, "main_insert_sql")}
    assert main[P23]["revenue_source"] == "bolagsverket" and main[P22]["revenue_source"] == "ratsit"
    assert batch.rules_by_company([(A, "", "revenue", "ratsit", 100), (A, P22, "revenue", "ratsit", 5000)]) == {
        A: {"": {"revenue": {"ratsit": 100}}, P22: {"revenue": {"ratsit": 5000}}}
    }


def test_changed_only_false_takes_every_page_and_pages_are_cut_at_page_size() -> None:
    client = FakeClient(answers())
    counts = run(client, [A, B, C], changed_only=False, page_size=2)
    assert (counts.companies, counts.considered, counts.pages, counts.periods) == (3, 3, 2, 0)
    watermark_calls = [sql for sql, _, _ in client.calls if sql == batch.suggestion_watermarks_sql()]
    assert watermark_calls == []
    pages = [params["company_ids"] for sql, params, _ in client.calls if sql == batch.current_suggestions_sql()]
    assert pages == [[A, B], [C]]
    assert client.inserts == []


def test_bad_ids_and_buckets_are_refused_before_any_query() -> None:
    client = FakeClient(answers())
    with pytest.raises(ValueError, match="10 or 12 digits"):
        run(client, ["abc"])
    with pytest.raises(ValueError, match="bucket out of range"):
        batch.fold_bucket(client, 64, changed_only=True, source_run_id="run-1", folded_at=FOLDED_AT)
    assert client.calls == []


def test_fold_bucket_reads_the_bucket_ids_then_folds_them() -> None:
    client = FakeClient(answers(bucket_company_ids_sql=[(A,), (B,)], suggestion_watermarks_sql=[(A, T0, 1)],
                                current_suggestions_sql=[suggestion_row(A, "ratsit")]))
    counts = batch.fold_bucket(client, 7, changed_only=True, source_run_id="run-1", folded_at=FOLDED_AT)
    assert client.calls[0][1] == {"bucket": 7} and (counts.companies, counts.considered, counts.periods) == (2, 1, 1)
