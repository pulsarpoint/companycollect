# tests/test_se_company_person_batch.py
"""The batch around the pure person fold: selection, paging, history before main. A fake
client answers each SELECT by its SQL-text function name and records every statement."""

import json
from datetime import UTC, datetime
from typing import Any

import pytest

from dagster_v3.defs.se_company.person import batch, tables
from dagster_v3.defs.se_company.person.fold import (
    FOLD_VERSION,
    MATCH_THRESHOLD as FOLD_MATCH_THRESHOLD,
    MatchPair,
    PublishedPerson,
)

T0 = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
T1 = datetime(2026, 9, 2, 8, 0, tzinfo=UTC)
T2 = datetime(2026, 9, 3, 8, 0, tzinfo=UTC)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
FOLDED_AT = datetime(2026, 9, 10, 9, 0, tzinfo=UTC)
A, B, F = "5560000001", "5560000002", "5560000003"


def normalized_row(company_id: str, source: str, slot: str, *, first="anna", middles=(),
                   last="svensson", parse_status="ok", birth_year=None, wikidata_id=None,
                   role_code="board_member", role_year=2024, role_from=None, role_to=None,
                   data="{}") -> tuple:
    """One row in NORMALIZED_SELECT_COLUMNS order, the shape current_normalized_sql returns."""
    display_first = " ".join(part.title() for part in (first, *middles))
    values = {
        "company_id": company_id, "source": source, "slot": slot,
        "normalized_id": f"{source}-{slot}".ljust(64, "0"), "parse_status": parse_status,
        "first_tokens": [first], "middle_tokens": list(middles), "last_tokens": [last],
        "display_first": display_first, "display_last": last.title(),
        "display_name": f"{display_first} {last.title()}", "birth_year": birth_year,
        "wikidata_id": wikidata_id, "role_code": role_code, "role_year": role_year,
        "role_from": role_from, "role_to": role_to, "data": data,
    }
    return tuple(values[column] for column in batch.NORMALIZED_SELECT_COLUMNS)


def person(company_id: str, key: str, **overrides) -> PublishedPerson:
    values: dict[str, Any] = dict(
        company_id=company_id, person_key=key, display_name="Anna Svensson",
        first_name="Anna", last_name="Svensson", birth_year=None, wikidata_id=None,
        sources=("bolagsverket",), slots=("s1",), normalized_ids=("bolagsverket-s1".ljust(64, "0"),),
        member_sources=("bolagsverket",), member_slots=("s1",), member_names=("Anna Svensson",),
        member_birth_years=(None,), member_wikidata_ids=("",), member_data=("{}",),
        role_codes=("board_member",), role_years=(2024,), role_sources=(("bolagsverket",),),
        current_roles=("board_member",), first_year=2024, last_year=2024,
        text_source="bolagsverket", data="{}", active=1, inactive_reason="",
        folded_at=T2, fold_version=FOLD_VERSION, source_run_id="run-0",
    )
    values.update(overrides)
    return PublishedPerson(**values)


def main_row(company_id: str, key: str, **overrides) -> tuple:
    """The read shape: MAIN_SELECT_COLUMNS is tables.MAIN_COLUMNS, so a PublishedPerson's own
    tuple is exactly what current_main_rows_sql returns."""
    row = person(company_id, key, **overrides)
    return row.as_tuple(row.folded_at)


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


def empty_scan(**extra) -> dict[str, list]:
    answers = {
        "normalized_watermarks_sql": [], "main_watermarks_sql": [], "rule_watermarks_sql": [],
        "company_precedence_watermarks_sql": [], "match_watermarks_sql": [],
        "global_precedence_watermark_sql": [(EPOCH,)],
        "current_normalized_sql": [], "current_main_rows_sql": [], "active_rules_sql": [],
        "company_precedence_sql": [], "match_pairs_sql": [],
    }
    answers.update(extra)
    return answers


def run(client, ids, *, changed_only=True, page_size=batch.PAGE_SIZE):
    return batch.fold_companies(
        client, ids, changed_only=changed_only, source_run_id="run-1",
        folded_at=FOLDED_AT, page_size=page_size,
    )


def inserted(client, sql_name: str) -> list[dict]:
    columns = tables.MAIN_COLUMNS if sql_name == "main_insert_sql" else tables.HISTORY_COLUMNS
    statement = getattr(batch, sql_name)()
    return [
        dict(zip(columns, values, strict=True))
        for sql, rows in client.inserts if sql == statement for values in rows
    ]


def match_pair_tuple(company_id: str, left: str, right: str, *, confidence=0.93,
                     reason="call name", name_a="Erik Bo Bengtsson", name_b="Bo Bengtsson"):
    """One row in MATCH_PAIR_SELECT_COLUMNS order, the shape match_pairs_sql returns."""
    values = {
        "company_id": company_id, "members_a": [left], "members_b": [right],
        "confidence": confidence, "reason": reason, "name_a": name_a, "name_b": name_b,
        "model": "deepseek-v4-flash", "prompt_version": "se-person-match-v1",
    }
    return tuple(values[column] for column in batch.MATCH_PAIR_SELECT_COLUMNS)


def test_the_match_sql_texts_pin_the_threshold_the_hash_join_and_the_state_watermark() -> None:
    pairs = batch.match_pairs_sql()
    assert f"FROM {tables.QUALIFIED_MATCH_TABLE} AS p FINAL" in pairs
    assert f"FROM {tables.QUALIFIED_MATCH_STATE_TABLE} FINAL" in pairs
    # Only the pairs of the company's CURRENT input: a re-match supersedes by hash, it does
    # not delete the previous input's rows.
    assert "s.company_id = p.company_id AND s.input_hash = p.input_hash" in pairs
    assert f"p.confidence >= {FOLD_MATCH_THRESHOLD}" in pairs
    assert "error = ''" in pairs
    assert pairs.count("%(company_ids)s") == 2
    assert "ORDER BY p.company_id, p.candidate_a, p.candidate_b" in pairs
    watermarks = batch.match_watermarks_sql()
    assert watermarks == (
        "SELECT company_id, max(matched_at) AS matched_at\n"
        f"FROM {tables.QUALIFIED_MATCH_STATE_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )
    assert batch.MATCH_PAIR_SELECT_COLUMNS == (
        "company_id", "members_a", "members_b", "confidence", "reason",
        "name_a", "name_b", "model", "prompt_version",
    )


def test_a_company_matched_after_its_last_fold_is_re_folded() -> None:
    """The fifth watermark (spec section 4): a new match is a new input, exactly like a new
    rule version or a precedence decision."""
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T0, 1)],
        main_watermarks_sql=[(A, T1)],
        match_watermarks_sql=[(A, T2)],
        current_normalized_sql=[normalized_row(A, "bolagsverket", "s1")],
    ))
    assert run(client, [A]).considered == 1
    # An older match stamp does not re-select it.
    quiet = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T0, 1)],
        main_watermarks_sql=[(A, T2)],
        match_watermarks_sql=[(A, T1)],
    ))
    assert run(quiet, [A]).considered == 0


def test_the_page_feeds_its_pairs_into_the_fold() -> None:
    left = "bolagsverket-s1".ljust(64, "0")
    right = "esef-e1".ljust(64, "0")
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T1, 2)],
        current_normalized_sql=[
            normalized_row(A, "bolagsverket", "s1", first="erik", middles=("bo",), last="bengtsson"),
            normalized_row(A, "esef", "e1", first="bo", last="bengtsson"),
        ],
        match_pairs_sql=[match_pair_tuple(A, left, right)],
    ))
    counts = run(client, [A])
    # Without the pair these are two persons; with it they are one.
    assert counts.persons == 1
    row = inserted(client, "main_insert_sql")[0]
    assert json.loads(row["data"])["llm_match"]["model"] == "deepseek-v4-flash"
    assert batch.match_pair_from_row(match_pair_tuple(A, left, right)) == MatchPair(
        members_a=(left,), members_b=(right,), confidence=0.93, reason="call name",
        name_a="Erik Bo Bengtsson", name_b="Bo Bengtsson",
        model="deepseek-v4-flash", prompt_version="se-person-match-v1",
    )


def test_the_page_reads_the_pairs_under_the_id_bound_settings() -> None:
    client = FakeClient(empty_scan(normalized_watermarks_sql=[(A, T1, 1)],
                                   current_normalized_sql=[normalized_row(A, "bolagsverket", "s1")]))
    run(client, [A])
    # FOLD_ID_BOUND_QUERY_SETTINGS is a dict, so it cannot sit in a set -- a list of the
    # settings every match_pairs_sql() call carried says the same thing.
    calls = [settings for sql, _, settings in client.calls if sql == batch.match_pairs_sql()]
    assert calls and all(settings == batch.FOLD_ID_BOUND_QUERY_SETTINGS for settings in calls)


def test_sql_texts_bind_ids_read_final_and_filter_drafts_and_non_ok_rows() -> None:
    current = batch.current_normalized_sql()
    assert "%(company_ids)s" in current and "FINAL" in current
    assert "source != 'reviewer_draft'" in current
    assert "parse_status = 'ok'" in current
    assert "ORDER BY company_id, source, slot" in current
    watermarks = batch.normalized_watermarks_sql()
    assert watermarks.count("FINAL") == 1 and "countIf(parse_status = 'ok')" in watermarks
    assert "source != 'reviewer_draft'" in watermarks
    rules = batch.active_rules_sql()
    assert "FINAL" in rules and "active = 1" in rules
    # The rule WATERMARK must not filter active: a Reset writes a new version of the same
    # rule row with active = 0 and a newer created_at, and a company that cannot see it
    # would keep the rule forever.
    assert "active" not in batch.rule_watermarks_sql()
    assert "field = 'name'" in batch.company_precedence_sql() and "removed = 0" in batch.company_precedence_sql()
    assert "company_id = ''" in batch.global_precedence_watermark_sql()
    assert batch.main_insert_sql().startswith(f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} (")
    assert batch.history_insert_sql().startswith(f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} (")
    assert f"modulo(cityHash64(company_id), {batch.BUCKET_COUNT})" in batch.bucket_company_ids_sql()
    assert batch.MAIN_SELECT_COLUMNS == tables.MAIN_COLUMNS


def test_a_first_fold_writes_history_then_main() -> None:
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T1, 2)],
        current_normalized_sql=[normalized_row(A, "bolagsverket", "s1"), normalized_row(A, "esef", "e1")],
    ))
    counts = run(client, [A])
    assert (counts.companies, counts.considered, counts.pages) == (1, 1, 1)
    assert (counts.persons, counts.created, counts.unchanged) == (1, 1, 0)
    assert [sql.split(" (")[0] for sql, _ in client.inserts] == [
        f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE}",
        f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE}",
    ]
    [row] = inserted(client, "main_insert_sql")
    assert row["sources"] == ["bolagsverket", "esef"] and row["folded_at"] == FOLDED_AT
    assert row["fold_version"] == FOLD_VERSION and row["source_run_id"] == "run-1"
    assert row["role_sources"] == [["bolagsverket", "esef"]]
    [history] = inserted(client, "history_insert_sql")
    assert (history["change_kind"], history["fold_run_id"], history["changed_at"]) == (
        "created", "run-1", FOLDED_AT
    )


def test_an_unchanged_company_rewrites_main_without_history() -> None:
    """The rewrite is what advances max(folded_at) so the selection converges: a company
    selected by a rule or a precedence export that changed nothing must still stamp its
    rows, or it would be selected again on every later run."""
    from dagster_v3.defs.se_company.person.fold import person_key

    stored = person(A, person_key(A, ("anna", "svensson")))
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T1, 1)], main_watermarks_sql=[(A, T0)],
        current_normalized_sql=[normalized_row(A, "bolagsverket", "s1")],
        current_main_rows_sql=[stored.as_tuple(T0)],
    ))
    counts = run(client, [A])
    assert (counts.created, counts.updated, counts.unchanged) == (0, 0, 1)
    assert [sql.split(" (")[0] for sql, _ in client.inserts] == [
        f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE}"
    ]
    [row] = inserted(client, "main_insert_sql")
    assert row["person_key"] == stored.person_key and row["folded_at"] == FOLDED_AT


def test_a_key_the_new_fold_no_longer_produces_is_withdrawn() -> None:
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T2, 1)], main_watermarks_sql=[(A, T0)],
        current_normalized_sql=[normalized_row(A, "bolagsverket", "s1", first="hakan", last="oberg")],
        current_main_rows_sql=[person(A, "k" * 64).as_tuple(T0)],
    ))
    counts = run(client, [A])
    rows = {row["person_key"]: row for row in inserted(client, "main_insert_sql")}
    assert len(rows) == 2 and counts.created == 1 and counts.withdrawn == 1
    assert (rows["k" * 64]["active"], rows["k" * 64]["inactive_reason"]) == (0, "withdrawn")
    assert rows["k" * 64]["sources"] == ["bolagsverket"]        # its blocks are kept
    history = {entry["change_kind"] for entry in inserted(client, "history_insert_sql")}
    assert history == {"created", "withdrawn"}


def test_a_selected_company_with_no_ok_rows_withdraws_its_previous_persons() -> None:
    """A company can be selected with zero current `ok` rows this page -- e.g. every row
    flipped to no_person, driving foldable=0 while the watermark still moved past
    max(folded_at). by_company.get(company_id, []) is then [] while current.get(company_id,
    []) still holds the old main row: fold_company_persons must see the company with no
    rows, not be skipped, so every previous key is withdrawn."""
    stored = person(A, "w" * 64)
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T1, 0)], main_watermarks_sql=[(A, T0)],
        current_main_rows_sql=[stored.as_tuple(T0)],
    ))
    counts = run(client, [A])
    assert counts.considered == 1 and counts.withdrawn == 1
    assert [sql.split(" (")[0] for sql, _ in client.inserts] == [
        f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE}",
        f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE}",
    ]
    [history] = inserted(client, "history_insert_sql")
    assert history["person_key"] == stored.person_key and history["change_kind"] == "withdrawn"
    [row] = inserted(client, "main_insert_sql")
    assert row["person_key"] == stored.person_key
    assert (row["active"], row["inactive_reason"]) == (0, "withdrawn")


def test_changed_only_selection_rules() -> None:
    client = FakeClient(empty_scan(
        # A: folded after its newest input -> skipped. B: a rule is newer -> folded.
        # F: no main row and an ok row -> folded.
        normalized_watermarks_sql=[(A, T0, 1), (B, T0, 1), (F, T1, 1)],
        main_watermarks_sql=[(A, T1), (B, T1)],
        rule_watermarks_sql=[(B, T2)],
        current_normalized_sql=[normalized_row(B, "bolagsverket", "s1"), normalized_row(F, "esef", "e1")],
    ))
    counts = run(client, [A, B, F])
    assert (counts.companies, counts.considered) == (3, 2)
    scoped = [params["company_ids"] for sql, params, _ in client.calls if sql == batch.current_normalized_sql()]
    assert scoped == [[B, F]]


def test_a_newer_precedence_export_selects_every_folded_company() -> None:
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T0, 1)], main_watermarks_sql=[(A, T1)],
        global_precedence_watermark_sql=[(T2,)],
        current_normalized_sql=[normalized_row(A, "bolagsverket", "s1")],
    ))
    assert run(client, [A]).considered == 1


def test_a_company_precedence_row_selects_only_that_company() -> None:
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T0, 1), (B, T0, 1)], main_watermarks_sql=[(A, T1), (B, T1)],
        company_precedence_watermarks_sql=[(B, T2)],
        current_normalized_sql=[normalized_row(B, "bolagsverket", "s1")],
        company_precedence_sql=[(B, "esef", 5000)],
    ))
    assert run(client, [A, B]).considered == 1


def test_a_company_with_only_partial_rows_and_no_main_row_is_never_selected() -> None:
    client = FakeClient(empty_scan(normalized_watermarks_sql=[(A, T1, 0)]))
    counts = run(client, [A])
    assert counts.considered == 0 and client.inserts == []


def test_rules_and_a_company_precedence_row_reach_the_fold() -> None:
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T1, 2)], rule_watermarks_sql=[(A, T1)],
        current_normalized_sql=[
            normalized_row(A, "bolagsverket", "s1"),
            normalized_row(A, "esef", "e1", first="hakan", last="oberg"),
        ],
        active_rules_sql=[(A, "r" * 64, "split", [], ["e1"])],
        company_precedence_sql=[(A, "esef", 5000)],
    ))
    counts = run(client, [A])
    rows = inserted(client, "main_insert_sql")
    assert len(rows) == 2 and counts.persons == 2 and counts.stale_rules == 0
    assert {row["text_source"] for row in rows} == {"bolagsverket", "esef"}


def test_every_id_bound_read_passes_the_query_settings_and_pages() -> None:
    client = FakeClient(empty_scan())
    run(client, [A, B, F], page_size=2)
    bound = [(params, settings) for sql, params, settings in client.calls if "%(company_ids)s" in sql]
    assert bound and all(settings == batch.FOLD_ID_BOUND_QUERY_SETTINGS for _, settings in bound)
    assert [params["company_ids"] for params, _ in bound][:4] == [[A, B]] * 4
    global_calls = [c for c in client.calls if c[0] == batch.global_precedence_watermark_sql()]
    assert len(global_calls) == 1               # one scalar per call, not per page


def test_a_full_page_renders_under_the_query_size_setting() -> None:
    """Rendered the way tests/test_se_company_address_batch.py renders it: the driver's
    escape_params against a SimpleNamespace context, with 12-digit ids (the wider of the two
    widths normalized_se_company_ids admits). No server needed."""
    from types import SimpleNamespace

    from clickhouse_driver.util.escape import escape_params

    DEFAULT_MAX_QUERY_SIZE = 262_144
    context = SimpleNamespace(
        server_info=SimpleNamespace(get_timezone=lambda: "UTC"),
        client_settings={"server_side_params": False},
    )
    ids = [str(556000000000 + index) for index in range(batch.PAGE_SIZE)]
    sizes = []
    for text in (batch.current_normalized_sql(), batch.current_main_rows_sql(),
                 batch.normalized_watermarks_sql(), batch.active_rules_sql(),
                 batch.company_precedence_sql(), batch.match_watermarks_sql(),
                 batch.match_pairs_sql()):
        rendered = text % escape_params({"company_ids": ids}, context)
        sizes.append(len(rendered.encode()))
        assert sizes[-1] < batch.FOLD_ID_BOUND_QUERY_SETTINGS["max_query_size"]
    assert max(sizes) > DEFAULT_MAX_QUERY_SIZE   # the raised setting is not decoration


def test_fold_bucket_reads_the_bucket_ids_then_folds_them() -> None:
    client = FakeClient(empty_scan(bucket_company_ids_sql=[(A,)]))
    counts = batch.fold_bucket(
        client, 7, changed_only=True, source_run_id="run-1", folded_at=FOLDED_AT
    )
    assert counts.companies == 1
    assert client.calls[0][1] == {"bucket": 7}
    assert client.calls[0][2] == batch.FOLD_ID_BOUND_QUERY_SETTINGS
    with pytest.raises(ValueError):
        batch.fold_bucket(client, 64, changed_only=True, source_run_id="run-1", folded_at=FOLDED_AT)


def test_fold_counts_as_metadata_names_every_counter() -> None:
    counts = batch.FoldCounts(
        companies=1, considered=2, pages=3, persons=4, created=5, updated=6, hidden=7,
        withdrawn=8, reactivated=9, unchanged=10, stale_rules=11, sets_split_by_birth_year=12,
    )
    assert set(counts.as_metadata()) == {
        "companies", "considered", "pages", "persons", "created", "updated", "hidden",
        "withdrawn", "reactivated", "unchanged", "stale_rules", "sets_split_by_birth_year",
        "fold_version",
    }
    assert counts.as_metadata()["fold_version"] == FOLD_VERSION


def test_invalid_company_ids_are_refused_before_any_query() -> None:
    client = FakeClient({})
    with pytest.raises(ValueError):
        run(client, ["12"])
    assert client.calls == []
