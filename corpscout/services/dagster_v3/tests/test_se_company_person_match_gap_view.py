"""Migration 000406's refreshable view, and the pin that keeps its body machine-rendered.

`corpscout.se_company_person_match_gap` (spec 2026-09-13 section 5, LLM-enhance slice 1) is
a REFRESHABLE materialized view over four tables the person entity already has: one row per
company that still carries a deterministic call-name or double-surname gap the stored
matches have not closed. Nothing writes it -- the fold, the normalizer, the rules, the
precedence and the match asset never see it -- so the only thing that can drift is the
SELECT itself. This file couples the two halves exactly as
tests/test_se_company_person_role_view.py does for 000402: the migration's body must be
`build_se_company_person_match_gap_sql()`'s render, and the DDL around it must be the
refreshable form this repo uses (engine INSIDE the view, `EMPTY`, hourly at :30).

It also pins the three constants the SELECT spells out by hand, against the modules that
own them: tables.py cannot import match.py or fold.py (both import tables.py), so the four
machine sources, the `ok` parse status and the 0.8 threshold are literals in the builder and
equalities here.
"""

from pathlib import Path

from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.fold import FOLDABLE_STATUS, MATCH_THRESHOLD
from dagster_v3.defs.se_company.person.match import MACHINE_SOURCES
from dagster_v3.defs.se_company.person.tables import build_se_company_person_match_gap_sql

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000406_corpscout_se_company_person_llm_enhance"
VIEW = "corpscout.se_company_person_match_gap"
MAIN = "corpscout.se_company_person"
NORMALIZED = "corpscout.se_company_person_normalized"
MATCH = "corpscout.se_company_person_match"
MATCH_STATE = "corpscout.se_company_person_match_state"
QUEUE = "corpscout.llm_queue_se_company_person"
RESPONSE = "corpscout.llm_response_se_company_person"


def _sql(suffix: str) -> str:
    return (MIGRATIONS_DIR / f"{MIGRATION}.{suffix}.sql").read_text(encoding="utf-8")


def _statements(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def _body(statement: str) -> str:
    """The statement without the comment lines that precede it."""
    lines = statement.splitlines()
    while lines and (not lines[0].strip() or lines[0].lstrip().startswith("--")):
        lines.pop(0)
    body = "\n".join(lines).strip()
    assert body, f"no statement left after stripping comments: {statement[:80]!r}"
    return body


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _executable(sql: str) -> str:
    return "\n".join(line.split("--")[0] for line in sql.splitlines())


def _view_body(sql: str) -> str:
    """The SELECT the CREATE MATERIALIZED VIEW installs, without its semicolon."""
    [statement] = [s for s in _statements(sql) if "CREATE MATERIALIZED VIEW" in s]
    marker = "\nEMPTY\nAS "
    return statement[statement.index(marker) + len(marker) :]


def test_the_view_body_is_the_builder_render_and_has_not_drifted_from_it() -> None:
    assert _normalized(_view_body(_sql("up"))) == _normalized(
        build_se_company_person_match_gap_sql()
    )


def test_the_pin_is_not_vacuous() -> None:
    body = _view_body(_sql("up"))

    assert body.startswith("WITH\n")
    # Whole-name matching: MAIN prefixes every other table named here, so it is counted
    # with the alias that follows it. The main table is read TWICE -- once for the member
    # spellings, once to map a matched member back to its person.
    assert body.count(f"FROM {MAIN} AS p FINAL") == 2
    assert body.count("ARRAY JOIN p.normalized_ids AS member_id") == 2
    assert f"INNER JOIN {NORMALIZED} AS n FINAL" in body
    assert "ON n.company_id = p.company_id AND n.normalized_id = member_id" in body
    assert f"FROM {MATCH} AS m FINAL" in body
    assert f"    FROM {MATCH_STATE} FINAL" in body
    # The view never reads itself, and nothing about the queue or the response table
    # belongs in it -- it is derived from the person entity alone.
    assert VIEW not in body
    assert QUEUE not in body and RESPONSE not in body
    for column in tables.MATCH_GAP_VIEW_COLUMNS:
        assert f" AS {column}" in body, column


def test_the_two_rules_are_spelled_as_section_5_1_defines_them() -> None:
    """Call name: equal surname token lists, one side's given-token SET a STRICT superset of
    the other's. Double surname: equal given sets, one side's surname two tokens and the
    other's one, and the single token one of the two. Both rules need DISJOINT source sets
    and a non-conflicting birth year, and both normalize the pair with least/greatest so the
    anti-join lines up whichever side the superset sat on."""
    body = _view_body(_sql("up"))

    assert "INNER JOIN members AS b ON a.company_id = b.company_id AND a.surname = b.surname" in body
    assert "AND hasAll(a.given_set, b.given_set)" in body
    assert "AND length(a.given_set) > length(b.given_set)" in body

    assert "INNER JOIN members AS b ON a.company_id = b.company_id AND a.given = b.given" in body
    assert "AND length(a.last_tokens) = 2" in body
    assert "AND length(b.last_tokens) = 1" in body
    assert "AND has(a.last_tokens, b.last_tokens[1])" in body

    assert body.count("AND empty(arrayIntersect(a.sources, b.sources))") == 2
    assert body.count(
        "AND (a.birth_year IS NULL OR b.birth_year IS NULL OR a.birth_year = b.birth_year)"
    ) == 2
    assert body.count("least(a.person_key, b.person_key) AS person_key_a") == 2
    assert body.count("greatest(a.person_key, b.person_key) AS person_key_b") == 2
    assert body.count("WHERE a.person_key != b.person_key") == 2
    # A pair counts only when no stored pair at or above the threshold already joins the two
    # persons: that LEFT ANTI JOIN is what makes this a "still needs work" list.
    assert "LEFT ANTI JOIN matched_pairs AS m" in body
    assert "AND m.person_key_a = g.person_key_a" in body
    assert "AND m.person_key_b = g.person_key_b" in body


def test_the_member_leg_reads_only_ok_rows_of_the_four_machine_sources() -> None:
    """The same gate `match.build_candidates` applies, in SQL: a reviewer row can never
    contribute a member, so a reviewer-only person contributes nothing at all. tables.py
    cannot import match.py or fold.py (both import tables.py), so the literals live in the
    builder and this is the equality that keeps them honest."""
    body = _view_body(_sql("up"))
    sources = ", ".join(f"'{source}'" for source in MACHINE_SOURCES)

    assert f"AND n.source IN ({sources})" in body
    assert f"WHERE p.active = 1 AND n.parse_status = '{FOLDABLE_STATUS}'" in body
    assert MACHINE_SOURCES == ("bolagsverket", "esef", "wikidata", "ratsit")
    assert FOLDABLE_STATUS == "ok"


def test_the_matched_leg_uses_the_folds_threshold_and_an_error_free_state_row() -> None:
    body = _view_body(_sql("up"))

    assert f"WHERE m.confidence >= {MATCH_THRESHOLD}" in body
    assert MATCH_THRESHOLD == 0.8
    assert "WHERE error = ''" in body
    assert "ON s.company_id = m.company_id AND s.input_hash = m.input_hash" in body


def test_the_up_migration_is_two_tables_two_alters_and_one_refreshable_view() -> None:
    statements = _statements(_sql("up"))

    assert len(statements) == 6
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    create = _body(statements[5])
    assert create.startswith(f"CREATE MATERIALIZED VIEW {VIEW}\n")
    assert "\nREFRESH EVERY 1 HOUR OFFSET 30 MINUTE\n" in create
    # The engine lives INSIDE the view (000326/000391/000402's form), so the view IS the
    # table every reader queries and there is no second object to keep in step.
    assert "\nENGINE = MergeTree\n" in create
    assert f"\nORDER BY ({', '.join(tables.MATCH_GAP_VIEW_ORDER_BY)})\n" in create
    # EMPTY: the CREATE returns at once and the first build is an explicit SYSTEM REFRESH
    # VIEW in the runbook -- never a SYSTEM WAIT VIEW, which outlives the migrate client's
    # read_timeout of 300 s and leaves the ledger dirty.
    assert "\nEMPTY\nAS WITH\n" in create
    assert "SYSTEM WAIT VIEW" not in _sql("up")
    # Nothing is destroyed on the way up.
    assert "DROP" not in _executable(_sql("up")).upper()
    assert "TRUNCATE" not in _executable(_sql("up")).upper()


def test_the_pair_alter_is_one_statement_and_carries_no_default_expression() -> None:
    """ClickHouse extends a sorting key only with a column added by the SAME ALTER, and
    refuses a key column that has a DEFAULT EXPRESSION -- proven on 26.5 while this was
    written: `ADD COLUMN request_id String DEFAULT '', MODIFY ORDER BY (...)` fails with
    code 36, BAD_ARGUMENTS. Without the clause the column still stores String's zero value,
    which is the `''` spec section 4.3 asks for. The state table's copy is NOT in a key, so
    it keeps the explicit DEFAULT."""
    statements = _statements(_sql("up"))
    pair_alter = _body(statements[3])
    state_alter = _body(statements[4])

    assert pair_alter.startswith(f"ALTER TABLE {MATCH}\n")
    assert "\n    ADD COLUMN IF NOT EXISTS request_id String,\n" in pair_alter
    assert "DEFAULT" not in pair_alter
    assert pair_alter.endswith(
        "    MODIFY ORDER BY (company_id, candidate_a, candidate_b, request_id)"
    )

    assert state_alter.startswith(f"ALTER TABLE {MATCH_STATE}\n")
    assert state_alter.endswith("    ADD COLUMN IF NOT EXISTS request_id String DEFAULT ''")
    assert "MODIFY ORDER BY" not in state_alter


def test_the_down_migration_removes_everything_it_can() -> None:
    """Forward-only in spirit: the view and the two new tables go, and the state table's
    column goes, but the PAIR table keeps `request_id` -- it is in that table's sorting key
    and ClickHouse cannot shrink a sorting key. Undoing it would mean rebuilding a live
    table, which a down migration must not do."""
    statements = _statements(_sql("down"))

    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"DROP VIEW IF EXISTS {VIEW}"
    assert _body(statements[2]) == f"DROP TABLE IF EXISTS {RESPONSE}"
    assert _body(statements[3]) == f"DROP TABLE IF EXISTS {QUEUE}"
    assert _body(statements[4]) == (
        f"ALTER TABLE {MATCH_STATE}\n    DROP COLUMN IF EXISTS request_id"
    )
    assert len(statements) == 5
    # One object in, one object out: the inline engine means the view owns its MergeTree and
    # DROP VIEW takes the data with it.
    assert f"DROP TABLE IF EXISTS {VIEW}" not in _sql("down")
    assert "CREATE MATERIALIZED VIEW" not in _sql("down")
    # The pair table is named ONCE in the down file, in prose, never in a statement.
    assert f"ALTER TABLE {MATCH}\n" not in _executable(_sql("down"))


def test_the_sort_keys_carry_no_nullable_column() -> None:
    """`allow_nullable_key` is off (dagster_v3/CLAUDE.md). Every key column here is a plain
    String or the view's own `company_id`, so nothing needs unwrapping -- and the view's two
    counters are cast to UInt32 in the SELECT rather than left as the aggregate's UInt64."""
    body = _view_body(_sql("up"))

    assert tables.MATCH_GAP_VIEW_ORDER_BY == ("company_id",)
    assert "toUInt32(countIf(g.is_call_name = 1)) AS call_name_pairs" in body
    assert "toUInt32(countIf(g.is_double_surname = 1)) AS double_surname_pairs" in body
    assert "Nullable" not in body
    executable_up = _executable(_sql("up"))
    # Both new tables key on (request_id, company_id) -- two plain Strings -- and neither
    # declares a Nullable column at all.
    assert executable_up.count("ORDER BY (request_id, company_id)") == 2
    assert "Nullable" not in executable_up


def test_the_select_carries_the_same_refresh_bounding_settings_since_000347() -> None:
    """The heaviest hourly refresh this entity owns (spec section 12): two ARRAY JOINs over
    1.27M active persons, a self-join of ~5.8M member rows and an anti-join. It ends with
    the identical trailing SETTINGS block 000347/000391/000402 carry -- copied verbatim, not
    a hand-typed near-copy."""
    settings_block = (
        "SETTINGS join_algorithm = 'grace_hash,hash',\n"
        "    grace_hash_join_initial_buckets = 16,\n"
        "    max_bytes_before_external_group_by = 8589934592,\n"
        "    max_bytes_before_external_sort = 8589934592,\n"
        "    max_memory_usage = 12884901888"
    )

    assert build_se_company_person_match_gap_sql().endswith(settings_block)
    assert _view_body(_sql("up")).endswith(settings_block)
