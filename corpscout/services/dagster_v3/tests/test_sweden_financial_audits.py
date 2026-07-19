import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_financial.audits import (
    QUALIFIED_SE_COMPANY_AUDITS_TABLE,
    SE_COMPANY_AUDITS_COLUMNS,
    SE_COMPANY_AUDITS_TABLE,
    _AUDIT_FIRM_CONCEPTS,
    _AUDIT_FIRM_PREFERRED_CONCEPT,
    _SWEDISH_MONTH_NAMES,
    _audits_quality_sql,
    build_audits_insert_sql,
    replace_se_company_audits_clickhouse,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"

_STAGE_TABLE = "`corpscout`.`_tmp_se_company_audits_test`"


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text(encoding="utf-8")


def _built_sql() -> str:
    return build_audits_insert_sql(_STAGE_TABLE)


def test_audits_table_and_columns_constants() -> None:
    assert SE_COMPANY_AUDITS_TABLE == "se_company_audits"
    assert QUALIFIED_SE_COMPANY_AUDITS_TABLE == "corpscout.se_company_audits"
    assert SE_COMPANY_AUDITS_COLUMNS == (
        "company_id",
        "fiscal_year",
        "statement_key",
        "audit_firm",
        "opinion_kind",
        "opinion_date",
        "resolved_at",
    )


def test_migration_000146_covers_audits_columns_in_order() -> None:
    """Contract test (mirrors officers.py's migration-column contract): every
    column in ``SE_COMPANY_AUDITS_COLUMNS`` must appear, in order, in
    migration 000146's up SQL -- pins the Python columns tuple to the
    ClickHouse schema so the two can never silently drift apart.
    """
    up_sql = _migration_sql("000146_corpscout_se_company_audits.up.sql")
    down_sql = _migration_sql("000146_corpscout_se_company_audits.down.sql")

    assert "CREATE DATABASE IF NOT EXISTS corpscout" in up_sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.se_company_audits" in up_sql

    last_index = -1
    for column_name in SE_COMPANY_AUDITS_COLUMNS:
        marker = f"    {column_name} "
        index = up_sql.index(marker)
        assert index > last_index, (
            f"column {column_name!r} is out of order relative to "
            "SE_COMPANY_AUDITS_COLUMNS in the migration file"
        )
        last_index = index

    assert "opinion_kind LowCardinality(String)" in up_sql
    assert "opinion_date Nullable(Date32)" in up_sql
    assert "resolved_at DateTime64(3, 'UTC')" in up_sql
    assert "ENGINE = MergeTree" in up_sql
    assert "ORDER BY (company_id, fiscal_year, statement_key)" in up_sql
    assert "DROP TABLE IF EXISTS corpscout.se_company_audits" in down_sql


def test_audits_insert_sql_targets_stage_table_with_explicit_columns() -> None:
    sql = _built_sql()

    assert sql.startswith(f"INSERT INTO {_STAGE_TABLE} (")
    columns_block = sql[: sql.index(")\nSELECT")]
    for column_name in SE_COMPANY_AUDITS_COLUMNS:
        assert column_name in columns_block


def test_audits_sql_contains_both_audit_firm_name_spellings() -> None:
    sql = _built_sql()

    assert "'ValtRevisionsbolagNamn'" in sql
    assert "'ValtRevisionsbolagsnamn'" in sql


def test_audits_sql_audit_firm_uses_deterministic_argmaxif_tiebreak() -> None:
    """Locks in Finding 1's fix: 608 real statements (verified live) carry a
    DIFFERENT audit_firm value under the two taxonomy spellings, so a plain
    anyIf has no deterministic winner (undocumented ClickHouse row-scan
    order). audit_firm must use argMaxIf with an explicit tuple comparator
    key -- (concept_local_name = 'ValtRevisionsbolagsnamn', fact_ordinal) --
    that deterministically prefers the newer ValtRevisionsbolagsnamn
    spelling, then the highest fact_ordinal.
    """
    sql = _built_sql()

    assert "anyIf(trim(coalesce(text_value, raw_value))" not in sql
    assert "argMaxIf(trim(coalesce(text_value, raw_value))," in sql
    assert (
        f"(concept_local_name = '{_AUDIT_FIRM_PREFERRED_CONCEPT}', fact_ordinal)" in sql
    )
    assert _AUDIT_FIRM_PREFERRED_CONCEPT == "ValtRevisionsbolagsnamn"

    # Both concepts must still be present as the argMaxIf's IF-condition set.
    argmaxif_index = sql.index("argMaxIf(trim(coalesce(text_value, raw_value))")
    condition_end = sql.index(")),\n        ''\n    ) AS audit_firm", argmaxif_index)
    condition_block = sql[argmaxif_index:condition_end]
    for concept in _AUDIT_FIRM_CONCEPTS:
        assert f"'{concept}'" in condition_block


def test_audits_sql_contains_both_opinion_concepts() -> None:
    sql = _built_sql()

    assert (
        "'RevisorspateckningRevisionsberattelseEnligtStandardutformning'" in sql
    )
    assert (
        "'RevisorspateckningRevisionsberattelseAvvikerStandardutformning'" in sql
    )


def test_audits_sql_avviker_wins_over_enligt_in_opinion_kind() -> None:
    """Locks in the module docstring's "Avviker-wins" rule: the multiIf
    classifying opinion_kind must test the Avviker (modified) concept
    BEFORE the Enligt (standard) concept, so a statement carrying both facts
    (a malformed/edge-case filing) resolves to 'modified', never 'standard'.
    """
    sql = _built_sql()

    modified_index = sql.index(
        "countIf(concept_local_name = "
        "'RevisorspateckningRevisionsberattelseAvvikerStandardutformning') > 0, 'modified'"
    )
    standard_index = sql.index(
        "countIf(concept_local_name = "
        "'RevisorspateckningRevisionsberattelseEnligtStandardutformning') > 0, 'standard'"
    )
    unknown_index = sql.index("'unknown'", standard_index)

    assert modified_index < standard_index < unknown_index


def test_audits_sql_fiscal_year_and_grouping() -> None:
    sql = _built_sql()

    assert (
        "toInt32(coalesce(toYear(report_period_end), 0)) AS fiscal_year" in sql
    )
    assert "GROUP BY company_id, fiscal_year, statement_key" in sql


def test_audits_sql_keeps_rows_with_known_firm_or_opinion() -> None:
    # A group with a known audit firm OR a resolved opinion is kept even if
    # the other field didn't resolve -- only a group with NEITHER is dropped.
    sql = _built_sql()

    assert "HAVING audit_firm != '' OR opinion_kind != 'unknown'" in sql


def test_audits_sql_opinion_date_recovers_text_typed_pateckning_facts() -> None:
    """Locks in Finding 2's fix: ~50k of ~305k resolved rows carry the
    pateckning date only as Swedish prose (date_value NULL, value_kind =
    'text'). opinion_date must coalesce the original maxIf(date_value, ...)
    with a second maxIf over a toDate32OrNull(...)-built date parsed from
    extractGroups(...) applied to the fact's text.
    """
    sql = _built_sql()

    assert "maxIf(date_value,\n" in sql
    assert "extractGroups(lowerUTF8(trim(coalesce(text_value, raw_value)))," in sql
    assert "toDate32OrNull(" in sql
    assert "leftPad(" in sql
    # The two maxIf branches must be combined via coalesce, and both must be
    # restricted to the same two pateckning concepts (not the firm concepts).
    coalesce_index = sql.index("coalesce(\n        maxIf(date_value,")
    opinion_date_index = sql.index("AS opinion_date", coalesce_index)
    opinion_date_block = sql[coalesce_index:opinion_date_index]
    assert (
        opinion_date_block.count(
            "'RevisorspateckningRevisionsberattelseEnligtStandardutformning'"
        )
        == 2
    )
    assert (
        opinion_date_block.count(
            "'RevisorspateckningRevisionsberattelseAvvikerStandardutformning'"
        )
        == 2
    )


def test_audits_sql_contains_all_twelve_swedish_month_names() -> None:
    """Locks in Finding 2's month-name-to-number multiIf mapping: all 12
    Swedish month names must appear in the built SQL, each mapped to its
    two-digit month number.
    """
    sql = _built_sql()

    assert len(_SWEDISH_MONTH_NAMES) == 12
    for month_number, month in enumerate(_SWEDISH_MONTH_NAMES, start=1):
        assert f"'{month}'" in sql
        assert f"'{month_number:02d}'" in sql


def test_audits_sql_text_date_regex_backslashes_are_doubled() -> None:
    """The regex sent to ClickHouse must double every backslash (``\\\\d``,
    ``\\\\s``) -- see the module docstring's "Text-typed opinion date
    recovery" section for why a single backslash is not reliably preserved
    by ClickHouse's string-literal escaping.
    """
    sql = _built_sql()

    assert "'(\\\\d{1,2})\\\\s+(" in sql
    assert ")\\\\s+(\\\\d{4})'" in sql


def test_audits_quality_sql_counts_expected_metrics() -> None:
    sql = _audits_quality_sql(_STAGE_TABLE)

    assert sql.startswith("SELECT")
    assert f"FROM {_STAGE_TABLE}" in sql
    assert "count() AS row_count" in sql
    assert "uniqExact(company_id) AS company_count" in sql
    assert "countIf(opinion_kind = 'modified') AS modified_opinion_count" in sql
    assert "countIf(opinion_kind = 'unknown') AS unknown_opinion_count" in sql
    assert "countIf(fiscal_year = 0) AS null_fiscal_year_count" in sql
    assert (
        "countIf(opinion_kind != 'unknown' AND opinion_date IS NULL) "
        "AS null_opinion_date_count" in sql
    )


def test_audits_sql_survives_percent_formatting_with_driver_params() -> None:
    """Regression test mirroring officers.py's %-format round-trip check:
    ``replace_se_company_audits_clickhouse`` calls ``client.execute(sql,
    {...})`` with a params dict (to carry ``resolved_at``), so
    clickhouse_driver Python-%-formats the whole query text against that
    dict. Unlike officers.py's LIKE/ILIKE patterns, this SQL has no ``%``
    literal to double -- the only ``%`` in the query is the single driver
    placeholder, and formatting must not raise or corrupt the SQL.
    """
    sql = _built_sql()
    params = {
        "resolved_at": datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        "source_run_id": "run-1",
    }

    formatted = sql % params

    assert "%" not in formatted


def test_audits_sql_has_no_bare_percent_placeholder_leaks() -> None:
    # Only resolved_at is a driver placeholder; no other %(name)s markers,
    # and no stray %% doubling (this SQL has no LIKE/ILIKE literals).
    sql = _built_sql()

    placeholders = set(re.findall(r"%\([a-zA-Z_]+\)s", sql))
    assert placeholders == {"%(resolved_at)s"}
    assert "%%" not in sql


def _simulate_audit_firm_argmaxif(rows: list[dict[str, object]]) -> str:
    """Pure-Python mirror of build_audits_insert_sql's audit_firm
    ``argMaxIf(v, (concept_local_name = 'ValtRevisionsbolagsnamn',
    fact_ordinal), concept_local_name IN (...))`` tiebreak (Finding 1) --
    reimplemented here, not imported, because this module's tests are
    string-level with no live ClickHouse to execute the real SQL against.
    Rows are assumed to already share one (company_id, fiscal_year,
    statement_key) GROUP BY key. ClickHouse argMaxIf picks the row (among
    those passing the If condition) with the greatest comparator tuple
    value, which is exactly what ``max(..., key=)`` does here.
    """
    candidates = [
        row for row in rows if row["concept_local_name"] in _AUDIT_FIRM_CONCEPTS
    ]
    if not candidates:
        return ""
    winner = max(
        candidates,
        key=lambda row: (
            row["concept_local_name"] == _AUDIT_FIRM_PREFERRED_CONCEPT,
            row["fact_ordinal"],
        ),
    )
    return winner["value"]


def test_audit_firm_argmaxif_prefers_new_spelling_when_values_differ() -> None:
    """Locks in Finding 1's fix: 608 real statements (verified live) carry a
    DIFFERENT audit_firm value under the two taxonomy spellings -- a bare
    anyIf has no deterministic winner. The (concept_local_name =
    'ValtRevisionsbolagsnamn', fact_ordinal) tuple deterministically prefers
    the newer ValtRevisionsbolagsnamn spelling even when its fact_ordinal is
    LOWER (the spelling guard dominates the tuple comparison), mirroring a
    real differing pair observed live: "Ernst & Young AB" (old spelling) vs
    "Ernst & Young Aktiebolag" (new spelling) for the same statement.
    """
    rows = [
        {
            "concept_local_name": "ValtRevisionsbolagNamn",
            "fact_ordinal": 10,
            "value": "Ernst & Young AB",
        },
        {
            "concept_local_name": "ValtRevisionsbolagsnamn",
            "fact_ordinal": 5,
            "value": "Ernst & Young Aktiebolag",
        },
    ]

    result = _simulate_audit_firm_argmaxif(rows)

    assert result == "Ernst & Young Aktiebolag"
    # Order-independence: ClickHouse's argMaxIf does not guarantee scan
    # order either.
    assert _simulate_audit_firm_argmaxif(list(reversed(rows))) == result


def test_audit_firm_argmaxif_breaks_ties_by_fact_ordinal() -> None:
    """When both facts share the SAME (preferred) spelling, the tuple's
    second element -- fact_ordinal -- breaks the tie: the later fact in
    document order wins.
    """
    rows = [
        {
            "concept_local_name": "ValtRevisionsbolagsnamn",
            "fact_ordinal": 3,
            "value": "Old Firm Name AB",
        },
        {
            "concept_local_name": "ValtRevisionsbolagsnamn",
            "fact_ordinal": 9,
            "value": "New Firm Name AB",
        },
    ]

    result = _simulate_audit_firm_argmaxif(rows)

    assert result == "New Firm Name AB"
    assert _simulate_audit_firm_argmaxif(list(reversed(rows))) == result


_SWEDISH_MONTH_NUMBERS = {
    month: month_number
    for month_number, month in enumerate(_SWEDISH_MONTH_NAMES, start=1)
}


def _simulate_swedish_text_date_parse(text: str) -> date | None:
    """Pure-Python mirror of build_audits_insert_sql's text-typed opinion
    date recovery (Finding 2): ``extractGroups(lowerUTF8(v), '(\\d{1,2})\\s+
    (<swedish months>)\\s+(\\d{4})')`` then a month-name multiIf then
    ``toDate32OrNull(...)`` -- reimplemented here, not imported, because
    this module's tests are string-level with no live ClickHouse to execute
    the real SQL against. Returns ``None`` (mirroring NULL) when the text
    doesn't match or the captured numbers don't form a valid date.
    """
    pattern = r"(\d{1,2})\s+(" + "|".join(_SWEDISH_MONTH_NUMBERS) + r")\s+(\d{4})"
    match = re.fullmatch(pattern, text.strip().lower())
    if match is None:
        return None
    day_str, month_name, year_str = match.groups()
    try:
        return date(int(year_str), _SWEDISH_MONTH_NUMBERS[month_name], int(day_str))
    except ValueError:
        return None


def test_swedish_text_date_parse_simulation_recovers_real_prose_dates() -> None:
    """Locks in Finding 2's fix against real examples observed live
    (2026-07-19), including whitespace/casing variance the SQL's
    ``lowerUTF8``/``trim`` handle: "15 maj 2024" -> 2024-05-15, and garbage
    text (no pateckning date present) rejected as None rather than raising.
    """
    assert _simulate_swedish_text_date_parse("15 maj 2024") == date(2024, 5, 15)
    assert _simulate_swedish_text_date_parse("7 juli 2023") == date(2023, 7, 7)
    assert _simulate_swedish_text_date_parse("  12 November 2025  ") == date(
        2025, 11, 12
    )
    assert _simulate_swedish_text_date_parse("garbage text") is None
    assert _simulate_swedish_text_date_parse("") is None
    assert _simulate_swedish_text_date_parse("32 maj 2024") is None  # invalid day


class _FakeAuditsClickHouseClient:
    def __init__(
        self,
        *,
        quality_row: tuple[object, ...],
        existing_row_count: int = 0,
    ) -> None:
        self.statements: list[str] = []
        self.table_checks: list[tuple[str, ...]] = []
        self.insert_parameters: dict[str, object] = {}
        self.quality_row = quality_row
        self.existing_row_count = existing_row_count

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if params is not None else ()
            self.table_checks.append(requested)
            return [(table,) for table in requested]
        if sql.startswith("CREATE TABLE"):
            return []
        if sql.startswith("INSERT INTO"):
            self.insert_parameters = params or {}
            return []
        if "AS row_count" in sql:
            return [self.quality_row]
        # The shrink guard's pre-EXCHANGE existing-row-count check has no
        # alias, so it must be checked AFTER "AS row_count" above.
        if sql.startswith("SELECT count() FROM"):
            return [(self.existing_row_count,)]
        if sql.startswith("EXCHANGE TABLES") or sql.startswith("DROP TABLE"):
            return []
        raise AssertionError(sql)


def _patch_clickhouse(
    monkeypatch, client: _FakeAuditsClickHouseClient
) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[_FakeAuditsClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


_DEFAULT_QUALITY_ROW = (500, 300, 40, 60, 15, 8)


def test_replace_se_company_audits_is_atomic_and_reports_counts(monkeypatch) -> None:
    client = _FakeAuditsClickHouseClient(quality_row=_DEFAULT_QUALITY_ROW)
    resource = _patch_clickhouse(monkeypatch, client)

    metadata = replace_se_company_audits_clickhouse(
        clickhouse=resource,
        source_run_id="audits-run",
        resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
    )

    assert client.table_checks == [
        ("se_company_audits", "se_financial_facts"),
    ]
    assert any(statement.startswith("CREATE TABLE") for statement in client.statements)
    assert any(statement.startswith("INSERT INTO") for statement in client.statements)
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    assert client.statements[-1].startswith("DROP TABLE")

    assert client.insert_parameters["source_run_id"] == "audits-run"
    assert client.insert_parameters["resolved_at"] == datetime(
        2026, 7, 19, 15, 0, tzinfo=UTC
    )

    assert metadata["row_count"] == 500
    assert metadata["company_count"] == 300
    assert metadata["modified_opinion_count"] == 40
    assert metadata["unknown_opinion_count"] == 60
    assert metadata["null_fiscal_year_count"] == 15
    assert metadata["null_opinion_date_count"] == 8
    assert metadata["table"] == QUALIFIED_SE_COMPANY_AUDITS_TABLE
    assert metadata["source_run_id"] == "audits-run"


def test_replace_se_company_audits_refuses_to_swap_an_empty_stage(
    monkeypatch,
) -> None:
    client = _FakeAuditsClickHouseClient(quality_row=(0, 0, 0, 0, 0, 0))
    resource = _patch_clickhouse(monkeypatch, client)

    with pytest.raises(ValueError, match="produced no rows"):
        replace_se_company_audits_clickhouse(
            clickhouse=resource,
            source_run_id="audits-run",
            resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        )

    assert not any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    # Cleanup still runs (finally block) even on a refused replace.
    assert client.statements[-1].startswith("DROP TABLE")


def test_replace_se_company_audits_refuses_shrink_below_half(monkeypatch) -> None:
    quality_row = (49, *_DEFAULT_QUALITY_ROW[1:])
    client = _FakeAuditsClickHouseClient(
        quality_row=quality_row, existing_row_count=100
    )
    resource = _patch_clickhouse(monkeypatch, client)

    with pytest.raises(ValueError, match="Refusing to replace ClickHouse table"):
        replace_se_company_audits_clickhouse(
            clickhouse=resource,
            source_run_id="audits-run",
            resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        )

    assert not any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    assert client.statements[-1].startswith("DROP TABLE")


def test_replace_se_company_audits_allow_shrink_overrides_guard(monkeypatch) -> None:
    quality_row = (1, *_DEFAULT_QUALITY_ROW[1:])
    client = _FakeAuditsClickHouseClient(
        quality_row=quality_row, existing_row_count=100
    )
    resource = _patch_clickhouse(monkeypatch, client)

    metadata = replace_se_company_audits_clickhouse(
        clickhouse=resource,
        source_run_id="audits-run",
        resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        allow_shrink=True,
    )

    assert metadata["row_count"] == 1
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )


def test_se_company_audits_clickhouse_asset_is_wired_correctly() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()

    node = repo.asset_graph.get(dg.AssetKey("se_company_audits_clickhouse"))
    assert node.group_name == "sweden_financial"
    assert node.pools == set()
    assert node.partitions_def is None
    assert node.parent_keys == {
        dg.AssetKey("sweden_financial_backfill_facts_clickhouse"),
        dg.AssetKey("sweden_financial_current_facts_clickhouse"),
    }

    # The audits table should refresh whenever metrics/history/officers
    # refresh: it lives in the same (currently unscheduled,
    # manually/backfill-triggered) clickhouse job selection.
    clickhouse_job_asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_financial_clickhouse_job"
        ).asset_layer.executable_asset_keys
    }
    assert "se_company_audits_clickhouse" in clickhouse_job_asset_keys
