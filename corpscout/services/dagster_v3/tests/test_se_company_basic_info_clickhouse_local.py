"""The basic-info tables and the batch layer's SQL on a real ClickHouse (spec section 9).

Claims a fake client cannot settle:
1. The real DDL applies, and FINAL on the suggestion table returns the newest version per
   (company_id, source) -- a re-suggestion replaces, a second source adds.
2. The watermark queries and the main/history INSERTs accept the exact texts and row
   shapes batch.py sends, under join_use_nulls 0 and 1.
"""

import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.batch import (
    bucket_company_ids_sql,
    current_main_rows_sql,
    current_suggestions_sql,
    history_insert_sql,
    main_insert_sql,
    main_watermarks_sql,
    suggestion_watermarks_sql,
)
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATIONS = (
    "000376_corpscout_se_company_basic_info_suggestion.up.sql",
    "000377_corpscout_se_company_basic_info.up.sql",
    "000378_corpscout_se_company_basic_info_history.up.sql",
    "000379_corpscout_se_company_basic_info_precedence.up.sql",
)


def _schema_statements() -> list[str]:
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(("CREATE DATABASE", "CREATE TABLE")):
                statements.append(statement)
    return statements


def _run(statements: list[str], *, join_use_nulls: int) -> list[str]:
    script = f"SET join_use_nulls = {join_use_nulls};\n" + ";\n".join(statements) + ";\n"
    completed = subprocess.run(
        _clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _bind(sql: str, **params: object) -> str:
    """Render batch.py's %(name)s parameters the way clickhouse-driver does."""
    rendered = sql
    for name, value in params.items():
        if isinstance(value, list):
            literal = "(" + ", ".join(f"'{v}'" for v in value) + ")"
        elif isinstance(value, str):
            literal = f"'{value}'"
        else:
            literal = str(value)
        rendered = rendered.replace(f"%({name})s", literal)
    return rendered


def _suggestion(company_id: str, source: str, suggested_at: str, *, legal_name: str | None, status: str | None) -> str:
    def lit(value: str | None) -> str:
        return "NULL" if value is None else f"'{value}'"

    return (
        f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE} "
        "(company_id, source, source_record_uid, observed_at, legal_name, status, suggested_at, source_run_id, extractor_version) VALUES "
        f"('{company_id}', '{source}', '{source}-uid', toDateTime64('{suggested_at}', 3, 'UTC'), "
        f"{lit(legal_name)}, {lit(status)}, toDateTime64('{suggested_at}', 3, 'UTC'), 'run-1', 'x-v1')"
    )


@pytest.mark.parametrize("join_use_nulls", [0, 1], ids=["join_use_nulls_off", "join_use_nulls_on"])
def test_final_returns_the_current_row_per_company_and_source(join_use_nulls: int) -> None:
    script = _schema_statements() + [
        _suggestion("5560000000", "scb", "2026-09-01 00:00:00", legal_name="Old AB", status="active"),
        _suggestion("5560000000", "scb", "2026-09-02 00:00:00", legal_name="New AB", status="active"),
        _suggestion("5560000000", "wikidata", "2026-09-01 12:00:00", legal_name="Wiki AB", status=None),
        _suggestion("5561111111", "bolagsverket", "2026-09-01 00:00:00", legal_name="B AB", status=None),
        _bind(current_suggestions_sql(), company_ids=["5560000000", "5561111111"]),
        _bind(suggestion_watermarks_sql(), company_ids=["5560000000", "5561111111"]) + " ORDER BY company_id",
        _bind(bucket_company_ids_sql(), bucket=0).replace(
            "WHERE modulo(cityHash64(company_id), 64) = 0", "WHERE 1 = 1"
        ),
    ]
    lines = _run(script, join_use_nulls=join_use_nulls)
    # current_suggestions_sql: three rows, the scb row is the 2026-09-02 version.
    assert lines[0].startswith("5560000000\tscb\tscb-uid\t2026-09-02 00:00:00.000\tNew AB")
    assert lines[1].startswith("5560000000\twikidata\twikidata-uid\t2026-09-01 12:00:00.000\tWiki AB")
    assert lines[2].startswith("5561111111\tbolagsverket\tbolagsverket-uid")
    # watermarks: the newest suggested_at per company.
    assert lines[3] == "5560000000\t2026-09-02 00:00:00.000"
    assert lines[4] == "5561111111\t2026-09-01 00:00:00.000"
    # the bucket query (with its filter widened to every company) lists both ids.
    assert lines[5:7] == ["5560000000", "5561111111"]


def test_null_and_empty_are_different_opinions_on_read() -> None:
    """The fold treats NULL as no opinion and '' as a stated empty value; the table must
    return them distinguishably (a NULL status reads back as \\N, an empty one as '')."""
    script = _schema_statements() + [
        _suggestion("5560000000", "reviewer", "2026-09-01 00:00:00", legal_name="X AB", status=None),
        _suggestion("5560000000", "scb", "2026-09-01 00:00:00", legal_name="X AB", status=""),
        f"SELECT source, status FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL ORDER BY source",
    ]
    lines = _run(script, join_use_nulls=0)
    assert lines == ["reviewer\t\\N", "scb\t"]


@pytest.mark.parametrize("join_use_nulls", [0, 1], ids=["join_use_nulls_off", "join_use_nulls_on"])
def test_main_and_history_inserts_accept_the_batch_row_shape(join_use_nulls: int) -> None:
    main_values = (
        "('5560000000', 'X AB', 'scb', NULL, '', 'active', 'scb', toDate32('1990-01-02'), 'scb', "
        "NULL, '', NULL, '', NULL, '', NULL, NULL, '', toDateTime64('2026-09-03 12:00:00', 3, 'UTC'), 'fold-v1', 'run-1')"
    )
    script = _schema_statements() + [
        f"{main_insert_sql()} {main_values}",
        f"{history_insert_sql()} {main_values[:-1]}, ['legal_name', 'status', 'incorporation_date'])",
        _bind(current_main_rows_sql(), company_ids=["5560000000"]),
        _bind(main_watermarks_sql(), company_ids=["5560000000"]),
        f"SELECT company_id, changed_fields FROM {tables.QUALIFIED_HISTORY_TABLE}",
    ]
    lines = _run(script, join_use_nulls=join_use_nulls)
    assert lines[0].startswith("5560000000\tX AB\tscb\t\\N\t\tactive\tscb\t1990-01-02\tscb")
    assert lines[1] == "5560000000\t2026-09-03 12:00:00.000"
    assert lines[2] == "5560000000\t['legal_name','status','incorporation_date']"
