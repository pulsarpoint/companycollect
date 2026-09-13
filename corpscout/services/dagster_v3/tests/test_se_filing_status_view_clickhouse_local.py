"""The filing-status view's data_available leg against a real ClickHouse (clickhouse-local).

Migration 000404 (final review F1) makes the leg's provenance follow the newest active
standalone period's winning sources: source_slug/source_url/source_file_format keep 000282's
Bolagsverket bulk-file values only when those sources include bolagsverket or
bolagsverket_comparative, otherwise the row is attributed to the entity itself
(se_company_financial) with no URL and no file format. A text assertion on the migration file
cannot see whether the newest ACTIVE period per company is the one that decides this, or
whether a hidden newer period can still win -- this runs the view's own SELECT on the engine
over migration 000401's entity DDL, 000282's observations table and 000404's own
CREATE OR REPLACE VIEW statement.
"""

import subprocess
from pathlib import Path

import pytest

from tests.clickhouse_local import clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
ENTITY_MIGRATION = "000401_corpscout_se_company_financial_entity.up.sql"
OBSERVATIONS_MIGRATION = "000282_corpscout_se_annual_report_filing_status.up.sql"
VIEW_MIGRATION = "000404_corpscout_se_financial_readers_entity.up.sql"
VIEW = "corpscout.se_annual_report_filing_status_current"
ENTITY = "corpscout.se_company_financial"
FOLDED_AT = "2026-09-12 10:00:00.000"


def _statements(migration: str) -> list[str]:
    text = (MIGRATIONS_DIR / migration).read_text(encoding="utf-8")
    return [
        "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--")).strip()
        for raw in text.split(";")
    ]


def _schema_statements() -> list[str]:
    statements: list[str] = []
    for statement in _statements(ENTITY_MIGRATION):
        if statement.upper().startswith("CREATE DATABASE") or statement.startswith("CREATE TABLE"):
            statements.append(statement)
    for statement in _statements(OBSERVATIONS_MIGRATION):
        if statement.startswith("CREATE TABLE"):
            statements.append(statement)
    [view_statement] = [s for s in _statements(VIEW_MIGRATION) if s.startswith("CREATE OR REPLACE VIEW")]
    statements.append(view_statement)
    return statements


def _row(company_id: str, scope: str, period_end: str, sources: list[str], active: int) -> str:
    fiscal_year = int(period_end[:4])
    sources_literal = "[" + ", ".join(f"'{source}'" for source in sources) + "]"
    return (
        f"('{company_id}', '{scope}', toDate32('{period_end}'), '{scope}:{period_end}', {fiscal_year}, 'SEK', "
        f"{sources_literal}, {active}, toDateTime64('{FOLDED_AT}', 3, 'UTC'))"
    )


ROWS = (
    # Newest active standalone period (2023) won partly by Bolagsverket: keeps 000282's
    # bulk-file provenance.
    _row("5560000001", "standalone", "2023-12-31", ["bolagsverket", "ratsit"], 1),
    _row("5560000001", "standalone", "2022-12-31", ["ratsit"], 1),
    # Ratsit-only company: its only period is not Bolagsverket, so provenance is the entity.
    _row("5560000002", "standalone", "2021-06-30", ["ratsit"], 1),
    # Hidden only (active = 0): absent from the view entirely.
    _row("5560000003", "standalone", "2024-12-31", ["bolagsverket"], 0),
    # Consolidated only: absent from the standalone leg.
    _row("5560000004", "consolidated", "2024-12-31", ["esef"], 1),
    # Newest ACTIVE period is Ratsit-only; a hidden newer Bolagsverket period must not win.
    _row("5560000005", "standalone", "2019-12-31", ["ratsit"], 1),
    _row("5560000005", "standalone", "2020-12-31", ["bolagsverket"], 0),
)

QUERY = (
    "SELECT company_id, filing_status, toString(report_period_end), source_slug, "
    "toString(source_file_format), source_url, source_record_id "
    f"FROM {VIEW} ORDER BY company_id FORMAT TSV"
)


def _read(query: str) -> list[list[str]]:
    insert = (
        f"INSERT INTO {ENTITY} (company_id, scope, period_end, period_key, fiscal_year, currency, sources, active, folded_at) VALUES "
        + ", ".join(ROWS)
    )
    script = ";\n".join([*_schema_statements(), insert, query]) + ";\n"
    try:
        completed = subprocess.run(clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900)
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line.split("\t") for line in completed.stdout.splitlines() if line.strip()]


def test_the_leg_keeps_bolagsverket_provenance_only_when_the_newest_active_period_won_by_it() -> None:
    rows = _read(QUERY)

    assert rows == [
        [
            "5560000001",
            "data_available",
            "2023-12-31",
            "sweden_financial",
            "application/xhtml+xml",
            "https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler",
            "financials-latest:5560000001",
        ],
        [
            "5560000002",
            "data_available",
            "2021-06-30",
            "se_company_financial",
            "\\N",
            "",
            "financials-latest:5560000002",
        ],
        [
            "5560000005",
            "data_available",
            "2019-12-31",
            "se_company_financial",
            "\\N",
            "",
            "financials-latest:5560000005",
        ],
    ]


def test_a_hidden_only_period_and_a_consolidated_only_period_are_absent() -> None:
    companies = {row[0] for row in _read(QUERY)}

    assert "5560000003" not in companies  # hidden only: active = 0
    assert "5560000004" not in companies  # consolidated only: absent from the standalone leg
