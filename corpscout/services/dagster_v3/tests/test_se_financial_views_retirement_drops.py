"""The financial slice-4 drop scripts say exactly what spec section 10's retirement list
says: the two Sweden-only financial source views, and nothing else.

The scripts live in corpscout/clickhouse/operations/ beside the ledger they retire from, and
they are owner-run: nothing in this repo executes them, so this file is the only thing
standing between a typo and a dropped production object. It reads the SQL, parses the object
names out, and compares them as WHOLE names -- se_company_financial is a prefix of the
entity's four sibling tables, none of which may ever appear in a DROP here.
"""

import re
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[3] / "clickhouse" / "operations"
DROPS = SCRIPTS / "se_financial_views_retirement_drops.sql"
PRECHECK = SCRIPTS / "se_financial_views_retirement_precheck.sql"
POSTCHECK = SCRIPTS / "se_financial_views_retirement_postcheck.sql"

DROP_ORDER = (
    ("VIEW", "se_financials_bolagsverket_current"),
    ("VIEW", "se_financials_esef_current"),
)
# Never droppable: the entity's five tables, the source tables the two views read, the filed
# reports table, the cross-country projection and the serving view.
KEPT = (
    "se_company_financial_suggestion",
    "se_company_financial",
    "se_company_financial_history",
    "se_company_financial_precedence",
    "se_company_financial_rule",
    "se_bolagsverket_financial_metrics",
    "esef_financial_metrics",
    "esef_filings",
    "company_identifier",
    "se_financial_reports",
    "se_company_financials_latest",
    "se_companies_serving",
)

_DROP = re.compile(r"^DROP (TABLE|VIEW) IF EXISTS corpscout\.(\w+);$", re.MULTILINE)


def _statements(path: Path) -> list[tuple[str, str]]:
    return [(kind, name) for kind, name in _DROP.findall(path.read_text(encoding="utf-8"))]


def test_the_drop_script_drops_exactly_the_two_views_in_order() -> None:
    assert _statements(DROPS) == list(DROP_ORDER)


def test_the_drop_script_names_no_kept_object_and_only_views() -> None:
    dropped = {name for _, name in _statements(DROPS)}
    assert dropped.isdisjoint(KEPT)
    assert all(kind == "VIEW" for kind, _ in _statements(DROPS))


def test_no_script_uses_sync() -> None:
    for path in (DROPS, PRECHECK, POSTCHECK):
        assert " SYNC" not in path.read_text(encoding="utf-8").upper(), path.name


def test_the_precheck_and_postcheck_cover_both_views() -> None:
    for path in (PRECHECK, POSTCHECK):
        sql = path.read_text(encoding="utf-8")
        for _, name in DROP_ORDER:
            assert f"'{name}'" in sql, f"{path.name} does not cover {name}"


def test_the_precheck_gates_on_zero_readers_the_engine_the_counts_and_the_repointed_serving_view() -> None:
    """Views cannot be UNDROPped (spec section 10), so the precheck is the safety: no view or
    materialized view may still read either name, both must be plain Views with their row
    counts recorded, and the serving view must already read the entity (migration 000404)."""
    sql = PRECHECK.read_text(encoding="utf-8")
    assert "count() = 0 AS no_readers" in sql and "(FROM|JOIN)" in sql
    assert "engine" in sql and "FROM system.tables" in sql
    # Gate 1b: the backoffice reads the views at request time, invisible to system.tables.
    assert "FROM system.query_log" in sql
    assert "hasAny(tables, ['corpscout.se_financials_bolagsverket_current', 'corpscout.se_financials_esef_current'])" in sql
    assert "log_comment != 'se_financial_views_retirement'" in sql
    assert sql.count("SETTINGS log_comment = 'se_financial_views_retirement'") == 2
    for view in ("bolagsverket", "esef"):
        assert f"SELECT count() AS se_financials_{view}_current_rows" in sql
        assert f"FROM corpscout.se_financials_{view}_current" in sql
    assert "corpscout.se_company_financial FINAL" in sql and "AS serving_reads_entity" in sql
    assert "system.view_refreshes" in sql


def test_the_postcheck_asserts_absence_and_the_kept_objects() -> None:
    sql = POSTCHECK.read_text(encoding="utf-8")
    assert "count() = 0 AS all_dropped" in sql
    assert "groupArray(name) AS still_present" in sql
    for name in KEPT:
        if name != "company_identifier" and name != "se_companies_serving":
            assert f"'{name}'" in sql, name
