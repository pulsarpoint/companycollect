"""The financial extractors' SQL on a real ClickHouse (spec 2026-09-11 section 7). Claims a
fake client cannot settle:
1. Each page select produces the fifty-nine supplied columns in a UNION ALL whose live and
   tombstone branches agree on every type, and every inserted row passes the table's CHECKs.
2. suggestion_id equals sha256(company_id, source, period_key, suggested_at) on every row.
3. The state-hash scope selects a company before anything is suggested, nothing after the
   page is written (it converges), selects it again when the rebuilt source drops a period,
   and converges once the tombstone is written -- under join_use_nulls 0 and 1.
4. Bolagsverket: the fuller of two statements wins a period; the comparative source takes the
   newest restating filing and carries revenue and total assets only.
5. ESEF: the newest version wins field by field with the older version filling its gaps; a
   blank currency becomes NULL; a negative employee count becomes NULL; the EUR filer keeps EUR.
6. Ratsit: only the latest report of the running normalizer version (a superseded
   normalizer generation's report and periods, sharing the same result_sha256, never leak
   in); MSEK scaled to full units with amount_scale 1000000; the USD twin copied; an undated
   period keyed on Dec 31 of its fiscal year and flagged; the longer of two periods with one
   end wins; an employment-only period publishes employees alone; a row without a unit, and
   a row with neither a date nor a fiscal year in range, are skipped; a consolidated report
   maps to the consolidated scope.
7. A company outside se_company_basic_info never reaches the suggestion table.
8. A source row with no figure and no employee count is skipped, not written, and the scan
   still converges (C1): a zero-metric Bolagsverket statement, an all-NULL ESEF filing whose
   only non-NULL source field is a negative employee count, and an all-NULL Ratsit period.
"""

import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.esef_filings import tables as esef_tables
from dagster_v3.defs.esef_filings.country_views import build_se_esef_view_sql
from dagster_v3.defs.se_company.basic_info.extract import insert_page_sql
from dagster_v3.defs.se_company.financial import bolagsverket, esef, ratsit
from dagster_v3.defs.se_company.financial.suggestions import FINANCIAL_TARGET
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
from tests.clickhouse_local import clickhouse_local_command, render

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "se_company_financial_source_tables.sql"
TABLE = FINANCIAL_TARGET.qualified_table
A, B, OUT = "5567081699", "5560000002", "5569999999"   # A: every source; B: ESEF (EUR) + Ratsit consolidated; OUT: not in the universe
LEI_A, LEI_B = "AAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBB"


def _statements(text: str) -> list[str]:
    out = []
    for raw in text.split(";"):
        statement = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--")).strip()
        if statement:
            out.append(statement)
    return out


def _schema() -> list[str]:
    schema = ["CREATE DATABASE IF NOT EXISTS corpscout"]
    schema += [s for s in _statements((MIGRATIONS_DIR / "000401_corpscout_se_company_financial_entity.up.sql").read_text(encoding="utf-8")) if s.startswith("CREATE TABLE")]
    schema += [s for s in _statements((MIGRATIONS_DIR / "000377_corpscout_se_company_basic_info.up.sql").read_text(encoding="utf-8")) if s.startswith("CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info\n")]
    schema += _statements(FIXTURE.read_text(encoding="utf-8"))
    schema.append(build_se_esef_view_sql(esef_tables.SE_ESEF_VIEWS[0]).rstrip(";"))
    return schema


ROWS = [
    f"INSERT INTO corpscout.se_company_basic_info (company_id, legal_name, legal_name_source, status, status_source, folded_at, fold_version, source_run_id) VALUES ('{A}', 'A AB', 'scb', 'active', 'scb', now64(3), 'v1', 'r'), ('{B}', 'B AB', 'scb', 'active', 'scb', now64(3), 'v1', 'r')",
    # Bolagsverket: A reported 2023; 2022 twice (s22a fuller than s22b); 2022 restated by the 2023 and 2024 filings (2024 newest); OUT is not in the universe.
    # s21 (C1): a reported 2021 statement with every metric and employees NULL -- must be skipped, not written as a pseudo-tombstone.
    f"""INSERT INTO corpscout.se_bolagsverket_financial_metrics (country_iso2, source_slug, source_run_id, source_record_id, statement_key, source_record_uid, company_id, report_period_start, report_period_end, fiscal_year, observation_kind, source_fiscal_year, currency, revenue_amount_original, revenue_amount_usd, operating_profit_loss_amount_original, operating_profit_loss_amount_usd, profit_loss_amount_original, profit_loss_amount_usd, total_assets_amount_original, total_assets_amount_usd, equity_amount_original, equity_amount_usd, employees, fx_rate_to_usd, fx_rate_date, fx_source, mapping_version, resolved_at) VALUES
    ('SE','sweden_financial','r','s23:1','s23','u23','{A}','2023-01-01','2023-12-31',2023,'reported',2023,'SEK',59016040,5876000,946563,94000,946563,94000,54302472,5400000,3379581,336000,2100,0.0996,'2023-12-29','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','s22a:1','s22a','u22a','{A}','2022-01-01','2022-12-31',2022,'reported',2022,'SEK',54910071,5500000,NULL,NULL,64959,6500,39228252,3900000,2433018,243000,1800,0.1,'2022-12-30','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','s22b:1','s22b','u22b','{A}','2022-01-01','2022-12-31',2022,'reported',2022,'SEK',54910071,5500000,NULL,NULL,NULL,NULL,39228252,3900000,NULL,NULL,NULL,0.1,'2022-12-30','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','s23:2','s23','u23','{A}','2022-01-01','2022-12-31',2022,'comparative',2023,'SEK',54910071,5500000,NULL,NULL,NULL,NULL,39228252,3900000,NULL,NULL,NULL,0.1,'2022-12-30','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','s24:2','s24','u24','{A}','2022-01-01','2022-12-31',2022,'comparative',2024,'SEK',54900000,5499000,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0.1,'2022-12-30','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','sX:1','sX','uX','{OUT}','2023-01-01','2023-12-31',2023,'reported',2023,'SEK',1,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0.1,'2023-12-29','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','s21:1','s21','u21','{A}','2021-01-01','2021-12-31',2021,'reported',2021,'SEK',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0.1,'2021-12-30','ecb','m1',now64(3))""",
    # ESEF: A has two versions of 2023 (v0 revenue + equity in SEK; v1 revenue + employees, blank currency); B files in EUR with a negative employee count.
    # B 2022 (C1): every money column NULL and employees -1 -- entirely NULL only after the
    # negative-employees rule, pinning that the WHERE reads the projection alias (employees),
    # not the raw source column (e.employees = -1, not NULL).
    f"INSERT INTO corpscout.esef_entity_registry_map (lei, country_iso2, registry_id_raw, registry_id, match_source, link_status, source_run_id, resolved_at) VALUES ('{LEI_A}','SE','{A}','{A}','register','register_verified','r',now64(3)), ('{LEI_B}','SE','{B}','{B}','register','register_verified','r',now64(3))",
    f"INSERT INTO corpscout.esef_filings (lei, entity_name, fxo_id, country, period_end, date_added, processed_at, json_url, package_url, report_url, viewer_url, package_sha256, error_count, warning_count, inconsistency_count, has_json_facts, source_url, source_run_id, resolved_at) VALUES ('{LEI_A}','A AB','{LEI_A}-2023-12-31-ESEF-SE-0','SE','2023-12-31','2024-04-01',now64(3),'','','','','',0,0,0,1,'','r',now64(3)), ('{LEI_A}','A AB','{LEI_A}-2023-12-31-ESEF-SE-1','SE','2023-12-31','2024-05-01',now64(3),'','','','','',0,0,0,1,'','r',now64(3)), ('{LEI_B}','B AB','{LEI_B}-2023-12-31-ESEF-SE-0','SE','2023-12-31','2024-04-01',now64(3),'','','','','',0,0,0,1,'','r',now64(3)), ('{LEI_B}','B AB','{LEI_B}-2022-12-31-ESEF-SE-0','SE','2022-12-31','2023-04-01',now64(3),'','','','','',0,0,0,1,'','r',now64(3))",
    f"""INSERT INTO corpscout.esef_financial_metrics (lei, entity_name, fxo_id, country, scope, fiscal_year, period_start, period_end, currency, revenue_amount_original, revenue_amount_usd, equity_amount_original, equity_amount_usd, employees, mapped_fact_count, source_fact_count, mapping_version, fx_rate_to_usd, fx_rate_date, fx_source, viewer_url, source_run_id, resolved_at) VALUES
    ('{LEI_A}','A AB','{LEI_A}-2023-12-31-ESEF-SE-0','SE','consolidated_ifrs',2023,'2023-01-01','2023-12-31','SEK',1296506000,129000000,2030344000,202000000,NULL,5,9,'m',0.0996,'2023-12-29','ecb','','r',now64(3)),
    ('{LEI_A}','A AB','{LEI_A}-2023-12-31-ESEF-SE-1','SE','consolidated_ifrs',2023,'2023-01-01','2023-12-31','',1300000000,129400000,NULL,NULL,4200,5,9,'m',0.0996,'2023-12-29','ecb','','r',now64(3)),
    ('{LEI_B}','B AB','{LEI_B}-2023-12-31-ESEF-SE-0','SE','consolidated_ifrs',2023,'2023-01-01','2023-12-31','EUR',5000000,5400000,NULL,NULL,-1,5,9,'m',1.08,'2023-12-29','ecb','','r',now64(3)),
    ('{LEI_B}','B AB','{LEI_B}-2022-12-31-ESEF-SE-0','SE','consolidated_ifrs',2022,'2022-01-01','2022-12-31','SEK',NULL,NULL,NULL,NULL,-1,5,9,'m',1.08,'2022-12-29','ecb','','r',now64(3))""",
    # Ratsit: A's latest report is 'a'*64 (the older 'e'*64 must not leak); B's consolidated report.
    # C2: a v1 report and a v1 period duplicate A's 2023 period under the SAME result_sha256
    # ('a'*64) but the superseded normalizer_version -- the pinned join must exclude them, so
    # the expected 2023 row stays the v2 values (60300000 / 6005001.802437), never the v1
    # revenue (99 / 9900000).
    # C1: period_index 7 (fiscal_year 2016) has every amount and employee_count NULL -- must
    # be skipped, not written as a pseudo-tombstone.
    # the 'c' v1 report is newer than every v2 report under a different hash; without the CTE's
    # version filter argMax picks it, the version-pinned join finds no v2 period under it, and A
    # vanishes from the Ratsit live rows, which the scope assertion catches -- so this row pins
    # the CTE filter behaviourally, where the 'a'*64 v1 duplicate pins only the join.
    f"INSERT INTO corpscout.se_ratsit_financial_reports (company_id, result_sha256, normalizer_version, financial_report_index, scope, monetary_unit, period_count, normalized_at) VALUES ('{A}', repeat('e',64), 'ratsit-normalizer-v2', 0, 'company', 'MSEK', 1, '2026-01-01 00:00:00'), ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 'company', 'MSEK', 7, '2026-09-01 00:00:00'), ('{A}', repeat('a',64), 'ratsit-normalizer-v1', 0, 'company', 'MSEK', 1, '2026-09-02 00:00:00'), ('{A}', repeat('c',64), 'ratsit-normalizer-v1', 0, 'company', 'MSEK', 1, '2026-09-03 00:00:00'), ('{B}', repeat('b',64), 'ratsit-normalizer-v2', 0, 'consolidated', 'MSEK', 1, '2026-09-01 00:00:00')",
    f"""INSERT INTO corpscout.se_ratsit_financial_periods (company_id, result_sha256, normalizer_version, financial_report_index, period_index, period_kind, scope, monetary_unit, fiscal_year, period_start, period_end, period_months, revenue_amount, revenue_amount_usd, equity_amount, equity_amount_usd, employee_count, fx_rate_to_usd, fx_rate_date, fx_source, normalized_at) VALUES
    ('{A}', repeat('e',64), 'ratsit-normalizer-v2', 0, 0, 'financial_only', 'company', 'MSEK', 2017, '2017-01-01', '2017-12-31', 12, 1, 100000, NULL, NULL, NULL, 0.1, '2017-12-29', 'ecb', '2026-01-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 0, 'financial_and_employment', 'company', 'MSEK', 2023, '2023-01-01', '2023-12-31', 12, 60.3, 6005001.802437, 3.4, 338000, 21, 0.099585436193, '2023-12-29', 'ECB EXR', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v1', 0, 0, 'financial_and_employment', 'company', 'MSEK', 2023, '2023-01-01', '2023-12-31', 12, 99, 9900000, 3.4, 338000, 21, 0.099585436193, '2023-12-29', 'ECB EXR', '2026-09-02 00:00:00'),
    ('{A}', repeat('c',64), 'ratsit-normalizer-v1', 0, 0, 'financial_and_employment', 'company', 'MSEK', 2023, '2023-01-01', '2023-12-31', 12, 77, 7700000, 3.4, 338000, 21, 0.099585436193, '2023-12-29', 'ECB EXR', '2026-09-03 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 1, 'financial_only', 'company', 'MSEK', 2021, NULL, NULL, NULL, 50, 5000000, NULL, NULL, NULL, 0.1, '2021-12-30', 'ECB EXR', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 2, 'financial_only', 'company', 'MSEK', 2020, '2020-07-01', '2020-12-31', 6, 20, 2000000, NULL, NULL, NULL, 0.1, '2020-12-30', 'ECB EXR', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 3, 'financial_only', 'company', 'MSEK', 2020, '2020-01-01', '2020-12-31', 12, 40, 4000000, NULL, NULL, NULL, 0.1, '2020-12-30', 'ECB EXR', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 4, 'employment_only', 'company', 'MSEK', 2019, '2019-01-01', '2019-12-31', 12, NULL, NULL, NULL, NULL, 15, NULL, NULL, '', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 5, 'financial_only', 'company', NULL, 2018, '2018-01-01', '2018-12-31', 12, 9, NULL, NULL, NULL, NULL, NULL, NULL, '', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 6, 'financial_only', 'company', 'MSEK', 1850, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL, '', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'ratsit-normalizer-v2', 0, 7, 'financial_only', 'company', 'MSEK', 2016, '2016-01-01', '2016-12-31', 12, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', '2026-09-01 00:00:00'),
    ('{B}', repeat('b',64), 'ratsit-normalizer-v2', 0, 0, 'financial_only', 'consolidated', 'MSEK', 2023, '2023-01-01', '2023-12-31', 12, 100, 10000000, NULL, NULL, NULL, 0.1, '2023-12-29', 'ECB EXR', '2026-09-01 00:00:00')""",
]

READ_SQL = (
    "SELECT source, company_id, period_key, toString(period_end_derived), ifNull(currency, 'NULL'), toString(amount_scale), "
    "ifNull(toString(revenue_amount_original), 'NULL'), ifNull(toString(revenue_amount_usd), 'NULL'), "
    "ifNull(toString(equity_amount_original), 'NULL'), ifNull(toString(employees), 'NULL'), "
    "ifNull(toString(filing_fiscal_year), 'NULL'), source_record_uid, ifNull(toString(period_months), 'NULL'), ifNull(toString(fx_rate_date), 'NULL') "
    f"FROM {TABLE} FINAL ORDER BY source, company_id, period_key FORMAT TSV"
)
IDENTITY_SQL = (
    f"SELECT count() FROM {TABLE} FINAL WHERE suggestion_id != lower(hex(SHA256(concat(company_id, '\\n', "
    "toString(source), '\\n', period_key, '\\n', toString(suggested_at)))))"
)
TOMBSTONE_SQL = (
    "SELECT source, company_id, period_key, scope, toString(period_end), toString(amount_scale), "
    "toString(revenue_amount_original IS NULL), toString(employees IS NULL), ifNull(currency, 'NULL') "
    f"FROM {TABLE} FINAL WHERE source = 'bolagsverket' AND period_key = 'standalone:2023-12-31' FORMAT TSV"
)
SOURCES = (
    ("bolagsverket", bolagsverket.reported_changed_scope_sql(), bolagsverket.reported_select_sql(), bolagsverket.BOLAGSVERKET_EXTRACTOR_VERSION),
    ("bolagsverket_comparative", bolagsverket.comparative_changed_scope_sql(), bolagsverket.comparative_select_sql(), bolagsverket.BOLAGSVERKET_COMPARATIVE_EXTRACTOR_VERSION),
    ("esef", esef.esef_changed_scope_sql(), esef.esef_select_sql(), esef.ESEF_EXTRACTOR_VERSION),
    ("ratsit", ratsit.ratsit_changed_scope_sql(), ratsit.ratsit_select_sql(), ratsit.RATSIT_EXTRACTOR_VERSION),
)


def _scope(scope_sql: str, source: str) -> str:
    # normalizer_version is unused text for the other three sources; render() only
    # substitutes placeholders that are actually present, so binding it unconditionally here
    # is harmless for them and required for ratsit's report CTE and periods join.
    return render(scope_sql, {"source": source, "normalizer_version": RATSIT_NORMALIZER_VERSION}) + "\nORDER BY company_id"


def _insert(select_sql: str, ids: list[str], version: str) -> str:
    return render(
        insert_page_sql(select_sql=select_sql, target=FINANCIAL_TARGET),
        {"company_ids": ids, "source_run_id": "run-1", "extractor_version": version, "normalizer_version": RATSIT_NORMALIZER_VERSION},
    )


def _run(join_use_nulls: int) -> dict[str, list[str]]:
    script = [f"SET join_use_nulls = {join_use_nulls}"] + _schema() + ROWS
    for name, scope_sql, select_sql, version in SOURCES:
        script += [f"SELECT '## scope-before {name}'", _scope(scope_sql, name), _insert(select_sql, [A, B, OUT], version), f"SELECT '## scope-after {name}'", _scope(scope_sql, name)]
    script += ["SELECT '## rows'", READ_SQL, "SELECT '## identity'", IDENTITY_SQL]
    bolagsverket_scope, bolagsverket_select, bolagsverket_version = SOURCES[0][1], SOURCES[0][2], SOURCES[0][3]
    script += [
        "ALTER TABLE corpscout.se_bolagsverket_financial_metrics DELETE WHERE statement_key = 's23' AND observation_kind = 'reported' SETTINGS mutations_sync = 2",
        "SELECT '## scope-after-delete'", _scope(bolagsverket_scope, "bolagsverket"),
        _insert(bolagsverket_select, [A], bolagsverket_version),
        "SELECT '## scope-after-tombstone'", _scope(bolagsverket_scope, "bolagsverket"),
        "SELECT '## tombstone'", TOMBSTONE_SQL,
    ]
    completed = subprocess.run(clickhouse_local_command(), input=";\n".join(script) + ";\n", capture_output=True, text=True, timeout=900)
    assert completed.returncode == 0, completed.stderr or completed.stdout
    sections: dict[str, list[str]] = {}
    current = ""
    for line in completed.stdout.splitlines():
        if line.startswith("## "):
            current = line[3:]
            sections[current] = []
        elif line.strip():
            sections[current].append(line)
    return sections


@pytest.mark.parametrize("join_use_nulls", [0, 1])
def test_the_four_extractors_write_the_expected_periods_and_converge(join_use_nulls: int) -> None:
    s = _run(join_use_nulls)
    assert s["scope-before bolagsverket"] == [A] and s["scope-after bolagsverket"] == []
    assert s["scope-before bolagsverket_comparative"] == [A] and s["scope-after bolagsverket_comparative"] == []
    assert s["scope-before esef"] == [B, A] and s["scope-after esef"] == []
    assert s["scope-before ratsit"] == [B, A] and s["scope-after ratsit"] == []
    rows = [line.split("\t") for line in s["rows"]]
    assert rows == [
        ["bolagsverket", A, "standalone:2022-12-31", "0", "SEK", "1", "54910071", "5500000", "2433018", "1800", "2022", "s22a", "12", "2022-12-30"],
        ["bolagsverket", A, "standalone:2023-12-31", "0", "SEK", "1", "59016040", "5876000", "3379581", "2100", "2023", "s23", "12", "2023-12-29"],
        ["bolagsverket_comparative", A, "standalone:2022-12-31", "0", "SEK", "1", "54900000", "5499000", "NULL", "NULL", "2024", "s24", "12", "2022-12-30"],
        ["esef", B, "consolidated:2023-12-31", "0", "EUR", "1", "5000000", "5400000", "NULL", "NULL", "NULL", f"{LEI_B}-2023-12-31-ESEF-SE-0", "12", "2023-12-29"],
        ["esef", A, "consolidated:2023-12-31", "0", "SEK", "1", "1300000000", "129400000", "2030344000", "4200", "NULL", f"{LEI_A}-2023-12-31-ESEF-SE-1", "12", "2023-12-29"],
        ["ratsit", B, "consolidated:2023-12-31", "0", "SEK", "1000000", "100000000", "10000000", "NULL", "NULL", "NULL", f"ratsit:{B}:0:0", "12", "2023-12-29"],
        ["ratsit", A, "standalone:2019-12-31", "0", "SEK", "1000000", "NULL", "NULL", "NULL", "15", "NULL", f"ratsit:{A}:0:4", "12", "NULL"],
        ["ratsit", A, "standalone:2020-12-31", "0", "SEK", "1000000", "40000000", "4000000", "NULL", "NULL", "NULL", f"ratsit:{A}:0:3", "12", "2020-12-30"],
        ["ratsit", A, "standalone:2021-12-31", "1", "SEK", "1000000", "50000000", "5000000", "NULL", "NULL", "NULL", f"ratsit:{A}:0:1", "NULL", "2021-12-30"],
        ["ratsit", A, "standalone:2023-12-31", "0", "SEK", "1000000", "60300000", "6005001.802437", "3400000", "21", "NULL", f"ratsit:{A}:0:0", "12", "2023-12-29"],
    ]
    assert s["identity"] == ["0"]
    assert s["scope-after-delete"] == [A] and s["scope-after-tombstone"] == []
    assert [line.split("\t") for line in s["tombstone"]] == [["bolagsverket", A, "standalone:2023-12-31", "standalone", "2023-12-31", "1", "1", "1", "NULL"]]
