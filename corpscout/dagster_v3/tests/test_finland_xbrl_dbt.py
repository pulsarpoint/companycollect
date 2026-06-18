from pathlib import Path

import duckdb
import pytest
from dbt.cli.main import dbtRunner

DBT_DIR = (
    Path(__file__).parents[1]
    / "src" / "dagster_v3" / "defs" / "finland_xbrl" / "dbt"
)


def _dbt(args, db_path, monkeypatch):
    monkeypatch.setenv("FINLAND_XBRL_DUCKDB_PATH", str(db_path))
    res = dbtRunner().invoke(args + ["--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)])
    assert res.success, res.exception


def _seed(db_path, sql_statements):
    conn = duckdb.connect(str(db_path))
    for sql in sql_statements:
        conn.execute(sql)
    conn.close()


def test_metric_map_seed_loads(tmp_path, monkeypatch):
    db = tmp_path / "finland_ytj.duckdb"
    _dbt(["seed"], db, monkeypatch)
    conn = duckdb.connect(str(db), read_only=True)
    rows = conn.execute(
        "select count(*), count(distinct metric_code) from finland_prh_xbrl.xbrl_metric_map"
    ).fetchone()
    assert rows == (12, 12)
    sample = conn.execute(
        "select metric_code from finland_prh_xbrl.xbrl_metric_map "
        "where concept_qname='fi_met:md103' and mcy_member_code='fi_MC:x673'"
    ).fetchone()
    assert sample == ("revenue",)


def test_eligible_model(tmp_path, monkeypatch):
    db = tmp_path / "finland_ytj.duckdb"
    _seed(db, [
        "create schema if not exists finland_prh_xbrl",
        "create schema if not exists finland_prhytj",
        """create table finland_prh_xbrl.financial_reports as select * from (values
            ('a','2023-12-31','2024-03-01','2024-01-01','2024-03-01','run-1', 5),
            ('b','2023-12-31','2024-03-01','2024-01-01','2024-03-01','run-1', 6)
          ) as t(business_id,financial_date,registration_date,
                 discovery_registered_date_start,discovery_registered_date_end,
                 source_run_id,source_record_number)""",
        """create table finland_prhytj.all_companies as select * from (values
            ('a','A Oy', true,  'https://a.fi'),
            ('b','B Oy', false, 'https://b.fi')
          ) as t(business_id,primary_name,is_active,website_normalized_url)""",
    ])
    _dbt(["build", "--select", "eligible_financial_reports"], db, monkeypatch)
    conn = duckdb.connect(str(db), read_only=True)
    rows = conn.execute(
        "select business_id, primary_name from finland_prh_xbrl.eligible_financial_reports order by business_id"
    ).fetchall()
    assert rows == [("a", "A Oy")]  # b is inactive -> excluded


def test_financial_metrics_model(tmp_path, monkeypatch):
    db = tmp_path / "finland_ytj.duckdb"
    _seed(db, [
        "create schema if not exists finland_prh_xbrl",
        """create table finland_prh_xbrl.fi_prh_xbrl_statement_documents as select * from (values
            ('k1','a','2023-12-31','2022-10-01','2023-09-30')
          ) as t(statement_key,business_id,financial_date,reported_period_start,reported_period_end)""",
        """create table finland_prh_xbrl.fi_prh_xbrl_facts_raw as select * from (values
            ('k1','fi_met:md103','fi_MC:x673','numeric','125000', false),
            ('k1','fi_met:zzz','fi_MC:zzz','numeric','1', false),
            ('k1','fi_met:md103','fi_MC:x673','numeric','110000', true)
          ) as t(statement_key,concept_qname,mcy_member_code,value_kind,numeric_value,is_comparative)""",
    ])
    _dbt(
        ["build", "--select", "fi_prh_xbrl_financial_metrics", "xbrl_metric_map"],
        db,
        monkeypatch,
    )
    conn = duckdb.connect(str(db), read_only=True)
    row = conn.execute(
        "select revenue, source_fact_count, mapped_fact_count, unmapped_numeric_fact_count, "
        "metric_warnings, period_start, period_end, mapping_version "
        "from finland_prh_xbrl.fi_prh_xbrl_financial_metrics where statement_key='k1'"
    ).fetchone()
    # revenue is the current (non-comparative) 125000; the is_comparative=true prior-period
    # 110000 row is excluded from the pivot but still counted in source_fact_count (all 3 facts).
    assert row == (
        125000.0,
        3,
        1,
        1,
        '["unmapped numeric facts: 1"]',
        "2022-10-01",
        "2023-09-30",
        "finland-prh-xbrl-metrics-v1",
    )
