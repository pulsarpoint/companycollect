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
