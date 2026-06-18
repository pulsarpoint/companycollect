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
