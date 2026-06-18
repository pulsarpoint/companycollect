import json
from pathlib import Path

import duckdb
import pytest
from dbt.cli.main import dbtRunner

from dagster_v3.defs.finland_resolved import dbt_plugin

DBT_DIR = (
    Path(__file__).parents[1]
    / "src" / "dagster_v3" / "defs" / "finland_resolved" / "dbt"
)


def test_plugin_registers_primary_industry_udf() -> None:
    conn = duckdb.connect(":memory:")
    dbt_plugin.Plugin(name="industry", plugin_config={}).configure_connection(conn)
    raw = json.dumps({"mainBusinessLine": {"code": "62010", "codeSet": "NACE_REV_2",
                                            "descriptions": [{"languageCode": "1", "description": "Ohjelmistot"}]}})
    out = conn.execute("select fi_primary_industry_json(?)", [raw]).fetchone()[0]
    parsed = json.loads(out)
    assert parsed["code"] == "62010"
    assert parsed["codeSet"] == "NACE_REV_2"
    assert parsed["description"] == "Ohjelmistot"
    assert parsed["language"] == "fi"
    assert conn.execute("select fi_primary_industry_json(null)").fetchone()[0] is None


def _seed_all_companies(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    conn.execute("create schema if not exists finland_prhytj")
    conn.execute(
        """
        create table finland_prhytj.all_companies as select * from (values
          ('fi-1','FI','Active One Oy','2024-01-01','', 'active', true,
           'https://example.fi/path','https://example.fi/path','example.fi','/path','2024-01-02','',
           'finland_prhytj','run-1','fi-1','hash1',
           '{"mainBusinessLine":{"code":"62010","codeSet":"NACE_REV_2","descriptions":[{"languageCode":"1","description":"Ohjelmistot"}]}}'),
          ('fi-2','FI','Ceased Two Oy','2020-01-01','2025-01-01','ceased', false,
           '','','','','','',
           'finland_prhytj','run-1','fi-2','hash2','{}')
        ) as t(business_id,country_iso2,primary_name,registration_date,end_date,lifecycle_status,is_active,
                website_url,website_normalized_url,website_host,website_path,website_registered_on,website_ended_on,
                source_slug,source_run_id,source_record_id,source_payload_hash,raw_company)
        """
    )
    conn.close()


def _dbt_build(db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINLAND_YTJ_DUCKDB_PATH", str(db_path))
    res = dbtRunner().invoke(
        ["build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    )
    assert res.success, res.exception


def test_fi_companies_model(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "finland_ytj.duckdb"
    _seed_all_companies(db)
    _dbt_build(db, monkeypatch)
    conn = duckdb.connect(str(db), read_only=True)
    rows = conn.execute(
        "select business_id, name, name_normalized, is_active, primary_website_host "
        "from finland_resolved.fi_companies order by business_id"
    ).fetchall()
    assert rows == [
        ("fi-1", "Active One Oy", "active one oy", True, "example.fi"),
        ("fi-2", "Ceased Two Oy", "ceased two oy", False, None),
    ]


def test_fi_websites_model(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "finland_ytj.duckdb"
    _seed_all_companies(db)
    _dbt_build(db, monkeypatch)
    conn = duckdb.connect(str(db), read_only=True)
    rows = conn.execute(
        "select business_id, website_normalized_url, website_host, is_current, is_primary "
        "from finland_resolved.fi_websites order by business_id"
    ).fetchall()
    # Only fi-1 has a website; fi-2 is filtered out (empty normalized url)
    assert rows == [("fi-1", "https://example.fi/path", "example.fi", True, True)]
