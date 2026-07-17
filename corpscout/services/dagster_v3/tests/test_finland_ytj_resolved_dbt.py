import json
from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest
from dbt.cli.main import dbtRunner

from dagster_v3.defs.finland_ytj import dbt_plugin

DBT_DIR = (
    Path(__file__).parents[1]
    / "src" / "dagster_v3" / "defs" / "finland_ytj" / "dbt"
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
        create table finland_prhytj.all_companies as
        select * replace (cast(last_modified as timestamptz) as last_modified)
        from (values
          ('fi-1','FI','Active One Oy','2024-01-01','',
           '2024-06-01 10:30:00+02','REGISTERED','',
           'active', true,
           'https://example.fi/path','https://example.fi/path','example.fi','/path','2024-01-02','',
           'finland_prhytj','run-1','fi-1','hash1',
           '{"businessId":{"value":"fi-1","registrationDate":"2024-01-01"},"names":[{"name":"Active One Oy","type":"1","registrationDate":"2024-01-01","endDate":null,"version":1,"source":"1"},{"name":"Active One old Oy","type":"1","registrationDate":"2020-01-01","endDate":"2023-12-31","version":2,"source":"1"}],"mainBusinessLine":{"code":"62010","codeSet":"NACE_REV_2","descriptions":[{"languageCode":"1","description":"Ohjelmistot"}]},"companyForms":[{"type":"16","registrationDate":"2024-01-01","version":1,"descriptions":[{"languageCode":"1","description":"Osakeyhtiö"},{"languageCode":"3","description":"Limited company"}]}],"registeredEntries":[{"register":"6","registrationDate":"2024-01-01"}]}'),
          ('fi-2','FI','Ceased Two Oy','2020-01-01','2025-01-01',
           NULL,'','',
           'ceased', false,
           '','','','','','',
           'finland_prhytj','run-1','fi-2','hash2','{}')
        ) as t(business_id,country_iso2,primary_name,registration_date,end_date,
                last_modified,trade_register_status,status,
                lifecycle_status,is_active,
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

    extracted = conn.execute(
        "select business_id_registration_date, eu_id, vat_id, trade_register_status, "
        "raw_status_code, last_modified, is_vat_registered, is_employer_registered, "
        "is_prepayment_registered, legal_form_code, legal_form_description_original, "
        "legal_form_description_language, legal_form_description_en "
        "from finland_resolved.fi_companies where business_id = 'fi-1'"
    ).fetchone()
    assert extracted == (
        date(2024, 1, 1),
        None,
        None,
        "REGISTERED",
        None,
        datetime(2024, 6, 1, 8, 30, 0),
        True,
        False,
        False,
        "16",
        "Osakeyhtiö",
        "fi",
        "Limited company",
    )

    ceased = conn.execute(
        "select business_id_registration_date, trade_register_status, raw_status_code, "
        "last_modified, is_vat_registered, legal_form_code "
        "from finland_resolved.fi_companies where business_id = 'fi-2'"
    ).fetchone()
    assert ceased == (None, "", None, None, False, None)


def test_fi_websites_model(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "finland_ytj.duckdb"
    _seed_all_companies(db)
    _dbt_build(db, monkeypatch)
    conn = duckdb.connect(str(db), read_only=True)
    rows = conn.execute(
        "select business_id, website_normalized_url, website_host, root_domain, "
        "is_current, is_primary "
        "from finland_resolved.fi_websites order by business_id"
    ).fetchall()
    # Only fi-1 has a website; fi-2 is filtered out (empty normalized url)
    assert rows == [("fi-1", "https://example.fi/path", "example.fi", "example.fi", True, True)]


def test_fi_names_model_extracts_name_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = tmp_path / "finland_ytj.duckdb"
    _seed_all_companies(db)
    _dbt_build(db, monkeypatch)
    conn = duckdb.connect(str(db), read_only=True)
    rows = conn.execute(
        "select business_id, name, name_type_code, registration_date, end_date, "
        "version, is_current, is_primary, source_code, source_record_id "
        "from finland_resolved.fi_names order by business_id, version"
    ).fetchall()
    assert rows == [
        (
            "fi-1",
            "Active One Oy",
            "1",
            date(2024, 1, 1),
            None,
            1,
            True,
            True,
            "1",
            "fi-1:name:0",
        ),
        (
            "fi-1",
            "Active One old Oy",
            "1",
            date(2020, 1, 1),
            date(2023, 12, 31),
            2,
            False,
            False,
            "1",
            "fi-1:name:1",
        ),
    ]


def test_fi_industries_model(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "finland_ytj.duckdb"
    _seed_all_companies(db)
    _dbt_build(db, monkeypatch)
    conn = duckdb.connect(str(db), read_only=True)
    row = conn.execute(
        "select source_industry_code, source_industry_code_set, description_original, "
        "description_language, nace_revision, nace_code, nace_normalized_code, "
        "nace_mapping_method, nace_mapping_status, is_primary "
        "from finland_resolved.fi_industries where business_id = 'fi-1'"
    ).fetchone()
    assert row == (
        "62010", "NACE_REV_2", "Ohjelmistot", "fi",
        "NACE_REV_2", "62010", "62010", "direct_code", "mapped", True,
    )
    miss = conn.execute(
        "select source_industry_code, nace_mapping_method, nace_mapping_status "
        "from finland_resolved.fi_industries where business_id = 'fi-2'"
    ).fetchone()
    assert miss == (None, "none", "missing_source_code")
