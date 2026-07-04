"""Tests for Latvia company-contacts extraction (shared-module consumer)."""

from dagster_v3.defs.latvia_ur.contacts import (
    LV_CONTACTS_SOURCE_SLUG,
    LV_CONTACT_COLUMNS,
    build_candidate_scan_sql,
    extract_latvia_contact_candidates,
)


def test_candidate_scan_sql_prefilters_and_paginates_by_regcode():
    sql = build_candidate_scan_sql(after_regcode="40003000000", limit=1000)
    assert "FROM corpscout.lv_companies" in sql
    assert "legal_name" in sql
    assert "match(" in sql          # shared CANDIDATE_TEXT_FILTER prefilter
    assert "regcode >" in sql       # keyset pagination, no OFFSET
    assert "LIMIT" in sql


def test_real_latvian_names_extract_domains():
    cases = {
        'SIA "cenuklubs.lv"': "cenuklubs.lv",
        "IK Akmenkalis.com": "akmenkalis.com",
        'Sabiedrība ar ierobežotu atbildību "Metinājumi.lv"': "metinājumi.lv",
        "IK 24dressup.lv": "24dressup.lv",
    }
    for legal_name, expected_domain in cases.items():
        candidates = extract_latvia_contact_candidates(
            regcode="40003xxxxx", legal_name=legal_name
        )
        assert [c.domain for c in candidates] == [expected_domain], legal_name


def test_plain_legal_names_extract_nothing():
    for legal_name in (
        'Sabiedrība ar ierobežotu atbildību "Ozoli"',
        "Individuālais komersants JURIS BĒRZIŅŠ",
        'AS "Latvijas Gāze"',
    ):
        assert extract_latvia_contact_candidates(regcode="1", legal_name=legal_name) == []


def test_columns_match_migration():
    from pathlib import Path

    migration = next(
        Path(__file__).joinpath("../../../clickhouse/migrations").resolve().glob(
            "*_corpscout_lv_company_contacts.up.sql"
        )
    ).read_text()
    for column in LV_CONTACT_COLUMNS:
        assert column in migration
    assert LV_CONTACTS_SOURCE_SLUG == "latvia_ur"
