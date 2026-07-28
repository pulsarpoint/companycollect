"""Tests for Latvia company-contacts extraction (shared-module consumer)."""

import datetime as dt
from pathlib import Path

from dagster_v3.defs.latvia_ur import contacts
from dagster_v3.defs.latvia_ur.contacts import (
    LV_CONTACTS_SOURCE_SLUG,
    build_candidate_scan_sql,
    extract_latvia_contact_candidates,
)
from tests.canonical_contact_tables import (
    assert_canonical_contacts_ddl,
    assert_canonical_domains_ddl,
)

MIG_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
CANONICAL_CONTACTS_MIGRATION = sorted(
    MIG_DIR.glob("*_corpscout_lv_canonical_contacts.up.sql")
)[-1].read_text()


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


def test_contacts_and_domains_conform_to_canonical_migration():
    assert_canonical_contacts_ddl(CANONICAL_CONTACTS_MIGRATION, "lv_company_contacts")
    assert_canonical_domains_ddl(CANONICAL_CONTACTS_MIGRATION, "lv_company_domains")
    assert LV_CONTACTS_SOURCE_SLUG == "latvia_ur"
    assert contacts.REGISTRY_ID_TYPE == "regcode"


def test_replace_latvia_company_contacts_writes_canonical_contact_and_domain_tables(monkeypatch):
    """Orchestrator writes BOTH canonical tables (two stage/EXCHANGE sequences) and
    returns the Task 4-shared counts-dict contract (mirrors the Czech orchestrator
    test in tests/test_czech_ares.py)."""

    class FakeClient:
        def __init__(self):
            self.commands: list[str] = []
            self.inserted: list[tuple] = []
            self._scan_batches = [
                [
                    ("40003000001", 'SIA "cenuklubs.lv"'),
                    ("40003000002", "IK dns-only.lv"),
                ],
            ]

        def execute(self, sql, params=None):
            stripped = sql.strip()
            self.commands.append(stripped)
            if stripped.startswith("SELECT regcode, legal_name"):
                return self._scan_batches.pop(0) if self._scan_batches else []
            if "commoncrawl_domains" in stripped:
                return [("cenuklubs.lv",)]
            return []

        def insert_rows(self, table, rows, *, columns, database):
            self.inserted.append((database, table, list(rows), columns))

    # cenuklubs.lv validates via CommonCrawl (see FakeClient.execute above);
    # dns-only.lv has no CommonCrawl hit, so the orchestrator asks for its
    # nameservers here — stub it out so the test never touches real DNS.
    monkeypatch.setattr(
        contacts,
        "resolve_nameservers_concurrently",
        lambda domains: {domain: ("ns1.example.lv",) for domain in domains},
    )

    fake = FakeClient()
    counts = contacts.replace_latvia_company_contacts_clickhouse(
        clickhouse_client=fake,
        resolved_at=dt.datetime(2026, 7, 4, tzinfo=dt.UTC),
    )

    assert counts.keys() == {
        "contact_facts", "domains", "primary_domains",
        "commoncrawl_validated", "dns_validated",
    }
    assert counts["contact_facts"] == 2  # cenuklubs.lv, dns-only.lv facts
    assert counts["domains"] == 2  # one row per registry_id
    assert counts["primary_domains"] == 2  # each registry_id has exactly one domain -> primary
    assert counts["commoncrawl_validated"] == 1
    assert counts["dns_validated"] == 1

    # Two stage/EXCHANGE sequences: contacts table first, then domains table.
    create_cmds = [c for c in fake.commands if c.startswith("CREATE TABLE")]
    exchange_cmds = [c for c in fake.commands if c.startswith("EXCHANGE TABLES")]
    drop_cmds = [c for c in fake.commands if c.startswith("DROP TABLE IF EXISTS")]
    assert len(create_cmds) == len(exchange_cmds) == len(drop_cmds) == 2
    assert create_cmds[0].endswith(f"AS {contacts.QUALIFIED_LV_CONTACTS_TABLE}")
    assert create_cmds[1].endswith(f"AS {contacts.QUALIFIED_LV_DOMAINS_TABLE}")
    assert contacts.QUALIFIED_LV_CONTACTS_TABLE in exchange_cmds[0]
    assert contacts.QUALIFIED_LV_DOMAINS_TABLE in exchange_cmds[1]

    # Correct qualified target tables and column lists for each write.
    assert len(fake.inserted) == 2
    contacts_database, contacts_stage_table, contact_rows, contact_columns = fake.inserted[0]
    domains_database, domains_stage_table, domain_rows, domain_columns = fake.inserted[1]
    assert contacts_database == domains_database == "corpscout"
    assert contacts_stage_table.startswith("_tmp_lv_company_contacts_")
    assert domains_stage_table.startswith("_tmp_lv_company_domains_")
    assert contact_columns == contacts.COMPANY_CONTACTS_COLUMNS
    assert domain_columns == contacts.COMPANY_DOMAINS_COLUMNS
    assert len(contact_rows) == 2
    assert len(domain_rows) == 2
