from pathlib import Path

from tests.canonical_contact_tables import (
    assert_canonical_contacts_ddl,
    assert_canonical_domains_ddl,
)

_MIGRATIONS = Path(__file__).joinpath("../../../clickhouse/migrations").resolve()


def _read(pattern: str) -> str:
    matches = sorted(_MIGRATIONS.glob(pattern))
    assert matches, pattern
    return matches[-1].read_text()


def test_cz_canonical_migration_conforms():
    sql = _read("*_corpscout_cz_canonical_contacts.up.sql")
    assert_canonical_contacts_ddl(sql, "cz_company_contacts")
    assert_canonical_domains_ddl(sql, "cz_company_domains")


def test_lv_canonical_migration_conforms():
    sql = _read("*_corpscout_lv_canonical_contacts.up.sql")
    assert_canonical_contacts_ddl(sql, "lv_company_contacts")
    assert_canonical_domains_ddl(sql, "lv_company_domains")


def test_br_canonical_migration_conforms():
    sql = _read("*_corpscout_br_canonical_contacts.up.sql")
    assert_canonical_contacts_ddl(sql, "br_company_contacts")
    assert_canonical_domains_ddl(sql, "br_company_domains")


def test_ee_canonical_migration_conforms():
    # ee's reshape is data-preserving (shadow + INSERT SELECT + EXCHANGE, not
    # drop+recreate — ee_company_domains is live-consumed by the domain graph),
    # so the canonical CREATEs in this migration use the __canonical-suffixed
    # shadow names; the conformance helper's table-name parameter accepts them
    # directly.
    sql = _read("*_corpscout_ee_canonical_contacts.up.sql")
    assert_canonical_contacts_ddl(sql, "ee_company_contacts__canonical")
    assert_canonical_domains_ddl(sql, "ee_company_domains__canonical")
