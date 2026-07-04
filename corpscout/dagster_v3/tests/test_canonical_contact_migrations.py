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
