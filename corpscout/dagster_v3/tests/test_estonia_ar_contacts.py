import json
from pathlib import Path

import duckdb

from dagster_v3.defs.estonia_ar import company_domains, contacts, resources, tables

MIG_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"
CONTACTS_MIGRATION = (MIG_DIR / "000027_corpscout_ee_company_contacts.up.sql").read_text()
CONTACTS_DOMAIN_MIGRATION = (
    MIG_DIR / "000028_corpscout_ee_company_contacts_domain.up.sql"
).read_text()
DOMAINS_MIGRATION = (MIG_DIR / "000029_corpscout_ee_company_domains.up.sql").read_text()


def _write_sample(path: Path) -> None:
    records = [
        {
            "ariregistri_kood": 16752073,
            "yldandmed": {
                "sidevahendid": [
                    {"liik": "EMAIL", "sisu": "info@acme.ee", "lopp_kpv": None},
                    {"liik": "WWW", "sisu": "https://www.acme.ee", "lopp_kpv": None},
                    {"liik": "MOB", "sisu": "+372 5000000", "lopp_kpv": "01.01.2020"},
                    {"liik": "EMAIL", "sisu": "   ", "lopp_kpv": None},
                ]
            },
        },
        # gmail (denylist) + kvatro.ee (shared with company 3) -> neither is a domain.
        {
            "ariregistri_kood": 200,
            "yldandmed": {
                "sidevahendid": [
                    {"liik": "EMAIL", "sisu": "owner@gmail.com", "lopp_kpv": None},
                    {"liik": "EMAIL", "sisu": "raamat@kvatro.ee", "lopp_kpv": None},
                ]
            },
        },
        {
            "ariregistri_kood": 300,
            "yldandmed": {
                "sidevahendid": [
                    {"liik": "EMAIL", "sisu": "bok@kvatro.ee", "lopp_kpv": None},
                    {"liik": "EMAIL", "sisu": "hello@uniquefirm.ee", "lopp_kpv": None},
                ]
            },
        },
    ]
    path.write_text(json.dumps(records))


def _build(tmp_path) -> Path:
    json_path = tmp_path / "yldandmed.json"
    _write_sample(json_path)
    db = tmp_path / "ee.duckdb"
    contacts._build_contacts_from_json(
        database_path=db, json_path=json_path, source_run_id="run-1"
    )
    return db


def test_contacts_domain_derivation(tmp_path):
    json_path = tmp_path / "yldandmed.json"
    _write_sample(json_path)
    db = tmp_path / "ee.duckdb"
    counts = contacts._build_contacts_from_json(
        database_path=db, json_path=json_path, source_run_id="run-1"
    )
    # 1 website domain (acme.ee), 2 email domains (acme.ee, uniquefirm.ee).
    assert counts["websites"] == 1
    assert counts["email_domains"] == 2

    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {contacts.DLT_DATASET_NAME}.{contacts.CONTACTS_TABLE}"
        ).fetchall()]
        assert set(cols) == set(tables.EE_COMPANY_CONTACTS_COLUMNS)
        rows = con.execute(
            f"select reg_code, contact_value, domain, domain_source "
            f"from {contacts.DLT_DATASET_NAME}.{contacts.CONTACTS_TABLE} "
            f"order by reg_code, contact_value"
        ).fetchall()

    by_val = {r[1]: r for r in rows}
    assert by_val["https://www.acme.ee"][2:] == ("acme.ee", "website")
    assert by_val["info@acme.ee"][2:] == ("acme.ee", "email")
    # gmail is denylisted; kvatro.ee is shared across 2 companies -> dropped.
    assert by_val["owner@gmail.com"][2:] == ("", "")
    assert by_val["raamat@kvatro.ee"][2:] == ("", "")
    assert by_val["bok@kvatro.ee"][2:] == ("", "")
    assert by_val["hello@uniquefirm.ee"][2:] == ("uniquefirm.ee", "email")
    # phone carries no domain.
    assert by_val["+372 5000000"][2:] == ("", "")


def test_company_domains_feeder(tmp_path):
    db = _build(tmp_path)
    counts = company_domains.build_estonia_ar_company_domains(
        database_path=db, source_run_id="run-1"
    )
    # company 16752073 (acme.ee, deduped website+email) + company 300 (uniquefirm.ee).
    assert counts == {"domains": 2, "website_domains": 1, "email_domains": 1, "companies": 2}

    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {company_domains.DLT_DATASET_NAME}.{company_domains.COMPANY_DOMAINS_TABLE}"
        ).fetchall()]
        assert cols == list(tables.EE_COMPANY_DOMAINS_COLUMNS)
        rows = con.execute(
            f"select reg_code, domain, domain_source, website_host, is_primary "
            f"from {company_domains.DLT_DATASET_NAME}.{company_domains.COMPANY_DOMAINS_TABLE} "
            f"order by reg_code"
        ).fetchall()

    # acme.ee deduped to ONE row, website source wins (host populated).
    assert rows[0] == ("16752073", "acme.ee", "website", "www.acme.ee", 1)
    assert rows[1] == ("300", "uniquefirm.ee", "email", "", 1)


def test_contact_type_en_map_covers_all_codes():
    assert resources.EE_CONTACT_TYPE_EN_BY_CODE == {
        "WWW": "Website",
        "EMAIL": "Email",
        "MOB": "Mobile",
        "TEL": "Phone",
        "FAX": "Fax",
        "MUU": "Other",
    }


def test_contacts_export_columns_match_migrations():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_EE_COMPANY_CONTACTS_TABLE}"
        in CONTACTS_MIGRATION
    )
    combined = CONTACTS_MIGRATION + "\n" + CONTACTS_DOMAIN_MIGRATION
    for column in tables.EE_COMPANY_CONTACTS_EXPORT_COLUMNS:
        assert column in combined, f"missing {column} across migrations 000027/000028"
    # the two domain columns specifically land in the ALTER migration.
    for column in tables.EE_COMPANY_CONTACTS_DOMAIN_COLUMNS:
        assert column in CONTACTS_DOMAIN_MIGRATION


def test_company_domains_export_columns_match_migration():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_EE_COMPANY_DOMAINS_TABLE}"
        in DOMAINS_MIGRATION
    )
    for column in tables.EE_COMPANY_DOMAINS_EXPORT_COLUMNS:
        assert f"    {column} " in DOMAINS_MIGRATION, f"missing {column} in migration 000029"
    assert tables.EE_COMPANY_DOMAINS_EXPORT_COLUMNS == tables.EE_COMPANY_DOMAINS_COLUMNS
