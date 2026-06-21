import json
from pathlib import Path

import duckdb

from dagster_v3.defs.estonia_ar import contacts, resources, tables

MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "clickhouse"
    / "migrations"
    / "000027_corpscout_ee_company_contacts.up.sql"
).read_text()


def _write_sample(path: Path) -> None:
    records = [
        {
            "ariregistri_kood": 16752073,
            "nimi": "X OÜ",
            "yldandmed": {
                "sidevahendid": [
                    {"liik": "EMAIL", "liik_tekstina": "E-post", "sisu": "info@x.ee", "lopp_kpv": None},
                    {"liik": "WWW", "liik_tekstina": "Veeb", "sisu": "https://x.ee", "lopp_kpv": None},
                    {"liik": "MOB", "liik_tekstina": "Mobiil", "sisu": "+372 5000000", "lopp_kpv": "01.01.2020"},
                    {"liik": "EMAIL", "liik_tekstina": "E-post", "sisu": "   ", "lopp_kpv": None},
                ]
            },
        },
        {"ariregistri_kood": 11694365, "nimi": "Y AS", "yldandmed": {"sidevahendid": []}},
    ]
    path.write_text(json.dumps(records))


def test_build_contacts_from_json(tmp_path):
    json_path = tmp_path / "yldandmed.json"
    _write_sample(json_path)
    db = tmp_path / "ee.duckdb"

    counts = contacts._build_contacts_from_json(
        database_path=db, json_path=json_path, source_run_id="run-1"
    )
    # blank-value EMAIL dropped; empty-contacts company yields no rows.
    assert counts == {"contacts": 3, "websites": 1}

    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {contacts.DLT_DATASET_NAME}.{contacts.CONTACTS_TABLE}"
        ).fetchall()]
        assert set(cols) == set(tables.EE_COMPANY_CONTACTS_COLUMNS)
        rows = con.execute(
            f"select contact_type, contact_type_en, contact_value, is_current, end_date "
            f"from {contacts.DLT_DATASET_NAME}.{contacts.CONTACTS_TABLE} order by contact_type"
        ).fetchall()

    by_type = {r[0]: r for r in rows}
    assert by_type["WWW"][1] == "Website"
    assert by_type["EMAIL"][1] == "Email"
    assert by_type["MOB"][1] == "Mobile"
    # is_current reflects the presence of an end date.
    assert by_type["EMAIL"][3] == 1 and by_type["EMAIL"][4] is None
    assert by_type["MOB"][3] == 0 and str(by_type["MOB"][4]) == "2020-01-01"


def test_contact_type_en_map_covers_all_codes():
    assert resources.EE_CONTACT_TYPE_EN_BY_CODE == {
        "WWW": "Website",
        "EMAIL": "Email",
        "MOB": "Mobile",
        "TEL": "Phone",
        "FAX": "Fax",
        "MUU": "Other",
    }


def test_export_columns_match_migration():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_EE_COMPANY_CONTACTS_TABLE}"
        in MIGRATION
    )
    for column in tables.EE_COMPANY_CONTACTS_EXPORT_COLUMNS:
        assert f"    {column} " in MIGRATION, f"missing {column} in migration 000027"
    # contacts carry no raw_*/hash columns to exclude — export == full tuple.
    assert (
        tables.EE_COMPANY_CONTACTS_EXPORT_COLUMNS == tables.EE_COMPANY_CONTACTS_COLUMNS
    )
