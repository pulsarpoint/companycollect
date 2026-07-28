from pathlib import Path

from dagster_v3.defs.estonia_ar import resources, tables

MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "clickhouse"
    / "migrations"
    / "000024_corpscout_ee_companies.up.sql"
).read_text()


def test_companies_columns_match_entities_schema():
    assert tables.EE_COMPANIES_COLUMNS == tuple(tables.ESTONIA_AR_ENTITIES_COLUMNS)
    # reg_code is the dlt primary key -> must be non-nullable.
    assert tables.ESTONIA_AR_ENTITIES_COLUMNS["reg_code"]["nullable"] is False


def test_export_columns_drop_raw_and_hash_only():
    assert tables.CLICKHOUSE_EXCLUDED_COLUMNS == {
        "raw_entity",
        "raw_financial_record",
        "source_payload_hash",
    }
    assert "source_payload_hash" in tables.EE_COMPANIES_COLUMNS
    assert "raw_entity" in tables.EE_COMPANIES_COLUMNS
    assert "source_payload_hash" not in tables.EE_COMPANIES_EXPORT_COLUMNS
    assert "raw_entity" not in tables.EE_COMPANIES_EXPORT_COLUMNS
    # every other column survives, order preserved.
    assert tables.EE_COMPANIES_EXPORT_COLUMNS == tuple(
        c for c in tables.EE_COMPANIES_COLUMNS
        if c not in {"source_payload_hash", "raw_entity"}
    )


def test_english_and_usd_translation_columns_present():
    # user requirement: every translatable field carries an _en column.
    for col in (
        "legal_form_original",
        "legal_form_en",
        "legal_form_subtype_original",
        "legal_form_subtype_en",
        "status_original",
        "status_en",
    ):
        assert col in tables.EE_COMPANIES_EXPORT_COLUMNS


def test_export_columns_match_migration():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_EE_COMPANIES_TABLE}" in MIGRATION
    )
    for column in tables.EE_COMPANIES_EXPORT_COLUMNS:
        assert f"    {column} " in MIGRATION, f"missing {column} in migration 000024"


def test_en_maps_cover_headline_vocabulary():
    assert resources.EE_LEGAL_FORM_EN_BY_NAME["Osaühing"] == "Private limited company"
    assert resources.EE_LEGAL_FORM_EN_BY_NAME["Aktsiaselts"] == "Public limited company"
    assert resources.EE_STATUS_EN_BY_CODE == {
        "R": "Registered",
        "L": "In liquidation",
        "N": "Bankrupt",
        "K": "Deleted",
    }
