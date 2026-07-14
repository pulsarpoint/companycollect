import datetime as dt
import json
from pathlib import Path

import duckdb
import pytest

from dagster_v3 import contact_extraction
from dagster_v3.defs.clickhouse.resolved import export_duckdb_connection_table_to_clickhouse
from dagster_v3.defs.estonia_ar import company_domains, contacts, resources, tables
from tests import canonical_contact_tables


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
        # TELEX is absent from EE_CONTACT_TYPE_BY_CODE (a live-data fact, alongside
        # MUU) -> contact_type must fall back to "other" while contact_type_raw
        # preserves the code verbatim.
        {
            "ariregistri_kood": 400,
            "yldandmed": {
                "sidevahendid": [
                    {"liik": "TELEX", "sisu": "TLX-99", "lopp_kpv": None},
                ]
            },
        },
    ]
    path.write_text(json.dumps(records))


def _build(tmp_path) -> Path:
    json_path = tmp_path / "yldandmed.json"
    _write_sample(json_path)
    db = tmp_path / "ee.duckdb"
    with duckdb.connect(str(db)) as conn:
        contacts._build_contacts_from_json(
            duckdb_connection=conn, json_path=json_path, source_run_id="run-1"
        )
    return db


def test_contacts_domain_derivation(tmp_path):
    json_path = tmp_path / "yldandmed.json"
    _write_sample(json_path)
    db = tmp_path / "ee.duckdb"
    with duckdb.connect(str(db)) as conn:
        counts = contacts._build_contacts_from_json(
            duckdb_connection=conn, json_path=json_path, source_run_id="run-1"
        )
    # 1 website domain (acme.ee), 2 email domains (acme.ee, uniquefirm.ee).
    assert counts["websites"] == 1
    assert counts["email_domains"] == 2

    stage_columns = tables.EE_COMPANY_CONTACTS_STAGE_COLUMNS
    canonical_width = len(contact_extraction.COMPANY_CONTACTS_COLUMNS)
    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {contacts.DLT_DATASET_NAME}.{contacts.CONTACTS_TABLE}"
        ).fetchall()]
        assert set(cols) == set(stage_columns)
        rows = con.execute(
            f"select {', '.join(stage_columns)} "
            f"from {contacts.DLT_DATASET_NAME}.{contacts.CONTACTS_TABLE} "
            f"order by registry_id, contact_value"
        ).fetchall()

    # Every produced row conforms to the canonical row shape (the validator only
    # ever sees the canonical 13 — the trailing internal enrichment pair is sliced
    # off here, exactly as the ClickHouse export does).
    for row in rows:
        canonical_contact_tables.assert_canonical_contact_row(row[:canonical_width])

    by_val = {row[stage_columns.index("contact_value")]: dict(zip(stage_columns, row)) for row in rows}

    acme_www = by_val["https://www.acme.ee"]
    assert (acme_www["domain"], acme_www["domain_source"]) == ("acme.ee", "website")
    assert (acme_www["contact_type"], acme_www["contact_type_raw"]) == ("website", "WWW")
    assert acme_www["source_field"] == "sidevahendid"

    acme_email = by_val["info@acme.ee"]
    assert (acme_email["domain"], acme_email["domain_source"]) == ("acme.ee", "email")
    assert (acme_email["contact_type"], acme_email["contact_type_raw"]) == ("email", "EMAIL")

    # gmail is denylisted; kvatro.ee is shared across 2 companies -> dropped.
    assert (by_val["owner@gmail.com"]["domain"], by_val["owner@gmail.com"]["domain_source"]) == ("", "")
    assert (by_val["raamat@kvatro.ee"]["domain"], by_val["raamat@kvatro.ee"]["domain_source"]) == ("", "")
    assert (by_val["bok@kvatro.ee"]["domain"], by_val["bok@kvatro.ee"]["domain_source"]) == ("", "")
    assert (by_val["hello@uniquefirm.ee"]["domain"], by_val["hello@uniquefirm.ee"]["domain_source"]) == (
        "uniquefirm.ee",
        "email",
    )

    # phone carries no domain; the end-dated contact is not current and valid_to
    # is parsed from the source's lopp_kpv field.
    mob = by_val["+372 5000000"]
    assert (mob["domain"], mob["domain_source"]) == ("", "")
    assert (mob["contact_type"], mob["contact_type_raw"]) == ("mobile", "MOB")
    assert mob["is_current"] == 0
    assert mob["valid_to"] == dt.date(2020, 1, 1)

    # TELEX (unmapped code -> live-data fact) falls back to "other"; the raw code
    # is preserved verbatim in contact_type_raw.
    telex = by_val["TLX-99"]
    assert telex["contact_type"] == "other"
    assert telex["contact_type_raw"] == "TELEX"


def test_company_domains_feeder(tmp_path):
    db = _build(tmp_path)
    with duckdb.connect(str(db)) as conn:
        counts = company_domains.build_estonia_ar_company_domains(
            duckdb_connection=conn, source_run_id="run-1"
        )
    # company 16752073 (acme.ee, deduped website+email) + company 300 (uniquefirm.ee).
    assert counts == {"domains": 2, "website_domains": 1, "email_domains": 1, "companies": 2}

    export_columns = tables.EE_COMPANY_DOMAINS_EXPORT_COLUMNS
    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {company_domains.DLT_DATASET_NAME}.{company_domains.COMPANY_DOMAINS_TABLE}"
        ).fetchall()]
        assert cols == list(export_columns)
        rows = con.execute(
            f"select {', '.join(export_columns)} "
            f"from {company_domains.DLT_DATASET_NAME}.{company_domains.COMPANY_DOMAINS_TABLE} "
            f"order by registry_id"
        ).fetchall()

    for row in rows:
        canonical_contact_tables.assert_canonical_domain_row(row)

    by_registry = {row[export_columns.index("registry_id")]: dict(zip(export_columns, row)) for row in rows}

    # acme.ee deduped to ONE row, website source wins (real URL columns, confidence 1.0).
    acme = by_registry["16752073"]
    assert acme["domain"] == "acme.ee"
    assert acme["domain_source"] == "website"
    assert acme["website_url"] == "https://www.acme.ee"
    assert acme["website_host"] == "www.acme.ee"
    assert acme["confidence"] == contact_extraction.WEBSITE_CONFIDENCE
    assert acme["is_primary"] == 1

    # uniquefirm.ee is email-sourced: empty URL columns, confidence 0.9.
    unique = by_registry["300"]
    assert unique["domain"] == "uniquefirm.ee"
    assert unique["domain_source"] == "email"
    assert unique["website_url"] == ""
    assert unique["website_normalized_url"] == ""
    assert unique["website_host"] == ""
    assert unique["confidence"] == contact_extraction.EMAIL_UNIQUE_CONFIDENCE
    assert unique["is_primary"] == 1


def test_contact_type_by_code_maps_known_codes_to_the_canonical_vocabulary():
    assert resources.EE_CONTACT_TYPE_BY_CODE == {
        "WWW": "website",
        "EMAIL": "email",
        "TEL": "phone",
        "MOB": "mobile",
        "FAX": "fax",
        "MUU": "other",
    }
    assert set(resources.EE_CONTACT_TYPE_BY_CODE.values()) <= contact_extraction.CONTACT_TYPE_VALUES


def test_contacts_export_columns_exclude_internal_enrichment_pair():
    assert tables.EE_COMPANY_CONTACTS_EXPORT_COLUMNS == contact_extraction.COMPANY_CONTACTS_COLUMNS
    assert tables.EE_COMPANY_CONTACTS_STAGE_COLUMNS == (
        contact_extraction.COMPANY_CONTACTS_COLUMNS + ("domain", "domain_source")
    )
    assert "domain" not in tables.EE_COMPANY_CONTACTS_EXPORT_COLUMNS
    assert "domain_source" not in tables.EE_COMPANY_CONTACTS_EXPORT_COLUMNS


def test_company_domains_export_columns_match_canonical():
    assert tables.EE_COMPANY_DOMAINS_EXPORT_COLUMNS == contact_extraction.COMPANY_DOMAINS_COLUMNS


class _FakeInsertClickHouseClient:
    def __init__(self):
        self.insert_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql, params=None):
        if sql.startswith("INSERT INTO"):
            self.insert_calls.append((sql, params))
            return []
        raise AssertionError(f"unexpected statement: {sql}")


def test_contacts_clickhouse_export_excludes_internal_enrichment_columns(tmp_path):
    # End-to-end proof (not just a static tuple comparison): build the real
    # DuckDB stage (canonical 13 + trailing domain/domain_source), export it the
    # same way clickhouse.py does, and confirm neither the generated INSERT's
    # column list nor any inserted row tuple carries the internal pair.
    db = _build(tmp_path)
    client = _FakeInsertClickHouseClient()
    with duckdb.connect(str(db), read_only=True) as conn:
        rows_written = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=conn,
            clickhouse_client=client,
            duckdb_schema=contacts.DLT_DATASET_NAME,
            duckdb_table=contacts.CONTACTS_TABLE,
            clickhouse_database=tables.ESTONIA_AR_DATABASE,
            clickhouse_table=tables.EE_COMPANY_CONTACTS_TABLE,
            columns=tables.EE_COMPANY_CONTACTS_EXPORT_COLUMNS,
            truncate=False,
        )

    assert rows_written > 0
    assert client.insert_calls
    sql, inserted_rows = client.insert_calls[0]
    assert "domain" not in sql
    assert "domain_source" not in sql
    canonical_width = len(contact_extraction.COMPANY_CONTACTS_COLUMNS)
    assert inserted_rows
    for row in inserted_rows:
        assert len(row) == canonical_width


def test_domain_row_validator_rejects_website_row_with_empty_url():
    # Proves the validator actually catches the bug class it exists for: a row
    # claiming domain_source='website' (the domain-discovery signal itself) but
    # missing the real URL columns.
    bad_row = (
        "EE", "estonia_ar", "run-1", "src-1", "16752073", "acme.ee", "website",
        "", 1.0, "", "", "", 1, 1, dt.datetime(2026, 7, 4, tzinfo=dt.UTC),
    )
    assert len(bad_row) == len(contact_extraction.COMPANY_DOMAINS_COLUMNS)
    with pytest.raises(AssertionError):
        canonical_contact_tables.assert_canonical_domain_row(bad_row)
