import datetime as dt
import json
from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.slovakia_rpo import industries, resources, tables

MIG_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
COMPANIES_MIGRATION = (MIG_DIR / "000041_corpscout_sk_companies.up.sql").read_text()
INDUSTRIES_MIGRATION = (MIG_DIR / "000042_corpscout_sk_industries.up.sql").read_text()


# --- fixture records (shape captured from the real rpo2.organizations dump) ---
R1 = {
    "id": 13553043,
    "identifiers": [{"value": "53038517", "validFrom": "2020-04-01"}],
    "fullNames": [
        {"value": "Silvia Hudáková", "validFrom": "2025-09-29"},
        {"value": "Silvia Škultétyová", "validTo": "2025-09-28", "validFrom": "2020-04-01"},
    ],
    "legalForms": [
        {"value": {"code": "101", "value": "Podnikateľ-fyzická osoba", "codelistCode": "CL000056"},
         "validFrom": "2020-04-01"},
    ],
    "addresses": [
        {"street": "Komárnik", "regNumber": 215, "buildingNumber": "14",
         "postalCodes": ["029 51"], "validFrom": "2020-04-01",
         "country": {"code": "703", "value": "Slovenská republika"},
         "municipality": {"code": "SK0317509809", "value": "Lokca"}},
    ],
    "establishment": "2020-04-01",
    "sourceRegister": {"value": {"code": "2", "value": "Živnostenský register"}},
    "statisticalCodes": {"mainActivity": {"code": "4619", "value": "Sprostredkovanie veľkoobchodu"}},
}
R2 = {
    "id": 222,
    "identifiers": [{"value": "12345678", "validFrom": "2010-05-05"}],
    "fullNames": [{"value": "Asseco s.r.o.", "validFrom": "2010-05-05"}],
    "legalForms": [
        {"value": {"code": "112", "value": "Spoločnosť s ručením obmedzeným"}, "validFrom": "2010-05-05"},
    ],
    "addresses": [
        {"street": "Bajkalská", "buildingNumber": "22", "postalCodes": ["82109"],
         "country": {"code": "703"}, "municipality": {"code": "SK010", "value": "Bratislava"}},
    ],
    "establishment": "2010-05-05",
    "statisticalCodes": {"mainActivity": {"code": "6201", "value": "Počítačové programovanie"}},
}
R3 = {  # terminated entity; identifier + legal form carry validTo, no main activity
    "id": 333,
    "identifiers": [{"value": "41754191", "validTo": "2005-07-20", "validFrom": "2005-04-01"}],
    "fullNames": [{"value": "Stará firma", "validTo": "2005-07-20", "validFrom": "2005-04-01"}],
    "legalForms": [
        {"value": {"code": "101", "value": "Podnikateľ-fyzická osoba"},
         "validTo": "2005-07-20", "validFrom": "2005-04-01"},
    ],
    "addresses": [],
    "establishment": "2005-04-01",
    "termination": "2005-07-20",
    "statisticalCodes": {},
}
NO_ICO = {"id": 444, "identifiers": [], "fullNames": [{"value": "Bez IČO"}], "legalForms": []}


def _copy_escape(value: str) -> str:
    return (
        value.replace("\\", "\\\\").replace("\t", "\\t")
        .replace("\n", "\\n").replace("\r", "\\r")
    )


def _make_dump(records) -> str:
    lines = [
        "CREATE TABLE rpo2.organizations (id bigint, data jsonb);",
        "COPY rpo2.organizations (id, data, created_at, updated_at, actualized_at) FROM stdin;",
    ]
    for rec in records:
        data = _copy_escape(json.dumps(rec, ensure_ascii=False))
        lines.append(f"{rec['id']}\t{data}\t2026-01-01\t2026-01-01\t2026-01-01")
    lines.append("\\.")
    lines.append("COPY rpo2.suborganizations (id, data) FROM stdin;")
    lines.append("999\t{}\t")
    lines.append("\\.")
    return "\n".join(lines) + "\n"


def _load_raw(tmp_path, records) -> Path:
    db = tmp_path / "sk.duckdb"
    con = duckdb.connect(str(db))
    parsed = list(resources.iter_dump_records(_make_dump(records).splitlines(keepends=True)))
    n = resources.load_records_into_duckdb(connection=con, records=parsed)
    con.close()
    assert n == len([r for r in records if r.get("identifiers")])
    return db


def test_unescape_copy_roundtrips_double_escaped_json():
    # JSON with an escaped quote + backslash -> COPY doubles the backslashes.
    original = json.dumps({"v": 'a"b\\c'})
    assert resources._unescape_copy(_copy_escape(original)) == original


def test_extract_company_resolves_current_versions():
    row = resources.extract_company(R1)
    assert row["ico"] == "53038517"
    # current fullName has no validTo (Hudáková), not the superseded Škultétyová.
    assert row["name"] == "Silvia Hudáková"
    assert row["legal_form_code"] == "101"
    assert row["main_activity_code"] == "4619"
    assert row["address_street"] == "Komárnik"
    assert row["address_reg_number"] == "215"
    assert row["address_building_number"] == "14"
    assert row["postal_code"] == "02951"  # spaces stripped
    assert row["city"] == "Lokca"
    assert row["termination"] == ""


def test_extract_company_terminated_and_missing_ico():
    row = resources.extract_company(R3)
    assert row["ico"] == "41754191" and row["termination"] == "2005-07-20"
    assert row["main_activity_code"] == ""
    assert resources.extract_company(NO_ICO) is None


def test_iter_dump_records_stops_at_organizations_block():
    records = list(resources.iter_dump_records(_make_dump([R1, R2]).splitlines(keepends=True)))
    assert [r["id"] for r in records] == [13553043, 222]  # suborganizations not yielded


def test_load_records_streams_arrow_batches(tmp_path):
    consumed = 0

    def records():
        nonlocal consumed
        for index in range(5):
            consumed += 1
            yield {
                **R2,
                "id": index,
                "identifiers": [{"value": str(index), "validFrom": "2010-05-05"}],
            }

    with duckdb.connect(str(tmp_path / "streamed.duckdb")) as connection:
        row_count = resources.load_records_into_duckdb(
            connection=connection,
            records=records(),
            batch_rows=2,
        )
        stored = connection.execute(
            f"select count(*) from {tables.DLT_DATASET_NAME}.{tables.RPO_RAW_TABLE}"
        ).fetchone()[0]

    assert consumed == 5
    assert row_count == 5
    assert stored == 5


def test_load_records_rejects_invalid_batch_size():
    with duckdb.connect(":memory:") as connection:
        with pytest.raises(
            ValueError,
            match="Slovak RPO insert batch size must be greater than zero",
        ):
            resources.load_records_into_duckdb(
                connection=connection,
                records=[],
                batch_rows=0,
            )


def test_companies_build(tmp_path):
    db = _load_raw(tmp_path, [R1, R2, R3])
    with duckdb.connect(str(db)) as con:
        counts = resources.build_slovakia_rpo_companies(connection=con, source_run_id="r1")
    assert counts == {"companies": 3, "active": 2}
    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}"
        ).fetchall()]
        assert cols == list(tables.SK_COMPANIES_COLUMNS)
        rows = {r[0]: r for r in con.execute(
            f"select ico, name, legal_form_en, is_active, established_date, terminated_date, "
            f"address, postal_code, city from {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}"
        ).fetchall()}
    assert rows["53038517"][1:] == (
        "Silvia Hudáková", "Sole trader (not registered in commercial register)",
        True, dt.date(2020, 4, 1), None, "Komárnik 215/14", "02951", "Lokca",
    )
    assert rows["12345678"][2] == "Limited liability company (s.r.o.)"
    assert rows["12345678"][6] == "Bajkalská 22"
    # terminated -> is_active False + terminated_date.
    assert rows["41754191"][3] is False and rows["41754191"][5] == dt.date(2005, 7, 20)


def test_industries_build_sknace_to_nace(tmp_path):
    db = _load_raw(tmp_path, [R1, R2, R3])
    with duckdb.connect(str(db)) as con:
        counts = industries.build_slovakia_rpo_industries(connection=con, source_run_id="r1")
    # R3 has no main activity -> dropped; R1 + R2 mapped.
    assert counts == {"industries": 2, "nace_mapped": 2}
    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {tables.DLT_DATASET_NAME}.{tables.INDUSTRIES_RAW_TABLE}"
        ).fetchall()]
        assert cols == list(tables.SK_INDUSTRIES_COLUMNS)
        rows = {r[0]: r for r in con.execute(
            f"select ico, source_industry_code, source_industry_code_set, nace_revision, "
            f"nace_code, nace_normalized_code, nace_mapping_status, is_primary "
            f"from {tables.DLT_DATASET_NAME}.{tables.INDUSTRIES_RAW_TABLE}"
        ).fetchall()}
    assert rows["53038517"][1:] == ("4619", "SK_NACE", "NACE_REV_2", "46.19", "4619", "mapped", 1)
    assert rows["12345678"][4] == "62.01"


def test_companies_export_columns_match_migration():
    assert f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_COMPANIES_TABLE}" in COMPANIES_MIGRATION
    for column in tables.SK_COMPANIES_EXPORT_COLUMNS:
        assert f"    {column} " in COMPANIES_MIGRATION, f"missing {column} in 000041"
    assert "raw_entity" not in tables.SK_COMPANIES_EXPORT_COLUMNS
    assert "source_payload_hash" not in tables.SK_COMPANIES_EXPORT_COLUMNS


def test_industries_export_columns_match_migration():
    assert f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_INDUSTRIES_TABLE}" in INDUSTRIES_MIGRATION
    for column in tables.SK_INDUSTRIES_EXPORT_COLUMNS:
        assert f"    {column} " in INDUSTRIES_MIGRATION, f"missing {column} in 000042"
    assert tables.SK_INDUSTRIES_EXPORT_COLUMNS == tables.SK_INDUSTRIES_COLUMNS


def test_legal_form_map():
    assert resources.SK_LEGAL_FORM_EN_BY_CODE["112"] == "Limited liability company (s.r.o.)"
    assert resources.SK_LEGAL_FORM_EN_BY_CODE["121"] == "Joint-stock company (a.s.)"


def test_register_job_and_schedule():
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    sch = repo.get_schedule_def("slovakia_rpo_register_schedule")
    assert sch.cron_schedule == "0 7 * * 1"
    assert sch.job.name == "slovakia_rpo_register_job"
    keys = {
        k.path[-1]
        for k in repo.get_job("slovakia_rpo_register_job").asset_layer.executable_asset_keys
    }
    assert keys == {
        "slovakia_rpo_raw_duckdb",
        "slovakia_rpo_companies_duckdb",
        "slovakia_rpo_clickhouse_companies",
        "slovakia_rpo_industries_duckdb",
        "slovakia_rpo_clickhouse_industries",
    }
