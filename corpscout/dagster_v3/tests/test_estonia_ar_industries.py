import json
from pathlib import Path

import duckdb

from dagster_v3.defs.estonia_ar import industries, tables

MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "clickhouse"
    / "migrations"
    / "000031_corpscout_ee_industries.up.sql"
).read_text()


def _write_sample(path: Path) -> None:
    records = [
        {
            "ariregistri_kood": 16752073,
            "yldandmed": {
                "teatatud_tegevusalad": [
                    {"emtak_kood": "73111", "emtak_tekstina": "Reklaam", "emtak_versioon_tekstina": "EMTAK 2025", "nace_kood": "73.11", "on_pohitegevusala": True, "lopp_kpv": None},
                    {"emtak_kood": "62010", "emtak_tekstina": "Programmeerimine", "emtak_versioon_tekstina": "EMTAK 2025", "nace_kood": "62.01", "on_pohitegevusala": False, "lopp_kpv": None},
                    # ended activity -> filtered out.
                    {"emtak_kood": "99999", "emtak_tekstina": "Vana", "emtak_versioon_tekstina": "EMTAK 2008", "nace_kood": "55.10", "on_pohitegevusala": False, "lopp_kpv": "01.01.2020"},
                ]
            },
        },
        # no nace_kood -> unmapped.
        {
            "ariregistri_kood": 300,
            "yldandmed": {
                "teatatud_tegevusalad": [
                    {"emtak_kood": "41201", "emtak_tekstina": "Ehitus", "emtak_versioon_tekstina": "EMTAK 2008", "nace_kood": None, "on_pohitegevusala": True, "lopp_kpv": None},
                ]
            },
        },
        {"ariregistri_kood": 400, "yldandmed": {"teatatud_tegevusalad": []}},
    ]
    path.write_text(json.dumps(records))


def test_industries_build(tmp_path):
    json_path = tmp_path / "yldandmed.json"
    _write_sample(json_path)
    db = tmp_path / "ee.duckdb"
    counts = industries._build_industries_from_json(
        database_path=db, json_path=json_path, source_run_id="run-1"
    )
    assert counts == {"industries": 3, "nace_mapped": 2, "companies": 2}

    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {industries.DLT_DATASET_NAME}.{industries.INDUSTRIES_TABLE}"
        ).fetchall()]
        assert cols == list(tables.EE_INDUSTRIES_COLUMNS)
        rows = con.execute(
            f"select reg_code, source_industry_code, source_industry_code_set, nace_revision, "
            f"nace_code, nace_normalized_code, nace_mapping_method, nace_mapping_status, is_primary "
            f"from {industries.DLT_DATASET_NAME}.{industries.INDUSTRIES_TABLE} "
            f"order by reg_code, source_industry_code"
        ).fetchall()

    # primary EMTAK 2025 activity with source-provided NACE.
    assert rows[1] == ("16752073", "73111", "EMTAK", "NACE_REV_2_1", "73.11", "7311", "source_provided", "mapped", 1)
    # secondary, not primary.
    assert rows[0][:2] == ("16752073", "62010") and rows[0][-1] == 0
    # no nace_kood -> unmapped, empty nace_code.
    assert rows[2] == ("300", "41201", "EMTAK", "NACE_REV_2", "", "", "source_provided", "unmapped", 1)


def test_export_columns_match_migration():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_EE_INDUSTRIES_TABLE}" in MIGRATION
    )
    for column in tables.EE_INDUSTRIES_EXPORT_COLUMNS:
        assert f"    {column} " in MIGRATION, f"missing {column} in migration 000031"
    assert tables.EE_INDUSTRIES_EXPORT_COLUMNS == tables.EE_INDUSTRIES_COLUMNS
