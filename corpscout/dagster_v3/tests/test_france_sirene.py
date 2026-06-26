from pathlib import Path

import duckdb

from dagster_v3.defs.france_sirene import industries, resources, tables

MIG_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"
COMPANIES_MIGRATION = (MIG_DIR / "000032_corpscout_fr_companies.up.sql").read_text()
INDUSTRIES_MIGRATION = (MIG_DIR / "000033_corpscout_fr_industries.up.sql").read_text()
COMPANIES_ADDRESS_MIGRATION = (
    MIG_DIR / "000034_corpscout_fr_companies_address.up.sql"
).read_text()

HEADER = (
    "siren,statutDiffusionUniteLegale,unitePurgeeUniteLegale,dateCreationUniteLegale,"
    "sigleUniteLegale,sexeUniteLegale,prenom1UniteLegale,prenom2UniteLegale,prenom3UniteLegale,"
    "prenom4UniteLegale,prenomUsuelUniteLegale,pseudonymeUniteLegale,identifiantAssociationUniteLegale,"
    "trancheEffectifsUniteLegale,anneeEffectifsUniteLegale,dateDernierTraitementUniteLegale,"
    "nombrePeriodesUniteLegale,categorieEntreprise,anneeCategorieEntreprise,dateDebut,"
    "etatAdministratifUniteLegale,nomUniteLegale,nomUsageUniteLegale,denominationUniteLegale,"
    "denominationUsuelle1UniteLegale,denominationUsuelle2UniteLegale,denominationUsuelle3UniteLegale,"
    "categorieJuridiqueUniteLegale,activitePrincipaleUniteLegale,nomenclatureActivitePrincipaleUniteLegale,"
    "nicSiegeUniteLegale,economieSocialeSolidaireUniteLegale,societeMissionUniteLegale,"
    "caractereEmployeurUniteLegale,activitePrincipaleNAF25UniteLegale"
)
ROWS = [
    # sole trader, active, NAF2025 present
    "000325175,O,,2000-09-26,,M,THIERRY,,,,THIERRY,,,NN,,2025-12-06T10:43:55,6,PME,2023,2018-02-07,A,JANOYER,,,,,,1000,32.12Z,NAFRev2,00065,,,,32.12Y",
    # sole trader, ceased, NAF2025 empty -> NAFRev2 fallback
    "001807254,O,,1972-05-01,,M,JACQUES-LUCIEN,,,,JACQUES-LUCIEN,,,NN,,2024-03-22T14:26:06,5,,,2014-12-31,C,BRETON,,,,,,1000,85.59A,NAFRev2,00022,,,,",
    # company (denomination), SA, active, ESS
    "552081317,O,,1909-07-31,LOREAL,,,,,,,,,42,2023,2025-01-01T00:00:00,3,GE,2023,2020-01-01,A,,,L OREAL,,,,5599,20.42Z,NAFRev2,01230,O,,O,20.42Z",
    # ceased SARL with a pre-2008 nomenclature (NAFRev1) -> industry is unmapped
    "333333333,O,,1985-01-01,,,,,,,,,,NN,,2010-01-01T00:00:00,1,,,2005-01-01,C,,,VIEUX COMMERCE,,,,5499,52.4A,NAFRev1,00011,,,,",
]


# Minimal StockEtablissement sample: a siège for L'OREAL, a siège for one sole
# trader, and a NON-siège row that must be filtered out.
ETAB_HEADER = (
    "siren,nic,siret,etablissementSiege,complementAdresseEtablissement,numeroVoieEtablissement,"
    "typeVoieEtablissement,libelleVoieEtablissement,codePostalEtablissement,libelleCommuneEtablissement,"
    "libelleCommuneEtrangerEtablissement,codeCommuneEtablissement,codeCedexEtablissement,"
    "libellePaysEtrangerEtablissement"
)
ETAB_ROWS = [
    "552081317,01230,55208131701230,true,,41,RUE,MARTRE,92110,CLICHY,,92024,,",
    "000325175,00065,00032517500065,true,BAT A,12,AV,DE LA GARE,75012,PARIS,,75112,,",
    # non-siège establishment for the same company -> must be excluded
    "552081317,00099,55208131700099,false,,9,RUE,AUTRE,69000,LYON,,69123,,",
]


def _load_raw(tmp_path, *, with_siege: bool = True) -> Path:
    csv = tmp_path / "ul.csv"
    csv.write_text("\n".join([HEADER, *ROWS]) + "\n")
    db = tmp_path / "fr.duckdb"
    con = duckdb.connect(str(db))
    con.execute(f"create schema {tables.DLT_DATASET_NAME}")
    con.execute(
        f"create table {tables.DLT_DATASET_NAME}.{tables.UNITE_LEGALE_RAW_TABLE} as "
        f"select * from read_csv('{csv}', header=true, all_varchar=true)"
    )
    if with_siege:
        etab = tmp_path / "etab.csv"
        etab.write_text("\n".join([ETAB_HEADER, *ETAB_ROWS]) + "\n")
        resources.build_etablissement_siege_from_csv(connection=con, csv_path=etab)
    con.close()
    return db


def test_siege_filter_and_address(tmp_path):
    db = _load_raw(tmp_path)
    with duckdb.connect(str(db), read_only=True) as con:
        rows = {r[0]: r for r in con.execute(
            f"select siren, address, address_supplement, postal_code, city, city_code "
            f"from {tables.DLT_DATASET_NAME}.{tables.ETABLISSEMENT_SIEGE_TABLE}"
        ).fetchall()}
    # non-siège row dropped -> only 2 sièges, one per company.
    assert set(rows) == {"552081317", "000325175"}
    assert rows["552081317"][1:] == ("41 RUE MARTRE", "", "92110", "CLICHY", "92024")
    assert rows["000325175"][1:] == ("12 AV DE LA GARE", "BAT A", "75012", "PARIS", "75112")


def test_companies_build(tmp_path):
    db = _load_raw(tmp_path)
    with duckdb.connect(str(db)) as con:
        counts = resources.build_france_sirene_companies(
            connection=con, source_run_id="r1", source_url="http://x/stock.zip"
        )
    assert counts == {"companies": 4, "active": 2}
    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}"
        ).fetchall()]
        assert cols == list(tables.FR_COMPANIES_COLUMNS)
        rows = {r[0]: r for r in con.execute(
            f"select siren, name, legal_form_en, status_en, is_active, naf_code, naf_nomenclature, "
            f"is_social_solidarity_economy from {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}"
        ).fetchall()}
    assert rows["000325175"][1:] == ("THIERRY JANOYER", "Sole trader", "Active", True, "32.12Y", "NAF2025", False)
    assert rows["001807254"][2:7] == ("Sole trader", "Ceased", False, "85.59A", "NAFRev2")
    assert rows["552081317"][1:] == ("L OREAL", "Public limited company (SA)", "Active", True, "20.42Z", "NAF2025", True)

    # Phase 2: the siège address is joined onto the company; companies with no
    # siège row keep empty address strings.
    with duckdb.connect(str(db), read_only=True) as con:
        addr = {r[0]: r for r in con.execute(
            f"select siren, address, postal_code, city, city_code "
            f"from {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}"
        ).fetchall()}
    assert addr["552081317"][1:] == ("41 RUE MARTRE", "92110", "CLICHY", "92024")
    assert addr["000325175"][1:] == ("12 AV DE LA GARE", "75012", "PARIS", "75112")
    assert addr["001807254"][1:] == ("", "", "", "")


def test_industries_build_naf_to_nace(tmp_path):
    db = _load_raw(tmp_path)
    with duckdb.connect(str(db)) as con:
        counts = industries.build_france_sirene_industries(
            connection=con, source_run_id="r1"
        )
    assert counts == {"industries": 4, "nace_mapped": 3}
    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {tables.DLT_DATASET_NAME}.{tables.INDUSTRIES_RAW_TABLE}"
        ).fetchall()]
        assert cols == list(tables.FR_INDUSTRIES_COLUMNS)
        rows = {r[0]: r for r in con.execute(
            f"select siren, source_industry_code, source_industry_code_set, nace_revision, "
            f"nace_code, nace_normalized_code, nace_mapping_status, is_primary "
            f"from {tables.DLT_DATASET_NAME}.{tables.INDUSTRIES_RAW_TABLE}"
        ).fetchall()}
    # NAF 2025 -> NACE Rev 2.1, trailing letter stripped.
    assert rows["000325175"][1:] == ("32.12Y", "NAF2025", "NACE_REV_2_1", "32.12", "3212", "mapped", 1)
    # NAF2025 empty -> NAF Rev2 -> NACE Rev 2.
    assert rows["001807254"][1:] == ("85.59A", "NAFRev2", "NACE_REV_2", "85.59", "8559", "mapped", 1)
    # pre-2008 nomenclature (NAFRev1) -> kept as source code but unmapped, no NACE.
    assert rows["333333333"][1:] == ("52.4A", "NAFRev1", "", "", "", "unmapped", 1)


def test_companies_export_columns_match_migration():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_COMPANIES_TABLE}" in COMPANIES_MIGRATION
    )
    # base columns in 000032; the Phase-2 address columns in the 000034 ALTER.
    combined = COMPANIES_MIGRATION + "\n" + COMPANIES_ADDRESS_MIGRATION
    for column in tables.FR_COMPANIES_EXPORT_COLUMNS:
        assert column in combined, f"missing {column} across migrations 000032/000034"
    for column in tables.FR_COMPANIES_ADDRESS_COLUMNS:
        assert column in COMPANIES_ADDRESS_MIGRATION
    assert "raw_entity" not in tables.FR_COMPANIES_EXPORT_COLUMNS
    assert "source_payload_hash" not in tables.FR_COMPANIES_EXPORT_COLUMNS


def test_industries_export_columns_match_migration():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_INDUSTRIES_TABLE}" in INDUSTRIES_MIGRATION
    )
    for column in tables.FR_INDUSTRIES_EXPORT_COLUMNS:
        assert f"    {column} " in INDUSTRIES_MIGRATION, f"missing {column} in 000033"
    assert tables.FR_INDUSTRIES_EXPORT_COLUMNS == tables.FR_INDUSTRIES_COLUMNS


def test_legal_form_and_status_maps():
    assert resources.FR_LEGAL_FORM_EN_BY_CODE["1000"] == "Sole trader"
    assert resources.FR_LEGAL_FORM_EN_BY_CODE["5710"] == "Simplified joint-stock company (SAS)"
    assert resources.FR_STATUS_EN_BY_CODE == {"A": "Active", "C": "Ceased"}


def test_register_job_and_schedule():
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    sch = repo.get_schedule_def("france_sirene_register_schedule")
    assert sch.cron_schedule == "0 7 6 * *"
    assert sch.job.name == "france_sirene_register_job"

    # .upstream() of the two exports pulls the single raw download once.
    keys = {
        k.path[-1]
        for k in repo.get_job("france_sirene_register_job").asset_layer.executable_asset_keys
    }
    assert keys == {
        "france_sirene_unite_legale_raw_duckdb",
        "france_sirene_etablissement_siege_raw_duckdb",
        "france_sirene_companies_duckdb",
        "france_sirene_clickhouse_companies",
        "france_sirene_industries_duckdb",
        "france_sirene_clickhouse_industries",
    }
    full = {
        k.path[-1]
        for k in repo.get_job("france_sirene_full_refresh_job").asset_layer.executable_asset_keys
    }
    assert full == keys
