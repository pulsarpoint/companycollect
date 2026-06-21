from pathlib import Path

import duckdb

from dagster_v3.defs.uk_companies_house import industries, resources, tables

MIG_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"
COMPANIES_MIGRATION = (MIG_DIR / "000035_corpscout_gb_companies.up.sql").read_text()
INDUSTRIES_MIGRATION = (MIG_DIR / "000036_corpscout_gb_industries.up.sql").read_text()
FINANCIALS_MIGRATION = (
    MIG_DIR / "000037_corpscout_gb_financial_metrics.up.sql"
).read_text()

HEADER = (
    "CompanyName,CompanyNumber,RegAddress.AddressLine1,RegAddress.AddressLine2,"
    "RegAddress.PostTown,RegAddress.County,RegAddress.Country,RegAddress.PostCode,"
    "CompanyCategory,CompanyStatus,CountryOfOrigin,DissolutionDate,IncorporationDate,"
    "SICCode.SicText_1,SICCode.SicText_2,SICCode.SicText_3,SICCode.SicText_4"
)
ROWS = [
    "ACME LTD,08209948,1 HIGH STREET,FLOOR 2,LONDON,GREATER LONDON,ENGLAND,EC1A 1AA,"
    "Private Limited Company,Active,United Kingdom,,11/09/2012,"
    "62012 - Business and domestic software development,"
    "70229 - Management consultancy activities other than financial management,,",
    "DORMANT CO,09999999,,,HARROGATE,,ENGLAND,HG1 1ND,Private Limited Company,Active,"
    "United Kingdom,,01/01/2016,99999 - Dormant Company,,,",
    "OLD PLC,00000001,FINANCE HOUSE,,EDINBURGH,,SCOTLAND,EH1 1AA,Public Limited Company,"
    "Dissolved,United Kingdom,15/03/2020,01/01/1990,64191 - Banks,,,",
]


def _load_raw(tmp_path) -> Path:
    csv = tmp_path / "uk.csv"
    csv.write_text("\n".join([HEADER, *ROWS]) + "\n")
    db = tmp_path / "uk.duckdb"
    con = duckdb.connect(str(db))
    con.execute(f"create schema {tables.DLT_DATASET_NAME}")
    con.execute(
        f"create table {tables.DLT_DATASET_NAME}.{tables.COMPANIES_RAW_TABLE} as "
        f"select * from read_csv('{csv}', header=true, all_varchar=true, normalize_names=true)"
    )
    con.close()
    return db


def test_companies_build(tmp_path):
    db = _load_raw(tmp_path)
    counts = resources.build_uk_companies_house_companies(
        database_path=db, source_run_id="r1", source_url="http://x.zip"
    )
    assert counts == {"companies": 3, "active": 2}
    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}"
        ).fetchall()]
        assert cols == list(tables.GB_COMPANIES_COLUMNS)
        rows = {r[0]: r for r in con.execute(
            f"select company_number, name, company_category, is_active, incorporation_date, "
            f"dissolution_date, address, address_line_2, postal_code, city, county, country "
            f"from {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}"
        ).fetchall()}
    import datetime as dt
    assert rows["08209948"][1:] == (
        "ACME LTD", "Private Limited Company", True, dt.date(2012, 9, 11), None,
        "1 HIGH STREET", "FLOOR 2", "EC1A 1AA", "LONDON", "GREATER LONDON", "ENGLAND",
    )
    # dissolved -> is_active False, dissolution date parsed.
    assert rows["00000001"][3] is False
    assert rows["00000001"][5] == dt.date(2020, 3, 15)


def test_industries_build_sic_to_nace(tmp_path):
    db = _load_raw(tmp_path)
    counts = industries.build_uk_companies_house_industries(database_path=db, source_run_id="r1")
    # ACME has 2 SIC, DORMANT 1 (unmapped), OLD 1 -> 4 rows, 3 mapped.
    assert counts == {"industries": 4, "nace_mapped": 3}
    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {tables.DLT_DATASET_NAME}.{tables.INDUSTRIES_RAW_TABLE}"
        ).fetchall()]
        assert cols == list(tables.GB_INDUSTRIES_COLUMNS)
        rows = con.execute(
            f"select company_number, source_industry_code, source_industry_code_set, "
            f"nace_revision, nace_code, nace_normalized_code, nace_mapping_status, is_primary "
            f"from {tables.DLT_DATASET_NAME}.{tables.INDUSTRIES_RAW_TABLE} "
            f"order by company_number, is_primary desc, source_industry_code"
        ).fetchall()
    by = {(r[0], r[1]): r for r in rows}
    # primary SIC -> NACE Rev2, first 4 digits.
    assert by[("08209948", "62012")][2:] == ("UK_SIC_2007", "NACE_REV_2", "62.01", "6201", "mapped", 1)
    assert by[("08209948", "70229")][6:] == ("mapped", 0)  # secondary
    # 99999 Dormant placeholder -> unmapped.
    assert by[("09999999", "99999")][4:] == ("99.99", "9999", "unmapped", 1)


def test_companies_export_columns_match_migration():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_COMPANIES_TABLE}" in COMPANIES_MIGRATION
    )
    for column in tables.GB_COMPANIES_EXPORT_COLUMNS:
        assert f"    {column} " in COMPANIES_MIGRATION, f"missing {column} in 000035"
    assert "raw_entity" not in tables.GB_COMPANIES_EXPORT_COLUMNS
    assert "source_payload_hash" not in tables.GB_COMPANIES_EXPORT_COLUMNS


def test_industries_export_columns_match_migration():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_INDUSTRIES_TABLE}" in INDUSTRIES_MIGRATION
    )
    for column in tables.GB_INDUSTRIES_EXPORT_COLUMNS:
        assert f"    {column} " in INDUSTRIES_MIGRATION, f"missing {column} in 000036"
    assert tables.GB_INDUSTRIES_EXPORT_COLUMNS == tables.GB_INDUSTRIES_COLUMNS


def test_resolve_basic_company_data_url():
    class _Resp:
        text = (
            '<a href="BasicCompanyDataAsOneFile-2026-05-01.zip">x</a> '
            '<a href="BasicCompanyDataAsOneFile-2026-06-01.zip">y</a>'
        )

        def raise_for_status(self):
            return None

    class _Session:
        def get(self, url, *, timeout, stream=False):
            return _Resp()

    url = resources.resolve_basic_company_data_url(session=_Session())
    assert url == tables.DOWNLOAD_BASE_URL + "BasicCompanyDataAsOneFile-2026-06-01.zip"


def test_financials_build_from_archive(tmp_path):
    import zipfile

    from dagster_v3.defs.uk_companies_house import financials
    from tests.test_xbrl_common import SAMPLE  # synthetic filing with revenue/profit

    archive = tmp_path / "accounts.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("Prod_synth_01234567.html", SAMPLE)
        zf.writestr("ignore.pdf", b"%PDF-1.4 not xbrl")
    db = tmp_path / "fin.duckdb"
    counts = financials.build_uk_financials_from_archive(
        database_path=db, archive_path=archive, source_run_id="r1", source_url="http://x.zip"
    )
    assert counts["companies"] == 1 and counts["with_revenue"] == 1
    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {tables.DLT_DATASET_NAME}.{tables.FINANCIAL_METRICS_TABLE}"
        ).fetchall()]
        assert cols == list(tables.GB_FINANCIAL_METRICS_COLUMNS)
        row = con.execute(
            f"select company_number, currency, revenue_amount_original, "
            f"net_result_amount_original, net_assets_amount_original "
            f"from {tables.DLT_DATASET_NAME}.{tables.FINANCIAL_METRICS_TABLE}"
        ).fetchone()
    assert row[0] == "01234567" and row[1] == "GBP"
    assert row[2] == 1234000  # turnover scale applied
    assert row[3] == -5000    # profit sign applied
    assert row[4] == 1495313  # net assets


def test_financials_export_columns_match_migration():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_FINANCIAL_METRICS_TABLE}"
        in FINANCIALS_MIGRATION
    )
    for column in tables.GB_FINANCIAL_METRICS_EXPORT_COLUMNS:
        assert f"    {column} " in FINANCIALS_MIGRATION, f"missing {column} in 000037"
    for metric in tables.UK_FINANCIAL_METRIC_NAMES:
        assert f"{metric}_amount_original" in tables.GB_FINANCIAL_METRICS_EXPORT_COLUMNS
        assert f"{metric}_amount_usd" in tables.GB_FINANCIAL_METRICS_EXPORT_COLUMNS


def test_api_latest_accounts_fetch_and_build(tmp_path):
    from dagster_v3.defs.uk_companies_house import documents_api
    from tests.test_xbrl_common import SAMPLE  # synthetic iXBRL with revenue/profit

    class _Resp:
        def __init__(self, *, payload=None, content=None):
            self._payload = payload
            self.content = content

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class _Session:
        def __init__(self):
            self.calls = []

        def get(self, url, *, auth=None, timeout=None, headers=None):
            self.calls.append(url)
            if "filing-history" in url:
                return _Resp(payload={"items": [
                    {"date": "2024-12-31", "category": "accounts",
                     "links": {"document_metadata": "https://doc-api/document/OLD"}},
                    {"date": "2025-12-31", "category": "accounts",
                     "links": {"document_metadata": "https://doc-api/document/NEW"}},
                ]})
            if url.endswith("/content"):
                return _Resp(content=SAMPLE.encode("utf-8"))
            if "/document/" in url:  # document metadata: offers iXBRL
                return _Resp(payload={
                    "resources": {"application/xhtml+xml": {}},
                    "links": {"document": url + "/content"},
                })
            return _Resp(payload={})

    session = _Session()
    client = documents_api.CompaniesHouseClient("KEY", session=session)
    db = tmp_path / "api.duckdb"
    counts = documents_api.build_financials_for_company_numbers(
        database_path=db, company_numbers=["01234567"], source_run_id="r1",
        client=client, request_delay_seconds=0,
    )
    assert counts["requested"] == 1 and counts["fetched"] == 1 and counts["companies"] == 1
    # newest filing's document fetched first (sorted by date desc).
    assert any(u.endswith("/document/NEW/content") for u in session.calls)
    with duckdb.connect(str(db), read_only=True) as con:
        row = con.execute(
            f"select company_number, source_slug, revenue_amount_original "
            f"from {tables.DLT_DATASET_NAME}.{tables.FINANCIAL_METRICS_TABLE}"
        ).fetchone()
    assert row[0] == "01234567"
    assert row[1] == "uk_companies_house_accounts_api"
    assert row[2] == 1234000


def test_api_missing_company_yields_empty(tmp_path):
    from dagster_v3.defs.uk_companies_house import documents_api

    class _Resp:
        content = None

        def raise_for_status(self):
            return None

        def json(self):
            return {"items": []}

    class _Session:
        def get(self, url, *, auth=None, timeout=None, headers=None):
            return _Resp()

    client = documents_api.CompaniesHouseClient("KEY", session=_Session())
    counts = documents_api.build_financials_for_company_numbers(
        database_path=tmp_path / "e.duckdb", company_numbers=["99999999"],
        source_run_id="r1", client=client, request_delay_seconds=0,
    )
    assert counts == {"companies": 0, "with_revenue": 0, "requested": 1, "fetched": 0, "missing": 1}


def test_register_job_and_schedule():
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    sch = repo.get_schedule_def("uk_companies_house_register_schedule")
    assert sch.cron_schedule == "0 7 7 * *"
    assert sch.job.name == "uk_companies_house_register_job"
    keys = {
        k.path[-1]
        for k in repo.get_job("uk_companies_house_register_job").asset_layer.executable_asset_keys
    }
    assert keys == {
        "uk_companies_house_raw_duckdb",
        "uk_companies_house_companies_duckdb",
        "uk_companies_house_clickhouse_companies",
        "uk_companies_house_industries_duckdb",
        "uk_companies_house_clickhouse_industries",
    }
