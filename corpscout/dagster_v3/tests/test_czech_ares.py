from pathlib import Path

import datetime as dt

import duckdb

from dagster_v3.defs.czech_ares import industries, resources, tables
from tests.canonical_contact_tables import (
    assert_canonical_contacts_ddl,
    assert_canonical_domains_ddl,
)

MIG_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"
COMPANIES_MIGRATION = (MIG_DIR / "000038_corpscout_cz_companies.up.sql").read_text()
INDUSTRIES_MIGRATION = (MIG_DIR / "000039_corpscout_cz_industries.up.sql").read_text()
CANONICAL_CONTACTS_MIGRATION = sorted(
    MIG_DIR.glob("*_corpscout_cz_canonical_contacts.up.sql")
)[-1].read_text()

HEADER = (
    "ICO,OKRESLAU,DDATVZN,DDATZAN,ZPZAN,DDATPAKT,FORMA,ROSFORMA,KATPO,NACE,NACE2025,"
    "ICZUJ,FIRMA,CISS2010,KODADM,TEXTADR,PSC,OBEC_TEXT,COBCE_TEXT,ULICE_TEXT,TYPCDOM,CDOM,COR,DATPLAT,PRIZNAK"
)
ROWS = [
    '00000175,CZ0100,1972-01-01,,,2026-01-15,332,332,320,6820,68200,500054,"Dipl. servis",13110,21,"",11000,Praha,Nové Město,Václavské náměstí,1,816,49,2026-06-15,',
    '27074358,CZ0100,2003-08-06,,,2026-01-15,121,121,250,6201,62010,500054,"Asseco a.s.",11001,123,"",14000,Praha,Praha 4,Budějovická,1,778,3,2026-06-15,',
    '12345678,CZ0642,2010-05-05,2020-03-15,,2026-01-15,112,112,110,4711,47110,582786,"Stará s.r.o.",11001,999,"",60200,Brno,Brno-střed,Masarykova,1,12,,2026-06-15,',
]


def _load_raw(tmp_path) -> Path:
    csv = tmp_path / "res.csv"
    csv.write_text("\n".join([HEADER, *ROWS]) + "\n")
    db = tmp_path / "cz.duckdb"
    con = duckdb.connect(str(db))
    con.execute("create schema czech_ares")
    con.execute(
        f"create table czech_ares.res_raw as "
        f"select * from read_csv('{csv}', header=true, all_varchar=true)"
    )
    con.close()
    return db


def test_stream_download_uses_plain_requests_session_with_user_agent(monkeypatch, tmp_path):
    created_sessions = []

    class FakeResponse:
        headers = {"Content-Length": "4"}
        content = b""

        def raise_for_status(self):
            return None

        def iter_content(self, *, chunk_size):
            assert chunk_size == resources.DOWNLOAD_CHUNK_BYTES
            yield b"data"

    class FakeSession:
        def __init__(self):
            self.headers = {}
            self.calls = []
            created_sessions.append(self)

        def get(self, url, *, timeout, stream=False):
            self.calls.append((url, timeout, stream))
            return FakeResponse()

    monkeypatch.setattr(resources.requests, "Session", FakeSession)

    dest = tmp_path / "res.csv"
    resources._stream_download(
        url="https://example.test/res.csv",
        dest=dest,
        timeout_seconds=17,
        session=None,
    )

    assert dest.read_bytes() == b"data"
    assert len(created_sessions) == 1
    assert created_sessions[0].headers["User-Agent"] == resources.DEFAULT_USER_AGENT
    assert created_sessions[0].calls == [("https://example.test/res.csv", 17, True)]


def test_companies_build(tmp_path):
    db = _load_raw(tmp_path)
    with duckdb.connect(str(db)) as con:
        counts = resources.build_czech_ares_companies(connection=con, source_run_id="r1")
    assert counts == {"companies": 3, "active": 2}
    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute("describe czech_ares.companies").fetchall()]
        assert cols == list(tables.CZ_COMPANIES_COLUMNS)
        rows = {r[0]: r for r in con.execute(
            "select ico, name, legal_form_en, is_active, established_date, terminated_date, "
            "address, postal_code, city from czech_ares.companies"
        ).fetchall()}
    assert rows["27074358"][1:] == (
        "Asseco a.s.", "Joint-stock company (a.s.)", True, dt.date(2003, 8, 6), None,
        "Budějovická 778/3", "14000", "Praha",
    )
    assert rows["00000175"][6] == "Václavské náměstí 816/49"
    # terminated -> is_active False + terminated_date.
    assert rows["12345678"][3] is False and rows["12345678"][5] == dt.date(2020, 3, 15)
    assert rows["12345678"][2] == "Limited liability company (s.r.o.)"


def test_industries_build_cznace_to_nace(tmp_path):
    db = _load_raw(tmp_path)
    with duckdb.connect(str(db)) as con:
        counts = industries.build_czech_ares_industries(connection=con, source_run_id="r1")
    assert counts == {"industries": 3, "nace_mapped": 3}
    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute("describe czech_ares.industries").fetchall()]
        assert cols == list(tables.CZ_INDUSTRIES_COLUMNS)
        rows = {r[0]: r for r in con.execute(
            "select ico, source_industry_code, source_industry_code_set, nace_revision, "
            "nace_code, nace_normalized_code, nace_mapping_status, is_primary "
            "from czech_ares.industries"
        ).fetchall()}
    # NACE2025 preferred -> NACE Rev 2.1, first 4 digits.
    assert rows["27074358"][1:] == ("62010", "CZ_NACE_2025", "NACE_REV_2_1", "62.01", "6201", "mapped", 1)
    assert rows["00000175"][4] == "68.20"


def test_companies_export_columns_match_migration():
    assert f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_COMPANIES_TABLE}" in COMPANIES_MIGRATION
    for column in tables.CZ_COMPANIES_EXPORT_COLUMNS:
        assert f"    {column} " in COMPANIES_MIGRATION, f"missing {column} in 000038"
    assert "raw_entity" not in tables.CZ_COMPANIES_EXPORT_COLUMNS


def test_industries_export_columns_match_migration():
    assert f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_INDUSTRIES_TABLE}" in INDUSTRIES_MIGRATION
    for column in tables.CZ_INDUSTRIES_EXPORT_COLUMNS:
        assert f"    {column} " in INDUSTRIES_MIGRATION, f"missing {column} in 000039"
    assert tables.CZ_INDUSTRIES_EXPORT_COLUMNS == tables.CZ_INDUSTRIES_COLUMNS


def test_clickhouse_candidate_batches_load_100k_company_names_after_ico():
    from dagster_v3.defs.czech_ares import contacts

    class FakeClient:
        def __init__(self):
            self.calls = []

        def execute(self, sql, params):
            self.calls.append((sql, params))
            return [("27074358", "Asseco a.s. www.asseco.cz")]

    fake = FakeClient()

    rows = contacts.load_company_contact_candidate_batch(fake, after_ico="12345678")

    assert rows == [("27074358", "Asseco a.s. www.asseco.cz")]
    sql, params = fake.calls[0]
    assert "OFFSET" not in sql.upper()
    assert "ico > %(after_ico)s" in sql
    assert params["batch_size"] == 100_000
    assert params["after_ico"] == "12345678"


def test_contacts_and_domains_conform_to_canonical_migration():
    assert_canonical_contacts_ddl(CANONICAL_CONTACTS_MIGRATION, tables.COMPANY_CONTACTS_TABLE_CH)
    assert_canonical_domains_ddl(CANONICAL_CONTACTS_MIGRATION, tables.COMPANY_DOMAINS_TABLE_CH)


def test_legal_form_map():
    assert resources.CZ_LEGAL_FORM_EN_BY_CODE["112"] == "Limited liability company (s.r.o.)"
    assert resources.CZ_LEGAL_FORM_EN_BY_CODE["121"] == "Joint-stock company (a.s.)"


def test_register_job_and_schedule():
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    sch = repo.get_schedule_def("czech_ares_register_schedule")
    assert sch.cron_schedule == "0 7 17 * *"
    assert sch.job.name == "czech_ares_register_job"
    keys = {
        k.path[-1]
        for k in repo.get_job("czech_ares_register_job").asset_layer.executable_asset_keys
    }
    assert keys == {
        "czech_ares_res_raw_duckdb",
        "czech_ares_companies_duckdb",
        "czech_ares_clickhouse_companies",
        "czech_ares_clickhouse_company_contacts",
        "czech_ares_industries_duckdb",
        "czech_ares_clickhouse_industries",
    }


def test_replace_czech_company_contacts_writes_canonical_contact_and_domain_tables(monkeypatch):
    """Orchestrator writes BOTH canonical tables (two stage/EXCHANGE sequences) and
    returns the Task 4-shared counts-dict contract."""
    from dagster_v3.defs.czech_ares import contacts

    class FakeClient:
        def __init__(self):
            self.commands: list[str] = []
            self.inserted: list[tuple] = []
            self._company_batches = [
                [
                    ("11111111", "Asseco a.s. www.asseco.cz info@asseco.cz"),
                    ("22222222", "DNS only dns-only.cz"),
                ],
                [],
            ]

        def execute(self, sql, params=None):
            stripped = sql.strip()
            self.commands.append(stripped)
            if stripped.startswith("SELECT ico, name"):
                return self._company_batches.pop(0)
            if "commoncrawl_domains" in stripped:
                return [("asseco.cz",)]
            return []

        def insert_rows(self, table, rows, *, columns, database):
            self.inserted.append((database, table, list(rows), columns))

    # asseco.cz validates via CommonCrawl (see FakeClient.execute above); dns-only.cz
    # has no CommonCrawl hit, so the orchestrator asks for its nameservers here —
    # stub it out so the test never touches real DNS.
    monkeypatch.setattr(
        contacts,
        "resolve_nameservers_concurrently",
        lambda domains: {domain: ("ns1.example.cz",) for domain in domains},
    )

    fake = FakeClient()
    counts = contacts.replace_czech_company_contacts_clickhouse(
        clickhouse_client=fake,
        resolved_at=dt.datetime(2026, 7, 4, tzinfo=dt.UTC),
    )

    assert counts.keys() == {
        "contact_facts", "domains", "primary_domains",
        "commoncrawl_validated", "dns_validated",
    }
    assert counts["contact_facts"] == 3  # www.asseco.cz, info@asseco.cz, dns-only.cz facts
    assert counts["domains"] == 2  # one row per registry_id (asseco.cz, dns-only.cz)
    assert counts["primary_domains"] == 2  # each registry_id has exactly one domain -> primary
    assert counts["commoncrawl_validated"] == 1
    assert counts["dns_validated"] == 1

    # Two stage/EXCHANGE sequences: contacts table first, then domains table.
    create_cmds = [c for c in fake.commands if c.startswith("CREATE TABLE")]
    exchange_cmds = [c for c in fake.commands if c.startswith("EXCHANGE TABLES")]
    drop_cmds = [c for c in fake.commands if c.startswith("DROP TABLE IF EXISTS")]
    assert len(create_cmds) == len(exchange_cmds) == len(drop_cmds) == 2
    assert create_cmds[0].endswith(f"AS {tables.QUALIFIED_COMPANY_CONTACTS_TABLE}")
    assert create_cmds[1].endswith(f"AS {tables.QUALIFIED_COMPANY_DOMAINS_TABLE}")
    assert tables.QUALIFIED_COMPANY_CONTACTS_TABLE in exchange_cmds[0]
    assert tables.QUALIFIED_COMPANY_DOMAINS_TABLE in exchange_cmds[1]

    # Correct qualified target tables and column lists for each write.
    assert len(fake.inserted) == 2
    contacts_database, contacts_stage_table, contact_rows, contact_columns = fake.inserted[0]
    domains_database, domains_stage_table, domain_rows, domain_columns = fake.inserted[1]
    assert contacts_database == domains_database == "corpscout"
    assert contacts_stage_table.startswith(f"_tmp_{tables.COMPANY_CONTACTS_TABLE_CH}_")
    assert domains_stage_table.startswith(f"_tmp_{tables.COMPANY_DOMAINS_TABLE_CH}_")
    assert contact_columns == contacts.COMPANY_CONTACTS_COLUMNS
    assert domain_columns == contacts.COMPANY_DOMAINS_COLUMNS
    assert len(contact_rows) == 3
    assert len(domain_rows) == 2
