from pathlib import Path

import datetime as dt

import duckdb

from dagster_v3.defs.czech_ares import industries, resources, tables

MIG_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"
COMPANIES_MIGRATION = (MIG_DIR / "000038_corpscout_cz_companies.up.sql").read_text()
INDUSTRIES_MIGRATION = (MIG_DIR / "000039_corpscout_cz_industries.up.sql").read_text()
CONTACTS_MIGRATION = (MIG_DIR / "000083_corpscout_cz_company_contacts.up.sql").read_text()

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


def test_contact_candidates_extract_domains_and_emails_from_company_name():
    from dagster_v3.defs.czech_ares import contacts

    candidates = contacts.extract_contact_candidates(
        ico="27074358",
        company_name="Asseco a.s. - www.asseco.cz, info@asseco.cz, https://portal.asseco.cz/path",
    )

    assert [candidate.contact_type for candidate in candidates] == [
        "domain",
        "email",
        "domain",
    ]
    assert [candidate.contact_value for candidate in candidates] == [
        "www.asseco.cz",
        "info@asseco.cz",
        "portal.asseco.cz",
    ]
    assert [candidate.domain for candidate in candidates] == [
        "asseco.cz",
        "asseco.cz",
        "asseco.cz",
    ]
    assert all(candidate.ico == "27074358" for candidate in candidates)


def test_contact_rows_keep_commoncrawl_and_dns_validated_domains_only():
    from dagster_v3.defs.czech_ares import contacts

    candidates_by_domain = contacts.extract_contact_candidates_by_domain(
        [
            ("27074358", "Asseco a.s. www.asseco.cz info@asseco.cz"),
            ("12345678", "DNS only dns-only.cz"),
            ("87654321", "Invalid missing.example"),
        ]
    )
    rows = list(
        contacts.iter_valid_contact_rows_from_domain_candidates(
            candidates_by_domain,
            commoncrawl_domains={"asseco.cz"},
            nameservers_by_domain={"dns-only.cz": ("ns1.dns-only.cz",)},
            source_run_id="run-1",
            resolved_at=dt.datetime(2026, 7, 3, 12, 0, tzinfo=dt.UTC),
        )
    )

    assert [row["contact_value"] for row in rows] == [
        "www.asseco.cz",
        "info@asseco.cz",
        "dns-only.cz",
    ]
    assert [row["domain_source"] for row in rows] == [
        "commoncrawl",
        "commoncrawl",
        "dns",
    ]
    assert [row["confidence"] for row in rows] == [0.95, 0.95, 0.7]
    assert all(row["domain"] in {"asseco.cz", "dns-only.cz"} for row in rows)


def test_contact_extraction_returns_domain_dictionary_before_validation():
    from dagster_v3.defs.czech_ares import contacts

    candidates_by_domain = contacts.extract_contact_candidates_by_domain(
        [
            ("27074358", "Asseco a.s. www.asseco.cz info@asseco.cz"),
            ("12345678", "Portal https://portal.example.cz/path"),
        ]
    )

    assert sorted(candidates_by_domain) == ["asseco.cz", "example.cz"]
    assert [candidate.contact_value for candidate in candidates_by_domain["asseco.cz"]] == [
        "www.asseco.cz",
        "info@asseco.cz",
    ]
    assert candidates_by_domain["example.cz"][0].contact_value == "portal.example.cz"


def test_replace_contact_table_inserts_contact_rows_in_batches(monkeypatch):
    from dagster_v3.defs.czech_ares import contacts

    monkeypatch.setattr(contacts, "CLICKHOUSE_INSERT_BATCH_SIZE", 2)

    class FakeClient:
        def __init__(self):
            self.commands = []
            self.inserted = []

        def execute(self, sql, params=None):
            self.commands.append(sql)
            return []

        def insert_rows(self, table, rows, *, columns, database):
            self.inserted.append((database, table, list(rows), columns))

    rows = (
        {
            "country_iso2": "CZ",
            "source_slug": contacts.CONTACTS_SOURCE_SLUG,
            "source_run_id": "run-1",
            "source_record_id": str(index),
            "ico": str(index),
            "company_name": f"Company {index}",
            "contact_type": "domain",
            "contact_value": f"example{index}.cz",
            "domain": f"example{index}.cz",
            "domain_source": "dns",
            "confidence": contacts.DNS_CONFIDENCE,
            "source_url": tables.RES_DATA_URL,
            "resolved_at": dt.datetime(2026, 7, 3, 12, 0, tzinfo=dt.UTC),
        }
        for index in range(3)
    )

    fake = FakeClient()
    counts = contacts._replace_contact_table(fake, rows)

    assert counts == {
        "contacts": 3,
        "domains": 3,
        "commoncrawl_validated": 0,
        "dns_validated": 3,
    }
    assert [len(inserted_rows) for _database, _table, inserted_rows, _columns in fake.inserted] == [2, 1]


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


def test_nameservers_for_domain_uses_parent_zone_authority(monkeypatch):
    from dagster_v3.defs.czech_ares import contacts

    class FakeNsAnswer:
        target = "A.NS.NIC.CZ."

    calls = []

    def fake_resolve(domain, record_type, *, lifetime):
        calls.append((domain, record_type, lifetime))
        if (domain, record_type) == ("cz", "NS"):
            return [FakeNsAnswer()]
        if (domain, record_type) == ("a.ns.nic.cz", "A"):
            return ["192.0.2.53"]
        return []

    def fake_authoritative_lookup(domain, parent_nameserver_addresses):
        assert domain == "example.cz"
        assert parent_nameserver_addresses == ("192.0.2.53",)
        return ("ns1.example.cz",)

    monkeypatch.setattr(contacts.dns.resolver, "resolve", fake_resolve)
    monkeypatch.setattr(
        contacts,
        "_resolve_domain_nameservers_from_parent",
        fake_authoritative_lookup,
    )

    assert contacts.nameservers_for_domain("example.cz") == ("ns1.example.cz",)
    assert calls == [
        ("cz", "NS", contacts.DNS_QUERY_TIMEOUT_SECONDS),
        ("a.ns.nic.cz", "A", contacts.DNS_QUERY_TIMEOUT_SECONDS),
        ("a.ns.nic.cz", "AAAA", contacts.DNS_QUERY_TIMEOUT_SECONDS),
    ]


def test_concurrent_nameserver_resolution_reuses_parent_zone_addresses(monkeypatch):
    from dagster_v3.defs.czech_ares import contacts

    parent_calls = []
    lookup_calls = []

    def fake_parent_addresses(parent_zone):
        parent_calls.append(parent_zone)
        return ("192.0.2.53",)

    def fake_authoritative_lookup(domain, parent_nameserver_addresses):
        lookup_calls.append((domain, tuple(parent_nameserver_addresses)))
        return (f"ns1.{domain}",)

    monkeypatch.setattr(contacts, "_parent_nameserver_addresses", fake_parent_addresses)
    monkeypatch.setattr(
        contacts,
        "_resolve_domain_nameservers_from_parent",
        fake_authoritative_lookup,
    )

    results = contacts._resolve_nameservers_concurrently(
        ["first.cz", "second.cz", "first.cz", "third.sk"]
    )

    assert parent_calls == ["cz", "sk"]
    assert results == {
        "first.cz": ("ns1.first.cz",),
        "second.cz": ("ns1.second.cz",),
        "third.sk": ("ns1.third.sk",),
    }
    assert sorted(lookup_calls) == [
        ("first.cz", ("192.0.2.53",)),
        ("second.cz", ("192.0.2.53",)),
        ("third.sk", ("192.0.2.53",)),
    ]


def test_authoritative_nameserver_lookup_uses_dns_resolver(monkeypatch):
    from dagster_v3.defs.czech_ares import contacts

    class FakeAnswer:
        target = "NS1.Example.CZ."

    class FakeResolver:
        instances = []

        def __init__(self, *, configure):
            assert configure is False
            self.nameservers = []
            self.timeout = None
            self.lifetime = None
            self.calls = []
            self.instances.append(self)

        def resolve(self, domain, record_type, *, lifetime):
            self.calls.append((domain, record_type, lifetime))
            return [FakeAnswer()]

    monkeypatch.setattr(contacts.dns.resolver, "Resolver", FakeResolver)

    nameservers = contacts._resolve_domain_nameservers_from_parent(
        "example.cz",
        ("192.0.2.53",),
    )

    assert nameservers == ("ns1.example.cz",)
    resolver = FakeResolver.instances[0]
    assert resolver.nameservers == ["192.0.2.53"]
    assert resolver.calls == [("example.cz", "NS", contacts.DNS_QUERY_TIMEOUT_SECONDS)]


def test_contacts_export_columns_match_migration():
    assert f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_COMPANY_CONTACTS_TABLE}" in CONTACTS_MIGRATION
    for column in tables.CZ_COMPANY_CONTACTS_EXPORT_COLUMNS:
        assert f"    {column} " in CONTACTS_MIGRATION, f"missing {column} in 000083"
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in CONTACTS_MIGRATION
    assert "ORDER BY (ico, contact_type, contact_value)" in CONTACTS_MIGRATION


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
