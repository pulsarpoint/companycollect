import datetime as dt

from dagster_v3 import contact_extraction
from dagster_v3.contact_extraction import (
    DNS_CONFIDENCE,
    commoncrawl_domains,
    extract_contact_candidates,
    extract_contact_candidates_by_domain,
    idna_ascii,
    iter_valid_contact_rows,
    nameservers_for_domain,
    replace_contact_table,
)


def test_contact_candidates_extract_domains_and_emails_from_company_name():
    candidates = extract_contact_candidates(
        record_id="27074358",
        text="Asseco a.s. - www.asseco.cz, info@asseco.cz, https://portal.asseco.cz/path",
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
    assert all(candidate.record_id == "27074358" for candidate in candidates)


def test_contact_rows_keep_commoncrawl_and_dns_validated_domains_only():
    candidates_by_domain = extract_contact_candidates_by_domain(
        [
            ("27074358", "Asseco a.s. www.asseco.cz info@asseco.cz"),
            ("12345678", "DNS only dns-only.cz"),
            ("87654321", "Invalid missing.example"),
        ]
    )
    rows = list(
        iter_valid_contact_rows(
            candidates_by_domain,
            commoncrawl_domains={"asseco.cz"},
            nameservers_by_domain={"dns-only.cz": ("ns1.dns-only.cz",)},
            source_slug="test_contact_extraction",
            resolved_at=dt.datetime(2026, 7, 3, 12, 0, tzinfo=dt.UTC),
        )
    )

    # Tuple layout: (source_slug, source_record_id, record_id, contact_type,
    # contact_value, domain, domain_source, confidence, resolved_at).
    assert [row[4] for row in rows] == ["www.asseco.cz", "info@asseco.cz", "dns-only.cz"]
    assert [row[6] for row in rows] == ["commoncrawl", "commoncrawl", "dns"]
    assert [row[7] for row in rows] == [0.95, 0.95, 0.7]
    assert all(row[5] in {"asseco.cz", "dns-only.cz"} for row in rows)
    assert all(row[0] == "test_contact_extraction" for row in rows)
    assert all(row[2] == row[1] for row in rows)  # record_id == source_record_id
    # A 9-tuple has no room for legacy per-country fields (country_iso2,
    # source_run_id, company_name, source_url) — structurally guaranteed here.
    assert all(len(row) == 9 for row in rows)


def test_contact_extraction_returns_domain_dictionary_before_validation():
    candidates_by_domain = extract_contact_candidates_by_domain(
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


def test_replace_contact_table_inserts_contact_rows_in_batches():
    class FakeClient:
        def __init__(self):
            self.commands = []
            self.inserted = []

        def execute(self, sql, params=None):
            self.commands.append(sql)
            return []

        def insert_rows(self, table, rows, *, columns, database):
            self.inserted.append((database, table, list(rows), columns))

    columns = (
        "source_slug",
        "source_record_id",
        "record_id",
        "contact_type",
        "contact_value",
        "domain",
        "domain_source",
        "confidence",
        "resolved_at",
    )
    rows = (
        (
            "test_contact_extraction",
            str(index),
            str(index),
            "domain",
            f"example{index}.cz",
            f"example{index}.cz",
            "dns",
            DNS_CONFIDENCE,
            dt.datetime(2026, 7, 3, 12, 0, tzinfo=dt.UTC),
        )
        for index in range(3)
    )

    fake = FakeClient()
    # batch_size is now an explicit parameter (per the shared interface) rather than
    # a module constant read at call time, so we pass it directly instead of
    # monkeypatching CLICKHOUSE_INSERT_BATCH_SIZE (which wouldn't affect the
    # already-bound default anyway).
    written = replace_contact_table(
        fake,
        qualified_table="corpscout.test_company_contacts",
        columns=columns,
        rows=rows,
        batch_size=2,
    )

    assert written == 3
    assert [len(inserted_rows) for _database, _table, inserted_rows, _columns in fake.inserted] == [
        2,
        1,
    ]
    assert any(cmd.strip().startswith("CREATE TABLE") for cmd in fake.commands)
    assert any(cmd.strip().startswith("EXCHANGE TABLES") for cmd in fake.commands)
    assert any(cmd.strip().startswith("DROP TABLE IF EXISTS") for cmd in fake.commands)


def test_nameservers_for_domain_uses_parent_zone_authority(monkeypatch):
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

    monkeypatch.setattr(contact_extraction.dns.resolver, "resolve", fake_resolve)
    monkeypatch.setattr(
        contact_extraction,
        "_resolve_domain_nameservers_from_parent",
        fake_authoritative_lookup,
    )

    assert nameservers_for_domain("example.cz") == ("ns1.example.cz",)
    assert calls == [
        ("cz", "NS", contact_extraction.DNS_QUERY_TIMEOUT_SECONDS),
        ("a.ns.nic.cz", "A", contact_extraction.DNS_QUERY_TIMEOUT_SECONDS),
        ("a.ns.nic.cz", "AAAA", contact_extraction.DNS_QUERY_TIMEOUT_SECONDS),
    ]


def test_concurrent_nameserver_resolution_reuses_parent_zone_addresses(monkeypatch):
    parent_calls = []
    lookup_calls = []

    def fake_parent_addresses(parent_zone):
        parent_calls.append(parent_zone)
        return ("192.0.2.53",)

    def fake_authoritative_lookup(domain, parent_nameserver_addresses):
        lookup_calls.append((domain, tuple(parent_nameserver_addresses)))
        return (f"ns1.{domain}",)

    monkeypatch.setattr(contact_extraction, "_parent_nameserver_addresses", fake_parent_addresses)
    monkeypatch.setattr(
        contact_extraction,
        "_resolve_domain_nameservers_from_parent",
        fake_authoritative_lookup,
    )

    results = contact_extraction.resolve_nameservers_concurrently(
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

    monkeypatch.setattr(contact_extraction.dns.resolver, "Resolver", FakeResolver)

    nameservers = contact_extraction._resolve_domain_nameservers_from_parent(
        "example.cz",
        ("192.0.2.53",),
    )

    assert nameservers == ("ns1.example.cz",)
    resolver = FakeResolver.instances[0]
    assert resolver.nameservers == ["192.0.2.53"]
    assert resolver.calls == [("example.cz", "NS", contact_extraction.DNS_QUERY_TIMEOUT_SECONDS)]


def test_idn_domain_extracts_and_normalizes_lowercase_unicode():
    candidates = extract_contact_candidates(
        record_id="40003xxxxx",
        text='Sabiedrība ar ierobežotu atbildību "Metinājumi.lv"',
    )
    assert [(c.contact_type, c.contact_value) for c in candidates] == [
        ("domain", "metinājumi.lv")
    ]
    assert candidates[0].domain == "metinājumi.lv"


def test_ascii_extraction_unchanged_by_idn_extension():
    # Byte-compatibility with the pre-IDN Czech regex for ASCII inputs. Domain
    # candidates are sorted by match position, and "www.asseco.cz" starts before
    # "info@asseco.cz" in the source text, so domain comes first (verified against
    # the original czech_ares.contacts implementation directly).
    candidates = extract_contact_candidates(
        record_id="123", text="Asseco a.s. - www.asseco.cz, info@asseco.cz"
    )
    assert [(c.contact_type, c.contact_value) for c in candidates] == [
        ("domain", "www.asseco.cz"),
        ("email", "info@asseco.cz"),
    ]
    assert {c.domain for c in candidates} == {"asseco.cz"}


def test_idna_ascii_encodes_idn_and_passes_ascii_through():
    # "metinājumi.lv" -> punycode verified via idna==3.18 (and Python's built-in
    # "idna" codec, and the raw punycode round-trip): xn--metinjumi-9bb.lv.
    assert idna_ascii("metinājumi.lv") == "xn--metinjumi-9bb.lv"
    assert idna_ascii("example.com") == "example.com"
    assert idna_ascii("") is None


def test_nameservers_for_domain_idna_encodes_before_dns(monkeypatch):
    seen = {}

    # Adapted from the brief: stub out the parent-zone/parent-nameserver lookups too
    # (not just _resolve_domain_nameservers_from_parent) so this test exercises only
    # the idna-encoding step and never touches the real network/DNS.
    monkeypatch.setattr(contact_extraction, "_parent_zone_for_domain", lambda domain: "lv")
    monkeypatch.setattr(
        contact_extraction,
        "_parent_nameserver_addresses",
        lambda parent_zone: ("192.0.2.53",),
    )

    def fake_resolve(domain, parent_nameserver_addresses):
        seen["domain"] = domain
        return ("ns1.example.com.",)

    monkeypatch.setattr(
        contact_extraction,
        "_resolve_domain_nameservers_from_parent",
        fake_resolve,
    )

    nameservers_for_domain("metinājumi.lv")
    assert seen["domain"] == "xn--metinjumi-9bb.lv"


def test_commoncrawl_lookup_tries_unicode_and_idna_forms():
    class _FakeClient:
        def __init__(self):
            self.queries = []

        def execute(self, sql, params=None):
            self.queries.append((sql, params))
            return [("xn--metinjumi-9bb.lv",)]

    client = _FakeClient()
    found = commoncrawl_domains(client, ["metinājumi.lv"])
    assert found == {"metinājumi.lv"}  # hit on the idna form counts for the unicode domain
    queried = str(client.queries)
    assert "xn--metinjumi-9bb.lv" in queried
