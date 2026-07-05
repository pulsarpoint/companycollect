import datetime as dt

from dagster_v3 import contact_extraction
from dagster_v3.contact_extraction import (
    DNS_CONFIDENCE,
    ContactCandidate,
    commoncrawl_domains,
    extract_contact_candidates,
    extract_contact_candidates_by_domain,
    idna_ascii,
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


def test_concurrent_nameserver_resolution_encodes_idna_before_dns_keyed_by_unicode(monkeypatch):
    # Mirrors test_nameservers_for_domain_idna_encodes_before_dns, but for the
    # concurrent resolver used by the production orchestrators: it must submit the
    # idna (punycode) form for DNS resolution, not rely on dnspython's implicit
    # IDNA2003 encoding, while the returned dict stays keyed by the unicode domain
    # callers use to key their candidates.
    zone_calls = []

    def fake_parent_zone(domain):
        zone_calls.append(domain)
        return "lv"

    monkeypatch.setattr(contact_extraction, "_parent_zone_for_domain", fake_parent_zone)
    monkeypatch.setattr(
        contact_extraction,
        "_parent_nameserver_addresses",
        lambda parent_zone: ("192.0.2.53",),
    )

    resolve_calls = []

    def fake_resolve(domain, parent_nameserver_addresses):
        resolve_calls.append(domain)
        return (f"ns1.{domain}",)

    monkeypatch.setattr(
        contact_extraction,
        "_resolve_domain_nameservers_from_parent",
        fake_resolve,
    )

    results = contact_extraction.resolve_nameservers_concurrently(["metinājumi.lv"])

    assert zone_calls == ["xn--metinjumi-9bb.lv"]
    assert resolve_calls == ["xn--metinjumi-9bb.lv"]
    assert results == {"metinājumi.lv": ("ns1.xn--metinjumi-9bb.lv",)}


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


def test_dotted_initials_abbreviation_yields_no_candidates():
    # "V.J.V.Ltd" parses as a domain (v.ltd is a real TLD) but is really initials +
    # legal form; >=2 single-char labels in the raw match kills it.
    assert extract_contact_candidates(record_id="1", text="V.J.V.Ltd") == []


def test_legal_form_only_abbreviation_yields_no_candidates():
    # "CO.LTD" would otherwise resolve fine (co.ltd is a real domain shape), but every
    # label is a bare legal-form abbreviation.
    assert extract_contact_candidates(record_id="1", text="NADA TRADING CO.LTD.") == []


def test_legal_form_registrable_domain_rejected_even_with_real_subdomain_label():
    # Found live in cz_company_contacts: "HD VinSe.co.ltd,s.r.o." matches host
    # "vinse.co.ltd", whose labels are NOT all legal-form — but the ATTRIBUTED
    # registrable domain is co.ltd, pure legal-form abbreviation junk.
    assert extract_contact_candidates(record_id="1", text="HD VinSe.co.ltd,s.r.o.") == []


def test_single_char_label_on_non_home_tld_yields_no_candidates():
    # "a.group" is a syntactically valid domain (.group is a real gTLD), but a
    # single-character label under a non-home TLD is almost always an abbreviation
    # ("A" as in "A. Group" / "A.Company"), not a real company domain.
    assert extract_contact_candidates(record_id="1", text="A.Group") == []


def test_multi_char_first_label_survives_the_abbreviation_guard():
    for text, home_tlds in (
        ("24dressup.lv", frozenset({"lv"})),
        ("la.lv", frozenset()),
        ("o2.cz", frozenset({"cz"})),
        ("dvm.co", frozenset()),  # 2-char first label allowed; only 1-char is rejected
    ):
        candidates = extract_contact_candidates(record_id="1", text=text, home_tlds=home_tlds)
        assert [c.contact_value for c in candidates] == [text.lower()], text


def test_email_local_part_initials_are_not_penalized():
    # The initials clause only looks at the matched DOMAIN host, never the email's
    # local part, so a legitimate initials-based email address survives.
    candidates = extract_contact_candidates(
        record_id="1", text="kontakt: j.k.novak@asseco.cz"
    )
    assert [(c.contact_type, c.contact_value, c.domain) for c in candidates] == [
        ("email", "j.k.novak@asseco.cz", "asseco.cz")
    ]


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


def test_pinned_dns_servers_parses_env(monkeypatch):
    monkeypatch.delenv("CONTACT_EXTRACTION_DNS_SERVERS", raising=False)
    assert contact_extraction.pinned_dns_servers() == ()
    monkeypatch.setenv("CONTACT_EXTRACTION_DNS_SERVERS", "100.64.0.53, 100.64.0.54,")
    assert contact_extraction.pinned_dns_servers() == ("100.64.0.53", "100.64.0.54")


def test_pinned_mode_stub_queries_only_pinned_servers(monkeypatch):
    monkeypatch.setenv("CONTACT_EXTRACTION_DNS_SERVERS", "100.64.0.53")
    seen = {}

    class _FakeStubResolver:
        def __init__(self, servers):
            seen["servers"] = tuple(servers)

        def resolve(self, domain, record_type):
            seen.setdefault("queries", []).append((domain, record_type))
            if record_type == "NS":
                return ["ns1.pinned.example.", "ns2.pinned.example."]
            raise AssertionError("SOA should not be queried when NS answers")

    monkeypatch.setattr(contact_extraction, "_stub_resolver", _FakeStubResolver)
    # Parent-walk internals must never be touched in pinned mode.
    monkeypatch.setattr(
        contact_extraction,
        "_parent_nameserver_addresses",
        lambda parent_zone: (_ for _ in ()).throw(AssertionError("parent walk used")),
    )

    results = contact_extraction.resolve_nameservers_concurrently(["metinājumi.lv"])
    assert results == {"metinājumi.lv": ("ns1.pinned.example", "ns2.pinned.example")}
    assert seen["servers"] == ("100.64.0.53",)
    # The wire query uses the IDNA2008 form even in pinned mode.
    assert seen["queries"] == [("xn--metinjumi-9bb.lv", "NS")]


def test_pinned_mode_falls_back_to_soa_then_empty(monkeypatch):
    monkeypatch.setenv("CONTACT_EXTRACTION_DNS_SERVERS", "100.64.0.53")

    class _SoaAnswer:
        mname = "ns.master.example."

    class _FakeStubResolver:
        def __init__(self, servers):
            pass

        def resolve(self, domain, record_type):
            if domain == "soa-only.lv":
                if record_type == "NS":
                    raise contact_extraction.dns.resolver.NoAnswer()
                return [_SoaAnswer()]
            raise contact_extraction.dns.resolver.NXDOMAIN()

    monkeypatch.setattr(contact_extraction, "_stub_resolver", _FakeStubResolver)
    assert contact_extraction.nameservers_for_domain("soa-only.lv") == ("ns.master.example",)
    assert contact_extraction.nameservers_for_domain("gone.lv") == ()


def test_vocabularies_are_closed_sets():
    assert contact_extraction.CONTACT_TYPE_VALUES == {
        "email", "phone", "mobile", "fax", "website", "domain_in_name", "other"
    }
    assert contact_extraction.DOMAIN_SOURCE_VALUES == {"website", "email", "name_embedded"}
    assert contact_extraction.VALIDATION_METHOD_VALUES == {"", "commoncrawl", "dns"}
    assert contact_extraction.WEBSITE_CONFIDENCE == 1.0
    assert contact_extraction.EMAIL_UNIQUE_CONFIDENCE == 0.9


def test_shared_denylist_is_single_source_of_truth():
    # Both Brazil (Phase C) and Estonia (Phase B) import the shared denylist and
    # max-companies threshold directly now -- identity, not equality/subset.
    from dagster_v3.defs.brazil_companies.rfb import contacts as br
    from dagster_v3.defs.estonia_ar import contacts as ee

    assert ee.EMAIL_PROVIDER_DENYLIST is contact_extraction.EMAIL_PROVIDER_DENYLIST
    assert br.EMAIL_PROVIDER_DENYLIST is contact_extraction.EMAIL_PROVIDER_DENYLIST
    assert ee.EMAIL_DOMAIN_MAX_COMPANIES == contact_extraction.EMAIL_DOMAIN_MAX_COMPANIES
    assert br.EMAIL_DOMAIN_MAX_COMPANIES == contact_extraction.EMAIL_DOMAIN_MAX_COMPANIES


def test_title_labels_guard_rejects_academic_title_domains():
    for text in ("Dr.Ing. Jan Novák", "Josef Svoboda, dipl.Ing.", "EUR.ING Karel Dvořák"):
        assert extract_contact_candidates(record_id="1", text=text, home_tlds=frozenset({"cz"})) == [], text


def test_title_labels_guard_keeps_brandlike_ing_domains():
    # all-labels rule: only fires when EVERY label is a title token.
    kept = extract_contact_candidates(record_id="1", text="boe.ing", home_tlds=frozenset())
    assert [c.domain for c in kept] == ["boe.ing"]
    kept = extract_contact_candidates(record_id="1", text="ing.cz s.r.o.", home_tlds=frozenset({"cz"}))
    assert [c.domain for c in kept] == ["ing.cz"]


def _fact_kwargs():
    return dict(country_iso2="CZ", source_slug="czech_ares", source_field="name",
                resolved_at=dt.datetime(2026, 7, 4, tzinfo=dt.UTC))


def test_contact_fact_rows_cover_all_candidates_regardless_of_validation():
    candidates = {
        "asseco.cz": [
            ContactCandidate("123", "email", "info@asseco.cz", "asseco.cz"),
            ContactCandidate("123", "domain", "www.asseco.cz", "asseco.cz"),
        ],
        "never-validates.cz": [
            ContactCandidate("456", "domain", "never-validates.cz", "never-validates.cz"),
        ],
    }
    rows = list(contact_extraction.iter_contact_fact_rows(candidates, **_fact_kwargs()))
    assert len(rows) == 3  # facts are validation-independent
    by_value = {row[7]: row for row in rows}
    email = by_value["info@asseco.cz"]
    assert email[:7] == ("CZ", "czech_ares", "", "123", "123", "email", "")
    assert email[8:12] == ("name", 1, None, "")
    domain_fact = by_value["www.asseco.cz"]
    assert domain_fact[5] == "domain_in_name"
    assert len(email) == len(contact_extraction.COMPANY_CONTACTS_COLUMNS)


def test_company_domain_rows_validated_only_and_deduped_per_registry_domain():
    candidates = {
        "asseco.cz": [
            ContactCandidate("123", "email", "info@asseco.cz", "asseco.cz"),
            ContactCandidate("123", "domain", "www.asseco.cz", "asseco.cz"),
        ],
        "dnsonly.cz": [ContactCandidate("456", "domain", "dnsonly.cz", "dnsonly.cz")],
        "dead.cz": [ContactCandidate("789", "domain", "dead.cz", "dead.cz")],
    }
    rows = list(contact_extraction.iter_company_domain_rows(
        candidates,
        commoncrawl_domains={"asseco.cz"},
        nameservers_by_domain={"dnsonly.cz": ("ns1.x.cz",), "dead.cz": ()},
        country_iso2="CZ", source_slug="czech_ares",
        resolved_at=dt.datetime(2026, 7, 4, tzinfo=dt.UTC),
    ))
    assert len(rows) == 2  # asseco deduped to ONE row despite two candidates; dead dropped
    by_domain = {row[5]: row for row in rows}
    cc = by_domain["asseco.cz"]
    assert cc[6:9] == ("name_embedded", "commoncrawl", contact_extraction.COMMONCRAWL_CONFIDENCE)
    assert cc[9:12] == ("", "", "")   # no website columns for name-embedded
    assert cc[12:14] == (1, 0)        # is_current=1, is_primary decided by election
    dns = by_domain["dnsonly.cz"]
    assert dns[7:9] == ("dns", contact_extraction.DNS_CONFIDENCE)
    assert len(cc) == len(contact_extraction.COMPANY_DOMAINS_COLUMNS)


def test_elect_primary_domains_one_winner_per_registry():
    def row(registry, domain, source, confidence, current=1):
        return ("CZ", "s", "", registry, registry, domain, source, "commoncrawl",
                confidence, "", "", "", current, 0,
                dt.datetime(2026, 7, 4, tzinfo=dt.UTC))

    rows = [
        row("1", "bbb.cz", "name_embedded", 0.95),
        row("1", "aa.cz", "name_embedded", 0.95),    # shorter domain wins at equal confidence
        row("2", "low.cz", "name_embedded", 0.70),
        row("2", "site.cz", "website", 0.70),        # website source beats higher-ranked others
        row("3", "only.cz", "name_embedded", 0.70),
    ]
    elected = contact_extraction.elect_primary_domains(rows)
    primaries = {r[4]: r[5] for r in elected if r[13] == 1}
    assert primaries == {"1": "aa.cz", "2": "site.cz", "3": "only.cz"}
    assert sum(1 for r in elected if r[13] == 1) == 3
    assert len(elected) == len(rows)  # non-winners kept with is_primary=0
