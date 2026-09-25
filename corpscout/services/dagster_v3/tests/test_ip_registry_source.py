"""Parsers for the IANA CSVs and RIR delegated-extended files, and the registry rule — no I/O."""

from datetime import date
from pathlib import Path

import pytest

from dagster_v3.defs.commoncrawl_rdap import registry
from dagster_v3.defs.ip_registry import source, tables

FIXTURES = Path(__file__).parent / "fixtures" / "ip_registry"


def excerpt(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_address_int_matches_clickhouse_touint128_of_toipv6():
    # ClickHouse: toUInt128(toIPv6('::ffff:1.2.3.4')) = 281470698652420 (verified on 26.5).
    assert source.address_int("1.2.3.4") == 281470698652420
    assert source.address_int("::ffff:1.2.3.4") == 281470698652420
    assert source.address_int("2600::") == 50510663839826803170344668290653093888
    assert source.address_int("103.35.64.49") - source.address_int("103.35.64.0") == 49


@pytest.mark.parametrize(
    ("designation", "rir"),
    [
        ("APNIC", "apnic"),
        ("RIPE NCC", "ripencc"),
        ("Administered by ARIN", "arin"),
        ("Administered by AFRINIC", "afrinic"),
        ("LACNIC", "lacnic"),
        ("IANA - Loopback", ""),
        ("Ford Motor Company", ""),
        ("Future use", ""),
        ("IANA", ""),
    ],
)
def test_designation_rir(designation, rir):
    assert source.designation_rir(designation) == rir


def test_parse_iana_ipv4_excerpt():
    blocks = source.parse_iana_csv(excerpt("iana-ipv4-excerpt.csv"), "iana_ipv4")
    assert len(blocks) == 23
    by_prefix = {block.prefix: block for block in blocks}
    assert set(by_prefix) == {
        f"{octet}.0.0.0/8"
        for octet in (
            0,
            1,
            2,
            5,
            8,
            14,
            19,
            23,
            27,
            38,
            41,
            45,
            85,
            100,
            101,
            102,
            103,
            104,
            111,
            113,
            195,
            240,
            255,
        )
    }
    apnic = by_prefix["103.0.0.0/8"]
    assert (
        apnic.ip_version,
        apnic.designation,
        apnic.rir,
        apnic.status,
        apnic.assigned_on,
    ) == (4, "APNIC", "apnic", "ALLOCATED", "2011-02")
    assert (apnic.first, apnic.last) == (
        source.address_int("103.0.0.0"),
        source.address_int("103.255.255.255"),
    )
    assert by_prefix["38.0.0.0/8"].designation == "PSINet, Inc."
    assert (
        by_prefix["8.0.0.0/8"].rir == "arin"
        and by_prefix["8.0.0.0/8"].status == "LEGACY"
    )
    assert (
        by_prefix["45.0.0.0/8"].rir == "arin"
        and by_prefix["45.0.0.0/8"].status == "LEGACY"
    )
    assert (
        by_prefix["19.0.0.0/8"].rir == "" and by_prefix["19.0.0.0/8"].status == "LEGACY"
    )
    assert (
        by_prefix["240.0.0.0/8"].rir == ""
        and by_prefix["240.0.0.0/8"].status == "RESERVED"
    )
    assert by_prefix["100.0.0.0/8"].note == "[6]"
    assert (
        by_prefix["8.0.0.0/8"].rdap
        == "https://rdap.arin.net/registryhttp://rdap.arin.net/registry"
    )
    assert sum(1 for block in blocks if block.rir) == 18


def test_parse_iana_ipv6_excerpt_handles_multiline_notes():
    blocks = source.parse_iana_csv(excerpt("iana-ipv6-excerpt.csv"), "iana_ipv6")
    assert [block.prefix for block in blocks] == [
        "2001::/23",
        "2001:200::/23",
        "2001:600::/23",
        "2001:1200::/23",
        "2001:2000::/19",
        "2001:4800::/23",
        "2600::/12",
        "2a00::/12",
        "2a10::/12",
        "2d00::/8",
    ]
    arin = blocks[6]
    assert (arin.rir, arin.status, arin.ip_version) == ("arin", "ALLOCATED", 6)
    assert (
        arin.note.startswith("2600::/22, 2604::/22")
        and "recent allocation (2006-10-03)" in arin.note
    )
    assert (arin.first, arin.last) == (
        source.address_int("2600::"),
        source.address_int("260f:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
    )
    assert blocks[0].rir == "" and blocks[0].designation == "IANA"
    assert blocks[-1].status == "RESERVED"
    assert sum(1 for block in blocks if block.rir) == 8


@pytest.mark.parametrize(
    "text",
    [
        "Prefix,Designation,Date,WHOIS,RDAP,Status,Note\n103/8,APNIC,2011-02,,,PENDING,\n",
        "Prefix,Designation,Date,Status,Note\n103/8,APNIC,2011-02,ALLOCATED,\n",
        "Prefix,Designation,Date,WHOIS,RDAP,Status,Note\n103/8,APNIC\n",
    ],
)
def test_parse_iana_rejects_unknown_status_columns_or_short_rows(text):
    with pytest.raises(ValueError):
        source.parse_iana_csv(text, "iana_ipv4")


def test_parse_md5_accepts_bsd_and_gnu_formats():
    assert source.parse_md5(
        "MD5 (delegated-ripencc-extended-latest) = 0ef1edc28a8a2bbf9b7f02e5be7da195\n"
    ) == ("0ef1edc28a8a2bbf9b7f02e5be7da195")
    assert source.parse_md5(
        "f3c86ced5a55b1505da587ec785a66b6  delegated-arin-extended-20260925\n"
    ) == ("f3c86ced5a55b1505da587ec785a66b6")
    with pytest.raises(ValueError):
        source.parse_md5("<html>moved</html>")


def test_parse_delegated_ripencc_excerpt_counts_the_whole_file_but_keeps_only_special_rows():
    parsed = source.parse_delegated(
        excerpt("delegated-ripencc-extended-excerpt"), "ripencc"
    )
    header = parsed.header
    assert (header.version, header.registry, header.serial, header.records) == (
        "2",
        "ripencc",
        "1790287199",
        11,
    )
    assert (header.start_date, header.end_date, header.utc_offset) == (
        "19700101",
        date(2026, 9, 24),
        "+0200",
    )
    assert parsed.summaries == {"ipv4": 8, "asn": 0, "ipv6": 3}
    assert parsed.record_lines == 11
    assert [(segment.start_address, segment.status) for segment in parsed.special] == [
        ("85.8.248.0", "available"),
        ("5.134.16.0", "reserved"),
    ]
    assert (parsed.special_count(4), parsed.special_count(6)) == (2, 0)
    available = parsed.special[0]  # seven-field line
    assert (
        available.registry,
        available.cc,
        available.ip_version,
        available.value,
    ) == ("ripencc", "", 4, 2048)
    assert available.cidrs == ("85.8.248.0/21",)
    assert (available.first, available.last) == (
        source.address_int("85.8.248.0"),
        source.address_int("85.8.255.255"),
    )


def test_parse_delegated_other_registries():
    apnic = source.parse_delegated(excerpt("delegated-apnic-extended-excerpt"), "apnic")
    assert apnic.header.start_date == "" and apnic.header.end_date == date(2026, 9, 25)
    assert {segment.start_address for segment in apnic.special} == {
        "14.102.240.0",
        "27.0.8.0",
    }
    arin = source.parse_delegated(excerpt("delegated-arin-extended-excerpt"), "arin")
    assert (
        arin.header.serial == "1790341220831"
        and arin.record_lines == 9
        and arin.summaries["asn"] == 2
    )
    [reserved] = arin.special  # 768 addresses are not a power of two: two CIDRs
    assert (reserved.cc, reserved.status, reserved.cidrs) == (
        "",
        "reserved",
        ("23.128.1.0/24", "23.128.2.0/23"),
    )
    assert (reserved.first, reserved.last) == (
        source.address_int("23.128.1.0"),
        source.address_int("23.128.3.255"),
    )
    lacnic = source.parse_delegated(
        excerpt("delegated-lacnic-extended-excerpt"), "lacnic"
    )
    assert [(s.ip_version, s.status) for s in lacnic.special] == [
        (4, "reserved"),
        (6, "available"),
        (6, "available"),
    ]
    v6 = lacnic.special[1]
    assert (v6.value, v6.cidrs) == (43, ("2001:1201:20::/43",))
    assert v6.last == source.address_int("2001:1201:3f:ffff:ffff:ffff:ffff:ffff")
    afrinic = source.parse_delegated(
        excerpt("delegated-afrinic-extended-excerpt"), "afrinic"
    )
    assert afrinic.header.start_date == "00000000"
    big = next(s for s in afrinic.special if s.start_address == "102.192.0.0")
    assert (big.cc, big.status, big.cidrs) == ("ZZ", "available", ("102.192.0.0/13",))


@pytest.mark.parametrize(
    ("registry_name", "mutate", "message"),
    [
        ("ripencc", lambda t: t.replace("|11|", "|12|", 1), "announces 12 records"),
        (
            "ripencc",
            lambda t: t.replace("ipv4|*|8|summary", "ipv4|*|7|summary"),
            "ipv4 summary 7",
        ),
        ("apnic", lambda t: t, "belongs to 'ripencc'"),
        (
            "ripencc",
            lambda t: t.replace("|allocated|172ce676", "|pending|172ce676"),
            "status 'pending'",
        ),
        (
            "ripencc",
            lambda t: t.replace("2|ripencc|", "ripencc|", 1),
            "not a version line",
        ),
        ("ripencc", lambda t: t.replace("|85.8.248.0|", "|85.8.248|"), "85.8.248"),
    ],
)
def test_parse_delegated_refuses_inconsistent_files(registry_name, mutate, message):
    # The RIPE excerpt parsed as another registry must be refused as a foreign file.
    text = mutate(excerpt("delegated-ripencc-extended-excerpt"))
    with pytest.raises(ValueError, match=message):
        source.parse_delegated(text, registry_name)


def context(*, ready=True, covered=0, iana=None, special=None):
    return registry.RegistryContext(
        ready=ready, covered_rir_blocks=covered, iana=iana, special=special
    )


IANA_APNIC = registry.IanaCoverage("APNIC", "apnic", "ALLOCATED")
IANA_ARIN_LEGACY = registry.IanaCoverage("Administered by ARIN", "arin", "LEGACY")
IANA_FORD = registry.IanaCoverage("Ford Motor Company", "", "LEGACY")
IANA_IANA = registry.IanaCoverage("IANA", "", "ALLOCATED")
IANA_FUTURE = registry.IanaCoverage("Future use", "", "RESERVED")
RESERVED = registry.SpecialCoverage(
    "lacnic",
    "reserved",
    source.address_int("45.68.105.0"),
    source.address_int("45.68.105.255"),
)
AVAILABLE = registry.SpecialCoverage(
    "ripencc",
    "available",
    source.address_int("85.8.248.0"),
    source.address_int("85.8.255.255"),
)


@pytest.mark.parametrize(
    ("label", "ctx", "expected"),
    [
        (
            "reference data not loaded",
            context(ready=False, covered=1, iana=IANA_APNIC),
            "unknown",
        ),
        (
            "covers one RIR block (APNIC-AP 103/8)",
            context(covered=1, iana=IANA_APNIC),
            "registry_level",
        ),
        (
            "covers two RIR blocks (2a00::/11)",
            context(
                covered=2,
                iana=registry.IanaCoverage("RIPE NCC", "ripencc", "ALLOCATED"),
            ),
            "registry_level",
        ),
        (
            "covers a block although its first address is reserved",
            context(covered=1, iana=IANA_ARIN_LEGACY, special=RESERVED),
            "registry_level",
        ),
        (
            "first address in a reserved range (LACNIC UNALLOCATED)",
            context(iana=IANA_ARIN_LEGACY, special=RESERVED),
            "unallocated",
        ),
        (
            "first address in an available range",
            context(
                iana=registry.IanaCoverage("RIPE NCC", "ripencc", "ALLOCATED"),
                special=AVAILABLE,
            ),
            "unallocated",
        ),
        ("no IANA block at all", context(iana=None), "unallocated"),
        ("IANA reserved block (240/8)", context(iana=IANA_FUTURE), "unallocated"),
        ("legacy single-holder block (Ford 19/8)", context(iana=IANA_FORD), "reusable"),
        (
            "IANA-designated but allocated block (2001::/23)",
            context(iana=IANA_IANA),
            "reusable",
        ),
        (
            "holder registration (FPT, Cloudflare, Google)",
            context(iana=IANA_APNIC),
            "reusable",
        ),
    ],
)
def test_registry_class_rule(label, ctx, expected):
    assert registry.registry_class(ctx) == expected, label


def test_registry_class_sql_mirrors_the_python_rule_text():
    for fragment in (
        "NOT ready, 'unknown'",
        "covered_rir_blocks > 0, 'registry_level'",
        "special.2 IN ('available', 'reserved') OR iana.3 IN ('', 'RESERVED'), 'unallocated'",
        "'reusable') AS registry_class",
    ):
        assert fragment in registry.REGISTRY_CLASS_SQL
    assert registry.UNALLOCATED_STATUSES == ("available", "reserved")
    assert (
        "dictGetOrDefault('corpscout.ip_registry_special_trie'"
        in registry.REGISTRY_CONTEXT_SQL
    )
    assert (
        registry.REGISTRY_CONTEXT_SQL.count("%(first)s") == 4
        and registry.REGISTRY_CONTEXT_SQL.count("%(last)s") == 1
    )
    assert registry.REGISTRY_CLASS_INSERT_SQL.startswith(
        "INSERT INTO corpscout.rdap_network_registry_class (network_key, registry_class,"
    )
    assert "WHERE registry_class != 'unknown'" in registry.REGISTRY_CLASS_REFRESH_SQL
    assert registry.mapped_address("1.2.3.4") in ("::ffff:1.2.3.4", "::ffff:102:304")
    assert registry.mapped_address("2600::") == "2600::"


def test_fixture_urls_are_the_published_ones():
    assert (
        tables.IANA_SOURCES["iana_ipv4"]
        == "https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.csv"
    )
    assert (
        tables.RIR_SOURCES["ripencc"]
        == "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest"
    )
    assert tables.SOURCES == (
        "iana_ipv4",
        "iana_ipv6",
        "afrinic",
        "apnic",
        "arin",
        "lacnic",
        "ripencc",
    )
    assert tables.SNAPSHOTS_KEPT == 2
