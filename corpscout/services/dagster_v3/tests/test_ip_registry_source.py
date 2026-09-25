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
    assert len(blocks) == 27
    by_prefix = {block.prefix: block for block in blocks}
    assert set(by_prefix) == {
        f"{octet}.0.0.0/8"
        for octet in (
            0,
            1,
            2,
            5,
            7,
            8,
            14,
            19,
            23,
            25,
            27,
            38,
            41,
            45,
            73,
            85,
            100,
            101,
            102,
            103,
            104,
            111,
            113,
            133,
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
    assert sum(1 for block in blocks if block.rir) == 22


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
        "Prefix,Designation,Date,WHOIS,RDAP,Status,Note\n103/8,APNIC,2011-02,,,ALLOCATED,,extra\n",
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
        13,
    )
    assert (header.start_date, header.end_date, header.utc_offset) == (
        "19700101",
        date(2026, 9, 24),
        "+0200",
    )
    assert parsed.summaries == {"ipv4": 9, "asn": 0, "ipv6": 4}
    assert parsed.record_lines == 13
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
        and arin.record_lines == 11
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
        ("ripencc", lambda t: t.replace("|13|", "|14|", 1), "announces 14 records"),
        (
            "ripencc",
            lambda t: t.replace("ipv4|*|9|summary", "ipv4|*|8|summary"),
            "ipv4 summary 8",
        ),
        ("apnic", lambda t: t, "belongs to 'ripencc'"),
        (
            "ripencc",
            lambda t: t.replace("ripencc|SE|ipv4|2.0.0.0|", "arin|SE|ipv4|2.0.0.0|"),
            "line 5 belongs to 'arin'",
        ),
        (
            "ripencc",
            lambda t: t.replace("ripencc|*|asn|*|0|summary", "arin|*|asn|*|0|summary"),
            "summary line 3 belongs to 'arin'",
        ),
        (
            "ripencc",
            lambda t: t.replace(
                "ripencc|*|asn|*|0|summary",
                "ripencc|*|asn|*|0|summary\nripencc|*|asn|*|0|summary",
            ),
            "duplicate asn summary",
        ),
        (
            "ripencc",
            lambda t: t.replace("ripencc|*|asn|*|0|summary\n", ""),
            "no asn summary line",
        ),
        (
            "ripencc",
            lambda t: t.replace(
                "ripencc|*|asn|*|0|summary", "ripencc|*|asn|*|1|summary"
            ),
            "asn summary 1 != 0",
        ),
        (
            "ripencc",
            lambda t: t.replace(
                "ripencc|*|asn|*|0|summary", "ripencc|*|ipx|*|0|summary"
            ),
            "unknown type 'ipx'",
        ),
        (
            "ripencc",
            lambda t: t.replace("|85.8.248.0|2048||", "|85.8.248.0|0||"),
            "85.8.248.0 \\+ 0 addresses is not an IPv4 range",
        ),
        (
            "ripencc",
            lambda t: t.replace("|85.8.248.0|2048||", "|255.255.255.0|512||"),
            "255.255.255.0 \\+ 512 addresses is not an IPv4 range",
        ),
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
        "NOT ifNull(ready, 0), 'unknown'",
        "ifNull(covered_rir_blocks, 0) > 0, 'registry_level'",
        "ifNull(special.2, '') IN ('available', 'reserved') OR ifNull(iana.3, '') IN ('', 'RESERVED'), 'unallocated'",
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


def test_parse_delegated_counts_asn_records_against_their_summary():
    arin = source.parse_delegated(excerpt("delegated-arin-extended-excerpt"), "arin")
    assert arin.summaries == {"asn": 2, "ipv4": 6, "ipv6": 3}
    text = excerpt("delegated-arin-extended-excerpt").replace(
        "arin|*|asn|*|2|summary", "arin|*|asn|*|3|summary"
    )
    with pytest.raises(ValueError, match="asn summary 3 != 2"):
        source.parse_delegated(text, "arin")


FIXTURE_FILES = (
    ("iana-ipv4-excerpt.csv", "iana_ipv4"),
    ("iana-ipv6-excerpt.csv", "iana_ipv6"),
    ("delegated-afrinic-extended-excerpt", "afrinic"),
    ("delegated-apnic-extended-excerpt", "apnic"),
    ("delegated-arin-extended-excerpt", "arin"),
    ("delegated-lacnic-extended-excerpt", "lacnic"),
    ("delegated-ripencc-extended-excerpt", "ripencc"),
)


def parse_fixture(text: str, name: str):
    if name.startswith("iana_"):
        return source.parse_iana_csv(text, name)
    return source.parse_delegated(text, name)


@pytest.mark.parametrize(("filename", "name"), FIXTURE_FILES)
def test_crlf_files_parse_like_lf_files(filename, name):
    text = excerpt(filename)
    assert parse_fixture(text.replace("\n", "\r\n"), name) == parse_fixture(text, name)


def test_parse_delegated_keeps_holder_blocks_wide_enough_for_an_iana_block():
    ripencc = source.parse_delegated(
        excerpt("delegated-ripencc-extended-excerpt"), "ripencc"
    )
    # 25.0.0.0/8 (UK MoD) covers IANA 25/8; 2a00::/22 is a candidate that covers no IANA
    # block — the exact containment test is left to SQL, so the kept rows are a superset.
    assert [(h.start_address, h.status, h.cidrs) for h in ripencc.holders] == [
        ("25.0.0.0", "assigned", ("25.0.0.0/8",)),
        ("2a00::", "allocated", ("2a00::/22",)),
    ]
    assert (ripencc.holder_count(4), ripencc.holder_count(6)) == (1, 1)
    arin = source.parse_delegated(excerpt("delegated-arin-extended-excerpt"), "arin")
    assert [h.start_address for h in arin.holders] == [
        "7.0.0.0",
        "19.0.0.0",
        "73.0.0.0",
    ]
    comcast = arin.holders[2]
    assert (comcast.first, comcast.last) == (
        source.address_int("73.0.0.0"),
        source.address_int("73.255.255.255"),
    )
    apnic = source.parse_delegated(excerpt("delegated-apnic-extended-excerpt"), "apnic")
    assert [(h.cc, h.start_address, h.cidrs) for h in apnic.holders] == [
        ("JP", "133.0.0.0", ("133.0.0.0/8",))
    ]
    for name in ("lacnic", "afrinic"):
        parsed = source.parse_delegated(
            excerpt(f"delegated-{name}-extended-excerpt"), name
        )
        assert parsed.holders == ()


def test_holder_max_prefix_is_derived_from_the_iana_blocks():
    blocks = source.parse_iana_csv(
        excerpt("iana-ipv4-excerpt.csv"), "iana_ipv4"
    ) + source.parse_iana_csv(excerpt("iana-ipv6-excerpt.csv"), "iana_ipv6")
    assert (
        source.holder_prefix_limits(blocks) == source.HOLDER_MAX_PREFIX == {4: 8, 6: 23}
    )


def reference_rows():
    iana = source.parse_iana_csv(
        excerpt("iana-ipv4-excerpt.csv"), "iana_ipv4"
    ) + source.parse_iana_csv(excerpt("iana-ipv6-excerpt.csv"), "iana_ipv6")
    holders, special = [], []
    for filename, name in FIXTURE_FILES[2:]:
        parsed = source.parse_delegated(excerpt(filename), name)
        holders.extend(parsed.holders)
        special.extend(parsed.special)
    return iana, holders, special


@pytest.mark.parametrize(
    ("first", "last", "covered", "expected"),
    [
        (
            "73.0.0.0",
            "73.255.255.255",
            0,
            "reusable",
        ),  # Comcast holds all of ARIN's 73/8
        ("103.0.0.0", "103.255.255.255", 1, "registry_level"),  # APNIC-AP
        ("133.0.0.0", "133.255.255.255", 0, "reusable"),  # JPNIC holds 133/8
        ("7.0.0.0", "7.255.255.255", 0, "reusable"),  # DoD
        ("25.0.0.0", "25.255.255.255", 0, "reusable"),  # UK MoD
        ("102.0.0.0", "103.255.255.255", 2, "registry_level"),
        ("2600::", "260f:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 1, "registry_level"),
        ("8.8.8.0", "8.8.8.255", 0, "reusable"),
        ("45.68.105.0", "45.68.105.255", 0, "unallocated"),  # LACNIC reserved
        ("240.0.0.0", "240.255.255.255", 0, "unallocated"),  # IANA Future use
    ],
)
def test_registry_context_reference_over_the_fixtures(first, last, covered, expected):
    iana, holders, special = reference_rows()
    ctx = registry.registry_context(
        source.address_int(first), source.address_int(last), iana, holders, special
    )
    assert (ctx.covered_rir_blocks, registry.registry_class(ctx)) == (covered, expected)


def test_registry_context_is_unknown_until_ready():
    iana, holders, special = reference_rows()
    ctx = registry.registry_context(
        source.address_int("103.0.0.0"),
        source.address_int("103.255.255.255"),
        iana,
        holders,
        special,
        ready=False,
    )
    assert registry.registry_class(ctx) == "unknown"


def test_registry_context_sql_excludes_holder_covered_blocks_and_defaults_ready():
    sql = registry.REGISTRY_CONTEXT_SQL
    assert sql.startswith(
        "SELECT ifNull((SELECT ready FROM corpscout.ip_registry_ready), 0) AS ready,"
    )
    # The holder exclusion lives once, in migration 000450's view (tests/test_ip_registry.py).
    assert (
        "ifNull((SELECT count() FROM corpscout.ip_registry_iana_blocks_rule_current\n     WHERE unheld_rir_block = 1 AND"
        in sql
    )
    assert "holder_blocks" not in sql
    assert tables.HOLDER_TABLE == "ip_registry_holder_blocks"
    assert tables.HOLDER_COLUMNS == tables.SPECIAL_COLUMNS
