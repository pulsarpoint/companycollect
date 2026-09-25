"""IP registry reference data against a real ClickHouse: migrations 000449/000450, snapshots, trie, rule parity."""

import re
from datetime import UTC, date, datetime
from ipaddress import IPv6Address
from pathlib import Path

import pytest

from dagster_v3.defs.commoncrawl_rdap import registry
from dagster_v3.defs.commoncrawl_rdap.assets import (
    RDAP_NETWORK_INSERT_SQL,
    RDAP_SEGMENT_INSERT_SQL,
)
from dagster_v3.defs.commoncrawl_rdap.rdap import (
    RdapLookupResponse,
    normalize_rdap_network,
)
from dagster_v3.defs.ip_registry import source, tables
from tests.test_ip_enrichment_input import server as server

MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
FIXTURES = Path(__file__).parent / "fixtures" / "ip_registry"
MIGRATION_449 = "000449_corpscout_ip_registry_reference_data"
MIGRATION_450 = "000450_corpscout_rdap_trie_registry_class_exclusion"
TEST_SOURCE = "HOST 'localhost' PORT 9000 USER 'test' PASSWORD 'test'"
IANA_DATE = date(2026, 9, 19)
SNAPSHOT_INSERT = f"INSERT INTO corpscout.{tables.SNAPSHOTS_TABLE} ({', '.join(tables.SNAPSHOT_COLUMNS)}) VALUES"
IANA_INSERT = f"INSERT INTO corpscout.{tables.IANA_TABLE} ({', '.join(tables.IANA_COLUMNS)}) VALUES"
SPECIAL_INSERT = f"INSERT INTO corpscout.{tables.SPECIAL_TABLE} ({', '.join(tables.SPECIAL_COLUMNS)}) VALUES"
HOLDER_INSERT = f"INSERT INTO corpscout.{tables.HOLDER_TABLE} ({', '.join(tables.HOLDER_COLUMNS)}) VALUES"
# The only tables migration 000449 may create: reference data stays limited to the IANA blocks,
# the special segments and the whole-block holder records (never the full delegation list).
TABLES_449 = {
    tables.SNAPSHOTS_TABLE,
    tables.IANA_TABLE,
    tables.SPECIAL_TABLE,
    tables.HOLDER_TABLE,
    "rdap_network_registry_class",
}
# Per-miss rule through ClickHouse: the context query and the SQL twin of registry_class().
CONTEXT_CLASS_SQL = (
    "SELECT covered_rir_blocks, "
    + registry.REGISTRY_CLASS_SQL
    + " FROM ("
    + registry.REGISTRY_CONTEXT_SQL
    + ")"
)


def apply_migration(client, name: str, *, before: str | None = None) -> None:
    """Run a migration's statements against the test server.

    GRANT/REVOKE/CREATE USER are skipped (the test user is an admin), dictionary sources are
    pointed at the test server with the test credentials, and lifetimes are zeroed so
    SYSTEM RELOAD DICTIONARY is the only refresh. ``before`` cuts the file at a marker.
    """
    sql = (MIGRATIONS / name).read_text(encoding="utf-8")
    if before is not None:
        sql = sql.split(before, 1)[0]
    for statement in sql.split(";"):
        lines = [
            line
            for line in statement.splitlines()
            if line.strip() and not line.lstrip().startswith("--")
        ]
        text = "\n".join(lines).strip()
        if not text or text.startswith(("GRANT", "REVOKE", "CREATE USER")):
            continue
        text = text.replace("USER 'corpscout_rdap_dictionary'", TEST_SOURCE)
        text = re.sub(r"LIFETIME\(MIN \d+ MAX \d+\)", "LIFETIME(0)", text)
        client.execute(text)


@pytest.fixture(scope="module")
def registry_server(server):
    client, resource = server
    # 000124 up to its dictionary (000450 recreates rdap_network_trie with the test source).
    apply_migration(
        client, "000124_corpscout_rdap_networks.up.sql", before="CREATE DICTIONARY"
    )
    apply_migration(client, f"{MIGRATION_449}.up.sql")
    apply_migration(client, f"{MIGRATION_450}.up.sql")
    return client, resource


def reload_tries(client) -> None:
    for name in (tables.SPECIAL_TRIE, "rdap_network_trie"):
        client.execute(f"SYSTEM RELOAD DICTIONARY corpscout.{name}")


@pytest.fixture
def clean(registry_server):
    client, resource = registry_server
    for table in (
        tables.SNAPSHOTS_TABLE,
        tables.IANA_TABLE,
        tables.SPECIAL_TABLE,
        tables.HOLDER_TABLE,
        "rdap_network_registry_class",
        "rdap_networks",
        "rdap_network_segments",
        "rdap_ip_lookup_results",
    ):
        client.execute(f"TRUNCATE TABLE corpscout.{table}")
    reload_tries(client)
    return client, resource


def segment_row(segment, snapshot_date, loaded_at) -> tuple:
    """A SpecialSegment or HolderBlock as a row of SPECIAL_COLUMNS / HOLDER_COLUMNS."""
    return (
        segment.registry,
        snapshot_date,
        segment.ip_version,
        segment.cc,
        segment.status,
        segment.start_address,
        segment.value,
        IPv6Address(segment.first),
        IPv6Address(segment.last),
        list(segment.cidrs),
        loaded_at,
    )


def seed_reference_data(client):
    """Insert the fixture excerpts as the current snapshot of all seven sources, reload the trie.

    Returns the parsed rows (iana blocks, holder blocks, special segments) for the pure-Python
    reference registry.registry_context().
    """
    loaded_at = datetime.now(UTC)
    iana, holders, special = [], [], []
    for source_name, url in tables.IANA_SOURCES.items():
        text = (FIXTURES / f"{source_name.replace('_', '-')}-excerpt.csv").read_text(
            encoding="utf-8"
        )
        blocks = source.parse_iana_csv(text, source_name)
        iana.extend(blocks)
        client.execute(
            IANA_INSERT,
            [
                (
                    b.source,
                    IANA_DATE,
                    b.ip_version,
                    b.prefix,
                    IPv6Address(b.first),
                    IPv6Address(b.last),
                    b.designation,
                    b.rir,
                    b.status,
                    b.assigned_on,
                    b.whois,
                    b.rdap,
                    b.note,
                    loaded_at,
                )
                for b in blocks
            ],
        )
        ipv4 = sum(b.ip_version == 4 for b in blocks)
        ipv6 = sum(b.ip_version == 6 for b in blocks)
        client.execute(
            SNAPSHOT_INSERT,
            [
                (
                    source_name,
                    IANA_DATE,
                    loaded_at,
                    "fixture",
                    "",
                    ipv4,
                    ipv6,
                    ipv4,
                    ipv6,
                    0,
                    0,
                    url,
                )
            ],
        )
    for registry_name, url in tables.RIR_SOURCES.items():
        text = (FIXTURES / f"delegated-{registry_name}-extended-excerpt").read_text(
            encoding="utf-8"
        )
        parsed = source.parse_delegated(text, registry_name)
        holders.extend(parsed.holders)
        special.extend(parsed.special)
        snapshot_date = parsed.header.end_date
        client.execute(
            SPECIAL_INSERT,
            [segment_row(s, snapshot_date, loaded_at) for s in parsed.special],
        )
        if parsed.holders:
            client.execute(
                HOLDER_INSERT,
                [segment_row(h, snapshot_date, loaded_at) for h in parsed.holders],
            )
        client.execute(
            SNAPSHOT_INSERT,
            [
                (
                    registry_name,
                    snapshot_date,
                    loaded_at,
                    "fixture",
                    parsed.header.serial,
                    parsed.summaries["ipv4"],
                    parsed.summaries["ipv6"],
                    parsed.special_count(4),
                    parsed.special_count(6),
                    parsed.holder_count(4),
                    parsed.holder_count(6),
                    url,
                )
            ],
        )
    reload_tries(client)
    return iana, holders, special


def network_response(rir, handle, start, end, name):
    return RdapLookupResponse(
        rir=rir,
        raw_response={
            "objectClassName": "ip network",
            "handle": handle,
            "startAddress": start,
            "endAddress": end,
            "ipVersion": "v6" if ":" in start else "v4",
            "name": name,
            "status": ["active"],
        },
    )


# (rir, handle, start, end, name, expected class) — the owner's examples plus the edge cases.
# Every block these touch is in the IANA excerpt, except OUTSIDE (no IANA block on purpose).
CASES = [
    (
        "apnic",
        "103.0.0.0 - 103.255.255.255",
        "103.0.0.0",
        "103.255.255.255",
        "APNIC-AP",
        "registry_level",
    ),
    (
        "apnic",
        "101.0.0.0 - 101.255.255.255",
        "101.0.0.0",
        "101.255.255.255",
        "APNIC-101",
        "registry_level",
    ),
    (
        "afrinic",
        "102.0.0.0 - 102.255.255.255",
        "102.0.0.0",
        "102.255.255.255",
        "AFRINIC-102",
        "registry_level",
    ),
    (
        "apnic",
        "113.0.0.0 - 113.255.255.255",
        "113.0.0.0",
        "113.255.255.255",
        "APNIC-113",
        "registry_level",
    ),
    (
        "arin",
        "NET6-2600-1",
        "2600::",
        "260f:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        "ARIN-6",
        "registry_level",
    ),
    (
        "ripencc",
        "EU-ZZ-2A00",
        "2a00::",
        "2a1f:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        "EU-ZZ-2A00",
        "registry_level",
    ),
    ("arin", "WIDE", "100.0.0.0", "103.255.255.255", "SOMEONE", "registry_level"),
    ("afrinic", "MID", "102.128.0.0", "103.255.255.255", "MID-BLOCK", "registry_level"),
    (
        "lacnic",
        "45.68.105.0/24",
        "45.68.105.0",
        "45.68.105.255",
        "UNALLOCATED",
        "unallocated",
    ),
    ("ripencc", "AVAILABLE", "85.8.248.0", "85.8.255.255", "AVAILABLE", "unallocated"),
    (
        "arin",
        "RESERVED-2ND-CIDR",
        "23.128.2.0",
        "23.128.3.255",
        "RESERVED",
        "unallocated",
    ),
    (
        "lacnic",
        "AVAILABLE6",
        "2001:1201:20::",
        "2001:1201:3f:ffff:ffff:ffff:ffff:ffff",
        "AVAILABLE6",
        "unallocated",
    ),
    ("arin", "FUTURE", "240.0.0.0", "240.255.255.255", "FUTURE-USE", "unallocated"),
    (
        "arin",
        "OUTSIDE",
        "4000::",
        "4000:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        "OUTSIDE-UNICAST",
        "unallocated",
    ),
    ("apnic", "FPT-VN", "103.35.64.0", "103.35.67.255", "FPT-VN", "reusable"),
    (
        "arin",
        "NET-104-16-0-0-1",
        "104.16.0.0",
        "104.31.255.255",
        "CLOUDFLARENET",
        "reusable",
    ),
    ("arin", "GOOGLE", "8.8.8.0", "8.8.8.255", "GOOGLE", "reusable"),
    (
        "arin",
        "GOOGLE-IPV6",
        "2001:4860::",
        "2001:4860:ffff:ffff:ffff:ffff:ffff:ffff",
        "GOOGLE-IPV6",
        "reusable",
    ),
    ("arin", "FORD-NET", "19.0.0.0", "19.255.255.255", "FORD-NET", "reusable"),
    ("ripencc", "DK-NET", "195.85.96.0", "195.85.101.255", "DK-NET", "reusable"),
    # Wider than the holder delegations it spans but not a whole IANA block: reusable by the
    # owner's decision (the "wider than the delegation" branch was removed).
    ("ripencc", "SE-AND-FR", "2.0.0.0", "2.3.255.255", "SE-AND-FR", "reusable"),
    ("apnic", "PARTIAL", "103.35.66.0", "103.35.69.255", "PARTIAL", "reusable"),
    # Whole RIR-designated /8s that one holder record covers entirely: the holder's own
    # registration, reusable (ruling R2).
    ("arin", "NET-73-0-0-0-1", "73.0.0.0", "73.255.255.255", "COMCAST", "reusable"),
    (
        "apnic",
        "133.0.0.0 - 133.255.255.255",
        "133.0.0.0",
        "133.255.255.255",
        "JPNIC-NET-JP",
        "reusable",
    ),
]
EXPECTED_COUNTS = {"registry_level": 8, "unallocated": 6, "reusable": 10}
EXPECTED_COVERED = {
    "apnic:103.0.0.0 - 103.255.255.255": 1,
    "apnic:101.0.0.0 - 101.255.255.255": 1,
    "afrinic:102.0.0.0 - 102.255.255.255": 1,
    "apnic:113.0.0.0 - 113.255.255.255": 1,
    "arin:NET6-2600-1": 1,
    "ripencc:EU-ZZ-2A00": 2,
    "arin:WIDE": 4,
    "afrinic:MID": 1,
}
COMCAST = "arin:NET-73-0-0-0-1"


def insert_case_networks(client, *, with_segments: bool = False):
    """Store the CASES as cached registrations; returns network_key -> (network, expected class)."""
    stored = {}
    for rir, handle, start, end, name, expected in CASES:
        normalized = normalize_rdap_network(
            network_response(rir, handle, start, end, name),
            fetched_at=datetime.now(UTC),
            segment_role="lookup_result",
        )
        client.execute(
            RDAP_NETWORK_INSERT_SQL, [normalized.network.clickhouse_values()]
        )
        if with_segments:
            client.execute(
                RDAP_SEGMENT_INSERT_SQL,
                [segment.clickhouse_values() for segment in normalized.segments],
            )
        stored[normalized.network.network_key] = (normalized.network, expected)
    return stored


def special_of(client, ip):
    [(row,)] = client.execute(
        "SELECT dictGetOrDefault('corpscout.ip_registry_special_trie', ('registry', 'status', 'segment_first', 'segment_last'), tuple(toIPv6(%(ip)s)), ('', '', toUInt128(0), toUInt128(0)))",
        {"ip": registry.mapped_address(ip)},
    )
    return row


def iana_of(client, ip):
    [(row,)] = client.execute(
        "SELECT (any(designation), any(rir), any(status)) FROM corpscout.ip_registry_iana_blocks_current WHERE toUInt128(first_ip) <= toUInt128(toIPv6(%(ip)s)) AND toUInt128(last_ip) >= toUInt128(toIPv6(%(ip)s))",
        {"ip": registry.mapped_address(ip)},
    )
    return tuple(row)


def trie_key(client, ip):
    [(key,)] = client.execute(
        "SELECT dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv6(%(ip)s)), '')",
        {"ip": registry.mapped_address(ip)},
    )
    return key


def served_keys(client) -> set[str]:
    return {
        key
        for (key,) in client.execute(
            "SELECT DISTINCT network_key FROM corpscout.rdap_network_segments_current"
        )
    }


def per_miss_sql(client, network) -> tuple[int, str]:
    """(covered_rir_blocks, registry_class) of the per-miss context query, rule in SQL."""
    [(covered, registry_class)] = client.execute(
        CONTEXT_CLASS_SQL,
        {
            "first": registry.mapped_address(network.start_address),
            "last": registry.mapped_address(network.end_address),
        },
    )
    return int(covered), registry_class


def table_columns(sql: str, table: str) -> tuple[str, ...]:
    body = sql.split(f"CREATE TABLE IF NOT EXISTS corpscout.{table}\n(", 1)[1]
    body = body.split("\n)", 1)[0]
    return tuple(line.split()[0] for line in body.strip().splitlines())


def test_migration_449_embeds_the_rule_and_reads_through_the_dictionary_user():
    up = (MIGRATIONS / f"{MIGRATION_449}.up.sql").read_text()
    down = (MIGRATIONS / f"{MIGRATION_449}.down.sql").read_text()
    assert registry.REGISTRY_CLASS_SQL in up  # verbatim, whitespace included
    assert up.count("USER 'corpscout_rdap_dictionary'") == 1
    assert up.count("LIFETIME(MIN 3600 MAX 7200)") == 1
    for name in (
        "ip_registry_snapshots",
        "ip_registry_current_snapshots",
        "ip_registry_special_segments",
        "ip_registry_special_segments_current",
        "ip_registry_special_trie_source",
    ):
        assert f"GRANT SELECT ON corpscout.{name} TO corpscout_rdap_dictionary" in up
        assert (
            f"REVOKE SELECT ON corpscout.{name} FROM corpscout_rdap_dictionary" in down
        )
    assert "holder_blocks TO" not in up  # the dictionary user never reads holder rows
    assert up.count("PARTITION BY (registry, snapshot_date)") == 2
    assert up.count("PARTITION BY (source, snapshot_date)") == 1
    assert "TTL" not in up  # retention is the loader's DROP PARTITION, never a TTL
    # Stored reference data is the IANA blocks, special segments and holder blocks only: no
    # table for the full allocated/assigned delegation list.
    assert (
        set(re.findall(r"CREATE TABLE IF NOT EXISTS corpscout\.(\w+)", up))
        == TABLES_449
    )
    assert set(re.findall(r"DROP TABLE IF EXISTS corpscout\.(\w+)", down)) == TABLES_449
    assert table_columns(up, tables.SNAPSHOTS_TABLE) == tables.SNAPSHOT_COLUMNS
    assert table_columns(up, tables.IANA_TABLE) == tables.IANA_COLUMNS
    assert table_columns(up, tables.SPECIAL_TABLE) == tables.SPECIAL_COLUMNS
    assert table_columns(up, tables.HOLDER_TABLE) == tables.HOLDER_COLUMNS
    assert (
        table_columns(up, "rdap_network_registry_class")
        == registry.REGISTRY_CLASS_COLUMNS
    )
    # The holder exclusion is written once, in the view both rule readers count from.
    assert up.count("CROSS JOIN corpscout.ip_registry_holder_blocks_current AS h") == 1
    assert "FROM corpscout.ip_registry_iana_blocks_rule_current" in up
    assert "b.unheld_rir_block = 1" in up
    assert (
        "FROM corpscout.ip_registry_iana_blocks_rule_current"
        in registry.REGISTRY_CONTEXT_SQL
    )
    assert (
        down.index("DROP DICTIONARY")
        < down.index("DROP VIEW IF EXISTS corpscout.ip_registry_special_trie_source")
        < down.index("DROP TABLE IF EXISTS corpscout.ip_registry_special_segments")
    )
    assert down.index(
        "DROP VIEW IF EXISTS corpscout.ip_registry_iana_blocks_rule_current"
    ) < down.index("DROP VIEW IF EXISTS corpscout.ip_registry_holder_blocks_current")


def test_seeded_snapshots_answer_lookups(clean):
    client, _ = clean
    seed_reference_data(client)
    assert client.execute("SELECT ready FROM corpscout.ip_registry_ready") == [(1,)]
    assert client.execute(
        "SELECT source, snapshot_date FROM corpscout.ip_registry_current_snapshots ORDER BY source"
    ) == [
        ("afrinic", date(2026, 9, 24)),
        ("apnic", date(2026, 9, 25)),
        ("arin", date(2026, 9, 25)),
        ("iana_ipv4", IANA_DATE),
        ("iana_ipv6", IANA_DATE),
        ("lacnic", date(2026, 9, 24)),
        ("ripencc", date(2026, 9, 24)),
    ]
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_special_segments_current"
    ) == [(10,)]
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_special_trie_source"
    ) == [(11,)]
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_iana_blocks_current"
    ) == [(37,)]
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_iana_blocks_current WHERE rir != ''"
    ) == [(30,)]
    assert client.execute(
        "SELECT registry, start_address FROM corpscout.ip_registry_holder_blocks_current ORDER BY registry, start_address"
    ) == [
        ("apnic", "133.0.0.0"),
        ("arin", "19.0.0.0"),
        ("arin", "7.0.0.0"),
        ("arin", "73.0.0.0"),
        ("ripencc", "25.0.0.0"),
        ("ripencc", "2a00::"),
    ]
    # Held: the RIR-designated blocks one holder record covers entirely (Ford's 19/8 is held
    # too but not RIR-designated, so it was never counted).
    assert client.execute(
        "SELECT prefix FROM corpscout.ip_registry_iana_blocks_rule_current WHERE rir != '' AND unheld_rir_block = 0 ORDER BY first_ip"
    ) == [("7.0.0.0/8",), ("25.0.0.0/8",), ("73.0.0.0/8",), ("133.0.0.0/8",)]
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_iana_blocks_rule_current WHERE unheld_rir_block = 1"
    ) == [(26,)]
    mapped = source.address_int
    assert special_of(client, "45.68.105.9") == (
        "lacnic",
        "reserved",
        mapped("45.68.105.0"),
        mapped("45.68.105.255"),
    )
    assert special_of(client, "23.128.3.7") == (
        "arin",
        "reserved",
        mapped("23.128.1.0"),
        mapped("23.128.3.255"),
    )  # second CIDR of 768 addresses
    assert special_of(client, "102.199.0.1")[:2] == ("afrinic", "available")
    assert special_of(client, "85.8.250.1")[:2] == ("ripencc", "available")
    assert special_of(client, "2001:1201:20::1")[:2] == ("lacnic", "available")
    assert special_of(client, "103.35.64.49") == ("", "", 0, 0)
    assert special_of(client, "8.8.8.8") == ("", "", 0, 0)
    assert iana_of(client, "103.0.0.0") == ("APNIC", "apnic", "ALLOCATED")
    assert iana_of(client, "19.5.0.1") == ("Ford Motor Company", "", "LEGACY")
    assert iana_of(client, "45.68.105.9") == ("Administered by ARIN", "arin", "LEGACY")
    assert iana_of(client, "2600:1f00::1") == ("ARIN", "arin", "ALLOCATED")
    assert iana_of(client, "4000::1") == ("", "", "")


def test_a_newer_ledger_row_switches_the_current_snapshot_but_a_partial_load_does_not(
    clean,
):
    client, _ = clean
    seed_reference_data(client)
    loaded_at = datetime.now(UTC)
    later = date(2026, 9, 25)
    # Rows without a ledger row are not current: 85.8.248.0/21 stays available and the
    # ripencc holders stay.
    client.execute(
        SPECIAL_INSERT,
        [
            (
                "ripencc",
                later,
                4,
                "",
                "reserved",
                "5.134.16.0",
                2048,
                IPv6Address(source.address_int("5.134.16.0")),
                IPv6Address(source.address_int("5.134.23.255")),
                ["5.134.16.0/21"],
                loaded_at,
            )
        ],
    )
    reload_tries(client)
    assert special_of(client, "85.8.250.1")[1] == "available"
    assert client.execute(
        "SELECT snapshot_date FROM corpscout.ip_registry_current_snapshots WHERE source = 'ripencc'"
    ) == [(date(2026, 9, 24),)]
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_holder_blocks_current WHERE registry = 'ripencc'"
    ) == [(2,)]
    client.execute(
        SNAPSHOT_INSERT,
        [
            (
                "ripencc",
                later,
                loaded_at,
                "fixture-2",
                "1790373599",
                8,
                3,
                1,
                0,
                0,
                0,
                "",
            )
        ],
    )
    reload_tries(client)
    assert special_of(client, "85.8.250.1") == (
        "",
        "",
        0,
        0,
    )  # the new snapshot lists only the reserved range
    assert special_of(client, "5.134.17.1")[1] == "reserved"
    # The new snapshot has no holder rows: UK MoD's 25/8 is no longer held.
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_holder_blocks_current WHERE registry = 'ripencc'"
    ) == [(0,)]
    assert client.execute(
        "SELECT unheld_rir_block FROM corpscout.ip_registry_iana_blocks_rule_current WHERE prefix = '25.0.0.0/8'"
    ) == [(1,)]
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_special_segments WHERE registry = 'ripencc'"
    ) == [(3,)]  # both snapshots kept until the loader drops the older one
    client.execute("TRUNCATE TABLE corpscout.ip_registry_snapshots")
    assert client.execute("SELECT ready FROM corpscout.ip_registry_ready") == [(0,)]


def test_per_miss_query_derived_view_and_python_rule_agree_on_every_case(clean):
    client, _ = clean
    stored = insert_case_networks(client)
    # Not ready: the view keeps every network (left join) and every rule says unknown.
    assert client.execute(
        "SELECT count(), groupUniqArray(registry_class) FROM corpscout.rdap_network_registry_class_derived"
    ) == [(len(CASES), ["unknown"])]
    for network, _ in stored.values():
        assert per_miss_sql(client, network)[1] == "unknown"
        assert registry.classify_registration(client, network).registry_class == (
            "unknown"
        )
    iana, holders, special = seed_reference_data(client)
    from_view = {
        key: (int(covered), registry_class)
        for key, registry_class, covered in client.execute(
            "SELECT network_key, registry_class, covered_rir_blocks FROM corpscout.rdap_network_registry_class_derived"
        )
    }
    from_per_miss_sql = {
        key: per_miss_sql(client, network) for key, (network, _) in stored.items()
    }
    from_python_on_sql_context = {}
    for key, (network, _) in stored.items():
        classification = registry.classify_registration(client, network)
        from_python_on_sql_context[key] = (
            classification.context.covered_rir_blocks,
            classification.registry_class,
        )
    from_reference = {}
    for key, (network, _) in stored.items():
        context = registry.registry_context(
            source.address_int(network.start_address),
            source.address_int(network.end_address),
            iana,
            holders,
            special,
        )
        from_reference[key] = (
            context.covered_rir_blocks,
            registry.registry_class(context),
        )
    expected = {
        key: (EXPECTED_COVERED.get(key, 0), expected)
        for key, (_, expected) in stored.items()
    }
    assert from_view == expected
    assert from_per_miss_sql == expected
    assert from_python_on_sql_context == expected
    assert from_reference == expected
    assert from_view[COMCAST] == (0, "reusable")
    assert from_view["apnic:133.0.0.0 - 133.255.255.255"] == (0, "reusable")
    assert {
        cls: [e for _, e in expected.values()].count(cls) for cls in EXPECTED_COUNTS
    } == EXPECTED_COUNTS
    # The persisted row shape is the same from both writers.
    for key in ("apnic:FPT-VN", "lacnic:45.68.105.0/24"):
        classification = registry.classify_registration(client, stored[key][0])
        client.execute(
            registry.REGISTRY_CLASS_INSERT_SQL,
            [classification.clickhouse_values(key, datetime.now(UTC))],
        )
    columns = ", ".join(registry.REGISTRY_CLASS_COLUMNS[:-1])
    from_python_rows = client.execute(
        f"SELECT {columns} FROM corpscout.rdap_network_registry_class_current ORDER BY network_key"
    )
    client.execute(registry.REGISTRY_CLASS_REFRESH_SQL)
    from_view_rows = client.execute(
        f"SELECT {columns} FROM corpscout.rdap_network_registry_class_current WHERE network_key IN ('apnic:FPT-VN', 'lacnic:45.68.105.0/24') ORDER BY network_key"
    )
    assert from_python_rows == from_view_rows
    assert [
        (row[0], row[1], row[4], row[5], row[6], row[7], row[8], row[9])
        for row in from_view_rows
    ] == [
        ("apnic:FPT-VN", "reusable", 0, "APNIC", "apnic", "ALLOCATED", "", ""),
        (
            "lacnic:45.68.105.0/24",
            "unallocated",
            0,
            "Administered by ARIN",
            "arin",
            "LEGACY",
            "lacnic",
            "reserved",
        ),
    ]
    assert [(int(row[10]), int(row[11])) for row in from_view_rows] == [
        (0, 0),
        (source.address_int("45.68.105.0"), source.address_int("45.68.105.255")),
    ]
    assert client.execute(
        "SELECT count() FROM corpscout.rdap_network_registry_class_current"
    ) == [(len(CASES),)]


def test_migration_450_serves_only_reusable_registrations_and_follows_reclassification(
    clean,
):
    client, _ = clean
    up = (MIGRATIONS / f"{MIGRATION_450}.up.sql").read_text()
    assert (
        "WHERE registry_class != 'reusable'" in up
        and "USER 'corpscout_rdap_dictionary'" in up
    )
    assert (
        up.index("DROP DICTIONARY")
        < up.index("DROP VIEW")
        < up.index("CREATE VIEW")
        < up.index("CREATE DICTIONARY")
    )
    stored = insert_case_networks(client, with_segments=True)
    reload_tries(client)
    # Without class rows every lookup_result segment is served, exactly as before 000450.
    assert trie_key(client, "101.1.2.3") == "apnic:101.0.0.0 - 101.255.255.255"
    seed_reference_data(client)
    client.execute(registry.REGISTRY_CLASS_REFRESH_SQL)
    reload_tries(client)
    assert served_keys(client) == {
        key for key, (_, expected) in stored.items() if expected == "reusable"
    }
    assert [
        trie_key(client, ip)
        for ip in (
            "101.1.2.3",
            "103.15.66.50",
            "103.35.64.49",
            "2600:1f00::1",
            "8.8.8.8",
            "73.1.2.3",
        )
    ] == ["", "", "apnic:FPT-VN", "", "arin:GOOGLE", COMCAST]
    # A later classification wins (ReplacingMergeTree by network_key): mark FPT
    # registry_level, then reusable again.
    for registry_class, expect_served in (
        ("registry_level", False),
        ("reusable", True),
    ):
        client.execute(
            registry.REGISTRY_CLASS_INSERT_SQL,
            [
                (
                    "apnic:FPT-VN",
                    registry_class,
                    IPv6Address(0),
                    IPv6Address(0),
                    0,
                    "",
                    "",
                    "",
                    "",
                    "",
                    IPv6Address(0),
                    IPv6Address(0),
                    datetime.now(UTC),
                )
            ],
        )
        reload_tries(client)
        assert ("apnic:FPT-VN" in served_keys(client)) is expect_served
    # Reference data drives the exclusion too: a newer ARIN snapshot without Comcast's holder
    # record makes 73/8 registry level at the next refresh, and the trie stops serving it.
    arin = source.parse_delegated(
        (FIXTURES / "delegated-arin-extended-excerpt").read_text(encoding="utf-8"),
        "arin",
    )
    later, loaded_at = date(2026, 9, 26), datetime.now(UTC)
    client.execute(
        SPECIAL_INSERT, [segment_row(s, later, loaded_at) for s in arin.special]
    )
    client.execute(
        HOLDER_INSERT,
        [
            segment_row(h, later, loaded_at)
            for h in arin.holders
            if h.start_address != "73.0.0.0"
        ],
    )
    client.execute(
        SNAPSHOT_INSERT,
        [("arin", later, loaded_at, "fixture-2", "", 6, 3, 1, 0, 2, 0, "")],
    )
    client.execute(registry.REGISTRY_CLASS_REFRESH_SQL)
    reload_tries(client)
    assert client.execute(
        f"SELECT registry_class, covered_rir_blocks FROM corpscout.rdap_network_registry_class_current WHERE network_key = '{COMCAST}'"
    ) == [("registry_level", 1)]
    assert COMCAST not in served_keys(client)
    assert trie_key(client, "73.1.2.3") == ""
