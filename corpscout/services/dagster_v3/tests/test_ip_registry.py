"""IP registry reference data against a real ClickHouse: migrations 000450/000451, snapshots, trie, rule parity."""

import hashlib
import re
from datetime import UTC, date, datetime, timedelta
from ipaddress import IPv6Address
from pathlib import Path

import dagster as dg
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
from dagster_v3.defs.ip_registry import assets, source, tables
from tests.test_ip_enrichment_input import server as server

MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
FIXTURES = Path(__file__).parent / "fixtures" / "ip_registry"
MIGRATION_450 = "000450_corpscout_ip_registry_reference_data"
MIGRATION_451 = "000451_corpscout_rdap_trie_registry_class_exclusion"
TEST_SOURCE = "HOST 'localhost' PORT 9000 USER 'test' PASSWORD 'test'"
IANA_DATE = date(2026, 9, 19)
SNAPSHOT_INSERT = f"INSERT INTO corpscout.{tables.SNAPSHOTS_TABLE} ({', '.join(tables.SNAPSHOT_COLUMNS)}) VALUES"
IANA_INSERT = f"INSERT INTO corpscout.{tables.IANA_TABLE} ({', '.join(tables.IANA_COLUMNS)}) VALUES"
SPECIAL_INSERT = f"INSERT INTO corpscout.{tables.SPECIAL_TABLE} ({', '.join(tables.SPECIAL_COLUMNS)}) VALUES"
HOLDER_INSERT = f"INSERT INTO corpscout.{tables.HOLDER_TABLE} ({', '.join(tables.HOLDER_COLUMNS)}) VALUES"
# The only tables migration 000450 may create: reference data stays limited to the IANA blocks,
# the special segments and the whole-block holder records (never the full delegation list).
TABLES_450 = {
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
    # 000124 up to its dictionary (000451 recreates rdap_network_trie with the test source).
    apply_migration(
        client, "000124_corpscout_rdap_networks.up.sql", before="CREATE DICTIONARY"
    )
    apply_migration(client, f"{MIGRATION_450}.up.sql")
    apply_migration(client, f"{MIGRATION_451}.up.sql")
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


def test_migration_450_embeds_the_rule_and_reads_through_the_dictionary_user():
    up = (MIGRATIONS / f"{MIGRATION_450}.up.sql").read_text()
    down = (MIGRATIONS / f"{MIGRATION_450}.down.sql").read_text()
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
        == TABLES_450
    )
    assert set(re.findall(r"DROP TABLE IF EXISTS corpscout\.(\w+)", down)) == TABLES_450
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


def test_migration_451_serves_only_reusable_registrations_and_follows_reclassification(
    clean,
):
    client, _ = clean
    up = (MIGRATIONS / f"{MIGRATION_451}.up.sql").read_text()
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
    # Without class rows every lookup_result segment is served, exactly as before 000451.
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


# --- The ip_registry Dagster module: loaders, retention, classification, checks -------------

IANA_LAST_MODIFIED = "Sat, 19 Sep 2026 00:44:20 GMT"
# The freshness checks run on this clock so the fixture dates (2026-09-24/25) stay fresh.
CHECK_NOW = datetime(2026, 9, 25, 12, tzinfo=UTC)
RIPENCC_EXCERPT = FIXTURES / "delegated-ripencc-extended-excerpt"


def fixture_http(monkeypatch, *, tamper=None, iana_last_modified=IANA_LAST_MODIFIED):
    """Serve the fixture excerpts (and matching .md5 files) instead of the internet."""
    bodies = {}
    for source_name, url in tables.IANA_SOURCES.items():
        bodies[url] = (
            (FIXTURES / f"{source_name.replace('_', '-')}-excerpt.csv").read_bytes(),
            {"Last-Modified": iana_last_modified},
        )
    for registry_name, url in tables.RIR_SOURCES.items():
        body = (FIXTURES / f"delegated-{registry_name}-extended-excerpt").read_bytes()
        digest = hashlib.md5(body).hexdigest()
        md5 = (
            f"{digest}  delegated-arin-extended-20260925\n"
            if registry_name == "arin"
            else f"MD5 (delegated-{registry_name}-extended-latest) = {digest}\n"
        )
        bodies[url] = (body, {})
        bodies[url + ".md5"] = (md5.encode(), {})
    bodies.update(tamper or {})
    calls = []

    def fetch(url):
        calls.append(url)
        return bodies[url]

    monkeypatch.setattr(assets, "fetch", fetch)
    monkeypatch.setattr(assets, "freshness_now", lambda: CHECK_NOW)
    monkeypatch.setattr(assets, "IANA_MIN_ROWS", {"iana_ipv4": 23, "iana_ipv6": 10})
    return calls


def dated_ripencc(day: bytes, body: bytes | None = None) -> dict:
    """A tamper dict serving the RIPE excerpt with another end date and a matching .md5."""
    body = RIPENCC_EXCERPT.read_bytes() if body is None else body
    dated = body.replace(b"|20260924|+0200", b"|" + day + b"|+0200", 1)
    url = tables.RIR_SOURCES["ripencc"]
    return {
        url: (dated, {}),
        url + ".md5": (f"MD5 (x) = {hashlib.md5(dated).hexdigest()}\n".encode(), {}),
    }


def loader_assets():
    """The loader assets without their retry policy (a failing step must not wait 5 minutes)."""
    return [
        assets.iana_blocks_asset(retry_policy=None),
        *(
            assets.special_segment_asset(registry_name, url, retry_policy=None)
            for registry_name, url in tables.RIR_SOURCES.items()
        ),
    ]


def refresh(resource, **config):
    loaders = loader_assets()
    return dg.materialize(
        [*loaders, assets.rdap_network_registry_class, *assets.checks],
        resources={"clickhouse": resource},
        run_config=(
            {"ops": {asset.op.name: {"config": config} for asset in loaders[1:]}}
            if config
            else None
        ),
        raise_on_error=False,
    )


def republished_ripencc() -> bytes:
    """The RIPE excerpt re-published for the same date: RIPE allocated the available
    85.8.248.0/21, and 2a00::/22 shrank to a /24 (no longer a holder block)."""
    return (
        RIPENCC_EXCERPT.read_bytes()
        .replace(
            b"ripencc||ipv4|85.8.248.0|2048||available\n",
            b"ripencc|SE|ipv4|85.8.248.0|2048|20260924|allocated|x\n",
        )
        .replace(b"ripencc|DE|ipv6|2a00::|22|", b"ripencc|DE|ipv6|2a00::|24|")
    )


def fail_insert(monkeypatch, sql: str):
    """Make every insert of one statement raise; returns the real insert_rows to restore."""
    real = assets.insert_rows

    def insert_rows(client, statement, rows):
        if statement == sql:
            raise RuntimeError("injected insert failure")
        real(client, statement, rows)

    monkeypatch.setattr(assets, "insert_rows", insert_rows)
    return real


def ripencc_rows(client, table: str, *, final: bool) -> list[str]:
    return [
        start
        for (start,) in client.execute(
            f"SELECT start_address FROM corpscout.{table} {'FINAL' if final else ''} WHERE registry = 'ripencc' ORDER BY start_address"
        )
    ]


def test_refresh_loads_every_source_classifies_and_passes_the_checks(
    clean, monkeypatch
):
    client, resource = clean
    stored = insert_case_networks(client)
    calls = fixture_http(monkeypatch)
    result = refresh(resource)
    assert result.success
    assert sorted(calls) == sorted(
        [
            *tables.IANA_SOURCES.values(),
            *tables.RIR_SOURCES.values(),
            *(url + ".md5" for url in tables.RIR_SOURCES.values()),
        ]
    )
    assert client.execute(
        "SELECT source, snapshot_date, records_ipv4, records_ipv6, segments_ipv4, segments_ipv6, holders_ipv4, holders_ipv6 FROM corpscout.ip_registry_snapshots FINAL ORDER BY source"
    ) == [
        ("afrinic", date(2026, 9, 24), 5, 2, 2, 0, 0, 0),
        ("apnic", date(2026, 9, 25), 7, 2, 2, 0, 1, 0),
        ("arin", date(2026, 9, 25), 6, 3, 1, 0, 3, 0),
        ("iana_ipv4", IANA_DATE, 27, 0, 27, 0, 0, 0),
        ("iana_ipv6", IANA_DATE, 0, 10, 0, 10, 0, 0),
        ("lacnic", date(2026, 9, 24), 3, 4, 1, 2, 0, 0),
        ("ripencc", date(2026, 9, 24), 9, 4, 2, 0, 1, 1),
    ]
    assert client.execute(
        "SELECT serial FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'arin'"
    ) == [("1790341220831",)]
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_special_segments_current"
    ) == [(10,)]
    # Only special and holder rows are stored, never the other allocated/assigned records.
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_special_segments"
    ) == [(10,)]
    assert client.execute(
        "SELECT registry, start_address, status FROM corpscout.ip_registry_holder_blocks_current ORDER BY registry, start_address"
    ) == [
        ("apnic", "133.0.0.0", "allocated"),
        ("arin", "19.0.0.0", "allocated"),
        ("arin", "7.0.0.0", "allocated"),
        ("arin", "73.0.0.0", "allocated"),
        ("ripencc", "25.0.0.0", "assigned"),
        ("ripencc", "2a00::", "allocated"),
    ]
    assert special_of(client, "45.68.105.9")[:2] == ("lacnic", "reserved")
    assert dict(
        client.execute(
            "SELECT network_key, registry_class FROM corpscout.rdap_network_registry_class_current"
        )
    ) == {key: expected for key, (_, expected) in stored.items()}
    materialization = result.asset_materializations_for_node(
        "rdap_network_registry_class"
    )[0].metadata
    assert materialization["networks_registry_level"].value == 8
    assert materialization["networks_total"].value == len(CASES)
    evaluations = result.get_asset_check_evaluations()
    assert len(evaluations) == 7
    assert all(evaluation.passed for evaluation in evaluations)
    assert {evaluation.check_name for evaluation in evaluations} == {
        "snapshot_fresh",
        "classification_complete",
    }
    loaded = result.asset_materializations_for_node(
        "ip_registry_special_segments_ripencc"
    )[0].metadata
    assert (
        loaded["loaded"].value,
        loaded["records_ipv4"].value,
        loaded["segments_ipv4"].value,
        loaded["holders_ipv4"].value,
        loaded["holders_ipv6"].value,
        loaded["md5"].value,
    ) == (True, 9, 2, 1, 1, hashlib.md5(RIPENCC_EXCERPT.read_bytes()).hexdigest())


def test_identical_snapshot_is_verified_not_reloaded(clean, monkeypatch):
    client, resource = clean
    fixture_http(monkeypatch)
    assert refresh(resource).success
    [(rows_before, verified_before)] = client.execute(
        "SELECT count(), max(verified_at) FROM corpscout.ip_registry_snapshots FINAL"
    )
    stored_before = [
        client.execute(f"SELECT count(), max(loaded_at) FROM corpscout.{table}")
        for table in (tables.IANA_TABLE, tables.SPECIAL_TABLE, tables.HOLDER_TABLE)
    ]
    again = refresh(resource)
    assert again.success
    assert (
        again.asset_materializations_for_node("ip_registry_special_segments_apnic")[0]
        .metadata["loaded"]
        .value
        is False
    )
    assert (
        again.asset_materializations_for_node("ip_registry_iana_blocks")[0]
        .metadata["iana_ipv4_loaded"]
        .value
        is False
    )
    [(rows_after, verified_after)] = client.execute(
        "SELECT count(), max(verified_at) FROM corpscout.ip_registry_snapshots FINAL"
    )
    assert (rows_after, rows_before) == (7, 7) and verified_after > verified_before
    assert [
        client.execute(f"SELECT count(), max(loaded_at) FROM corpscout.{table}")
        for table in (tables.IANA_TABLE, tables.SPECIAL_TABLE, tables.HOLDER_TABLE)
    ] == stored_before


def test_a_republished_same_date_file_replaces_its_snapshot(clean, monkeypatch):
    """ReplacingMergeTree keeps rows a re-published file removed unless the partition is dropped."""
    client, resource = clean
    fixture_http(monkeypatch)
    assert refresh(resource).success
    # Same end date, different content: RIPE allocated the available 85.8.248.0/21, and the
    # 2a00::/22 record shrank to a /24 (no longer a holder block). IANA re-published the IPv4
    # file under the same Last-Modified without the 240/8 row.
    changed = republished_ripencc()
    iana_ipv4 = (FIXTURES / "iana-ipv4-excerpt.csv").read_bytes()
    iana_lines = iana_ipv4.splitlines(keepends=True)
    dropped_line = next(line for line in iana_lines if line.startswith(b"240/8"))
    iana_url = tables.IANA_SOURCES["iana_ipv4"]
    fixture_http(
        monkeypatch,
        tamper={
            **dated_ripencc(b"20260924", changed),
            iana_url: (
                iana_ipv4.replace(dropped_line, b""),
                {"Last-Modified": IANA_LAST_MODIFIED},
            ),
        },
    )
    result = refresh(resource)
    assert result.success
    ripencc = result.asset_materializations_for_node(
        "ip_registry_special_segments_ripencc"
    )[0].metadata
    assert (ripencc["loaded"].value, ripencc["dropped_snapshots"].value) == (True, 0)
    assert client.execute(
        "SELECT snapshot_date, checksum, segments_ipv4, holders_ipv4, holders_ipv6 FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'ripencc'"
    ) == [(date(2026, 9, 24), hashlib.md5(changed).hexdigest(), 1, 1, 0)]
    # Exactly the new rows, with and without FINAL: the older versions were deleted.
    for final in (True, False):
        assert ripencc_rows(client, tables.SPECIAL_TABLE, final=final) == ["5.134.16.0"]
        assert ripencc_rows(client, tables.HOLDER_TABLE, final=final) == ["25.0.0.0"]
    assert special_of(client, "85.8.250.1") == ("", "", 0, 0)
    for final in ("FINAL", ""):
        assert client.execute(
            f"SELECT count() FROM corpscout.ip_registry_iana_blocks {final} WHERE source = 'iana_ipv4'"
        ) == [(26,)]
    assert iana_of(client, "240.0.0.1") == ("", "", "")


def test_bad_checksum_older_file_and_shrinking_snapshot_are_refused(clean, monkeypatch):
    client, resource = clean
    ripencc = tables.RIR_SOURCES["ripencc"]
    body = RIPENCC_EXCERPT.read_bytes()
    fixture_http(
        monkeypatch,
        tamper={
            ripencc + ".md5": (
                b"MD5 (delegated-ripencc-extended-latest) = " + b"0" * 32 + b"\n",
                {},
            )
        },
    )
    result = refresh(resource)
    assert not result.success
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'ripencc'"
    ) == [(0,)]
    for table in (tables.SPECIAL_TABLE, tables.HOLDER_TABLE):
        assert client.execute(
            f"SELECT count() FROM corpscout.{table} WHERE registry = 'ripencc'"
        ) == [(0,)]
    # The other six loaded.
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_snapshots FINAL"
    ) == [(6,)]
    assert client.execute("SELECT ready FROM corpscout.ip_registry_ready") == [(0,)]
    fixture_http(monkeypatch)
    assert refresh(resource).success
    # A file dated before the current snapshot is refused.
    fixture_http(monkeypatch, tamper=dated_ripencc(b"20260923"))
    assert not refresh(resource).success
    # A newer file that lost four of its nine IPv4 records (allocated ones: the special rows
    # are untouched, the guard watches the whole file) is refused unless allow_shrink is set.
    shrunk = (
        body.replace(b"2|ripencc|1790287199|13|", b"2|ripencc|1790373599|9|", 1)
        .replace(b"ripencc|*|ipv4|*|9|summary", b"ripencc|*|ipv4|*|5|summary", 1)
        .replace(
            b"ripencc|SE|ipv4|2.0.0.0|131072|20100712|allocated|12a581c1-ea86-46af-9554-77e3b4ab3df5\n",
            b"",
        )
        .replace(
            b"ripencc|SE|ipv4|2.2.0.0|65536|20100712|allocated|12a581c1-ea86-46af-9554-77e3b4ab3df5\n",
            b"",
        )
        .replace(
            b"ripencc|FR|ipv4|2.3.0.0|65536|20100712|allocated|9a489e65-dd78-443e-96ab-e21e016b5113\n",
            b"",
        )
        .replace(
            b"ripencc|PS|ipv4|1.178.112.0|4096|20071126|allocated|172ce676-8ded-4901-9812-793bd0b4ec77\n",
            b"",
        )
    )
    fixture_http(monkeypatch, tamper=dated_ripencc(b"20260925", shrunk))
    assert not refresh(resource).success
    assert client.execute(
        "SELECT snapshot_date FROM corpscout.ip_registry_current_snapshots WHERE source = 'ripencc'"
    ) == [(date(2026, 9, 24),)]
    fixture_http(monkeypatch, tamper=dated_ripencc(b"20260925", shrunk))
    assert refresh(resource, allow_shrink=True).success
    assert client.execute(
        "SELECT snapshot_date, records_ipv4, segments_ipv4 FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'ripencc' ORDER BY snapshot_date DESC LIMIT 1"
    ) == [(date(2026, 9, 25), 5, 2)]


def test_loader_keeps_only_the_current_and_previous_snapshot(clean, monkeypatch):
    client, resource = clean
    for day in (b"20260924", b"20260925", b"20260926"):
        fixture_http(monkeypatch, tamper=dated_ripencc(day))
        result = refresh(resource)
        assert result.success
    # The ledger keeps every load.
    assert client.execute(
        "SELECT snapshot_date FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'ripencc' ORDER BY snapshot_date"
    ) == [(date(2026, 9, 24),), (date(2026, 9, 25),), (date(2026, 9, 26),)]
    # Rows: current + previous only, in both RIR data tables.
    for table in (tables.SPECIAL_TABLE, tables.HOLDER_TABLE):
        assert client.execute(
            f"SELECT DISTINCT snapshot_date FROM corpscout.{table} WHERE registry = 'ripencc' ORDER BY snapshot_date"
        ) == [(date(2026, 9, 25),), (date(2026, 9, 26),)]
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_special_segments_current WHERE registry = 'ripencc'"
    ) == [(2,)]
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_holder_blocks_current WHERE registry = 'ripencc'"
    ) == [(2,)]
    assert (
        result.asset_materializations_for_node("ip_registry_special_segments_ripencc")[
            0
        ]
        .metadata["dropped_snapshots"]
        .value
        == 1
    )
    # An untouched source keeps its one snapshot.
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_special_segments WHERE registry = 'apnic'"
    ) == [(2,)]


def test_freshness_check_flags_missing_stale_and_unverified_sources():
    now = datetime(2026, 9, 25, 12, tzinfo=UTC)
    fresh = now - timedelta(hours=6)
    rows = [
        ("ripencc", date(2026, 9, 24), fresh),
        ("apnic", date(2026, 9, 20), fresh),  # stale RIR snapshot
        ("arin", date(2026, 9, 25), now - timedelta(days=3)),  # not re-verified
        ("iana_ipv4", date(2026, 3, 1), fresh),  # IANA: an old snapshot is fine
    ]
    passed = assets.snapshot_freshness(("ripencc",), rows, now)
    assert passed.passed
    assert passed.metadata["ripencc_snapshot_date"].value == "2026-09-24"
    assert assets.snapshot_freshness(("iana_ipv4",), rows, now).passed
    stale = assets.snapshot_freshness(("apnic",), rows, now)
    assert not stale.passed
    assert "apnic: snapshot 2026-09-20 is stale" in stale.description
    unverified = assets.snapshot_freshness(("arin",), rows, now)
    assert not unverified.passed
    assert "arin: last verified 2026-09-22" in unverified.description
    missing = assets.snapshot_freshness(("lacnic",), rows, now)
    assert not missing.passed and "lacnic: no snapshot" in missing.description


def test_definitions_expose_the_daily_job_stopped_by_default():
    assert assets.ip_registry_daily.cron_schedule == "5 6 * * *"
    assert assets.ip_registry_daily.default_status == dg.DefaultScheduleStatus.STOPPED
    assert assets.ip_registry_daily.job_name == "ip_registry_refresh_job"
    names = {
        asset.key.to_user_string()
        for asset in [
            assets.ip_registry_iana_blocks,
            *assets.special_segment_assets,
            assets.rdap_network_registry_class,
        ]
    }
    assert names == {
        "ip_registry_iana_blocks",
        "rdap_network_registry_class",
        *(f"ip_registry_special_segments_{r}" for r in tables.RIR_SOURCES),
    }
    assert len(assets.checks) == 7


def test_a_failed_same_date_republish_keeps_the_current_rows_visible_and_a_retry_converges(
    clean, monkeypatch
):
    """Ruling R5: a re-published current snapshot is never empty or partial for readers."""
    client, resource = clean
    stored = insert_case_networks(client)
    fixture_http(monkeypatch)
    assert refresh(resource).success
    original = hashlib.md5(RIPENCC_EXCERPT.read_bytes()).hexdigest()
    changed = republished_ripencc()
    available = stored["ripencc:AVAILABLE"][0]
    assert per_miss_sql(client, available) == (0, "unallocated")
    # The re-published file dies after its special rows, before its holder rows.
    fixture_http(monkeypatch, tamper=dated_ripencc(b"20260924", changed))
    real_insert_rows = fail_insert(monkeypatch, assets.HOLDER_INSERT_SQL)
    assert not refresh(resource).success
    assert client.execute(
        "SELECT checksum FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'ripencc'"
    ) == [(original,)]
    # Readers see a superset of the old and the new rows, never an empty snapshot.
    assert ripencc_rows(client, tables.SPECIAL_TABLE, final=True) == [
        "5.134.16.0",
        "85.8.248.0",
    ]
    assert client.execute(
        "SELECT start_address FROM corpscout.ip_registry_special_segments_current WHERE registry = 'ripencc' ORDER BY start_address"
    ) == [("5.134.16.0",), ("85.8.248.0",)]
    assert client.execute(
        "SELECT start_address FROM corpscout.ip_registry_holder_blocks_current WHERE registry = 'ripencc' ORDER BY start_address"
    ) == [("25.0.0.0",), ("2a00::",)]
    assert client.execute(
        "SELECT unheld_rir_block FROM corpscout.ip_registry_iana_blocks_rule_current WHERE prefix = '25.0.0.0/8'"
    ) == [(0,)]
    reload_tries(client)
    assert special_of(client, "85.8.250.1")[:2] == ("ripencc", "available")
    assert per_miss_sql(client, available) == (0, "unallocated")
    context = registry.classify_registration(client, available).context
    assert context.ready and context.special.status == "available"
    assert client.execute(
        "SELECT registry_class FROM corpscout.rdap_network_registry_class_current WHERE network_key = 'ripencc:AVAILABLE'"
    ) == [("unallocated",)]
    # The retry converges to exactly the new rows and reclassifies.
    monkeypatch.setattr(assets, "insert_rows", real_insert_rows)
    result = refresh(resource)
    assert result.success
    assert client.execute(
        "SELECT checksum FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'ripencc'"
    ) == [(hashlib.md5(changed).hexdigest(),)]
    for final in (True, False):
        assert ripencc_rows(client, tables.SPECIAL_TABLE, final=final) == ["5.134.16.0"]
        assert ripencc_rows(client, tables.HOLDER_TABLE, final=final) == ["25.0.0.0"]
    assert special_of(client, "85.8.250.1") == ("", "", 0, 0)
    assert client.execute(
        "SELECT registry_class FROM corpscout.rdap_network_registry_class_current WHERE network_key = 'ripencc:AVAILABLE'"
    ) == [("reusable",)]


def test_a_run_that_died_before_deleting_stale_rows_is_repaired_by_the_next_run(
    clean, monkeypatch
):
    client, resource = clean
    fixture_http(monkeypatch)
    assert refresh(resource).success
    changed = republished_ripencc()
    fixture_http(monkeypatch, tamper=dated_ripencc(b"20260924", changed))

    def delete_fails(*args, **kwargs):
        raise RuntimeError("injected delete failure")

    real_delete = assets.delete_stale_rows
    monkeypatch.setattr(assets, "delete_stale_rows", delete_fails)
    assert not refresh(resource).success
    # The ledger already names the new file, the stale rows are still visible (a superset).
    assert client.execute(
        "SELECT checksum FROM corpscout.ip_registry_snapshots FINAL WHERE source = 'ripencc'"
    ) == [(hashlib.md5(changed).hexdigest(),)]
    assert ripencc_rows(client, tables.SPECIAL_TABLE, final=True) == [
        "5.134.16.0",
        "85.8.248.0",
    ]
    # The same file again: identical to the ledger, but the stored rows do not match it.
    monkeypatch.setattr(assets, "delete_stale_rows", real_delete)
    result = refresh(resource)
    assert result.success
    metadata = result.asset_materializations_for_node(
        "ip_registry_special_segments_ripencc"
    )[0].metadata
    assert (metadata["loaded"].value, metadata["repaired"].value) == (True, True)
    for final in (True, False):
        assert ripencc_rows(client, tables.SPECIAL_TABLE, final=final) == ["5.134.16.0"]
        assert ripencc_rows(client, tables.HOLDER_TABLE, final=final) == ["25.0.0.0"]
    again = (
        refresh(resource)
        .asset_materializations_for_node("ip_registry_special_segments_ripencc")[0]
        .metadata
    )
    assert (again["loaded"].value, again["repaired"].value) == (False, False)


def test_a_failed_new_date_load_keeps_the_old_snapshot_and_a_retry_drops_its_leftovers(
    clean, monkeypatch
):
    client, resource = clean
    fixture_http(monkeypatch)
    assert refresh(resource).success
    # The failed attempt's file lists one more reserved range than the file of the retry.
    extra = (
        RIPENCC_EXCERPT.read_bytes()
        .replace(b"2|ripencc|1790287199|13|", b"2|ripencc|1790287199|14|", 1)
        .replace(b"ripencc|*|ipv4|*|9|summary", b"ripencc|*|ipv4|*|10|summary", 1)
        .replace(
            b"ripencc||ipv4|5.134.16.0|2048||reserved\n",
            b"ripencc||ipv4|5.134.16.0|2048||reserved\nripencc||ipv4|5.134.24.0|2048||reserved\n",
        )
    )
    fixture_http(monkeypatch, tamper=dated_ripencc(b"20260925", extra))
    real_insert_rows = fail_insert(monkeypatch, assets.HOLDER_INSERT_SQL)
    assert not refresh(resource).success
    assert client.execute(
        "SELECT snapshot_date FROM corpscout.ip_registry_current_snapshots WHERE source = 'ripencc'"
    ) == [(date(2026, 9, 24),)]
    assert client.execute(
        "SELECT snapshot_date, count() FROM corpscout.ip_registry_special_segments_current WHERE registry = 'ripencc' GROUP BY snapshot_date"
    ) == [(date(2026, 9, 24), 2)]
    assert client.execute(
        "SELECT count() FROM corpscout.ip_registry_special_segments WHERE registry = 'ripencc' AND snapshot_date = '2026-09-25'"
    ) == [(3,)]  # leftovers of the failed load, not current
    monkeypatch.setattr(assets, "insert_rows", real_insert_rows)
    fixture_http(monkeypatch, tamper=dated_ripencc(b"20260925"))
    assert refresh(resource).success
    assert client.execute(
        "SELECT snapshot_date FROM corpscout.ip_registry_current_snapshots WHERE source = 'ripencc'"
    ) == [(date(2026, 9, 25),)]
    for table, expected in (
        (tables.SPECIAL_TABLE, [("5.134.16.0",), ("85.8.248.0",)]),
        (tables.HOLDER_TABLE, [("25.0.0.0",), ("2a00::",)]),
    ):
        assert (
            client.execute(
                f"SELECT start_address FROM corpscout.{table} WHERE registry = 'ripencc' AND snapshot_date = '2026-09-25' ORDER BY start_address"
            )
            == expected
        )


def test_delegated_download_is_fetched_once_more_on_a_checksum_mismatch(monkeypatch):
    url = tables.RIR_SOURCES["ripencc"]
    new = RIPENCC_EXCERPT.read_bytes()
    md5 = f"MD5 (x) = {hashlib.md5(new).hexdigest()}\n".encode()
    answers = {url: [b"replaced meanwhile", new], url + ".md5": [md5, md5]}
    calls = []

    def fetch(requested):
        calls.append(requested)
        return answers[requested].pop(0), {}

    monkeypatch.setattr(assets, "fetch", fetch)
    assert assets.fetch_delegated(url) == (new, md5.decode())
    assert calls == [url, url + ".md5", url, url + ".md5"]


def test_downloads_identify_themselves_and_loaders_retry():
    class Response:
        content, headers = b"body", {"Last-Modified": IANA_LAST_MODIFIED}

        def raise_for_status(self):
            return None

    seen = {}

    def get(url, **kwargs):
        seen.update(url=url, **kwargs)
        return Response()

    original = assets.requests.get
    assets.requests.get = get
    try:
        assert assets.fetch("https://example.invalid/file") == (
            b"body",
            {"Last-Modified": IANA_LAST_MODIFIED},
        )
    finally:
        assets.requests.get = original
    assert seen["headers"] == {"User-Agent": "CorpScout ip-registry/1.0"}
    for asset in (assets.ip_registry_iana_blocks, *assets.special_segment_assets):
        assert asset.op.retry_policy == dg.RetryPolicy(max_retries=2, delay=300)
    assert assets.rdap_network_registry_class.op.retry_policy is None
