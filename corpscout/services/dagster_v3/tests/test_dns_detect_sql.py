"""dns-detect candidate selection, run against a real ClickHouse engine."""

import json
import subprocess
from pathlib import Path

from clickhouse_driver import Client

from dagster_v3.defs.dns_detect import sql
from tests.clickhouse_local import clickhouse_local_command

MIGRATION = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations" / "000468_corpscout_dns_detect.up.sql"
DNS_STORE = """
CREATE TABLE corpscout.commoncrawl_domain_dns_records (`record_id` FixedString(16), `root_domain` String, `name` String, `record_type` SimpleAggregateFunction(any, LowCardinality(String)), `record_type_code` UInt16, `record_class_code` UInt16, `value` SimpleAggregateFunction(any, String), `rdata_wire` SimpleAggregateFunction(any, String), `priority` SimpleAggregateFunction(any, UInt16), `sources` SimpleAggregateFunction(groupUniqArrayArray, Array(String)), `discoveries` SimpleAggregateFunction(groupUniqArrayArray, Array(String)), `seen_dates` SimpleAggregateFunction(groupUniqArrayArray, Array(Date)), `first_seen` SimpleAggregateFunction(min, DateTime64(3, 'UTC')), `last_seen` SimpleAggregateFunction(max, DateTime64(3, 'UTC')), `last_loaded_at` SimpleAggregateFunction(max, DateTime64(3, 'UTC'))) ENGINE = AggregatingMergeTree PARTITION BY cityHash64(root_domain) % 16 ORDER BY (root_domain, name, record_type_code, record_class_code, record_id);
"""
DOMAIN = "example.se"
SEEN = "'2026-08-01 00:00:00', '2026-09-20 00:00:00'"

# (id, name, type, value)
RECORDS = [
    (1, "example.se", "NS", "ns1.loopia.se."),                        # never resolved
    (2, "example.se", "MX", "10 mx.loopia.se."),                      # resolved, current
    (3, "example.se", "SOA", "ns1.loopia.se. h. 1 2 3 4 5"),          # resolved under old rules
    (4, "example.se", "A", "192.0.2.1"),                              # ip analyzer, old ip version
    (5, "www.example.se", "CNAME", "example.se."),                    # cname, old ip version only: still current
    (6, "example.se", "TXT", '"apple-domain-verification=x"'),        # resolved, but its window grew since
    (7, "bounce.example.se", "TXT", '"v=spf1 include:x.net -all"'),   # SPF below the apex
    (8, "shop.example.se", "A", "192.0.2.2"),                         # not routable
    (9, "s1._domainkey.example.se", "CNAME", "dkim.x.net."),          # DKIM
    (10, "example.se", "CAA", '0 issue "letsencrypt.org"'),           # type the resolver ignores
    (11, "example.se", "TXT", ""),                                    # blank value: would wedge the bucket
]


def driver_render(query: str, params: dict) -> str:
    """Substitute parameters exactly as production does (clickhouse-driver's
    %-formatting), so a stray % in the SQL fails here too. No connection is made."""
    client = Client("localhost")
    return client.substitute_params(query, params, client.connection.context)


def hexid(n: int) -> str:
    return f"{n:032x}"


def setup() -> str:
    rows = ", ".join(
        f"(unhex('{hexid(i)}'), '{DOMAIN}', '{name}', '{rtype}', '{value.replace(chr(39), chr(92) + chr(39))}', {SEEN})"
        for i, name, rtype, value in RECORDS
    )
    res = lambda i, name, rtype, analyzer, rules, ip, rto: (  # noqa: E731
        f"(unhex('{hexid(i)}'), '{DOMAIN}', '{name}', '{rtype}', '{analyzer}', '{rules}', '{ip}', 0, [], "
        f"toDate('2026-08-01'), toDate('{rto}'), now64(3))"
    )
    resolutions = ", ".join([
        res(2, "example.se", "MX", "mx", "R", "I", "2026-09-20"),
        res(3, "example.se", "SOA", "soa", "R-old", "I", "2026-09-20"),
        res(4, "example.se", "A", "ip", "R", "I-old", "2026-09-20"),
        res(5, "www.example.se", "CNAME", "cname", "R", "I-old", "2026-09-20"),
        res(6, "example.se", "TXT", "txt", "R", "I", "2026-09-10"),
    ])
    return (
        MIGRATION.read_text() + DNS_STORE
        + f"INSERT INTO corpscout.commoncrawl_domain_dns_records (record_id, root_domain, name, record_type, value, first_seen, last_seen) VALUES {rows};\n"
        + "INSERT INTO corpscout.dns_record_resolutions (record_id, root_domain, record_name, record_type, analyzer, rules_version, ip_version, "
        + f"result_count, findings, record_from, record_to, resolved_at) VALUES {resolutions};\n"
    )


def run(text: str) -> list[list]:
    result = subprocess.run(clickhouse_local_command(), input=text, capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def bucket() -> int:
    return run(f"SELECT cityHash64('{DOMAIN}') % 128 FORMAT JSONCompactEachRow;")[0][0]


def test_candidates_are_routable_unresolved_or_stale_records() -> None:
    query = driver_render(sql.candidates_sql("corpscout", bucket()), {"rules_version": "R", "ip_version": "I"})
    rows = run(setup() + query + " ORDER BY record_id FORMAT JSONCompactEachRow;")
    got = {int(r[0], 16): r for r in rows}
    assert sorted(got) == [1, 3, 4, 6, 7, 9]
    # The record fields travel as dns-detect expects them.
    assert got[1][1:] == ["example.se", "example.se", "NS", "ns1.loopia.se.", "2026-08-01", "2026-09-20"]


def test_other_buckets_select_nothing() -> None:
    other = (bucket() + 1) % sql.PARTITION_COUNT
    query = driver_render(sql.candidates_sql("corpscout", other), {"rules_version": "R", "ip_version": "I"})
    assert run(setup() + query + " FORMAT JSONCompactEachRow;") == []
