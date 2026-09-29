"""Bucket SQL for domain_service_intervals: same answers as the per-domain views."""

import json
import subprocess
from pathlib import Path

from clickhouse_driver import Client

from dagster_v3.defs.dns_detect import sql
from tests.clickhouse_local import clickhouse_local_command
from tests.test_dns_detect_views import T1, T2, insert, resolution, result, rid

ROOT = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
SCHEMA = (ROOT / "000468_corpscout_dns_detect.up.sql").read_text() + (ROOT / "000470_corpscout_domain_service_intervals.up.sql").read_text()
_CLIENT = Client("localhost")


def render(query: str) -> str:
    return _CLIENT.substitute_params(query, {}, _CLIENT.connection.context)


def run(body: str) -> list[list]:
    out = subprocess.run(clickhouse_local_command(), input=SCHEMA + body, capture_output=True, text=True, timeout=300)
    assert out.returncode == 0, out.stderr
    return [json.loads(line) for line in out.stdout.splitlines() if line.strip()]


def bucket_of(domain: str) -> int:
    return run(f"SELECT cityHash64('{domain}') % 128 FORMAT JSONCompactEachRow;")[0][0]


def build(fixture: str, bucket: int) -> str:
    return (fixture
            + render(sql.intervals_insert_sql("corpscout", "domain_service_intervals", bucket)) + ";\n"
            + render(sql.counts_insert_sql("corpscout", "provider_service_counts", "domain_service_intervals", bucket)) + ";\n")


COLS = "service_type, provider_key, provider_slug, service_keys, first_seen, last_seen, evidence, analyzers, record_types"


def parity(domain: str, fixture: str) -> tuple[list, list]:
    body = build(fixture, bucket_of(domain))
    view = run(body + f"SELECT {COLS} FROM corpscout.domain_services_history(domain = '{domain}') ORDER BY ALL FORMAT JSONCompactEachRow;")
    table = run(body + f"SELECT {COLS} FROM corpscout.domain_service_intervals WHERE root_domain = '{domain}' ORDER BY ALL FORMAT JSONCompactEachRow;")
    return view, table


def test_intervals_equal_the_history_view_with_fallback_merge_and_gap() -> None:
    fixture = insert(
        [resolution(rid(1), "a.se", "NS", "ns", T1, "2025-01-01", "2026-09-01"),
         resolution(rid(2), "a.se", "SOA", "soa", T1, "2024-01-01", "2026-09-01"),
         resolution(rid(3), "a.se", "MX", "mx", T1, "2025-01-01", "2026-09-01")],
        [result(rid(1), "a.se", "NS", "ns", "dns", "loopia", "2025-01-01", "2025-06-01", T1),
         result(rid(1), "a.se", "NS", "ns", "dns", "loopia", "2025-06-20", "2025-09-01", T1),   # 19-day gap: merged
         result(rid(1), "a.se", "NS", "ns", "dns", "loopia", "2026-01-01", "2026-09-01", T1),   # 122-day gap: new period
         result(rid(2), "a.se", "SOA", "soa", "dns", "nsone.net", "2025-01-01", "2026-09-01", T1, fallback=1),
         result(rid(2), "a.se", "SOA", "soa", "dns", "binero", "2024-01-01", "2024-06-01", T1, fallback=1),
         result(rid(3), "a.se", "MX", "mx", "email", "google", "2025-01-01", "2026-09-01", T1)],
    )
    view, table = parity("a.se", fixture)
    assert table == view and len(table) == 4  # loopia x2, binero, google


def test_older_resolutions_are_ignored_like_the_view() -> None:
    fixture = insert(
        [resolution(rid(1), "b.se", "NS", "ns", T1, "2026-01-01", "2026-09-01"),
         resolution(rid(1), "b.se", "NS", "ns", T2, "2026-01-01", "2026-09-01")],
        [result(rid(1), "b.se", "NS", "ns", "dns", "old", "2026-01-01", "2026-09-01", T1),
         result(rid(1), "b.se", "NS", "ns", "dns", "new", "2026-01-01", "2026-09-01", T2)],
    )
    view, table = parity("b.se", fixture)
    assert table == view and [r[1] for r in table] == ["new"]


def test_soa_only_domain_keeps_its_fallback_interval() -> None:
    fixture = insert(
        [resolution(rid(1), "c.se", "SOA", "soa", T1, "2026-01-01", "2026-09-01")],
        [result(rid(1), "c.se", "SOA", "soa", "dns", "binero", "2026-01-01", "2026-09-01", T1, fallback=1)],
    )
    view, table = parity("c.se", fixture)
    assert table == view and len(table) == 1


def test_is_current_matches_domain_services_now() -> None:
    fixture = insert(
        [resolution(rid(1), "d.se", "NS", "ns", T1, "2025-01-01", "2026-09-01"),
         resolution(rid(2), "d.se", "MX", "mx", T1, "2025-01-01", "2026-09-01")],
        [result(rid(1), "d.se", "NS", "ns", "dns", "loopia", "2025-01-01", "2025-05-01", T1),
         result(rid(1), "d.se", "NS", "ns", "dns", "cloudflare", "2025-06-01", "2026-09-01", T1),
         result(rid(2), "d.se", "MX", "mx", "email", "google", "2025-01-01", "2026-09-01", T1)],
    )
    body = build(fixture, bucket_of("d.se"))
    now = run(body + "SELECT provider_key FROM corpscout.domain_services_now(domain = 'd.se') ORDER BY ALL FORMAT JSONCompactEachRow;")
    table = run(body + "SELECT provider_key FROM corpscout.domain_service_intervals WHERE root_domain = 'd.se' AND is_current ORDER BY ALL FORMAT JSONCompactEachRow;")
    assert table == now == [["cloudflare"], ["google"]]


def test_counts_per_service_type_and_totals_and_unmapped_keys() -> None:
    fixture = insert(
        [resolution(rid(1), "e.se", "NS", "ns", T1, "2026-01-01", "2026-09-01"),
         resolution(rid(2), "e.se", "MX", "mx", T1, "2026-01-01", "2026-09-01")],
        [result(rid(1), "e.se", "NS", "ns", "dns", "google", "2026-01-01", "2026-09-01", T1),
         result(rid(2), "e.se", "MX", "mx", "email", "google", "2026-01-01", "2026-09-01", T1)],
    )
    # an unmapped key: provider_slug '' (the fixture helper sets slug = key, so insert one by hand)
    fixture += ("INSERT INTO corpscout.dns_record_resolutions VALUES (unhex('00000000000000000000000000000009'), 'e.se', 'e.se', 'CNAME', 'cname', "
                "'r1', 'i1', 1, [], toDate('2026-01-01'), toDate('2026-09-01'), toDateTime64('2026-09-01 00:00:00', 3, 'UTC'));\n"
                "INSERT INTO corpscout.dns_record_services VALUES (unhex('00000000000000000000000000000009'), 'e.se', 'e.se', 'CNAME', 'cname', 's', "
                "'hosting', 'unmapped.net', '', '', 'r', 0.5, 0, toDate('2026-01-01'), toDate('2026-09-01'), toDateTime64('2026-09-01 00:00:00', 3, 'UTC'));\n")
    body = build(fixture, bucket_of("e.se"))
    rows = run(body + "SELECT provider_slug, provider_key, service_type, domains_now, domains_ever FROM corpscout.provider_service_counts ORDER BY ALL FORMAT JSONCompactEachRow;")
    assert rows == [["", "unmapped.net", "", 1, 1], ["", "unmapped.net", "hosting", 1, 1],
                    ["google", "google", "", 1, 1], ["google", "google", "dns", 1, 1], ["google", "google", "email", 1, 1]]


def test_current_results_count_ignores_records_re_resolved_to_nothing() -> None:
    fixture = insert(
        [resolution(rid(1), "f.se", "NS", "ns", T1, "2026-01-01", "2026-09-01"),
         resolution(rid(1), "f.se", "NS", "ns", T2, "2026-01-01", "2026-09-01")],
        [result(rid(1), "f.se", "NS", "ns", "dns", "old", "2026-01-01", "2026-09-01", T1)],
    )
    q = render(sql.current_results_count_sql("corpscout", bucket_of("f.se")))
    assert run(fixture + q + " FORMAT JSONCompactEachRow;") == [[0]]
