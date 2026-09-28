"""dns-detect tables and history views (migration 000467), run in clickhouse-local."""

import json
import subprocess
from pathlib import Path

from tests.clickhouse_local import clickhouse_local_command

MIGRATION = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations" / "000467_corpscout_dns_detect.up.sql"


def run(sql: str) -> list[list]:
    result = subprocess.run(clickhouse_local_command(), input=MIGRATION.read_text() + sql, capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def resolution(rid: str, domain: str, rtype: str, analyzer: str, at: str, rfrom: str, rto: str) -> str:
    return (
        f"({rid}, '{domain}', '{domain}', '{rtype}', '{analyzer}', 'r1', 'i1', 1, [], "
        f"toDate('{rfrom}'), toDate('{rto}'), toDateTime64('{at}', 3, 'UTC'))"
    )


def result(rid: str, domain: str, rtype: str, analyzer: str, stype: str, key: str, frm: str, to: str, at: str, fallback: int = 0) -> str:
    return (
        f"({rid}, '{domain}', '{domain}', '{rtype}', '{analyzer}', 'subj', '{stype}', '{key}', '{key}', '{key}.svc', "
        f"'rule', 1, {fallback}, toDate('{frm}'), toDate('{to}'), toDateTime64('{at}', 3, 'UTC'))"
    )


RES_COLS = "(record_id, root_domain, record_name, record_type, analyzer, rules_version, ip_version, result_count, findings, record_from, record_to, resolved_at)"
SVC_COLS = "(record_id, root_domain, record_name, record_type, analyzer, subject, service_type, provider_key, provider_slug, service_key, rule_id, confidence, fallback, valid_from, valid_to, resolved_at)"


def insert(resolutions: list[str], results: list[str]) -> str:
    sql = ""
    if resolutions:
        sql += f"INSERT INTO corpscout.dns_record_resolutions {RES_COLS} VALUES " + ", ".join(resolutions) + ";\n"
    if results:
        sql += f"INSERT INTO corpscout.dns_record_services {SVC_COLS} VALUES " + ", ".join(results) + ";\n"
    return sql


def rid(n: int) -> str:
    return f"unhex('{n:032x}')"


T1, T2 = "2026-09-01 00:00:00", "2026-09-20 00:00:00"


def test_current_keeps_only_each_records_latest_resolution() -> None:
    sql = insert(
        [resolution(rid(1), "a.se", "NS", "ns", T1, "2026-01-01", "2026-09-01"),
         resolution(rid(1), "a.se", "NS", "ns", T2, "2026-01-01", "2026-09-01")],
        [result(rid(1), "a.se", "NS", "ns", "dns", "oldprov", "2026-01-01", "2026-09-01", T1),
         result(rid(1), "a.se", "NS", "ns", "dns", "newprov", "2026-01-01", "2026-09-01", T2)],
    )
    rows = run(sql + "SELECT provider_key FROM corpscout.dns_record_services_current ORDER BY ALL FORMAT JSONCompactEachRow;")
    assert rows == [["newprov"]]


def test_record_re_resolved_to_nothing_hides_its_old_results() -> None:
    sql = insert(
        [resolution(rid(1), "a.se", "NS", "ns", T1, "2026-01-01", "2026-09-01"),
         resolution(rid(1), "a.se", "NS", "ns", T2, "2026-01-01", "2026-09-01")],
        [result(rid(1), "a.se", "NS", "ns", "dns", "oldprov", "2026-01-01", "2026-09-01", T1)],
    )
    assert run(sql + "SELECT count() FROM corpscout.dns_record_services_current FORMAT JSONCompactEachRow;") == [[0]]


def test_history_drops_fallback_where_ns_covers_and_keeps_it_elsewhere() -> None:
    sql = insert(
        [resolution(rid(1), "a.se", "NS", "ns", T1, "2026-03-01", "2026-09-01"),
         resolution(rid(2), "a.se", "SOA", "soa", T1, "2026-01-01", "2026-09-01")],
        [result(rid(1), "a.se", "NS", "ns", "dns", "loopia", "2026-03-01", "2026-09-01", T1),
         # SOA names another provider for the same whole period: suppressed where NS covers it.
         result(rid(2), "a.se", "SOA", "soa", "dns", "nsone.net", "2026-03-01", "2026-09-01", T1, fallback=1),
         # SOA before NS was ever seen: nothing covers it, so it stays.
         result(rid(2), "a.se", "SOA", "soa", "dns", "binero", "2026-01-01", "2026-02-01", T1, fallback=1)],
    )
    rows = run(sql + "SELECT provider_key, toString(first_seen), toString(last_seen) FROM corpscout.domain_services_history WHERE root_domain = 'a.se' ORDER BY ALL FORMAT JSONCompactEachRow;")
    assert rows == [["binero", "2026-01-01", "2026-02-01"], ["loopia", "2026-03-01", "2026-09-01"]]


def test_history_merges_gaps_under_45_days_and_keeps_real_gaps() -> None:
    res, svc = [], []
    windows = [("2026-01-01", "2026-01-10"), ("2026-02-05", "2026-02-10"),  # 26 days apart: merged
               ("2026-05-01", "2026-05-10")]                                  # 80 days later: a new interval
    for n, (f, t) in enumerate(windows, start=1):
        res.append(resolution(rid(n), "a.se", "NS", "ns", T1, f, t))
        svc.append(result(rid(n), "a.se", "NS", "ns", "dns", "loopia", f, t, T1))
    rows = run(insert(res, svc) + (
        "SELECT provider_key, toString(first_seen), toString(last_seen), evidence, analyzers "
        "FROM corpscout.domain_services_history ORDER BY first_seen FORMAT JSONCompactEachRow;"))
    assert rows == [
        ["loopia", "2026-01-01", "2026-02-10", 2, ["ns"]],
        ["loopia", "2026-05-01", "2026-05-10", 1, ["ns"]],
    ]


def test_now_keeps_intervals_reaching_the_latest_scan_of_their_record_type() -> None:
    sql = insert(
        [resolution(rid(1), "a.se", "NS", "ns", T1, "2026-01-01", "2026-03-01"),
         resolution(rid(2), "a.se", "NS", "ns", T1, "2026-06-01", "2026-09-20"),
         resolution(rid(3), "a.se", "MX", "mx", T1, "2026-01-01", "2026-09-20"),
         # A newer MX scan found a record that resolves to nothing (still counts as the latest MX scan).
         resolution(rid(4), "a.se", "MX", "mx", T1, "2026-09-25", "2026-09-25")],
        [result(rid(1), "a.se", "NS", "ns", "dns", "binero", "2026-01-01", "2026-03-01", T1),
         result(rid(2), "a.se", "NS", "ns", "dns", "loopia", "2026-06-01", "2026-09-20", T1),
         result(rid(3), "a.se", "MX", "mx", "email", "google", "2026-01-01", "2026-09-20", T1)],
    )
    rows = run(sql + "SELECT service_type, provider_key FROM corpscout.domain_services_now ORDER BY ALL FORMAT JSONCompactEachRow;")
    # Binero ended before the latest NS scan; Google's MX was absent from the latest MX scan.
    assert rows == [["dns", "loopia"]]
