"""The provider-recon loader SQL, run against a real ClickHouse engine."""

import json
import re
import subprocess
from datetime import datetime
from pathlib import Path

from dagster_v3.defs.provider_recon import sql, tables
from tests.clickhouse_local import clickhouse_local_command, render

REPO = Path(__file__).resolve().parents[3]
MIGRATION = REPO / "clickhouse" / "migrations" / "000460_corpscout_provider_recon.up.sql"
FIXTURE = Path(__file__).parent / "fixtures" / "provider_recon" / "aws_latest.json"
T1 = datetime(2026, 9, 28, 3, 12, 30)
T2 = datetime(2026, 9, 29, 3, 12, 30)


def schema_without_s3() -> str:
    """The migration's tables, with the S3 mapping swapped for a Memory table."""
    statements = [s.strip() for s in MIGRATION.read_text().split(";") if s.strip()]
    kept = []
    for s in statements:
        if tables.DOCUMENTS_S3_TABLE in s and "ENGINE = S3" in s:
            kept.append(f"CREATE TABLE corpscout.{tables.DOCUMENTS_S3_TABLE} (json String) ENGINE = Memory")
        else:
            kept.append(s)
    return ";\n".join(kept) + ";"


def run(sql_text: str) -> list[list]:
    result = subprocess.run(clickhouse_local_command(), input=sql_text, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def insert_doc(doc: dict) -> str:
    raw = json.dumps(doc).replace("\\", "\\\\").replace("'", "\\'")
    return f"INSERT INTO corpscout.{tables.DOCUMENTS_S3_TABLE} VALUES ('{raw}');\n"


def load(at: datetime) -> str:
    return "".join(render(stmt, {"loaded_at": at}) + ";\n" for _, stmt in sql.load_statements("corpscout"))


def fixture() -> dict:
    return json.loads(FIXTURE.read_text())


def test_load_normalises_services_ranges_and_rules() -> None:
    rows = run(
        schema_without_s3()
        + insert_doc(fixture())
        + load(T1)
        + """
SELECT provider_slug, provider_name, service_key, service_types, provider_keys, toString(collected_at) FROM corpscout.provider_services FINAL FORMAT JSONCompactEachRow;
SELECT cidr, ip_family, toString(range_start), toString(range_end), status, toString(first_seen), toString(removed_at), removal_action FROM corpscout.provider_ip_ranges FINAL ORDER BY cidr, first_seen FORMAT JSONCompactEachRow;
SELECT kind, rule_key, status, removal_action FROM corpscout.provider_rules FINAL ORDER BY kind FORMAT JSONCompactEachRow;
SELECT cidr FROM corpscout.provider_ip_ranges_current ORDER BY cidr FORMAT JSONCompactEachRow;
"""
    )
    assert rows[0] == ["aws", "Amazon Web Services", "aws.cloudfront", ["cdn"], ["amazonaws.com", "cloudfront.net"], "2026-09-28 03:12:05.000"]
    ranges = rows[1:5]
    assert ranges == [
        ["13.32.0.0/15", 4, "::ffff:13.32.0.0", "::ffff:13.33.255.255", "removed", "2026-06-01", "2026-06-18", "grace_expired"],
        ["13.32.0.0/15", 4, "::ffff:13.32.0.0", "::ffff:13.33.255.255", "active", "2026-07-01", None, ""],
        ["2600:9000::/28", 6, "2600:9000::", "2600:900f:ffff:ffff:ffff:ffff:ffff:ffff", "missing", "2026-09-27", None, ""],
        ["52.84.0.0/15", 4, "::ffff:52.84.0.0", "::ffff:52.85.255.255", "active", "2026-09-27", None, ""],
    ]
    rules = rows[5:9]
    assert rules == [
        ["asn", "AS16509", "active", ""],
        ["dns", "CNAME target suffix cloudfront.net", "active", ""],
        ["http", "header x-amz-cf-id exists ", "removed", "definition_removed"],
        ["ptr", "suffix cloudfront.net", "active", ""],
    ]
    assert rows[9:] == [["13.32.0.0/15"], ["2600:9000::/28"], ["52.84.0.0/15"]]


def test_reload_is_idempotent_after_final() -> None:
    rows = run(
        schema_without_s3()
        + insert_doc(fixture())
        + load(T1)
        + load(T2)
        + """
SELECT count(), toString(max(loaded_at)) FROM corpscout.provider_ip_ranges FINAL FORMAT JSONCompactEachRow;
SELECT count() FROM corpscout.provider_rules FINAL FORMAT JSONCompactEachRow;
"""
    )
    assert rows == [[4, "2026-09-29 03:12:30.000"], [4]]


def test_load_keeps_rows_absent_from_later_documents() -> None:
    later = fixture()
    ranges = later["services"][0]["evidence"]["ip_ranges"]
    later["services"][0]["evidence"]["ip_ranges"] = [r for r in ranges if r["first_seen"] != "2026-06-01"]
    rows = run(
        schema_without_s3()
        + insert_doc(fixture())
        + load(T1)
        + f"TRUNCATE TABLE corpscout.{tables.DOCUMENTS_S3_TABLE};\n"
        + insert_doc(later)
        + load(T2)
        + """
SELECT status, toString(removed_at), toString(loaded_at) FROM corpscout.provider_ip_ranges FINAL WHERE first_seen = '2026-06-01' FORMAT JSONCompactEachRow;
"""
    )
    assert rows == [["removed", "2026-06-18", "2026-09-28 03:12:30.000"]]


def test_documents_count_sql() -> None:
    rows = run(schema_without_s3() + insert_doc(fixture()) + sql.documents_count_sql("corpscout") + " FORMAT JSONCompactEachRow;")
    assert rows == [[1]]


def test_insert_columns_match_the_migration() -> None:
    ddl = MIGRATION.read_text()
    for table, stmt in sql.load_statements("corpscout"):
        body = re.search(rf"CREATE TABLE IF NOT EXISTS corpscout\.{table}\s*\((.*?)\)\s*ENGINE", ddl, re.S).group(1)
        columns = [line.strip().split()[0] for line in body.strip().splitlines() if line.strip()]
        inserted = re.search(r"INSERT INTO `?\w+`?\.`?\w+`?\s*\((.*?)\)", stmt, re.S).group(1)
        assert [c.strip() for c in inserted.split(",")] == columns, table
