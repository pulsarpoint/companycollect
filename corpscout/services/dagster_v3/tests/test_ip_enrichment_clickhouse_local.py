"""Exercise the IP enrichment history and serving contract in ClickHouse."""

import json
import subprocess
from pathlib import Path

import pytest

from tests.clickhouse_local import clickhouse_local_command, literal


MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
MIGRATION = "000433_corpscout_ip_enrichment"


def test_ip_enrichment_history_current_and_rollback() -> None:
    up = (MIGRATIONS / f"{MIGRATION}.up.sql").read_text()
    down = (MIGRATIONS / f"{MIGRATION}.down.sql").read_text()
    sql = """
    SYSTEM STOP MERGES corpscout.ip_enrichment_results;
    INSERT INTO corpscout.ip_enrichment_input
        (task_id, input_id, ip, source_name, source_record_id)
    VALUES
        ('00000000-0000-0000-0000-000000000001', 'dns:1', '8.8.8.8', 'dns', '1'),
        ('00000000-0000-0000-0000-000000000002', 'crawl:2', '8.8.8.8', 'commoncrawl', '2');
    SELECT count(), uniqExact(ip), uniqExact(source_name)
    FROM corpscout.ip_enrichment_input FORMAT JSONCompactEachRow;

    INSERT INTO corpscout.ip_enrichment_results
        (ip, result_id, completed_at, city_lookup_status, asn_lookup_status,
         rdap_lookup_status, country_iso_code, city_name, asn, rdap_name,
         city_checked_at, asn_checked_at, rdap_checked_at)
    VALUES
        ('8.8.8.8', '00000000-0000-0000-0000-000000000001', '2026-09-01',
         'found', 'found', 'found', 'US', 'Old city', 15169, 'Old registration',
         '2026-09-01', '2026-09-01', '2026-09-01'),
        ('8.8.8.8', '00000000-0000-0000-0000-000000000002', '2026-09-02',
         'found', 'retryable_error', 'retryable_error', 'US', NULL, NULL, NULL,
         '2026-09-02', '2026-09-02', '2026-09-02'),
        ('2001:4860:4860::8888', '00000000-0000-0000-0000-000000000003', '2026-09-02',
         'retryable_error', 'not_attempted', 'retryable_error', NULL, NULL, NULL, NULL,
         '2026-09-02', NULL, '2026-09-02'),
        ('127.0.0.1', '00000000-0000-0000-0000-000000000004', '2026-09-02',
         'not_global', 'not_global', 'not_global', NULL, NULL, NULL, NULL,
         '2026-09-02', '2026-09-02', '2026-09-02');
    -- Replaying an identical result must not add logical history.
    INSERT INTO corpscout.ip_enrichment_results
    SELECT * EXCEPT (bucket, ip_version) FROM corpscout.ip_enrichment_results
    WHERE result_id = '00000000-0000-0000-0000-000000000002';
    SELECT count() FROM corpscout.ip_enrichment_results FINAL FORMAT JSONCompactEachRow;
    SELECT city_name, country_iso_code, asn, rdap_name,
           asn_lookup_status, asn_data_status, rdap_lookup_status, rdap_data_status,
           toString(asn_data_at)
    FROM corpscout.ip_enrichment_current WHERE ip='8.8.8.8' FORMAT JSONCompactEachRow;
    -- A view filter must not resurrect an older attempt whose status matches.
    SELECT count() FROM corpscout.ip_enrichment_current
    WHERE ip='8.8.8.8' AND rdap_lookup_status='found' FORMAT JSONCompactEachRow;
    SELECT ip_version, city_data_status, city_name FROM corpscout.ip_enrichment_current
    WHERE ip='2001:4860:4860::8888' FORMAT JSONCompactEachRow;
    SELECT city_lookup_status, rdap_lookup_status, country_iso_code
    FROM corpscout.ip_enrichment_current WHERE ip='127.0.0.1' FORMAT JSONCompactEachRow;

    INSERT INTO corpscout.ip_enrichment_results
        (ip, result_id, completed_at, city_lookup_status, asn_lookup_status, rdap_lookup_status)
    VALUES ('8.8.8.8', '00000000-0000-0000-0000-000000000005', '2026-09-03',
            'not_found', 'not_found', 'not_found');
    SELECT country_iso_code, asn, rdap_name, city_data_status, rdap_data_status
    FROM corpscout.ip_enrichment_current WHERE ip='8.8.8.8' FORMAT JSONCompactEachRow;
    -- Same completion time has a stable tie-breaker, independent of merge order.
    INSERT INTO corpscout.ip_enrichment_results
        (ip, result_id, completed_at, city_lookup_status, country_iso_code)
    VALUES ('8.8.8.8', '00000000-0000-0000-0000-000000000006', '2026-09-03', 'found', 'GB');
    SELECT country_iso_code, asn, rdap_name FROM corpscout.ip_enrichment_current
    WHERE ip='8.8.8.8' FORMAT JSONCompactEachRow;
    -- A later task reusing an older cache must not replace more recent lookup data.
    INSERT INTO corpscout.ip_enrichment_results
        (ip, result_id, completed_at, city_lookup_status, country_iso_code, city_checked_at)
    VALUES ('8.8.8.8', '00000000-0000-0000-0000-000000000007', '2026-09-04',
            'found', 'US', '2026-09-01');
    SELECT country_iso_code, toString(completed_at), toString(city_data_result_id)
    FROM corpscout.ip_enrichment_current WHERE ip='8.8.8.8' FORMAT JSONCompactEachRow;
    SELECT count() FROM corpscout.ip_enrichment_results FINAL FORMAT JSONCompactEachRow;
    """
    result = subprocess.run(
        clickhouse_local_command(),
        input=up
        + sql
        + up
        + down
        + down
        + """
        SELECT count() FROM system.tables WHERE database='corpscout' FORMAT JSONCompactEachRow;
        """,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert [json.loads(line) for line in result.stdout.splitlines()] == [
        [2, 1, 2],
        [4],
        [
            None,
            "US",
            15169,
            "Old registration",
            "retryable_error",
            "found",
            "retryable_error",
            "found",
            "2026-09-01 00:00:00.000000",
        ],
        [0],
        [6, "retryable_error", None],
        ["not_global", "not_global", None],
        [None, None, None, "not_found", "not_found"],
        ["GB", None, None],
        ["GB", "2026-09-04 00:00:00.000000", "00000000-0000-0000-0000-000000000006"],
        [7],
        [0],
    ]


@pytest.mark.parametrize("table", ["ip_enrichment_input", "ip_enrichment_results"])
@pytest.mark.parametrize("ip", ["not-an-ip", "2001:4860:4860:0:0:0:0:8888"])
def test_ip_enrichment_rejects_invalid_or_noncanonical_ips(table: str, ip: str) -> None:
    up = (MIGRATIONS / f"{MIGRATION}.up.sql").read_text()
    if table == "ip_enrichment_input":
        insert = f"""INSERT INTO corpscout.{table} (ip, input_id, source_name)
        VALUES ({literal(ip)}, 'record:ip', 'dns');"""
    else:
        insert = f"""INSERT INTO corpscout.{table} (ip, result_id)
        VALUES ({literal(ip)}, '00000000-0000-0000-0000-000000000001');"""
    result = subprocess.run(
        clickhouse_local_command(),
        input=up + insert,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode != 0
    assert "canonical_ip" in result.stderr and "VIOLATED_CONSTRAINT" in result.stderr
