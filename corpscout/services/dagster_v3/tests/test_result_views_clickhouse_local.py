"""Result history, latest attempt and latest success are distinct query contracts."""

import json
import subprocess
from pathlib import Path

import pytest

from tests.clickhouse_local import clickhouse_local_command


@pytest.mark.parametrize("kind", ["full_crawl", "jobs_crawl", "site_info"])
def test_crawl_latest_views_preserve_history_and_configuration(kind: str) -> None:
    migrations = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
    up = (migrations / "000430_corpscout_website_crawl_type_results.up.sql").read_text()
    down = (migrations / "000430_corpscout_website_crawl_type_results.down.sql").read_text()
    table = f"corpscout.website_{kind}_results"
    result = subprocess.run(
        clickhouse_local_command(),
        input=up + f"""
        SYSTEM STOP MERGES {table};
        INSERT INTO {table}
            (domain, request_id, attempt, work_key, state, successful, finished_at, ingested_at)
        VALUES
            ('example.org', 'first', 1, 'config-a', 'completed', true, '2026-09-01', '2026-09-01'),
            ('example.org', 'retry', 1, 'config-a', 'completed', true, '2026-09-02', '2026-09-02'),
            ('example.org', 'other', 1, 'config-b', 'completed', true, '2026-09-03', '2026-09-03');
        -- A corrected write for the same attempt supersedes its older successful version.
        INSERT INTO {table}
            (domain, request_id, attempt, work_key, state, successful, finished_at, ingested_at)
        VALUES ('example.org', 'retry', 1, 'config-a', 'failed', false, '2026-09-02', '2026-09-04');
        SELECT count() FROM {table} FINAL FORMAT JSONCompactEachRow;
        SELECT work_key, request_id, successful FROM {table}_latest
            ORDER BY work_key FORMAT JSONCompactEachRow;
        SELECT work_key, request_id FROM {table}_latest_success
            ORDER BY work_key FORMAT JSONCompactEachRow;
        -- Filtering a latest view must not resurrect an older successful attempt.
        SELECT work_key FROM {table}_latest WHERE successful
            ORDER BY work_key FORMAT JSONCompactEachRow;
        """ + up + down + down + """
        SELECT count() FROM system.tables WHERE database='corpscout' FORMAT JSONCompactEachRow;
        """,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert [json.loads(line) for line in result.stdout.splitlines()] == [
        [3],
        ["config-a", "retry", False],
        ["config-b", "other", True],
        ["config-a", "first"],
        ["config-b", "other"],
        ["config-b"],
        [0],
    ]
