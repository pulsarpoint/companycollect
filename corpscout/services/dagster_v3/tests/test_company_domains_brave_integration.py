"""Execute the selection SQL and Copy interception against their real runtimes."""

import json
import subprocess

import pytest
from pathlib import Path


from dagster_v3.defs.company_domains.publication import EXPORT_COLUMNS
from tests.clickhouse_local import clickhouse_local_command

pytestmark = pytest.mark.integration
MIGRATION = (
    Path(__file__).parents[3]
    / "clickhouse/migrations/000414_corpscout_se_company_brave_domains.up.sql"
)


def test_current_answers_keep_latest_success_per_company_and_query_type():
    # The S3 engine is exercised separately against a real server and object store.
    sql = (
        MIGRATION.read_text().split("-- Full immutable responses", 1)[0]
        + """
INSERT INTO corpscout.se_company_brave_search_results_latest_success (result_id,company_id,country_code,status,query_type,answer_text,completed_at)
VALUES ('new','1','SE','success','website','Latest','2026-09-15 12:00:00'),
('old','1','SE','success','website','Previous','2026-09-14 12:00:00'),
('owner','1','SE','success','owner','Owner answer','2026-09-14 12:00:00');
SELECT result_id,answer_text FROM corpscout.se_company_brave_search_results_latest_success FINAL ORDER BY query_type FORMAT JSONCompactEachRow;
SELECT name FROM system.columns WHERE database='corpscout' AND table='se_company_brave_search_results_latest_success' ORDER BY position FORMAT JSONCompactEachRow;
"""
    )
    result = subprocess.run(
        clickhouse_local_command(),
        input=sql,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stderr
    assert [
        json.loads(line) for line in result.stdout.splitlines() if line.strip()
    ] == [
        ["owner", "Owner answer"],
        ["new", "Latest"],
        *[[column] for column in EXPORT_COLUMNS],
        ["archive_path"],
    ]
