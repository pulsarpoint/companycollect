"""Exercise the removal guard and live domain readers against ClickHouse."""
import json
import subprocess
from pathlib import Path

import pytest

from tests.clickhouse_local import clickhouse_local_command
from tests.test_se_company_domain_clickhouse_local import setup_sql as country_setup_sql
from tests.domain_sources_schema import central_schema_sql

MIGRATIONS = Path(__file__).parents[3] / "clickhouse/migrations"
DROP = (MIGRATIONS / "000464_corpscout_remove_company_domains.up.sql").read_text()
OLD_BUILDS = """
CREATE TABLE corpscout.company_domain_current (country_code String, company_id String, root_domain String) ENGINE=Memory;
CREATE TABLE corpscout.company_domains_build (root_domain String) ENGINE=Memory;
CREATE TABLE corpscout.company_domain_current_build (root_domain String) ENGINE=Memory;
"""


def setup_sql():
    return country_setup_sql() + central_schema_sql() + (MIGRATIONS / "000462_corpscout_domains_sources.up.sql").read_text() + (MIGRATIONS / "000463_corpscout_domain_source_readers.up.sql").read_text()


def test_removal_keeps_evidence_and_live_reviews_queryable():
    result = subprocess.run(clickhouse_local_command(), input=setup_sql() + OLD_BUILDS + DROP + """
SELECT count() FROM system.tables WHERE database='corpscout' AND name IN
    ('company_domains','company_domain_current','company_domains_build','company_domain_current_build','company_domains_resolved')
FORMAT JSONCompactEachRow;
SELECT company_id, root_domain, review_status, is_active FROM corpscout.se_company_domain_resolved FORMAT JSONCompactEachRow;
INSERT INTO corpscout.se_company_domain_rule VALUES ('5561552760','legacy.se','rejected',0,'operator','','','2026-09-28 10:00:00');
SELECT review_status,is_active FROM corpscout.se_company_domain_resolved FORMAT JSONCompactEachRow;
INSERT INTO corpscout.se_company_domain_rule VALUES ('5561552760','legacy.se','confirmed_primary',0,'operator','','','2026-09-28 10:01:00');
SELECT review_status,is_active,suggested_primary FROM corpscout.se_company_domain_resolved FORMAT JSONCompactEachRow;
""", capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    # throwIf returns zero for each successful guard.
    assert [json.loads(line) for line in result.stdout.splitlines()] == [
        0, 0, 0, 0, [0], ['5561552760', 'legacy.se', 'confirmed_related', 1],
        ['rejected', 0], ['confirmed_primary', 1, 1],
    ]


@pytest.mark.parametrize("extra, message", [
    ("INSERT INTO corpscout.company_domains (country_code,company_id,root_domain) VALUES ('NO','123','new.no');", "remaining company_domains associations"),
    ("INSERT INTO corpscout.company_domain_current VALUES ('SE','5561552760','missing.se');", "remaining company_domain_current associations"),
    ("INSERT INTO corpscout.company_domains SELECT * REPLACE ('rejected' AS review_status,toDateTime64('2026-09-28 11:00:00',3,'UTC') AS reviewed_at,toDateTime64('2026-09-28 11:00:00',3,'UTC') AS resolved_at) FROM corpscout.company_domains FINAL;", "remaining company_domains reviews"),
])
def test_removal_refuses_to_discard_unmigrated_data(extra, message):
    result = subprocess.run(clickhouse_local_command(), input=setup_sql() + OLD_BUILDS + extra + DROP,
                            capture_output=True, text=True, timeout=120)
    assert result.returncode != 0
    assert message in result.stderr
