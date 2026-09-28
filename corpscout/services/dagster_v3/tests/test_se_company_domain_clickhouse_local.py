"""Run the actual migration and every source's stable-state/withdrawal SQL on ClickHouse."""
import json
import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.basic_info.extract import ExtractConfig
from dagster_v3.defs.se_company.domain import tables
from dagster_v3.defs.se_company.domain.common_crawl import common_crawl_live_sql
from dagster_v3.defs.se_company.domain.esef import esef_live_sql
from dagster_v3.defs.se_company.domain.suggestions import run_domain_source
from dagster_v3.defs.se_company.domain.wikidata import wikidata_live_sql
from dagster_v3.defs.sweden_company.companies_current import DOMAINS_SET
from tests.clickhouse_local import clickhouse_local_command
from tests.company_domain_source_schema import claim_schema_sql
from tests.domain_sources_schema import execute_sql
from tests.identity_registration_support import identity_postgres as identity_postgres
from tests.test_commoncrawl_domain_graph_integration import graph_ch as graph_ch
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
)

pytestmark = pytest.mark.integration
MIGRATIONS = Path(__file__).parents[3] / "clickhouse/migrations"
COMPANY_ID = "5561552760"
SCHEMA = """
CREATE TABLE corpscout.se_company_basic_info (company_id String, legal_name String, lei String, wikidata_id String) ENGINE=ReplacingMergeTree ORDER BY company_id;
INSERT INTO corpscout.se_company_basic_info VALUES ('5561552760', 'Example AB', '', 'Q1');
CREATE TABLE corpscout.company_identifier (country_code String, company_id String, issuer_scheme String, issuer_id String, is_current UInt8) ENGINE=Memory;
CREATE TABLE corpscout.wikidata_company_identifiers (wikidata_id String, identifier_type String, identifier_value String) ENGINE=ReplacingMergeTree ORDER BY (wikidata_id, identifier_type, identifier_value);
INSERT INTO corpscout.wikidata_company_identifiers VALUES ('Q1', 'se_orgnr', '556155-2760');
CREATE TABLE corpscout.wikidata_company_domains (registry_id String, domain String, website_url String, website_host String, is_primary UInt8, confidence Float32, is_current UInt8, resolved_at DateTime64(3, 'UTC')) ENGINE=ReplacingMergeTree(resolved_at) ORDER BY (registry_id, domain);
INSERT INTO corpscout.wikidata_company_domains VALUES ('Q1','example.se','https://example.se','example.se',1,0.95,1,'2026-09-14 00:00:00');
CREATE TABLE corpscout.se_esef_domains (company_id String, source_document_id String, registrable_domain String, roles_json String, corroborated UInt8, evidence_count UInt32, evidence_json String, extracted_at DateTime64(3, 'UTC'), extraction_status String) ENGINE=Memory;
INSERT INTO corpscout.se_esef_domains VALUES ('5561552760', 'doc', 'example.se', '["company_website"]', 1, 2, '{"claim":"official company website"}', '2026-09-14 00:00:00', 'ok');
INSERT INTO corpscout.se_esef_domains VALUES ('5561552760', 'doc', 'auditor.se', '["auditor"]', 1, 2, '{}', '2026-09-14 00:00:00', 'ok');
CREATE TABLE corpscout.se_esef_filings (fxo_id String, viewer_url String, report_url String, package_url String) ENGINE=Memory;
INSERT INTO corpscout.se_esef_filings VALUES ('doc', 'https://filing.test/doc', '', '');
CREATE TABLE corpscout.company_domain_suggestions_active (country_iso2 String, company_id String, root_domain String, rank UInt32, total_score Float64, scoring_version String, candidate_sources Array(String), suggested_at DateTime64(3, 'UTC')) ENGINE=Memory;
INSERT INTO corpscout.company_domain_suggestions_active VALUES ('SE', '5561552760', 'candidate.se', 1, 85, 'v1', ['identity'], '2026-09-14 00:00:00');
CREATE TABLE corpscout.company_domain_suggestion_evidence_active (country_iso2 String, company_id String, root_domain String, signal_type String, source_field String, company_value String, domain_value String, score_contribution Float64, source_url String) ENGINE=Memory;
INSERT INTO corpscout.company_domain_suggestion_evidence_active VALUES ('SE', '5561552760', 'candidate.se', 'company_id', 'id', '5561552760', '5561552760', 50, 'https://candidate.se/about');
"""


def setup_sql():
    migration = (MIGRATIONS / "000269_corpscout_company_domains.up.sql").read_text()
    existing = migration[:migration.index(";", migration.index("CREATE TABLE")) + 1]
    seed = """
INSERT INTO corpscout.company_domains (country_code,company_id,root_domain,website_url,website_host,review_status,reviewed_by,reviewed_at,is_active,first_seen_at,last_seen_at,resolved_at)
VALUES ('SE','5561552760','legacy.se','https://legacy.se','legacy.se','confirmed_related','operator','2026-09-13 00:00:00',1,'2026-09-13 00:00:00','2026-09-13 00:00:00','2026-09-13 00:00:00');
"""
    return existing + seed + "\n" + (MIGRATIONS / "000408_corpscout_se_company_domain_entity.up.sql").read_text().split("CREATE ROLE")[0] + (MIGRATIONS / "000417_corpscout_se_company_domain_brave.up.sql").read_text() + (MIGRATIONS / "000461_corpscout_se_domain_readers.up.sql").read_text().split("CREATE OR REPLACE VIEW corpscout.website_domain_relationship_inputs")[0] + SCHEMA


@pytest.mark.parametrize("join_use_nulls", [0, 1])
def test_company_domain_presence_includes_unverified_sources_and_live_reviews(join_use_nulls):
    statements = [f"SET join_use_nulls={join_use_nulls};", setup_sql(), """
INSERT INTO corpscout.se_company_domain
    (company_id, root_domain, sources, source_confidences, source_record_ids, source_urls, confidence_bases, association, active, inactive_reason, folded_at)
VALUES
    ('5561552761', 'brave.se', ['brave'], [0.5], ['answer'], [''], ['source'], 'uncertain', 0, 'unverified', '2026-09-20 00:00:00'),
    ('5561552762', 'gone.se', [], [], [], [], [], 'uncertain', 0, 'withdrawn', '2026-09-20 00:00:00'),
    ('5561552763', 'unrelated.se', ['brave'], [0.5], ['answer'], [''], ['source'], 'not_connected', 0, 'rejected', '2026-09-20 00:00:00'),
    ('5561552764', 'rejected.se', ['brave'], [0.5], ['answer'], [''], ['source'], 'uncertain', 0, 'unverified', '2026-09-20 00:00:00');
INSERT INTO corpscout.se_company_domain_rule VALUES
    ('5561552764', 'rejected.se', 'rejected', 0, 'reviewer', '', '', '2026-09-20 00:01:00');
""", f"SELECT DISTINCT company_id FROM ({DOMAINS_SET}) ORDER BY company_id FORMAT JSONCompactEachRow;"]
    result = subprocess.run(clickhouse_local_command(), input="\n".join(statements), capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    assert [json.loads(line) for line in result.stdout.splitlines() if line.strip()] == [
        [COMPANY_ID], ["5561552761"],
    ]


@pytest.mark.parametrize("join_use_nulls", [0, 1])
def test_migration_source_sync_hashes_error_preservation_and_withdrawal(join_use_nulls, graph_ch, identity_postgres):
    client = graph_ch
    execute_sql(client, "DROP DATABASE corpscout SYNC; CREATE DATABASE corpscout;")
    execute_sql(client, setup_sql() + claim_schema_sql())
    client.execute(f"SET join_use_nulls={join_use_nulls}")
    def run(source, live):
        return run_domain_source(client, source=source, live_sql=live,
            config=ExtractConfig(execute=True), run_id="test", log=lambda *a: None)
    for source, live in [("wikidata", wikidata_live_sql), ("esef_filing", esef_live_sql), ("common_crawl_identity", common_crawl_live_sql)]:
        assert run(source, live)["inserted"] == 1
        assert run(source, live)["inserted"] == 0
    assert client.execute("SELECT count() FROM corpscout.se_company_domain_sources FINAL WHERE removed=0") == [(3,)]
    # Failed document extraction keeps prior evidence; successful empty input withdraws it.
    client.execute("TRUNCATE TABLE corpscout.se_esef_domains")
    client.execute("INSERT INTO corpscout.se_esef_domains VALUES ('5561552760','doc','','[]',0,0,'{}','2026-09-14 01:00:00','error')")
    assert run("esef_filing", esef_live_sql)["inserted"] == 0
    client.execute("TRUNCATE TABLE corpscout.se_esef_domains")
    assert run("esef_filing", esef_live_sql)["inserted"] == 1
    assert run("esef_filing", esef_live_sql)["inserted"] == 0
    assert client.execute("SELECT count() FROM corpscout.se_company_domain_sources FINAL WHERE removed=0") == [(2,)]
    assert client.execute("SELECT count() FROM corpscout.se_company_domain_sources s LEFT ANTI JOIN corpscout.domains d ON s.domain_id=d.domain_id") == [(0,)]
    assert client.execute("SELECT count() FROM corpscout.websites") == [(1,)]  # Only Wikidata supplied a real URL.
    for table, columns in [(tables.SOURCE_TABLE, tables.SOURCE_COLUMNS), (tables.MAIN_TABLE, tables.MAIN_COLUMNS), (tables.HISTORY_TABLE, tables.HISTORY_COLUMNS), (tables.VERIFICATION_TABLE, tables.VERIFICATION_COLUMNS)]:
        actual = client.execute("SELECT name FROM system.columns WHERE database='corpscout' AND table=%(table)s AND default_kind!='MATERIALIZED' ORDER BY position", {"table": table})
        assert tuple(name for (name,) in actual) == columns
