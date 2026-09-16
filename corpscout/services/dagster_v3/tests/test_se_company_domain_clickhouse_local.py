"""Run the actual migration and every source's stable-state/withdrawal SQL on ClickHouse."""
import json
from pathlib import Path
import subprocess

import pytest

from dagster_v3.defs.se_company import state_scan
from dagster_v3.defs.se_company.basic_info.extract import insert_page_sql
from dagster_v3.defs.se_company.domain import tables
from dagster_v3.defs.se_company.domain.common_crawl import common_crawl_live_sql
from dagster_v3.defs.se_company.domain.esef import esef_live_sql
from dagster_v3.defs.se_company.domain.wikidata import wikidata_live_sql
from dagster_v3.defs.se_company.domain.suggestions import TARGET, source_scan
from tests.clickhouse_local import clickhouse_local_command, render

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
    return existing + seed + "\n" + (MIGRATIONS / "000408_corpscout_se_company_domain_entity.up.sql").read_text().split("CREATE ROLE")[0] + (MIGRATIONS / "000417_corpscout_se_company_domain_brave.up.sql").read_text() + SCHEMA


@pytest.mark.parametrize("join_use_nulls", [0, 1])
def test_migration_source_sync_hashes_error_preservation_and_withdrawal(join_use_nulls):
    statements = [f"SET join_use_nulls={join_use_nulls};", setup_sql()]
    params = {"company_ids": (COMPANY_ID,), "source_run_id": "test", "extractor_version": "v1"}
    statements.append("SELECT 'imported-review', review_status, is_active FROM corpscout.company_domains_resolved WHERE company_id='5561552760' FORMAT JSONCompactEachRow;")
    expected = [["imported-review", "confirmed_related", 1]]
    statements.append("INSERT INTO corpscout.se_company_domain_rule VALUES ('5561552760','legacy.se','rejected',0,'reviewer','unrelated','hash','2026-09-14 00:00:00');")
    statements.append("SELECT 'live-review', review_status, is_active FROM corpscout.company_domains_resolved WHERE company_id='5561552760' FORMAT JSONCompactEachRow;")
    expected.append(["live-review", "rejected", 0])
    for source, live in [("wikidata", wikidata_live_sql), ("esef_filing", esef_live_sql), ("common_crawl_identity", common_crawl_live_sql)]:
        scan = source_scan(source)
        changed = state_scan.changed_scope_sql(scan, source=source, live_sql=live(scoped=False))
        selected = state_scan.select_sql(scan, source=source, live_sql=live(scoped=True))
        statements.append(f"SELECT 'new-{source}', count() FROM ({changed}) FORMAT JSONCompactEachRow;")
        expected.append([f"new-{source}", 1])
        statements.append(render(insert_page_sql(select_sql=selected, target=TARGET), params) + ";")
        statements.append(f"SELECT 'idle-{source}', count() FROM ({changed}) FORMAT JSONCompactEachRow;")
        expected.append([f"idle-{source}", 0])
    statements.append("SELECT 'domains', count() FROM corpscout.se_company_domain_suggestion FINAL WHERE removed=0 FORMAT JSONCompactEachRow;")
    expected.append(["domains", 3])
    # Failed document extraction retains its last good observation; successful empty
    # extraction withdraws it, while the independent Wikidata claim remains intact.
    statements.extend(["TRUNCATE TABLE corpscout.se_esef_domains;", "INSERT INTO corpscout.se_esef_domains VALUES ('5561552760','doc','','[]',0,0,'{}','2026-09-14 01:00:00','error');"])
    changed = state_scan.changed_scope_sql(source_scan("esef_filing"), source="esef_filing", live_sql=esef_live_sql(scoped=False))
    statements.append(f"SELECT 'failed-document', count() FROM ({changed}) FORMAT JSONCompactEachRow;")
    expected.append(["failed-document", 0])
    statements.append("TRUNCATE TABLE corpscout.se_esef_domains;")
    statements.append(f"SELECT 'withdraw', count() FROM ({changed}) FORMAT JSONCompactEachRow;")
    expected.append(["withdraw", 1])
    selected = state_scan.select_sql(source_scan("esef_filing"), source="esef_filing", live_sql=esef_live_sql(scoped=True))
    statements.append(render(insert_page_sql(select_sql=selected, target=TARGET), params) + ";")
    statements.append(f"SELECT 'withdrawn-idle', count() FROM ({changed}) FORMAT JSONCompactEachRow;")
    expected.append(["withdrawn-idle", 0])
    statements.append("SELECT 'remaining', count() FROM corpscout.se_company_domain_suggestion FINAL WHERE removed=0 FORMAT JSONCompactEachRow;")
    expected.append(["remaining", 2])
    # Runtime insertion contracts match migration order, including audit snapshots.
    for table, columns in [(tables.SUGGESTION_TABLE, tables.SUGGESTION_COLUMNS), (tables.MAIN_TABLE, tables.MAIN_COLUMNS), (tables.HISTORY_TABLE, tables.HISTORY_COLUMNS), (tables.VERIFICATION_TABLE, tables.VERIFICATION_COLUMNS)]:
        statements.append(f"SELECT '{table}', groupArray(name) FROM (SELECT name FROM system.columns WHERE database='corpscout' AND table='{table}' ORDER BY position) FORMAT JSONCompactEachRow;")
        expected.append([table, list(columns)])
    result = subprocess.run(clickhouse_local_command(), input="\n".join(statements), capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    actual = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    for row in actual:
        if isinstance(row[1], str) and row[1].isdigit():
            row[1] = int(row[1])
    assert actual == expected
