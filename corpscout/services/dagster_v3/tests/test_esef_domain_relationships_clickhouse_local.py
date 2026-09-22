import json
import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.common.domain_relationships import digest, model_settings

from dagster_v3.defs.esef_filings.domain_relationship_assets import pending_relationships_sql
from dagster_v3.defs.esef_filings.domain_relationships import ANALYSIS_COLUMNS, SYSTEM_PROMPT, RelationshipProfile
from tests.clickhouse_local import clickhouse_local_command, literal, render

pytestmark = pytest.mark.integration
MIGRATIONS = Path(__file__).parents[3] / "clickhouse/migrations"


def test_current_publication_and_pending_scan_respect_evidence_identity_and_failed_retries():
    evidence = json.dumps([{"source_context": {"version": "esef-domain-context-v1", "text": "Source"}}])
    params = {"context_version": "esef-domain-context-v1", "analysis_version": "esef-domain-relationships-v1",
              "prompt_hash": digest(SYSTEM_PROMPT), "model_config_json": model_settings(RelationshipProfile()),
              "max_domains": 25}
    schema = '''CREATE DATABASE corpscout;
CREATE TABLE corpscout.esef_domains (source_document_id String, registrable_domain String, package_sha256 String,
 lei String, period_end Date32, evidence_json String, extraction_status String) ENGINE=ReplacingMergeTree ORDER BY (source_document_id, registrable_domain);
CREATE TABLE corpscout.esef_filings (fxo_id String, package_sha256 String, lei String, entity_name String,
 viewer_url String, report_url String, package_url String) ENGINE=ReplacingMergeTree ORDER BY fxo_id;
CREATE TABLE corpscout.esef_entity_registry_map (lei String, registry_id String, country_iso2 String,
 link_status String) ENGINE=ReplacingMergeTree ORDER BY lei;
INSERT INTO corpscout.esef_filings VALUES ('doc','pkg','lei','Issuer','https://report','','');
INSERT INTO corpscout.esef_entity_registry_map VALUES ('lei','5560639147','SE','register_verified');
'''
    migration = "\n".join((MIGRATIONS / name).read_text() for name in (
        "000423_corpscout_esef_domain_relationships.up.sql",
        "000424_corpscout_esef_relationship_projection.up.sql",
    ))
    domain = f"INSERT INTO corpscout.esef_domains VALUES ('doc','example.com','pkg','lei','2024-12-31',{literal(evidence)},'ok');"
    insert = f'''INSERT INTO corpscout.esef_domain_relationship_analysis
    (attempt_id,source_document_id,registrable_domain,package_sha256,lei,reporting_entity_name,period_end,
     source_evidence_hash,input_hash,analysis_version,prompt_hash,model_config_json,status,statements_json,analyzed_at)
    VALUES ('1','doc','example.com','pkg','lei','Issuer','2024-12-31','{digest(evidence)}','{'0' * 64}',
        '{params['analysis_version']}','{params['prompt_hash']}',{literal(params['model_config_json'])},
        'success','[]','2026-09-18 00:00:00');'''
    pending = "SELECT count() FROM (" + render(pending_relationships_sql(scoped=False), params) + ") FORMAT JSONCompactEachRow;"
    current = "SELECT count(source_document_id), groupArray(registrable_domain) FROM corpscout.se_company_domain_relationships FORMAT JSONCompactEachRow;"
    statements = [schema, migration, domain, pending,
        render(pending_relationships_sql(scoped=False), params) + " FORMAT JSONEachRow;",
        insert, pending, current,
        "INSERT INTO corpscout.esef_domain_relationship_analysis SELECT * REPLACE ('2' AS attempt_id, 'http_error' AS status, toDateTime64('2026-09-18 01:00:00',6,'UTC') AS analyzed_at) FROM corpscout.esef_domain_relationship_analysis;",
        current, pending,
        "INSERT INTO corpscout.esef_domains SELECT * REPLACE ('[]' AS evidence_json) FROM corpscout.esef_domains FINAL;",
        current,
        "SELECT groupArray(name) FROM (SELECT name FROM system.columns WHERE database='corpscout' AND table='esef_domain_relationship_analysis' ORDER BY position) FORMAT JSONCompactEachRow;",
    ]
    result = subprocess.run(clickhouse_local_command(), input="\n".join(statements), capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    results = [json.loads(line) for line in result.stdout.splitlines()]
    selected = results.pop(1)
    assert selected == {
        "source_document_id": "doc", "package_sha256": "pkg", "lei": "lei",
        "reporting_entity_name": "Issuer", "period_end": "2024-12-31",
        "registrable_domain": "example.com", "source_url": "https://report", "evidence_json": evidence,
    }
    assert results == [[1], [0], [1, ["example.com"]], [1, ["example.com"]], [0], [0, []], [list(ANALYSIS_COLUMNS)]]
