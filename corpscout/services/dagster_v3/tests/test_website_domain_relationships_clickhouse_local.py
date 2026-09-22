import json
import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.common.domain_relationships import digest, model_settings
from dagster_v3.defs.website_relationships.assets import pending_relationships_sql
from dagster_v3.defs.website_relationships.relationships import ANALYSIS_COLUMNS, SYSTEM_PROMPT, WebsiteRelationshipProfile
from tests.clickhouse_local import clickhouse_local_command, literal, render

pytestmark = pytest.mark.integration


def test_website_attribution_cache_and_publication_against_real_clickhouse():
    evidence = json.dumps({"source_host": "issuer.com", "destination_domain": "nova.com",
                           "source_url": "https://issuer.com/partners", "fetched_at": "2026-09-18T10:00:00Z",
                           "context_version": "website-domain-context-v1", "surrounding_text": "Nova supplies us."})
    schema = '''CREATE DATABASE corpscout;
CREATE TABLE corpscout.company_domains_resolved (country_code String, company_id String, website_host String, is_active UInt8) ENGINE=MergeTree ORDER BY (country_code, company_id);
CREATE TABLE corpscout.se_company_basic_info (company_id String, legal_name String) ENGINE=ReplacingMergeTree ORDER BY company_id;
CREATE TABLE corpscout.website_crawl_results_latest (result_id String, domain String, result_kind String, source_path String, finished_at Nullable(DateTime64(6,'UTC')), external_links Nullable(String)) ENGINE=MergeTree ORDER BY result_id;
INSERT INTO corpscout.company_domains_resolved VALUES ('SE','123','issuer.com',1), ('SE','wrong','crawl-target.com',1), ('SE','123','issuer.com',1), ('SE','inactive','other.com',0);
INSERT INTO corpscout.se_company_basic_info VALUES ('123','Issuer'), ('wrong','Wrong company'), ('inactive','Other');
'''
    links = '[' + evidence + ',' + evidence.replace('issuer.com', 'other.com') + ']'
    insert_source = f"INSERT INTO corpscout.website_crawl_results_latest VALUES ('result','crawl-target.com','crawl','s3/path','2026-09-18 10:00:00',{literal(links)});"
    migration = (Path(__file__).parents[3] / 'clickhouse/migrations/000425_corpscout_website_domain_relationships.up.sql').read_text(encoding='utf-8')
    params = {"analysis_version": WebsiteRelationshipProfile().prompt_version, "prompt_hash": digest(SYSTEM_PROMPT),
              "model_config_json": model_settings(WebsiteRelationshipProfile()), "result_ids": ('result',), "max_domains": 25}
    pending = 'SELECT country_code,company_id,source_host FROM (' + render(pending_relationships_sql(scoped=True), params) + ') FORMAT JSONCompactEachRow;'
    publish = 'SELECT company_id,registrable_domain FROM corpscout.website_domain_relationships_current FORMAT JSONCompactEachRow;'
    insert_answer = f'''INSERT INTO corpscout.website_domain_relationship_analysis
    (attempt_id,result_id,crawl_domain,result_kind,source_path,source_host,country_code,company_id,
     reporting_entity_name,registrable_domain,captured_at,source_url,source_evidence_hash,
     analysis_version,prompt_hash,model_config_json,status,analyzed_at)
    SELECT '1', result_id,crawl_domain,result_kind,source_path,source_host,country_code,company_id,
     reporting_entity_name,registrable_domain,captured_at,source_url,lower(hex(SHA256(evidence_json))),
     {literal(params['analysis_version'])},{literal(params['prompt_hash'])},{literal(params['model_config_json'])},'success',now64(6)
    FROM corpscout.website_domain_relationship_inputs;'''
    statements = [schema, migration, insert_source, pending, insert_answer, pending, publish,
        "INSERT INTO corpscout.website_domain_relationship_analysis SELECT * REPLACE ('2' AS attempt_id,'http_error' AS status, now64(6) + INTERVAL 1 HOUR AS analyzed_at) FROM corpscout.website_domain_relationship_analysis;",
        publish,
        # Changed evidence invalidates the answer even with the same result identifier.
        f"INSERT INTO corpscout.website_crawl_results_latest VALUES ('result','crawl-target.com','crawl','s3/path','2026-09-18 10:00:00', {literal('[' + evidence.replace('Nova supplies us.', 'Nova no longer supplies us.') + ']')});",
        'SELECT count() FROM corpscout.website_domain_relationships_current FORMAT JSONCompactEachRow;',
        # Same numeric company id in another country makes the host ambiguous.
        "INSERT INTO corpscout.company_domains_resolved VALUES ('NO','123','issuer.com',1);",
        'SELECT count() FROM corpscout.website_domain_relationship_inputs FORMAT JSONCompactEachRow;',
        'SELECT count() FROM corpscout.website_domain_relationships_current FORMAT JSONCompactEachRow;',
        "SELECT groupArray(name) FROM (SELECT name FROM system.columns WHERE database='corpscout' AND table='website_domain_relationship_analysis' ORDER BY position) FORMAT JSONCompactEachRow;",
    ]
    result = subprocess.run(clickhouse_local_command(), input='\n'.join(statements), capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    assert [json.loads(line) for line in result.stdout.splitlines()] == [
        ['SE','123','issuer.com'], ['123','nova.com'], ['123','nova.com'], [0], [0], [0], [list(ANALYSIS_COLUMNS)],
    ]
