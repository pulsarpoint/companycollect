"""Real ClickHouse contracts: publication, history, typed facts and additive rollback."""

import json
import subprocess
from pathlib import Path

import pytest

from tests.clickhouse_local import clickhouse_local_command

MIGRATION = "000452_corpscout_website_crawl_normalized"
DIRECTORY = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
FIRST = "00000000-0000-0000-0000-000000000001"
SECOND = "00000000-0000-0000-0000-000000000002"
THIRD = "00000000-0000-0000-0000-000000000003"
IDENTITY = """domain String
crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3)
request_id String
attempt UInt32
normalization_id UUID"""
DETAIL = IDENTITY + """
page_id String
row_index UInt32
source_url String
source_locator String"""
SCHEMAS = {
    "scans": IDENTITY + """
normalization_revision UInt64
parser_version String
source_schema String
source_ingested_at DateTime64(6, 'UTC')
normalized_at DateTime64(6, 'UTC')
source_run_id String
normalization_run_id String
input_revision UInt64
work_key String
website_url String
final_url Nullable(String)
state Enum8('completed' = 1, 'failed' = 2, 'cancelled' = 3)
crawl_status LowCardinality(String)
successful Bool
started_at Nullable(DateTime64(6, 'UTC'))
finished_at DateTime64(6, 'UTC')
stop_reason Nullable(String)
error Nullable(String)
archive_path String
page_count UInt32
row_counts Map(String, UInt64)
structured_jobs_status Enum8('not_available' = 1, 'completed' = 2, 'partial' = 3, 'failed' = 4)""",
    "site_profiles": DETAIL + """
evidence_status LowCardinality(String)
crawl_decision LowCardinality(String)
scope LowCardinality(String)
site_types Array(String)
research_profiles Array(String)
purpose_original Nullable(String)
operator_name Nullable(String)
description_original Nullable(String)
evidence Array(String)
full_crawl_all Nullable(Bool)
classification_overridden Nullable(Bool)""",
    "business_activities": DETAIL + """
activity_original String
scope LowCardinality(String)
classification_evidence Array(String)""",
    "pages": DETAIL + """
requested_url String
final_url Nullable(String)
canonical_url Nullable(String)
http_status Nullable(UInt16)
fetch_status LowCardinality(String)
observation_status Nullable(String)
observation_schema Nullable(String)
fetched_at Nullable(DateTime64(6, 'UTC'))
title_original Nullable(String)
description_original Nullable(String)
language Nullable(String)
errors Array(String)""",
    "contacts": DETAIL + """
contact_type LowCardinality(String)
value String
raw_value Nullable(String)
extraction_source LowCardinality(String)
entity_reference Nullable(String)""",
    "identifiers": DETAIL + """
identifier_type LowCardinality(String)
value String
raw_value Nullable(String)
extraction_source LowCardinality(String)
entity_reference Nullable(String)""",
    "structured_data": DETAIL + """
script_index Nullable(UInt32)
entity_path String
entity_id Nullable(String)
entity_types Array(String)
property_path String
array_index Nullable(UInt32)
value_type Enum8('string' = 1, 'number' = 2, 'boolean' = 3, 'null' = 4)
value_string Nullable(String)
value_number Nullable(Float64)
value_number_original Nullable(String)
value_boolean Nullable(Bool)
parse_status LowCardinality(String)""",
    "links": DETAIL + """
target_url String
label_original Nullable(String)
category LowCardinality(String)
document_type Nullable(String)
language Nullable(String)
relationships Array(String)""",
    "jobs": DETAIL + """
source_job_id Nullable(String)
job_url Nullable(String)
title_original String
employer Nullable(String)
location_original Nullable(String)
department_original Nullable(String)
employment_type Nullable(String)
workplace_type Nullable(String)
description_original Nullable(String)
posted_at Nullable(DateTime64(6, 'UTC'))
posted_at_original Nullable(String)
expires_at Nullable(DateTime64(6, 'UTC'))
expires_at_original Nullable(String)
extraction_source Enum8('jsonld' = 1, 'legacy_record' = 2)
evidence Array(String)""",
}


def run_sql(sql):
    result = subprocess.run(
        clickhouse_local_command(), input=sql, capture_output=True,
        text=True, timeout=120, check=False,
    )
    assert result.returncode == 0, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines()]


def migration():
    return (DIRECTORY / f"{MIGRATION}.up.sql").read_text()


def scan(norm=FIRST, *, request="request-1", revision=1, crawl_type="full",
         state="completed", status="finished", success=True, day=20, attempt=1):
    return f"""INSERT INTO corpscout.website_crawl_scans
        (domain,crawl_type,request_id,attempt,normalization_id,
         normalization_revision,parser_version,state,crawl_status,successful,finished_at)
        VALUES ('example.se','{crawl_type}','{request}',{attempt},'{norm}',
          {revision},'1','{state}','{status}',{int(success)},'2026-09-{day} 10:00:00');"""


def contact(norm=FIRST, *, request="request-1", value="old@example.se", crawl_type="full"):
    return f"""INSERT INTO corpscout.website_crawl_contacts
        (domain,crawl_type,request_id,attempt,normalization_id,page_id,row_index,
         source_url,contact_type,value)
        VALUES ('example.se','{crawl_type}','{request}',1,'{norm}','p0001',1,
                'https://example.se/','email','{value}');"""


def test_migration_files_exist():
    assert (DIRECTORY / f"{MIGRATION}.up.sql").exists()
    assert (DIRECTORY / f"{MIGRATION}.down.sql").exists()


@pytest.mark.parametrize("table", SCHEMAS)
def test_native_schema_and_sort_key(table):
    rows = run_sql(migration() + f"""
        DESCRIBE TABLE corpscout.website_crawl_{table} FORMAT JSONEachRow;
        SELECT engine, sorting_key, partition_key FROM system.tables
        WHERE database='corpscout' AND name='website_crawl_{table}' FORMAT JSONEachRow;
    """)
    assert [(r["name"], r["type"]) for r in rows[:-1]] == [
        tuple(line.split(" ", 1)) for line in SCHEMAS[table].splitlines()
    ]
    assert rows[-1] == {
        "engine": "ReplacingMergeTree", "partition_key": "",
        "sorting_key": "domain, crawl_type, request_id, attempt" + (
            "" if table == "scans" else ", normalization_id, page_id, row_index"
        ),
    }


def test_unpublished_rows_and_duplicate_retries_are_hidden():
    rows = run_sql(migration() + contact() + contact() + """
        SELECT count() FROM corpscout.website_crawl_contacts_published FORMAT JSONCompactEachRow;
    """ + scan() + """
        SELECT value FROM corpscout.website_crawl_contacts_current FORMAT JSONCompactEachRow;
        SELECT count() FROM corpscout.website_crawl_contacts_published FORMAT JSONCompactEachRow;
    """)
    assert rows == [[0], ["old@example.se"], [1]]


def test_reparse_publishes_all_or_none_and_late_old_revision_does_not_return():
    rows = run_sql(migration() + contact() + scan() + contact(SECOND, value="new@example.se") + """
        SELECT value FROM corpscout.website_crawl_contacts_current FORMAT JSONCompactEachRow;
    """ + scan(SECOND, revision=2) + scan(FIRST, revision=1) + """
        SELECT value FROM corpscout.website_crawl_contacts_published FORMAT JSONCompactEachRow;
    """ + scan(THIRD, revision=3) + """
        SELECT count() FROM corpscout.website_crawl_contacts_current FORMAT JSONCompactEachRow;
        SELECT normalization_revision FROM corpscout.website_crawl_scans_latest FORMAT JSONCompactEachRow;
    """)
    assert rows == [["old@example.se"], ["new@example.se"], [0], [3]]


@pytest.mark.parametrize("state,status,success", [
    ("failed", "failed", False), ("cancelled", "cancelled", False),
    ("completed", "partial", False), ("completed", "needs_review", False),
    ("completed", "skip_crawling", True),
])
def test_new_unusable_full_attempt_preserves_previous_good_data(state, status, success):
    rows = run_sql(migration() + scan() + contact() + scan(
        SECOND, request="request-2", day=25, state=state, status=status, success=success,
    ) + contact(SECOND, request="request-2", value="unusable@example.se") + """
        SELECT request_id, crawl_status FROM corpscout.website_crawl_scans_latest FORMAT JSONCompactEachRow;
        SELECT request_id FROM corpscout.website_crawl_scans_latest_usable FORMAT JSONCompactEachRow;
        SELECT value FROM corpscout.website_crawl_contacts_current FORMAT JSONCompactEachRow;
        SELECT count() FROM corpscout.website_crawl_contacts_published FORMAT JSONCompactEachRow;
    """)
    assert rows == [["request-2", status], ["request-1"], ["old@example.se"], [2]]


def test_latest_usable_resolves_revision_before_filtering_success():
    rows = run_sql(migration() + scan() + contact() + scan(
        SECOND, revision=2, state="failed", status="failed", success=False,
    ) + """
        SELECT count() FROM corpscout.website_crawl_scans_latest_usable FORMAT JSONCompactEachRow;
        SELECT count() FROM corpscout.website_crawl_contacts_current FORMAT JSONCompactEachRow;
    """)
    assert rows == [[0], [0]]


def test_basic_skipped_profile_and_job_crawl_are_independent_from_full():
    rows = run_sql(migration() + scan() + scan(
        SECOND, crawl_type="site_info", status="skip_crawling", day=25,
    ) + scan(THIRD, crawl_type="jobs", day=24) + """
        SELECT crawl_type, request_id FROM corpscout.website_crawl_scans_latest_usable
        ORDER BY crawl_type FORMAT JSONCompactEachRow;
    """)
    assert rows == [["full", "request-1"], ["jobs", "request-1"], ["site_info", "request-1"]]


def test_classification_activities_and_jobs_retain_values_and_unknowns():
    identity = f"'example.se','full','request-1',1,'{FIRST}','p0001',1"
    rows = run_sql(migration() + scan() + f"""
        INSERT INTO corpscout.website_crawl_site_profiles
            (domain,crawl_type,request_id,attempt,normalization_id,page_id,row_index,
             purpose_original,site_types,research_profiles,evidence,evidence_status,crawl_decision)
        VALUES ({identity},'Sell truck accessories',['company','online_store'],['general'],['Source quotation'],'source_matched','skip_crawling');
        INSERT INTO corpscout.website_crawl_business_activities
            (domain,crawl_type,request_id,attempt,normalization_id,page_id,row_index,activity_original,classification_evidence)
        VALUES ({identity},'Retail of vehicle accessories',['Source quotation']);
        INSERT INTO corpscout.website_crawl_jobs
            (domain,crawl_type,request_id,attempt,normalization_id,page_id,row_index,title_original,employer,extraction_source,evidence)
        VALUES ({identity},'Engineer','Actual employer','jsonld',['Hiring engineer']);
        SELECT purpose_original,site_types,research_profiles,full_crawl_all,classification_overridden
        FROM corpscout.website_crawl_site_profiles_current FORMAT JSONCompactEachRow;
        SELECT activity_original,classification_evidence FROM corpscout.website_crawl_business_activities_current FORMAT JSONCompactEachRow;
        SELECT title_original,employer,posted_at,expires_at,description_original
        FROM corpscout.website_crawl_jobs_current FORMAT JSONCompactEachRow;
        SELECT structured_jobs_status FROM corpscout.website_crawl_scans_latest FORMAT JSONCompactEachRow;
    """)
    assert rows == [
        ["Sell truck accessories", ["company", "online_store"], ["general"], None, None],
        ["Retail of vehicle accessories", ["Source quotation"]],
        ["Engineer", "Actual employer", None, None, None], ["not_available"],
    ]


@pytest.mark.parametrize("values", [
    "'string','text',NULL,NULL,NULL", "'number',NULL,12.5,'12.5',NULL",
    "'boolean',NULL,NULL,NULL,true", "'null',NULL,NULL,NULL,NULL",
])
def test_structured_scalar_types(values):
    rows = run_sql(migration() + scan() + f"""
        INSERT INTO corpscout.website_crawl_structured_data
        (domain,crawl_type,request_id,attempt,normalization_id,page_id,row_index,
         value_type,value_string,value_number,value_number_original,value_boolean)
        VALUES ('example.se','full','request-1',1,'{FIRST}','p0001',1,{values});
        SELECT count() FROM corpscout.website_crawl_structured_data_current FORMAT JSONCompactEachRow;
    """)
    assert rows == [[1]]


def test_structured_number_retains_exact_source_text():
    rows = run_sql(migration() + scan() + f"""
        INSERT INTO corpscout.website_crawl_structured_data
        (domain,crawl_type,request_id,attempt,normalization_id,page_id,row_index,
         value_type,value_number,value_number_original)
        VALUES ('example.se','full','request-1',1,'{FIRST}','p0001',1,
                'number',9007199254740993,'9007199254740993');
        SELECT value_number_original FROM corpscout.website_crawl_structured_data_current FORMAT JSONCompactEachRow;
    """)
    assert rows == [["9007199254740993"]]


def test_skipped_full_crawl_updates_profile_without_replacing_deep_data():
    rows = run_sql(migration() + scan() + contact() + scan(
        SECOND, request="request-2", status="skip_crawling", day=25,
    ) + f"""
        INSERT INTO corpscout.website_crawl_site_profiles
            (domain,crawl_type,request_id,attempt,normalization_id,page_id,row_index,
             evidence_status,crawl_decision,site_types)
        VALUES ('example.se','full','request-1',1,'{FIRST}','p0001',1,
                'source_matched','continue_crawling',['company']),
               ('example.se','full','request-2',1,'{SECOND}','p0001',1,
                'source_matched','skip_crawling',['online_store']);
        INSERT INTO corpscout.website_crawl_business_activities
            (domain,crawl_type,request_id,attempt,normalization_id,page_id,row_index,activity_original)
        VALUES ('example.se','full','request-2',1,'{SECOND}','p0001',1,'Online retail');
        SELECT site_types FROM corpscout.website_crawl_site_profiles_current FORMAT JSONCompactEachRow;
        SELECT activity_original FROM corpscout.website_crawl_business_activities_current FORMAT JSONCompactEachRow;
        SELECT value FROM corpscout.website_crawl_contacts_current FORMAT JSONCompactEachRow;
    """)
    assert rows == [[["online_store"]], ["Online retail"], ["old@example.se"]]


@pytest.mark.parametrize("fragment", [
    scan(attempt=0), scan(revision=0), scan(norm="00000000-0000-0000-0000-000000000000"),
    scan(state="failed", success=True), scan(status="needs_review", success=True),
    scan(request=""), scan(crawl_type="invalid"),
    f"""INSERT INTO corpscout.website_crawl_structured_data
        (domain,crawl_type,request_id,attempt,normalization_id,page_id,row_index,value_type,value_string,value_number)
        VALUES ('example.se','full','request-1',1,'{FIRST}','p0001',1,'string','text',12);""",
])
def test_invalid_contracts_are_rejected(fragment):
    result = subprocess.run(clickhouse_local_command(), input=migration() + fragment,
        capture_output=True, text=True, timeout=120, check=False)
    assert result.returncode != 0
    assert "CONSTRAINT" in result.stderr or "Unknown element" in result.stderr


def test_up_down_up_preserves_existing_result_tables():
    down = (DIRECTORY / f"{MIGRATION}.down.sql").read_text()
    rows = run_sql("""CREATE DATABASE corpscout;
        CREATE TABLE corpscout.website_full_crawl_results (value String) ENGINE=MergeTree ORDER BY value;
        INSERT INTO corpscout.website_full_crawl_results VALUES ('preserved');
    """ + migration() + migration() + down + """
        SELECT name FROM system.tables WHERE database='corpscout' FORMAT JSONCompactEachRow;
        SELECT value FROM corpscout.website_full_crawl_results FORMAT JSONCompactEachRow;
    """ + migration() + """
        SELECT count() FROM system.tables WHERE database='corpscout' AND engine='ReplacingMergeTree' FORMAT JSONCompactEachRow;
    """)
    assert rows == [["website_full_crawl_results"], ["preserved"], [9]]
