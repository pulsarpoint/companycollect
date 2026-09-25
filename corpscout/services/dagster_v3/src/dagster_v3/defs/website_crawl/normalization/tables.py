"""Migration 452 export contracts. ClickHouse DDL remains migration-owned."""

PARSER_VERSION = "1"

SCHEMAS = {
    "scans": """domain String
crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3)
request_id String
attempt UInt32
normalization_id UUID
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
    "site_profiles": """domain String
crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3)
request_id String
attempt UInt32
normalization_id UUID
page_id String
row_index UInt32
source_url String
source_locator String
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
    "business_activities": """domain String
crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3)
request_id String
attempt UInt32
normalization_id UUID
page_id String
row_index UInt32
source_url String
source_locator String
activity_original String
scope LowCardinality(String)
classification_evidence Array(String)""",
    "pages": """domain String
crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3)
request_id String
attempt UInt32
normalization_id UUID
page_id String
row_index UInt32
source_url String
source_locator String
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
    "contacts": """domain String
crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3)
request_id String
attempt UInt32
normalization_id UUID
page_id String
row_index UInt32
source_url String
source_locator String
contact_type LowCardinality(String)
value String
raw_value Nullable(String)
extraction_source LowCardinality(String)
entity_reference Nullable(String)""",
    "identifiers": """domain String
crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3)
request_id String
attempt UInt32
normalization_id UUID
page_id String
row_index UInt32
source_url String
source_locator String
identifier_type LowCardinality(String)
value String
raw_value Nullable(String)
extraction_source LowCardinality(String)
entity_reference Nullable(String)""",
    "structured_data": """domain String
crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3)
request_id String
attempt UInt32
normalization_id UUID
page_id String
row_index UInt32
source_url String
source_locator String
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
    "links": """domain String
crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3)
request_id String
attempt UInt32
normalization_id UUID
page_id String
row_index UInt32
source_url String
source_locator String
target_url String
label_original Nullable(String)
category LowCardinality(String)
document_type Nullable(String)
language Nullable(String)
relationships Array(String)""",
    "jobs": """domain String
crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3)
request_id String
attempt UInt32
normalization_id UUID
page_id String
row_index UInt32
source_url String
source_locator String
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

COLUMNS = {
    table: tuple(line.split(" ", 1)[0] for line in schema.splitlines())
    for table, schema in SCHEMAS.items()
}


def new_row(table: str, **values) -> dict:
    """Complete the explicit export shape, preserving nulls and absent sections."""
    unexpected = values.keys() - set(COLUMNS[table])
    if unexpected:
        raise ValueError(f"Unknown {table} columns: {sorted(unexpected)}")
    row = {}
    for line in SCHEMAS[table].splitlines():
        name, kind = line.split(" ", 1)
        if name in values:
            row[name] = values[name]
        elif kind.startswith("Nullable("):
            row[name] = None
        elif kind.startswith("Array("):
            row[name] = []
        elif kind.startswith("Map("):
            row[name] = {}
        elif kind in {"UInt32", "UInt64"}:
            row[name] = 0
        elif kind == "Bool":
            row[name] = False
        elif kind in {"String", "LowCardinality(String)"}:
            row[name] = ""
        else:
            raise ValueError(f"Missing required {table}.{name}")
    return row


def duckdb_schema(table: str) -> str:
    """Translate the pinned native types for the temporary typed staging boundary."""
    columns = []
    for line in SCHEMAS[table].splitlines():
        name, kind = line.split(" ", 1)
        nullable = kind.startswith("Nullable(")
        if nullable:
            kind = kind[9:-1]
        if kind.startswith("Enum8(") or kind == "LowCardinality(String)":
            kind = "VARCHAR"
        elif kind.startswith("DateTime64("):
            kind = "TIMESTAMPTZ"
        else:
            kind = {
                "String": "VARCHAR",
                "UInt16": "USMALLINT",
                "UInt32": "UINTEGER",
                "UInt64": "UBIGINT",
                "Bool": "BOOLEAN",
                "Float64": "DOUBLE",
                "UUID": "UUID",
                "Array(String)": "VARCHAR[]",
                "Map(String, UInt64)": "MAP(VARCHAR, UBIGINT)",
            }[kind]
        columns.append(f'"{name}" {kind}' + ("" if nullable else " NOT NULL"))
    return ", ".join(columns)
