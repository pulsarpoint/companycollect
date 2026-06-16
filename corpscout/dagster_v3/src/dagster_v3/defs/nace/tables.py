NACE_DATABASE = "reference"
NACE_CATEGORIES_TABLE = "nace_categories"
QUALIFIED_NACE_CATEGORIES_TABLE = f"{NACE_DATABASE}.{NACE_CATEGORIES_TABLE}"

NACE_CATEGORIES_COLUMNS = (
    "classification_version",
    "code",
    "normalized_code",
    "parent_code",
    "level",
    "section_code",
    "description_en",
    "concept_uri",
    "parent_concept_uri",
    "source_scheme_uri",
    "source_url",
    "source_payload_hash",
    "valid_from",
    "valid_to",
    "is_current",
    "source_run_id",
    "pulled_at",
    "_dlt_load_id",
    "_dlt_id",
)

NACE_CATEGORIES_DDL = f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED_NACE_CATEGORIES_TABLE}
(
    classification_version LowCardinality(String),
    code String,
    normalized_code String,
    parent_code Nullable(String),
    level LowCardinality(String),
    section_code Nullable(String),
    description_en String,
    concept_uri String,
    parent_concept_uri Nullable(String),
    source_scheme_uri String,
    source_url String,
    source_payload_hash FixedString(64),
    valid_from Date,
    valid_to Nullable(Date),
    is_current UInt8,
    source_run_id String,
    pulled_at DateTime64(3, 'UTC'),
    _dlt_load_id String,
    _dlt_id String
)
ENGINE = MergeTree
ORDER BY (classification_version, normalized_code)
"""
