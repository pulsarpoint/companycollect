CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.esef_source_documents_v2
(
    source_document_id          String,
    document_type               LowCardinality(String),
    lei                         String,
    entity_name                 String,
    country_iso2                LowCardinality(String),
    company_id                  String,
    period_end                  String,
    fiscal_year                 UInt16,
    package_url                 String,
    report_url                  String,
    viewer_url                  String,
    package_sha256              String,
    package_object_key          String,
    package_size_bytes          UInt64,
    parsed_artifact_object_key  String,
    artifact_schema_version     UInt16,
    parser_name                 LowCardinality(String),
    parser_version              String,
    archive_status              LowCardinality(String),
    extraction_status           LowCardinality(String),
    fact_count                  UInt32,
    text_fact_count             UInt32,
    numeric_fact_count          UInt32,
    contact_candidate_count     UInt32,
    website_candidate_count     UInt32,
    validation_error_count      UInt32,
    validation_warning_count    UInt32,
    source_processed_at         String,
    source_run_id               String,
    extracted_at                String,
    processed_week              Date,
    resolved_at                 DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY processed_week
ORDER BY (processed_week, source_document_id);

CREATE TABLE IF NOT EXISTS corpscout.esef_facts_v2
(
    lei                 String,
    fxo_id              String,
    period_end          Date32,
    fact_id             String,
    concept_qname       String,
    concept_namespace   LowCardinality(String),
    concept_local_name  String,
    period_start        Nullable(Date32),
    period_instant      Nullable(Date32),
    period_duration_end Nullable(Date32),
    unit                LowCardinality(String),
    currency            LowCardinality(String),
    value_kind          LowCardinality(String),
    raw_value           String,
    amount_original     Nullable(Decimal128(2)),
    decimals            Nullable(Int32),
    dimensions          String,
    language            LowCardinality(String),
    source_run_id       String,
    processed_week      Date,
    resolved_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY processed_week
ORDER BY (processed_week, lei, period_end, fxo_id, fact_id);

CREATE TABLE IF NOT EXISTS corpscout.esef_document_contact_candidates_v2
(
    candidate_id                FixedString(64),
    source_document_id          String,
    package_sha256              String,
    lei                         String,
    country_iso2                LowCardinality(String),
    company_id                  String,
    period_end                  String,
    fiscal_year                 UInt16,
    candidate_kind              LowCardinality(String),
    normalized_value            String,
    country_code                LowCardinality(String),
    registrable_domain          String,
    hosts_json                  String,
    normalized_urls_json        String,
    suggested_roles_json        String,
    evidence_json               String,
    evidence_count              UInt16,
    extractor_versions_json     String,
    source_run_id               String,
    extracted_at                String,
    processed_week              Date,
    resolved_at                 DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY processed_week
ORDER BY (
    processed_week,
    source_document_id,
    candidate_kind,
    normalized_value,
    candidate_id
);

CREATE TABLE IF NOT EXISTS corpscout.esef_document_concept_labels_v2
(
    label_id               FixedString(64),
    source_document_id     String,
    source_record_uid      FixedString(64) DEFAULT lower(hex(SHA256(concat('company-source-record-v1\nfile\nesef_report_package\n', lowerUTF8(package_sha256))))),
    package_sha256         FixedString(64),
    lei                    String,
    country_iso2           LowCardinality(String),
    company_id             String,
    period_end             String,
    fiscal_year            UInt16,
    concept_qname          String,
    concept_namespace_uri  String,
    concept_local_name     String,
    is_extension           Bool,
    label_role             LowCardinality(String),
    language               LowCardinality(String),
    label                  String,
    is_report_language     Bool,
    source_run_id          String,
    extracted_at           String,
    processed_week         Date,
    resolved_at            DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY processed_week
ORDER BY (
    processed_week,
    source_document_id,
    concept_namespace_uri,
    concept_local_name,
    label_role,
    language,
    label_id
);

CREATE TABLE IF NOT EXISTS corpscout.esef_fact_disclosures_v2
(
    disclosure_id       FixedString(64),
    source_document_id  String,
    source_record_uid   FixedString(64),
    source_fact_id      String,
    package_sha256      FixedString(64),
    lei                 String,
    country_iso2        LowCardinality(String),
    company_id          String,
    period_end          String,
    fiscal_year         UInt16,
    concept_qname       String,
    concept_local_name  String,
    language            LowCardinality(String),
    raw_value_sha256    FixedString(64),
    parser_name         LowCardinality(String),
    parser_version      String,
    blocks_json         String,
    plain_text          String,
    block_count         UInt32,
    table_count         UInt32,
    source_run_id       String,
    extracted_at        String,
    processed_week      Date,
    resolved_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY processed_week
ORDER BY (processed_week, source_document_id, source_fact_id, disclosure_id);
