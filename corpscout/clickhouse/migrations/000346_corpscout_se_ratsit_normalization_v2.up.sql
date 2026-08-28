-- Additive rollout for ratsit-normalizer-v2. Legacy columns and the
-- people-at-address table remain until v2 has been materialized and audited.
CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_ratsit_company_industry_codes
    ADD COLUMN IF NOT EXISTS source_industry_code Nullable(String)
        CODEC(ZSTD(3)) AFTER industry_description,
    ADD COLUMN IF NOT EXISTS source_industry_code_set LowCardinality(String)
        DEFAULT '' AFTER source_industry_code,
    ADD COLUMN IF NOT EXISTS industry_description_original Nullable(String)
        CODEC(ZSTD(3)) AFTER source_industry_code_set,
    ADD COLUMN IF NOT EXISTS nace_revision LowCardinality(String)
        DEFAULT '' AFTER industry_description_original,
    ADD COLUMN IF NOT EXISTS nace_code Nullable(String)
        CODEC(ZSTD(3)) AFTER nace_revision,
    ADD COLUMN IF NOT EXISTS nace_normalized_code Nullable(String)
        CODEC(ZSTD(3)) AFTER nace_code,
    ADD COLUMN IF NOT EXISTS nace_mapping_method LowCardinality(String)
        DEFAULT '' AFTER nace_normalized_code,
    ADD COLUMN IF NOT EXISTS nace_mapping_status LowCardinality(String)
        DEFAULT '' AFTER nace_mapping_method,
    ADD CONSTRAINT se_ratsit_industry_nace_mapping CHECK
        nace_mapping_status = ''
        OR (
            source_industry_code_set = 'SNI_2025'
            AND nace_revision = 'NACE_REV_2_1'
            AND nace_mapping_method = 'sni_four_digit_prefix'
            AND nace_mapping_status IN ('mapped', 'unmapped')
            AND match(ifNull(source_industry_code, ''), '^[0-9]{5}$')
            AND match(ifNull(nace_normalized_code, ''), '^[0-9]{4}$')
        );

ALTER TABLE corpscout.se_ratsit_establishments
    ADD COLUMN IF NOT EXISTS source_industry_code Nullable(String)
        CODEC(ZSTD(3)) AFTER industry_description,
    ADD COLUMN IF NOT EXISTS source_industry_code_set LowCardinality(String)
        DEFAULT '' AFTER source_industry_code,
    ADD COLUMN IF NOT EXISTS industry_description_original Nullable(String)
        CODEC(ZSTD(3)) AFTER source_industry_code_set,
    ADD COLUMN IF NOT EXISTS nace_revision LowCardinality(String)
        DEFAULT '' AFTER industry_description_original,
    ADD COLUMN IF NOT EXISTS nace_code Nullable(String)
        CODEC(ZSTD(3)) AFTER nace_revision,
    ADD COLUMN IF NOT EXISTS nace_normalized_code Nullable(String)
        CODEC(ZSTD(3)) AFTER nace_code,
    ADD COLUMN IF NOT EXISTS nace_mapping_method LowCardinality(String)
        DEFAULT '' AFTER nace_normalized_code,
    ADD COLUMN IF NOT EXISTS nace_mapping_status LowCardinality(String)
        DEFAULT '' AFTER nace_mapping_method,
    ADD COLUMN IF NOT EXISTS employee_count_min Nullable(UInt32)
        AFTER number_of_employees,
    ADD COLUMN IF NOT EXISTS employee_count_max Nullable(UInt32)
        AFTER employee_count_min,
    ADD COLUMN IF NOT EXISTS employee_count_open_ended Bool
        DEFAULT false AFTER employee_count_max,
    ADD CONSTRAINT se_ratsit_establishment_nace_mapping CHECK
        nace_mapping_status = ''
        OR (
            source_industry_code_set = 'SNI_2025'
            AND nace_revision = 'NACE_REV_2_1'
            AND nace_mapping_method = 'sni_four_digit_prefix'
            AND nace_mapping_status IN ('mapped', 'unmapped')
            AND match(ifNull(source_industry_code, ''), '^[0-9]{5}$')
            AND match(ifNull(nace_normalized_code, ''), '^[0-9]{4}$')
        ),
    ADD CONSTRAINT se_ratsit_establishment_employee_range CHECK
        (
            employee_count_min IS NULL
            AND employee_count_max IS NULL
            AND employee_count_open_ended = false
        )
        OR (
            employee_count_min IS NOT NULL
            AND employee_count_max IS NOT NULL
            AND employee_count_max >= employee_count_min
            AND employee_count_open_ended = false
        )
        OR (
            employee_count_min IS NOT NULL
            AND employee_count_max IS NULL
            AND employee_count_open_ended = true
        );

ALTER TABLE corpscout.se_ratsit_responsible_people
    ADD COLUMN IF NOT EXISTS display_name_raw Nullable(String)
        CODEC(ZSTD(3)) AFTER display_name,
    ADD COLUMN IF NOT EXISTS name Nullable(String)
        CODEC(ZSTD(3)) AFTER display_name_raw,
    ADD COLUMN IF NOT EXISTS age Nullable(UInt16) AFTER name,
    ADD COLUMN IF NOT EXISTS identity_available Bool
        DEFAULT false AFTER age,
    ADD CONSTRAINT se_ratsit_responsible_identity CHECK
        normalizer_version != 'ratsit-normalizer-v2'
        OR identity_available = true
        OR (
            name IS NULL
            AND age IS NULL
            AND profile_url IS NULL
        );

ALTER TABLE corpscout.se_ratsit_financial_periods
    ADD COLUMN IF NOT EXISTS period_kind LowCardinality(String)
        DEFAULT '' AFTER period_index,
    ADD CONSTRAINT se_ratsit_financial_period_kind CHECK
        period_kind IN (
            '',
            'employment_only',
            'financial_only',
            'financial_and_employment'
        );

DROP VIEW IF EXISTS corpscout.se_ratsit_company_industries_with_nace;

CREATE VIEW corpscout.se_ratsit_company_industries_with_nace AS
SELECT
    industry.*,
    class_category.description_en AS nace_class_name_en,
    class_category.section_code AS nace_section_code,
    section_category.description_en AS nace_section_name_en
FROM corpscout.se_ratsit_company_industry_codes AS industry FINAL
LEFT JOIN corpscout.nace_categories AS class_category FINAL
    ON class_category.classification_version = industry.nace_revision
   AND class_category.normalized_code = industry.nace_normalized_code
   AND class_category.level = 'class'
LEFT JOIN corpscout.nace_categories AS section_category FINAL
    ON section_category.classification_version = industry.nace_revision
   AND section_category.normalized_code = class_category.section_code
   AND section_category.level = 'section';

DROP VIEW IF EXISTS corpscout.se_ratsit_establishments_with_nace;

CREATE VIEW corpscout.se_ratsit_establishments_with_nace AS
SELECT
    establishment.*,
    class_category.description_en AS nace_class_name_en,
    class_category.section_code AS nace_section_code,
    section_category.description_en AS nace_section_name_en
FROM corpscout.se_ratsit_establishments AS establishment FINAL
LEFT JOIN corpscout.nace_categories AS class_category FINAL
    ON class_category.classification_version = establishment.nace_revision
   AND class_category.normalized_code = establishment.nace_normalized_code
   AND class_category.level = 'class'
LEFT JOIN corpscout.nace_categories AS section_category FINAL
    ON section_category.classification_version = establishment.nace_revision
   AND section_category.normalized_code = class_category.section_code
   AND section_category.level = 'section';
