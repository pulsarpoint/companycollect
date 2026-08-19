CREATE DATABASE IF NOT EXISTS corpscout;

-- Preserve the pre-normalized draft rows while the new source-observation
-- pipeline is introduced. They are derived data and are not copied into the
-- new inbox because they already merge several sources.
RENAME TABLE corpscout.se_company_person_draft
TO corpscout.se_company_person_draft_legacy;

-- Immutable inbox for person observations copied from source tables. A new
-- semantic source version gets a new draft_id. An existing draft_id is never
-- updated by the collector.
CREATE TABLE corpscout.se_company_person_draft
(
    draft_id UUID,
    company_id String,
    source LowCardinality(String),
    source_entity_id String,
    source_record_uid String,
    person_profile_hash FixedString(64),
    person_role_hash FixedString(64),
    source_value_json String,
    fiscal_year Nullable(UInt16),
    source_observed_at DateTime64(3, 'UTC'),
    source_run_id String,
    created_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (company_id, source, draft_id);

-- This table is the controlled role vocabulary. The LLM-facing schema will
-- expose active role_code values as its enum. NEW_ROLE_REQUIRED remains an
-- output sentinel and is intentionally not a publishable role in this table.
CREATE TABLE corpscout.company_person_role_type
(
    role_code String,
    display_name String,
    role_group LowCardinality(String),
    description String,
    is_active UInt8,
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (role_code)
AS
SELECT
    role_code,
    display_name,
    role_group,
    description,
    is_active,
    toDateTime64(seed_at, 3, 'UTC') AS created_at,
    toDateTime64(seed_at, 3, 'UTC') AS updated_at
FROM VALUES(
    'role_code String, display_name String, role_group String, description String, is_active UInt8, seed_at String',
    ('board_chair', 'Board chair', 'governance', 'Chair of the company board.', 1, '2026-08-19 00:00:00'),
    ('board_member', 'Board member', 'governance', 'Member of the company board.', 1, '2026-08-19 00:00:00'),
    ('deputy_board_member', 'Deputy board member', 'governance', 'Deputy or alternate member of the company board.', 1, '2026-08-19 00:00:00'),
    ('chief_executive_officer', 'Chief executive officer', 'executive', 'Highest-ranking executive responsible for company management.', 1, '2026-08-19 00:00:00'),
    ('deputy_chief_executive_officer', 'Deputy chief executive officer', 'executive', 'Executive formally serving as deputy to the chief executive officer.', 1, '2026-08-19 00:00:00'),
    ('chief_financial_officer', 'Chief financial officer', 'executive', 'Executive responsible for the company financial function.', 1, '2026-08-19 00:00:00'),
    ('executive', 'Executive', 'executive', 'Member of executive management without a more specific active canonical role.', 1, '2026-08-19 00:00:00'),
    ('auditor', 'Auditor', 'audit', 'Person serving as an auditor of the company.', 1, '2026-08-19 00:00:00'),
    ('audit_partner', 'Audit partner', 'audit', 'Named audit partner responsible for the company audit.', 1, '2026-08-19 00:00:00'),
    ('liquidator', 'Liquidator', 'governance', 'Person appointed to administer the company liquidation.', 1, '2026-08-19 00:00:00'),
    ('founder', 'Founder', 'ownership', 'Person identified as a founder of the company.', 1, '2026-08-19 00:00:00'),
    ('owner', 'Owner', 'ownership', 'Person identified as an owner of the company.', 1, '2026-08-19 00:00:00')
);
