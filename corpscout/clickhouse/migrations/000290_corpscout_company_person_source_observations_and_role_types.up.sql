CREATE DATABASE IF NOT EXISTS corpscout;

-- The source-observation draft table this migration also created was dropped by migration
-- 000332 on 2026-08-27, and its DDL left this file in SE person slice 0 (2026-09-09) per the
-- dev-phase ledger policy. The role catalog below stays -- Serbia seeds into it too.

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
