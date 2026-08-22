CREATE DATABASE IF NOT EXISTS corpscout;

-- INSERT ... SELECT rather than INSERT ... VALUES: the golang-migrate ClickHouse driver
-- only accepts VALUES inserts in batch mode and rejects them at apply time.
INSERT INTO corpscout.company_person_role_type (
    role_code,
    display_name,
    role_group,
    description,
    is_active,
    created_at,
    updated_at
)
SELECT
    'employee_board_representative' AS role_code,
    'Employee board representative' AS display_name,
    'governance' AS role_group,
    'Employee-appointed member of the company board representing the workforce.' AS description,
    1 AS is_active,
    toDateTime64('2026-08-20 00:00:00', 3, 'UTC') AS created_at,
    toDateTime64('2026-08-20 00:00:00', 3, 'UTC') AS updated_at;
