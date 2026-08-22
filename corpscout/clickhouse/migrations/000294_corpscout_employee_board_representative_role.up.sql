CREATE DATABASE IF NOT EXISTS corpscout;

INSERT INTO corpscout.company_person_role_type (
    role_code,
    display_name,
    role_group,
    description,
    is_active,
    created_at,
    updated_at
)
VALUES (
    'employee_board_representative',
    'Employee board representative',
    'governance',
    'Employee-appointed member of the company board representing the workforce.',
    1,
    toDateTime64('2026-08-20 00:00:00', 3, 'UTC'),
    toDateTime64('2026-08-20 00:00:00', 3, 'UTC')
);
