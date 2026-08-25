ALTER TABLE corpscout.company_person_role_type DELETE WHERE role_code IN
(
    'legal_representative',
    'other_representative',
    'director',
    'supervisory_board_member',
    'executive_board_member',
    'management_board_member',
    'procurist',
    'group_procurist',
    'beneficial_owner'
);

DROP TABLE IF EXISTS corpscout.rs_apr_company_beneficial_owners_current;
DROP TABLE IF EXISTS corpscout.rs_apr_company_beneficial_owner_observations;
DROP TABLE IF EXISTS corpscout.rs_apr_company_representatives_current;
DROP TABLE IF EXISTS corpscout.rs_apr_company_representative_observations;
