DROP TABLE IF EXISTS corpscout.company_person_role_type;
DROP TABLE IF EXISTS corpscout.se_company_person_draft;

RENAME TABLE corpscout.se_company_person_draft_legacy
TO corpscout.se_company_person_draft;
