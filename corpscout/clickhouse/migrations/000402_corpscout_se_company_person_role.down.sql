CREATE DATABASE IF NOT EXISTS corpscout;

-- Undoes 000402. The view owns its MergeTree (the engine is declared inside it), so one
-- DROP VIEW removes the definition and the data together and nothing is left behind.
-- Nothing else has to come back: the view is derived, so no row it held was ever the
-- only copy of anything, and the two tables it read are not this migration's business.
--
-- THE BACKOFFICE DOES NOT FALL BACK ON ITS OWN. The deployed backoffice reads this name
-- through PERSON_ROLE_SQL, and after a rollback that read fails with UNKNOWN_TABLE --
-- `loadSePersonDetail`'s `loadPersonRoleRows` (F4) is the thing that catches that (and
-- any other failure reading the view), logs it once, and answers `[]` instead of letting
-- the rejection take the whole People tab down. Only because of that catch does the
-- Roles panel fall back to the person row's own arrays post-rollback -- the same
-- fallback it shows in the ordinary window between the CREATE and the first refresh.

DROP VIEW IF EXISTS corpscout.se_company_person_role;
