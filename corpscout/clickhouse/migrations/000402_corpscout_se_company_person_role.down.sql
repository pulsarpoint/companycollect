CREATE DATABASE IF NOT EXISTS corpscout;

-- Undoes 000402. The view owns its MergeTree (the engine is declared inside it), so one
-- DROP VIEW removes the definition and the data together and nothing is left behind.
-- Nothing else has to come back: the view is derived, so no row it held was ever the
-- only copy of anything, and the two tables it read are not this migration's business.
-- The deployed backoffice reads this name through PERSON_ROLE_SQL, and after a rollback
-- its Roles panel falls back to the person row's arrays -- the same thing it does in the
-- window between the CREATE and the first refresh.

DROP VIEW IF EXISTS corpscout.se_company_person_role;
