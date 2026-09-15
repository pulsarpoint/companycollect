CREATE DATABASE IF NOT EXISTS corpscout;

-- Remove only the auxiliary index. Keep task identity and the sort key so
-- existing independent selections cannot be mixed by a schema rollback.
ALTER TABLE corpscout.company_brave_search_input DROP INDEX IF EXISTS task_id_index;
