DROP VIEW processing.task_progress;
DROP VIEW processing.brave_export;
ALTER TABLE processing.items DROP CONSTRAINT items_accepted_result_id_fkey;
DROP TABLE processing.results;
DROP TABLE processing.export_batches;
DROP TABLE processing.items;
DROP TABLE processing.tasks;
DROP SCHEMA processing;
