CREATE DATABASE IF NOT EXISTS corpscout;

-- Preserve the existing table UUID and input_id primary key. Each selection is
-- immutable once ready. The task index skips unrelated selection granules.
ALTER TABLE corpscout.company_brave_search_input
    ADD COLUMN task_id String,
    MODIFY ORDER BY (input_id, task_id),
    ADD INDEX task_id_index task_id TYPE set(0) GRANULARITY 1;

ALTER TABLE corpscout.company_brave_search_input MATERIALIZE INDEX task_id_index;
