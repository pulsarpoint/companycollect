ALTER TABLE processing.tasks DROP CONSTRAINT tasks_status_check;
ALTER TABLE processing.tasks ADD CONSTRAINT tasks_status_check
    CHECK (status IN ('preparing','selected','ready','cancelled'));

-- Pre-initializer rows belong to the legacy empty task_id selection in ClickHouse.
UPDATE processing.tasks
SET source_info=jsonb_set(source_info,'{selection_task_id}','""'::jsonb)
WHERE source_info->>'relation'='corpscout.company_brave_search_input'
  AND NOT (source_info ? 'selection_task_id');
