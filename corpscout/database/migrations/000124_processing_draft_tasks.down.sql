-- Never discard the ownership/freeze/cleanup history of adopted tasks silently.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM processing.tasks WHERE queue_scope IS NOT NULL)
       OR EXISTS (SELECT 1 FROM processing.input_submissions) THEN
        RAISE EXCEPTION 'Draft queue metadata is in use; reconcile adopted tasks before rollback';
    END IF;
END $$;

DROP TABLE processing.input_submissions;
DROP INDEX processing.processing_completed_input_cleanup;
DROP INDEX processing.processing_one_draft_task;
ALTER TABLE processing.tasks DROP CONSTRAINT tasks_queue_lifecycle_check;
ALTER TABLE processing.tasks DROP CONSTRAINT tasks_status_check;
ALTER TABLE processing.tasks ADD CONSTRAINT tasks_status_check
    CHECK (status IN ('preparing','selected','ready','cancelled'));
ALTER TABLE processing.tasks
    DROP COLUMN queue_scope,
    DROP COLUMN frozen_at,
    DROP COLUMN completed_at,
    DROP COLUMN inputs_purged_at;
