-- Fixed inputs live in ClickHouse. PostgreSQL retains identities, state and results.
-- An unfinished legacy snapshot needs to finish before removing its input payloads.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM processing.items WHERE state IN ('queued','running','retry_wait'))
       OR EXISTS (SELECT 1 FROM processing.tasks WHERE status='preparing') THEN
        RAISE EXCEPTION 'Finish legacy processing tasks before migrating their input storage';
    END IF;
END $$;

UPDATE processing.results r SET payload = jsonb_build_object(
    'country_code', coalesce(i.input_data->>'country_code', ''),
    'company_id', coalesce(i.input_data->>'company_id', r.input_id),
    'company_name', coalesce(i.input_data->>'company_name', ''),
    'query', i.query
) || r.payload FROM processing.items i
WHERE (r.task_id,r.input_id)=(i.task_id,i.input_id);

CREATE OR REPLACE VIEW processing.brave_export AS
SELECT r.result_id::text AS result_id, r.task_id::text AS task_id,
    r.input_id, r.work_key, r.attempt, r.status,
    coalesce(r.export_batch_id::text, '') AS export_batch_id,
    coalesce(r.payload->>'country_code', '') AS country_code,
    coalesce(r.payload->>'company_id', r.input_id) AS company_id,
    coalesce(r.payload->>'company_name', '') AS company_name,
    coalesce(r.payload->>'query', '') AS query,
    coalesce(t.config->>'query_type', '') AS query_type,
    t.processor AS processor_version,
    coalesce(r.payload->>'answer_text', '') AS answer_text,
    coalesce(r.payload->>'route', '') AS route,
    coalesce(r.payload->>'source_url', '') AS source_url,
    coalesce(r.payload->>'error_type', '') AS error_type,
    coalesce(r.payload->>'source_run_id', '') AS source_run_id,
    r.completed_at
FROM processing.results r JOIN processing.tasks t USING (task_id)
WHERE t.processor = 'brave-v2';

ALTER TABLE processing.items DROP COLUMN input_data, DROP COLUMN query, DROP COLUMN work_key;
ALTER TABLE processing.tasks
    ADD COLUMN source_info jsonb,
    ADD COLUMN source_cursor text,
    ADD COLUMN admitted_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN succeeded_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN terminal_failed_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN skipped_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN cancelled_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN unpublished_count bigint NOT NULL DEFAULT 0;

UPDATE processing.tasks t SET
    admitted_count=p.total, succeeded_count=p.succeeded,
    terminal_failed_count=p.terminal_failed, skipped_count=p.skipped,
    cancelled_count=p.cancelled, unpublished_count=p.unpublished
FROM processing.task_progress p WHERE t.task_id=p.task_id;

-- Count terminal transitions in the same transaction as their durable result.
-- Open work is bounded and covered by the existing partial indexes.
CREATE FUNCTION processing.count_terminal_items() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.state IS DISTINCT FROM NEW.state THEN
        UPDATE processing.tasks SET
            succeeded_count=succeeded_count+(NEW.state='succeeded')::int-(OLD.state='succeeded')::int,
            terminal_failed_count=terminal_failed_count+(NEW.state='terminal_failed')::int-(OLD.state='terminal_failed')::int,
            skipped_count=skipped_count+(NEW.state='skipped')::int-(OLD.state='skipped')::int,
            cancelled_count=cancelled_count+(NEW.state='cancelled')::int-(OLD.state='cancelled')::int
        WHERE task_id=NEW.task_id;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER count_terminal_items AFTER UPDATE OF state ON processing.items
FOR EACH ROW WHEN (OLD.state IN ('succeeded','terminal_failed','skipped','cancelled')
                OR NEW.state IN ('succeeded','terminal_failed','skipped','cancelled'))
EXECUTE FUNCTION processing.count_terminal_items();

CREATE FUNCTION processing.count_unpublished_results() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE processing.tasks SET unpublished_count=unpublished_count+1 WHERE task_id=NEW.task_id;
    RETURN NEW;
END $$;
CREATE TRIGGER count_unpublished_results AFTER INSERT ON processing.results
FOR EACH ROW EXECUTE FUNCTION processing.count_unpublished_results();

CREATE FUNCTION processing.count_published_results() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE processing.tasks SET unpublished_count=unpublished_count-NEW.result_count WHERE task_id=NEW.task_id;
    RETURN NEW;
END $$;
CREATE TRIGGER count_published_results AFTER UPDATE OF published_at ON processing.export_batches
FOR EACH ROW WHEN (OLD.published_at IS NULL AND NEW.published_at IS NOT NULL)
EXECUTE FUNCTION processing.count_published_results();

CREATE OR REPLACE VIEW processing.task_progress AS
SELECT t.task_id,t.processor,t.status,t.total,
    t.total-t.admitted_count +
        (SELECT count(*) FROM processing.items i WHERE i.task_id=t.task_id AND i.state='queued') AS queued,
    (SELECT count(*) FROM processing.items i WHERE i.task_id=t.task_id AND i.state='running') AS running,
    (SELECT count(*) FROM processing.items i WHERE i.task_id=t.task_id AND i.state='retry_wait') AS retry_wait,
    t.succeeded_count AS succeeded, t.terminal_failed_count AS terminal_failed,
    t.skipped_count AS skipped, t.cancelled_count AS cancelled,
    t.total-t.succeeded_count-t.terminal_failed_count-t.skipped_count-t.cancelled_count AS remaining,
    t.unpublished_count AS unpublished
FROM processing.tasks t;
