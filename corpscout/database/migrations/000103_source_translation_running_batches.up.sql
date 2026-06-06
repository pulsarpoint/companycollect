CREATE SCHEMA IF NOT EXISTS source_translation;

CREATE VIEW source_translation.running_queue_batches AS
SELECT
  'brreg'::text AS source,
  batch_id,
  count(*)::integer AS company_count,
  COALESCE(sum(num_of_characters), 0)::integer AS estimated_chars,
  min(status_changed_at) AS claimed_at
FROM brreg_source.translation_queue_entries
WHERE status = 'running'
  AND batch_id IS NOT NULL
GROUP BY batch_id
UNION ALL
SELECT
  'ariregister'::text AS source,
  batch_id,
  count(*)::integer AS company_count,
  COALESCE(sum(num_of_characters), 0)::integer AS estimated_chars,
  min(status_changed_at) AS claimed_at
FROM ariregister_source.translation_queue_entries
WHERE status = 'running'
  AND batch_id IS NOT NULL
GROUP BY batch_id;
