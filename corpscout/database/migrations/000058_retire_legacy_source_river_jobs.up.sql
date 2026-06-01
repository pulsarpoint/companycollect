UPDATE river_job
SET state = 'cancelled',
    finalized_at = now(),
    metadata = metadata || jsonb_build_object(
      'cleanup_reason',
      'legacy source_pull/source_process workers removed'
    )
WHERE kind IN ('source_pull', 'source_process')
  AND state NOT IN ('cancelled', 'completed', 'discarded');
