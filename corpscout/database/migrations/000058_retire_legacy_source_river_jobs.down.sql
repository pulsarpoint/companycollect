DO $$
BEGIN
  IF to_regclass('river_job') IS NOT NULL THEN
    UPDATE river_job
    SET state = 'retryable',
        finalized_at = NULL,
        metadata = metadata - 'cleanup_reason'
    WHERE kind IN ('source_pull', 'source_process')
      AND state = 'cancelled'
      AND metadata->>'cleanup_reason' = 'legacy source_pull/source_process workers removed';
  END IF;
END
$$;
