-- ORDER BY expressions cannot be reduced safely in place. Reverting this metadata-only extension
-- would require rebuilding the 491M-row table, so an ALTER TABLE rollback is intentionally omitted.
SELECT 1;
