-- Forward-only: shadows may contain new history or rollback tables after cutover.
SELECT throwIf(1, 'Webtech page migration requires the documented UUID-aware rollback; no tables were dropped');
