CREATE DATABASE IF NOT EXISTS corpscout;

-- Review finding (brazil-connection-history, IMPORTANT 2): MIN_SNAPSHOT_EDGE_RATIO
-- (history.py) is a coarse floor, deliberately loose enough to tolerate real
-- month-over-month register churn -- which also means it tolerates a single
-- missing socios ZIP part. RFB ships socios as ~10 roughly equal-sized parts,
-- so one missing part is only a ~10% edge-count drop: comfortably inside that
-- 50% floor, and never caught by it. A mid-stream truncation of one part is
-- NOT the silent case (zipfile fails loudly extracting a corrupt archive) --
-- a part that simply never downloaded IS silent, and is exactly what this
-- column exists to catch.
--
-- The snapshot manifest (tables.SNAPSHOT_FILE_COLUMNS) already records one
-- row per downloaded file with its family, so the socios part count for a
-- month is knowable exactly, not estimated. This column carries that count
-- into the permanent ledger so the next run can compare it against the
-- previous merged month and refuse a DECREASE -- no threshold, no
-- false-positive risk on legitimate register churn (a shrinking register
-- does not change how many ZIP files RFB split socios into). See
-- history.assert_snapshot_part_count_is_not_decreasing.
--
-- DEFAULT 0 rather than Nullable: existing ledger rows (merged before this
-- column existed) read as 0, which the guard already treats the same as "no
-- baseline to compare against" (mirroring the previous_edges_in_snapshot <= 0
-- handling in assert_snapshot_edge_count_is_plausible) -- so the first run
-- after this migration is never refused against a baseline that was never
-- actually recorded.
ALTER TABLE corpscout.br_company_relations_snapshots
    ADD COLUMN IF NOT EXISTS socios_part_count UInt32 DEFAULT 0;
