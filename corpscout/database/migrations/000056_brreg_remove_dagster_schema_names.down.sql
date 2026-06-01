DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'brreg_enhanced_raw_inputs'
      AND column_name = 'orchestrator_run_id'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'brreg_enhanced_raw_inputs'
      AND column_name = 'dagster_run_id'
  ) THEN
    ALTER TABLE brreg_enhanced_raw_inputs
      RENAME COLUMN orchestrator_run_id TO dagster_run_id;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'brreg_enhanced_raw_inputs'
      AND column_name = 'asset_key'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'brreg_enhanced_raw_inputs'
      AND column_name = 'dagster_asset_key'
  ) THEN
    ALTER TABLE brreg_enhanced_raw_inputs
      RENAME COLUMN asset_key TO dagster_asset_key;
  END IF;
END $$;

DO $$
BEGIN
  IF to_regclass('idx_brreg_enhanced_orchestrator_run') IS NOT NULL
     AND to_regclass('idx_brreg_enhanced_dagster_run') IS NULL THEN
    ALTER INDEX idx_brreg_enhanced_orchestrator_run
      RENAME TO idx_brreg_enhanced_dagster_run;
  END IF;
END $$;

DROP INDEX IF EXISTS idx_brreg_enhanced_dagster_run;
CREATE INDEX idx_brreg_enhanced_dagster_run
  ON brreg_enhanced_raw_inputs(dagster_run_id)
  WHERE dagster_run_id IS NOT NULL;

ALTER TABLE brreg_source_domains
  DROP CONSTRAINT IF EXISTS chk_brreg_source_domains_source;

UPDATE brreg_source_domains
SET source = 'dagster'
WHERE source = 'workflow';

ALTER TABLE brreg_source_domains
  ALTER COLUMN source SET DEFAULT 'dagster',
  ADD CONSTRAINT chk_brreg_source_domains_source
    CHECK (source IN ('dagster', 'manual', 'corpscout'));
