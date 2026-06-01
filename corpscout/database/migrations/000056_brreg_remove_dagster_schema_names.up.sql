DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'brreg_enhanced_raw_inputs'
      AND column_name = 'dagster_run_id'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'brreg_enhanced_raw_inputs'
      AND column_name = 'orchestrator_run_id'
  ) THEN
    ALTER TABLE brreg_enhanced_raw_inputs
      RENAME COLUMN dagster_run_id TO orchestrator_run_id;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'brreg_enhanced_raw_inputs'
      AND column_name = 'dagster_asset_key'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'brreg_enhanced_raw_inputs'
      AND column_name = 'asset_key'
  ) THEN
    ALTER TABLE brreg_enhanced_raw_inputs
      RENAME COLUMN dagster_asset_key TO asset_key;
  END IF;
END $$;

DO $$
BEGIN
  IF to_regclass('idx_brreg_enhanced_dagster_run') IS NOT NULL
     AND to_regclass('idx_brreg_enhanced_orchestrator_run') IS NULL THEN
    ALTER INDEX idx_brreg_enhanced_dagster_run
      RENAME TO idx_brreg_enhanced_orchestrator_run;
  END IF;
END $$;

DROP INDEX IF EXISTS idx_brreg_enhanced_orchestrator_run;
CREATE INDEX idx_brreg_enhanced_orchestrator_run
  ON brreg_enhanced_raw_inputs(orchestrator_run_id)
  WHERE orchestrator_run_id IS NOT NULL;

ALTER TABLE brreg_source_domains
  DROP CONSTRAINT IF EXISTS chk_brreg_source_domains_source;

UPDATE brreg_source_domains
SET source = 'workflow'
WHERE source = 'dagster';

ALTER TABLE brreg_source_domains
  ALTER COLUMN source SET DEFAULT 'workflow',
  ADD CONSTRAINT chk_brreg_source_domains_source
    CHECK (source IN ('workflow', 'manual', 'corpscout'));
