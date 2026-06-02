CREATE TABLE brreg_workflow.nace_mappings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  nace_code_id UUID NOT NULL REFERENCES nace_codes(id) ON DELETE RESTRICT,
  source_field TEXT NOT NULL,
  classification_type TEXT NOT NULL,
  position SMALLINT NOT NULL DEFAULT 1,
  source_code TEXT NOT NULL,
  source_description TEXT,
  mapped_nace_code TEXT NOT NULL,
  mapping_method TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 1,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_brreg_workflow_nace_mappings_source_field CHECK (
    source_field IN ('naeringskode1', 'naeringskode2', 'naeringskode3', 'hjelpeenhetskode')
  ),
  CONSTRAINT chk_brreg_workflow_nace_mappings_classification_type CHECK (
    classification_type IN ('industry', 'helper_unit')
  ),
  CONSTRAINT chk_brreg_workflow_nace_mappings_position CHECK (position BETWEEN 1 AND 10),
  CONSTRAINT chk_brreg_workflow_nace_mappings_source_code CHECK (btrim(source_code) <> ''),
  CONSTRAINT chk_brreg_workflow_nace_mappings_mapped_code CHECK (btrim(mapped_nace_code) <> ''),
  CONSTRAINT chk_brreg_workflow_nace_mappings_method CHECK (
    mapping_method IN ('sn_level_5_to_nace_class', 'nace_exact')
  ),
  CONSTRAINT chk_brreg_workflow_nace_mappings_confidence CHECK (confidence BETWEEN 0 AND 1),
  CONSTRAINT chk_brreg_workflow_nace_mappings_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  UNIQUE (raw_record_id, source_field, source_code, nace_code_id)
);

CREATE INDEX idx_brreg_workflow_nace_mappings_raw_record
  ON brreg_workflow.nace_mappings(raw_record_id, position);

CREATE INDEX idx_brreg_workflow_nace_mappings_nace_code
  ON brreg_workflow.nace_mappings(nace_code_id);

CREATE INDEX idx_brreg_workflow_nace_mappings_source_code
  ON brreg_workflow.nace_mappings(source_code);

CREATE INDEX idx_brreg_workflow_nace_mappings_mapped_code
  ON brreg_workflow.nace_mappings(mapped_nace_code);

CREATE OR REPLACE VIEW brreg_workflow.v_nace_mappings AS
SELECT
  mapping.id,
  mapping.raw_record_id,
  raw.organization_number,
  raw.organization_name,
  mapping.source_field,
  mapping.classification_type,
  mapping.position,
  mapping.source_code,
  mapping.source_description,
  mapping.mapped_nace_code,
  mapping.mapping_method,
  mapping.confidence,
  mapping.nace_code_id,
  nace_classification.revision AS nace_revision,
  nace_code.code AS nace_code,
  nace_code.level AS nace_level,
  nace_code.level_name AS nace_level_name,
  nace_code.title AS nace_title,
  mapping.evidence,
  mapping.created_at,
  mapping.updated_at
FROM brreg_workflow.nace_mappings mapping
JOIN brreg_workflow.raw_records raw ON raw.id = mapping.raw_record_id
JOIN nace_codes nace_code ON nace_code.id = mapping.nace_code_id
JOIN nace_classifications nace_classification ON nace_classification.id = nace_code.classification_id;

GRANT SELECT ON brreg_workflow.nace_mappings TO corpscout_anon;
GRANT SELECT ON brreg_workflow.v_nace_mappings TO corpscout_anon;
