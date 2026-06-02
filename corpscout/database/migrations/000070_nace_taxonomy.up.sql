CREATE TABLE nace_classifications (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  code_system TEXT NOT NULL DEFAULT 'NACE',
  revision TEXT NOT NULL,
  name TEXT NOT NULL,
  valid_from DATE,
  valid_to DATE,
  source_url TEXT,
  source_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_nace_classifications_code_system CHECK (code_system = 'NACE'),
  CONSTRAINT chk_nace_classifications_revision CHECK (btrim(revision) <> ''),
  CONSTRAINT chk_nace_classifications_name CHECK (btrim(name) <> ''),
  CONSTRAINT chk_nace_classifications_source_metadata_object CHECK (jsonb_typeof(source_metadata) = 'object'),
  UNIQUE (code_system, revision)
);

CREATE TABLE nace_codes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  classification_id UUID NOT NULL REFERENCES nace_classifications(id) ON DELETE CASCADE,
  code TEXT NOT NULL,
  normalized_code TEXT NOT NULL,
  level SMALLINT NOT NULL,
  level_name TEXT NOT NULL,
  parent_code TEXT,
  parent_id UUID REFERENCES nace_codes(id) ON DELETE SET NULL,
  title TEXT NOT NULL,
  description TEXT,
  includes TEXT,
  excludes TEXT,
  notes JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_hash TEXT NOT NULL,
  active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_nace_codes_code CHECK (btrim(code) <> ''),
  CONSTRAINT chk_nace_codes_normalized_code CHECK (btrim(normalized_code) <> ''),
  CONSTRAINT chk_nace_codes_level CHECK (level BETWEEN 1 AND 4),
  CONSTRAINT chk_nace_codes_level_name CHECK (level_name IN ('section', 'division', 'group', 'class')),
  CONSTRAINT chk_nace_codes_title CHECK (btrim(title) <> ''),
  CONSTRAINT chk_nace_codes_notes_object CHECK (jsonb_typeof(notes) = 'object'),
  CONSTRAINT chk_nace_codes_source_payload_object CHECK (jsonb_typeof(source_payload) = 'object'),
  UNIQUE (classification_id, code)
);

CREATE TABLE nace_code_aliases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  nace_code_id UUID NOT NULL REFERENCES nace_codes(id) ON DELETE CASCADE,
  alias_type TEXT NOT NULL,
  alias_code TEXT NOT NULL,
  normalized_alias_code TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'nace',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_nace_code_aliases_type CHECK (alias_type IN ('exact', 'normalized', 'search')),
  CONSTRAINT chk_nace_code_aliases_code CHECK (btrim(alias_code) <> ''),
  CONSTRAINT chk_nace_code_aliases_normalized CHECK (btrim(normalized_alias_code) <> ''),
  CONSTRAINT chk_nace_code_aliases_source CHECK (source = 'nace'),
  CONSTRAINT chk_nace_code_aliases_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (nace_code_id, alias_type, alias_code),
  UNIQUE (source, alias_type, normalized_alias_code, nace_code_id)
);

CREATE INDEX idx_nace_codes_classification_level_code
  ON nace_codes(classification_id, level, code);

CREATE INDEX idx_nace_codes_parent
  ON nace_codes(parent_id)
  WHERE parent_id IS NOT NULL;

CREATE INDEX idx_nace_codes_parent_code
  ON nace_codes(classification_id, parent_code)
  WHERE parent_code IS NOT NULL;

CREATE INDEX idx_nace_codes_normalized
  ON nace_codes(classification_id, normalized_code);

CREATE INDEX idx_nace_codes_active
  ON nace_codes(classification_id, active, level, code);

CREATE INDEX idx_nace_code_aliases_lookup
  ON nace_code_aliases(source, alias_type, normalized_alias_code);

CREATE OR REPLACE VIEW v_nace_taxonomy_state AS
SELECT
  nc.id AS classification_id,
  nc.code_system,
  nc.revision,
  nc.name,
  nc.valid_from,
  nc.valid_to,
  count(ncode.id) FILTER (WHERE ncode.active) AS active_codes,
  count(ncode.id) FILTER (WHERE NOT ncode.active) AS inactive_codes,
  count(ncode.id) FILTER (WHERE ncode.active AND ncode.level_name = 'section') AS sections,
  count(ncode.id) FILTER (WHERE ncode.active AND ncode.level_name = 'division') AS divisions,
  count(ncode.id) FILTER (WHERE ncode.active AND ncode.level_name = 'group') AS groups,
  count(ncode.id) FILTER (WHERE ncode.active AND ncode.level_name = 'class') AS classes,
  max(ncode.updated_at) AS codes_updated_at,
  nc.updated_at AS classification_updated_at
FROM nace_classifications nc
LEFT JOIN nace_codes ncode ON ncode.classification_id = nc.id
GROUP BY nc.id;

CREATE OR REPLACE VIEW v_nace_code_tree AS
SELECT
  nc.code_system,
  nc.revision,
  ncode.id,
  ncode.classification_id,
  ncode.code,
  ncode.normalized_code,
  ncode.level,
  ncode.level_name,
  ncode.parent_code,
  ncode.parent_id,
  parent.code AS parent_nace_code,
  ncode.title,
  ncode.description,
  ncode.includes,
  ncode.excludes,
  ncode.active,
  ncode.created_at,
  ncode.updated_at
FROM nace_codes ncode
JOIN nace_classifications nc ON nc.id = ncode.classification_id
LEFT JOIN nace_codes parent ON parent.id = ncode.parent_id;

GRANT SELECT ON nace_classifications TO corpscout_anon;
GRANT SELECT ON nace_codes TO corpscout_anon;
GRANT SELECT ON nace_code_aliases TO corpscout_anon;
GRANT SELECT ON v_nace_taxonomy_state TO corpscout_anon;
GRANT SELECT ON v_nace_code_tree TO corpscout_anon;
