WITH canonical_sources AS (
  SELECT id
  FROM data_sources
  WHERE registry_key IN (
    'finland/prhytj',
    'united_states/coloradoentities',
    'united_states/irseobmf',
    'united_states/secedgar'
  )
)
UPDATE companies
SET primary_source_id = NULL
WHERE primary_source_id IS NOT NULL
  AND primary_source_id NOT IN (SELECT id FROM canonical_sources);

WITH canonical_sources AS (
  SELECT id
  FROM data_sources
  WHERE registry_key IN (
    'finland/prhytj',
    'united_states/coloradoentities',
    'united_states/irseobmf',
    'united_states/secedgar'
  )
)
UPDATE company_aliases
SET source_id = NULL
WHERE source_id IS NOT NULL
  AND source_id NOT IN (SELECT id FROM canonical_sources);

WITH canonical_sources AS (
  SELECT id
  FROM data_sources
  WHERE registry_key IN (
    'finland/prhytj',
    'united_states/coloradoentities',
    'united_states/irseobmf',
    'united_states/secedgar'
  )
)
UPDATE legacy_brreg_company_raw_inputs
SET source_pull_run_id = NULL
WHERE source_pull_run_id IN (
  SELECT id
  FROM source_pull_runs
  WHERE source_id NOT IN (SELECT id FROM canonical_sources)
);

DELETE FROM data_sources
WHERE registry_key NOT IN (
  'finland/prhytj',
  'united_states/coloradoentities',
  'united_states/irseobmf',
  'united_states/secedgar'
);
