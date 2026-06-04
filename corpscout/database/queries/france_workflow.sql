-- name: BeginFranceWorkflowRun :one
INSERT INTO france_workflow.workflow_runs (
  orchestrator,
  orchestrator_run_id,
  run_type,
  metadata
) VALUES (
  COALESCE(sqlc.narg('orchestrator')::text, 'temporal'),
  sqlc.arg('orchestrator_run_id')::text,
  sqlc.arg('run_type')::text,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (orchestrator_run_id) DO UPDATE
SET
  orchestrator = EXCLUDED.orchestrator,
  run_type = EXCLUDED.run_type,
  status = 'running',
  started_at = now(),
  finished_at = NULL,
  error = NULL,
  metadata = EXCLUDED.metadata
RETURNING id;

-- name: FinishFranceWorkflowRunWithStats :one
UPDATE france_workflow.workflow_runs
SET
  status = sqlc.arg('status')::text,
  finished_at = now(),
  records_seen = sqlc.arg('records_seen')::integer,
  records_completed = sqlc.arg('records_completed')::integer,
  records_failed = sqlc.arg('records_failed')::integer,
  error = sqlc.narg('error')::text
WHERE id = sqlc.arg('id')::uuid
RETURNING id;

-- name: CreateFranceBulkSnapshot :one
INSERT INTO france_workflow.bulk_snapshots (
  workflow_run_id,
  snapshot_date,
  dataset_release,
  status,
  downloaded_at,
  metadata
) VALUES (
  sqlc.narg('workflow_run_id')::uuid,
  sqlc.narg('snapshot_date')::date,
  sqlc.narg('dataset_release')::text,
  'downloaded',
  now(),
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
RETURNING id;

-- name: MarkFranceBulkSnapshotParsed :exec
UPDATE france_workflow.bulk_snapshots
SET
  status = 'parsed',
  parsed_at = now(),
  records_seen = sqlc.arg('records_seen')::integer,
  records_written = sqlc.arg('records_written')::integer,
  metadata = COALESCE(sqlc.narg('metadata')::jsonb, metadata)
WHERE id = sqlc.arg('id')::uuid;

-- RecordFranceSourceFile relies on UNIQUE (bulk_snapshot_id, dataset_key) from the migration.
-- name: RecordFranceSourceFile :one
INSERT INTO france_workflow.source_files (
  bulk_snapshot_id,
  dataset_key,
  resource_id,
  stable_url,
  resolved_url,
  file_name,
  file_format,
  content_type,
  content_length_bytes,
  checksum_type,
  checksum_value,
  rows_seen,
  rows_written,
  status,
  error,
  metadata
) VALUES (
  sqlc.arg('bulk_snapshot_id')::uuid,
  sqlc.arg('dataset_key')::text,
  sqlc.arg('resource_id')::text,
  sqlc.arg('stable_url')::text,
  sqlc.narg('resolved_url')::text,
  sqlc.narg('file_name')::text,
  sqlc.arg('file_format')::text,
  sqlc.narg('content_type')::text,
  sqlc.narg('content_length_bytes')::bigint,
  sqlc.narg('checksum_type')::text,
  sqlc.narg('checksum_value')::text,
  sqlc.arg('rows_seen')::integer,
  sqlc.arg('rows_written')::integer,
  sqlc.arg('status')::text,
  sqlc.narg('error')::text,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (bulk_snapshot_id, dataset_key) DO UPDATE
SET
  resource_id = EXCLUDED.resource_id,
  stable_url = EXCLUDED.stable_url,
  resolved_url = EXCLUDED.resolved_url,
  file_name = EXCLUDED.file_name,
  file_format = EXCLUDED.file_format,
  content_type = EXCLUDED.content_type,
  content_length_bytes = EXCLUDED.content_length_bytes,
  checksum_type = EXCLUDED.checksum_type,
  checksum_value = EXCLUDED.checksum_value,
  rows_seen = EXCLUDED.rows_seen,
  rows_written = EXCLUDED.rows_written,
  status = EXCLUDED.status,
  error = EXCLUDED.error,
  metadata = EXCLUDED.metadata,
  updated_at = now()
RETURNING id;

-- name: GetCurrentFranceWorkflowRawLegalUnit :one
SELECT id, payload_hash
FROM france_workflow.raw_legal_units
WHERE siren = sqlc.arg('siren')::text
  AND is_current = true;

-- name: SupersedeCurrentFranceWorkflowRawLegalUnit :exec
UPDATE france_workflow.raw_legal_units
SET
  is_current = false,
  last_seen_at = now()
WHERE siren = sqlc.arg('siren')::text
  AND payload_hash <> sqlc.arg('payload_hash')::text
  AND is_current = true;

-- name: UpsertFranceWorkflowRawLegalUnit :one
WITH upserted AS (
  INSERT INTO france_workflow.raw_legal_units (
    source_file_id,
    source_native_id,
    siren,
    diffusion_status,
    is_purged,
    created_date,
    acronym,
    gender,
    first_name_1,
    first_name_2,
    first_name_3,
    first_name_4,
    usual_first_name,
    pseudonym,
    association_identifier,
    employee_band,
    employee_year,
    source_updated_at,
    period_count,
    company_category,
    company_category_year,
    period_started_at,
    administrative_status,
    birth_name,
    usage_name,
    legal_name,
    usual_name_1,
    usual_name_2,
    usual_name_3,
    legal_form_code,
    primary_activity_code,
    primary_activity_nomenclature,
    headquarters_nic,
    social_solidarity,
    mission_company,
    is_employer,
    primary_activity_naf25_code,
    raw_payload,
    payload_hash,
    is_current,
    metadata
  ) VALUES (
    sqlc.narg('source_file_id')::uuid,
    sqlc.arg('source_native_id')::text,
    sqlc.arg('siren')::text,
    sqlc.narg('diffusion_status')::text,
    sqlc.narg('is_purged')::boolean,
    sqlc.narg('created_date')::date,
    sqlc.narg('acronym')::text,
    sqlc.narg('gender')::text,
    sqlc.narg('first_name_1')::text,
    sqlc.narg('first_name_2')::text,
    sqlc.narg('first_name_3')::text,
    sqlc.narg('first_name_4')::text,
    sqlc.narg('usual_first_name')::text,
    sqlc.narg('pseudonym')::text,
    sqlc.narg('association_identifier')::text,
    sqlc.narg('employee_band')::text,
    sqlc.narg('employee_year')::text,
    sqlc.narg('source_updated_at')::timestamptz,
    sqlc.narg('period_count')::integer,
    sqlc.narg('company_category')::text,
    sqlc.narg('company_category_year')::text,
    sqlc.narg('period_started_at')::date,
    sqlc.narg('administrative_status')::text,
    sqlc.narg('birth_name')::text,
    sqlc.narg('usage_name')::text,
    sqlc.narg('legal_name')::text,
    sqlc.narg('usual_name_1')::text,
    sqlc.narg('usual_name_2')::text,
    sqlc.narg('usual_name_3')::text,
    sqlc.narg('legal_form_code')::text,
    sqlc.narg('primary_activity_code')::text,
    sqlc.narg('primary_activity_nomenclature')::text,
    sqlc.narg('headquarters_nic')::text,
    sqlc.narg('social_solidarity')::boolean,
    sqlc.narg('mission_company')::boolean,
    sqlc.narg('is_employer')::boolean,
    sqlc.narg('primary_activity_naf25_code')::text,
    sqlc.arg('raw_payload')::jsonb,
    sqlc.arg('payload_hash')::text,
    true,
    COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
  )
  ON CONFLICT (siren, payload_hash) DO UPDATE
  SET
    source_file_id = EXCLUDED.source_file_id,
    diffusion_status = EXCLUDED.diffusion_status,
    is_purged = EXCLUDED.is_purged,
    created_date = EXCLUDED.created_date,
    acronym = EXCLUDED.acronym,
    gender = EXCLUDED.gender,
    first_name_1 = EXCLUDED.first_name_1,
    first_name_2 = EXCLUDED.first_name_2,
    first_name_3 = EXCLUDED.first_name_3,
    first_name_4 = EXCLUDED.first_name_4,
    usual_first_name = EXCLUDED.usual_first_name,
    pseudonym = EXCLUDED.pseudonym,
    association_identifier = EXCLUDED.association_identifier,
    employee_band = EXCLUDED.employee_band,
    employee_year = EXCLUDED.employee_year,
    source_updated_at = EXCLUDED.source_updated_at,
    period_count = EXCLUDED.period_count,
    company_category = EXCLUDED.company_category,
    company_category_year = EXCLUDED.company_category_year,
    period_started_at = EXCLUDED.period_started_at,
    administrative_status = EXCLUDED.administrative_status,
    birth_name = EXCLUDED.birth_name,
    usage_name = EXCLUDED.usage_name,
    legal_name = EXCLUDED.legal_name,
    usual_name_1 = EXCLUDED.usual_name_1,
    usual_name_2 = EXCLUDED.usual_name_2,
    usual_name_3 = EXCLUDED.usual_name_3,
    legal_form_code = EXCLUDED.legal_form_code,
    primary_activity_code = EXCLUDED.primary_activity_code,
    primary_activity_nomenclature = EXCLUDED.primary_activity_nomenclature,
    headquarters_nic = EXCLUDED.headquarters_nic,
    social_solidarity = EXCLUDED.social_solidarity,
    mission_company = EXCLUDED.mission_company,
    is_employer = EXCLUDED.is_employer,
    primary_activity_naf25_code = EXCLUDED.primary_activity_naf25_code,
    raw_payload = EXCLUDED.raw_payload,
    is_current = true,
    last_seen_at = now(),
    metadata = EXCLUDED.metadata
  RETURNING id
)
SELECT
  id,
  1::integer AS rows_written
FROM upserted;

-- name: GetCurrentFranceWorkflowRawEstablishment :one
SELECT id, payload_hash
FROM france_workflow.raw_establishments
WHERE siret = sqlc.arg('siret')::text
  AND is_current = true;

-- name: SupersedeCurrentFranceWorkflowRawEstablishment :exec
UPDATE france_workflow.raw_establishments
SET
  is_current = false,
  last_seen_at = now()
WHERE siret = sqlc.arg('siret')::text
  AND payload_hash <> sqlc.arg('payload_hash')::text
  AND is_current = true;

-- name: UpsertFranceWorkflowRawEstablishment :one
WITH upserted AS (
  INSERT INTO france_workflow.raw_establishments (
    source_file_id,
    source_native_id,
    siren,
    nic,
    siret,
    diffusion_status,
    created_date,
    employee_band,
    employee_year,
    crafts_registry_activity,
    source_updated_at,
    is_headquarters,
    period_count,
    address_complement,
    street_number,
    street_number_suffix,
    last_street_number,
    last_street_number_suffix,
    street_type,
    street_label,
    postal_code,
    city_label,
    foreign_city_label,
    special_distribution,
    commune_code,
    cedex_code,
    cedex_label,
    foreign_country_code,
    foreign_country_label,
    address_identifier,
    lambert_x,
    lambert_y,
    address_complement_2,
    street_number_2,
    street_number_suffix_2,
    street_type_2,
    street_label_2,
    postal_code_2,
    city_label_2,
    foreign_city_label_2,
    special_distribution_2,
    commune_code_2,
    cedex_code_2,
    cedex_label_2,
    foreign_country_code_2,
    foreign_country_label_2,
    period_started_at,
    administrative_status,
    trade_name_1,
    trade_name_2,
    trade_name_3,
    usual_name,
    primary_activity_code,
    primary_activity_nomenclature,
    is_employer,
    primary_activity_naf25_code,
    raw_payload,
    payload_hash,
    is_current,
    metadata
  ) VALUES (
    sqlc.narg('source_file_id')::uuid,
    sqlc.arg('source_native_id')::text,
    sqlc.arg('siren')::text,
    sqlc.arg('nic')::text,
    sqlc.arg('siret')::text,
    sqlc.narg('diffusion_status')::text,
    sqlc.narg('created_date')::date,
    sqlc.narg('employee_band')::text,
    sqlc.narg('employee_year')::text,
    sqlc.narg('crafts_registry_activity')::text,
    sqlc.narg('source_updated_at')::timestamptz,
    sqlc.narg('is_headquarters')::boolean,
    sqlc.narg('period_count')::integer,
    sqlc.narg('address_complement')::text,
    sqlc.narg('street_number')::text,
    sqlc.narg('street_number_suffix')::text,
    sqlc.narg('last_street_number')::text,
    sqlc.narg('last_street_number_suffix')::text,
    sqlc.narg('street_type')::text,
    sqlc.narg('street_label')::text,
    sqlc.narg('postal_code')::text,
    sqlc.narg('city_label')::text,
    sqlc.narg('foreign_city_label')::text,
    sqlc.narg('special_distribution')::text,
    sqlc.narg('commune_code')::text,
    sqlc.narg('cedex_code')::text,
    sqlc.narg('cedex_label')::text,
    sqlc.narg('foreign_country_code')::text,
    sqlc.narg('foreign_country_label')::text,
    sqlc.narg('address_identifier')::text,
    sqlc.narg('lambert_x')::text,
    sqlc.narg('lambert_y')::text,
    sqlc.narg('address_complement_2')::text,
    sqlc.narg('street_number_2')::text,
    sqlc.narg('street_number_suffix_2')::text,
    sqlc.narg('street_type_2')::text,
    sqlc.narg('street_label_2')::text,
    sqlc.narg('postal_code_2')::text,
    sqlc.narg('city_label_2')::text,
    sqlc.narg('foreign_city_label_2')::text,
    sqlc.narg('special_distribution_2')::text,
    sqlc.narg('commune_code_2')::text,
    sqlc.narg('cedex_code_2')::text,
    sqlc.narg('cedex_label_2')::text,
    sqlc.narg('foreign_country_code_2')::text,
    sqlc.narg('foreign_country_label_2')::text,
    sqlc.narg('period_started_at')::date,
    sqlc.narg('administrative_status')::text,
    sqlc.narg('trade_name_1')::text,
    sqlc.narg('trade_name_2')::text,
    sqlc.narg('trade_name_3')::text,
    sqlc.narg('usual_name')::text,
    sqlc.narg('primary_activity_code')::text,
    sqlc.narg('primary_activity_nomenclature')::text,
    sqlc.narg('is_employer')::boolean,
    sqlc.narg('primary_activity_naf25_code')::text,
    sqlc.arg('raw_payload')::jsonb,
    sqlc.arg('payload_hash')::text,
    true,
    COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
  )
  ON CONFLICT (siret, payload_hash) DO UPDATE
  SET
    source_file_id = EXCLUDED.source_file_id,
    diffusion_status = EXCLUDED.diffusion_status,
    created_date = EXCLUDED.created_date,
    employee_band = EXCLUDED.employee_band,
    employee_year = EXCLUDED.employee_year,
    crafts_registry_activity = EXCLUDED.crafts_registry_activity,
    source_updated_at = EXCLUDED.source_updated_at,
    is_headquarters = EXCLUDED.is_headquarters,
    period_count = EXCLUDED.period_count,
    address_complement = EXCLUDED.address_complement,
    street_number = EXCLUDED.street_number,
    street_number_suffix = EXCLUDED.street_number_suffix,
    last_street_number = EXCLUDED.last_street_number,
    last_street_number_suffix = EXCLUDED.last_street_number_suffix,
    street_type = EXCLUDED.street_type,
    street_label = EXCLUDED.street_label,
    postal_code = EXCLUDED.postal_code,
    city_label = EXCLUDED.city_label,
    foreign_city_label = EXCLUDED.foreign_city_label,
    special_distribution = EXCLUDED.special_distribution,
    commune_code = EXCLUDED.commune_code,
    cedex_code = EXCLUDED.cedex_code,
    cedex_label = EXCLUDED.cedex_label,
    foreign_country_code = EXCLUDED.foreign_country_code,
    foreign_country_label = EXCLUDED.foreign_country_label,
    address_identifier = EXCLUDED.address_identifier,
    lambert_x = EXCLUDED.lambert_x,
    lambert_y = EXCLUDED.lambert_y,
    address_complement_2 = EXCLUDED.address_complement_2,
    street_number_2 = EXCLUDED.street_number_2,
    street_number_suffix_2 = EXCLUDED.street_number_suffix_2,
    street_type_2 = EXCLUDED.street_type_2,
    street_label_2 = EXCLUDED.street_label_2,
    postal_code_2 = EXCLUDED.postal_code_2,
    city_label_2 = EXCLUDED.city_label_2,
    foreign_city_label_2 = EXCLUDED.foreign_city_label_2,
    special_distribution_2 = EXCLUDED.special_distribution_2,
    commune_code_2 = EXCLUDED.commune_code_2,
    cedex_code_2 = EXCLUDED.cedex_code_2,
    cedex_label_2 = EXCLUDED.cedex_label_2,
    foreign_country_code_2 = EXCLUDED.foreign_country_code_2,
    foreign_country_label_2 = EXCLUDED.foreign_country_label_2,
    period_started_at = EXCLUDED.period_started_at,
    administrative_status = EXCLUDED.administrative_status,
    trade_name_1 = EXCLUDED.trade_name_1,
    trade_name_2 = EXCLUDED.trade_name_2,
    trade_name_3 = EXCLUDED.trade_name_3,
    usual_name = EXCLUDED.usual_name,
    primary_activity_code = EXCLUDED.primary_activity_code,
    primary_activity_nomenclature = EXCLUDED.primary_activity_nomenclature,
    is_employer = EXCLUDED.is_employer,
    primary_activity_naf25_code = EXCLUDED.primary_activity_naf25_code,
    raw_payload = EXCLUDED.raw_payload,
    is_current = true,
    last_seen_at = now(),
    metadata = EXCLUDED.metadata
  RETURNING id
)
SELECT
  id,
  1::integer AS rows_written
FROM upserted;
