BEGIN;

CREATE TEMP TABLE fixture_rows (
  id,
  file_run_id,
  source_name,
  finished_at,
  run_dir,
  source_file_name,
  content_sha256,
  content_length_bytes,
  records_written
) ON COMMIT DROP AS
SELECT
  *
FROM (
  VALUES
    (
      '5ff2d8aa-1399-40b0-864d-f8c9648b02d3',
      '6c13416f-0640-4687-b1ad-f21bb465cc82',
      'finland_prhytj',
      '2026-06-08T20:15:30.04597Z',
      '/var/lib/corpscout/company-source-fixtures/finland/sources/prhytj/runs/20260608T201348Z-prhytj',
      'source.ndjson',
      '3913c14b88464491ff41a9774d3c632332c455145157c5a5a51b92f8a1f35d59',
      2206435243,
      819175
    ),
    (
      'f07c6543-11ed-4e1e-b54e-a91a0e7ce100',
      '8af58ed2-8b24-4f10-9048-69065f1b70a2',
      'united_states_coloradoentities',
      '2026-06-08T23:40:15.70123Z',
      '/var/lib/corpscout/company-source-fixtures/united_states/sources/coloradoentities/runs/20260608T233837Z-coloradoentities',
      'source.ndjson',
      '836e6c7a5aaf353beefdf900dad4dbf9cd70d5eb2036ac7c39eada0ef5269790',
      1964424829,
      3062196
    ),
    (
      '37b81ec9-57fd-464a-9d56-29832c2e982f',
      '5d20ad6d-6644-4fec-b3d1-975e7dfdba47',
      'united_states_irseobmf',
      '2026-06-08T20:36:44.902965Z',
      '/var/lib/corpscout/company-source-fixtures/united_states/sources/irseobmf/runs/20260608T203550Z-irseobmf',
      'source.ndjson',
      '4ca6c3765a368ead0d3f1b2733e9a24df585ffc95442bcb298fba22d1f25a5b3',
      1079790264,
      1974830
    ),
    (
      '74296752-69f6-4d16-8e3e-aa2e049f5924',
      '5feae2f9-c7c2-4c1b-bf98-3e23e7904e04',
      'united_states_secedgar',
      '2026-06-08T20:35:00.356203Z',
      '/var/lib/corpscout/company-source-fixtures/united_states/sources/secedgar/runs/20260608T203500Z-secedgar',
      'source.json',
      '779a857037e6a22026b98f2fa97ddf806f9e64e330763b5ad2132a58dee600ad',
      795227,
      10400
    )
) AS rows (
  id,
  file_run_id,
  source_name,
  finished_at,
  run_dir,
  source_file_name,
  content_sha256,
  content_length_bytes,
  records_written
);

INSERT INTO data_source_action_runs (
  id,
  source_id,
  action_id,
  action,
  status,
  temporal_workflow_id,
  temporal_run_id,
  started_at,
  finished_at,
  input,
  result,
  error_message,
  created_at
)
SELECT
  fixture_rows.id::uuid,
  data_sources.id,
  data_source_actions.id,
  data_source_actions.action,
  'succeeded',
  'fixture-existing-artifact-' || fixture_rows.source_name,
  NULL,
  fixture_rows.finished_at::timestamptz,
  fixture_rows.finished_at::timestamptz,
  jsonb_build_object(
    'source_name', fixture_rows.source_name,
    'trigger', 'fixture',
    'fixture_key', 'existing_source_artifact'
  ),
  jsonb_build_object(
    'action_run_id', fixture_rows.id,
    'run_dir', fixture_rows.run_dir,
    'source_file_path', fixture_rows.run_dir || '/' || fixture_rows.source_file_name,
    'source_file_name', fixture_rows.source_file_name,
    'content_sha256', fixture_rows.content_sha256,
    'content_length_bytes', fixture_rows.content_length_bytes::bigint,
    'records_written', fixture_rows.records_written::bigint,
    'fixture_key', 'existing_source_artifact'
  ),
  NULL,
  fixture_rows.finished_at::timestamptz
FROM fixture_rows
JOIN data_sources ON data_sources.name = fixture_rows.source_name
JOIN data_source_actions
  ON data_source_actions.source_id = data_sources.id
 AND data_source_actions.action = 'pull_source'
ON CONFLICT (id) DO UPDATE SET
  source_id = EXCLUDED.source_id,
  action_id = EXCLUDED.action_id,
  action = EXCLUDED.action,
  status = EXCLUDED.status,
  temporal_workflow_id = EXCLUDED.temporal_workflow_id,
  temporal_run_id = EXCLUDED.temporal_run_id,
  started_at = EXCLUDED.started_at,
  finished_at = EXCLUDED.finished_at,
  input = EXCLUDED.input,
  result = EXCLUDED.result,
  error_message = EXCLUDED.error_message,
  created_at = EXCLUDED.created_at;

INSERT INTO data_source_file_runs (
  id,
  source_id,
  source_file_id,
  parent_action_run_id,
  status,
  temporal_workflow_id,
  temporal_run_id,
  started_at,
  finished_at,
  path,
  content_sha256,
  content_length_bytes,
  records_written,
  error_message,
  log,
  created_at
)
SELECT
  fixture_rows.file_run_id::uuid,
  data_sources.id,
  data_source_files.id,
  fixture_rows.id::uuid,
  'succeeded',
  'fixture-existing-artifact-file-' || fixture_rows.source_name || '-source',
  NULL,
  fixture_rows.finished_at::timestamptz,
  fixture_rows.finished_at::timestamptz,
  fixture_rows.run_dir || '/' || fixture_rows.source_file_name,
  fixture_rows.content_sha256,
  fixture_rows.content_length_bytes::bigint,
  fixture_rows.records_written::bigint,
  NULL,
  '[]'::jsonb,
  fixture_rows.finished_at::timestamptz
FROM fixture_rows
JOIN data_sources ON data_sources.name = fixture_rows.source_name
JOIN data_source_files
  ON data_source_files.source_id = data_sources.id
 AND data_source_files.file_key = 'source'
ON CONFLICT (id) DO UPDATE SET
  source_id = EXCLUDED.source_id,
  source_file_id = EXCLUDED.source_file_id,
  parent_action_run_id = EXCLUDED.parent_action_run_id,
  status = EXCLUDED.status,
  temporal_workflow_id = EXCLUDED.temporal_workflow_id,
  temporal_run_id = EXCLUDED.temporal_run_id,
  started_at = EXCLUDED.started_at,
  finished_at = EXCLUDED.finished_at,
  path = EXCLUDED.path,
  content_sha256 = EXCLUDED.content_sha256,
  content_length_bytes = EXCLUDED.content_length_bytes,
  records_written = EXCLUDED.records_written,
  error_message = EXCLUDED.error_message,
  log = EXCLUDED.log,
  created_at = EXCLUDED.created_at;

COMMIT;
