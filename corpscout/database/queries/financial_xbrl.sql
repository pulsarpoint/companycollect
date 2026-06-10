-- name: UpsertFinlandPRHXBRLDiscoveryWindow :one
INSERT INTO financial_xbrl.finland_prh_xbrl_discovery_windows (
  source_id,
  registered_date_start,
  registered_date_end,
  action_run_id,
  temporal_workflow_id,
  temporal_run_id
) VALUES (
  sqlc.arg(source_id),
  sqlc.arg(registered_date_start),
  sqlc.arg(registered_date_end),
  sqlc.narg(action_run_id),
  sqlc.narg(temporal_workflow_id),
  sqlc.narg(temporal_run_id)
)
ON CONFLICT (source_id, registered_date_start, registered_date_end) DO UPDATE SET
  action_run_id = EXCLUDED.action_run_id,
  temporal_workflow_id = EXCLUDED.temporal_workflow_id,
  temporal_run_id = EXCLUDED.temporal_run_id,
  updated_at = now()
RETURNING *;

-- name: UpdateFinlandPRHXBRLDiscoveryProgress :one
UPDATE financial_xbrl.finland_prh_xbrl_discovery_windows
SET
  total_results = sqlc.arg(total_results),
  pages_discovered = sqlc.arg(pages_discovered),
  statements_discovered = sqlc.arg(statements_discovered),
  last_completed_page = sqlc.arg(last_completed_page),
  updated_at = now()
WHERE id = sqlc.arg(id)
RETURNING *;

-- name: CompleteFinlandPRHXBRLDiscoveryWindow :one
UPDATE financial_xbrl.finland_prh_xbrl_discovery_windows
SET
  total_results = sqlc.arg(total_results),
  pages_discovered = sqlc.arg(pages_discovered),
  statements_discovered = sqlc.arg(statements_discovered),
  last_completed_page = sqlc.arg(last_completed_page),
  completed_at = now(),
  updated_at = now()
WHERE id = sqlc.arg(id)
RETURNING *;

-- name: UpsertFinlandPRHXBRLStatementArtifact :one
INSERT INTO financial_xbrl.finland_prh_xbrl_statement_artifacts (
  source_id,
  business_id,
  financial_date,
  registration_date,
  source_url,
  first_discovered_run_id,
  latest_action_run_id
) VALUES (
  sqlc.arg(source_id),
  sqlc.arg(business_id),
  sqlc.arg(financial_date),
  sqlc.narg(registration_date),
  sqlc.arg(source_url),
  sqlc.narg(first_discovered_run_id),
  sqlc.narg(latest_action_run_id)
)
ON CONFLICT (source_id, business_id, financial_date) DO UPDATE SET
  registration_date = COALESCE(EXCLUDED.registration_date, financial_xbrl.finland_prh_xbrl_statement_artifacts.registration_date),
  source_url = EXCLUDED.source_url,
  latest_action_run_id = EXCLUDED.latest_action_run_id,
  updated_at = now()
RETURNING *;

-- name: ListFinlandPRHXBRLStatementArtifactsToDownload :many
SELECT *
FROM financial_xbrl.finland_prh_xbrl_statement_artifacts
WHERE source_id = sqlc.arg(source_id)
  AND (
    download_status = 'pending'
    OR (download_status = 'failed' AND sqlc.arg(retry_failed)::boolean)
  )
ORDER BY registration_date NULLS LAST, business_id, financial_date
LIMIT sqlc.arg(row_limit);

-- name: MarkFinlandPRHXBRLStatementArtifactDownloading :one
UPDATE financial_xbrl.finland_prh_xbrl_statement_artifacts
SET
  download_status = 'downloading',
  attempts = attempts + 1,
  last_attempt_at = now(),
  latest_action_run_id = sqlc.narg(latest_action_run_id),
  last_error_message = NULL,
  updated_at = now()
WHERE id = sqlc.arg(id)
RETURNING *;

-- name: MarkFinlandPRHXBRLStatementArtifactSucceeded :one
UPDATE financial_xbrl.finland_prh_xbrl_statement_artifacts
SET
  download_status = 'succeeded',
  xml_path = sqlc.arg(xml_path),
  xml_sha256 = sqlc.arg(xml_sha256),
  xml_size_bytes = sqlc.arg(xml_size_bytes),
  downloaded_at = now(),
  latest_action_run_id = sqlc.narg(latest_action_run_id),
  last_error_message = NULL,
  updated_at = now()
WHERE id = sqlc.arg(id)
RETURNING *;

-- name: MarkFinlandPRHXBRLStatementArtifactFailed :one
UPDATE financial_xbrl.finland_prh_xbrl_statement_artifacts
SET
  download_status = 'failed',
  latest_action_run_id = sqlc.narg(latest_action_run_id),
  last_error_message = NULLIF(sqlc.arg(last_error_message)::text, ''),
  updated_at = now()
WHERE id = sqlc.arg(id)
RETURNING *;
