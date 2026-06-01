-- name: InsertImportBatch :one
INSERT INTO domain_import_batches (filename, csv_s3_key)
VALUES ($1, $2)
RETURNING *;

-- name: UpdateImportBatchRiverJob :exec
UPDATE domain_import_batches SET river_job_id = $2 WHERE id = $1;

-- name: UpdateImportBatchStarted :exec
UPDATE domain_import_batches
SET status = 'processing', rows_total = $2
WHERE id = $1;

-- name: UpdateImportBatchCompleted :exec
UPDATE domain_import_batches
SET status        = $2,
    rows_imported = $3,
    rows_skipped  = $4,
    rows_failed   = $5,
    error_message = $6,
    completed_at  = now()
WHERE id = $1;
