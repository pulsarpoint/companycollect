-- name: GetSyncCheckpoint :one
SELECT source_name, cursor, last_completed_at, updated_at
FROM source_sync_checkpoints
WHERE source_name = $1;
