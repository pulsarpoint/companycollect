-- name: CreateTemporalScheduleMetadata :one
INSERT INTO temporal_schedule_metadata (
  temporal_schedule_id,
  workflow_key,
  workflow_name,
  task_queue,
  domain,
  purpose,
  display_name,
  description,
  enabled,
  tags,
  metadata
) VALUES (
  @temporal_schedule_id,
  @workflow_key,
  @workflow_name,
  @task_queue,
  @domain,
  @purpose,
  @display_name,
  @description,
  @enabled,
  @tags,
  @metadata
)
RETURNING *;

-- name: UpdateTemporalScheduleMetadata :one
UPDATE temporal_schedule_metadata
SET
  workflow_key = @workflow_key,
  workflow_name = @workflow_name,
  task_queue = @task_queue,
  domain = @domain,
  purpose = @purpose,
  display_name = @display_name,
  description = @description,
  enabled = @enabled,
  tags = @tags,
  metadata = @metadata
WHERE temporal_schedule_id = @temporal_schedule_id
RETURNING *;

-- name: GetTemporalScheduleMetadata :one
SELECT *
FROM temporal_schedule_metadata
WHERE temporal_schedule_id = @temporal_schedule_id;

-- name: ListTemporalScheduleMetadata :many
SELECT *
FROM temporal_schedule_metadata
WHERE
  (@workflow_key::text = '' OR workflow_key = @workflow_key)
  AND (@domain::text = '' OR domain = @domain)
ORDER BY created_at DESC, temporal_schedule_id ASC;

-- name: DeleteTemporalScheduleMetadata :exec
DELETE FROM temporal_schedule_metadata
WHERE temporal_schedule_id = @temporal_schedule_id;
