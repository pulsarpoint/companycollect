-- name: GetSourceByName :one
SELECT * FROM data_sources WHERE name = $1;

-- name: ListSources :many
SELECT * FROM data_sources ORDER BY name;

-- name: UpdateSourceEnabled :exec
UPDATE data_sources SET enabled = $2, updated_at = now() WHERE name = $1;

-- name: UpdateSourceScheduleEnabled :exec
UPDATE data_sources SET schedule_enabled = $2, updated_at = now() WHERE name = $1;

-- name: UpdateSourceSchedule :exec
UPDATE data_sources
SET schedule_kind = $2, schedule_expression = $3, updated_at = now()
WHERE name = $1;

-- name: UpdateSourceConfig :exec
UPDATE data_sources SET config = $2, updated_at = now() WHERE name = $1;

-- name: UpdateSourceStarted :exec
UPDATE data_sources SET last_started_at = now(), updated_at = now() WHERE name = $1;

-- name: GetSourcesWithCapabilities :many
SELECT * FROM data_sources
WHERE array_length(capabilities, 1) > 0
ORDER BY name;
