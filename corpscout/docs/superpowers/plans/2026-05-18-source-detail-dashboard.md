# Source Detail Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the existing minimal source detail page with a four-tab operator dashboard for schedule control, editable source config, pull run logs, and raw input queue inspection.

**Architecture:** Add one database migration for `data_sources.schedule_enabled`, the unified `v_source_raw_inputs` PostgREST view, and read grants. Keep raw input state changes behind scheduler Go API endpoints. Build the UI from current scheduler API types plus PostgREST reads, with the Raw Inputs tab hidden for sources outside the raw input allowlist.

**Tech Stack:** PostgreSQL migrations, sqlc, Go 1.26.1, Chi, River, React Router v7, React 19, shadcn-style local UI components, TanStack Table, PostgREST proxy at `/api/v1/db/*`, sonner toasts. Go commands use `cd scheduler && GOWORK=off ...`; UI commands use `cd ui && npm run ...`.

---

## Scope Notes

- Source spec: `docs/superpowers/specs/2026-05-18-source-detail-dashboard-design.md`.
- MVP schedule support is `schedule_kind = "interval"` with Go duration strings such as `24h`.
- `enabled = false` continues to pause automatic scheduling only; manual triggers and already queued workers still run.
- Raw Inputs supports these tables: `gleif_company_raw_inputs`, `companies_house_company_raw_inputs`, `brreg_company_raw_inputs`, `ai_company_profile_raw_inputs`, `domain_discovery_raw_inputs`.
- `nvd_cpe` and `nvd_cve` are not part of this dashboard's Raw Inputs tab because their source tables do not share the row queue shape.

---

## File Map

### Create

- `database/migrations/000020_source_detail_dashboard.up.sql`
- `database/migrations/000020_source_detail_dashboard.down.sql`
- `scheduler/internal/httpapi/raw_inputs.go`
- `scheduler/internal/app/app_test.go`
- `ui/app/components/ui/tabs.tsx`
- `ui/app/components/app/source-detail/sourceDetailUtils.ts`
- `ui/app/components/app/source-detail/SourceHeader.tsx`
- `ui/app/components/app/source-detail/ScheduleTab.tsx`
- `ui/app/components/app/source-detail/ConfigTab.tsx`
- `ui/app/components/app/source-detail/LogsTab.tsx`
- `ui/app/components/app/source-detail/RawInputsTab.tsx`
- `ui/app/components/app/source-detail/RawInputSheet.tsx`

### Modify

- `database/queries/sources.sql`
- `database/queries/raw_inputs.sql`
- `scheduler/internal/db/gen/*`
- `scheduler/internal/app/app.go`
- `scheduler/internal/httpapi/handlers.go`
- `scheduler/internal/httpapi/sources.go`
- `scheduler/internal/httpapi/sources_test.go`
- `scheduler/internal/httpapi/testhelpers_test.go`
- `ui/app/types/api.ts`
- `ui/app/lib/api.ts`
- `ui/app/routes/sources.tsx`
- `ui/app/routes/sources_.$name.tsx`
- `ui/app/components/app/SourcesTable.tsx`
- `ui/app/components/app/PullRunsTable.tsx`

---

## Task 1: Database Shape And sqlc Generation

**Files:**
- Create: `database/migrations/000020_source_detail_dashboard.up.sql`
- Create: `database/migrations/000020_source_detail_dashboard.down.sql`
- Modify: `database/queries/sources.sql`
- Modify: `database/queries/raw_inputs.sql`
- Modify generated: `scheduler/internal/db/gen/*`
- Modify test stub: `scheduler/internal/httpapi/testhelpers_test.go`

- [ ] **Step 1: Add the up migration**

Create `database/migrations/000020_source_detail_dashboard.up.sql`:

```sql
ALTER TABLE data_sources
    ADD COLUMN schedule_enabled BOOLEAN NOT NULL DEFAULT TRUE;

CREATE OR REPLACE VIEW v_source_raw_inputs AS
  SELECT
    id,
    'gleif' AS source_name,
    'gleif_company_raw_inputs' AS source_input_table,
    lei AS source_native_id,
    processing_status,
    processing_attempts,
    processing_error,
    first_seen_at,
    last_seen_at,
    payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'gleif_company_raw_inputs'
        AND ssl.source_input_key = id::text
    ) AS has_suggestion
  FROM gleif_company_raw_inputs

  UNION ALL

  SELECT
    id,
    'companies_house' AS source_name,
    'companies_house_company_raw_inputs' AS source_input_table,
    company_number AS source_native_id,
    processing_status,
    processing_attempts,
    processing_error,
    first_seen_at,
    last_seen_at,
    payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'companies_house_company_raw_inputs'
        AND ssl.source_input_key = id::text
    ) AS has_suggestion
  FROM companies_house_company_raw_inputs

  UNION ALL

  SELECT
    id,
    'brreg' AS source_name,
    'brreg_company_raw_inputs' AS source_input_table,
    organization_number AS source_native_id,
    processing_status,
    processing_attempts,
    processing_error,
    first_seen_at,
    last_seen_at,
    payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'brreg_company_raw_inputs'
        AND ssl.source_input_key = id::text
    ) AS has_suggestion
  FROM brreg_company_raw_inputs

  UNION ALL

  SELECT
    id,
    'ai_company_profile' AS source_name,
    'ai_company_profile_raw_inputs' AS source_input_table,
    COALESCE(normalized_domain, '') AS source_native_id,
    processing_status,
    processing_attempts,
    processing_error,
    first_seen_at,
    last_seen_at,
    payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'ai_company_profile_raw_inputs'
        AND ssl.source_input_key = id::text
    ) AS has_suggestion
  FROM ai_company_profile_raw_inputs

  UNION ALL

  SELECT
    id,
    'domain_discovery' AS source_name,
    'domain_discovery_raw_inputs' AS source_input_table,
    domain AS source_native_id,
    processing_status,
    processing_attempts,
    processing_error,
    first_seen_at,
    last_seen_at,
    payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'domain_discovery_raw_inputs'
        AND ssl.source_input_key = id::text
    ) AS has_suggestion
  FROM domain_discovery_raw_inputs;

GRANT SELECT ON v_source_raw_inputs TO corpscout_anon;
GRANT SELECT ON gleif_company_raw_inputs TO corpscout_anon;
GRANT SELECT ON companies_house_company_raw_inputs TO corpscout_anon;
GRANT SELECT ON brreg_company_raw_inputs TO corpscout_anon;
GRANT SELECT ON ai_company_profile_raw_inputs TO corpscout_anon;
GRANT SELECT ON domain_discovery_raw_inputs TO corpscout_anon;
GRANT SELECT ON suggestion_source_links TO corpscout_anon;
```

- [ ] **Step 2: Add the down migration**

Create `database/migrations/000020_source_detail_dashboard.down.sql`:

```sql
REVOKE SELECT ON suggestion_source_links FROM corpscout_anon;
REVOKE SELECT ON domain_discovery_raw_inputs FROM corpscout_anon;
REVOKE SELECT ON ai_company_profile_raw_inputs FROM corpscout_anon;
REVOKE SELECT ON brreg_company_raw_inputs FROM corpscout_anon;
REVOKE SELECT ON companies_house_company_raw_inputs FROM corpscout_anon;
REVOKE SELECT ON gleif_company_raw_inputs FROM corpscout_anon;
REVOKE SELECT ON v_source_raw_inputs FROM corpscout_anon;

DROP VIEW IF EXISTS v_source_raw_inputs;

ALTER TABLE data_sources
    DROP COLUMN IF EXISTS schedule_enabled;
```

- [ ] **Step 3: Extend source queries**

Append this query to `database/queries/sources.sql` after `UpdateSourceEnabled`:

```sql
-- name: UpdateSourceScheduleEnabled :exec
UPDATE data_sources SET schedule_enabled = $2, updated_at = now() WHERE name = $1;
```

Keep `UpdateSourceConfig` as the single config write query.

- [ ] **Step 4: Add raw input retry/ignore queries**

Append these queries to `database/queries/raw_inputs.sql`:

```sql
-- name: RetryGLEIFRawInput :one
UPDATE gleif_company_raw_inputs
SET processing_status = 'pending',
    processing_error = NULL,
    processing_lease_by = NULL,
    processing_lease_until = NULL,
    processed_at = NULL,
    updated_at = now()
WHERE id = $1 AND processing_status IN ('failed', 'ignored')
RETURNING id;

-- name: IgnoreGLEIFRawInput :one
UPDATE gleif_company_raw_inputs
SET processing_status = 'ignored',
    processing_lease_by = NULL,
    processing_lease_until = NULL,
    updated_at = now()
WHERE id = $1 AND processing_status IN ('pending', 'failed')
RETURNING id;

-- name: RetryCompaniesHouseRawInput :one
UPDATE companies_house_company_raw_inputs
SET processing_status = 'pending',
    processing_error = NULL,
    processing_lease_by = NULL,
    processing_lease_until = NULL,
    processed_at = NULL,
    updated_at = now()
WHERE id = $1 AND processing_status IN ('failed', 'ignored')
RETURNING id;

-- name: IgnoreCompaniesHouseRawInput :one
UPDATE companies_house_company_raw_inputs
SET processing_status = 'ignored',
    processing_lease_by = NULL,
    processing_lease_until = NULL,
    updated_at = now()
WHERE id = $1 AND processing_status IN ('pending', 'failed')
RETURNING id;

-- name: RetryBrregRawInput :one
UPDATE brreg_company_raw_inputs
SET processing_status = 'pending',
    processing_error = NULL,
    processing_lease_by = NULL,
    processing_lease_until = NULL,
    processed_at = NULL,
    updated_at = now()
WHERE id = $1 AND processing_status IN ('failed', 'ignored')
RETURNING id;

-- name: IgnoreBrregRawInput :one
UPDATE brreg_company_raw_inputs
SET processing_status = 'ignored',
    processing_lease_by = NULL,
    processing_lease_until = NULL,
    updated_at = now()
WHERE id = $1 AND processing_status IN ('pending', 'failed')
RETURNING id;

-- name: RetryAIRawInput :one
UPDATE ai_company_profile_raw_inputs
SET processing_status = 'pending',
    processing_error = NULL,
    processing_lease_by = NULL,
    processing_lease_until = NULL,
    processed_at = NULL,
    updated_at = now()
WHERE id = $1 AND processing_status IN ('failed', 'ignored')
RETURNING id;

-- name: IgnoreAIRawInput :one
UPDATE ai_company_profile_raw_inputs
SET processing_status = 'ignored',
    processing_lease_by = NULL,
    processing_lease_until = NULL,
    updated_at = now()
WHERE id = $1 AND processing_status IN ('pending', 'failed')
RETURNING id;

-- name: RetryDomainDiscoveryRawInput :one
UPDATE domain_discovery_raw_inputs
SET processing_status = 'pending',
    processing_error = NULL,
    processing_lease_by = NULL,
    processing_lease_until = NULL,
    processed_at = NULL,
    updated_at = now()
WHERE id = $1 AND processing_status IN ('failed', 'ignored')
RETURNING id;

-- name: IgnoreDomainDiscoveryRawInput :one
UPDATE domain_discovery_raw_inputs
SET processing_status = 'ignored',
    processing_lease_by = NULL,
    processing_lease_until = NULL,
    updated_at = now()
WHERE id = $1 AND processing_status IN ('pending', 'failed')
RETURNING id;
```

- [ ] **Step 5: Regenerate sqlc**

Run:

```bash
cd scheduler && GOWORK=off sqlc generate -f ../database/sqlc.yaml
```

Expected: command exits 0 and generated files under `scheduler/internal/db/gen/` include `ScheduleEnabled bool` on `DataSource` plus the ten new raw input methods.

- [ ] **Step 6: Update the HTTP test stub**

Add zero-value stub methods to `scheduler/internal/httpapi/testhelpers_test.go` so `stubQuerier` still satisfies `db.Querier`:

```go
func (s *stubQuerier) UpdateSourceScheduleEnabled(ctx context.Context, arg db.UpdateSourceScheduleEnabledParams) error {
	ret := s.Called(ctx, arg)
	return ret.Error(0)
}

func (s *stubQuerier) RetryGLEIFRawInput(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(uuid.UUID), ret.Error(1)
}

func (s *stubQuerier) IgnoreGLEIFRawInput(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(uuid.UUID), ret.Error(1)
}

func (s *stubQuerier) RetryCompaniesHouseRawInput(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(uuid.UUID), ret.Error(1)
}

func (s *stubQuerier) IgnoreCompaniesHouseRawInput(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(uuid.UUID), ret.Error(1)
}

func (s *stubQuerier) RetryBrregRawInput(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(uuid.UUID), ret.Error(1)
}

func (s *stubQuerier) IgnoreBrregRawInput(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(uuid.UUID), ret.Error(1)
}

func (s *stubQuerier) RetryAIRawInput(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(uuid.UUID), ret.Error(1)
}

func (s *stubQuerier) IgnoreAIRawInput(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(uuid.UUID), ret.Error(1)
}

func (s *stubQuerier) RetryDomainDiscoveryRawInput(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(uuid.UUID), ret.Error(1)
}

func (s *stubQuerier) IgnoreDomainDiscoveryRawInput(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(uuid.UUID), ret.Error(1)
}
```

- [ ] **Step 7: Verify backend compiles after generation**

Run:

```bash
cd scheduler && GOWORK=off go test ./internal/httpapi ./internal/db/gen
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add database/migrations/000020_source_detail_dashboard.*.sql database/queries/sources.sql database/queries/raw_inputs.sql scheduler/internal/db/gen scheduler/internal/httpapi/testhelpers_test.go
git commit -m "feat: add source dashboard database contracts"
```

---

## Task 2: Source Scheduling And PATCH API

**Files:**
- Modify: `scheduler/internal/app/app.go`
- Create: `scheduler/internal/app/app_test.go`
- Modify: `scheduler/internal/httpapi/sources.go`
- Modify: `scheduler/internal/httpapi/sources_test.go`

- [ ] **Step 1: Add scheduler helper tests**

Create `scheduler/internal/app/app_test.go`:

```go
package app

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func TestSourceScheduleDueSkipsDisabledSource(t *testing.T) {
	due, err := sourceScheduleDue(db.DataSource{
		Enabled:            false,
		ScheduleEnabled:    true,
		ScheduleKind:       "interval",
		ScheduleExpression: ptrString("24h"),
	}, time.Now())
	require.NoError(t, err)
	require.False(t, due)
}

func TestSourceScheduleDueSkipsPausedSchedule(t *testing.T) {
	due, err := sourceScheduleDue(db.DataSource{
		Enabled:            true,
		ScheduleEnabled:    false,
		ScheduleKind:       "interval",
		ScheduleExpression: ptrString("24h"),
	}, time.Now())
	require.NoError(t, err)
	require.False(t, due)
}

func TestSourceScheduleDueAcceptsDueInterval(t *testing.T) {
	now := time.Date(2026, 5, 18, 10, 0, 0, 0, time.UTC)
	due, err := sourceScheduleDue(db.DataSource{
		Enabled:            true,
		ScheduleEnabled:    true,
		ScheduleKind:       "interval",
		ScheduleExpression: ptrString("24h"),
		LastStartedAt: pgtype.Timestamptz{
			Time:  now.Add(-25 * time.Hour),
			Valid: true,
		},
	}, now)
	require.NoError(t, err)
	require.True(t, due)
}

func TestSourceScheduleDueRejectsInvalidDuration(t *testing.T) {
	due, err := sourceScheduleDue(db.DataSource{
		Enabled:            true,
		ScheduleEnabled:    true,
		ScheduleKind:       "interval",
		ScheduleExpression: ptrString("every 24h"),
	}, time.Now())
	require.Error(t, err)
	require.False(t, due)
}

func ptrString(s string) *string {
	return &s
}
```

- [ ] **Step 2: Add the scheduler helper and use it**

In `scheduler/internal/app/app.go`, add this helper below `scheduleSources` and update `scheduleOnce` to call it:

```go
func sourceScheduleDue(src db.DataSource, now time.Time) (bool, error) {
	if !src.Enabled {
		return false, nil
	}
	if !src.ScheduleEnabled {
		return false, nil
	}
	if src.ScheduleKind != "interval" {
		return false, nil
	}
	if src.ScheduleExpression == nil {
		return false, nil
	}
	interval, err := time.ParseDuration(*src.ScheduleExpression)
	if err != nil {
		return false, errors.Wrap(err, "parse schedule expression")
	}
	if src.LastStartedAt.Valid && now.Before(src.LastStartedAt.Time.Add(interval)) {
		return false, nil
	}
	return true, nil
}
```

Replace the inline guards in `scheduleOnce` with:

```go
due, err := sourceScheduleDue(src, time.Now())
if err != nil {
	slog.Warn("schedule sources: invalid schedule_expression", "source", src.Name, "expr", src.ScheduleExpression, "error", err)
	continue
}
if !due {
	continue
}
```

- [ ] **Step 3: Add PATCH tests**

Add these tests to `scheduler/internal/httpapi/sources_test.go`:

```go
func TestPatchSource_updates_schedule_enabled(t *testing.T) {
	q := &stubQuerier{}
	q.On("UpdateSourceScheduleEnabled", mock.Anything, db.UpdateSourceScheduleEnabledParams{
		Name: "gleif", ScheduleEnabled: false,
	}).Return(nil)

	r := routerForHandlers(q)
	body := strings.NewReader(`{"schedule_enabled": false}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	q.AssertExpectations(t)
}

func TestPatchSource_rejects_invalid_schedule_expression(t *testing.T) {
	q := &stubQuerier{}
	q.On("GetSourceByName", mock.Anything, "gleif").Return(db.DataSource{
		Name:               "gleif",
		ScheduleKind:       "interval",
		ScheduleExpression: ptrString("24h"),
	}, nil)

	r := routerForHandlers(q)
	body := strings.NewReader(`{"schedule_expression": "every 24h"}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
}

func TestPatchSource_merges_config_preserving_types(t *testing.T) {
	q := &stubQuerier{}
	q.On("GetSourceByName", mock.Anything, "gleif").Return(db.DataSource{
		Name:   "gleif",
		Config: json.RawMessage(`{"base_url":"https://old.example","rate_limit_rps":2}`),
	}, nil)
	q.On("UpdateSourceConfig", mock.Anything, db.UpdateSourceConfigParams{
		Name: "gleif",
		Config: json.RawMessage(`{"base_url":"https://old.example","rate_limit_rps":5}`),
	}).Return(nil)

	r := routerForHandlers(q)
	body := strings.NewReader(`{"config":{"rate_limit_rps":5}}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	q.AssertExpectations(t)
}

func TestPatchSource_rejects_secret_config_key(t *testing.T) {
	q := &stubQuerier{}
	r := routerForHandlers(q)
	body := strings.NewReader(`{"config":{"api_token":"secret"}}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/gleif", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
}
```

- [ ] **Step 4: Extend `patchSourceRequest` and config helpers**

In `scheduler/internal/httpapi/sources.go`, extend imports with:

```go
import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"time"
)
```

Replace `patchSourceRequest` with:

```go
type patchSourceRequest struct {
	Enabled            *bool                      `json:"enabled"`
	ScheduleEnabled    *bool                      `json:"schedule_enabled"`
	ScheduleKind       *string                    `json:"schedule_kind"`
	ScheduleExpression *string                    `json:"schedule_expression"`
	Config             map[string]json.RawMessage `json:"config"`
}
```

Add helpers near the request type:

```go
var forbiddenConfigKey = regexp.MustCompile(`(?i)(key|secret|token|password)`)

func validateConfigPatch(patch map[string]json.RawMessage) error {
	for key, raw := range patch {
		if forbiddenConfigKey.MatchString(key) {
			return errors.Newf("config key %q is not allowed", key)
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return errors.Wrapf(err, "config value for %q must be valid json", key)
		}
		if err := validateNestedConfigKeys(key, value); err != nil {
			return err
		}
	}
	return nil
}

func validateNestedConfigKeys(path string, value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	for key, nested := range obj {
		if forbiddenConfigKey.MatchString(key) {
			return errors.Newf("config key %q is not allowed", path+"."+key)
		}
		if err := validateNestedConfigKeys(path+"."+key, nested); err != nil {
			return err
		}
	}
	return nil
}

func mergeConfig(existing json.RawMessage, patch map[string]json.RawMessage) (json.RawMessage, error) {
	merged := map[string]json.RawMessage{}
	if len(existing) > 0 && string(existing) != "null" {
		if err := json.Unmarshal(existing, &merged); err != nil {
			return nil, errors.Wrap(err, "decode existing config")
		}
	}
	for key, value := range patch {
		merged[key] = value
	}
	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	buf := bytes.NewBufferString("{")
	for i, key := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		keyJSON, _ := json.Marshal(key)
		buf.Write(keyJSON)
		buf.WriteByte(':')
		buf.Write(merged[key])
	}
	buf.WriteByte('}')
	return json.RawMessage(buf.Bytes()), nil
}
```

Add `github.com/cockroachdb/errors` to the imports.

- [ ] **Step 5: Update `handlePatchSource`**

In `handlePatchSource`, after the enabled block, add:

```go
if req.ScheduleEnabled != nil {
	if err := h.db.UpdateSourceScheduleEnabled(r.Context(), db.UpdateSourceScheduleEnabledParams{
		Name: name, ScheduleEnabled: *req.ScheduleEnabled,
	}); err != nil {
		slog.Error("update source schedule enabled", "name", name, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
}
```

Before calling `UpdateSourceSchedule`, validate interval expressions:

```go
if kind == "interval" && expr != nil {
	if _, err := time.ParseDuration(*expr); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "schedule_expression must be a Go duration")
		return
	}
}
```

After the schedule block, add config merging:

```go
if req.Config != nil {
	if err := validateConfigPatch(req.Config); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	src, err := h.db.GetSourceByName(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusNotFound, "source not found")
		return
	}
	merged, err := mergeConfig(src.Config, req.Config)
	if err != nil {
		slog.Error("merge source config", "name", name, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if err := h.db.UpdateSourceConfig(r.Context(), db.UpdateSourceConfigParams{
		Name: name, Config: merged,
	}); err != nil {
		slog.Error("update source config", "name", name, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
}
```

- [ ] **Step 6: Verify**

Run:

```bash
cd scheduler && GOWORK=off go test ./internal/app ./internal/httpapi
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add scheduler/internal/app/app.go scheduler/internal/app/app_test.go scheduler/internal/httpapi/sources.go scheduler/internal/httpapi/sources_test.go
git commit -m "feat: add source scheduling and config controls"
```

---

## Task 3: Raw Input Retry And Ignore API

**Files:**
- Create: `scheduler/internal/httpapi/raw_inputs.go`
- Modify: `scheduler/internal/httpapi/handlers.go`
- Modify: `scheduler/internal/httpapi/sources_test.go`

- [ ] **Step 1: Add handler tests**

Append to `scheduler/internal/httpapi/sources_test.go`:

```go
func TestRetryRawInput_returns_422_for_unsupported_source(t *testing.T) {
	q := &stubQuerier{}
	rowID := uuid.New()
	q.On("GetSourceByName", mock.Anything, "nvd_cpe").Return(db.DataSource{
		Name:           "nvd_cpe",
		InputTableName: "cpe_dictionary",
	}, nil)

	r := routerForHandlers(q)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/nvd_cpe/raw-inputs/"+rowID.String()+"/retry", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	q.AssertExpectations(t)
}

func TestRetryRawInput_resets_ai_row_without_river(t *testing.T) {
	q := &stubQuerier{}
	rowID := uuid.New()
	q.On("GetSourceByName", mock.Anything, "ai_company_profile").Return(db.DataSource{
		Name:           "ai_company_profile",
		InputTableName: "ai_company_profile_raw_inputs",
	}, nil)
	q.On("RetryAIRawInput", mock.Anything, rowID).Return(rowID, nil)

	r := routerForHandlers(q)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/ai_company_profile/raw-inputs/"+rowID.String()+"/retry", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	q.AssertExpectations(t)
}

func TestIgnoreRawInput_resets_domain_row(t *testing.T) {
	q := &stubQuerier{}
	rowID := uuid.New()
	q.On("GetSourceByName", mock.Anything, "domain_discovery").Return(db.DataSource{
		Name:           "domain_discovery",
		InputTableName: "domain_discovery_raw_inputs",
	}, nil)
	q.On("IgnoreDomainDiscoveryRawInput", mock.Anything, rowID).Return(rowID, nil)

	r := routerForHandlers(q)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/domain_discovery/raw-inputs/"+rowID.String()+"/ignore", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	q.AssertExpectations(t)
}

func TestRetryRawInput_returns_503_when_processor_source_needs_river(t *testing.T) {
	q := &stubQuerier{}
	rowID := uuid.New()
	q.On("GetSourceByName", mock.Anything, "gleif").Return(db.DataSource{
		Name:           "gleif",
		InputTableName: "gleif_company_raw_inputs",
	}, nil)

	r := routerForHandlers(q)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/gleif/raw-inputs/"+rowID.String()+"/retry", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	q.AssertExpectations(t)
}
```

- [ ] **Step 2: Create the raw input handler file**

Create `scheduler/internal/httpapi/raw_inputs.go`:

```go
package httpapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/cockroachdb/errors"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

type rawInputSupport struct {
	canProcess bool
	retry      func(context.Context, uuid.UUID) (uuid.UUID, error)
	ignore     func(context.Context, uuid.UUID) (uuid.UUID, error)
}

func (h *Handlers) rawInputSupport(src db.DataSource) (rawInputSupport, bool) {
	switch src.InputTableName {
	case "gleif_company_raw_inputs":
		return rawInputSupport{canProcess: true, retry: h.db.RetryGLEIFRawInput, ignore: h.db.IgnoreGLEIFRawInput}, true
	case "companies_house_company_raw_inputs":
		return rawInputSupport{canProcess: true, retry: h.db.RetryCompaniesHouseRawInput, ignore: h.db.IgnoreCompaniesHouseRawInput}, true
	case "brreg_company_raw_inputs":
		return rawInputSupport{canProcess: true, retry: h.db.RetryBrregRawInput, ignore: h.db.IgnoreBrregRawInput}, true
	case "ai_company_profile_raw_inputs":
		return rawInputSupport{retry: h.db.RetryAIRawInput, ignore: h.db.IgnoreAIRawInput}, true
	case "domain_discovery_raw_inputs":
		return rawInputSupport{retry: h.db.RetryDomainDiscoveryRawInput, ignore: h.db.IgnoreDomainDiscoveryRawInput}, true
	default:
		return rawInputSupport{}, false
	}
}

func (h *Handlers) handleRetryRawInput(w http.ResponseWriter, r *http.Request) {
	src, rowID, support, ok := h.resolveRawInputAction(w, r)
	if !ok {
		return
	}
	if support.canProcess && h.rv == nil {
		writeError(w, http.StatusServiceUnavailable, "scheduler not available")
		return
	}
	if _, err := support.retry(r.Context(), rowID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusUnprocessableEntity, "raw input row is not retryable")
			return
		}
		slog.Error("retry raw input", "source", src.Name, "row_id", rowID, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if support.canProcess {
		if _, err := h.rv.Insert(r.Context(), workers.SourceProcessArgs{
			SourceName: src.Name,
		}, &river.InsertOpts{
			Queue: "source_process",
			UniqueOpts: river.UniqueOpts{
				ByArgs:  true,
				ByState: []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateRunning, rivertype.JobStateScheduled},
			},
		}); err != nil {
			slog.Error("enqueue source process after retry", "source", src.Name, "row_id", rowID, "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "retried"})
}

func (h *Handlers) handleIgnoreRawInput(w http.ResponseWriter, r *http.Request) {
	src, rowID, support, ok := h.resolveRawInputAction(w, r)
	if !ok {
		return
	}
	if _, err := support.ignore(r.Context(), rowID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusUnprocessableEntity, "raw input row cannot be ignored")
			return
		}
		slog.Error("ignore raw input", "source", src.Name, "row_id", rowID, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ignored"})
}

func (h *Handlers) resolveRawInputAction(w http.ResponseWriter, r *http.Request) (db.DataSource, uuid.UUID, rawInputSupport, bool) {
	name := chi.URLParam(r, "name")
	rowID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid raw input id")
		return db.DataSource{}, uuid.UUID{}, rawInputSupport{}, false
	}
	src, err := h.db.GetSourceByName(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusNotFound, "source not found")
		return db.DataSource{}, uuid.UUID{}, rawInputSupport{}, false
	}
	support, supported := h.rawInputSupport(src)
	if !supported {
		writeError(w, http.StatusUnprocessableEntity, "raw input retry not supported for this source")
		return db.DataSource{}, uuid.UUID{}, rawInputSupport{}, false
	}
	return src, rowID, support, true
}
```

- [ ] **Step 3: Register the routes**

In `scheduler/internal/httpapi/handlers.go`, add:

```go
r.Post("/sources/{name}/raw-inputs/{id}/retry", h.handleRetryRawInput)
r.Post("/sources/{name}/raw-inputs/{id}/ignore", h.handleIgnoreRawInput)
```

Place these next to the existing source routes.

- [ ] **Step 4: Verify**

Run:

```bash
cd scheduler && GOWORK=off go test ./internal/httpapi
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scheduler/internal/httpapi/raw_inputs.go scheduler/internal/httpapi/handlers.go scheduler/internal/httpapi/sources_test.go
git commit -m "feat: add raw input retry and ignore endpoints"
```

---

## Task 4: UI API Types And Shared Utilities

**Files:**
- Modify: `ui/app/types/api.ts`
- Modify: `ui/app/lib/api.ts`
- Create: `ui/app/components/app/source-detail/sourceDetailUtils.ts`
- Modify: `ui/app/components/app/PullRunsTable.tsx`

- [ ] **Step 1: Replace stale source and pull-run types**

In `ui/app/types/api.ts`, replace `SourceConfig`, `DataSource`, `PullRun`, and `PullRunsResponse` with:

```ts
export type SourceConfig = Record<string, unknown>;

export interface DataSource {
  id: string;
  name: string;
  display_name: string | null;
  description: string | null;
  source_group: string;
  input_table_name: string;
  pull_task_type: string;
  processor_task_type: string | null;
  enabled: boolean;
  schedule_enabled: boolean;
  schedule_kind: "manual" | "interval" | "cron" | "event";
  schedule_expression: string | null;
  config: SourceConfig;
  last_started_at: string | null;
  last_success_at: string | null;
  last_failed_at: string | null;
  last_source_marker_type: string | null;
  last_source_marker: string | null;
  last_source_modified_at: string | null;
  last_error: string | null;
  consecutive_failures: number;
  created_at: string;
  updated_at: string;
}

export interface PullRun {
  id: string;
  source_id: string;
  source_name: string;
  river_job_id: number | null;
  task_type: string;
  trigger_type: string;
  status: "running" | "succeeded" | "failed" | "cancelled";
  started_at: string;
  finished_at: string | null;
  rows_seen: number;
  raw_rows_inserted: number;
  raw_rows_updated: number;
  raw_rows_unchanged: number;
  error_message: string | null;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface PullRunsResponse {
  items: PullRun[];
  page: number;
  limit: number;
}
```

Append raw input types:

```ts
export interface SourceRawInput {
  id: string;
  source_name: string;
  source_input_table: string;
  source_native_id: string;
  processing_status: "pending" | "processing" | "processed" | "failed" | "ignored" | "superseded";
  processing_attempts: number;
  processing_error: string | null;
  first_seen_at: string;
  last_seen_at: string;
  payload_hash: string;
  has_suggestion: boolean;
}

export interface SuggestionSourceLink {
  id: string;
  suggestion_table: string;
  suggestion_id: string;
  source_id: string;
  source_input_table: string;
  source_input_key: string;
  source_pull_run_id: string | null;
  confidence: number | null;
  evidence_excerpt: string | null;
  created_at: string;
}

export interface RawPayloadRow {
  raw_payload: Record<string, unknown>;
}
```

- [ ] **Step 2: Extend the scheduler API client**

In `ui/app/lib/api.ts`, replace `patchSource` with:

```ts
patchSource: (
  name: string,
  body: {
    enabled?: boolean;
    schedule_enabled?: boolean;
    schedule_kind?: DataSource["schedule_kind"];
    schedule_expression?: string | null;
    config?: Record<string, unknown>;
  },
) => patch<{ status: string }>(`/sources/${name}`, body),
```

Add:

```ts
retryRawInput: (name: string, id: string) =>
  post<{ status: string }>(`/sources/${name}/raw-inputs/${id}/retry`, {}),

ignoreRawInput: (name: string, id: string) =>
  post<{ status: string }>(`/sources/${name}/raw-inputs/${id}/ignore`, {}),
```

- [ ] **Step 3: Add shared source detail utilities**

Create `ui/app/components/app/source-detail/sourceDetailUtils.ts`:

```ts
import type { DataSource, SourceRawInput } from "~/types/api";

export const RAW_INPUT_TABLES = new Set([
  "gleif_company_raw_inputs",
  "companies_house_company_raw_inputs",
  "brreg_company_raw_inputs",
  "ai_company_profile_raw_inputs",
  "domain_discovery_raw_inputs",
]);

export function hasRawInputs(source: DataSource): boolean {
  return RAW_INPUT_TABLES.has(source.input_table_name);
}

export function sourceDisplayName(source: DataSource): string {
  return source.display_name || source.name;
}

export function validateDuration(value: string): string | undefined {
  if (!/^\d+[hms]$/.test(value.trim())) {
    return "Use a Go duration such as 24h, 12h, or 30m.";
  }
  return undefined;
}

export function statusClass(status: string): string {
  if (status === "succeeded" || status === "processed") return "bg-green-100 text-green-800 border-green-200";
  if (status === "failed") return "bg-red-100 text-red-800 border-red-200";
  if (status === "running" || status === "processing") return "bg-blue-100 text-blue-800 border-blue-200";
  if (status === "ignored" || status === "cancelled" || status === "superseded") return "bg-gray-100 text-gray-700 border-gray-200";
  return "bg-amber-100 text-amber-800 border-amber-200";
}

export function liveStatusFilter(statusGroup: "live" | "archive"): string {
  return statusGroup === "live"
    ? "in.(pending,processing,failed)"
    : "in.(processed,ignored,superseded)";
}

export function canRetry(row: SourceRawInput): boolean {
  return row.processing_status === "failed" || row.processing_status === "ignored";
}

export function canIgnore(row: SourceRawInput): boolean {
  return row.processing_status === "pending" || row.processing_status === "failed";
}

export function sourceHasProcessor(source: DataSource): boolean {
  return source.input_table_name === "gleif_company_raw_inputs"
    || source.input_table_name === "companies_house_company_raw_inputs"
    || source.input_table_name === "brreg_company_raw_inputs";
}
```

- [ ] **Step 4: Update `PullRunsTable` for new fields**

In `ui/app/components/app/PullRunsTable.tsx`, replace `completed_at`, `records_fetched`, and `records_upserted` usage with `finished_at`, `rows_seen`, and `raw_rows_inserted + raw_rows_updated`.

The duration helper becomes:

```ts
function duration(run: PullRun): string {
  if (!run.finished_at) return "-";
  const secs = differenceInSeconds(new Date(run.finished_at), new Date(run.started_at));
  if (secs < 60) return `${secs}s`;
  return `${Math.floor(secs / 60)}m ${secs % 60}s`;
}
```

The row count cells become:

```tsx
<TableCell className="text-right">{run.rows_seen.toLocaleString()}</TableCell>
<TableCell className="text-right">{(run.raw_rows_inserted + run.raw_rows_updated).toLocaleString()}</TableCell>
```

- [ ] **Step 5: Verify UI types**

Run:

```bash
cd ui && npm run typecheck
```

Expected: it may still fail because route files still use old `DataSource` fields; note the errors and fix them in Task 5.

- [ ] **Step 6: Commit after Task 5 if typecheck cannot pass here**

If typecheck fails only because `sources.tsx` and `sources_.$name.tsx` still reference old source fields, do not commit yet. Continue to Task 5 and commit Tasks 4 and 5 together.

---

## Task 5: Source Detail Route Shell, Schedule Tab, Config Tab, Logs Tab

**Files:**
- Create: `ui/app/components/ui/tabs.tsx`
- Create: `ui/app/components/app/source-detail/SourceHeader.tsx`
- Create: `ui/app/components/app/source-detail/ScheduleTab.tsx`
- Create: `ui/app/components/app/source-detail/ConfigTab.tsx`
- Create: `ui/app/components/app/source-detail/LogsTab.tsx`
- Modify: `ui/app/routes/sources_.$name.tsx`
- Modify: `ui/app/routes/sources.tsx`
- Modify: `ui/app/components/app/SourcesTable.tsx`

- [ ] **Step 1: Add local Tabs component**

Create `ui/app/components/ui/tabs.tsx`:

```tsx
"use client";

import * as React from "react";
import { Tabs as TabsPrimitive } from "radix-ui";
import { cn } from "~/lib/utils";

function Tabs({ className, ...props }: React.ComponentProps<typeof TabsPrimitive.Root>) {
  return <TabsPrimitive.Root data-slot="tabs" className={cn("flex flex-col gap-4", className)} {...props} />;
}

function TabsList({ className, ...props }: React.ComponentProps<typeof TabsPrimitive.List>) {
  return (
    <TabsPrimitive.List
      data-slot="tabs-list"
      className={cn("inline-flex h-10 items-center justify-start gap-1 rounded-md bg-muted p-1 text-muted-foreground", className)}
      {...props}
    />
  );
}

function TabsTrigger({ className, ...props }: React.ComponentProps<typeof TabsPrimitive.Trigger>) {
  return (
    <TabsPrimitive.Trigger
      data-slot="tabs-trigger"
      className={cn(
        "inline-flex h-8 items-center justify-center whitespace-nowrap rounded-sm px-3 text-sm font-medium transition-colors",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50",
        "data-[state=active]:bg-background data-[state=active]:text-foreground data-[state=active]:shadow-sm",
        className,
      )}
      {...props}
    />
  );
}

function TabsContent({ className, ...props }: React.ComponentProps<typeof TabsPrimitive.Content>) {
  return <TabsPrimitive.Content data-slot="tabs-content" className={cn("outline-none", className)} {...props} />;
}

export { Tabs, TabsList, TabsTrigger, TabsContent };
```

- [ ] **Step 2: Add `SourceHeader`**

Create `ui/app/components/app/source-detail/SourceHeader.tsx`:

```tsx
import { Badge } from "~/components/ui/badge";
import type { DataSource } from "~/types/api";
import { sourceDisplayName } from "./sourceDetailUtils";

interface SourceHeaderProps {
  source: DataSource;
}

export function SourceHeader({ source }: SourceHeaderProps) {
  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-2">
        <h1 className="text-2xl font-semibold tracking-normal">{sourceDisplayName(source)}</h1>
        <Badge variant="outline">{source.source_group}</Badge>
        <Badge variant="outline">{source.pull_task_type}</Badge>
        {source.enabled
          ? <Badge className="bg-green-100 text-green-800 border-green-200" variant="outline">Enabled</Badge>
          : <Badge className="bg-gray-100 text-gray-700 border-gray-200" variant="outline">Disabled</Badge>}
      </div>
      {source.description && <p className="max-w-4xl text-sm text-muted-foreground">{source.description}</p>}
    </div>
  );
}
```

- [ ] **Step 3: Add `ScheduleTab`**

Create `ui/app/components/app/source-detail/ScheduleTab.tsx` with props:

```tsx
interface ScheduleTabProps {
  source: DataSource;
  saving: boolean;
  triggering: boolean;
  onPatch: (body: Parameters<typeof api.patchSource>[1]) => Promise<void>;
  onTrigger: () => Promise<void>;
}
```

Implement:
- enabled banner calls `onPatch({ enabled: !source.enabled })`
- schedule banner calls `onPatch({ schedule_enabled: !source.schedule_enabled })`
- duration input initialized from `source.schedule_expression ?? ""`
- client validation with `validateDuration`
- save calls `onPatch({ schedule_kind: "interval", schedule_expression: duration })`
- next run display uses `source.last_started_at` plus parsed duration hours/minutes
- manual trigger button calls `onTrigger`

Use existing `Card`, `Button`, `Input`, `Alert`, `Badge` components; do not nest cards.

- [ ] **Step 4: Add `ConfigTab`**

Create `ui/app/components/app/source-detail/ConfigTab.tsx`.

Use this state shape:

```ts
interface ConfigRow {
  id: string;
  key: string;
  value: string;
  error?: string;
}
```

Convert source config to rows:

```ts
function configToRows(config: Record<string, unknown>): ConfigRow[] {
  return Object.entries(config).map(([key, value]) => ({
    id: crypto.randomUUID(),
    key,
    value: JSON.stringify(value),
  }));
}
```

On save:
- reject empty keys
- reject keys matching `/key|secret|token|password/i`
- parse each value with `JSON.parse`
- build `Record<string, unknown>`
- call `onPatch({ config })`

- [ ] **Step 5: Add `LogsTab`**

Create `ui/app/components/app/source-detail/LogsTab.tsx` that owns pull-run pagination:

```tsx
export function LogsTab({ sourceName }: { sourceName: string }) {
  // page state, loading state, api.getPullRuns(page, 20, sourceName)
  // render PullRunsTable plus Previous / Next controls
}
```

Keep the existing "has more" convention: `items.length === pageSize`.

- [ ] **Step 6: Replace source detail route with tab shell**

Replace `ui/app/routes/sources_.$name.tsx` with a route that:
- loads `api.getSource(name)`
- renders back link, `SourceHeader`, and `Tabs`
- tabs: Schedule, Config, Logs, and Raw Inputs only when `hasRawInputs(source)`
- passes `onPatch` that updates local `source` optimistically after successful `api.patchSource`
- passes `onTrigger` that calls `api.triggerSource`

The route should not load jobs anymore; this dashboard is source-focused and the spec does not include the old jobs card.

- [ ] **Step 7: Update source list page for new fields**

In `ui/app/components/app/SourcesTable.tsx`:
- replace `source_type` with `source_group`
- replace `crawl_interval_hours` with `schedule_expression ?? "-"`
- replace `last_crawled_at` with `last_started_at`
- keep the existing enabled switch and trigger button

In `ui/app/routes/sources.tsx`, keep the existing `api.patchSource(name, { enabled })` call.

- [ ] **Step 8: Verify UI compiles**

Run:

```bash
cd ui && npm run typecheck
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add ui/app/components/ui/tabs.tsx ui/app/components/app/source-detail ui/app/routes/sources.tsx 'ui/app/routes/sources_.$name.tsx' ui/app/components/app/SourcesTable.tsx ui/app/components/app/PullRunsTable.tsx ui/app/types/api.ts ui/app/lib/api.ts
git commit -m "feat: build source detail dashboard shell"
```

---

## Task 6: Raw Inputs Tab And Sheet

**Files:**
- Create: `ui/app/components/app/source-detail/RawInputsTab.tsx`
- Create: `ui/app/components/app/source-detail/RawInputSheet.tsx`
- Modify: `ui/app/routes/sources_.$name.tsx`

- [ ] **Step 1: Create `RawInputSheet`**

Create `ui/app/components/app/source-detail/RawInputSheet.tsx` with props:

```ts
interface RawInputSheetProps {
  source: DataSource;
  row: SourceRawInput | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onChanged: () => void;
}
```

On open and non-null row:
- load payload with `pgrest<RawPayloadRow>(row.source_input_table, { id: "eq." + row.id, select: "raw_payload", limit: 1 })`
- load links with `pgrest<SuggestionSourceLink>("suggestion_source_links", { source_input_table: "eq." + row.source_input_table, source_input_key: "eq." + row.id, order: "created_at.desc" })`

Render:
- `SheetContent` with `className="w-full overflow-y-auto sm:max-w-2xl"`
- header with native id, table, status badge, attempts
- Retry and Ignore buttons using `api.retryRawInput` and `api.ignoreRawInput`
- metadata grid
- suggestion links list
- last error block
- raw payload `pre` with `JSON.stringify(payload, null, 2)`

After retry/ignore success:
- toast success
- call `onChanged()`
- close sheet

- [ ] **Step 2: Create `RawInputsTab`**

Create `ui/app/components/app/source-detail/RawInputsTab.tsx`.

State:

```ts
const [group, setGroup] = useState<"live" | "archive">("live");
const [page, setPage] = useState(1);
const [rows, setRows] = useState<SourceRawInput[]>([]);
const [total, setTotal] = useState(0);
const [loading, setLoading] = useState(false);
const [selected, setSelected] = useState<SourceRawInput | null>(null);
```

Fetch rows with:

```ts
const result = await pgrest<SourceRawInput>("v_source_raw_inputs", {
  source_name: `eq.${source.name}`,
  processing_status: liveStatusFilter(group),
  order: "last_seen_at.desc",
  limit: PAGE_SIZE,
  offset: (page - 1) * PAGE_SIZE,
});
```

Use TanStack `useReactTable` with columns:
- Status badge
- Native ID
- First Seen
- Attempts
- Error truncated
- Suggestion check mark or dash

Rows should be clickable and open `RawInputSheet`.

- [ ] **Step 3: Wire Raw Inputs into the route**

In `ui/app/routes/sources_.$name.tsx`, import `RawInputsTab` and render:

```tsx
{hasRawInputs(source) && (
  <TabsContent value="raw-inputs">
    <RawInputsTab source={source} />
  </TabsContent>
)}
```

- [ ] **Step 4: Verify UI**

Run:

```bash
cd ui && npm run typecheck
cd ui && npm run build
```

Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
git add ui/app/components/app/source-detail/RawInputsTab.tsx ui/app/components/app/source-detail/RawInputSheet.tsx 'ui/app/routes/sources_.$name.tsx'
git commit -m "feat: add source raw input queue UI"
```

---

## Task 7: End-To-End Verification

**Files:**
- Verify only unless a previous task exposed a bug.

- [ ] **Step 1: Run backend tests**

```bash
cd scheduler && GOWORK=off go test ./...
```

Expected: PASS.

- [ ] **Step 2: Run UI checks**

```bash
cd ui && npm run typecheck
cd ui && npm run build
```

Expected: PASS.

- [ ] **Step 3: Apply migrations in the local stack**

If Docker services are running, run:

```bash
make migrate-up
```

Expected: migration 020 applies successfully.

- [ ] **Step 4: Smoke test in browser**

Start or reuse the local app. Open:

```text
http://localhost:8083/sources/gleif
http://localhost:8083/sources/nvd_cpe
```

Verify:
- GLEIF shows Schedule, Config, Logs, and Raw Inputs tabs.
- NVD CPE does not show Raw Inputs.
- Pause Schedule sends `PATCH {"schedule_enabled": false}` and updates the banner.
- Config save preserves numeric JSON values.
- Raw Inputs opens a sheet and fetches payload on demand.

- [ ] **Step 5: Final commit if any verification fixes were needed**

```bash
git status --short
git add <changed-files>
git commit -m "fix: polish source detail dashboard"
```

Skip this commit if `git status --short` is clean.

---

## Self-Review Checklist

- Spec coverage:
  - schedule_enabled migration and scheduler gate: Task 1 and Task 2
  - PATCH enabled/schedule/config: Task 2
  - retry/ignore raw input API: Task 3
  - PostgREST raw input view, grants, and sheet payload reads: Task 1 and Task 6
  - four-tab UI with conditional Raw Inputs: Task 5 and Task 6
- Placeholder scan:
  - No placeholder markers are present.
  - Code snippets name concrete files and functions.
- Type consistency:
  - Backend field is `schedule_enabled` JSON and `ScheduleEnabled` Go.
  - UI source type uses `source_group`, `pull_task_type`, `input_table_name`, `schedule_expression`, and `last_started_at`.
  - Pull runs use `finished_at`, `rows_seen`, `raw_rows_inserted`, `raw_rows_updated`, and `raw_rows_unchanged`.
