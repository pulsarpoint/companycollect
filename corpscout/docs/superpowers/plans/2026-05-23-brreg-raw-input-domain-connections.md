# BRREG Raw Input Domain Connections Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add BRREG raw-input-to-domain evidence links, expose them in the raw input list/detail UI, allow manual add/remove, and submit active links into company-domain suggestions.

**Architecture:** Corpscout owns the canonical schema, sqlc queries, scheduler API, processor, and React UI. Data-pipelines receives raw input IDs from scheduler enhancement actions and writes BRREG discoveries into the new bridge table instead of the approved company-domain layer. The bridge table stays BRREG-specific and stores evidence only; approved `company_domains` remain unchanged until suggestion review.

**Tech Stack:** PostgreSQL migrations, sqlc, Go with pgx/chi/Temporal, React Router/TypeScript, pnpm, Temporal Go workflow tests, pgxmock, testify.

---

## Boundaries And Files

- Schema and sqlc live in `/Users/graovic/pulsarpoint/ppoint/corpscout/database`.
- Scheduler handlers, task starts, and processors live in `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal`.
- UI code lives in `/Users/graovic/pulsarpoint/ppoint/corpscout/ui/app`.
- Data-pipeline contracts, workflow, and activities live in `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker`.
- Leave `/Users/graovic/pulsarpoint/ppoint/data-pipelines/.github/workflows/go-worker-image.yml` unstaged; it is pre-existing dirty work.
- Use sqlc projection/command structs for new DB reads and writes. Keep the existing `raw_inputs.go` dynamic union as the only raw SQL list boundary because it already owns cross-source sorting and filtering.
- Use `log/slog` at HTTP/worker boundaries and wrap returned Go errors with `github.com/cockroachdb/errors` in new Corpscout code.

## Task 0: Baseline Guard

**Files:**
- Inspect: `/Users/graovic/pulsarpoint/ppoint/corpscout`
- Inspect: `/Users/graovic/pulsarpoint/ppoint/data-pipelines`

- [ ] **Step 1: Confirm branches and dirty files**

Run:

```bash
git -C /Users/graovic/pulsarpoint/ppoint/corpscout status --short --branch
git -C /Users/graovic/pulsarpoint/ppoint/data-pipelines status --short --branch
```

Expected: both repositories report branch `main`; data-pipelines also reports `M .github/workflows/go-worker-image.yml`.

- [ ] **Step 2: Keep unrelated dirty work out of commits**

Run:

```bash
git -C /Users/graovic/pulsarpoint/ppoint/data-pipelines diff -- .github/workflows/go-worker-image.yml
```

Expected: review only. Do not stage this file during any task.

## Task 1: Corpscout Schema And sqlc Bridge Queries

**Files:**
- Create: `/Users/graovic/pulsarpoint/ppoint/corpscout/database/migrations/000048_brreg_raw_input_domains.up.sql`
- Create: `/Users/graovic/pulsarpoint/ppoint/corpscout/database/migrations/000048_brreg_raw_input_domains.down.sql`
- Create: `/Users/graovic/pulsarpoint/ppoint/corpscout/database/queries/brreg_raw_input_domains.sql`
- Create: `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/db/brreg_raw_input_domains_migration_test.go`
- Modify generated: `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/db/gen`

- [ ] **Step 1: Write the failing migration contract test**

Create `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/db/brreg_raw_input_domains_migration_test.go`:

```go
package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregRawInputDomainsMigrationDefinesBridgeTable(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000048_brreg_raw_input_domains.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE brreg_raw_input_domains")
	require.Contains(t, sql, "raw_input_id UUID NOT NULL REFERENCES brreg_company_raw_inputs(id) ON DELETE CASCADE")
	require.Contains(t, sql, "domain_id UUID NOT NULL REFERENCES domains(id)")
	require.Contains(t, sql, "action_id UUID REFERENCES brreg_raw_input_actions(id) ON DELETE SET NULL")
	require.Contains(t, sql, "signal IN ('manual', 'wikidata', 'certsh', 'whois', 'search', 'heuristic')")
	require.Contains(t, sql, "status IN ('active', 'removed')")
	require.Contains(t, sql, "UNIQUE (raw_input_id, domain_id, signal)")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_raw_input_domains_raw_status")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_raw_input_domains_domain_status")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_raw_input_domains_action")
	require.Contains(t, sql, "GRANT SELECT ON brreg_raw_input_domains TO corpscout_anon")
}

func TestBrregRawInputDomainsMigrationAddsSourceRawInputCount(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000048_brreg_raw_input_domains.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "connected_domain_count")
	require.Contains(t, sql, "FROM brreg_raw_input_domains brid")
	require.Contains(t, sql, "brid.status = 'active'")
	require.Contains(t, sql, "0::bigint AS connected_domain_count")
	require.Contains(t, sql, "GRANT SELECT ON v_source_raw_inputs TO corpscout_anon")
}
```

- [ ] **Step 2: Run the failing migration test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/db -run 'TestBrregRawInputDomains' -count=1
```

Expected: FAIL because `000048_brreg_raw_input_domains.up.sql` does not exist.

- [ ] **Step 3: Add the up migration**

Create `/Users/graovic/pulsarpoint/ppoint/corpscout/database/migrations/000048_brreg_raw_input_domains.up.sql` with this table and index block at the top:

```sql
CREATE TABLE brreg_raw_input_domains (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_input_id UUID NOT NULL REFERENCES brreg_company_raw_inputs(id) ON DELETE CASCADE,
  domain_id UUID NOT NULL REFERENCES domains(id),
  action_id UUID REFERENCES brreg_raw_input_actions(id) ON DELETE SET NULL,
  signal TEXT NOT NULL,
  confidence SMALLINT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  removed_at TIMESTAMPTZ,
  removed_by TEXT,
  CONSTRAINT chk_brreg_raw_input_domains_signal CHECK (
    signal IN ('manual', 'wikidata', 'certsh', 'whois', 'search', 'heuristic')
  ),
  CONSTRAINT chk_brreg_raw_input_domains_confidence CHECK (confidence BETWEEN 1 AND 100),
  CONSTRAINT chk_brreg_raw_input_domains_status CHECK (status IN ('active', 'removed')),
  CONSTRAINT chk_brreg_raw_input_domains_metadata CHECK (jsonb_typeof(metadata) = 'object'),
  CONSTRAINT chk_brreg_raw_input_domains_removed_fields CHECK (
    (status = 'removed' AND removed_at IS NOT NULL) OR (status = 'active' AND removed_at IS NULL)
  ),
  UNIQUE (raw_input_id, domain_id, signal)
);

CREATE INDEX idx_brreg_raw_input_domains_raw_status
  ON brreg_raw_input_domains(raw_input_id, status);

CREATE INDEX idx_brreg_raw_input_domains_domain_status
  ON brreg_raw_input_domains(domain_id, status);

CREATE INDEX idx_brreg_raw_input_domains_action
  ON brreg_raw_input_domains(action_id)
  WHERE action_id IS NOT NULL;

GRANT SELECT ON brreg_raw_input_domains TO corpscout_anon;
```

In the same migration, recreate `v_source_raw_inputs` from the current `/Users/graovic/pulsarpoint/ppoint/corpscout/database/migrations/000047_brreg_raw_input_state.up.sql` definition and add one output column named `connected_domain_count`. In every non-BRREG branch add:

```sql
0::bigint AS connected_domain_count
```

In the BRREG branch add this select expression:

```sql
COALESCE(bdc.connected_domain_count, 0)::bigint AS connected_domain_count
```

and this join beside the existing `v_brreg_raw_input_action_attributes` join:

```sql
LEFT JOIN LATERAL (
  SELECT count(*)::bigint AS connected_domain_count
  FROM brreg_raw_input_domains brid
  WHERE brid.raw_input_id = bri.id
    AND brid.status = 'active'
) bdc ON true
```

End the migration with:

```sql
GRANT SELECT ON v_source_raw_inputs TO corpscout_anon;
```

- [ ] **Step 4: Add the down migration**

Create `/Users/graovic/pulsarpoint/ppoint/corpscout/database/migrations/000048_brreg_raw_input_domains.down.sql` with this top block:

```sql
DROP VIEW IF EXISTS v_source_raw_inputs;
DROP TABLE IF EXISTS brreg_raw_input_domains;
```

Append lines 214 through 464 from `/Users/graovic/pulsarpoint/ppoint/corpscout/database/migrations/000047_brreg_raw_input_state.up.sql`. Those lines start with:

```sql
CREATE OR REPLACE VIEW v_source_raw_inputs AS
```

and end with:

```sql
  FROM domain_discovery_raw_inputs ddri;
```

Keep this final grant:

```sql
GRANT SELECT ON v_source_raw_inputs TO corpscout_anon;
```

Verify the down migration does not reference the new bridge:

```bash
rg -n "connected_domain_count|FROM brreg_raw_input_domains|JOIN brreg_raw_input_domains" /Users/graovic/pulsarpoint/ppoint/corpscout/database/migrations/000048_brreg_raw_input_domains.down.sql
```

Expected: no output.

- [ ] **Step 5: Add sqlc queries for the bridge**

Create `/Users/graovic/pulsarpoint/ppoint/corpscout/database/queries/brreg_raw_input_domains.sql`:

```sql
-- name: ListBrregRawInputDomains :many
SELECT
  brid.id,
  brid.raw_input_id,
  brid.domain_id,
  d.domain,
  brid.action_id,
  brid.signal,
  brid.confidence,
  brid.status,
  brid.metadata,
  brid.created_at,
  brid.updated_at,
  brid.removed_at,
  brid.removed_by
FROM brreg_raw_input_domains brid
JOIN domains d ON d.id = brid.domain_id
WHERE brid.raw_input_id = $1
  AND brid.status = 'active'
ORDER BY brid.confidence DESC, d.domain, brid.created_at DESC;

-- name: CountActiveBrregRawInputDomains :one
SELECT count(*)::bigint
FROM brreg_raw_input_domains
WHERE raw_input_id = $1
  AND status = 'active';

-- name: UpsertManualBrregRawInputDomain :one
WITH domain_row AS (
  INSERT INTO domains (domain, import_source)
  VALUES (sqlc.arg('domain')::text, 'manual_upload')
  ON CONFLICT (domain) DO UPDATE
    SET last_verified_at = now()
  RETURNING id
)
INSERT INTO brreg_raw_input_domains (
  raw_input_id,
  domain_id,
  signal,
  confidence,
  status,
  metadata,
  removed_at,
  removed_by
)
SELECT
  sqlc.arg('raw_input_id')::uuid,
  domain_row.id,
  'manual',
  100,
  'active',
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  NULL,
  NULL
FROM domain_row
ON CONFLICT (raw_input_id, domain_id, signal) DO UPDATE SET
  confidence = 100,
  status = 'active',
  metadata = brreg_raw_input_domains.metadata || EXCLUDED.metadata,
  removed_at = NULL,
  removed_by = NULL,
  updated_at = now()
RETURNING *;

-- name: RemoveBrregRawInputDomain :one
UPDATE brreg_raw_input_domains
SET status = 'removed',
    removed_at = now(),
    removed_by = sqlc.arg('removed_by')::text,
    updated_at = now()
WHERE id = sqlc.arg('id')::uuid
  AND raw_input_id = sqlc.arg('raw_input_id')::uuid
  AND status = 'active'
RETURNING *;

-- name: ListActiveBrregRawInputDomainsForSuggestion :many
SELECT
  brid.id,
  brid.raw_input_id,
  brid.domain_id,
  d.domain,
  brid.action_id,
  brid.signal,
  brid.confidence,
  brid.metadata,
  brid.created_at
FROM brreg_raw_input_domains brid
JOIN domains d ON d.id = brid.domain_id
WHERE brid.raw_input_id = $1
  AND brid.status = 'active'
ORDER BY brid.confidence DESC, d.domain;
```

- [ ] **Step 6: Generate sqlc output**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off sqlc generate -f ../database/sqlc.yaml
```

Expected: generated files under `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/db/gen` include `brreg_raw_input_domains.sql.go`, and `querier.go` includes the new methods.

- [ ] **Step 7: Run schema/sqlc tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/db -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit schema work**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add database/migrations/000048_brreg_raw_input_domains.up.sql \
  database/migrations/000048_brreg_raw_input_domains.down.sql \
  database/queries/brreg_raw_input_domains.sql \
  scheduler/internal/db/brreg_raw_input_domains_migration_test.go \
  scheduler/internal/db/gen
git commit -m "feat: add brreg raw input domain bridge"
```

## Task 2: Scheduler Raw Input API Domain Count And Manual Controls

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/httpapi/raw_inputs.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/httpapi/handlers.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/httpapi/raw_inputs_test.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/httpapi/testhelpers_test.go`

- [ ] **Step 1: Add failing API tests**

Add tests in `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/httpapi/raw_inputs_test.go`:

```go
func TestListRawInputsIncludesConnectedDomainCount(t *testing.T) {
	pool := newMockPool(t)
	h := NewHandlers(nil, nil, pool, nil, nil, "", nil, "")
	r := chi.NewRouter()
	h.RegisterRoutes(r)

	rawID := uuid.New().String()
	createdAt := time.Now().UTC()

	pool.ExpectQuery("COUNT(*) FROM;;brreg_company_raw_inputs;;brreg_raw_input_domains brid;;brid.status = 'active'").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(1)))
	pool.ExpectQuery("SELECT id, source, name, native_id, status, translation_status, has_suggestion, state, connected_domain_count").
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "source", "name", "native_id", "status", "translation_status", "has_suggestion", "state", "connected_domain_count",
			"latest_translation_action_status", "has_successful_translation", "latest_enhancement_action_status", "has_successful_enhancement",
			"latest_submission_action_status", "has_successful_submission", "created_at",
		}).AddRow(rawID, "brreg", "BORTIGARD AS", "810202572", "pending", "translated", false, "input", int64(2),
			nil, nil, nil, nil, nil, nil, createdAt))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs?source=brreg", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var body struct {
		Items []rawInputRow `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Items, 1)
	require.Equal(t, int64(2), body.Items[0].ConnectedDomainCount)
}

func TestGetBrregRawInputReturnsConnectedDomains(t *testing.T) {
	pool := newMockPool(t)
	h := NewHandlers(nil, nil, pool, nil, nil, "", nil, "")
	r := chi.NewRouter()
	h.RegisterRoutes(r)

	rawID := uuid.New().String()
	connectionID := uuid.New()
	domainID := uuid.New()
	now := time.Now().UTC()

	pool.ExpectQuery("FROM brreg_company_raw_inputs bri;;v_brreg_raw_input_action_attributes").
		WithArgs(rawID).
		WillReturnRows(brregRawInputDetailRows(rawID, now))
	pool.ExpectQuery("FROM brreg_raw_input_domains brid;;JOIN domains d").
		WithArgs(uuid.MustParse(rawID)).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "raw_input_id", "domain_id", "domain", "action_id", "signal", "confidence", "status", "metadata",
			"created_at", "updated_at", "removed_at", "removed_by",
		}).AddRow(connectionID, uuid.MustParse(rawID), domainID, "bortigard.no", nil, "manual", int16(100), "active", []byte(`{"created_by":"ops"}`), now, now, nil, nil))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs/brreg/"+rawID, nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var body rawInputDetail
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.ConnectedDomains, 1)
	require.Equal(t, "bortigard.no", body.ConnectedDomains[0].Domain)
	require.Equal(t, "manual", body.ConnectedDomains[0].Signal)
}

func TestAddAndRemoveBrregRawInputDomainRoutes(t *testing.T) {
	rawID := uuid.New()
	connectionID := uuid.New()
	domainID := uuid.New()
	now := time.Now().UTC()
	q := &stubQuerier{
		upsertManualBrregRawInputDomain: func(ctx context.Context, arg db.UpsertManualBrregRawInputDomainParams) (db.BrregRawInputDomain, error) {
			require.Equal(t, rawID, arg.RawInputID)
			require.Equal(t, "bortigard.no", arg.Domain)
			return db.BrregRawInputDomain{ID: connectionID, RawInputID: rawID, DomainID: domainID, Signal: "manual", Confidence: 100, Status: "active", Metadata: []byte(`{"created_by":"ops"}`), CreatedAt: now, UpdatedAt: now}, nil
		},
		removeBrregRawInputDomain: func(ctx context.Context, arg db.RemoveBrregRawInputDomainParams) (db.BrregRawInputDomain, error) {
			require.Equal(t, connectionID, arg.ID)
			require.Equal(t, rawID, arg.RawInputID)
			require.Equal(t, "ops", arg.RemovedBy)
			return db.BrregRawInputDomain{ID: connectionID, RawInputID: rawID, DomainID: domainID, Signal: "manual", Confidence: 100, Status: "removed", Metadata: []byte(`{}`), CreatedAt: now, UpdatedAt: now}, nil
		},
	}
	h := NewHandlers(q, nil, nil, nil, nil, "", nil, "")
	r := chi.NewRouter()
	h.RegisterRoutes(r)

	addReq := httptest.NewRequest(http.MethodPost, "/api/v1/raw-inputs/brreg/"+rawID.String()+"/domains", strings.NewReader(`{"domain":"https://bortigard.no/path","note":"verified website"}`))
	addReq.Header.Set("Content-Type", "application/json")
	addRec := httptest.NewRecorder()
	r.ServeHTTP(addRec, addReq)
	require.Equal(t, http.StatusOK, addRec.Code)

	removeReq := httptest.NewRequest(http.MethodPost, "/api/v1/raw-inputs/brreg/"+rawID.String()+"/domains/"+connectionID.String()+"/remove", strings.NewReader(`{}`))
	removeReq.Header.Set("Content-Type", "application/json")
	removeRec := httptest.NewRecorder()
	r.ServeHTTP(removeRec, removeReq)
	require.Equal(t, http.StatusOK, removeRec.Code)
}
```

If helper row constructors are not present, add a `brregRawInputDetailRows` helper in the same test file using the exact columns selected by the BRREG detail query.

- [ ] **Step 2: Run the failing API tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run 'Test(ListRawInputsIncludesConnectedDomainCount|GetBrregRawInputReturnsConnectedDomains|AddAndRemoveBrregRawInputDomainRoutes)' -count=1
```

Expected: FAIL because response fields and routes do not exist.

- [ ] **Step 3: Add response types and list count SQL**

Patch `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/httpapi/raw_inputs.go`.

Add this field to `rawInputRow` after `State`:

```go
ConnectedDomainCount int64 `json:"connected_domain_count"`
```

Add this type near `rawInputDetail`:

```go
type rawInputConnectedDomain struct {
	ID         string          `json:"id"`
	DomainID   string          `json:"domain_id"`
	Domain     string          `json:"domain"`
	ActionID   string          `json:"action_id,omitempty"`
	Signal     string          `json:"signal"`
	Confidence int16           `json:"confidence"`
	Status     string          `json:"status"`
	Metadata   json.RawMessage `json:"metadata"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
	RemovedAt  *time.Time      `json:"removed_at,omitempty"`
	RemovedBy  string          `json:"removed_by,omitempty"`
}
```

Add this field to `rawInputDetail` after `HasSuccessfulSubmission`:

```go
ConnectedDomains []rawInputConnectedDomain `json:"connected_domains,omitempty"`
```

In the raw input list query builder, use this BRREG count join:

```go
domainCountSelect := "0::bigint AS connected_domain_count"
if src.source == "brreg" {
	joinClause += `
LEFT JOIN LATERAL (
	SELECT count(*)::bigint AS connected_domain_count
	FROM brreg_raw_input_domains brid
	WHERE brid.raw_input_id = ri.id
	  AND brid.status = 'active'
) bdc ON true`
	domainCountSelect = "COALESCE(bdc.connected_domain_count, 0)::bigint AS connected_domain_count"
}
```

Add `domainCountSelect` between `state` and `state_rank` in the union select. Update the outer data query and scan order so `connected_domain_count` is read into `row.ConnectedDomainCount`.

- [ ] **Step 4: Add detail domain loader**

Add this helper to `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/httpapi/raw_inputs.go`:

```go
func (h *Handlers) listBrregRawInputConnectedDomains(ctx context.Context, rawInputID uuid.UUID) ([]rawInputConnectedDomain, error) {
	q := h.db
	if q == nil && h.pool != nil {
		q = db.New(h.pool)
	}
	if q == nil {
		return nil, errors.New("database query interface not available")
	}
	rows, err := q.ListBrregRawInputDomains(ctx, rawInputID)
	if err != nil {
		return nil, errors.Wrap(err, "list brreg raw input domains")
	}
	out := make([]rawInputConnectedDomain, 0, len(rows))
	for _, row := range rows {
		item := rawInputConnectedDomain{
			ID:         row.ID.String(),
			DomainID:   row.DomainID.String(),
			Domain:     row.Domain,
			Signal:     row.Signal,
			Confidence: row.Confidence,
			Status:     row.Status,
			Metadata:   json.RawMessage(row.Metadata),
			CreatedAt:  row.CreatedAt,
			UpdatedAt:  row.UpdatedAt,
		}
		if row.ActionID.Valid {
			item.ActionID = row.ActionID.UUID.String()
		}
		if row.RemovedAt.Valid {
			item.RemovedAt = &row.RemovedAt.Time
		}
		if row.RemovedBy != nil {
			item.RemovedBy = *row.RemovedBy
		}
		out = append(out, item)
	}
	return out, nil
}
```

After the BRREG detail scan succeeds in `handleGetRawInput`, parse the UUID and attach connected domains:

```go
rawUUID, parseErr := uuid.Parse(idStr)
if parseErr != nil {
	writeError(w, http.StatusBadRequest, "invalid raw input id")
	return
}
domains, domainErr := h.listBrregRawInputConnectedDomains(r.Context(), rawUUID)
if domainErr != nil {
	slog.Error("list brreg raw input connected domains", "id", idStr, "error", domainErr)
	writeError(w, http.StatusInternalServerError, "internal error")
	return
}
row.ConnectedDomains = domains
```

- [ ] **Step 5: Add manual add/remove handlers**

Add route registrations in `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/httpapi/handlers.go` below the raw input detail route:

```go
r.Post("/raw-inputs/brreg/{id}/domains", h.handleAddBrregRawInputDomain)
r.Post("/raw-inputs/brreg/{id}/domains/{connection_id}/remove", h.handleRemoveBrregRawInputDomain)
```

Add these request and helper declarations in `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/httpapi/raw_inputs.go`:

```go
// Add "regexp" to the existing import block.

type addBrregRawInputDomainRequest struct {
	Domain string `json:"domain"`
	Note   string `json:"note,omitempty"`
}

type removeBrregRawInputDomainRequest struct {
	Reason string `json:"reason,omitempty"`
}

var domainLabelRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

func normalizeDomainInput(raw string) (string, error) {
	s := strings.TrimSpace(strings.ToLower(raw))
	s = strings.TrimPrefix(s, "http://")
	s = strings.TrimPrefix(s, "https://")
	if slash := strings.IndexByte(s, '/'); slash >= 0 {
		s = s[:slash]
	}
	if colon := strings.IndexByte(s, ':'); colon >= 0 {
		s = s[:colon]
	}
	s = strings.Trim(s, ".")
	if len(s) < 4 || len(s) > 253 || !strings.Contains(s, ".") {
		return "", errors.New("invalid domain")
	}
	for _, label := range strings.Split(s, ".") {
		if !domainLabelRE.MatchString(label) {
			return "", errors.New("invalid domain")
		}
	}
	return s, nil
}
```

Add handler methods:

```go
func (h *Handlers) handleAddBrregRawInputDomain(w http.ResponseWriter, r *http.Request) {
	rawID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid raw input id")
		return
	}
	var req addBrregRawInputDomainRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	domain, err := normalizeDomainInput(req.Domain)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "invalid domain")
		return
	}
	metadata, err := json.Marshal(map[string]any{
		"created_by": "ops",
		"source":     "manual",
		"note":       strings.TrimSpace(req.Note),
	})
	if err != nil {
		slog.Error("marshal manual brreg domain metadata", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	row, err := h.db.UpsertManualBrregRawInputDomain(r.Context(), db.UpsertManualBrregRawInputDomainParams{
		RawInputID: rawID,
		Domain:     domain,
		Metadata:   metadata,
	})
	if err != nil {
		slog.Error("add brreg raw input domain", "raw_input_id", rawID, "domain", domain, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, row)
}

func (h *Handlers) handleRemoveBrregRawInputDomain(w http.ResponseWriter, r *http.Request) {
	rawID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid raw input id")
		return
	}
	connectionID, err := uuid.Parse(chi.URLParam(r, "connection_id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid connection id")
		return
	}
	var req removeBrregRawInputDomainRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	row, err := h.db.RemoveBrregRawInputDomain(r.Context(), db.RemoveBrregRawInputDomainParams{
		ID:         connectionID,
		RawInputID: rawID,
		RemovedBy:  "ops",
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "domain connection not found")
			return
		}
		slog.Error("remove brreg raw input domain", "raw_input_id", rawID, "connection_id", connectionID, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, row)
}
```

- [ ] **Step 6: Extend test stubs**

Patch `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/httpapi/testhelpers_test.go` so `stubQuerier` has function fields and methods for:

```go
listBrregRawInputDomains       func(context.Context, uuid.UUID) ([]db.ListBrregRawInputDomainsRow, error)
upsertManualBrregRawInputDomain func(context.Context, db.UpsertManualBrregRawInputDomainParams) (db.BrregRawInputDomain, error)
removeBrregRawInputDomain       func(context.Context, db.RemoveBrregRawInputDomainParams) (db.BrregRawInputDomain, error)
```

Methods must delegate to the function fields when set and return zero values when unset.

- [ ] **Step 7: Run API tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run 'RawInput|Brreg' -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit API work**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/httpapi/raw_inputs.go \
  scheduler/internal/httpapi/handlers.go \
  scheduler/internal/httpapi/raw_inputs_test.go \
  scheduler/internal/httpapi/testhelpers_test.go
git commit -m "feat: expose brreg raw input domain links"
```

## Task 3: Scheduler Enhancement Start Passes Raw Input IDs

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/tasksvc/types.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/tasksvc/starter.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/tasksvc/starter_test.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/httpapi/sources.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/httpapi/sources_test.go`

- [ ] **Step 1: Add failing scheduler tests**

Update the BRREG enhancement route test in `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/httpapi/sources_test.go` so the mocked query rows include `raw_input_id`:

```go
rawID := uuid.New().String()
pool.ExpectQuery("SELECT;;n.id::text AS raw_input_id;;n.organization_number;;COALESCE(n.organization_name, '');;c.action_id::text").
	WillReturnRows(pgxmock.NewRows([]string{"raw_input_id", "organization_number", "organization_name", "action_id"}).
		AddRow(rawID, "810202572", "BORTIGARD AS", "action-1"))
```

Update `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/tasksvc/starter_test.go` to assert the Temporal payload contains:

```json
{"native_id":"810202572","name":"BORTIGARD AS","raw_input_id":"7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"}
```

- [ ] **Step 2: Run the failing scheduler tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/httpapi ./internal/tasksvc -run 'BrregDomainEnhancement|EnhanceBrregDomains' -count=1
```

Expected: FAIL because `CompanyLookup` lacks `RawInputID` and the route does not select it.

- [ ] **Step 3: Add raw input ID to scheduler contract**

Patch `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/tasksvc/types.go`:

```go
type CompanyLookup struct {
	NativeID   string `json:"native_id"`
	Name       string `json:"name"`
	RawInputID string `json:"raw_input_id,omitempty"`
}
```

- [ ] **Step 4: Select raw input IDs in enhancement queueing**

Patch the final select in `handleEnhanceBrregDomains` in `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/httpapi/sources.go`:

```sql
SELECT
    n.id::text AS raw_input_id,
    n.organization_number,
    COALESCE(n.organization_name, ''),
    c.action_id::text
FROM next_attempt n
JOIN created_actions c ON c.raw_input_id = n.id
ORDER BY n.organization_number
```

Patch row scanning:

```go
var company tasksvc.CompanyLookup
var actionID string
if err := rows.Scan(&company.RawInputID, &company.NativeID, &company.Name, &actionID); err != nil {
	slog.Error("scan brreg enhancement action", "error", err)
	writeError(w, http.StatusInternalServerError, "internal error")
	return
}
companies = append(companies, company)
actionIDs[company.NativeID] = actionID
```

- [ ] **Step 5: Run scheduler tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/httpapi ./internal/tasksvc -run 'BrregDomainEnhancement|EnhanceBrregDomains' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit scheduler enhancement payload work**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/tasksvc/types.go \
  scheduler/internal/tasksvc/starter.go \
  scheduler/internal/tasksvc/starter_test.go \
  scheduler/internal/httpapi/sources.go \
  scheduler/internal/httpapi/sources_test.go
git commit -m "feat: pass brreg raw input ids to enhancement workflows"
```

## Task 4: Data-Pipelines Writes BRREG Discoveries To Bridge Table

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/contracts/contracts.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/workflows/enrich_company_domains.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/workflows/enrich_company_domains_test.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/activities/activities.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/activities/activities_test.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/go.mod`
- Modify generated: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/go.sum`

- [ ] **Step 1: Add failing activity tests**

Add tests in `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/activities/activities_test.go`:

```go
func TestFilterForDomainDiscoveryBrregUsesRawInputDomainBridge(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	rawID1 := "7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"
	rawID2 := "4d80f241-0e9e-48b2-b4b4-8f919a5ff34d"
	mock.ExpectQuery(`SELECT DISTINCT raw_input_id::text FROM brreg_raw_input_domains`).
		WithArgs([]string{rawID1, rawID2}).
		WillReturnRows(pgxmock.NewRows([]string{"raw_input_id"}).AddRow(rawID1))

	result, err := acts.FilterForDomainDiscovery(context.Background(), contracts.FilterForDomainDiscoveryParams{
		Source:    "brreg",
		NativeIDs: []string{"810202572", "999999999"},
		Companies: []contracts.CompanyLookup{
			{NativeID: "810202572", Name: "BORTIGARD AS", RawInputID: rawID1},
			{NativeID: "999999999", Name: "NEW AS", RawInputID: rawID2},
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"999999999"}, result.NeedDiscovery)
}

func TestWriteDiscoveredDomainsBrregWritesRawInputBridge(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	rawID := "7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"
	actionID := "25dbfdd1-6971-4498-8061-d296d1651986"
	domainID := "030c7e19-f08b-487c-a8c1-41cb969a0b59"

	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO domains`).
		WithArgs("bortigard.no").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(domainID))
	mock.ExpectExec(`INSERT INTO brreg_raw_input_domains`).
		WithArgs(rawID, domainID, actionID, "search", int16(80), pgxmock.AnyArg(), false).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	err := acts.WriteDiscoveredDomains(context.Background(), contracts.WriteDiscoveredDomainsParams{
		Source: "brreg",
		Companies: []contracts.CompanyLookup{{
			NativeID:   "810202572",
			Name:       "BORTIGARD AS",
			RawInputID: rawID,
		}},
		ActionIDs: map[string]string{"810202572": actionID},
		Discoveries: []contracts.DomainDiscovery{{
			NativeID:   "810202572",
			Domain:     "https://bortigard.no/",
			Signal:     "duckduckgo",
			Confidence: 80,
		}},
	})
	require.NoError(t, err)
}

func TestWriteDiscoveredDomainsBrregForceReactivatesRemovedConnection(t *testing.T) {
	mock := newMock(t)
	acts := activities.NewGoActivitiesWithDB(mock)

	rawID := "7ffd5bf3-f96e-4907-9ef3-096eb4056ab8"
	actionID := "25dbfdd1-6971-4498-8061-d296d1651986"
	domainID := "030c7e19-f08b-487c-a8c1-41cb969a0b59"

	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO domains`).
		WithArgs("bortigard.no").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(domainID))
	mock.ExpectExec(`INSERT INTO brreg_raw_input_domains`).
		WithArgs(rawID, domainID, actionID, "heuristic", int16(70), pgxmock.AnyArg(), true).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	err := acts.WriteDiscoveredDomains(context.Background(), contracts.WriteDiscoveredDomainsParams{
		Source: "brreg",
		Companies: []contracts.CompanyLookup{{
			NativeID:   "810202572",
			Name:       "BORTIGARD AS",
			RawInputID: rawID,
		}},
		ActionIDs: map[string]string{"810202572": actionID},
		Force:     true,
		Discoveries: []contracts.DomainDiscovery{{
			NativeID:   "810202572",
			Domain:     "bortigard.no",
			Signal:     "heuristic",
			Confidence: 70,
		}},
	})
	require.NoError(t, err)
}
```

- [ ] **Step 2: Update workflow test expectation**

Patch `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/workflows/enrich_company_domains_test.go` so `CompanyLookup` includes `RawInputID`, `FilterForDomainDiscoveryParams` includes `Companies`, and `WriteDiscoveredDomainsParams` includes `Companies`, `ActionIDs`, and `Force`:

```go
company := contracts.CompanyLookup{
	NativeID:   "810202572",
	Name:       "BORTIGARD AS",
	RawInputID: "7ffd5bf3-f96e-4907-9ef3-096eb4056ab8",
}
```

Expected write params:

```go
contracts.WriteDiscoveredDomainsParams{
	Source:      "brreg",
	Companies:   []contracts.CompanyLookup{company},
	ActionIDs:   map[string]string{"810202572": "action-1"},
	Force:       true,
	Discoveries: []contracts.DomainDiscovery{{NativeID: "810202572", Domain: "bortigard.no", Signal: "heuristic", Confidence: 80}},
}
```

- [ ] **Step 3: Run failing data-pipeline tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go test ./activities ./workflows -run 'DomainDiscovery|DiscoveredDomains|EnrichCompanyDomainsMarksRawInputActionEvents' -count=1
```

Expected: FAIL because contract fields and BRREG bridge writes do not exist.

- [ ] **Step 4: Extend data-pipeline contracts**

Patch `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/contracts/contracts.go`:

```go
type CompanyLookup struct {
	NativeID   string `json:"native_id"`
	Name       string `json:"name"`
	RawInputID string `json:"raw_input_id,omitempty"`
}
```

Patch `FilterForDomainDiscoveryParams`:

```go
type FilterForDomainDiscoveryParams struct {
	Source    string          `json:"source"`
	NativeIDs []string        `json:"native_ids"`
	Companies []CompanyLookup `json:"companies,omitempty"`
	Force     bool            `json:"force"`
}
```

Patch `WriteDiscoveredDomainsParams`:

```go
type WriteDiscoveredDomainsParams struct {
	Source      string            `json:"source"`
	Companies   []CompanyLookup   `json:"companies,omitempty"`
	ActionIDs   map[string]string `json:"action_ids,omitempty"`
	Force       bool              `json:"force,omitempty"`
	Discoveries []DomainDiscovery `json:"discoveries"`
}
```

- [ ] **Step 5: Pass companies/action IDs through the workflow**

Patch `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/workflows/enrich_company_domains.go`.

When calling `FilterForDomainDiscovery`, pass `Companies`:

```go
if err := workflow.ExecuteActivity(goCtx, goAct.FilterForDomainDiscovery, contracts.FilterForDomainDiscoveryParams{
	Source:    input.Source,
	NativeIDs: nativeIDs,
	Companies: input.Companies,
	Force:     input.Force,
}).Get(ctx, &filterResult); err != nil {
	return contracts.EnrichCompanyDomainsResult{}, err
}
```

When calling `WriteDiscoveredDomains`, pass the batch context:

```go
if err := workflow.ExecuteActivity(goCtx, goAct.WriteDiscoveredDomains, contracts.WriteDiscoveredDomainsParams{
	Source:      input.Source,
	Companies:   batchCompanies,
	ActionIDs:   batchActionIDs,
	Force:       input.Force,
	Discoveries: discoverResult.Discoveries,
}).Get(ctx, nil); err != nil {
	return contracts.EnrichCompanyDomainsResult{}, err
}
```

- [ ] **Step 6: Add BRREG bridge write helpers**

Patch `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/activities/activities.go`.

Add imports:

```go
import (
	"net"
	"strings"

	"github.com/cockroachdb/errors"
	pgx "github.com/jackc/pgx/v5"
)
```

Add helper functions near the domain enrichment section:

```go
func companiesByNativeID(companies []contracts.CompanyLookup) map[string]contracts.CompanyLookup {
	out := make(map[string]contracts.CompanyLookup, len(companies))
	for _, company := range companies {
		out[company.NativeID] = company
	}
	return out
}

func normalizeDiscoveryDomain(raw string) (string, bool) {
	s := strings.TrimSpace(strings.ToLower(raw))
	s = strings.TrimPrefix(s, "http://")
	s = strings.TrimPrefix(s, "https://")
	if slash := strings.IndexByte(s, '/'); slash >= 0 {
		s = s[:slash]
	}
	if host, _, err := net.SplitHostPort(s); err == nil {
		s = host
	}
	s = strings.Trim(s, ".")
	if s == "" || !strings.Contains(s, ".") {
		return "", false
	}
	return s, true
}

func normalizeDiscoverySignal(signal string) string {
	switch strings.ToLower(strings.TrimSpace(signal)) {
	case "manual":
		return "manual"
	case "wikidata":
		return "wikidata"
	case "certsh", "crtsh":
		return "certsh"
	case "whois":
		return "whois"
	case "duckduckgo", "search":
		return "search"
	case "heuristic":
		return "heuristic"
	default:
		return "heuristic"
	}
}

func clampConfidence(confidence int) int16 {
	if confidence < 1 {
		return 1
	}
	if confidence > 100 {
		return 100
	}
	return int16(confidence)
}
```

- [ ] **Step 7: Make filter use the BRREG bridge for BRREG**

Patch `FilterForDomainDiscovery`:

```go
if params.Source == "brreg" {
	if params.Force || len(params.NativeIDs) == 0 {
		return contracts.FilterForDomainDiscoveryResult{NeedDiscovery: params.NativeIDs}, nil
	}
	companyMap := companiesByNativeID(params.Companies)
	rawIDs := make([]string, 0, len(params.NativeIDs))
	nativeByRawID := make(map[string]string, len(params.NativeIDs))
	for _, nativeID := range params.NativeIDs {
		company := companyMap[nativeID]
		if company.RawInputID == "" {
			return contracts.FilterForDomainDiscoveryResult{}, errors.Newf("missing brreg raw input id for native id %s", nativeID)
		}
		rawIDs = append(rawIDs, company.RawInputID)
		nativeByRawID[company.RawInputID] = nativeID
	}
	rows, err := a.pool.Query(ctx,
		`SELECT DISTINCT raw_input_id::text
		 FROM brreg_raw_input_domains
		 WHERE raw_input_id::text = ANY($1)
		   AND status = 'active'`,
		rawIDs,
	)
	if err != nil {
		return contracts.FilterForDomainDiscoveryResult{}, errors.Wrap(err, "query brreg raw input domain bridge")
	}
	defer rows.Close()

	already := map[string]struct{}{}
	for rows.Next() {
		var rawID string
		if err := rows.Scan(&rawID); err != nil {
			return contracts.FilterForDomainDiscoveryResult{}, errors.Wrap(err, "scan brreg raw input domain bridge")
		}
		if nativeID := nativeByRawID[rawID]; nativeID != "" {
			already[nativeID] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return contracts.FilterForDomainDiscoveryResult{}, errors.Wrap(err, "iterate brreg raw input domain bridge")
	}
	need := make([]string, 0, len(params.NativeIDs))
	for _, nativeID := range params.NativeIDs {
		if _, ok := already[nativeID]; !ok {
			need = append(need, nativeID)
		}
	}
	return contracts.FilterForDomainDiscoveryResult{NeedDiscovery: need}, nil
}
```

Leave the current `company_domains` path for non-BRREG sources.

- [ ] **Step 8: Write BRREG discoveries into `domains` and bridge rows**

Patch `WriteDiscoveredDomains` so BRREG calls a new helper:

```go
func (a *GoActivities) WriteDiscoveredDomains(ctx context.Context, params contracts.WriteDiscoveredDomainsParams) error {
	if params.Source == "brreg" {
		return a.writeBrregRawInputDomains(ctx, params)
	}
	for _, d := range params.Discoveries {
		if d.NativeID == "" || d.Domain == "" {
			continue
		}
		_, err := a.pool.Exec(ctx, `
			INSERT INTO company_domains (native_id, source, domain, signal, confidence)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (native_id, source, domain) DO UPDATE
				SET signal       = EXCLUDED.signal,
				    confidence   = EXCLUDED.confidence,
				    last_seen_at = now()
		`, d.NativeID, params.Source, d.Domain, d.Signal, d.Confidence)
		if err != nil {
			return errors.Wrapf(err, "upsert company_domain %s/%s", d.NativeID, d.Domain)
		}
	}
	return nil
}
```

Add the helper:

```go
func (a *GoActivities) writeBrregRawInputDomains(ctx context.Context, params contracts.WriteDiscoveredDomainsParams) error {
	companyMap := companiesByNativeID(params.Companies)
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "begin brreg raw input domain write")
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	for _, discovery := range params.Discoveries {
		company := companyMap[discovery.NativeID]
		if company.RawInputID == "" {
			return errors.Newf("missing brreg raw input id for native id %s", discovery.NativeID)
		}
		domain, ok := normalizeDiscoveryDomain(discovery.Domain)
		if !ok {
			continue
		}
		signal := normalizeDiscoverySignal(discovery.Signal)
		confidence := clampConfidence(discovery.Confidence)

		var domainID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO domains (domain)
			VALUES ($1)
			ON CONFLICT (domain) DO UPDATE SET last_verified_at = now()
			RETURNING id::text
		`, domain).Scan(&domainID); err != nil {
			return errors.Wrapf(err, "upsert domain %s", domain)
		}

		actionID := params.ActionIDs[discovery.NativeID]
		metadata, err := json.Marshal(map[string]any{
			"source":        params.Source,
			"native_id":     discovery.NativeID,
			"raw_input_id":  company.RawInputID,
			"action_id":     actionID,
			"raw_signal":    discovery.Signal,
			"signal":        signal,
			"reactivated":   params.Force,
			"reactivated_by": "ops",
		})
		if err != nil {
			return errors.Wrap(err, "marshal brreg raw input domain metadata")
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO brreg_raw_input_domains (
				raw_input_id, domain_id, action_id, signal, confidence, status, metadata, removed_at, removed_by
			)
			VALUES ($1::uuid, $2::uuid, NULLIF($3, '')::uuid, $4, $5, 'active', $6::jsonb, NULL, NULL)
			ON CONFLICT (raw_input_id, domain_id, signal) DO UPDATE SET
				confidence = EXCLUDED.confidence,
				action_id = EXCLUDED.action_id,
				metadata = brreg_raw_input_domains.metadata || EXCLUDED.metadata ||
					CASE WHEN $7::boolean THEN jsonb_build_object('reactivated_at', now()) ELSE '{}'::jsonb END,
				status = CASE WHEN $7::boolean THEN 'active' ELSE brreg_raw_input_domains.status END,
				removed_at = CASE WHEN $7::boolean THEN NULL ELSE brreg_raw_input_domains.removed_at END,
				removed_by = CASE WHEN $7::boolean THEN NULL ELSE brreg_raw_input_domains.removed_by END,
				updated_at = now()
			WHERE brreg_raw_input_domains.status = 'active' OR $7::boolean
		`, company.RawInputID, domainID, actionID, signal, confidence, metadata, params.Force); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return errors.Wrapf(err, "upsert brreg raw input domain %s/%s", discovery.NativeID, domain)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return errors.Wrap(err, "commit brreg raw input domain write")
	}
	return nil
}
```

- [ ] **Step 9: Add `cockroachdb/errors` dependency if missing**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go get github.com/cockroachdb/errors@latest
```

Expected: `go.mod` and `go.sum` include `github.com/cockroachdb/errors`.

- [ ] **Step 10: Run data-pipeline targeted tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go test ./activities ./workflows -run 'DomainDiscovery|DiscoveredDomains|EnrichCompanyDomainsMarksRawInputActionEvents' -count=1
```

Expected: PASS.

- [ ] **Step 11: Commit data-pipeline bridge writer**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add services/go-worker/contracts/contracts.go \
  services/go-worker/workflows/enrich_company_domains.go \
  services/go-worker/workflows/enrich_company_domains_test.go \
  services/go-worker/activities/activities.go \
  services/go-worker/activities/activities_test.go \
  services/go-worker/go.mod \
  services/go-worker/go.sum
git commit -m "feat: write brreg discovered domains to raw input bridge"
```

## Task 5: BRREG Processor Submits Active Domain Links

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/workers/brreg_processor.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/workers/brreg_processor_test.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/workers/processor_testmock_test.go`

- [ ] **Step 1: Add failing processor tests**

Add this test to `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/workers/brreg_processor_test.go`:

```go
func TestBrregProcessor_ExistingCompanyWithDomainLinksCreatesDomainSuggestions(t *testing.T) {
	ctx := context.Background()
	rawID := uuid.New()
	sourceID := uuid.New()
	companyID := uuid.New()
	domainConnectionID := uuid.New()
	domainID := uuid.New()
	rawRow := db.ClaimPendingBrregRawInputsRow{
		ID:                 rawID,
		OrganizationNumber: "810202572",
		OrganizationName:   ptrStr("BORTIGARD AS"),
		PayloadHash:        "br123",
		ProcessingStatus:   "processing",
	}

	insertedDomainSuggestion := false
	rawInputSubmitted := false
	claimCalls := 0
	q := &mockQuerier{
		claimBrreg: func() []db.ClaimPendingBrregRawInputsRow {
			claimCalls++
			if claimCalls == 1 {
				return []db.ClaimPendingBrregRawInputsRow{rawRow}
			}
			return nil
		},
		getCompanyByRegAndCountry: func(reg *string, iso string) (db.Company, error) {
			require.Equal(t, "NO", iso)
			return db.Company{ID: companyID, RegistrationNumber: reg}, nil
		},
		getSourceByName: func(name string) (db.DataSource, error) {
			return db.DataSource{ID: sourceID, Name: name}, nil
		},
		listActiveBrregRawInputDomainsForSuggestion: func(id uuid.UUID) ([]db.ListActiveBrregRawInputDomainsForSuggestionRow, error) {
			require.Equal(t, rawID, id)
			return []db.ListActiveBrregRawInputDomainsForSuggestionRow{{
				ID:         domainConnectionID,
				RawInputID: rawID,
				DomainID:   domainID,
				Domain:     "bortigard.no",
				Signal:     "manual",
				Confidence: 100,
				Metadata:   []byte(`{"created_by":"ops"}`),
			}}, nil
		},
		insertSuggestion: func(arg db.InsertSuggestionParams) (db.Suggestion, error) {
			require.True(t, arg.TargetCompanyID.Valid)
			require.Equal(t, companyID, arg.TargetCompanyID.UUID)
			return db.Suggestion{ID: uuid.New()}, nil
		},
		insertSuggestionCompanyDomain: func(arg db.InsertSuggestionCompanyDomainParams) (db.SuggestionCompanyDomain, error) {
			require.Equal(t, "add", arg.Operation)
			require.Equal(t, "bortigard.no", arg.Domain)
			require.Equal(t, "candidate", arg.RelationshipType)
			require.Equal(t, "needs_review", arg.DomainStatus)
			require.Equal(t, "manual_import", arg.Signal)
			require.Equal(t, int16(100), arg.SignalConfidence)
			require.Contains(t, string(arg.Evidence), domainConnectionID.String())
			insertedDomainSuggestion = true
			return db.SuggestionCompanyDomain{ID: uuid.New(), SuggestionID: arg.SuggestionID}, nil
		},
		markBrregSubmitted: func(id uuid.UUID) error {
			require.Equal(t, rawID, id)
			rawInputSubmitted = true
			return nil
		},
	}

	proc := workers.NewBrregProcessor(q)
	require.NoError(t, proc.ProcessBatch(ctx, "brreg"))
	require.True(t, insertedDomainSuggestion)
	require.True(t, rawInputSubmitted)
}
```

Add a failure test that `InsertSuggestionCompanyDomain` returning an error causes `MarkBrregRawInputFailed` with text containing `insert domain suggestion`.

- [ ] **Step 2: Run failing processor tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/workers -run 'BrregProcessor_.*Domain' -count=1
```

Expected: FAIL because the mock and processor do not call the domain query or insert domain suggestions.

- [ ] **Step 3: Extend worker test mock**

Patch `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/workers/processor_testmock_test.go` with fields:

```go
listActiveBrregRawInputDomainsForSuggestion func(uuid.UUID) ([]db.ListActiveBrregRawInputDomainsForSuggestionRow, error)
insertSuggestionCompanyDomain               func(db.InsertSuggestionCompanyDomainParams) (db.SuggestionCompanyDomain, error)
```

Add methods:

```go
func (q *mockQuerier) ListActiveBrregRawInputDomainsForSuggestion(ctx context.Context, id uuid.UUID) ([]db.ListActiveBrregRawInputDomainsForSuggestionRow, error) {
	if q.listActiveBrregRawInputDomainsForSuggestion != nil {
		return q.listActiveBrregRawInputDomainsForSuggestion(id)
	}
	return nil, nil
}

func (q *mockQuerier) InsertSuggestionCompanyDomain(ctx context.Context, arg db.InsertSuggestionCompanyDomainParams) (db.SuggestionCompanyDomain, error) {
	if q.insertSuggestionCompanyDomain != nil {
		return q.insertSuggestionCompanyDomain(arg)
	}
	return db.SuggestionCompanyDomain{ID: uuid.New(), SuggestionID: arg.SuggestionID}, nil
}
```

- [ ] **Step 4: Add processor helpers**

Patch `/Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/internal/workers/brreg_processor.go`.

Add imports:

```go
import "encoding/json"
```

Add helper functions:

```go
func mapBrregRawInputDomainSignal(signal string) string {
	switch signal {
	case "manual":
		return "manual_import"
	case "heuristic":
		return "search"
	case "wikidata", "certsh", "whois", "search":
		return signal
	default:
		return "search"
	}
}

func brregRawInputDomainEvidence(src db.DataSource, row db.ClaimPendingBrregRawInputsRow, domain db.ListActiveBrregRawInputDomainsForSuggestionRow) ([]byte, error) {
	evidence := map[string]any{
		"source":              src.Name,
		"source_input_table":  "brreg_company_raw_inputs",
		"raw_input_id":        row.ID.String(),
		"raw_input_domain_id": domain.ID.String(),
		"domain_id":           domain.DomainID.String(),
		"source_native_id":    row.OrganizationNumber,
		"payload_hash":        row.PayloadHash,
		"signal":              domain.Signal,
		"metadata":            json.RawMessage(domain.Metadata),
	}
	if domain.ActionID.Valid {
		evidence["action_id"] = domain.ActionID.UUID.String()
	}
	return json.Marshal(evidence)
}
```

- [ ] **Step 5: Create domain suggestions in the same row transaction**

Patch `processOneWithQueries`:

1. Load active raw-input domains before deciding whether a suggestion is needed:

```go
domainLinks, err := q.ListActiveBrregRawInputDomainsForSuggestion(ctx, row.ID)
if err != nil {
	return errors.Wrap(err, "list brreg raw input domains for suggestion")
}
```

2. Track the root suggestion:

```go
var suggestionID uuid.UUID
var targetCompanyID uuid.UUID
createdSuggestion := false
```

3. When an existing company has no website change but `len(domainLinks) > 0`, create a root suggestion with `TargetCompanyID` set to that company.

4. After profile suggestion logic, insert each active domain link:

```go
if createdSuggestion {
	for _, domainLink := range domainLinks {
		evidence, err := brregRawInputDomainEvidence(src, row, domainLink)
		if err != nil {
			return errors.Wrap(err, "marshal brreg raw input domain evidence")
		}
		params := db.InsertSuggestionCompanyDomainParams{
			SuggestionID:      suggestionID,
			Operation:         "add",
			Confidence:        ptrFloat32(float32(domainLink.Confidence) / 100),
			Domain:            domainLink.Domain,
			RelationshipType:  "candidate",
			DomainStatus:      "needs_review",
			Signal:            mapBrregRawInputDomainSignal(domainLink.Signal),
			SignalConfidence:  domainLink.Confidence,
			Evidence:          evidence,
		}
		if targetCompanyID != uuid.Nil {
			params.TargetRowID = pgUUID(targetCompanyID)
		}
		if _, err := q.InsertSuggestionCompanyDomain(ctx, params); err != nil {
			return errors.Wrap(err, "insert domain suggestion")
		}
	}
	return q.MarkBrregRawInputSubmitted(ctx, row.ID)
}
return q.MarkBrregRawInputProcessed(ctx, row.ID)
```

- [ ] **Step 6: Run processor tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/workers -run 'BrregProcessor' -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit processor work**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/workers/brreg_processor.go \
  scheduler/internal/workers/brreg_processor_test.go \
  scheduler/internal/workers/processor_testmock_test.go
git commit -m "feat: submit brreg raw input domains for review"
```

## Task 6: UI Shows Domain Counts And Detail Controls

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/ui/app/types/api.ts`
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/ui/app/lib/api.ts`
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/ui/app/components/app/RawInputsTable.tsx`
- Modify: `/Users/graovic/pulsarpoint/ppoint/corpscout/ui/app/components/app/RawInputDetailSheet.tsx`
- Generated build output: `/Users/graovic/pulsarpoint/ppoint/corpscout/ui/build`

- [ ] **Step 1: Add TypeScript API types**

Patch `/Users/graovic/pulsarpoint/ppoint/corpscout/ui/app/types/api.ts`.

Add to `RawInput`:

```ts
connected_domain_count: number;
```

Add this interface:

```ts
export interface RawInputConnectedDomain {
  id: string;
  domain_id: string;
  domain: string;
  action_id?: string;
  signal: "manual" | "wikidata" | "certsh" | "whois" | "search" | "heuristic";
  confidence: number;
  status: "active" | "removed";
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
  removed_at?: string;
  removed_by?: string;
}
```

Add to `RawInputDetail`:

```ts
connected_domains?: RawInputConnectedDomain[];
```

- [ ] **Step 2: Add API client methods**

Patch `/Users/graovic/pulsarpoint/ppoint/corpscout/ui/app/lib/api.ts` inside `api`:

```ts
addBrregRawInputDomain: (rawInputId: string, body: { domain: string; note?: string }) =>
  post<unknown>(`/raw-inputs/brreg/${rawInputId}/domains`, body),

removeBrregRawInputDomain: (rawInputId: string, connectionId: string, body: { reason?: string } = {}) =>
  post<unknown>(`/raw-inputs/brreg/${rawInputId}/domains/${connectionId}/remove`, body),
```

- [ ] **Step 3: Add the table column**

Patch `/Users/graovic/pulsarpoint/ppoint/corpscout/ui/app/components/app/RawInputsTable.tsx` in the column list after the state/action status columns:

```tsx
cols.push({
  accessorKey: "connected_domain_count",
  header: "Domains",
  cell: ({ row }) => {
    const count = row.original.connected_domain_count ?? 0;
    return (
      <span className="tabular-nums text-muted-foreground">
        {count}
      </span>
    );
  },
});
```

Keep the row click behavior unchanged so the detail sheet opens when an operator clicks a BRREG row.

- [ ] **Step 4: Add connected domains UI state**

Patch `/Users/graovic/pulsarpoint/ppoint/corpscout/ui/app/components/app/RawInputDetailSheet.tsx`.

Add state near existing state declarations:

```tsx
const [domainValue, setDomainValue] = useState("");
const [domainNote, setDomainNote] = useState("");
const [domainActionError, setDomainActionError] = useState<string | null>(null);
const [domainActionPending, setDomainActionPending] = useState(false);
```

Add refresh helper:

```tsx
const reloadDetail = useCallback(() => {
  if (!open || !source || !id) return;
  setLoading(true);
  setError(null);
  api.getRawInput(source, id)
    .then(setDetail)
    .catch((err: unknown) => setError(errorMessage(err, "Failed to load raw input")))
    .finally(() => setLoading(false));
}, [open, source, id]);
```

Use `reloadDetail` from the existing `useEffect`.

- [ ] **Step 5: Add manual add/remove actions in the sheet**

Add handlers in `RawInputDetailSheet.tsx`:

```tsx
const addDomain = async () => {
  if (!detail || detail.source !== "brreg") return;
  const domain = domainValue.trim();
  if (!domain) {
    setDomainActionError("Domain is required");
    return;
  }
  setDomainActionPending(true);
  setDomainActionError(null);
  try {
    await api.addBrregRawInputDomain(detail.id, {
      domain,
      note: domainNote.trim() || undefined,
    });
    setDomainValue("");
    setDomainNote("");
    reloadDetail();
  } catch (err) {
    setDomainActionError(errorMessage(err, "Failed to add domain"));
  } finally {
    setDomainActionPending(false);
  }
};

const removeDomain = async (connectionId: string) => {
  if (!detail || detail.source !== "brreg") return;
  setDomainActionPending(true);
  setDomainActionError(null);
  try {
    await api.removeBrregRawInputDomain(detail.id, connectionId);
    reloadDetail();
  } catch (err) {
    setDomainActionError(errorMessage(err, "Failed to remove domain"));
  } finally {
    setDomainActionPending(false);
  }
};
```

Add this section below the status/action summary and above payload JSON:

```tsx
{detail.source === "brreg" ? (
  <section className="space-y-3 border-t pt-4">
    <div className="flex items-center justify-between gap-3">
      <h3 className="text-sm font-medium">Connected domains</h3>
      <span className="text-xs text-muted-foreground tabular-nums">
        {(detail.connected_domains ?? []).length}
      </span>
    </div>

    <div className="grid gap-2 sm:grid-cols-[1fr_1fr_auto]">
      <input
        value={domainValue}
        onChange={(event) => setDomainValue(event.target.value)}
        aria-label="Domain"
        className="h-9 rounded-md border bg-background px-3 text-sm"
      />
      <input
        value={domainNote}
        onChange={(event) => setDomainNote(event.target.value)}
        aria-label="Note"
        className="h-9 rounded-md border bg-background px-3 text-sm"
      />
      <button
        type="button"
        onClick={addDomain}
        disabled={domainActionPending}
        className="h-9 rounded-md border px-3 text-sm"
      >
        Add
      </button>
    </div>

    {domainActionError ? (
      <p className="text-xs text-destructive">{domainActionError}</p>
    ) : null}

    <div className="divide-y rounded-md border">
      {(detail.connected_domains ?? []).length === 0 ? (
        <div className="px-3 py-3 text-sm text-muted-foreground">No connected domains</div>
      ) : (
        (detail.connected_domains ?? []).map((connection) => (
          <div key={connection.id} className="flex items-center justify-between gap-3 px-3 py-2">
            <div className="min-w-0">
              <div className="truncate text-sm font-medium">{connection.domain}</div>
              <div className="mt-1 flex flex-wrap gap-2 text-xs text-muted-foreground">
                <span>{connection.signal}</span>
                <span className="tabular-nums">{connection.confidence}</span>
                <span>{connection.status}</span>
              </div>
            </div>
            <button
              type="button"
              onClick={() => removeDomain(connection.id)}
              disabled={domainActionPending}
              className="h-8 rounded-md border px-2 text-xs"
            >
              Remove
            </button>
          </div>
        ))
      )}
    </div>
  </section>
) : null}
```

- [ ] **Step 6: Run UI checks**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
pnpm build
```

Expected: both commands PASS and update `/Users/graovic/pulsarpoint/ppoint/corpscout/ui/build`.

- [ ] **Step 7: Commit UI work**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add ui/app/types/api.ts \
  ui/app/lib/api.ts \
  ui/app/components/app/RawInputsTable.tsx \
  ui/app/components/app/RawInputDetailSheet.tsx \
  ui/build
git commit -m "feat: manage brreg raw input domain links in ui"
```

## Task 7: End-To-End Verification

**Files:**
- Verify: `/Users/graovic/pulsarpoint/ppoint/corpscout`
- Verify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker`

- [ ] **Step 1: Run Corpscout scheduler tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./...
```

Expected: PASS.

- [ ] **Step 2: Run data-pipelines Go worker tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go test ./...
```

Expected: PASS.

- [ ] **Step 3: Run UI checks**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
pnpm build
```

Expected: PASS.

- [ ] **Step 4: Start local app services**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
docker compose up -d --build scheduler ui
```

Expected: `corpscout-scheduler` and `corpscout-ui` containers are running.

- [ ] **Step 5: Browser smoke test**

Open:

```text
http://localhost:8094/sources/brreg/raw_input
```

Verify:

- [ ] The table has a `Domains` column.
- [ ] At least one row opens the detail sheet on click.
- [ ] The detail sheet shows connected domains, raw payload, translated payload when present, and lifecycle action statuses.
- [ ] Adding `manual-test.example` creates a manual connection.
- [ ] Removing a manual test connection hides it from the active connected domains list and decrements the table count after refresh.

- [ ] **Step 6: Final git checks**

Run:

```bash
git -C /Users/graovic/pulsarpoint/ppoint/corpscout status --short
git -C /Users/graovic/pulsarpoint/ppoint/data-pipelines status --short
```

Expected:

```text
# Corpscout has no uncommitted files after commits.
# data-pipelines only has the pre-existing .github/workflows/go-worker-image.yml dirty file.
```

- [ ] **Step 7: Report completed commits**

Run:

```bash
git -C /Users/graovic/pulsarpoint/ppoint/corpscout log --oneline -5
git -C /Users/graovic/pulsarpoint/ppoint/data-pipelines log --oneline -3
```

Expected: logs include the commits from Tasks 1, 2, 3, 5, 6 in Corpscout and Task 4 in data-pipelines.
