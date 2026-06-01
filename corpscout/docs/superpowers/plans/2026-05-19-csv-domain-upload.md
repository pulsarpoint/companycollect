# CSV Domain Upload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let users upload a CSV file of domains (with optional company names) that are imported into the database with company links created when the company is found by exact name match.

**Architecture:** A multipart POST handler uploads the CSV to S3 and immediately enqueues a `domain_import` River job. The worker reads the CSV from S3, upserts each domain with `import_source='manual_upload'`, and optionally links it to an existing company (exact name match → `company_domains` with `signal='manual_upload'`). No crawl jobs are queued; no company suggestions are created. A new `domain_import_batches` table tracks progress. The domains table gains an `import_source` column; `company_domains.signal` is extended with `'manual_upload'`; the `v_domains` view is updated to expose `import_source`.

**Tech Stack:** Go (encoding/csv, River queue, pgx/v5, sqlc), S3 (existing s3client), React Router v7 + shadcn/ui

---

## File Structure

**Create:**
- `database/migrations/000028_domain_import.up.sql` — schema changes (column, table, constraint, view)
- `database/migrations/000028_domain_import.down.sql` — rollback
- `database/queries/domain_import.sql` — batch CRUD queries
- `scheduler/internal/workers/domain_import.go` — DomainImportWorker
- `scheduler/internal/workers/domain_import_test.go` — worker unit tests
- `scheduler/internal/httpapi/domain_import.go` — POST /domains/import + GET /domains/import-batches handlers
- `scheduler/internal/httpapi/domain_import_test.go` — handler tests
- `ui/app/components/app/UploadDomainsDialog.tsx` — upload dialog component

**Modify:**
- `database/queries/domains.sql` — add `UpsertDomainWithSource`
- `database/queries/companies.sql` — add `GetCompanyByExactName`
- `scheduler/internal/workers/workers.go` — add `DomainImportArgs`
- `scheduler/internal/app/river.go` — register `DomainImportWorker` + `domain_import` queue
- `scheduler/internal/httpapi/handlers.go` — add 2 import routes
- `scheduler/internal/httpapi/testhelpers_test.go` — add 6 new stub methods
- `ui/app/types/api.ts` — add `DomainImportBatch`, update `VDomain` with `import_source`
- `ui/app/lib/api.ts` — add `uploadDomainsCSV`
- `ui/app/routes/domains.tsx` — add Upload button + dialog

---

### Task 1: Database migration

**Files:**
- Create: `database/migrations/000028_domain_import.up.sql`
- Create: `database/migrations/000028_domain_import.down.sql`

- [ ] **Step 1: Write the up migration**

```sql
-- database/migrations/000028_domain_import.up.sql

-- 1. Add import_source to domains (existing rows default to 'crawler')
ALTER TABLE domains
    ADD COLUMN import_source TEXT NOT NULL DEFAULT 'crawler'
    CHECK (import_source IN ('crawler', 'manual_upload'));

-- 2. Track CSV upload batches
CREATE TABLE domain_import_batches (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    filename      TEXT        NOT NULL,
    csv_s3_key    TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    rows_total    INTEGER     NOT NULL DEFAULT 0,
    rows_imported INTEGER     NOT NULL DEFAULT 0,
    rows_skipped  INTEGER     NOT NULL DEFAULT 0,
    rows_failed   INTEGER     NOT NULL DEFAULT 0,
    error_message TEXT,
    river_job_id  BIGINT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ
);

CREATE INDEX ON domain_import_batches(created_at DESC);

-- 3. Extend signal enum on company_domains to allow manual uploads
ALTER TABLE company_domains
    DROP CONSTRAINT company_domains_signal_check,
    ADD CONSTRAINT company_domains_signal_check
        CHECK (signal IN ('registry_website','wikidata','certsh','whois','search','manual_upload'));

-- 4. Update v_domains to expose import_source
CREATE OR REPLACE VIEW v_domains AS
SELECT
    d.id,
    d.domain,
    d.import_source,
    d.first_seen_at,
    d.last_verified_at,
    COUNT(DISTINCT cd.company_id)::int AS company_count,
    MAX(cd.confidence)                 AS max_confidence,
    (
        SELECT c2.name
        FROM company_domains cd2
        JOIN companies c2 ON c2.id = cd2.company_id
        WHERE cd2.domain_id = d.id
        ORDER BY cd2.confidence DESC
        LIMIT 1
    ) AS primary_company_name,
    (
        SELECT c2.id
        FROM company_domains cd2
        JOIN companies c2 ON c2.id = cd2.company_id
        WHERE cd2.domain_id = d.id
        ORDER BY cd2.confidence DESC
        LIMIT 1
    ) AS primary_company_id,
    (
        SELECT cd2.signal
        FROM company_domains cd2
        WHERE cd2.domain_id = d.id
        ORDER BY cd2.confidence DESC
        LIMIT 1
    ) AS primary_signal,
    (
        SELECT MAX(j.created_at)
        FROM domain_crawl_jobs j
        WHERE j.domain_id = d.id
    ) AS last_crawled_at,
    EXISTS (
        SELECT 1
        FROM domain_crawl_jobs j
        WHERE j.domain_id = d.id
    ) AS crawled
FROM domains d
LEFT JOIN company_domains cd ON cd.domain_id = d.id
GROUP BY d.id, d.domain, d.import_source, d.first_seen_at, d.last_verified_at;
```

- [ ] **Step 2: Write the down migration**

```sql
-- database/migrations/000028_domain_import.down.sql

-- Restore v_domains without import_source
CREATE OR REPLACE VIEW v_domains AS
SELECT
    d.id,
    d.domain,
    d.first_seen_at,
    d.last_verified_at,
    COUNT(DISTINCT cd.company_id)::int AS company_count,
    MAX(cd.confidence)                 AS max_confidence,
    (
        SELECT c2.name
        FROM company_domains cd2
        JOIN companies c2 ON c2.id = cd2.company_id
        WHERE cd2.domain_id = d.id
        ORDER BY cd2.confidence DESC
        LIMIT 1
    ) AS primary_company_name,
    (
        SELECT c2.id
        FROM company_domains cd2
        JOIN companies c2 ON c2.id = cd2.company_id
        WHERE cd2.domain_id = d.id
        ORDER BY cd2.confidence DESC
        LIMIT 1
    ) AS primary_company_id,
    (
        SELECT cd2.signal
        FROM company_domains cd2
        WHERE cd2.domain_id = d.id
        ORDER BY cd2.confidence DESC
        LIMIT 1
    ) AS primary_signal,
    (
        SELECT MAX(j.created_at)
        FROM domain_crawl_jobs j
        WHERE j.domain_id = d.id
    ) AS last_crawled_at,
    EXISTS (
        SELECT 1
        FROM domain_crawl_jobs j
        WHERE j.domain_id = d.id
    ) AS crawled
FROM domains d
LEFT JOIN company_domains cd ON cd.domain_id = d.id
GROUP BY d.id, d.domain, d.first_seen_at, d.last_verified_at;

-- Remove manual_upload from signal check
ALTER TABLE company_domains
    DROP CONSTRAINT company_domains_signal_check,
    ADD CONSTRAINT company_domains_signal_check
        CHECK (signal IN ('registry_website','wikidata','certsh','whois','search'));

DROP TABLE IF EXISTS domain_import_batches;

ALTER TABLE domains DROP COLUMN IF EXISTS import_source;
```

- [ ] **Step 3: Apply migration**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off make migrate-up
```

Expected output contains: `000028/u domain_import`

- [ ] **Step 4: Verify in psql**

```bash
psql "postgres://corpscout:corpscout@localhost:5435/corpscout" -c "\d domains" | grep import_source
psql "postgres://corpscout:corpscout@localhost:5435/corpscout" -c "\d domain_import_batches"
psql "postgres://corpscout:corpscout@localhost:5435/corpscout" -c "SELECT column_name FROM information_schema.columns WHERE table_name='v_domains' ORDER BY ordinal_position;"
```

Expected: `import_source` column in domains, `domain_import_batches` table exists, `import_source` visible in v_domains columns.

- [ ] **Step 5: Commit**

```bash
git add database/migrations/000028_domain_import.up.sql database/migrations/000028_domain_import.down.sql
git commit -m "db: migration 000028 — import_source, domain_import_batches, extend signal enum, update v_domains"
```

---

### Task 2: SQL queries

**Files:**
- Modify: `database/queries/domain_import.sql` (create)
- Modify: `database/queries/domains.sql`
- Modify: `database/queries/companies.sql`

- [ ] **Step 1: Create domain_import.sql**

```sql
-- database/queries/domain_import.sql

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

-- name: GetImportBatch :one
SELECT * FROM domain_import_batches WHERE id = $1;

-- name: ListImportBatches :many
SELECT * FROM domain_import_batches ORDER BY created_at DESC LIMIT $1;
```

- [ ] **Step 2: Add UpsertDomainWithSource to domains.sql**

Append to `database/queries/domains.sql`:

```sql
-- name: UpsertDomainWithSource :one
INSERT INTO domains (domain, import_source)
VALUES ($1, $2)
ON CONFLICT (domain) DO UPDATE SET last_verified_at = now()
RETURNING *;
```

- [ ] **Step 3: Add GetCompanyByExactName to companies.sql**

Append to `database/queries/companies.sql`:

```sql
-- name: GetCompanyByExactName :one
SELECT * FROM companies WHERE lower(name) = lower($1) LIMIT 1;
```

- [ ] **Step 4: Commit**

```bash
git add database/queries/domain_import.sql database/queries/domains.sql database/queries/companies.sql
git commit -m "db: add domain import batch queries, UpsertDomainWithSource, GetCompanyByExactName"
```

---

### Task 3: Run sqlc generate

**Files:**
- Modify: `scheduler/internal/db/gen/` (regenerated — do not hand-edit)

- [ ] **Step 1: Generate**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off make sqlc-generate
```

Expected: no errors, updated files in `scheduler/internal/db/gen/`.

- [ ] **Step 2: Verify new methods exist**

```bash
grep -n "InsertImportBatch\|UpdateImportBatchRiverJob\|UpdateImportBatchStarted\|UpdateImportBatchCompleted\|GetImportBatch\|ListImportBatches\|UpsertDomainWithSource\|GetCompanyByExactName" scheduler/internal/db/gen/querier.go
```

Expected: all 8 method names appear in the interface.

- [ ] **Step 3: Verify build**

```bash
GOWORK=off go build ./... 2>&1
```

Expected: compile errors only about missing stub methods in `testhelpers_test.go` (the interface grew). This is expected and will be fixed in Task 7.

- [ ] **Step 4: Commit**

```bash
git add scheduler/internal/db/gen/
git commit -m "db: regenerate sqlc after domain import queries"
```

---

### Task 4: DomainImportWorker

**Files:**
- Modify: `scheduler/internal/workers/workers.go`
- Create: `scheduler/internal/workers/domain_import.go`

- [ ] **Step 1: Add DomainImportArgs to workers.go**

Append to `scheduler/internal/workers/workers.go`:

```go
// DomainImportArgs are the arguments for a CSV domain import River job.
type DomainImportArgs struct {
	BatchID   string `json:"batch_id"`
	CsvS3Key  string `json:"csv_s3_key"`
}

func (DomainImportArgs) Kind() string { return "domain_import" }
```

- [ ] **Step 2: Write the failing test first**

Create `scheduler/internal/workers/domain_import_test.go`:

```go
package workers_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"testing"

	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

// fakeS3 is an in-memory replacement for s3client.Client used in worker tests.
type fakeS3 struct {
	objects map[string][]byte
}

func newFakeS3() *fakeS3 { return &fakeS3{objects: make(map[string][]byte)} }

func (f *fakeS3) Upload(ctx context.Context, key string, body []byte, _ string) error {
	f.objects[key] = body
	return nil
}

func (f *fakeS3) Download(ctx context.Context, key string) ([]byte, string, error) {
	data, ok := f.objects[key]
	if !ok {
		return nil, "", pgx.ErrNoRows
	}
	return data, "text/csv", nil
}

func buildCSV(rows [][]string) []byte {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.WriteAll(rows)
	w.Flush()
	return buf.Bytes()
}

// importQuerier is a configurable db.Querier for domain import tests.
type importQuerier struct {
	db.Querier
	updateBatchStarted    func(ctx context.Context, arg db.UpdateImportBatchStartedParams) error
	updateBatchCompleted  func(ctx context.Context, arg db.UpdateImportBatchCompletedParams) error
	upsertDomainWithSrc   func(ctx context.Context, arg db.UpsertDomainWithSourceParams) (db.Domain, error)
	getCompanyByExactName func(ctx context.Context, name string) (db.Company, error)
	upsertCompanyDomain   func(ctx context.Context, arg db.UpsertCompanyDomainParams) (db.CompanyDomain, error)
}

func (q *importQuerier) UpdateImportBatchStarted(ctx context.Context, arg db.UpdateImportBatchStartedParams) error {
	if q.updateBatchStarted != nil {
		return q.updateBatchStarted(ctx, arg)
	}
	return nil
}

func (q *importQuerier) UpdateImportBatchCompleted(ctx context.Context, arg db.UpdateImportBatchCompletedParams) error {
	if q.updateBatchCompleted != nil {
		return q.updateBatchCompleted(ctx, arg)
	}
	return nil
}

func (q *importQuerier) UpsertDomainWithSource(ctx context.Context, arg db.UpsertDomainWithSourceParams) (db.Domain, error) {
	if q.upsertDomainWithSrc != nil {
		return q.upsertDomainWithSrc(ctx, arg)
	}
	return db.Domain{ID: uuid.New(), Domain: arg.Domain}, nil
}

func (q *importQuerier) GetCompanyByExactName(ctx context.Context, name string) (db.Company, error) {
	if q.getCompanyByExactName != nil {
		return q.getCompanyByExactName(ctx, name)
	}
	return db.Company{}, pgx.ErrNoRows
}

func (q *importQuerier) UpsertCompanyDomain(ctx context.Context, arg db.UpsertCompanyDomainParams) (db.CompanyDomain, error) {
	if q.upsertCompanyDomain != nil {
		return q.upsertCompanyDomain(ctx, arg)
	}
	return db.CompanyDomain{}, nil
}

func TestDomainImportWorker_NoCompany_JustInsertsDomain(t *testing.T) {
	ctx := context.Background()
	batchID := uuid.New()
	s3 := newFakeS3()
	csvKey := "imports/test.csv"
	s3.objects[csvKey] = buildCSV([][]string{
		{"num", "domain", "company"},
		{"1", "example.com", ""},
	})

	domainUpserted := false
	q := &importQuerier{
		upsertDomainWithSrc: func(_ context.Context, arg db.UpsertDomainWithSourceParams) (db.Domain, error) {
			assert.Equal(t, "example.com", arg.Domain)
			assert.Equal(t, "manual_upload", arg.ImportSource)
			domainUpserted = true
			return db.Domain{ID: uuid.New(), Domain: arg.Domain}, nil
		},
	}

	w := workers.NewDomainImportWorker(q, s3)
	job := &river.Job[workers.DomainImportArgs]{
		Args: workers.DomainImportArgs{BatchID: batchID.String(), CsvS3Key: csvKey},
	}
	err := w.Work(ctx, job)
	require.NoError(t, err)
	assert.True(t, domainUpserted, "domain should be upserted with import_source=manual_upload")
}

func TestDomainImportWorker_WithKnownCompany_LinksCompanyDomain(t *testing.T) {
	ctx := context.Background()
	batchID := uuid.New()
	companyID := uuid.New()
	s3 := newFakeS3()
	csvKey := "imports/test2.csv"
	s3.objects[csvKey] = buildCSV([][]string{
		{"num", "domain", "company"},
		{"1", "acme.com", "Acme Corp"},
	})

	linkedCompanyID := uuid.UUID{}
	q := &importQuerier{
		getCompanyByExactName: func(_ context.Context, name string) (db.Company, error) {
			assert.Equal(t, "Acme Corp", name)
			return db.Company{ID: companyID, Name: name}, nil
		},
		upsertCompanyDomain: func(_ context.Context, arg db.UpsertCompanyDomainParams) (db.CompanyDomain, error) {
			linkedCompanyID = arg.CompanyID
			assert.Equal(t, "manual_upload", arg.Signal)
			assert.Equal(t, int16(90), arg.Confidence)
			assert.Equal(t, "needs_review", arg.Status)
			return db.CompanyDomain{}, nil
		},
	}

	w := workers.NewDomainImportWorker(q, s3)
	job := &river.Job[workers.DomainImportArgs]{
		Args: workers.DomainImportArgs{BatchID: batchID.String(), CsvS3Key: csvKey},
	}
	err := w.Work(ctx, job)
	require.NoError(t, err)
	assert.Equal(t, companyID, linkedCompanyID, "company domain link should use the found company's ID")
}

func TestDomainImportWorker_WithUnknownCompany_SkipsLinking(t *testing.T) {
	ctx := context.Background()
	batchID := uuid.New()
	s3 := newFakeS3()
	csvKey := "imports/test3.csv"
	s3.objects[csvKey] = buildCSV([][]string{
		{"num", "domain", "company"},
		{"1", "newco.com", "Unknown Corp"},
	})

	linkAttempted := false
	q := &importQuerier{
		// getCompanyByExactName returns ErrNoRows (default) — company not found
		upsertCompanyDomain: func(_ context.Context, _ db.UpsertCompanyDomainParams) (db.CompanyDomain, error) {
			linkAttempted = true
			return db.CompanyDomain{}, nil
		},
	}

	w := workers.NewDomainImportWorker(q, s3)
	job := &river.Job[workers.DomainImportArgs]{
		Args: workers.DomainImportArgs{BatchID: batchID.String(), CsvS3Key: csvKey},
	}
	err := w.Work(ctx, job)
	require.NoError(t, err)
	assert.False(t, linkAttempted, "company domain link should NOT be created when company is not found")
}
```

- [ ] **Step 3: Run failing tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/workers/... -run TestDomainImportWorker -v 2>&1 | tail -20
```

Expected: FAIL with "workers.NewDomainImportWorker undefined"

- [ ] **Step 4: Implement DomainImportWorker**

Create `scheduler/internal/workers/domain_import.go`:

```go
package workers

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

// s3Downloader is the subset of s3client.Client that DomainImportWorker needs.
// Defined as an interface so tests can inject a fake.
type s3Downloader interface {
	Download(ctx context.Context, key string) ([]byte, string, error)
}

// DomainImportWorker processes a CSV domain import batch.
type DomainImportWorker struct {
	river.WorkerDefaults[DomainImportArgs]
	db db.Querier
	s3 s3Downloader
}

// NewDomainImportWorker constructs a DomainImportWorker.
func NewDomainImportWorker(q db.Querier, s3 s3Downloader) *DomainImportWorker {
	return &DomainImportWorker{db: q, s3: s3}
}

// Work reads the CSV from S3, processes each row, and updates the batch record.
func (w *DomainImportWorker) Work(ctx context.Context, job *river.Job[DomainImportArgs]) error {
	args := job.Args
	batchID, err := uuid.Parse(args.BatchID)
	if err != nil {
		return errors.Wrap(err, "parse batch_id")
	}

	data, _, err := w.s3.Download(ctx, args.CsvS3Key)
	if err != nil {
		return errors.Wrap(err, "download csv from s3")
	}

	r := csv.NewReader(bytes.NewReader(data))
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1 // allow variable columns
	records, err := r.ReadAll()
	if err != nil {
		return errors.Wrap(err, "parse csv")
	}

	// Skip header row.
	rows := records
	if len(records) > 0 {
		rows = records[1:]
	}

	if err := w.db.UpdateImportBatchStarted(ctx, db.UpdateImportBatchStartedParams{
		ID:        batchID,
		RowsTotal: int32(len(rows)),
	}); err != nil {
		slog.Warn("update import batch started", "batch_id", batchID, "error", err)
	}

	var imported, skipped, failed int32
	for _, rec := range rows {
		if len(rec) < 2 {
			failed++
			continue
		}
		domainStr := strings.ToLower(strings.TrimSpace(rec[1]))
		if domainStr == "" {
			skipped++
			continue
		}
		companyName := ""
		if len(rec) >= 3 {
			companyName = strings.TrimSpace(rec[2])
		}

		if processErr := w.processRow(ctx, domainStr, companyName); processErr != nil {
			slog.Warn("import row failed", "domain", domainStr, "error", processErr)
			failed++
			continue
		}
		imported++
	}

	finalStatus := "completed"
	if imported == 0 && failed > 0 {
		finalStatus = "failed"
	}

	if err := w.db.UpdateImportBatchCompleted(ctx, db.UpdateImportBatchCompletedParams{
		ID:           batchID,
		Status:       finalStatus,
		RowsImported: imported,
		RowsSkipped:  skipped,
		RowsFailed:   failed,
	}); err != nil {
		slog.Error("update import batch completed", "batch_id", batchID, "error", err)
		return errors.Wrap(err, "update import batch")
	}

	slog.Info("domain import batch completed",
		"batch_id", batchID,
		"imported", imported,
		"skipped", skipped,
		"failed", failed,
	)
	return nil
}

func (w *DomainImportWorker) processRow(ctx context.Context, domainStr, companyName string) error {
	d, err := w.db.UpsertDomainWithSource(ctx, db.UpsertDomainWithSourceParams{
		Domain:       domainStr,
		ImportSource: "manual_upload",
	})
	if err != nil {
		return errors.Wrap(err, "upsert domain")
	}

	if companyName == "" {
		return nil
	}

	company, err := w.db.GetCompanyByExactName(ctx, companyName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Company not found — skip linking, domain is still imported.
			return nil
		}
		return errors.Wrap(err, "lookup company by name")
	}

	evidence, _ := json.Marshal(map[string]any{"source": "csv_upload"})
	_, err = w.db.UpsertCompanyDomain(ctx, db.UpsertCompanyDomainParams{
		CompanyID:        company.ID,
		DomainID:         d.ID,
		RelationshipType: "candidate",
		Status:           "needs_review",
		Signal:           "manual_upload",
		Confidence:       90,
		Evidence:         evidence,
	})
	return errors.Wrap(err, "upsert company domain")
}
```

- [ ] **Step 5: Run the tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/workers/... -run TestDomainImportWorker -v 2>&1
```

Expected: all 3 tests PASS

- [ ] **Step 6: Run all worker tests**

```bash
GOWORK=off go test ./internal/workers/... 2>&1
```

Expected: PASS (no regressions)

- [ ] **Step 7: Commit**

```bash
git add scheduler/internal/workers/workers.go scheduler/internal/workers/domain_import.go scheduler/internal/workers/domain_import_test.go
git commit -m "feat: DomainImportWorker — CSV import, company linking, crawl queueing"
```

---

### Task 5: Wire worker into River + app

**Files:**
- Modify: `scheduler/internal/app/river.go`

- [ ] **Step 1: Register DomainImportWorker in river.go**

In `scheduler/internal/app/river.go`, add the worker construction, `SetRiverClient` call, queue config, and registration. The diff to apply:

```go
// After the existing line:
//   domainCrawlWorker := workers.NewDomainCrawlWorker(q, crawler, s3)
// Add:
	domainImportWorker := workers.NewDomainImportWorker(q, s3)

// After the existing line:
//   river.AddWorker(w, domainCrawlWorker)
// Add:
	river.AddWorker(w, domainImportWorker)

// In the Queues map, add the domain_import queue:
//   "domain_import": {MaxWorkers: 2},

// After the existing line:
//   sourcePullWorker.SetRiverClient(rc)
// Add:
//   domainImportWorker.SetRiverClient(rc)
```

The full `setupRiver` function after the edit:

```go
func setupRiver(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, q db.Querier, crawler *crawlerclient.Client, s3 *s3client.Client) (*river.Client[pgx.Tx], error) {
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return nil, err
	}
	res, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	if err != nil {
		return nil, err
	}
	for _, v := range res.Versions {
		slog.Info("river migration applied", "version", v.Version, "direction", "up")
	}

	sourcePullWorker := workers.NewSourcePullWorker(q, crawler, pool)
	sourceProcessWorker := workers.NewSourceProcessWorker(q, pool)
	domainCrawlWorker := workers.NewDomainCrawlWorker(q, crawler, s3)
	domainImportWorker := workers.NewDomainImportWorker(q, s3)

	w := river.NewWorkers()
	river.AddWorker(w, sourcePullWorker)
	river.AddWorker(w, sourceProcessWorker)
	river.AddWorker(w, domainCrawlWorker)
	river.AddWorker(w, domainImportWorker)

	riverCfg := &river.Config{
		Queues: map[string]river.QueueConfig{
			"source_pull":    {MaxWorkers: cfg.CrawlConcurrency},
			"source_process": {MaxWorkers: cfg.DomainConcurrency},
			"domain_crawl":   {MaxWorkers: 3},
			"domain_import":  {MaxWorkers: 2},
		},
		Workers: w,
	}

	rc, err := river.NewClient(riverpgxv5.New(pool), riverCfg)
	if err != nil {
		return nil, err
	}
	sourcePullWorker.SetRiverClient(rc)
	return rc, nil
}
```

- [ ] **Step 2: Verify build**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go build ./... 2>&1
```

Expected: still the `testhelpers_test.go` compile errors only (not fixed until Task 7).

- [ ] **Step 3: Commit**

```bash
git add scheduler/internal/app/river.go
git commit -m "feat: wire DomainImportWorker into River (domain_import queue, maxWorkers=2)"
```

---

### Task 6: HTTP import handler

**Files:**
- Create: `scheduler/internal/httpapi/domain_import.go`
- Modify: `scheduler/internal/httpapi/handlers.go`

- [ ] **Step 1: Create domain_import.go**

```go
// scheduler/internal/httpapi/domain_import.go
package httpapi

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"github.com/riverqueue/river"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

func (h *Handlers) handleImportDomains(w http.ResponseWriter, r *http.Request) {
	if h.s3 == nil {
		writeError(w, http.StatusServiceUnavailable, "storage not available")
		return
	}

	// 10 MB limit
	if err := r.ParseMultipartForm(10 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "request too large (max 10MB)")
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing 'file' form field")
		return
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, 10<<20))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read file")
		return
	}

	batchID := uuid.New()
	s3Key := fmt.Sprintf("imports/%s.csv", batchID)

	if err := h.s3.Upload(r.Context(), s3Key, data, "text/csv"); err != nil {
		slog.Error("upload import csv to s3", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to store file")
		return
	}

	batch, err := h.db.InsertImportBatch(r.Context(), db.InsertImportBatchParams{
		Filename:  header.Filename,
		CsvS3Key:  s3Key,
	})
	if err != nil {
		slog.Error("insert import batch", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create import batch")
		return
	}

	riverJob, err := h.rv.Insert(r.Context(), workers.DomainImportArgs{
		BatchID:  batch.ID.String(),
		CsvS3Key: s3Key,
	}, &river.InsertOpts{Queue: "domain_import"})
	if err != nil {
		slog.Error("enqueue domain import job", "error", err, "batch_id", batch.ID)
		writeError(w, http.StatusInternalServerError, "failed to enqueue import job")
		return
	}

	if err := h.db.UpdateImportBatchRiverJob(r.Context(), db.UpdateImportBatchRiverJobParams{
		ID:          batch.ID,
		RiverJobID:  &riverJob.Job.ID,
	}); err != nil {
		slog.Warn("set river job id on import batch", "error", err)
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"batch_id":     batch.ID,
		"river_job_id": riverJob.Job.ID,
	})
}

func (h *Handlers) handleListImportBatches(w http.ResponseWriter, r *http.Request) {
	limit := min(queryInt(r, "limit", 20), 100)
	batches, err := h.db.ListImportBatches(r.Context(), int32(limit))
	if err != nil {
		slog.Error("list import batches", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if batches == nil {
		batches = []db.DomainImportBatch{}
	}
	writeJSON(w, http.StatusOK, batches)
}
```

- [ ] **Step 2: Register routes in handlers.go**

In `scheduler/internal/httpapi/handlers.go`, inside `RegisterRoutes`, add after the existing domain routes:

```go
r.Post("/domains/import", h.handleImportDomains)
r.Get("/domains/import-batches", h.handleListImportBatches)
```

Add these two lines directly before the line `r.Get("/domains/{id}", h.handleGetDomain)` so the static routes take priority over the parameterized `{id}` route.

The relevant block should look like:

```go
r.Get("/domains", h.handleListDomains)
r.Post("/domains/import", h.handleImportDomains)
r.Get("/domains/import-batches", h.handleListImportBatches)
r.Get("/domains/{id}", h.handleGetDomain)
r.Post("/domains/{id}/crawl", h.handleTriggerDomainCrawl)
// ... rest unchanged
```

- [ ] **Step 3: Verify build (excluding test files)**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go build ./internal/httpapi/... 2>&1
```

Expected: PASS (test files still broken, production code compiles)

- [ ] **Step 4: Commit**

```bash
git add scheduler/internal/httpapi/domain_import.go scheduler/internal/httpapi/handlers.go
git commit -m "feat: POST /domains/import and GET /domains/import-batches handlers"
```

---

### Task 7: Fix testhelpers + handler tests

**Files:**
- Modify: `scheduler/internal/httpapi/testhelpers_test.go`
- Create: `scheduler/internal/httpapi/domain_import_test.go`

- [ ] **Step 1: Add stub methods to testhelpers_test.go**

Append the following before the final `var _ db.Querier = (*stubQuerier)(nil)` line in `scheduler/internal/httpapi/testhelpers_test.go`:

```go
// Domain import batch stubs
func (s *stubQuerier) InsertImportBatch(ctx context.Context, arg db.InsertImportBatchParams) (db.DomainImportBatch, error) {
	if !s.hasExpectation("InsertImportBatch") {
		return db.DomainImportBatch{}, nil
	}
	ret := s.Called(ctx, arg)
	return ret.Get(0).(db.DomainImportBatch), ret.Error(1)
}

func (s *stubQuerier) UpdateImportBatchRiverJob(ctx context.Context, arg db.UpdateImportBatchRiverJobParams) error {
	return nil
}

func (s *stubQuerier) UpdateImportBatchStarted(ctx context.Context, arg db.UpdateImportBatchStartedParams) error {
	return nil
}

func (s *stubQuerier) UpdateImportBatchCompleted(ctx context.Context, arg db.UpdateImportBatchCompletedParams) error {
	return nil
}

func (s *stubQuerier) GetImportBatch(ctx context.Context, id uuid.UUID) (db.DomainImportBatch, error) {
	return db.DomainImportBatch{}, nil
}

func (s *stubQuerier) ListImportBatches(ctx context.Context, limit int32) ([]db.DomainImportBatch, error) {
	if !s.hasExpectation("ListImportBatches") {
		return nil, nil
	}
	ret := s.Called(ctx, limit)
	if v, ok := ret.Get(0).([]db.DomainImportBatch); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

// GetCompanyByExactName stub
func (s *stubQuerier) GetCompanyByExactName(ctx context.Context, name string) (db.Company, error) {
	return db.Company{}, pgx.ErrNoRows
}

// UpsertDomainWithSource stub
func (s *stubQuerier) UpsertDomainWithSource(ctx context.Context, arg db.UpsertDomainWithSourceParams) (db.Domain, error) {
	return db.Domain{ID: uuid.New(), Domain: arg.Domain}, nil
}
```

- [ ] **Step 2: Verify all tests compile**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go build ./... 2>&1
```

Expected: PASS (no compile errors)

- [ ] **Step 3: Write the handler tests**

Create `scheduler/internal/httpapi/domain_import_test.go`:

```go
package httpapi_test

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleImportDomains_MissingFile_Returns400(t *testing.T) {
	q := &stubQuerier{}
	r := routerForHandlers(q)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/domains/import", nil)
	req.Header.Set("Content-Type", "multipart/form-data; boundary=xxx")

	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleListImportBatches_ReturnsEmptyList(t *testing.T) {
	q := &stubQuerier{}
	r := routerForHandlers(q)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/domains/import-batches", nil)

	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

// multipartCSV builds a multipart/form-data body with a "file" field containing csvData.
func multipartCSV(t *testing.T, csvData string) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "upload.csv")
	require.NoError(t, err)
	_, err = fw.Write([]byte(csvData))
	require.NoError(t, err)
	require.NoError(t, mw.Close())
	return &buf, mw.FormDataContentType()
}
```

- [ ] **Step 4: Run all handler tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/httpapi/... -v 2>&1 | tail -30
```

Expected: all existing tests pass, 2 new tests pass.

- [ ] **Step 5: Run all tests**

```bash
GOWORK=off go test ./... 2>&1
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add scheduler/internal/httpapi/testhelpers_test.go scheduler/internal/httpapi/domain_import_test.go
git commit -m "test: domain import handler tests + stub methods for new querier methods"
```

---

### Task 8: Frontend types

**Files:**
- Modify: `ui/app/types/api.ts`

- [ ] **Step 1: Add DomainImportBatch type and update VDomain**

In `ui/app/types/api.ts`, add after the existing `VDomain` interface:

```typescript
export interface DomainImportBatch {
  id: string;
  filename: string;
  csv_s3_key: string;
  status: "pending" | "processing" | "completed" | "failed";
  rows_total: number;
  rows_imported: number;
  rows_skipped: number;
  rows_failed: number;
  error_message: string | null;
  river_job_id: number | null;
  created_at: string;
  completed_at: string | null;
}
```

And update the `VDomain` interface to add `import_source` (new field after `id`):

```typescript
export interface VDomain {
  id: string;
  domain: string;
  import_source: string;    // add this line
  first_seen_at: string | null;
  last_verified_at: string | null;
  company_count: number;
  max_confidence: number | null;
  primary_company_name: string | null;
  primary_company_id: string | null;
  primary_signal: string | null;
  crawled: boolean;
  last_crawled_at: string | null;
}
```

- [ ] **Step 2: Run typecheck**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
```

Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add ui/app/types/api.ts
git commit -m "feat: add DomainImportBatch type, add import_source to VDomain"
```

---

### Task 9: UploadDomainsDialog component + api helper

**Files:**
- Modify: `ui/app/lib/api.ts`
- Create: `ui/app/components/app/UploadDomainsDialog.tsx`

- [ ] **Step 1: Add uploadDomainsCSV to api.ts**

In `ui/app/lib/api.ts`, add the following import at the top (with the other type imports):

```typescript
import type {
  // ... existing imports ...
  DomainImportBatch,
} from "~/types/api";
```

Then append at the end of the file (alongside the other standalone exports):

```typescript
export async function uploadDomainsCSV(file: File): Promise<DomainImportBatch> {
  const formData = new FormData();
  formData.append("file", file);
  const res = await fetch(`${BASE}/domains/import`, {
    method: "POST",
    body: formData,
    // Browser sets Content-Type with boundary automatically for FormData
  });
  if (!res.ok) throw await responseError(res);
  return res.json() as Promise<DomainImportBatch>;
}

export function getImportBatches(limit = 10): Promise<DomainImportBatch[]> {
  return get<DomainImportBatch[]>(`/domains/import-batches?limit=${limit}`);
}
```

- [ ] **Step 2: Write UploadDomainsDialog.tsx**

Create `ui/app/components/app/UploadDomainsDialog.tsx`:

```tsx
import { useRef, useState } from "react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "~/components/ui/dialog";
import { uploadDomainsCSV } from "~/lib/api";
import type { DomainImportBatch } from "~/types/api";

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSuccess?: (batch: DomainImportBatch) => void;
}

export function UploadDomainsDialog({ open, onOpenChange, onSuccess }: Props) {
  const [file, setFile] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  function handleFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    setFile(e.target.files?.[0] ?? null);
  }

  async function handleSubmit() {
    if (!file) return;
    setLoading(true);
    try {
      const batch = await uploadDomainsCSV(file);
      toast.success(`Import started — batch ${batch.id.slice(0, 8)}…`);
      onOpenChange(false);
      onSuccess?.(batch);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Upload failed.");
    } finally {
      setLoading(false);
    }
  }

  function handleOpenChange(v: boolean) {
    if (!v) {
      setFile(null);
      if (inputRef.current) inputRef.current.value = "";
    }
    onOpenChange(v);
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Upload Domains CSV</DialogTitle>
        </DialogHeader>
        <div className="space-y-4 py-2">
          <p className="text-sm text-muted-foreground">
            CSV must have a header row and columns in this order:
          </p>
          <pre className="rounded bg-muted px-3 py-2 text-xs font-mono">
            num,domain,company{"\n"}
            1,example.com,Acme Corp{"\n"}
            2,orphan.io,
          </pre>
          <p className="text-sm text-muted-foreground">
            <strong>company</strong> is optional. When provided, the company is looked up by exact
            name and linked to the domain if found. Unrecognised company names are ignored — the
            domain is still imported.
          </p>
          <input
            ref={inputRef}
            type="file"
            accept=".csv,text/csv"
            className="block w-full text-sm text-foreground file:mr-3 file:rounded file:border file:border-input file:bg-background file:px-3 file:py-1 file:text-sm file:font-medium"
            onChange={handleFileChange}
          />
          {file && (
            <p className="text-xs text-muted-foreground">
              {file.name} ({(file.size / 1024).toFixed(1)} KB)
            </p>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => handleOpenChange(false)} disabled={loading}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} disabled={!file || loading}>
            {loading ? "Uploading…" : "Upload"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
```

- [ ] **Step 3: Run typecheck**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add ui/app/lib/api.ts ui/app/components/app/UploadDomainsDialog.tsx
git commit -m "feat: uploadDomainsCSV api helper + UploadDomainsDialog component"
```

---

### Task 10: Wire upload button into domains page

**Files:**
- Modify: `ui/app/routes/domains.tsx`

- [ ] **Step 1: Add import and state**

At the top of `ui/app/routes/domains.tsx`, add `UploadDomainsDialog` to the imports:

```tsx
import { UploadDomainsDialog } from "~/components/app/UploadDomainsDialog";
```

Inside `DomainsPage()`, add state for the dialog:

```tsx
const [uploadOpen, setUploadOpen] = useState(false);
```

- [ ] **Step 2: Add Upload button to the page header**

Change the header `<div>` from:

```tsx
<div className="flex items-center justify-between">
  <h1 className="text-xl font-semibold">Domains</h1>
  <span className="text-sm text-muted-foreground">{total.toLocaleString()} total</span>
</div>
```

To:

```tsx
<div className="flex items-center justify-between">
  <h1 className="text-xl font-semibold">Domains</h1>
  <div className="flex items-center gap-3">
    <span className="text-sm text-muted-foreground">{total.toLocaleString()} total</span>
    <Button variant="outline" size="sm" onClick={() => setUploadOpen(true)}>
      Upload CSV
    </Button>
  </div>
</div>
```

- [ ] **Step 3: Add UploadDomainsDialog to the JSX**

Add the dialog at the bottom of the return block, after the existing `<CrawlDomainDialog>` elements:

```tsx
<UploadDomainsDialog
  open={uploadOpen}
  onOpenChange={setUploadOpen}
  onSuccess={() => fetchData()}
/>
```

- [ ] **Step 4: Run typecheck**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ui/app/routes/domains.tsx
git commit -m "feat: Upload CSV button on domains page"
```

---

## Self-Review

**Spec coverage:**
- ✅ `import_source` column on domains table tracking manual_upload vs crawler
- ✅ CSV upload API (POST /domains/import) — multipart form, 10 MB limit
- ✅ Background River job for processing
- ✅ `domain_import_batches` table tracking status and row counts
- ✅ Company linking (exact name match → company_domains with signal='manual_upload')
- ✅ Unknown company name → skip linking, domain still imported
- ✅ No company column → domain imported only, nothing else triggered
- ✅ UI upload dialog with CSV format guide
- ✅ Upload button on domains page
- ✅ `v_domains` exposes `import_source`
- ✅ `VDomain` TypeScript type updated

**Placeholder scan:** None found.

**Type consistency:**
- `DomainImportArgs.BatchID` → parsed in worker as `uuid.Parse(args.BatchID)` ✅
- `db.UpdateImportBatchStartedParams.RowsTotal int32` matches `int32(len(rows))` ✅
- `db.UpdateImportBatchCompletedParams` fields match the SQL positional params ✅
- `s3Downloader` interface matches `*s3client.Client` methods ✅
- `riverInserter` interface matches `*river.Client[pgx.Tx].Insert` signature ✅
- `DomainImportBatch` TypeScript type matches Go `DomainImportBatch` struct from sqlc ✅
