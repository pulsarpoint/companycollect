# Postgres Source Translation Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the SQLite translation workset dispatch loop with a Postgres company-level translation queue for BRREG and Ariregister.

**Architecture:** Each source schema owns an operational `translation_queue_entries` table with one row per queued company. Temporal prepares the queue as a separate activity, then repeatedly claims company rows into a batch, translates/apply them, and updates queue row status. Queue state is intentionally not historical; metrics and history can move to another system later.

**Tech Stack:** Go, Temporal Go SDK, PostgreSQL, sqlc, pgx, NATS request/reply, React UI unchanged for the first version.

---

## File Structure

- Create: `database/migrations/000102_source_translation_queue.down.sql`
- Create: `database/migrations/000102_source_translation_queue.up.sql`
- Create: `database/queries/brreg_translation_queue.sql`
- Create: `database/queries/ariregister_translation_queue.sql`
- Create: `scheduler/internal/sourcetranslation/queue.go`
- Create: `scheduler/internal/sourcetranslation/queue_test.go`
- Create: `scheduler/internal/brreg/companydata/translation_queue.go`
- Create: `scheduler/internal/ariregister/companydata/translation_queue.go`
- Modify: `scheduler/internal/brreg/actions/company_translation_workset_actions.go`
- Modify: `scheduler/internal/ariregister/actions/company_translation_workset_actions.go`
- Modify: `scheduler/internal/brreg/workflow/company_translation.go`
- Modify: `scheduler/internal/ariregister/workflow/company_translation.go`
- Modify: `scheduler/internal/brreg/workflow/company_translation_test.go`
- Modify: `scheduler/internal/ariregister/workflow/company_translation_test.go`
- Test: `scheduler/internal/brreg/companydata/workset_test.go`
- Test: `scheduler/internal/ariregister/companydata/workset_test.go`

## Queue Semantics

Statuses:

```text
pending
running
succeeded
failed
```

Operational rules:

- A queue entry row has exactly one current status.
- `pending` rows are eligible to claim.
- `running` rows are already assigned to a batch and block new global batches.
- `succeeded` and `failed` rows are terminal operational states for the current queue.
- Queue preparation deletes old terminal rows for companies that are being re-queued.
- A partial unique index prevents duplicate active work for the same company.
- Stale `running` rows are reset to `pending` by comparing `status_changed_at` with a threshold.
- There is no `translation_batches` table in the first version.
- `batch_id` on queue rows is enough to find all companies in the current batch.

---

### Task 1: Add Source Queue Tables

**Files:**
- Create: `database/migrations/000102_source_translation_queue.up.sql`
- Create: `database/migrations/000102_source_translation_queue.down.sql`

- [ ] **Step 1: Write the migration**

Create `database/migrations/000102_source_translation_queue.up.sql`:

```sql
CREATE TABLE brreg_source.translation_queue_entries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES brreg_source.companies(id) ON DELETE CASCADE,
  status text NOT NULL,
  num_of_characters integer NOT NULL,
  batch_id text,
  status_changed_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT chk_brreg_source_translation_queue_status
    CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
  CONSTRAINT chk_brreg_source_translation_queue_chars
    CHECK (num_of_characters >= 0),
  CONSTRAINT chk_brreg_source_translation_queue_batch
    CHECK (
      (status = 'running' AND batch_id IS NOT NULL)
      OR (status <> 'running')
    )
);

CREATE UNIQUE INDEX uq_brreg_source_translation_queue_active_company
  ON brreg_source.translation_queue_entries(company_id)
  WHERE status IN ('pending', 'running');

CREATE INDEX idx_brreg_source_translation_queue_pending
  ON brreg_source.translation_queue_entries(status, status_changed_at, company_id)
  WHERE status = 'pending';

CREATE INDEX idx_brreg_source_translation_queue_running
  ON brreg_source.translation_queue_entries(status, status_changed_at)
  WHERE status = 'running';

CREATE INDEX idx_brreg_source_translation_queue_batch
  ON brreg_source.translation_queue_entries(batch_id)
  WHERE batch_id IS NOT NULL;

CREATE TABLE ariregister_source.translation_queue_entries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  status text NOT NULL,
  num_of_characters integer NOT NULL,
  batch_id text,
  status_changed_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_translation_queue_status
    CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
  CONSTRAINT chk_ariregister_source_translation_queue_chars
    CHECK (num_of_characters >= 0),
  CONSTRAINT chk_ariregister_source_translation_queue_batch
    CHECK (
      (status = 'running' AND batch_id IS NOT NULL)
      OR (status <> 'running')
    )
);

CREATE UNIQUE INDEX uq_ariregister_source_translation_queue_active_company
  ON ariregister_source.translation_queue_entries(company_id)
  WHERE status IN ('pending', 'running');

CREATE INDEX idx_ariregister_source_translation_queue_pending
  ON ariregister_source.translation_queue_entries(status, status_changed_at, company_id)
  WHERE status = 'pending';

CREATE INDEX idx_ariregister_source_translation_queue_running
  ON ariregister_source.translation_queue_entries(status, status_changed_at)
  WHERE status = 'running';

CREATE INDEX idx_ariregister_source_translation_queue_batch
  ON ariregister_source.translation_queue_entries(batch_id)
  WHERE batch_id IS NOT NULL;
```

Create `database/migrations/000102_source_translation_queue.down.sql`:

```sql
DROP TABLE IF EXISTS ariregister_source.translation_queue_entries;
DROP TABLE IF EXISTS brreg_source.translation_queue_entries;
```

- [ ] **Step 2: Verify migration ordering**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
ls database/migrations | tail -20
```

Expected: `000102_source_translation_queue.up.sql` and `.down.sql` appear after the current `000101_*` migrations.

- [ ] **Step 3: Commit**

```bash
git add database/migrations/000102_source_translation_queue.up.sql database/migrations/000102_source_translation_queue.down.sql
git commit -m "feat: add source translation queue tables"
```

---

### Task 2: Add sqlc Queue Queries

**Files:**
- Create: `database/queries/brreg_translation_queue.sql`
- Create: `database/queries/ariregister_translation_queue.sql`
- Generated: `scheduler/internal/db/gen/brreg_translation_queue.sql.go`
- Generated: `scheduler/internal/db/gen/ariregister_translation_queue.sql.go`

- [ ] **Step 1: Write BRREG queue queries**

Create `database/queries/brreg_translation_queue.sql`:

```sql
-- name: PrepareBrregTranslationQueue :one
WITH selected_companies AS (
  SELECT translation_status.company_id
  FROM brreg_source.mv_company_translation_status translation_status
  JOIN brreg_source.companies company ON company.id = translation_status.company_id
  LEFT JOIN brreg_source.mv_company_explorer entry ON entry.company_id = translation_status.company_id
  WHERE translation_status.translation_missing_count > 0
    AND (
      COALESCE(cardinality(sqlc.arg('company_ids')::uuid[]), 0) = 0
      OR translation_status.company_id = ANY(sqlc.arg('company_ids')::uuid[])
    )
    AND (
      sqlc.narg('query')::text IS NULL
      OR company.organization_name ILIKE '%' || sqlc.narg('query')::text || '%'
      OR company.organization_number ILIKE '%' || sqlc.narg('query')::text || '%'
      OR coalesce(entry.primary_industry_label, '') ILIKE '%' || sqlc.narg('query')::text || '%'
      OR coalesce(entry.city, '') ILIKE '%' || sqlc.narg('query')::text || '%'
      OR coalesce(entry.municipality, '') ILIKE '%' || sqlc.narg('query')::text || '%'
    )
    AND (sqlc.narg('lifecycle_status')::text IS NULL OR company.lifecycle_status = sqlc.narg('lifecycle_status')::text)
    AND (sqlc.narg('registration_status')::text IS NULL OR company.registration_status = sqlc.narg('registration_status')::text)
    AND (sqlc.narg('translation_status')::text IS NULL OR sqlc.narg('translation_status')::text = 'missing')
    AND (
      sqlc.narg('website_status')::text IS NULL
      OR (sqlc.narg('website_status')::text = 'with' AND entry.website_count > 0)
      OR (sqlc.narg('website_status')::text = 'without' AND entry.website_count = 0)
    )
  ORDER BY translation_status.min_missing_priority ASC,
    coalesce(entry.updated_at, translation_status.updated_at) DESC,
    company.organization_number ASC,
    translation_status.company_id ASC
  LIMIT NULLIF(GREATEST(sqlc.arg('company_limit')::integer, 0), 0)
),
estimated AS (
  SELECT
    missing.company_id,
    GREATEST(sum(length(btrim(missing.source_text)))::integer, 0) AS num_of_characters
  FROM brreg_source.v_missing_translations missing
  JOIN selected_companies selected ON selected.company_id = missing.company_id
  WHERE nullif(btrim(missing.source_text), '') IS NOT NULL
  GROUP BY missing.company_id
),
deleted_terminal AS (
  DELETE FROM brreg_source.translation_queue_entries queue
  USING estimated
  WHERE queue.company_id = estimated.company_id
    AND queue.status IN ('succeeded', 'failed')
  RETURNING queue.id
),
inserted AS (
  INSERT INTO brreg_source.translation_queue_entries (
    company_id, status, num_of_characters, batch_id, status_changed_at, created_at, updated_at
  )
  SELECT
    estimated.company_id,
    'pending',
    estimated.num_of_characters,
    NULL,
    now(),
    now(),
    now()
  FROM estimated
  ON CONFLICT DO NOTHING
  RETURNING id
)
SELECT
  (SELECT count(*)::integer FROM estimated) AS companies_seen,
  (SELECT count(*)::integer FROM inserted) AS companies_queued,
  (SELECT count(*)::integer FROM deleted_terminal) AS terminal_rows_deleted;

-- name: ResetStaleBrregTranslationQueueEntries :one
WITH reset_rows AS (
  UPDATE brreg_source.translation_queue_entries
  SET status = 'pending',
      batch_id = NULL,
      status_changed_at = now(),
      updated_at = now()
  WHERE status = 'running'
    AND status_changed_at < now() - (sqlc.arg('stale_running_seconds')::integer * interval '1 second')
  RETURNING id
)
SELECT count(*)::integer AS rows_reset FROM reset_rows;

-- name: CountRunningBrregTranslationQueueEntries :one
SELECT count(*)::integer AS running_count
FROM brreg_source.translation_queue_entries
WHERE status = 'running';

-- name: ClaimBrregTranslationQueueBatch :many
WITH locked AS (
  SELECT id, company_id, num_of_characters, status_changed_at
  FROM brreg_source.translation_queue_entries
  WHERE status = 'pending'
  ORDER BY status_changed_at ASC, company_id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT GREATEST(sqlc.arg('max_candidate_rows')::integer, 1)
),
ranked AS (
  SELECT
    id,
    company_id,
    num_of_characters,
    sum(num_of_characters) OVER (ORDER BY status_changed_at ASC, company_id ASC) AS running_chars,
    row_number() OVER (ORDER BY status_changed_at ASC, company_id ASC) AS row_number
  FROM locked
),
selected AS (
  SELECT id
  FROM ranked
  WHERE running_chars <= GREATEST(sqlc.arg('max_request_chars')::integer, 1)
     OR row_number = 1
),
updated AS (
  UPDATE brreg_source.translation_queue_entries queue
  SET status = 'running',
      batch_id = sqlc.arg('batch_id')::text,
      status_changed_at = now(),
      updated_at = now()
  FROM selected
  WHERE queue.id = selected.id
  RETURNING queue.company_id, queue.num_of_characters
)
SELECT company_id, num_of_characters
FROM updated
ORDER BY company_id ASC;

-- name: ReleaseBrregTranslationQueueBatch :one
WITH released AS (
  UPDATE brreg_source.translation_queue_entries
  SET status = 'pending',
      batch_id = NULL,
      status_changed_at = now(),
      updated_at = now()
  WHERE status = 'running'
    AND batch_id = sqlc.arg('batch_id')::text
  RETURNING id
)
SELECT count(*)::integer AS rows_released FROM released;

-- name: CompleteBrregTranslationQueueBatch :one
WITH completed AS (
  UPDATE brreg_source.translation_queue_entries
  SET status = 'succeeded',
      batch_id = NULL,
      status_changed_at = now(),
      updated_at = now()
  WHERE status = 'running'
    AND batch_id = sqlc.arg('batch_id')::text
  RETURNING id
)
SELECT count(*)::integer AS rows_completed FROM completed;

-- name: FailBrregTranslationQueueBatch :one
WITH failed AS (
  UPDATE brreg_source.translation_queue_entries
  SET status = 'failed',
      batch_id = NULL,
      status_changed_at = now(),
      updated_at = now()
  WHERE status = 'running'
    AND batch_id = sqlc.arg('batch_id')::text
  RETURNING id
)
SELECT count(*)::integer AS rows_failed FROM failed;
```

- [ ] **Step 2: Write Ariregister queue queries**

Create `database/queries/ariregister_translation_queue.sql`:

```sql
-- name: PrepareAriregisterTranslationQueue :one
WITH selected_companies AS (
  SELECT translation_status.company_id
  FROM ariregister_source.mv_company_translation_status translation_status
  JOIN ariregister_source.companies company ON company.id = translation_status.company_id
  LEFT JOIN ariregister_source.mv_company_explorer entry ON entry.company_id = translation_status.company_id
  WHERE translation_status.translation_missing_count > 0
    AND (
      COALESCE(cardinality(sqlc.arg('company_ids')::uuid[]), 0) = 0
      OR translation_status.company_id = ANY(sqlc.arg('company_ids')::uuid[])
    )
    AND (
      sqlc.narg('query')::text IS NULL
      OR company.legal_name ILIKE '%' || sqlc.narg('query')::text || '%'
      OR company.registry_code ILIKE '%' || sqlc.narg('query')::text || '%'
      OR coalesce(entry.primary_industry_label, '') ILIKE '%' || sqlc.narg('query')::text || '%'
      OR coalesce(entry.city_or_area, '') ILIKE '%' || sqlc.narg('query')::text || '%'
    )
    AND (sqlc.narg('lifecycle_status')::text IS NULL OR company.lifecycle_status = sqlc.narg('lifecycle_status')::text)
    AND (sqlc.narg('registration_status')::text IS NULL OR company.registration_status = sqlc.narg('registration_status')::text)
    AND (sqlc.narg('translation_status')::text IS NULL OR sqlc.narg('translation_status')::text = 'missing')
    AND (
      sqlc.narg('website_status')::text IS NULL
      OR (sqlc.narg('website_status')::text = 'with' AND entry.website_count > 0)
      OR (sqlc.narg('website_status')::text = 'without' AND entry.website_count = 0)
    )
  ORDER BY translation_status.min_missing_priority ASC,
    coalesce(entry.updated_at, translation_status.updated_at) DESC,
    company.registry_code ASC,
    translation_status.company_id ASC
  LIMIT NULLIF(GREATEST(sqlc.arg('company_limit')::integer, 0), 0)
),
estimated AS (
  SELECT
    missing.company_id,
    GREATEST(sum(length(btrim(missing.source_text)))::integer, 0) AS num_of_characters
  FROM ariregister_source.v_missing_translations missing
  JOIN selected_companies selected ON selected.company_id = missing.company_id
  WHERE nullif(btrim(missing.source_text), '') IS NOT NULL
  GROUP BY missing.company_id
),
deleted_terminal AS (
  DELETE FROM ariregister_source.translation_queue_entries queue
  USING estimated
  WHERE queue.company_id = estimated.company_id
    AND queue.status IN ('succeeded', 'failed')
  RETURNING queue.id
),
inserted AS (
  INSERT INTO ariregister_source.translation_queue_entries (
    company_id, status, num_of_characters, batch_id, status_changed_at, created_at, updated_at
  )
  SELECT
    estimated.company_id,
    'pending',
    estimated.num_of_characters,
    NULL,
    now(),
    now(),
    now()
  FROM estimated
  ON CONFLICT DO NOTHING
  RETURNING id
)
SELECT
  (SELECT count(*)::integer FROM estimated) AS companies_seen,
  (SELECT count(*)::integer FROM inserted) AS companies_queued,
  (SELECT count(*)::integer FROM deleted_terminal) AS terminal_rows_deleted;

-- name: ResetStaleAriregisterTranslationQueueEntries :one
WITH reset_rows AS (
  UPDATE ariregister_source.translation_queue_entries
  SET status = 'pending',
      batch_id = NULL,
      status_changed_at = now(),
      updated_at = now()
  WHERE status = 'running'
    AND status_changed_at < now() - (sqlc.arg('stale_running_seconds')::integer * interval '1 second')
  RETURNING id
)
SELECT count(*)::integer AS rows_reset FROM reset_rows;

-- name: CountRunningAriregisterTranslationQueueEntries :one
SELECT count(*)::integer AS running_count
FROM ariregister_source.translation_queue_entries
WHERE status = 'running';

-- name: ClaimAriregisterTranslationQueueBatch :many
WITH locked AS (
  SELECT id, company_id, num_of_characters, status_changed_at
  FROM ariregister_source.translation_queue_entries
  WHERE status = 'pending'
  ORDER BY status_changed_at ASC, company_id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT GREATEST(sqlc.arg('max_candidate_rows')::integer, 1)
),
ranked AS (
  SELECT
    id,
    company_id,
    num_of_characters,
    sum(num_of_characters) OVER (ORDER BY status_changed_at ASC, company_id ASC) AS running_chars,
    row_number() OVER (ORDER BY status_changed_at ASC, company_id ASC) AS row_number
  FROM locked
),
selected AS (
  SELECT id
  FROM ranked
  WHERE running_chars <= GREATEST(sqlc.arg('max_request_chars')::integer, 1)
     OR row_number = 1
),
updated AS (
  UPDATE ariregister_source.translation_queue_entries queue
  SET status = 'running',
      batch_id = sqlc.arg('batch_id')::text,
      status_changed_at = now(),
      updated_at = now()
  FROM selected
  WHERE queue.id = selected.id
  RETURNING queue.company_id, queue.num_of_characters
)
SELECT company_id, num_of_characters
FROM updated
ORDER BY company_id ASC;

-- name: ReleaseAriregisterTranslationQueueBatch :one
WITH released AS (
  UPDATE ariregister_source.translation_queue_entries
  SET status = 'pending',
      batch_id = NULL,
      status_changed_at = now(),
      updated_at = now()
  WHERE status = 'running'
    AND batch_id = sqlc.arg('batch_id')::text
  RETURNING id
)
SELECT count(*)::integer AS rows_released FROM released;

-- name: CompleteAriregisterTranslationQueueBatch :one
WITH completed AS (
  UPDATE ariregister_source.translation_queue_entries
  SET status = 'succeeded',
      batch_id = NULL,
      status_changed_at = now(),
      updated_at = now()
  WHERE status = 'running'
    AND batch_id = sqlc.arg('batch_id')::text
  RETURNING id
)
SELECT count(*)::integer AS rows_completed FROM completed;

-- name: FailAriregisterTranslationQueueBatch :one
WITH failed AS (
  UPDATE ariregister_source.translation_queue_entries
  SET status = 'failed',
      batch_id = NULL,
      status_changed_at = now(),
      updated_at = now()
  WHERE status = 'running'
    AND batch_id = sqlc.arg('batch_id')::text
  RETURNING id
)
SELECT count(*)::integer AS rows_failed FROM failed;
```

- [ ] **Step 3: Generate sqlc**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/database
sqlc generate
```

Expected: `scheduler/internal/db/gen/brreg_translation_queue.sql.go` and `scheduler/internal/db/gen/ariregister_translation_queue.sql.go` are generated.

- [ ] **Step 4: Run generated package tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/db/gen -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add database/queries/brreg_translation_queue.sql database/queries/ariregister_translation_queue.sql scheduler/internal/db/gen
git commit -m "feat: add source translation queue queries"
```

---

### Task 3: Add Shared Queue Result Types

**Files:**
- Create: `scheduler/internal/sourcetranslation/queue.go`
- Create: `scheduler/internal/sourcetranslation/queue_test.go`

- [ ] **Step 1: Write failing unit tests for shared queue helpers**

Create `scheduler/internal/sourcetranslation/queue_test.go`:

```go
package sourcetranslation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildTranslationQueueTermsSeparatesCachedAndUncachedTerms(t *testing.T) {
	fields := []MissingField{
		{
			CompanyID:            "company-1",
			SourceTable:          "source.companies",
			SourceRowID:          "company-1",
			SourceColumn:         "legal_form_label",
			TargetColumn:         "legal_form_label_en",
			SourceText:           "Osaühing",
			SourceTextNormalized: "osaühing",
			TermKey:              TermKey("Osaühing"),
		},
		{
			CompanyID:            "company-1",
			SourceTable:          "source.companies",
			SourceRowID:          "company-1",
			SourceColumn:         "country_label",
			TargetColumn:         "country_label_en",
			SourceText:           "Eesti",
			SourceTextNormalized: "eesti",
			TermKey:              TermKey("Eesti"),
		},
	}
	cached := map[string]CachedTerm{
		TermKey("Eesti"): {
			TermKey:        TermKey("Eesti"),
			TranslatedText: "Estonia",
		},
	}

	result := BuildTranslationQueueTerms(fields, cached)

	require.Equal(t, []TranslationTerm{{
		TermKey:              TermKey("Osaühing"),
		SourceText:           "Osaühing",
		SourceTextNormalized: "osaühing",
	}}, result.UncachedTerms)
	require.Len(t, result.CachedBindings, 1)
	require.Equal(t, "company-1", result.CachedBindings[0].CompanyID)
	require.Equal(t, "country_label_en", result.CachedBindings[0].TargetColumn)
	require.Equal(t, "Estonia", result.CachedBindings[0].TranslatedText)
}

func TestBuildTranslationBindingsForResultsMapsResultsBackToCompanies(t *testing.T) {
	fields := []MissingField{
		{
			CompanyID:    "company-1",
			SourceTable:  "source.companies",
			SourceRowID:  "company-1",
			SourceColumn: "legal_form_label",
			TargetColumn: "legal_form_label_en",
			SourceText:   "Osaühing",
			TermKey:      TermKey("Osaühing"),
		},
	}
	results := []TranslationTermResult{{
		TermKey:        TermKey("Osaühing"),
		TranslatedText: "Private limited company",
		Status:         "succeeded",
	}}

	bindings := BuildTranslationBindingsForResults(fields, results)

	require.Len(t, bindings, 1)
	require.Equal(t, "company-1", bindings[0].CompanyID)
	require.Equal(t, "legal_form_label_en", bindings[0].TargetColumn)
	require.Equal(t, "Private limited company", bindings[0].TranslatedText)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/sourcetranslation -run 'TestBuildTranslationQueueTerms|TestBuildTranslationBindingsForResults' -count=1
```

Expected: FAIL because `BuildTranslationQueueTerms` and `BuildTranslationBindingsForResults` do not exist.

- [ ] **Step 3: Add shared helper implementation**

Create `scheduler/internal/sourcetranslation/queue.go`:

```go
package sourcetranslation

import "strings"

type QueueTermBuildResult struct {
	UncachedTerms  []TranslationTerm
	CachedBindings []TranslationBinding
}

func BuildTranslationQueueTerms(fields []MissingField, cached map[string]CachedTerm) QueueTermBuildResult {
	result := QueueTermBuildResult{
		UncachedTerms:  make([]TranslationTerm, 0),
		CachedBindings: make([]TranslationBinding, 0),
	}
	seenUncached := make(map[string]struct{})
	for _, field := range fields {
		field = normalizeMissingFieldForQueue(field)
		if field.CompanyID == "" || field.SourceText == "" || field.TermKey == "" {
			continue
		}
		if cachedTerm := cached[field.TermKey]; strings.TrimSpace(cachedTerm.TranslatedText) != "" {
			result.CachedBindings = append(result.CachedBindings, TranslationBinding{
				CompanyID:      field.CompanyID,
				SourceTable:    field.SourceTable,
				SourceRowID:    field.SourceRowID,
				SourceColumn:   field.SourceColumn,
				TargetColumn:   field.TargetColumn,
				TranslatedText: strings.TrimSpace(cachedTerm.TranslatedText),
			})
			continue
		}
		if _, ok := seenUncached[field.TermKey]; ok {
			continue
		}
		seenUncached[field.TermKey] = struct{}{}
		result.UncachedTerms = append(result.UncachedTerms, TranslationTerm{
			TermKey:              field.TermKey,
			SourceText:           field.SourceText,
			SourceTextNormalized: field.SourceTextNormalized,
		})
	}
	return result
}

func BuildTranslationBindingsForResults(fields []MissingField, results []TranslationTermResult) []TranslationBinding {
	translatedByTerm := make(map[string]string, len(results))
	for _, result := range results {
		if strings.TrimSpace(result.Status) != "succeeded" {
			continue
		}
		translated := strings.TrimSpace(result.TranslatedText)
		if translated == "" {
			continue
		}
		translatedByTerm[strings.TrimSpace(result.TermKey)] = translated
	}
	bindings := make([]TranslationBinding, 0, len(fields))
	for _, field := range fields {
		field = normalizeMissingFieldForQueue(field)
		translated := translatedByTerm[field.TermKey]
		if translated == "" {
			continue
		}
		bindings = append(bindings, TranslationBinding{
			CompanyID:      field.CompanyID,
			SourceTable:    field.SourceTable,
			SourceRowID:    field.SourceRowID,
			SourceColumn:   field.SourceColumn,
			TargetColumn:   field.TargetColumn,
			TranslatedText: translated,
		})
	}
	return bindings
}

func TranslationTermKeys(fields []MissingField) []string {
	keys := make([]string, 0, len(fields))
	seen := make(map[string]struct{})
	for _, field := range fields {
		field = normalizeMissingFieldForQueue(field)
		if field.TermKey == "" {
			continue
		}
		if _, ok := seen[field.TermKey]; ok {
			continue
		}
		seen[field.TermKey] = struct{}{}
		keys = append(keys, field.TermKey)
	}
	return keys
}

func normalizeMissingFieldForQueue(field MissingField) MissingField {
	field.CompanyID = strings.TrimSpace(field.CompanyID)
	field.SourceTable = strings.TrimSpace(field.SourceTable)
	field.SourceRowID = strings.TrimSpace(field.SourceRowID)
	field.SourceColumn = strings.TrimSpace(field.SourceColumn)
	field.TargetColumn = strings.TrimSpace(field.TargetColumn)
	field.SourceText = strings.TrimSpace(field.SourceText)
	field.SourceTextNormalized = strings.TrimSpace(field.SourceTextNormalized)
	field.TermKey = strings.TrimSpace(field.TermKey)
	if field.SourceTextNormalized == "" {
		field.SourceTextNormalized = NormalizeText(field.SourceText)
	}
	if field.TermKey == "" && field.SourceText != "" {
		field.TermKey = TermKey(field.SourceText)
	}
	return field
}
```

- [ ] **Step 4: Run shared helper tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/sourcetranslation -run 'TestBuildTranslationQueueTerms|TestBuildTranslationBindingsForResults' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scheduler/internal/sourcetranslation/queue.go scheduler/internal/sourcetranslation/queue_test.go
git commit -m "feat: add source translation queue helpers"
```

---

### Task 4: Add Companydata Queue APIs

**Files:**
- Create: `scheduler/internal/brreg/companydata/translation_queue.go`
- Create: `scheduler/internal/ariregister/companydata/translation_queue.go`
- Modify: `scheduler/internal/brreg/companydata/workset_test.go`
- Modify: `scheduler/internal/ariregister/companydata/workset_test.go`

- [ ] **Step 1: Write failing BRREG queue API test**

Append to `scheduler/internal/brreg/companydata/workset_test.go`:

```go
func TestStorePreparesAndClaimsBrregTranslationQueue(t *testing.T) {
	tx := testdb.BeginTx(t)
	first := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999555901",
		OrganizationName:      "QUEUE FIRST AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
	})
	second := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999555902",
		OrganizationName:      "QUEUE SECOND AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
	})
	store := New(tx)

	prepared, err := store.PrepareTranslationQueue(t.Context(), PrepareTranslationQueueCommand{
		CompanyIDs: []string{first.CompanyID.String(), second.CompanyID.String()},
		Filters:    map[string]string{"translation_status": "missing"},
		Limit:      2,
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, prepared.CompaniesQueued)

	claimed, err := store.ClaimTranslationQueueBatch(t.Context(), ClaimTranslationQueueBatchCommand{
		BatchID:             "workflow-1:1",
		MaxRequestChars:     1,
		MaxCandidateRows:    10,
		StaleRunningSeconds: 1800,
	})
	require.NoError(t, err)
	require.Len(t, claimed.CompanyIDs, 1)
	require.Contains(t, []string{first.CompanyID.String(), second.CompanyID.String()}, claimed.CompanyIDs[0])
	require.EqualValues(t, 1, claimed.CompaniesClaimed)
	require.Greater(t, claimed.NumOfCharacters, int32(0))
}
```

- [ ] **Step 2: Write failing Ariregister queue API test**

Append to `scheduler/internal/ariregister/companydata/workset_test.go`:

```go
func TestStorePreparesAndClaimsAriregisterTranslationQueue(t *testing.T) {
	tx := testdb.BeginTx(t)
	first := seedAriregisterCompanyData(t, tx, ariregisterCompanySeed{
		RegistryCode:            "999555911",
		LegalName:               "QUEUE FIRST OU",
		RegistrationStatusLabel: "Registrisse kantud",
		LegalFormLabel:          "Osaühing",
		AddressCountryLabel:     "Eesti",
	})
	second := seedAriregisterCompanyData(t, tx, ariregisterCompanySeed{
		RegistryCode:            "999555912",
		LegalName:               "QUEUE SECOND OU",
		RegistrationStatusLabel: "Registrisse kantud",
		LegalFormLabel:          "Osaühing",
		AddressCountryLabel:     "Eesti",
	})
	store := New(tx)
	require.NoError(t, store.RefreshTranslationStatus(t.Context()))

	prepared, err := store.PrepareTranslationQueue(t.Context(), PrepareTranslationQueueCommand{
		CompanyIDs: []string{first.CompanyID.String(), second.CompanyID.String()},
		Filters:    map[string]string{"translation_status": "missing"},
		Limit:      2,
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, prepared.CompaniesQueued)

	claimed, err := store.ClaimTranslationQueueBatch(t.Context(), ClaimTranslationQueueBatchCommand{
		BatchID:             "workflow-1:1",
		MaxRequestChars:     1,
		MaxCandidateRows:    10,
		StaleRunningSeconds: 1800,
	})
	require.NoError(t, err)
	require.Len(t, claimed.CompanyIDs, 1)
	require.Contains(t, []string{first.CompanyID.String(), second.CompanyID.String()}, claimed.CompanyIDs[0])
	require.EqualValues(t, 1, claimed.CompaniesClaimed)
	require.Greater(t, claimed.NumOfCharacters, int32(0))
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
CORPSCOUT_TEST_DATABASE_URL="$DATABASE_URL" go test ./internal/brreg/companydata ./internal/ariregister/companydata -run 'TestStorePreparesAndClaims.*TranslationQueue' -count=1
```

Expected: FAIL because queue command/result types and methods do not exist.

- [ ] **Step 4: Implement BRREG companydata queue API**

Create `scheduler/internal/brreg/companydata/translation_queue.go`:

```go
package companydata

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	"github.com/pulsarpoint/corpscout/scheduler/internal/db"
)

type PrepareTranslationQueueCommand struct {
	CompanyIDs []string
	Filters    map[string]string
	Limit      int32
}

type PrepareTranslationQueueResult struct {
	CompaniesSeen       int32
	CompaniesQueued     int32
	TerminalRowsDeleted int32
}

type ClaimTranslationQueueBatchCommand struct {
	BatchID             string
	MaxRequestChars     int32
	MaxCandidateRows    int32
	StaleRunningSeconds int32
}

type ClaimTranslationQueueBatchResult struct {
	Status           string
	BatchID          string
	CompanyIDs       []string
	CompaniesClaimed int32
	NumOfCharacters  int32
}

type QueueBatchResult struct {
	RowsAffected int32
}

func (s *Store) PrepareTranslationQueue(ctx context.Context, command PrepareTranslationQueueCommand) (PrepareTranslationQueueResult, error) {
	if s == nil || s.pool == nil {
		return PrepareTranslationQueueResult{}, errors.New("brreg companydata database not available")
	}
	if err := s.RefreshTranslationStatus(ctx); err != nil {
		return PrepareTranslationQueueResult{}, err
	}
	ids, err := parseQueueCompanyIDs(command.CompanyIDs)
	if err != nil {
		return PrepareTranslationQueueResult{}, err
	}
	row, err := db.New(s.pool).PrepareBrregTranslationQueue(ctx, db.PrepareBrregTranslationQueueParams{
		CompanyLimit:       int32(command.Limit),
		CompanyIds:         ids,
		Query:              optionalQueueFilter(command.Filters, "query", "q"),
		LifecycleStatus:    optionalQueueFilter(command.Filters, "state", "lifecycle_state", "lifecycle_status"),
		RegistrationStatus: optionalQueueFilter(command.Filters, "registration_status"),
		TranslationStatus:  optionalQueueFilter(command.Filters, "translation_status"),
		WebsiteStatus:      optionalQueueFilter(command.Filters, "website_status"),
	})
	if err != nil {
		return PrepareTranslationQueueResult{}, errors.Wrap(err, "prepare brreg translation queue")
	}
	return PrepareTranslationQueueResult{
		CompaniesSeen:       row.CompaniesSeen,
		CompaniesQueued:     row.CompaniesQueued,
		TerminalRowsDeleted: row.TerminalRowsDeleted,
	}, nil
}

func (s *Store) ClaimTranslationQueueBatch(ctx context.Context, command ClaimTranslationQueueBatchCommand) (ClaimTranslationQueueBatchResult, error) {
	if s == nil || s.pool == nil {
		return ClaimTranslationQueueBatchResult{}, errors.New("brreg companydata database not available")
	}
	command = normalizeClaimTranslationQueueBatchCommand(command)
	queries := db.New(s.pool)
	if _, err := queries.ResetStaleBrregTranslationQueueEntries(ctx, command.StaleRunningSeconds); err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "reset stale brreg translation queue entries")
	}
	running, err := queries.CountRunningBrregTranslationQueueEntries(ctx)
	if err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "count running brreg translation queue entries")
	}
	if running > 0 {
		return ClaimTranslationQueueBatchResult{Status: "blocked"}, nil
	}
	rows, err := queries.ClaimBrregTranslationQueueBatch(ctx, db.ClaimBrregTranslationQueueBatchParams{
		BatchID:          command.BatchID,
		MaxRequestChars:  command.MaxRequestChars,
		MaxCandidateRows: command.MaxCandidateRows,
	})
	if err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "claim brreg translation queue batch")
	}
	if len(rows) == 0 {
		return ClaimTranslationQueueBatchResult{Status: "drained"}, nil
	}
	result := ClaimTranslationQueueBatchResult{
		Status:           "claimed",
		BatchID:          command.BatchID,
		CompanyIDs:       make([]string, 0, len(rows)),
		CompaniesClaimed: int32(len(rows)),
	}
	for _, row := range rows {
		result.CompanyIDs = append(result.CompanyIDs, row.CompanyID.String())
		result.NumOfCharacters += row.NumOfCharacters
	}
	return result, nil
}

func (s *Store) ReleaseTranslationQueueBatch(ctx context.Context, batchID string) (QueueBatchResult, error) {
	row, err := db.New(s.pool).ReleaseBrregTranslationQueueBatch(ctx, strings.TrimSpace(batchID))
	if err != nil {
		return QueueBatchResult{}, errors.Wrap(err, "release brreg translation queue batch")
	}
	return QueueBatchResult{RowsAffected: row}, nil
}

func (s *Store) CompleteTranslationQueueBatch(ctx context.Context, batchID string) (QueueBatchResult, error) {
	row, err := db.New(s.pool).CompleteBrregTranslationQueueBatch(ctx, strings.TrimSpace(batchID))
	if err != nil {
		return QueueBatchResult{}, errors.Wrap(err, "complete brreg translation queue batch")
	}
	return QueueBatchResult{RowsAffected: row}, nil
}

func (s *Store) FailTranslationQueueBatch(ctx context.Context, batchID string) (QueueBatchResult, error) {
	row, err := db.New(s.pool).FailBrregTranslationQueueBatch(ctx, strings.TrimSpace(batchID))
	if err != nil {
		return QueueBatchResult{}, errors.Wrap(err, "fail brreg translation queue batch")
	}
	return QueueBatchResult{RowsAffected: row}, nil
}

func normalizeClaimTranslationQueueBatchCommand(command ClaimTranslationQueueBatchCommand) ClaimTranslationQueueBatchCommand {
	command.BatchID = strings.TrimSpace(command.BatchID)
	if command.MaxRequestChars <= 0 {
		command.MaxRequestChars = 6000
	}
	if command.MaxCandidateRows <= 0 {
		command.MaxCandidateRows = 500
	}
	if command.StaleRunningSeconds <= 0 {
		command.StaleRunningSeconds = 1800
	}
	return command
}

func parseQueueCompanyIDs(values []string) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, 0, len(values))
	for _, value := range normalizedTextValues(values) {
		id, err := uuid.Parse(value)
		if err != nil {
			return nil, errors.Wrapf(err, "parse queue company id %q", value)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func optionalQueueFilter(filters map[string]string, keys ...string) *string {
	value := strings.TrimSpace(textFilterValue(filters, keys...))
	if value == "" {
		return nil
	}
	return &value
}
```

- [ ] **Step 5: Implement Ariregister companydata queue API**

Create `scheduler/internal/ariregister/companydata/translation_queue.go` with the same public types and methods as BRREG, using Ariregister generated queries:

```go
package companydata

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	"github.com/pulsarpoint/corpscout/scheduler/internal/db"
)

type PrepareTranslationQueueCommand struct {
	CompanyIDs []string
	Filters    map[string]string
	Limit      int32
}

type PrepareTranslationQueueResult struct {
	CompaniesSeen       int32
	CompaniesQueued     int32
	TerminalRowsDeleted int32
}

type ClaimTranslationQueueBatchCommand struct {
	BatchID             string
	MaxRequestChars     int32
	MaxCandidateRows    int32
	StaleRunningSeconds int32
}

type ClaimTranslationQueueBatchResult struct {
	Status           string
	BatchID          string
	CompanyIDs       []string
	CompaniesClaimed int32
	NumOfCharacters  int32
}

type QueueBatchResult struct {
	RowsAffected int32
}

func (s *Store) PrepareTranslationQueue(ctx context.Context, command PrepareTranslationQueueCommand) (PrepareTranslationQueueResult, error) {
	if s == nil || s.pool == nil {
		return PrepareTranslationQueueResult{}, errors.New("ariregister companydata database not available")
	}
	if err := s.RefreshTranslationStatus(ctx); err != nil {
		return PrepareTranslationQueueResult{}, err
	}
	ids, err := parseQueueCompanyIDs(command.CompanyIDs)
	if err != nil {
		return PrepareTranslationQueueResult{}, err
	}
	row, err := db.New(s.pool).PrepareAriregisterTranslationQueue(ctx, db.PrepareAriregisterTranslationQueueParams{
		CompanyLimit:       int32(command.Limit),
		CompanyIds:         ids,
		Query:              optionalQueueFilter(command.Filters, "query", "q"),
		LifecycleStatus:    optionalQueueFilter(command.Filters, "state", "lifecycle_state", "lifecycle_status"),
		RegistrationStatus: optionalQueueFilter(command.Filters, "registration_status"),
		TranslationStatus:  optionalQueueFilter(command.Filters, "translation_status"),
		WebsiteStatus:      optionalQueueFilter(command.Filters, "website_status"),
	})
	if err != nil {
		return PrepareTranslationQueueResult{}, errors.Wrap(err, "prepare ariregister translation queue")
	}
	return PrepareTranslationQueueResult{
		CompaniesSeen:       row.CompaniesSeen,
		CompaniesQueued:     row.CompaniesQueued,
		TerminalRowsDeleted: row.TerminalRowsDeleted,
	}, nil
}

func (s *Store) ClaimTranslationQueueBatch(ctx context.Context, command ClaimTranslationQueueBatchCommand) (ClaimTranslationQueueBatchResult, error) {
	if s == nil || s.pool == nil {
		return ClaimTranslationQueueBatchResult{}, errors.New("ariregister companydata database not available")
	}
	command = normalizeClaimTranslationQueueBatchCommand(command)
	queries := db.New(s.pool)
	if _, err := queries.ResetStaleAriregisterTranslationQueueEntries(ctx, command.StaleRunningSeconds); err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "reset stale ariregister translation queue entries")
	}
	running, err := queries.CountRunningAriregisterTranslationQueueEntries(ctx)
	if err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "count running ariregister translation queue entries")
	}
	if running > 0 {
		return ClaimTranslationQueueBatchResult{Status: "blocked"}, nil
	}
	rows, err := queries.ClaimAriregisterTranslationQueueBatch(ctx, db.ClaimAriregisterTranslationQueueBatchParams{
		BatchID:          command.BatchID,
		MaxRequestChars:  command.MaxRequestChars,
		MaxCandidateRows: command.MaxCandidateRows,
	})
	if err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "claim ariregister translation queue batch")
	}
	if len(rows) == 0 {
		return ClaimTranslationQueueBatchResult{Status: "drained"}, nil
	}
	result := ClaimTranslationQueueBatchResult{
		Status:           "claimed",
		BatchID:          command.BatchID,
		CompanyIDs:       make([]string, 0, len(rows)),
		CompaniesClaimed: int32(len(rows)),
	}
	for _, row := range rows {
		result.CompanyIDs = append(result.CompanyIDs, row.CompanyID.String())
		result.NumOfCharacters += row.NumOfCharacters
	}
	return result, nil
}

func (s *Store) ReleaseTranslationQueueBatch(ctx context.Context, batchID string) (QueueBatchResult, error) {
	row, err := db.New(s.pool).ReleaseAriregisterTranslationQueueBatch(ctx, strings.TrimSpace(batchID))
	if err != nil {
		return QueueBatchResult{}, errors.Wrap(err, "release ariregister translation queue batch")
	}
	return QueueBatchResult{RowsAffected: row}, nil
}

func (s *Store) CompleteTranslationQueueBatch(ctx context.Context, batchID string) (QueueBatchResult, error) {
	row, err := db.New(s.pool).CompleteAriregisterTranslationQueueBatch(ctx, strings.TrimSpace(batchID))
	if err != nil {
		return QueueBatchResult{}, errors.Wrap(err, "complete ariregister translation queue batch")
	}
	return QueueBatchResult{RowsAffected: row}, nil
}

func (s *Store) FailTranslationQueueBatch(ctx context.Context, batchID string) (QueueBatchResult, error) {
	row, err := db.New(s.pool).FailAriregisterTranslationQueueBatch(ctx, strings.TrimSpace(batchID))
	if err != nil {
		return QueueBatchResult{}, errors.Wrap(err, "fail ariregister translation queue batch")
	}
	return QueueBatchResult{RowsAffected: row}, nil
}

func normalizeClaimTranslationQueueBatchCommand(command ClaimTranslationQueueBatchCommand) ClaimTranslationQueueBatchCommand {
	command.BatchID = strings.TrimSpace(command.BatchID)
	if command.MaxRequestChars <= 0 {
		command.MaxRequestChars = 6000
	}
	if command.MaxCandidateRows <= 0 {
		command.MaxCandidateRows = 500
	}
	if command.StaleRunningSeconds <= 0 {
		command.StaleRunningSeconds = 1800
	}
	return command
}

func parseQueueCompanyIDs(values []string) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, 0, len(values))
	for _, value := range normalizedTextValues(values) {
		id, err := uuid.Parse(value)
		if err != nil {
			return nil, errors.Wrapf(err, "parse queue company id %q", value)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func optionalQueueFilter(filters map[string]string, keys ...string) *string {
	value := strings.TrimSpace(textFilterValue(filters, keys...))
	if value == "" {
		return nil
	}
	return &value
}
```

- [ ] **Step 6: Run companydata queue tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
CORPSCOUT_TEST_DATABASE_URL="$DATABASE_URL" go test ./internal/brreg/companydata ./internal/ariregister/companydata -run 'TestStorePreparesAndClaims.*TranslationQueue' -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add scheduler/internal/brreg/companydata scheduler/internal/ariregister/companydata
git commit -m "feat: add company translation queue APIs"
```

---

### Task 5: Replace Translation Activities With Queue Activities

**Files:**
- Modify: `scheduler/internal/brreg/actions/company_translation_workset_actions.go`
- Modify: `scheduler/internal/ariregister/actions/company_translation_workset_actions.go`

- [ ] **Step 1: Add queue action types**

In each source action file, add source-specific aliases around the companydata queue types:

```go
type PrepareBrregTranslationQueueInput struct {
	IDs     []string          `json:"ids,omitempty"`
	Filters map[string]string `json:"filters,omitempty"`
	Limit   int32             `json:"limit,omitempty"`
}

type PrepareBrregTranslationQueueResult = companydata.PrepareTranslationQueueResult

type ClaimBrregTranslationQueueBatchInput struct {
	BatchID             string `json:"batch_id"`
	MaxRequestChars     int32  `json:"max_request_chars,omitempty"`
	MaxCandidateRows    int32  `json:"max_candidate_rows,omitempty"`
	StaleRunningSeconds int32  `json:"stale_running_seconds,omitempty"`
}

type ClaimBrregTranslationQueueBatchResult = companydata.ClaimTranslationQueueBatchResult

type ReleaseBrregTranslationQueueBatchInput struct {
	BatchID string `json:"batch_id"`
}

type ReleaseBrregTranslationQueueBatchResult = companydata.QueueBatchResult

type CompleteBrregTranslationQueueBatchInput struct {
	BatchID string `json:"batch_id"`
}

type CompleteBrregTranslationQueueBatchResult = companydata.QueueBatchResult
```

Use Ariregister names in the Ariregister file:

```go
type PrepareAriregisterTranslationQueueInput struct {
	IDs     []string          `json:"ids,omitempty"`
	Filters map[string]string `json:"filters,omitempty"`
	Limit   int32             `json:"limit,omitempty"`
}
```

- [ ] **Step 2: Implement prepare, claim, release, and complete actions**

In BRREG actions, add:

```go
func (a *CompanyTranslationActions) PrepareBrregTranslationQueue(
	ctx context.Context,
	input PrepareBrregTranslationQueueInput,
) (PrepareBrregTranslationQueueResult, error) {
	if a == nil || a.store == nil {
		return PrepareBrregTranslationQueueResult{}, errors.New("brreg companydata store not available")
	}
	result, err := a.store.PrepareTranslationQueue(ctx, companydata.PrepareTranslationQueueCommand{
		CompanyIDs: input.IDs,
		Filters:    input.Filters,
		Limit:      input.Limit,
	})
	if err != nil {
		return PrepareBrregTranslationQueueResult{}, errors.Wrap(err, "prepare brreg translation queue")
	}
	return result, nil
}

func (a *CompanyTranslationActions) ClaimBrregTranslationQueueBatch(
	ctx context.Context,
	input ClaimBrregTranslationQueueBatchInput,
) (ClaimBrregTranslationQueueBatchResult, error) {
	if a == nil || a.store == nil {
		return ClaimBrregTranslationQueueBatchResult{}, errors.New("brreg companydata store not available")
	}
	result, err := a.store.ClaimTranslationQueueBatch(ctx, companydata.ClaimTranslationQueueBatchCommand{
		BatchID:             input.BatchID,
		MaxRequestChars:     input.MaxRequestChars,
		MaxCandidateRows:    input.MaxCandidateRows,
		StaleRunningSeconds: input.StaleRunningSeconds,
	})
	if err != nil {
		return ClaimBrregTranslationQueueBatchResult{}, errors.Wrap(err, "claim brreg translation queue batch")
	}
	return result, nil
}

func (a *CompanyTranslationActions) ReleaseBrregTranslationQueueBatch(
	ctx context.Context,
	input ReleaseBrregTranslationQueueBatchInput,
) (ReleaseBrregTranslationQueueBatchResult, error) {
	if a == nil || a.store == nil {
		return ReleaseBrregTranslationQueueBatchResult{}, errors.New("brreg companydata store not available")
	}
	return a.store.ReleaseTranslationQueueBatch(ctx, input.BatchID)
}

func (a *CompanyTranslationActions) CompleteBrregTranslationQueueBatch(
	ctx context.Context,
	input CompleteBrregTranslationQueueBatchInput,
) (CompleteBrregTranslationQueueBatchResult, error) {
	if a == nil || a.store == nil {
		return CompleteBrregTranslationQueueBatchResult{}, errors.New("brreg companydata store not available")
	}
	return a.store.CompleteTranslationQueueBatch(ctx, input.BatchID)
}
```

Repeat the same implementation in Ariregister with Ariregister type/function/error names.

- [ ] **Step 3: Modify translate batch activity to load company fields by claimed company IDs**

Change each `Translate*TranslationWorksetBatchInput` to include:

```go
CompanyIDs []string `json:"company_ids"`
```

Inside `TranslateBrregTranslationWorksetBatch`, replace use of `input.Terms` with this sequence:

```go
fields, err := a.store.LoadMissingTranslationFields(ctx, sourcetranslation.LoadMissingFieldsCommand{
	PromptVersion: input.PromptVersion,
	CompanyIDs:    input.CompanyIDs,
})
if err != nil {
	return TranslateBrregTranslationWorksetBatchResult{}, errors.Wrap(err, "load brreg translation queue fields")
}
termKeys := sourcetranslation.TranslationTermKeys(fields)
cachedTerms, err := a.store.LoadCachedTranslationTerms(ctx, sourcetranslation.LoadCachedTermsCommand{
	PromptVersion: input.PromptVersion,
	TermKeys:      termKeys,
})
if err != nil {
	return TranslateBrregTranslationWorksetBatchResult{}, errors.Wrap(err, "load brreg cached queue terms")
}
built := sourcetranslation.BuildTranslationQueueTerms(fields, cachedTerms)
for _, bindingGroup := range groupQueueBindingsByCompany(built.CachedBindings) {
	if _, err := a.store.ApplyCompanyTranslations(ctx, sourcetranslation.ApplyCompanyTranslationsCommand{
		CompanyID: bindingGroup.CompanyID,
		Bindings:  bindingGroup.Bindings,
	}); err != nil {
		return TranslateBrregTranslationWorksetBatchResult{}, errors.Wrap(err, "apply cached brreg queue translations")
	}
}
```

If `built.UncachedTerms` is empty, return an empty successful result and let the workflow complete the queue batch.

If there are uncached terms, send them to NATS exactly as the current activity does, save successful/failed terms through `a.store.SaveTranslationTerms`, build bindings with `sourcetranslation.BuildTranslationBindingsForResults`, and apply bindings grouped by company.

- [ ] **Step 4: Add local grouping helper**

Add in each action file:

```go
type queueBindingGroup struct {
	CompanyID string
	Bindings  []sourcetranslation.TranslationBinding
}

func groupQueueBindingsByCompany(bindings []sourcetranslation.TranslationBinding) []queueBindingGroup {
	ordered := make([]queueBindingGroup, 0)
	indexByCompany := make(map[string]int)
	for _, binding := range bindings {
		companyID := strings.TrimSpace(binding.CompanyID)
		if companyID == "" {
			continue
		}
		index, ok := indexByCompany[companyID]
		if !ok {
			index = len(ordered)
			indexByCompany[companyID] = index
			ordered = append(ordered, queueBindingGroup{CompanyID: companyID})
		}
		ordered[index].Bindings = append(ordered[index].Bindings, binding)
	}
	return ordered
}
```

Import `strings` and `github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation`.

- [ ] **Step 5: Run focused action package tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/brreg/actions ./internal/ariregister/actions -count=1
```

Expected: PASS or no test files.

- [ ] **Step 6: Commit**

```bash
git add scheduler/internal/brreg/actions/company_translation_workset_actions.go scheduler/internal/ariregister/actions/company_translation_workset_actions.go
git commit -m "feat: add translation queue activities"
```

---

### Task 6: Change Temporal Workflows To Prepare And Drain Postgres Queue

**Files:**
- Modify: `scheduler/internal/brreg/workflow/company_translation.go`
- Modify: `scheduler/internal/ariregister/workflow/company_translation.go`
- Modify: `scheduler/internal/brreg/workflow/company_translation_test.go`
- Modify: `scheduler/internal/ariregister/workflow/company_translation_test.go`

- [ ] **Step 1: Add failing workflow test for queue preparation as a separate action**

In BRREG workflow tests, add:

```go
func TestTranslateBrregSourceCompaniesPreparesQueueBeforeClaiming(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateBrregSourceCompanies)
	events := make([]string, 0)

	env.RegisterActivityWithOptions(func(input PrepareBrregTranslationQueueInput) (PrepareBrregTranslationQueueResult, error) {
		events = append(events, "prepare")
		return PrepareBrregTranslationQueueResult{CompaniesQueued: 1}, nil
	}, activity.RegisterOptions{Name: prepareBrregTranslationQueueActivity})
	env.RegisterActivityWithOptions(func(input ClaimBrregTranslationQueueBatchInput) (ClaimBrregTranslationQueueBatchResult, error) {
		events = append(events, "claim")
		return ClaimBrregTranslationQueueBatchResult{Status: "drained"}, nil
	}, activity.RegisterOptions{Name: claimBrregTranslationQueueBatchActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{"prepare", "claim"}, events)
}
```

Add the same test in Ariregister with Ariregister names.

- [ ] **Step 2: Add failing workflow test for blocked queue sleep**

In BRREG workflow tests, add:

```go
func TestTranslateBrregSourceCompaniesSleepsWhenQueueHasRunningBatch(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateBrregSourceCompanies)

	env.RegisterActivityWithOptions(func(PrepareBrregTranslationQueueInput) (PrepareBrregTranslationQueueResult, error) {
		return PrepareBrregTranslationQueueResult{CompaniesQueued: 2}, nil
	}, activity.RegisterOptions{Name: prepareBrregTranslationQueueActivity})

	var claimTimes []time.Time
	var claimCalls int
	env.RegisterActivityWithOptions(func(ClaimBrregTranslationQueueBatchInput) (ClaimBrregTranslationQueueBatchResult, error) {
		claimTimes = append(claimTimes, env.Now())
		claimCalls++
		if claimCalls == 1 {
			return ClaimBrregTranslationQueueBatchResult{Status: "blocked"}, nil
		}
		return ClaimBrregTranslationQueueBatchResult{Status: "drained"}, nil
	}, activity.RegisterOptions{Name: claimBrregTranslationQueueBatchActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{
		BatchDelaySeconds: 5,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, claimTimes, 2)
	require.GreaterOrEqual(t, claimTimes[1].Sub(claimTimes[0]), 5*time.Second)
}
```

Add the same test in Ariregister with Ariregister names.

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/brreg/workflow ./internal/ariregister/workflow -run 'PreparesQueue|SleepsWhenQueueHasRunningBatch' -count=1
```

Expected: FAIL because queue workflow activity constants and loop do not exist.

- [ ] **Step 4: Add workflow activity constants and type aliases**

In BRREG workflow file, add:

```go
prepareBrregTranslationQueueActivity       = "PrepareBrregTranslationQueue"
claimBrregTranslationQueueBatchActivity    = "ClaimBrregTranslationQueueBatch"
releaseBrregTranslationQueueBatchActivity  = "ReleaseBrregTranslationQueueBatch"
completeBrregTranslationQueueBatchActivity = "CompleteBrregTranslationQueueBatch"
```

Add type aliases:

```go
type PrepareBrregTranslationQueueInput = actions.PrepareBrregTranslationQueueInput
type PrepareBrregTranslationQueueResult = actions.PrepareBrregTranslationQueueResult
type ClaimBrregTranslationQueueBatchInput = actions.ClaimBrregTranslationQueueBatchInput
type ClaimBrregTranslationQueueBatchResult = actions.ClaimBrregTranslationQueueBatchResult
type ReleaseBrregTranslationQueueBatchInput = actions.ReleaseBrregTranslationQueueBatchInput
type ReleaseBrregTranslationQueueBatchResult = actions.ReleaseBrregTranslationQueueBatchResult
type CompleteBrregTranslationQueueBatchInput = actions.CompleteBrregTranslationQueueBatchInput
type CompleteBrregTranslationQueueBatchResult = actions.CompleteBrregTranslationQueueBatchResult
```

Repeat with Ariregister names.

- [ ] **Step 5: Add carried queue prepared state**

Add to both workflow input structs:

```go
QueuePrepared bool  `json:"queue_prepared,omitempty"`
QueueBatchID  int32 `json:"queue_batch_id,omitempty"`
```

- [ ] **Step 6: Replace initial workset build with queue preparation**

In each workflow, replace the `Build*TranslationWorkset` block with:

```go
if !input.QueuePrepared {
	companyLimit := int32(input.BatchSize)
	if input.AllRecords {
		companyLimit = int32(input.MaxCompaniesPerBatch)
	} else if input.Limit > 0 {
		companyLimit = int32(input.Limit)
	}
	var prepared PrepareBrregTranslationQueueResult
	if err := temporalworkflow.ExecuteActivity(ctx, prepareBrregTranslationQueueActivity, PrepareBrregTranslationQueueInput{
		IDs:     input.IDs,
		Filters: input.Filters,
		Limit:   companyLimit,
	}).Get(ctx, &prepared); err != nil {
		return result, errors.Wrap(err, "prepare brreg translation queue")
	}
	result.CompaniesClaimed += prepared.CompaniesQueued
	if prepared.CompaniesQueued == 0 {
		result.Status = "drained"
		return result, nil
	}
	input.QueuePrepared = true
	input.CarriedCompaniesClaimed = result.CompaniesClaimed
}
```

Use Ariregister names and error text in the Ariregister workflow.

- [ ] **Step 7: Replace claim/translate/save/apply loop with queue draining loop**

Use this shape in BRREG:

```go
for {
	input.QueueBatchID++
	batchID := workflowInfo.WorkflowExecution.ID + ":" + strconv.FormatInt(int64(input.QueueBatchID), 10)
	var claimed ClaimBrregTranslationQueueBatchResult
	if err := temporalworkflow.ExecuteActivity(ctx, claimBrregTranslationQueueBatchActivity, ClaimBrregTranslationQueueBatchInput{
		BatchID:             batchID,
		MaxRequestChars:     int32(input.MaxRequestChars),
		MaxCandidateRows:    int32(input.MaxCompaniesPerBatch),
		StaleRunningSeconds: int32(input.LeaseSeconds),
	}).Get(ctx, &claimed); err != nil {
		return result, errors.Wrap(err, "claim brreg translation queue batch")
	}
	if claimed.Status == "blocked" {
		if err := temporalworkflow.Sleep(ctx, time.Duration(input.BatchDelaySeconds)*time.Second); err != nil {
			return result, errors.Wrap(err, "wait for brreg translation queue batch slot")
		}
		continue
	}
	if claimed.Status == "drained" || len(claimed.CompanyIDs) == 0 {
		break
	}
	result.BatchesProcessed++
	result.RequestCharsClaimed += claimed.NumOfCharacters

	var translated TranslateBrregTranslationWorksetBatchResult
	if err := temporalworkflow.ExecuteActivity(ctx, translateBrregTranslationWorksetBatchActivity, TranslateBrregTranslationWorksetBatchInput{
		BatchID:       0,
		BatchKey:      claimed.BatchID,
		CompanyIDs:    claimed.CompanyIDs,
		Provider:      input.Provider,
		Model:         input.Model,
		PromptVersion: input.PromptVersion,
	}).Get(ctx, &translated); err != nil {
		var released ReleaseBrregTranslationQueueBatchResult
		_ = temporalworkflow.ExecuteActivity(ctx, releaseBrregTranslationQueueBatchActivity, ReleaseBrregTranslationQueueBatchInput{
			BatchID: claimed.BatchID,
		}).Get(ctx, &released)
		if sleepErr := temporalworkflow.Sleep(ctx, time.Duration(input.BatchDelaySeconds)*time.Second); sleepErr != nil {
			return result, errors.Wrap(sleepErr, "wait after failed brreg translation queue batch")
		}
		continue
	}
	result.TermsSucceeded += int32(len(translated.Results))

	var completed CompleteBrregTranslationQueueBatchResult
	if err := temporalworkflow.ExecuteActivity(ctx, completeBrregTranslationQueueBatchActivity, CompleteBrregTranslationQueueBatchInput{
		BatchID: claimed.BatchID,
	}).Get(ctx, &completed); err != nil {
		return result, errors.Wrap(err, "complete brreg translation queue batch")
	}
	if shouldContinueAsNewAfterBatch(input, result.BatchesProcessed) {
		input.CarriedCompaniesClaimed = result.CompaniesClaimed
		input.CarriedRequestChars = result.RequestCharsClaimed
		input.CarriedTermsSucceeded = result.TermsSucceeded
		input.CarriedTermsFailed = result.TermsFailed
		input.CarriedBatchesProcessed = result.BatchesProcessed
		return result, temporalworkflow.NewContinueAsNewError(ctx, TranslateBrregSourceCompanies, input)
	}
}
```

Add `BatchKey string` to `TranslateBrregTranslationWorksetBatchInput` and `TranslateAriregisterTranslationWorksetBatchInput` in the action types. Use `BatchKey` for logs. Keep the old numeric `BatchID` only if existing tests need it during transition.

- [ ] **Step 8: Run focused workflow tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/brreg/workflow ./internal/ariregister/workflow -run 'PreparesQueue|SleepsWhenQueueHasRunningBatch' -count=1
```

Expected: PASS.

- [ ] **Step 9: Run full workflow tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/brreg/workflow ./internal/ariregister/workflow -count=1
```

Expected: PASS after adapting older SQLite workset tests to the new queue semantics.

- [ ] **Step 10: Commit**

```bash
git add scheduler/internal/brreg/workflow scheduler/internal/ariregister/workflow
git commit -m "feat: drain source translation queue in workflows"
```

---

### Task 7: Register New Temporal Activities

**Files:**
- Modify: `scheduler/internal/app/brreg_company_translation_temporal.go`
- Modify: `scheduler/internal/app/ariregister_company_translation_temporal.go`
- Modify: `scheduler/internal/app/temporal_test.go`

- [ ] **Step 1: Register BRREG queue activities**

In `scheduler/internal/app/brreg_company_translation_temporal.go`, register:

```go
w.RegisterActivityWithOptions(
	resources.companyTranslation.PrepareBrregTranslationQueue,
	activity.RegisterOptions{Name: "PrepareBrregTranslationQueue"},
)
w.RegisterActivityWithOptions(
	resources.companyTranslation.ClaimBrregTranslationQueueBatch,
	activity.RegisterOptions{Name: "ClaimBrregTranslationQueueBatch"},
)
w.RegisterActivityWithOptions(
	resources.companyTranslation.ReleaseBrregTranslationQueueBatch,
	activity.RegisterOptions{Name: "ReleaseBrregTranslationQueueBatch"},
)
w.RegisterActivityWithOptions(
	resources.companyTranslation.CompleteBrregTranslationQueueBatch,
	activity.RegisterOptions{Name: "CompleteBrregTranslationQueueBatch"},
)
```

- [ ] **Step 2: Register Ariregister queue activities**

In `scheduler/internal/app/ariregister_company_translation_temporal.go`, register:

```go
w.RegisterActivityWithOptions(
	resources.ariregisterCompanyTranslation.PrepareAriregisterTranslationQueue,
	activity.RegisterOptions{Name: "PrepareAriregisterTranslationQueue"},
)
w.RegisterActivityWithOptions(
	resources.ariregisterCompanyTranslation.ClaimAriregisterTranslationQueueBatch,
	activity.RegisterOptions{Name: "ClaimAriregisterTranslationQueueBatch"},
)
w.RegisterActivityWithOptions(
	resources.ariregisterCompanyTranslation.ReleaseAriregisterTranslationQueueBatch,
	activity.RegisterOptions{Name: "ReleaseAriregisterTranslationQueueBatch"},
)
w.RegisterActivityWithOptions(
	resources.ariregisterCompanyTranslation.CompleteAriregisterTranslationQueueBatch,
	activity.RegisterOptions{Name: "CompleteAriregisterTranslationQueueBatch"},
)
```

- [ ] **Step 3: Run app Temporal tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/app -run Temporal -count=1
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add scheduler/internal/app/brreg_company_translation_temporal.go scheduler/internal/app/ariregister_company_translation_temporal.go scheduler/internal/app/temporal_test.go
git commit -m "feat: register translation queue activities"
```

---

### Task 8: End-To-End Verification

**Files:**
- Verify only unless test failures require targeted edits.

- [ ] **Step 1: Run backend unit tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/sourcetranslation ./internal/brreg/companydata ./internal/ariregister/companydata ./internal/brreg/actions ./internal/ariregister/actions ./internal/brreg/workflow ./internal/ariregister/workflow ./internal/app ./internal/httpapi -count=1
```

Expected: PASS.

- [ ] **Step 2: Run real database tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
CORPSCOUT_TEST_DATABASE_URL="$DATABASE_URL" go test ./internal/brreg/companydata ./internal/ariregister/companydata -count=1
```

Expected: PASS.

- [ ] **Step 3: Run sqlc generation check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/database
sqlc generate
cd ..
git diff --exit-code -- scheduler/internal/db/gen
```

Expected: exit code `0`; generated files are already committed and unchanged.

- [ ] **Step 4: Run UI typecheck**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
pnpm typecheck
```

Expected: PASS. The queue change should not require UI changes.

- [ ] **Step 5: Run whitespace check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git diff --check
```

Expected: no output.

- [ ] **Step 6: Manual local workflow check**

Start the stack after migrations are applied:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
docker compose up -d --build postgres scheduler ui
```

Trigger Ariregister translation from `http://localhost:8094/sources/ariregister/source_entries`.

Check queue state:

```sql
SELECT status, count(*)
FROM ariregister_source.translation_queue_entries
GROUP BY status
ORDER BY status;
```

Expected during processing: at most one group of `running` rows has a non-null `batch_id`; pending rows drain over time; rows move to `succeeded`.

- [ ] **Step 7: Inspect final status**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git status --short
```

Expected: only the files intentionally changed by this plan are listed. If verification required targeted fixes, review those diffs before deciding whether to commit them with the related task or a final `test: verify source translation queue` commit.

---

## Self-Review

- Spec coverage: the plan creates a Postgres company-level queue, prepares it as a separate Temporal action, claims batches by summed `num_of_characters`, stores `batch_id` on queue entries, throttles by checking `running` rows, resets stale running rows by `status_changed_at`, and retries LLM failures by releasing the batch to `pending` and sleeping.
- Placeholder scan: the plan contains no implementation placeholders; code snippets include concrete table definitions, queries, API shapes, workflow loop shape, and commands.
- Type consistency: queue status names are `pending`, `running`, `succeeded`, and `failed`; queue APIs use the same command/result names in BRREG and Ariregister companydata packages; workflow action names mirror those APIs.
