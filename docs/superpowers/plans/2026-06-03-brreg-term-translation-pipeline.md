# BRREG Term Translation Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace BRREG row-level translation tasks with a term-based pipeline that translates each distinct source term once, caches it, and applies it to every missing `_en` field.

**Architecture:** Corpscout owns database state and target writes. PostgreSQL exposes a whitelisted missing-fields view and term cache; Temporal coordinates publishing missing terms to NATS; the translation service consumes term batches and publishes term results; a Corpscout result consumer upserts cache rows and applies successful terms to source tables. The existing `translate_field` action-task workflow stays in place until the new pipeline is verified, then BRREG UI/actions switch to the term workflow.

**Tech Stack:** Go, PostgreSQL/sqlc, Temporal Go SDK, NATS JetStream, Python translation-service, React Router UI.

---

## File Structure

### Database
- Create `corpscout/database/migrations/000079_brreg_source_translation_terms.up.sql`
- Create `corpscout/database/migrations/000079_brreg_source_translation_terms.down.sql`
- Modify `corpscout/database/queries/brreg_source_profile.sql`
- Regenerate `corpscout/scheduler/internal/db/gen/brreg_source_profile.sql.go`
- Regenerate `corpscout/scheduler/internal/db/gen/querier.go`
- Add migration tests in `corpscout/scheduler/internal/db/brreg_source_translation_terms_migration_test.go`

### Scheduler BRREG DB Boundary
- Create `corpscout/scheduler/internal/brreg/db/term_translation.go`
- Create `corpscout/scheduler/internal/brreg/db/term_translation_test.go`
- Modify `corpscout/scheduler/internal/brreg/db/types.go`

### Scheduler NATS Client
- Create `corpscout/scheduler/internal/translationclient/terms.go`
- Create `corpscout/scheduler/internal/translationclient/terms_test.go`

### Scheduler BRREG Actions and Workflow
- Create `corpscout/scheduler/internal/brreg/actions/term_translation_actions.go`
- Create `corpscout/scheduler/internal/brreg/actions/term_translation_actions_test.go`
- Create `corpscout/scheduler/internal/brreg/workflow/term_translation.go`
- Create `corpscout/scheduler/internal/brreg/workflow/term_translation_test.go`
- Create `corpscout/scheduler/internal/app/brreg_term_translation_temporal.go`
- Modify `corpscout/scheduler/internal/app/temporal.go`
- Modify `corpscout/scheduler/internal/app/temporal_test.go`

### Scheduler Result Consumer
- Create `corpscout/scheduler/internal/brreg/actions/term_translation_consumer.go`
- Create `corpscout/scheduler/internal/brreg/actions/term_translation_consumer_test.go`
- Modify `corpscout/scheduler/internal/app/server.go`
- Modify `corpscout/scheduler/internal/config/config.go`
- Modify `corpscout/scheduler/internal/config/config_test.go`

### Scheduler HTTP/UI
- Modify `corpscout/scheduler/internal/httpapi/workflow_triggers.go`
- Modify `corpscout/scheduler/internal/httpapi/workflow_triggers_test.go`
- Modify `corpscout/scheduler/internal/httpapi/handlers.go`
- Modify `corpscout/scheduler/internal/httpapi/brreg_source_entries.go`
- Modify `corpscout/scheduler/internal/httpapi/brreg_source_entries_test.go`
- Modify `corpscout/ui/app/lib/api.ts`
- Modify `corpscout/ui/app/types/api.ts`
- Modify `corpscout/ui/app/components/app/BrregTranslationActionForm.tsx`
- Modify `corpscout/ui/app/components/app/BrregSourceEntryActionSheet.tsx`
- Modify `corpscout/ui/app/components/app/BrregSourceEntriesTable.tsx`

### Translation Service
- Modify `data-pipelines/services/translation-service/src/corpscout_translation_service/models.py`
- Modify `data-pipelines/services/translation-service/src/corpscout_translation_service/service.py`
- Modify `data-pipelines/services/translation-service/src/corpscout_translation_service/nats_worker.py`
- Modify `data-pipelines/services/translation-service/src/corpscout_translation_service/api.py`
- Add `data-pipelines/services/translation-service/tests/test_term_translation.py`
- Modify `data-pipelines/services/translation-service/tests/test_nats_worker.py`

---

## Message Contracts

### NATS Subjects

Use these constants in both Go and Python:

```text
brreg.translation.terms.request
brreg.translation.terms.result
```

### Request Payload

```json
{
  "request_id": "uuid",
  "source": "brreg",
  "source_lang": "no",
  "target_lang": "en",
  "provider": "default",
  "model": "",
  "prompt_version": "v1",
  "terms": [
    {
      "term_key": "sha256-normalized-key",
      "source_text": "Aksjeselskap",
      "source_text_normalized": "aksjeselskap"
    }
  ]
}
```

### Result Payload

```json
{
  "request_id": "uuid",
  "source": "brreg",
  "source_lang": "no",
  "target_lang": "en",
  "provider": "default",
  "model": "qwen3:6b",
  "prompt_version": "v1",
  "results": [
    {
      "term_key": "sha256-normalized-key",
      "source_text": "Aksjeselskap",
      "source_text_normalized": "aksjeselskap",
      "translated_text": "Limited liability company",
      "status": "succeeded"
    }
  ],
  "failures": [
    {
      "term_key": "sha256-normalized-key",
      "source_text": "Ugyldig verdi",
      "source_text_normalized": "ugyldig verdi",
      "status": "failed_retryable",
      "error_code": "llm_request_failed",
      "error": "request timed out"
    }
  ]
}
```

Corpscout ignores unknown target write instructions in messages because target writes are derived only from `brreg_source.v_missing_translation_fields`.

---

### Task 1: Add Term Cache Table and Missing Field View

**Files:**
- Create `corpscout/database/migrations/000079_brreg_source_translation_terms.up.sql`
- Create `corpscout/database/migrations/000079_brreg_source_translation_terms.down.sql`
- Test `corpscout/scheduler/internal/db/brreg_source_translation_terms_migration_test.go`

- [x] **Step 1: Write migration test for table, indexes, and view**

Create `corpscout/scheduler/internal/db/brreg_source_translation_terms_migration_test.go`:

```go
package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestBRREGSourceTranslationTermsMigration(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()

	_, err := tx.Exec(ctx, `
INSERT INTO brreg_source.translation_terms (
  source,
  source_lang,
  target_lang,
  source_text_normalized,
  source_text,
  term_key,
  status
) VALUES (
  'brreg',
  'no',
  'en',
  'aksjeselskap',
  'Aksjeselskap',
  repeat('a', 64),
  'pending'
)`)
	require.NoError(t, err)

	var viewExists bool
	err = tx.QueryRow(ctx, `
SELECT to_regclass('brreg_source.v_missing_translation_fields') IS NOT NULL
`).Scan(&viewExists)
	require.NoError(t, err)
	require.True(t, viewExists)
}
```

- [x] **Step 2: Run migration test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestBRREGSourceTranslationTermsMigration -count=1
```

Expected: FAIL because `brreg_source.translation_terms` does not exist.

- [x] **Step 3: Add migration up file**

Create `corpscout/database/migrations/000079_brreg_source_translation_terms.up.sql`:

```sql
CREATE TABLE brreg_source.translation_terms (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source TEXT NOT NULL DEFAULT 'brreg',
  source_lang TEXT NOT NULL,
  target_lang TEXT NOT NULL,
  source_text_normalized TEXT NOT NULL,
  source_text TEXT NOT NULL,
  term_key TEXT NOT NULL,
  translated_text TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  provider TEXT,
  model TEXT,
  prompt_version TEXT NOT NULL DEFAULT 'v1',
  error TEXT,
  error_code TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  translated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_brreg_source_translation_terms_source CHECK (source = 'brreg'),
  CONSTRAINT chk_brreg_source_translation_terms_status CHECK (
    status IN ('pending', 'queued', 'succeeded', 'failed_retryable', 'failed_terminal')
  ),
  CONSTRAINT chk_brreg_source_translation_terms_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  CONSTRAINT chk_brreg_source_translation_terms_source_text CHECK (btrim(source_text) <> ''),
  CONSTRAINT chk_brreg_source_translation_terms_normalized CHECK (btrim(source_text_normalized) <> ''),
  CONSTRAINT chk_brreg_source_translation_terms_key CHECK (term_key ~ '^[0-9a-f]{64}$')
);

CREATE UNIQUE INDEX uq_brreg_source_translation_terms_key
  ON brreg_source.translation_terms(source, source_lang, target_lang, prompt_version, term_key);

CREATE INDEX idx_brreg_source_translation_terms_status
  ON brreg_source.translation_terms(status, updated_at);

CREATE INDEX idx_brreg_source_translation_terms_lookup
  ON brreg_source.translation_terms(source_lang, target_lang, prompt_version, source_text_normalized);

CREATE OR REPLACE VIEW brreg_source.v_missing_translation_fields AS
SELECT
  'brreg_source.companies'::text AS source_table,
  company.id AS source_row_id,
  company.id AS company_id,
  'organization_form_label'::text AS source_column,
  'organization_form_label_en'::text AS target_column,
  'no'::text AS source_lang,
  'en'::text AS target_lang,
  company.organization_form_label AS source_text,
  lower(btrim(company.organization_form_label)) AS source_text_normalized,
  encode(digest(lower(btrim(company.organization_form_label)), 'sha256'), 'hex') AS term_key
FROM brreg_source.companies company
WHERE company.organization_form_label IS NOT NULL
  AND btrim(company.organization_form_label) <> ''
  AND (company.organization_form_label_en IS NULL OR btrim(company.organization_form_label_en) = '')
UNION ALL
SELECT
  'brreg_source.companies',
  company.id,
  company.id,
  'response_class',
  'response_class_en',
  'no',
  'en',
  company.response_class,
  lower(btrim(company.response_class)),
  encode(digest(lower(btrim(company.response_class)), 'sha256'), 'hex')
FROM brreg_source.companies company
WHERE company.response_class IS NOT NULL
  AND btrim(company.response_class) <> ''
  AND (company.response_class_en IS NULL OR btrim(company.response_class_en) = '')
UNION ALL
SELECT
  'brreg_source.companies',
  company.id,
  company.id,
  'activity_description',
  'activity_description_en',
  'no',
  'en',
  company.activity_description,
  lower(btrim(company.activity_description)),
  encode(digest(lower(btrim(company.activity_description)), 'sha256'), 'hex')
FROM brreg_source.companies company
WHERE company.activity_description IS NOT NULL
  AND btrim(company.activity_description) <> ''
  AND (company.activity_description_en IS NULL OR btrim(company.activity_description_en) = '')
UNION ALL
SELECT
  'brreg_source.companies',
  company.id,
  company.id,
  'statutory_purpose',
  'statutory_purpose_en',
  'no',
  'en',
  company.statutory_purpose,
  lower(btrim(company.statutory_purpose)),
  encode(digest(lower(btrim(company.statutory_purpose)), 'sha256'), 'hex')
FROM brreg_source.companies company
WHERE company.statutory_purpose IS NOT NULL
  AND btrim(company.statutory_purpose) <> ''
  AND (company.statutory_purpose_en IS NULL OR btrim(company.statutory_purpose_en) = '')
UNION ALL
SELECT
  'brreg_source.capital',
  capital.id,
  capital.company_id,
  'capital_type',
  'capital_type_en',
  'no',
  'en',
  capital.capital_type,
  lower(btrim(capital.capital_type)),
  encode(digest(lower(btrim(capital.capital_type)), 'sha256'), 'hex')
FROM brreg_source.capital capital
WHERE capital.capital_type IS NOT NULL
  AND btrim(capital.capital_type) <> ''
  AND (capital.capital_type_en IS NULL OR btrim(capital.capital_type_en) = '');
```

- [x] **Step 4: Add migration down file**

Create `corpscout/database/migrations/000079_brreg_source_translation_terms.down.sql`:

```sql
DROP VIEW IF EXISTS brreg_source.v_missing_translation_fields;
DROP TABLE IF EXISTS brreg_source.translation_terms;
```

- [x] **Step 5: Run migration test and verify it passes**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestBRREGSourceTranslationTermsMigration -count=1
```

Expected: PASS.

---

### Task 2: Add SQL Queries for Term Cache and Applying Cached Translations

**Files:**
- Modify `corpscout/database/queries/brreg_source_profile.sql`
- Regenerate `corpscout/scheduler/internal/db/gen/brreg_source_profile.sql.go`
- Regenerate `corpscout/scheduler/internal/db/gen/querier.go`
- Create `corpscout/scheduler/internal/brreg/db/term_translation.go`
- Create `corpscout/scheduler/internal/brreg/db/term_translation_test.go`
- Modify `corpscout/scheduler/internal/brreg/db/types.go`

- [x] **Step 1: Write DB gateway test for cached translation apply**

Create `corpscout/scheduler/internal/brreg/db/term_translation_test.go`:

```go
package brregdb

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestApplyCachedTermTranslationsUpdatesCompanyAndCapital(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()

	rawRecordID := uuid.New()
	companyID := uuid.New()
	capitalID := uuid.New()

	_, err := tx.Exec(ctx, `
INSERT INTO brreg_workflow.raw_records (
  id, source_native_id, organization_number, organization_name,
  registration_status, country_iso2, raw_payload, payload_hash
) VALUES ($1, '999111222', '999111222', 'TERM TEST AS', 'active', 'NO', '{}'::jsonb, repeat('a', 64))
`, rawRecordID)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.companies (
  id, raw_record_id, organization_number, organization_name, country_iso2,
  organization_form_label, response_class, lifecycle_status, registration_status, row_status
) VALUES ($1, $2, '999111222', 'TERM TEST AS', 'NO', 'Aksjeselskap', 'Enhet', 'active', 'active', 'active')
`, companyID, rawRecordID)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.capital (
  id, company_id, raw_record_id, capital_type, original_amount, original_currency, raw_capital_payload
) VALUES ($1, $2, $3, 'Aksjekapital', 30000, 'NOK', '{}'::jsonb)
`, capitalID, companyID, rawRecordID)
	require.NoError(t, err)

	result, err := New(tx).UpsertTranslationTerms(ctx, UpsertTranslationTermsCommand{
		Terms: []TranslationTermResult{
			{
				SourceLang: "no", TargetLang: "en", PromptVersion: "v1",
				TermKey: "7b4f0efad7dbce2736e0cbad65f4c9f95bcfdcaa56fdb6b51662dd0fdf2fdf86",
				SourceTextNormalized: "aksjeselskap", SourceText: "Aksjeselskap",
				TranslatedText: "Limited liability company", Status: "succeeded",
			},
			{
				SourceLang: "no", TargetLang: "en", PromptVersion: "v1",
				TermKey: "31be9cde9b0edb7d3f21239c72cf55d5114ac5fb4c95a8b3576a7c93efec31d6",
				SourceTextNormalized: "enhet", SourceText: "Enhet",
				TranslatedText: "Entity", Status: "succeeded",
			},
			{
				SourceLang: "no", TargetLang: "en", PromptVersion: "v1",
				TermKey: "bb5a5dbb23975b71e3a1f32818e7dc19b87571d4fbc00f1a28ea41da5fc3b277",
				SourceTextNormalized: "aksjekapital", SourceText: "Aksjekapital",
				TranslatedText: "Share capital", Status: "succeeded",
			},
		},
	})
	require.NoError(t, err)
	require.EqualValues(t, 3, result.TermsUpserted)

	applied, err := New(tx).ApplyCachedTermTranslations(ctx, ApplyCachedTermTranslationsCommand{
		PromptVersion: "v1",
		Limit:         100,
	})
	require.NoError(t, err)
	require.EqualValues(t, 3, applied.FieldsApplied)

	var organizationFormLabelEN string
	var responseClassEN string
	err = tx.QueryRow(ctx, `
SELECT organization_form_label_en, response_class_en
FROM brreg_source.companies
WHERE id = $1
`, companyID).Scan(&organizationFormLabelEN, &responseClassEN)
	require.NoError(t, err)
	require.Equal(t, "Limited liability company", organizationFormLabelEN)
	require.Equal(t, "Entity", responseClassEN)

	var capitalTypeEN string
	err = tx.QueryRow(ctx, `
SELECT capital_type_en
FROM brreg_source.capital
WHERE id = $1
`, capitalID).Scan(&capitalTypeEN)
	require.NoError(t, err)
	require.Equal(t, "Share capital", capitalTypeEN)
}
```

- [x] **Step 2: Run test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/brreg/db -run TestApplyCachedTermTranslationsUpdatesCompanyAndCapital -count=1
```

Expected: FAIL because `UpsertTranslationTerms` does not exist.

- [x] **Step 3: Add sqlc queries**

Append to `corpscout/database/queries/brreg_source_profile.sql`:

```sql
-- name: InsertPendingBrregTranslationTerms :one
WITH inserted AS (
  INSERT INTO brreg_source.translation_terms (
    source,
    source_lang,
    target_lang,
    source_text_normalized,
    source_text,
    term_key,
    status,
    provider,
    model,
    prompt_version,
    metadata,
    last_requested_at,
    updated_at
  )
  SELECT
    'brreg',
    missing.source_lang,
    missing.target_lang,
    missing.source_text_normalized,
    min(missing.source_text),
    missing.term_key,
    'pending',
    sqlc.narg('provider')::text,
    sqlc.narg('model')::text,
    sqlc.arg('prompt_version')::text,
    jsonb_build_object('workflow_id', sqlc.narg('workflow_id')::text),
    now(),
    now()
  FROM brreg_source.v_missing_translation_fields missing
  LEFT JOIN brreg_source.translation_terms existing
    ON existing.source = 'brreg'
   AND existing.source_lang = missing.source_lang
   AND existing.target_lang = missing.target_lang
   AND existing.prompt_version = sqlc.arg('prompt_version')::text
   AND existing.term_key = missing.term_key
  WHERE existing.id IS NULL
  GROUP BY missing.source_lang, missing.target_lang, missing.source_text_normalized, missing.term_key
  LIMIT CASE WHEN sqlc.arg('limit')::integer <= 0 THEN NULL ELSE sqlc.arg('limit')::integer END
  ON CONFLICT (source, source_lang, target_lang, prompt_version, term_key) DO UPDATE
  SET last_requested_at = now(),
      updated_at = now()
  RETURNING id
)
SELECT count(*)::integer AS terms_inserted
FROM inserted;

-- name: MarkBrregTranslationTermsQueued :many
WITH picked AS (
  SELECT id
  FROM brreg_source.translation_terms
  WHERE source = 'brreg'
    AND status IN ('pending', 'failed_retryable')
    AND prompt_version = sqlc.arg('prompt_version')::text
    AND attempt_count < sqlc.arg('max_attempts')::integer
  ORDER BY updated_at, id
  LIMIT sqlc.arg('limit')::integer
  FOR UPDATE SKIP LOCKED
),
queued AS (
  UPDATE brreg_source.translation_terms term
  SET status = 'queued',
      attempt_count = term.attempt_count + 1,
      provider = sqlc.narg('provider')::text,
      model = sqlc.narg('model')::text,
      last_requested_at = now(),
      updated_at = now()
  FROM picked
  WHERE term.id = picked.id
  RETURNING
    term.id,
    term.source_lang,
    term.target_lang,
    term.source_text_normalized,
    term.source_text,
    term.term_key,
    term.attempt_count
)
SELECT * FROM queued;

-- name: UpsertBrregTranslationTermResult :exec
INSERT INTO brreg_source.translation_terms (
  source,
  source_lang,
  target_lang,
  source_text_normalized,
  source_text,
  term_key,
  translated_text,
  status,
  provider,
  model,
  prompt_version,
  error,
  error_code,
  metadata,
  translated_at,
  updated_at
) VALUES (
  'brreg',
  sqlc.arg('source_lang')::text,
  sqlc.arg('target_lang')::text,
  sqlc.arg('source_text_normalized')::text,
  sqlc.arg('source_text')::text,
  sqlc.arg('term_key')::text,
  sqlc.narg('translated_text')::text,
  sqlc.arg('status')::text,
  sqlc.narg('provider')::text,
  sqlc.narg('model')::text,
  sqlc.arg('prompt_version')::text,
  sqlc.narg('error')::text,
  sqlc.narg('error_code')::text,
  sqlc.arg('metadata')::jsonb,
  CASE WHEN sqlc.arg('status')::text = 'succeeded' THEN now() ELSE NULL END,
  now()
)
ON CONFLICT (source, source_lang, target_lang, prompt_version, term_key) DO UPDATE
SET translated_text = EXCLUDED.translated_text,
    status = EXCLUDED.status,
    provider = EXCLUDED.provider,
    model = EXCLUDED.model,
    error = EXCLUDED.error,
    error_code = EXCLUDED.error_code,
    metadata = brreg_source.translation_terms.metadata || EXCLUDED.metadata,
    translated_at = CASE WHEN EXCLUDED.status = 'succeeded' THEN now() ELSE brreg_source.translation_terms.translated_at END,
    updated_at = now();

-- name: ApplyBrregSourceCompanyCachedTranslationTerms :one
WITH matched AS (
  SELECT missing.source_row_id, missing.target_column, term.translated_text
  FROM brreg_source.v_missing_translation_fields missing
  JOIN brreg_source.translation_terms term
    ON term.source = 'brreg'
   AND term.source_lang = missing.source_lang
   AND term.target_lang = missing.target_lang
   AND term.prompt_version = sqlc.arg('prompt_version')::text
   AND term.term_key = missing.term_key
   AND term.status = 'succeeded'
  WHERE missing.source_table = 'brreg_source.companies'
    AND missing.target_column IN (
      'organization_form_label_en',
      'response_class_en',
      'activity_description_en',
      'statutory_purpose_en'
    )
  ORDER BY missing.source_row_id
  LIMIT CASE WHEN sqlc.arg('limit')::integer <= 0 THEN NULL ELSE sqlc.arg('limit')::integer END
),
updated AS (
  UPDATE brreg_source.companies company
  SET organization_form_label_en = CASE WHEN matched.target_column = 'organization_form_label_en' THEN matched.translated_text ELSE company.organization_form_label_en END,
      response_class_en = CASE WHEN matched.target_column = 'response_class_en' THEN matched.translated_text ELSE company.response_class_en END,
      activity_description_en = CASE WHEN matched.target_column = 'activity_description_en' THEN matched.translated_text ELSE company.activity_description_en END,
      statutory_purpose_en = CASE WHEN matched.target_column = 'statutory_purpose_en' THEN matched.translated_text ELSE company.statutory_purpose_en END,
      updated_at = now()
  FROM matched
  WHERE company.id = matched.source_row_id
  RETURNING company.id
)
SELECT count(*)::integer AS fields_applied FROM updated;

-- name: ApplyBrregSourceCapitalCachedTranslationTerms :one
WITH matched AS (
  SELECT missing.source_row_id, missing.target_column, term.translated_text
  FROM brreg_source.v_missing_translation_fields missing
  JOIN brreg_source.translation_terms term
    ON term.source = 'brreg'
   AND term.source_lang = missing.source_lang
   AND term.target_lang = missing.target_lang
   AND term.prompt_version = sqlc.arg('prompt_version')::text
   AND term.term_key = missing.term_key
   AND term.status = 'succeeded'
  WHERE missing.source_table = 'brreg_source.capital'
    AND missing.target_column = 'capital_type_en'
  ORDER BY missing.source_row_id
  LIMIT CASE WHEN sqlc.arg('limit')::integer <= 0 THEN NULL ELSE sqlc.arg('limit')::integer END
),
updated AS (
  UPDATE brreg_source.capital capital
  SET capital_type_en = matched.translated_text,
      updated_at = now()
  FROM matched
  WHERE capital.id = matched.source_row_id
  RETURNING capital.id
)
SELECT count(*)::integer AS fields_applied FROM updated;

-- name: CountBrregMissingTranslationFields :one
SELECT count(*)::integer AS missing_fields
FROM brreg_source.v_missing_translation_fields;
```

- [x] **Step 4: Regenerate sqlc output**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
sqlc generate -f database/sqlc.yaml
```

Expected: generated files change under `corpscout/scheduler/internal/db/gen`.

- [x] **Step 5: Add gateway types**

Modify `corpscout/scheduler/internal/brreg/db/types.go`:

```go
type EnsurePendingTranslationTermsCommand struct {
	Provider      string
	Model         string
	PromptVersion string
	WorkflowID    string
	Limit         int32
}

type EnsurePendingTranslationTermsResult struct {
	TermsInserted int32
}

type ClaimQueuedTranslationTermsCommand struct {
	Provider       string
	Model          string
	PromptVersion  string
	Limit          int32
	MaxAttempts    int32
}

type QueuedTranslationTerm struct {
	ID                   string
	SourceLang           string
	TargetLang           string
	SourceTextNormalized string
	SourceText           string
	TermKey              string
	AttemptCount         int32
}

type TranslationTermResult struct {
	SourceLang           string
	TargetLang           string
	SourceTextNormalized string
	SourceText           string
	TermKey              string
	TranslatedText       string
	Status               string
	Provider             string
	Model                string
	PromptVersion        string
	Error                string
	ErrorCode            string
	Metadata             map[string]any
}

type UpsertTranslationTermsCommand struct {
	Terms []TranslationTermResult
}

type UpsertTranslationTermsResult struct {
	TermsUpserted int32
}

type ApplyCachedTermTranslationsCommand struct {
	PromptVersion string
	Limit         int32
}

type ApplyCachedTermTranslationsResult struct {
	FieldsApplied int32
}
```

- [x] **Step 6: Add gateway implementation**

Create `corpscout/scheduler/internal/brreg/db/term_translation.go`:

```go
package brregdb

import (
	"context"
	"encoding/json"

	"github.com/cockroachdb/errors"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const defaultTermPromptVersion = "v1"

func (g *Gateway) EnsurePendingTranslationTerms(ctx context.Context, command EnsurePendingTranslationTermsCommand) (EnsurePendingTranslationTermsResult, error) {
	if g.pool == nil {
		return EnsurePendingTranslationTermsResult{}, errors.New("brreg database pool not available")
	}
	if command.PromptVersion == "" {
		command.PromptVersion = defaultTermPromptVersion
	}
	row, err := db.New(g.pool).InsertPendingBrregTranslationTerms(ctx, db.InsertPendingBrregTranslationTermsParams{
		Provider:      nilIfEmpty(command.Provider),
		Model:         nilIfEmpty(command.Model),
		PromptVersion: command.PromptVersion,
		WorkflowID:    nilIfEmpty(command.WorkflowID),
		Limit:         int32Limit(command.Limit),
	})
	if err != nil {
		return EnsurePendingTranslationTermsResult{}, errors.Wrap(err, "insert pending brreg translation terms")
	}
	return EnsurePendingTranslationTermsResult{TermsInserted: row.TermsInserted}, nil
}

func (g *Gateway) ClaimQueuedTranslationTerms(ctx context.Context, command ClaimQueuedTranslationTermsCommand) ([]QueuedTranslationTerm, error) {
	if g.pool == nil {
		return nil, errors.New("brreg database pool not available")
	}
	if command.PromptVersion == "" {
		command.PromptVersion = defaultTermPromptVersion
	}
	rows, err := db.New(g.pool).MarkBrregTranslationTermsQueued(ctx, db.MarkBrregTranslationTermsQueuedParams{
		Provider:      nilIfEmpty(command.Provider),
		Model:         nilIfEmpty(command.Model),
		PromptVersion: command.PromptVersion,
		MaxAttempts:   command.MaxAttempts,
		Limit:         command.Limit,
	})
	if err != nil {
		return nil, errors.Wrap(err, "mark brreg translation terms queued")
	}
	terms := make([]QueuedTranslationTerm, 0, len(rows))
	for _, row := range rows {
		terms = append(terms, QueuedTranslationTerm{
			ID:                   row.ID.String(),
			SourceLang:           row.SourceLang,
			TargetLang:           row.TargetLang,
			SourceTextNormalized: row.SourceTextNormalized,
			SourceText:           row.SourceText,
			TermKey:              row.TermKey,
			AttemptCount:         row.AttemptCount,
		})
	}
	return terms, nil
}

func (g *Gateway) UpsertTranslationTerms(ctx context.Context, command UpsertTranslationTermsCommand) (UpsertTranslationTermsResult, error) {
	if g.pool == nil {
		return UpsertTranslationTermsResult{}, errors.New("brreg database pool not available")
	}
	queries := db.New(g.pool)
	for _, term := range command.Terms {
		if term.PromptVersion == "" {
			term.PromptVersion = defaultTermPromptVersion
		}
		metadata, err := json.Marshal(term.Metadata)
		if err != nil {
			return UpsertTranslationTermsResult{}, errors.Wrap(err, "marshal brreg translation term metadata")
		}
		if len(metadata) == 0 || string(metadata) == "null" {
			metadata = []byte(`{}`)
		}
		if err := queries.UpsertBrregTranslationTermResult(ctx, db.UpsertBrregTranslationTermResultParams{
			SourceLang:           term.SourceLang,
			TargetLang:           term.TargetLang,
			SourceTextNormalized: term.SourceTextNormalized,
			SourceText:           term.SourceText,
			TermKey:              term.TermKey,
			TranslatedText:       nilIfEmpty(term.TranslatedText),
			Status:               term.Status,
			Provider:             nilIfEmpty(term.Provider),
			Model:                nilIfEmpty(term.Model),
			PromptVersion:        term.PromptVersion,
			Error:                nilIfEmpty(term.Error),
			ErrorCode:            nilIfEmpty(term.ErrorCode),
			Metadata:             metadata,
		}); err != nil {
			return UpsertTranslationTermsResult{}, errors.Wrap(err, "upsert brreg translation term result")
		}
	}
	return UpsertTranslationTermsResult{TermsUpserted: int32(len(command.Terms))}, nil
}

func (g *Gateway) ApplyCachedTermTranslations(ctx context.Context, command ApplyCachedTermTranslationsCommand) (ApplyCachedTermTranslationsResult, error) {
	if g.pool == nil {
		return ApplyCachedTermTranslationsResult{}, errors.New("brreg database pool not available")
	}
	if command.PromptVersion == "" {
		command.PromptVersion = defaultTermPromptVersion
	}
	queries := db.New(g.pool)
	company, err := queries.ApplyBrregSourceCompanyCachedTranslationTerms(ctx, db.ApplyBrregSourceCompanyCachedTranslationTermsParams{
		PromptVersion: command.PromptVersion,
		Limit:         int32Limit(command.Limit),
	})
	if err != nil {
		return ApplyCachedTermTranslationsResult{}, errors.Wrap(err, "apply brreg company cached translation terms")
	}
	capital, err := queries.ApplyBrregSourceCapitalCachedTranslationTerms(ctx, db.ApplyBrregSourceCapitalCachedTranslationTermsParams{
		PromptVersion: command.PromptVersion,
		Limit:         int32Limit(command.Limit),
	})
	if err != nil {
		return ApplyCachedTermTranslationsResult{}, errors.Wrap(err, "apply brreg capital cached translation terms")
	}
	return ApplyCachedTermTranslationsResult{FieldsApplied: company.FieldsApplied + capital.FieldsApplied}, nil
}

func nilIfEmpty(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func int32Limit(value int32) int32 {
	if value < 0 {
		return 0
	}
	return value
}
```

- [x] **Step 7: Run gateway test and verify it passes**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/brreg/db -run TestApplyCachedTermTranslationsUpdatesCompanyAndCapital -count=1
```

Expected: PASS.

---

### Task 3: Add NATS Term Translation Client

**Files:**
- Create `corpscout/scheduler/internal/translationclient/terms.go`
- Create `corpscout/scheduler/internal/translationclient/terms_test.go`

- [x] **Step 1: Write request payload test**

Create `corpscout/scheduler/internal/translationclient/terms_test.go`:

```go
package translationclient

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTermTranslationRequestPayloadRoundTrips(t *testing.T) {
	payload := TermTranslationRequest{
		RequestID:     "request-1",
		Source:        "brreg",
		SourceLang:    "no",
		TargetLang:    "en",
		Provider:      "default",
		Model:         "",
		PromptVersion: "v1",
		Terms: []TermTranslationRequestTerm{{
			TermKey:              "abc",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
		}},
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	var decoded TermTranslationRequest
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, "brreg", decoded.Source)
	require.Equal(t, "Aksjeselskap", decoded.Terms[0].SourceText)
}
```

- [x] **Step 2: Run test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/translationclient -run TestTermTranslationRequestPayloadRoundTrips -count=1
```

Expected: FAIL because term translation request types do not exist.

- [x] **Step 3: Add term message types and constants**

Create `corpscout/scheduler/internal/translationclient/terms.go`:

```go
package translationclient

const (
	DefaultTermTranslationRequestSubject = "brreg.translation.terms.request"
	DefaultTermTranslationResultSubject  = "brreg.translation.terms.result"
)

type TermTranslationRequest struct {
	RequestID     string                       `json:"request_id"`
	Source        string                       `json:"source"`
	SourceLang    string                       `json:"source_lang"`
	TargetLang    string                       `json:"target_lang"`
	Provider      string                       `json:"provider"`
	Model         string                       `json:"model,omitempty"`
	PromptVersion string                       `json:"prompt_version"`
	Terms         []TermTranslationRequestTerm `json:"terms"`
}

type TermTranslationRequestTerm struct {
	TermKey              string `json:"term_key"`
	SourceText           string `json:"source_text"`
	SourceTextNormalized string `json:"source_text_normalized"`
}

type TermTranslationResult struct {
	RequestID     string                         `json:"request_id"`
	Source        string                         `json:"source"`
	SourceLang    string                         `json:"source_lang"`
	TargetLang    string                         `json:"target_lang"`
	Provider      string                         `json:"provider"`
	Model         string                         `json:"model,omitempty"`
	PromptVersion string                         `json:"prompt_version"`
	Results       []TermTranslationResultItem    `json:"results"`
	Failures      []TermTranslationFailureResult `json:"failures"`
}

type TermTranslationResultItem struct {
	TermKey              string `json:"term_key"`
	SourceText           string `json:"source_text"`
	SourceTextNormalized string `json:"source_text_normalized"`
	TranslatedText       string `json:"translated_text"`
	Status               string `json:"status"`
}

type TermTranslationFailureResult struct {
	TermKey              string `json:"term_key"`
	SourceText           string `json:"source_text"`
	SourceTextNormalized string `json:"source_text_normalized"`
	Status               string `json:"status"`
	ErrorCode            string `json:"error_code,omitempty"`
	Error                string `json:"error,omitempty"`
}
```

- [x] **Step 4: Run test and verify it passes**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/translationclient -run TestTermTranslationRequestPayloadRoundTrips -count=1
```

Expected: PASS.

---

### Task 4: Add Term Translation Actions

**Files:**
- Create `corpscout/scheduler/internal/brreg/actions/term_translation_actions.go`
- Create `corpscout/scheduler/internal/brreg/actions/term_translation_actions_test.go`

- [x] **Step 1: Write unit test for request construction**

Create `corpscout/scheduler/internal/brreg/actions/term_translation_actions_test.go`:

```go
package actions

import (
	"testing"

	"github.com/stretchr/testify/require"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
)

func TestTermTranslationRequestFromQueuedTerms(t *testing.T) {
	request := termTranslationRequestFromQueuedTerms(PublishBrregTranslationTermsInput{
		RequestID:     "request-1",
		Provider:      "default",
		Model:         "qwen3:6b",
		PromptVersion: "v1",
	}, []brregdb.QueuedTranslationTerm{{
		TermKey:              "abc",
		SourceLang:           "no",
		TargetLang:           "en",
		SourceText:           "Aksjeselskap",
		SourceTextNormalized: "aksjeselskap",
	}})

	require.Equal(t, "brreg", request.Source)
	require.Equal(t, "no", request.SourceLang)
	require.Equal(t, "en", request.TargetLang)
	require.Equal(t, "Aksjeselskap", request.Terms[0].SourceText)
}
```

- [x] **Step 2: Run test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/brreg/actions -run TestTermTranslationRequestFromQueuedTerms -count=1
```

Expected: FAIL because the helper does not exist.

- [x] **Step 3: Add term translation action types and helper**

Create `corpscout/scheduler/internal/brreg/actions/term_translation_actions.go`:

```go
package actions

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

type TermTranslationActions struct {
	gateway *brregdb.Gateway
	nats    *nats.Conn
}

func NewTermTranslationActions(gateway *brregdb.Gateway, natsConn *nats.Conn) *TermTranslationActions {
	return &TermTranslationActions{gateway: gateway, nats: natsConn}
}

type EnsureBrregTranslationTermsInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id"`
	Provider           string `json:"provider,omitempty"`
	Model              string `json:"model,omitempty"`
	PromptVersion      string `json:"prompt_version,omitempty"`
	Limit              int32  `json:"limit,omitempty"`
}

type EnsureBrregTranslationTermsResult struct {
	TermsInserted int32 `json:"terms_inserted"`
}

type PublishBrregTranslationTermsInput struct {
	RequestID      string `json:"request_id,omitempty"`
	Provider       string `json:"provider,omitempty"`
	Model          string `json:"model,omitempty"`
	PromptVersion  string `json:"prompt_version,omitempty"`
	Limit          int32  `json:"limit"`
	MaxAttempts    int32  `json:"max_attempts"`
	RequestSubject string `json:"request_subject,omitempty"`
}

type PublishBrregTranslationTermsResult struct {
	TermsPublished int32 `json:"terms_published"`
}

type ApplyBrregCachedTranslationTermsInput struct {
	PromptVersion string `json:"prompt_version,omitempty"`
	Limit         int32  `json:"limit,omitempty"`
}

type ApplyBrregCachedTranslationTermsResult struct {
	FieldsApplied int32 `json:"fields_applied"`
}

func termTranslationRequestFromQueuedTerms(input PublishBrregTranslationTermsInput, terms []brregdb.QueuedTranslationTerm) translationclient.TermTranslationRequest {
	requestID := input.RequestID
	if requestID == "" {
		requestID = uuid.NewString()
	}
	sourceLang := "no"
	targetLang := "en"
	if len(terms) > 0 {
		sourceLang = terms[0].SourceLang
		targetLang = terms[0].TargetLang
	}
	request := translationclient.TermTranslationRequest{
		RequestID:     requestID,
		Source:        "brreg",
		SourceLang:    sourceLang,
		TargetLang:    targetLang,
		Provider:      input.Provider,
		Model:         input.Model,
		PromptVersion: input.PromptVersion,
		Terms:         make([]translationclient.TermTranslationRequestTerm, 0, len(terms)),
	}
	for _, term := range terms {
		request.Terms = append(request.Terms, translationclient.TermTranslationRequestTerm{
			TermKey:              term.TermKey,
			SourceText:           term.SourceText,
			SourceTextNormalized: term.SourceTextNormalized,
		})
	}
	return request
}

func (a *TermTranslationActions) EnsureBrregTranslationTerms(ctx context.Context, input EnsureBrregTranslationTermsInput) (EnsureBrregTranslationTermsResult, error) {
	result, err := a.gateway.EnsurePendingTranslationTerms(ctx, brregdb.EnsurePendingTranslationTermsCommand{
		Provider:      input.Provider,
		Model:         input.Model,
		PromptVersion: input.PromptVersion,
		WorkflowID:    input.TemporalWorkflowID,
		Limit:         input.Limit,
	})
	if err != nil {
		return EnsureBrregTranslationTermsResult{}, err
	}
	return EnsureBrregTranslationTermsResult{TermsInserted: result.TermsInserted}, nil
}

func (a *TermTranslationActions) PublishBrregTranslationTerms(ctx context.Context, input PublishBrregTranslationTermsInput) (PublishBrregTranslationTermsResult, error) {
	if a.nats == nil {
		return PublishBrregTranslationTermsResult{}, errors.New("nats connection not available")
	}
	terms, err := a.gateway.ClaimQueuedTranslationTerms(ctx, brregdb.ClaimQueuedTranslationTermsCommand{
		Provider:      input.Provider,
		Model:         input.Model,
		PromptVersion: input.PromptVersion,
		Limit:         input.Limit,
		MaxAttempts:   input.MaxAttempts,
	})
	if err != nil {
		return PublishBrregTranslationTermsResult{}, err
	}
	if len(terms) == 0 {
		return PublishBrregTranslationTermsResult{}, nil
	}
	request := termTranslationRequestFromQueuedTerms(input, terms)
	data, err := json.Marshal(request)
	if err != nil {
		return PublishBrregTranslationTermsResult{}, errors.Wrap(err, "marshal brreg term translation request")
	}
	subject := input.RequestSubject
	if subject == "" {
		subject = translationclient.DefaultTermTranslationRequestSubject
	}
	if err := a.nats.Publish(subject, data); err != nil {
		return PublishBrregTranslationTermsResult{}, errors.Wrap(err, "publish brreg term translation request")
	}
	slog.DebugContext(ctx, "published brreg term translation request", "terms", len(terms), "subject", subject)
	return PublishBrregTranslationTermsResult{TermsPublished: int32(len(terms))}, nil
}

func (a *TermTranslationActions) ApplyBrregCachedTranslationTerms(ctx context.Context, input ApplyBrregCachedTranslationTermsInput) (ApplyBrregCachedTranslationTermsResult, error) {
	result, err := a.gateway.ApplyCachedTermTranslations(ctx, brregdb.ApplyCachedTermTranslationsCommand{
		PromptVersion: input.PromptVersion,
		Limit:         input.Limit,
	})
	if err != nil {
		return ApplyBrregCachedTranslationTermsResult{}, err
	}
	return ApplyBrregCachedTranslationTermsResult{FieldsApplied: result.FieldsApplied}, nil
}
```

- [x] **Step 4: Run unit test and verify it passes**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/brreg/actions -run TestTermTranslationRequestFromQueuedTerms -count=1
```

Expected: PASS.

---

### Task 5: Add Translation Result Consumer

**Files:**
- Create `corpscout/scheduler/internal/brreg/actions/term_translation_consumer.go`
- Create `corpscout/scheduler/internal/brreg/actions/term_translation_consumer_test.go`

- [x] **Step 1: Write result mapping test**

Append to `corpscout/scheduler/internal/brreg/actions/term_translation_consumer_test.go`:

```go
package actions

import (
	"testing"

	"github.com/stretchr/testify/require"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

func TestTermResultsFromTranslationResultMapsSuccessAndFailure(t *testing.T) {
	results := termResultsFromTranslationResult(translationclient.TermTranslationResult{
		Provider: "default",
		Model: "qwen3:6b",
		PromptVersion: "v1",
		SourceLang: "no",
		TargetLang: "en",
		Results: []translationclient.TermTranslationResultItem{{
			TermKey: "abc",
			SourceText: "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TranslatedText: "Limited liability company",
			Status: "succeeded",
		}},
		Failures: []translationclient.TermTranslationFailureResult{{
			TermKey: "def",
			SourceText: "Feil",
			SourceTextNormalized: "feil",
			Status: "failed_retryable",
			ErrorCode: "llm_request_failed",
			Error: "timeout",
		}},
	})

	require.Equal(t, []brregdb.TranslationTermResult{
		{
			SourceLang: "no", TargetLang: "en", SourceTextNormalized: "aksjeselskap",
			SourceText: "Aksjeselskap", TermKey: "abc", TranslatedText: "Limited liability company",
			Status: "succeeded", Provider: "default", Model: "qwen3:6b", PromptVersion: "v1",
		},
		{
			SourceLang: "no", TargetLang: "en", SourceTextNormalized: "feil",
			SourceText: "Feil", TermKey: "def", Status: "failed_retryable",
			Provider: "default", Model: "qwen3:6b", PromptVersion: "v1",
			Error: "timeout", ErrorCode: "llm_request_failed",
		},
	}, results)
}
```

- [x] **Step 2: Run test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/brreg/actions -run TestTermResultsFromTranslationResultMapsSuccessAndFailure -count=1
```

Expected: FAIL because `termResultsFromTranslationResult` does not exist.

- [x] **Step 3: Add consumer mapping**

Create `corpscout/scheduler/internal/brreg/actions/term_translation_consumer.go`:

```go
package actions

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

type TermTranslationResultConsumer struct {
	gateway *brregdb.Gateway
	nats    *nats.Conn
	subject string
}

func NewTermTranslationResultConsumer(gateway *brregdb.Gateway, natsConn *nats.Conn, subject string) *TermTranslationResultConsumer {
	if subject == "" {
		subject = translationclient.DefaultTermTranslationResultSubject
	}
	return &TermTranslationResultConsumer{gateway: gateway, nats: natsConn, subject: subject}
}

func termResultsFromTranslationResult(result translationclient.TermTranslationResult) []brregdb.TranslationTermResult {
	terms := make([]brregdb.TranslationTermResult, 0, len(result.Results)+len(result.Failures))
	for _, item := range result.Results {
		terms = append(terms, brregdb.TranslationTermResult{
			SourceLang: itemOrDefault(result.SourceLang, "no"),
			TargetLang: itemOrDefault(result.TargetLang, "en"),
			SourceTextNormalized: item.SourceTextNormalized,
			SourceText: item.SourceText,
			TermKey: item.TermKey,
			TranslatedText: item.TranslatedText,
			Status: item.Status,
			Provider: result.Provider,
			Model: result.Model,
			PromptVersion: itemOrDefault(result.PromptVersion, "v1"),
		})
	}
	for _, failure := range result.Failures {
		terms = append(terms, brregdb.TranslationTermResult{
			SourceLang: itemOrDefault(result.SourceLang, "no"),
			TargetLang: itemOrDefault(result.TargetLang, "en"),
			SourceTextNormalized: failure.SourceTextNormalized,
			SourceText: failure.SourceText,
			TermKey: failure.TermKey,
			Status: failure.Status,
			Provider: result.Provider,
			Model: result.Model,
			PromptVersion: itemOrDefault(result.PromptVersion, "v1"),
			Error: failure.Error,
			ErrorCode: failure.ErrorCode,
		})
	}
	return terms
}

func itemOrDefault(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func (c *TermTranslationResultConsumer) HandleMessage(ctx context.Context, msg *nats.Msg) error {
	var result translationclient.TermTranslationResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return errors.Wrap(err, "decode brreg term translation result")
	}
	terms := termResultsFromTranslationResult(result)
	if _, err := c.gateway.UpsertTranslationTerms(ctx, brregdb.UpsertTranslationTermsCommand{Terms: terms}); err != nil {
		return err
	}
	applied, err := c.gateway.ApplyCachedTermTranslations(ctx, brregdb.ApplyCachedTermTranslationsCommand{
		PromptVersion: result.PromptVersion,
		Limit: 10000,
	})
	if err != nil {
		return err
	}
	slog.DebugContext(ctx, "handled brreg term translation result", "terms", len(terms), "fields_applied", applied.FieldsApplied)
	return nil
}
```

- [x] **Step 4: Run test and verify it passes**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/brreg/actions -run TestTermResultsFromTranslationResultMapsSuccessAndFailure -count=1
```

Expected: PASS.

---

### Task 6: Add Term Translation Temporal Workflow

**Files:**
- Create `corpscout/scheduler/internal/brreg/workflow/term_translation.go`
- Create `corpscout/scheduler/internal/brreg/workflow/term_translation_test.go`
- Create `corpscout/scheduler/internal/app/brreg_term_translation_temporal.go`
- Modify `corpscout/scheduler/internal/app/temporal.go`
- Modify `corpscout/scheduler/internal/app/temporal_test.go`

- [x] **Step 1: Write workflow test**

Create `corpscout/scheduler/internal/brreg/workflow/term_translation_test.go`:

```go
package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestTranslateBrregSourceTermsPublishesAndAppliesUntilDrained(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	ensureCalls := 0
	env.RegisterActivityWithOptions(func(input EnsureBrregTranslationTermsInput) (EnsureBrregTranslationTermsResult, error) {
		ensureCalls++
		if ensureCalls == 1 {
			return EnsureBrregTranslationTermsResult{TermsInserted: 2}, nil
		}
		return EnsureBrregTranslationTermsResult{}, nil
	}, activity.RegisterOptions{Name: "EnsureBrregTranslationTerms"})

	publishCalls := 0
	env.RegisterActivityWithOptions(func(input PublishBrregTranslationTermsInput) (PublishBrregTranslationTermsResult, error) {
		publishCalls++
		if publishCalls == 1 {
			return PublishBrregTranslationTermsResult{TermsPublished: 2}, nil
		}
		return PublishBrregTranslationTermsResult{}, nil
	}, activity.RegisterOptions{Name: "PublishBrregTranslationTerms"})

	env.RegisterActivityWithOptions(func(input ApplyBrregCachedTranslationTermsInput) (ApplyBrregCachedTranslationTermsResult, error) {
		return ApplyBrregCachedTranslationTermsResult{FieldsApplied: 2}, nil
	}, activity.RegisterOptions{Name: "ApplyBrregCachedTranslationTerms"})

	env.ExecuteWorkflow(TranslateBrregSourceTerms, TranslateBrregSourceTermsInput{
		Limit: 100,
		TermBatchSize: 50,
		MaxAttempts: 3,
		MaxLoops: 3,
		PromptVersion: "v1",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result TranslateBrregSourceTermsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 2, result.TermsPublished)
	require.EqualValues(t, 2, result.FieldsApplied)
}
```

- [x] **Step 2: Run workflow test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/brreg/workflow -run TestTranslateBrregSourceTermsPublishesAndAppliesUntilDrained -count=1
```

Expected: FAIL because workflow does not exist.

- [x] **Step 3: Add workflow**

Create `corpscout/scheduler/internal/brreg/workflow/term_translation.go`:

```go
package workflow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/actions"
)

const (
	TranslateBrregSourceTermsTaskQueue    = "brreg-term-translation"
	TranslateBrregSourceTermsWorkflowName = "TranslateBrregSourceTerms"

	ensureBrregTranslationTermsActivity      = "EnsureBrregTranslationTerms"
	publishBrregTranslationTermsActivity     = "PublishBrregTranslationTerms"
	applyBrregCachedTranslationTermsActivity = "ApplyBrregCachedTranslationTerms"
)

type EnsureBrregTranslationTermsInput = actions.EnsureBrregTranslationTermsInput
type EnsureBrregTranslationTermsResult = actions.EnsureBrregTranslationTermsResult
type PublishBrregTranslationTermsInput = actions.PublishBrregTranslationTermsInput
type PublishBrregTranslationTermsResult = actions.PublishBrregTranslationTermsResult
type ApplyBrregCachedTranslationTermsInput = actions.ApplyBrregCachedTranslationTermsInput
type ApplyBrregCachedTranslationTermsResult = actions.ApplyBrregCachedTranslationTermsResult

type TranslateBrregSourceTermsInput struct {
	Limit         int32  `json:"limit,omitempty"`
	TermBatchSize int32 `json:"term_batch_size,omitempty"`
	MaxAttempts   int32 `json:"max_attempts,omitempty"`
	MaxLoops       int32 `json:"max_loops,omitempty"`
	Provider      string `json:"provider,omitempty"`
	Model         string `json:"model,omitempty"`
	PromptVersion string `json:"prompt_version,omitempty"`
	Trigger       string `json:"trigger,omitempty"`
}

type TranslateBrregSourceTermsResult struct {
	Status          string `json:"status"`
	TermsInserted   int32  `json:"terms_inserted"`
	TermsPublished  int32  `json:"terms_published"`
	FieldsApplied   int32  `json:"fields_applied"`
	LoopsProcessed  int32  `json:"loops_processed"`
}

func TranslateBrregSourceTerms(ctx temporalworkflow.Context, input TranslateBrregSourceTermsInput) (TranslateBrregSourceTermsResult, error) {
	input = normalizeTranslateBrregSourceTermsInput(input)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 5 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval: time.Minute,
			MaximumAttempts: 3,
		},
	})
	info := temporalworkflow.GetInfo(ctx)
	result := TranslateBrregSourceTermsResult{Status: "running"}

	for loop := int32(0); loop < input.MaxLoops; loop++ {
		result.LoopsProcessed++
		var applied ApplyBrregCachedTranslationTermsResult
		if err := temporalworkflow.ExecuteActivity(ctx, applyBrregCachedTranslationTermsActivity, ApplyBrregCachedTranslationTermsInput{
			PromptVersion: input.PromptVersion,
			Limit: input.Limit,
		}).Get(ctx, &applied); err != nil {
			return TranslateBrregSourceTermsResult{}, err
		}
		result.FieldsApplied += applied.FieldsApplied

		var ensured EnsureBrregTranslationTermsResult
		if err := temporalworkflow.ExecuteActivity(ctx, ensureBrregTranslationTermsActivity, EnsureBrregTranslationTermsInput{
			TemporalWorkflowID: info.WorkflowExecution.ID,
			Provider: input.Provider,
			Model: input.Model,
			PromptVersion: input.PromptVersion,
			Limit: input.Limit,
		}).Get(ctx, &ensured); err != nil {
			return TranslateBrregSourceTermsResult{}, err
		}
		result.TermsInserted += ensured.TermsInserted

		var published PublishBrregTranslationTermsResult
		if err := temporalworkflow.ExecuteActivity(ctx, publishBrregTranslationTermsActivity, PublishBrregTranslationTermsInput{
			Provider: input.Provider,
			Model: input.Model,
			PromptVersion: input.PromptVersion,
			Limit: input.TermBatchSize,
			MaxAttempts: input.MaxAttempts,
		}).Get(ctx, &published); err != nil {
			return TranslateBrregSourceTermsResult{}, err
		}
		result.TermsPublished += published.TermsPublished

		if ensured.TermsInserted == 0 && published.TermsPublished == 0 {
			result.Status = "succeeded"
			return result, nil
		}
		temporalworkflow.Sleep(ctx, 5*time.Second)
	}

	return result, temporal.NewContinueAsNewError(ctx, TranslateBrregSourceTerms, input)
}

func normalizeTranslateBrregSourceTermsInput(input TranslateBrregSourceTermsInput) TranslateBrregSourceTermsInput {
	if input.Limit <= 0 {
		input.Limit = 10000
	}
	if input.TermBatchSize <= 0 {
		input.TermBatchSize = 100
	}
	if input.MaxAttempts <= 0 {
		input.MaxAttempts = 3
	}
	if input.MaxLoops <= 0 {
		input.MaxLoops = 100
	}
	if input.PromptVersion == "" {
		input.PromptVersion = "v1"
	}
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	if input.Provider == "" {
		input.Provider = "default"
	}
	return input
}
```

- [x] **Step 4: Register workflow and activities directly**

Create `corpscout/scheduler/internal/app/brreg_term_translation_temporal.go`:

```go
package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
)

func newBrregTermTranslationTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating brreg term translation temporal worker", "task_queue", brregworkflow.TranslateBrregSourceTermsTaskQueue)
	worker := temporalworker.New(temporalClient, brregworkflow.TranslateBrregSourceTermsTaskQueue, temporalworker.Options{})
	worker.RegisterWorkflow(brregworkflow.TranslateBrregSourceTerms)
	worker.RegisterActivityWithOptions(resources.termTranslationActions.EnsureBrregTranslationTerms, activity.RegisterOptions{Name: "EnsureBrregTranslationTerms"})
	worker.RegisterActivityWithOptions(resources.termTranslationActions.PublishBrregTranslationTerms, activity.RegisterOptions{Name: "PublishBrregTranslationTerms"})
	worker.RegisterActivityWithOptions(resources.termTranslationActions.ApplyBrregCachedTranslationTerms, activity.RegisterOptions{Name: "ApplyBrregCachedTranslationTerms"})
	return worker
}
```

Modify `corpscout/scheduler/internal/app/temporal.go`:

```go
termTranslationActions *brregactions.TermTranslationActions
```

Add construction:

```go
natsConn, err := nats.Connect(cfg.NATSURL)
if err != nil {
	return nil, errors.Wrap(err, "connect nats for brreg term translation")
}
```

Add to resources:

```go
termTranslationActions: brregactions.NewTermTranslationActions(gateway, natsConn),
```

Add worker to `newTemporalWorkers`:

```go
newBrregTermTranslationTemporalWorker(temporalClient, resources),
```

- [x] **Step 5: Run workflow/app tests and verify they pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/brreg/workflow -run TestTranslateBrregSourceTermsPublishesAndAppliesUntilDrained -count=1
GOWORK=off go test ./internal/app -run TestNewTemporalWorkers -count=1
```

Expected: PASS after updating expected worker count in `temporal_test.go`.

---

### Task 7: Add HTTP Trigger and BRREG UI Action Switch

**Files:**
- Modify `corpscout/scheduler/internal/httpapi/workflow_triggers.go`
- Modify `corpscout/scheduler/internal/httpapi/workflow_triggers_test.go`
- Modify `corpscout/scheduler/internal/httpapi/handlers.go`
- Modify `corpscout/ui/app/lib/api.ts`
- Modify `corpscout/ui/app/types/api.ts`
- Modify `corpscout/ui/app/components/app/BrregTranslationActionForm.tsx`
- Modify `corpscout/ui/app/components/app/BrregSourceEntryActionSheet.tsx`

- [x] **Step 1: Write HTTP trigger test**

Append to `corpscout/scheduler/internal/httpapi/workflow_triggers_test.go`:

```go
func TestStartBrregTermTranslationWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/term-translation", bytes.NewBufferString(`{
		"limit": 10000,
		"term_batch_size": 100,
		"max_attempts": 3,
		"provider": "default",
		"model": "qwen3:6b",
		"prompt_version": "v1",
		"trigger": "manual"
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"workflow":"TranslateBrregSourceTerms"`)
	require.Equal(t, "brreg-term-translation", tc.options.TaskQueue)
	require.Equal(t, reflect.ValueOf(brregworkflow.TranslateBrregSourceTerms).Pointer(), reflect.ValueOf(tc.workflow).Pointer())
}
```

- [x] **Step 2: Run HTTP test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run TestStartBrregTermTranslationWorkflowStartsTemporalWorkflow -count=1
```

Expected: FAIL with 404.

- [x] **Step 3: Add handler and route**

Modify `corpscout/scheduler/internal/httpapi/workflow_triggers.go`:

```go
type startBrregTermTranslationWorkflowRequest struct {
	Limit         int32  `json:"limit,omitempty"`
	TermBatchSize int32 `json:"term_batch_size,omitempty"`
	MaxAttempts   int32 `json:"max_attempts,omitempty"`
	Provider      string `json:"provider,omitempty"`
	Model         string `json:"model,omitempty"`
	PromptVersion string `json:"prompt_version,omitempty"`
	Trigger       string `json:"trigger,omitempty"`
}
```

Add handler:

```go
func (h *Handlers) handleStartBrregTermTranslationWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}
	var req startBrregTermTranslationWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	workflowID := newWorkflowID("brreg-term-translation")
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{ID: workflowID, TaskQueue: brregworkflow.TranslateBrregSourceTermsTaskQueue},
		brregworkflow.TranslateBrregSourceTerms,
		brregworkflow.TranslateBrregSourceTermsInput{
			Limit: req.Limit,
			TermBatchSize: req.TermBatchSize,
			MaxAttempts: req.MaxAttempts,
			Provider: strings.TrimSpace(req.Provider),
			Model: strings.TrimSpace(req.Model),
			PromptVersion: strings.TrimSpace(req.PromptVersion),
			Trigger: strings.TrimSpace(req.Trigger),
		},
	)
	if err != nil {
		slog.Error("start brreg term translation workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status: "started",
		Workflow: brregworkflow.TranslateBrregSourceTermsWorkflowName,
		WorkflowID: workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}
```

Modify `corpscout/scheduler/internal/httpapi/handlers.go`:

```go
r.Post("/workflows/brreg/term-translation", h.handleStartBrregTermTranslationWorkflow)
```

- [x] **Step 4: Add UI API method**

Modify `corpscout/ui/app/lib/api.ts`:

```ts
translateBrregTerms: (
  body: {
    limit?: number;
    term_batch_size?: number;
    max_attempts?: number;
    provider?: string;
    model?: string;
    prompt_version?: string;
    trigger?: string;
  } = {},
) =>
  post<{ status: string; workflow_id: string; workflow_run_id?: string }>(
    "/workflows/brreg/term-translation",
    body,
  ),
```

Modify `corpscout/ui/app/components/app/BrregTranslationActionForm.tsx` so the source-entry action calls `api.translateBrregTerms` when `recordLabel === "source entries"` and keeps `api.translateBrreg` for raw input translation.

- [x] **Step 5: Run HTTP and UI checks**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run TestStartBrregTermTranslationWorkflowStartsTemporalWorkflow -count=1

cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
pnpm typecheck
pnpm build
```

Expected: PASS. UI build may print existing Vite sourcemap warnings but exits 0.

---

### Task 8: Add Term Translation Support to Python Translation Service

**Files:**
- Modify `data-pipelines/services/translation-service/src/corpscout_translation_service/models.py`
- Modify `data-pipelines/services/translation-service/src/corpscout_translation_service/service.py`
- Modify `data-pipelines/services/translation-service/src/corpscout_translation_service/nats_worker.py`
- Modify `data-pipelines/services/translation-service/src/corpscout_translation_service/api.py`
- Add `data-pipelines/services/translation-service/tests/test_term_translation.py`
- Modify `data-pipelines/services/translation-service/tests/test_nats_worker.py`

- [x] **Step 1: Write service test for term dictionary translation**

Create `data-pipelines/services/translation-service/tests/test_term_translation.py`:

```python
import pytest

from corpscout_translation_service.models import TermTranslationRequest, TermTranslationRequestTerm
from corpscout_translation_service.service import TranslationService


class FakeLLM:
    async def translate_terms(self, *, terms, source_lang, target_lang, provider, model, prompt_version):
        return {term: f"{term} EN" for term in terms}


@pytest.mark.asyncio
async def test_translate_terms_returns_one_result_per_term():
    service = TranslationService(llm=FakeLLM())
    response = await service.translate_terms(
        TermTranslationRequest(
            request_id="request-1",
            source="brreg",
            source_lang="no",
            target_lang="en",
            provider="default",
            model="qwen3:6b",
            prompt_version="v1",
            terms=[
                TermTranslationRequestTerm(
                    term_key="abc",
                    source_text="Aksjeselskap",
                    source_text_normalized="aksjeselskap",
                )
            ],
        )
    )

    assert response.results[0].translated_text == "Aksjeselskap EN"
    assert response.failures == []
```

- [x] **Step 2: Run test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/translation-service
uv run pytest tests/test_term_translation.py -q
```

Expected: FAIL because term models/service method do not exist.

- [x] **Step 3: Add Pydantic models**

Modify `data-pipelines/services/translation-service/src/corpscout_translation_service/models.py`:

```python
class TermTranslationRequestTerm(BaseModel):
    term_key: str
    source_text: str
    source_text_normalized: str


class TermTranslationRequest(BaseModel):
    request_id: str
    source: str = "brreg"
    source_lang: str = "no"
    target_lang: str = "en"
    provider: str = "default"
    model: str | None = None
    prompt_version: str = "v1"
    terms: list[TermTranslationRequestTerm]


class TermTranslationResultItem(BaseModel):
    term_key: str
    source_text: str
    source_text_normalized: str
    translated_text: str
    status: str = "succeeded"


class TermTranslationFailureResult(BaseModel):
    term_key: str
    source_text: str
    source_text_normalized: str
    status: str
    error_code: str | None = None
    error: str | None = None


class TermTranslationResponse(BaseModel):
    request_id: str
    source: str
    source_lang: str
    target_lang: str
    provider: str
    model: str | None = None
    prompt_version: str
    results: list[TermTranslationResultItem] = Field(default_factory=list)
    failures: list[TermTranslationFailureResult] = Field(default_factory=list)
```

- [x] **Step 4: Add service method**

Modify `data-pipelines/services/translation-service/src/corpscout_translation_service/service.py`:

```python
async def translate_terms(self, request: TermTranslationRequest) -> TermTranslationResponse:
    source_terms = [term.source_text for term in request.terms]
    translated = await self.llm.translate_terms(
        terms=source_terms,
        source_lang=request.source_lang,
        target_lang=request.target_lang,
        provider=request.provider,
        model=request.model,
        prompt_version=request.prompt_version,
    )
    results: list[TermTranslationResultItem] = []
    failures: list[TermTranslationFailureResult] = []
    for term in request.terms:
        translated_text = translated.get(term.source_text, "").strip()
        if translated_text:
            results.append(TermTranslationResultItem(
                term_key=term.term_key,
                source_text=term.source_text,
                source_text_normalized=term.source_text_normalized,
                translated_text=translated_text,
            ))
        else:
            failures.append(TermTranslationFailureResult(
                term_key=term.term_key,
                source_text=term.source_text,
                source_text_normalized=term.source_text_normalized,
                status="failed_retryable",
                error_code="missing_term_translation",
                error="translation service did not return a translated value for the term",
            ))
    return TermTranslationResponse(
        request_id=request.request_id,
        source=request.source,
        source_lang=request.source_lang,
        target_lang=request.target_lang,
        provider=request.provider,
        model=request.model,
        prompt_version=request.prompt_version,
        results=results,
        failures=failures,
    )
```

- [x] **Step 5: Add NATS worker subject handling**

Modify `data-pipelines/services/translation-service/src/corpscout_translation_service/nats_worker.py`:

```python
TERM_REQUEST_SUBJECT = "brreg.translation.terms.request"
TERM_RESULT_SUBJECT = "brreg.translation.terms.result"
```

Add handler:

```python
async def handle_term_translation_message(self, msg):
    request = TermTranslationRequest.model_validate_json(msg.data)
    response = await self.service.translate_terms(request)
    await self.nc.publish(TERM_RESULT_SUBJECT, response.model_dump_json().encode("utf-8"))
    await msg.ack()
```

Subscribe in worker startup:

```python
await js.subscribe(TERM_REQUEST_SUBJECT, cb=self.handle_term_translation_message, durable="translation-terms")
```

- [x] **Step 6: Add HTTP endpoint for direct smoke tests**

Modify `data-pipelines/services/translation-service/src/corpscout_translation_service/api.py`:

```python
@router.post("/v1/terms/translate", response_model=TermTranslationResponse)
async def translate_terms(request: TermTranslationRequest):
    return await service.translate_terms(request)
```

- [x] **Step 7: Run service tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/translation-service
uv run pytest tests/test_term_translation.py tests/test_nats_worker.py -q
```

Expected: PASS.

---

### Task 9: Wire Result Consumer Lifecycle in Scheduler

**Files:**
- Modify `corpscout/scheduler/internal/app/server.go`
- Modify `corpscout/scheduler/internal/app/temporal.go`
- Modify `corpscout/scheduler/internal/config/config.go`
- Modify `corpscout/scheduler/internal/config/config_test.go`

- [x] **Step 1: Add config values**

Modify `corpscout/scheduler/internal/config/config.go`:

```go
BRREGTermTranslationResultSubject string
```

Read env:

```go
BRREGTermTranslationResultSubject: getenv("CORPSCOUT_BRREG_TERM_TRANSLATION_RESULT_SUBJECT", translationclient.DefaultTermTranslationResultSubject),
```

- [x] **Step 2: Add consumer start/stop to server**

Modify `corpscout/scheduler/internal/app/server.go` to start one goroutine after temporal workers start:

```go
termConsumer := brregactions.NewTermTranslationResultConsumer(gateway, natsConn, cfg.BRREGTermTranslationResultSubject)
subscription, err := natsConn.Subscribe(cfg.BRREGTermTranslationResultSubject, func(msg *nats.Msg) {
	if err := termConsumer.HandleMessage(context.Background(), msg); err != nil {
		slog.Error("handle brreg term translation result", "error", err)
	}
})
```

On server shutdown:

```go
if subscription != nil {
	_ = subscription.Unsubscribe()
}
```

- [x] **Step 3: Run app/config tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/config ./internal/app -count=1
```

Expected: PASS.

---

### Task 10: Switch BRREG Source UI Stats to Term Translation

**Files:**
- Modify `corpscout/database/queries/brreg_source_profile.sql`
- Regenerate sqlc files
- Modify `corpscout/scheduler/internal/httpapi/brreg_source_entries.go`
- Modify `corpscout/scheduler/internal/httpapi/brreg_source_entries_test.go`
- Modify `corpscout/ui/app/types/api.ts`
- Modify `corpscout/ui/app/components/app/BrregSourceEntriesTable.tsx`

- [x] **Step 1: Update source entry projection query**

Modify source entry SQL so translation stats are:

```sql
translation_missing_count = count rows from brreg_source.v_missing_translation_fields by company_id
translation_pending_count = count distinct pending/queued terms matching missing fields
translation_succeeded_count = count distinct succeeded terms matching source company fields
translation_running_count = 0
translation_failed_count = count distinct failed_retryable/failed_terminal terms matching missing fields
```

- [x] **Step 2: Add failed count to API type**

Modify `corpscout/ui/app/types/api.ts`:

```ts
translation_failed_count: number;
```

- [x] **Step 3: Update UI badge**

Modify `corpscout/ui/app/components/app/BrregSourceEntriesTable.tsx` so the translation state shows:

```text
N missing
N queued
N done
N failed
```

Remove dependence on `translation_running_count` for term workflow display.

- [x] **Step 4: Run API/UI tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run TestListBrregSourceEntries -count=1

cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
pnpm typecheck
pnpm build
```

Expected: PASS.

---

### Task 11: Disable Old BRREG Source `translate_field` Task Creation

**Files:**
- Modify `corpscout/database/queries/brreg_source_profile.sql`
- Modify `corpscout/scheduler/internal/brreg/actions/translation_actions.go`
- Modify `corpscout/scheduler/internal/brreg/actions/source_translation_actions_test.go`
- Modify `corpscout/scheduler/internal/brreg/workflow/translation.go`

- [x] **Step 1: Stop source-entry translation action from calling old workflow**

Keep raw input translation workflow for legacy raw records if needed. For BRREG source entries, the UI must call `/api/v1/workflows/brreg/term-translation`.

- [x] **Step 2: Remove or isolate `PrepareBrregSourceTranslationTasks` usage**

Modify `corpscout/scheduler/internal/brreg/actions/translation_actions.go` so `PrepareBrregTranslationWorkflow` no longer prepares `brreg_source.action_tasks` when invoked from the source-entry UI path. The source-entry UI no longer invokes this function after Task 7, so no new behavior is needed here beyond test cleanup.

- [x] **Step 3: Add safety test**

Add a test to `corpscout/scheduler/internal/httpapi/workflow_triggers_test.go` verifying source-entry translation action endpoint is `/workflows/brreg/term-translation`, while raw input action remains `/workflows/brreg/translation`.

- [x] **Step 4: Run full scheduler tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./...
```

Expected: PASS.

---

### Task 12: End-to-End Verification

**Files:**
- No production file changes.

- [ ] **Step 1: Reset a small translation sample**

Run against local/dev database:

```sql
UPDATE brreg_source.companies
SET organization_form_label_en = NULL,
    response_class_en = NULL
WHERE id IN (
  SELECT id FROM brreg_source.companies ORDER BY updated_at DESC LIMIT 20
);
```

- [ ] **Step 2: Start services**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
docker compose up -d scheduler ui

cd /Users/graovic/pulsarpoint/ppoint/companycollect/deploy/services
docker compose up -d nats translation-service translation-worker
```

- [ ] **Step 3: Trigger term translation**

Use UI:

```text
http://localhost:8094/sources/brreg/source_entries
Action -> Translation -> Start translation
```

Or API:

```bash
curl -X POST http://localhost:8094/api/v1/workflows/brreg/term-translation \
  -H 'Content-Type: application/json' \
  -d '{"limit":1000,"term_batch_size":100,"max_attempts":3,"provider":"default","prompt_version":"v1"}'
```

- [ ] **Step 4: Verify cache and target writes**

Run:

```sql
SELECT status, count(*) FROM brreg_source.translation_terms GROUP BY status ORDER BY status;
SELECT count(*) FROM brreg_source.v_missing_translation_fields;
SELECT organization_form_label, organization_form_label_en, response_class, response_class_en
FROM brreg_source.companies
WHERE organization_form_label IS NOT NULL
ORDER BY updated_at DESC
LIMIT 20;
```

Expected:
- `translation_terms` has `succeeded` rows.
- `v_missing_translation_fields` decreases.
- repeated values like `Aksjeselskap` are translated once and applied to many rows.

---

## Final Verification

Run all checks:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./...

cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
pnpm typecheck
pnpm build

cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/translation-service
uv run pytest -q

cd /Users/graovic/pulsarpoint/ppoint/companycollect
git diff --check
```

Expected:
- Go tests pass.
- UI typecheck/build exit 0.
- Translation-service tests pass.
- `git diff --check` prints no whitespace errors.

---

## Self-Review

- Spec coverage: The plan covers the DB term cache, missing-field source of truth, whitelisted target writes, NATS request/result contracts, Temporal workflow, translation-service support, UI switch, old action-task removal, and end-to-end verification.
- Completeness scan: No implementation task contains unresolved filler text. Each task names exact files and commands.
- Type consistency: The plan consistently uses `TranslateBrregSourceTerms`, `translation_terms`, `v_missing_translation_fields`, `TermTranslationRequest`, and `TermTranslationResult`.
