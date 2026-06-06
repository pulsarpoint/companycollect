# JetStream Translation Buffer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace request/reply term translation with a JetStream-backed buffer so the translation service always has a next batch ready while Postgres remains the queue source of truth.

**Architecture:** Temporal workflows prepare Postgres translation queues. A scheduler background dispatcher keeps each source topped up to two running/published batches by claiming Postgres rows and publishing JetStream jobs. A scheduler result collector consumes JetStream results, saves terms, applies bindings, and completes or releases Postgres batches. The Python translation service pull-consumes one JetStream job at a time, acks early after payload validation, calls the LLM, publishes a result, and moves to the next job.

**Tech Stack:** Go, PostgreSQL/sqlc, pgx, NATS JetStream, Temporal, React UI config surface, Python asyncio, `nats.py`, Pydantic.

---

## File Structure

Scheduler files:

- Create `scheduler/internal/translationqueue/contracts.go`: shared Go job/result payload structs and subjects.
- Create `scheduler/internal/translationqueue/jetstream.go`: concrete JetStream publisher/pull-consumer helper.
- Create `scheduler/internal/translationqueue/source.go`: source adapter interface and source registry for BRREG and Ariregister.
- Create `scheduler/internal/translationqueue/dispatcher.go`: source buffer refill loop.
- Create `scheduler/internal/translationqueue/result_collector.go`: result handling loop and idempotent Postgres completion.
- Create `scheduler/internal/translationqueue/service.go`: lifecycle wrapper that starts dispatcher and result collector.
- Modify `database/migrations/000104_source_translation_queue_job_config.up.sql`: persist provider/model/prompt/source language on queue rows.
- Modify `database/migrations/000104_source_translation_queue_job_config.down.sql`: remove those columns.
- Modify `database/queries/brreg_translation_queue.sql` and `database/queries/ariregister_translation_queue.sql`: insert job config and claim batches with homogeneous config.
- Regenerate `scheduler/internal/db/gen/*translation_queue.sql.go` with sqlc.
- Modify `scheduler/internal/brreg/companydata/translation_queue.go` and `scheduler/internal/ariregister/companydata/translation_queue.go`: expose source buffer target and job config in queue commands/results.
- Modify `scheduler/internal/brreg/actions/company_translation_workset_actions.go` and `scheduler/internal/ariregister/actions/company_translation_workset_actions.go`: pass provider/model into queue preparation, and keep batch apply helpers reusable.
- Modify `scheduler/internal/brreg/workflow/company_translation.go` and `scheduler/internal/ariregister/workflow/company_translation.go`: prepare queues and return `queued` instead of synchronously calling the LLM.
- Modify `scheduler/internal/config/config.go`: add JetStream buffer config.
- Modify `scheduler/internal/app/server.go`: start and stop the translation queue service.

Translation-service files:

- Modify `../data-pipelines/services/translation-service/src/corpscout_translation_service/models.py`: add JetStream job/result models.
- Modify `../data-pipelines/services/translation-service/src/corpscout_translation_service/nats_worker.py`: add pull consumer loop for JetStream jobs and result publishing.
- Modify `../data-pipelines/services/translation-service/tests/test_nats_worker.py`: cover early ack, sequential processing, and result publish.

---

### Task 1: Add Shared Translation Job Contracts

**Files:**
- Create: `scheduler/internal/translationqueue/contracts.go`
- Test: `scheduler/internal/translationqueue/contracts_test.go`
- Modify: `../data-pipelines/services/translation-service/src/corpscout_translation_service/models.py`
- Test: `../data-pipelines/services/translation-service/tests/test_models.py`

- [ ] **Step 1: Write Go contract tests**

Create `scheduler/internal/translationqueue/contracts_test.go`:

```go
package translationqueue

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTranslationJobPayloadJSONShape(t *testing.T) {
	payload := TranslationJob{
		JobID:         "job-1",
		BatchID:       "workflow/batch/000001",
		Source:        "brreg",
		SourceLang:    "no",
		TargetLang:    "en",
		Provider:      "deepseek",
		Model:         "deepseek-chat",
		PromptVersion: "v1",
		CompanyIDs:    []string{"company-a"},
		Terms: []TranslationJobTerm{{
			TermKey:              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
		}},
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"job_id":"job-1",
		"batch_id":"workflow/batch/000001",
		"source":"brreg",
		"source_lang":"no",
		"target_lang":"en",
		"provider":"deepseek",
		"model":"deepseek-chat",
		"prompt_version":"v1",
		"company_ids":["company-a"],
		"terms":[{
			"term_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"source_text":"Aksjeselskap",
			"source_text_normalized":"aksjeselskap"
		}]
	}`, string(body))
}

func TestTranslationResultPayloadJSONShape(t *testing.T) {
	payload := TranslationResult{
		JobID:         "job-1",
		BatchID:       "workflow/batch/000001",
		Source:        "brreg",
		Status:        "succeeded",
		Provider:      "deepseek",
		Model:         "deepseek-chat",
		PromptVersion: "v1",
		CompanyIDs:    []string{"company-a"},
		DurationMS:    1234,
		Results: []TranslationResultTerm{{
			TermKey:              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TranslatedText:       "Limited liability company",
			Status:               "succeeded",
		}},
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"job_id":"job-1",
		"batch_id":"workflow/batch/000001",
		"source":"brreg",
		"status":"succeeded",
		"provider":"deepseek",
		"model":"deepseek-chat",
		"prompt_version":"v1",
		"company_ids":["company-a"],
		"duration_ms":1234,
		"results":[{
			"term_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"source_text":"Aksjeselskap",
			"source_text_normalized":"aksjeselskap",
			"translated_text":"Limited liability company",
			"status":"succeeded"
		}],
		"failures":null
	}`, string(body))
}
```

- [ ] **Step 2: Run Go contract tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/translationqueue -run 'TestTranslation(Job|Result)PayloadJSONShape' -count=1
```

Expected: FAIL because `scheduler/internal/translationqueue` and payload types do not exist.

- [ ] **Step 3: Implement Go contracts**

Create `scheduler/internal/translationqueue/contracts.go`:

```go
package translationqueue

const (
	JobsSubject    = "source.translation.jobs"
	ResultsSubject = "source.translation.results"
	StreamName     = "SOURCE_TRANSLATION"
)

type TranslationJob struct {
	JobID         string               `json:"job_id"`
	BatchID       string               `json:"batch_id"`
	Source        string               `json:"source"`
	SourceLang    string               `json:"source_lang"`
	TargetLang    string               `json:"target_lang"`
	Provider      string               `json:"provider"`
	Model         string               `json:"model,omitempty"`
	PromptVersion string               `json:"prompt_version"`
	CompanyIDs    []string             `json:"company_ids"`
	Terms         []TranslationJobTerm `json:"terms"`
}

type TranslationJobTerm struct {
	TermKey              string `json:"term_key"`
	SourceText           string `json:"source_text"`
	SourceTextNormalized string `json:"source_text_normalized"`
}

type TranslationResult struct {
	JobID         string                     `json:"job_id"`
	BatchID       string                     `json:"batch_id"`
	Source        string                     `json:"source"`
	Status        string                     `json:"status"`
	Provider      string                     `json:"provider"`
	Model         string                     `json:"model,omitempty"`
	PromptVersion string                     `json:"prompt_version"`
	CompanyIDs    []string                   `json:"company_ids"`
	DurationMS    int                        `json:"duration_ms"`
	Results       []TranslationResultTerm    `json:"results"`
	Failures      []TranslationFailureResult `json:"failures"`
}

type TranslationResultTerm struct {
	TermKey              string `json:"term_key"`
	SourceText           string `json:"source_text"`
	SourceTextNormalized string `json:"source_text_normalized"`
	TranslatedText       string `json:"translated_text"`
	Status               string `json:"status"`
}

type TranslationFailureResult struct {
	TermKey              string `json:"term_key"`
	SourceText           string `json:"source_text"`
	SourceTextNormalized string `json:"source_text_normalized"`
	Status               string `json:"status"`
	ErrorCode            string `json:"error_code,omitempty"`
	Error                string `json:"error,omitempty"`
}
```

- [ ] **Step 4: Run Go contract tests and verify they pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/translationqueue -run 'TestTranslation(Job|Result)PayloadJSONShape' -count=1
```

Expected: PASS.

- [ ] **Step 5: Write Python model tests**

Append to `../data-pipelines/services/translation-service/tests/test_models.py`:

```python
from corpscout_translation_service.models import (
    JetStreamTranslationJob,
    JetStreamTranslationJobTerm,
    JetStreamTranslationResult,
    JetStreamTranslationResultItem,
)


def test_jetstream_translation_job_model_accepts_scheduler_payload() -> None:
    job = JetStreamTranslationJob.model_validate(
        {
            "job_id": "job-1",
            "batch_id": "workflow/batch/000001",
            "source": "brreg",
            "source_lang": "no",
            "target_lang": "en",
            "provider": "deepseek",
            "model": "deepseek-chat",
            "prompt_version": "v1",
            "company_ids": ["company-a"],
            "terms": [
                {
                    "term_key": "a" * 64,
                    "source_text": "Aksjeselskap",
                    "source_text_normalized": "aksjeselskap",
                }
            ],
        }
    )

    assert job.terms == [
        JetStreamTranslationJobTerm(
            term_key="a" * 64,
            source_text="Aksjeselskap",
            source_text_normalized="aksjeselskap",
        )
    ]


def test_jetstream_translation_result_model_emits_scheduler_payload() -> None:
    result = JetStreamTranslationResult(
        job_id="job-1",
        batch_id="workflow/batch/000001",
        source="brreg",
        status="succeeded",
        provider="deepseek",
        model="deepseek-chat",
        prompt_version="v1",
        company_ids=["company-a"],
        duration_ms=1234,
        results=[
            JetStreamTranslationResultItem(
                term_key="a" * 64,
                source_text="Aksjeselskap",
                source_text_normalized="aksjeselskap",
                translated_text="Limited liability company",
            )
        ],
    )

    dumped = result.model_dump(exclude_none=True)
    assert dumped["company_ids"] == ["company-a"]
    assert dumped["results"][0]["status"] == "succeeded"
```

- [ ] **Step 6: Run Python model tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/data-pipelines/services/translation-service
uv run pytest tests/test_models.py -q
```

Expected: FAIL because `JetStreamTranslationJob` and related classes do not exist.

- [ ] **Step 7: Implement Python models**

Append to `../data-pipelines/services/translation-service/src/corpscout_translation_service/models.py`:

```python
JetStreamResultStatus = Literal["succeeded", "partial", "failed"]


class JetStreamTranslationJobTerm(BaseModel):
    term_key: str = Field(pattern=TERM_KEY_PATTERN)
    source_text: str = Field(min_length=1)
    source_text_normalized: str = Field(min_length=1)


class JetStreamTranslationJob(BaseModel):
    job_id: str = Field(min_length=1)
    batch_id: str = Field(min_length=1)
    source: str = Field(min_length=1)
    source_lang: str = Field(min_length=2)
    target_lang: str = Field(min_length=2)
    provider: str = Field(default="default", min_length=1)
    model: str | None = None
    prompt_version: str = Field(default="v1", min_length=1)
    company_ids: list[str] = Field(min_length=1)
    terms: list[JetStreamTranslationJobTerm] = Field(min_length=1)

    @model_validator(mode="after")
    def reject_duplicate_term_keys(self) -> "JetStreamTranslationJob":
        term_keys = [term.term_key for term in self.terms]
        if len(set(term_keys)) != len(term_keys):
            raise ValueError("terms must not contain duplicate term_key values")
        return self


class JetStreamTranslationResultItem(BaseModel):
    term_key: str = Field(pattern=TERM_KEY_PATTERN)
    source_text: str = Field(min_length=1)
    source_text_normalized: str = Field(min_length=1)
    translated_text: str
    status: TermResultStatus = "succeeded"


class JetStreamTranslationFailureItem(BaseModel):
    term_key: str = Field(pattern=TERM_KEY_PATTERN)
    source_text: str = Field(min_length=1)
    source_text_normalized: str = Field(min_length=1)
    status: TermFailureStatus = "failed_retryable"
    error_code: str | None = None
    error: str | None = None


class JetStreamTranslationResult(BaseModel):
    job_id: str = Field(min_length=1)
    batch_id: str = Field(min_length=1)
    source: str = Field(min_length=1)
    status: JetStreamResultStatus
    provider: str = Field(default="default", min_length=1)
    model: str | None = None
    prompt_version: str = Field(default="v1", min_length=1)
    company_ids: list[str] = Field(default_factory=list)
    duration_ms: int = Field(ge=0)
    results: list[JetStreamTranslationResultItem] = Field(default_factory=list)
    failures: list[JetStreamTranslationFailureItem] = Field(default_factory=list)
```

- [ ] **Step 8: Run Python model tests and verify they pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/data-pipelines/services/translation-service
uv run pytest tests/test_models.py -q
```

Expected: PASS.

- [ ] **Step 9: Commit contracts**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/translationqueue/contracts.go scheduler/internal/translationqueue/contracts_test.go ../data-pipelines/services/translation-service/src/corpscout_translation_service/models.py ../data-pipelines/services/translation-service/tests/test_models.py
git commit -m "feat: add source translation jetstream contracts"
```

Expected: commit succeeds.

---

### Task 2: Persist Translation Job Config On Queue Entries

**Files:**
- Create: `database/migrations/000104_source_translation_queue_job_config.up.sql`
- Create: `database/migrations/000104_source_translation_queue_job_config.down.sql`
- Modify: `database/queries/brreg_translation_queue.sql`
- Modify: `database/queries/ariregister_translation_queue.sql`
- Modify: `scheduler/internal/brreg/companydata/translation_queue.go`
- Modify: `scheduler/internal/ariregister/companydata/translation_queue.go`
- Test: `scheduler/internal/db/gen/translation_queue_query_shape_test.go`
- Test: `scheduler/internal/brreg/companydata/workset_test.go`
- Test: `scheduler/internal/ariregister/companydata/workset_test.go`

- [ ] **Step 1: Write migration shape test**

Create `scheduler/internal/db/source_translation_queue_job_config_migration_test.go`:

```go
package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceTranslationQueueJobConfigMigrationAddsDispatchMetadata(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000104_source_translation_queue_job_config.up.sql")
	require.NoError(t, err)
	sql := string(body)

	for _, schema := range []string{"brreg_source", "ariregister_source"} {
		require.Contains(t, sql, "ALTER TABLE "+schema+".translation_queue_entries")
		require.Contains(t, sql, "ADD COLUMN provider text NOT NULL DEFAULT 'default'")
		require.Contains(t, sql, "ADD COLUMN model text NOT NULL DEFAULT ''")
		require.Contains(t, sql, "ADD COLUMN prompt_version text NOT NULL DEFAULT 'v1'")
		require.Contains(t, sql, "ADD COLUMN source_lang text NOT NULL")
		require.Contains(t, sql, "ADD COLUMN target_lang text NOT NULL DEFAULT 'en'")
	}
}
```

- [ ] **Step 2: Run migration shape test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/db -run TestSourceTranslationQueueJobConfigMigrationAddsDispatchMetadata -count=1
```

Expected: FAIL because migration `000104` does not exist.

- [ ] **Step 3: Add queue metadata migration**

Create `database/migrations/000104_source_translation_queue_job_config.up.sql`:

```sql
ALTER TABLE brreg_source.translation_queue_entries
  ADD COLUMN provider text NOT NULL DEFAULT 'default',
  ADD COLUMN model text NOT NULL DEFAULT '',
  ADD COLUMN prompt_version text NOT NULL DEFAULT 'v1',
  ADD COLUMN source_lang text NOT NULL DEFAULT 'no',
  ADD COLUMN target_lang text NOT NULL DEFAULT 'en';

ALTER TABLE ariregister_source.translation_queue_entries
  ADD COLUMN provider text NOT NULL DEFAULT 'default',
  ADD COLUMN model text NOT NULL DEFAULT '',
  ADD COLUMN prompt_version text NOT NULL DEFAULT 'v1',
  ADD COLUMN source_lang text NOT NULL DEFAULT 'et',
  ADD COLUMN target_lang text NOT NULL DEFAULT 'en';

CREATE INDEX idx_brreg_source_translation_queue_pending_config
  ON brreg_source.translation_queue_entries(status, provider, model, prompt_version, source_lang, target_lang, status_changed_at, company_id)
  WHERE status = 'pending';

CREATE INDEX idx_ariregister_source_translation_queue_pending_config
  ON ariregister_source.translation_queue_entries(status, provider, model, prompt_version, source_lang, target_lang, status_changed_at, company_id)
  WHERE status = 'pending';
```

Create `database/migrations/000104_source_translation_queue_job_config.down.sql`:

```sql
DROP INDEX IF EXISTS ariregister_source.idx_ariregister_source_translation_queue_pending_config;
DROP INDEX IF EXISTS brreg_source.idx_brreg_source_translation_queue_pending_config;

ALTER TABLE ariregister_source.translation_queue_entries
  DROP COLUMN IF EXISTS target_lang,
  DROP COLUMN IF EXISTS source_lang,
  DROP COLUMN IF EXISTS prompt_version,
  DROP COLUMN IF EXISTS model,
  DROP COLUMN IF EXISTS provider;

ALTER TABLE brreg_source.translation_queue_entries
  DROP COLUMN IF EXISTS target_lang,
  DROP COLUMN IF EXISTS source_lang,
  DROP COLUMN IF EXISTS prompt_version,
  DROP COLUMN IF EXISTS model,
  DROP COLUMN IF EXISTS provider;
```

- [ ] **Step 4: Update queue SQL shape tests**

Extend `scheduler/internal/db/gen/translation_queue_query_shape_test.go` with:

```go
func TestTranslationQueuePreparePersistsDispatchConfig(t *testing.T) {
	files := []string{
		"../../../../database/queries/brreg_translation_queue.sql",
		"../../../../database/queries/ariregister_translation_queue.sql",
	}
	for _, file := range files {
		body, err := os.ReadFile(file)
		require.NoError(t, err)
		sql := string(body)
		require.Contains(t, sql, "provider, model, prompt_version, source_lang, target_lang")
		require.Contains(t, sql, "sqlc.arg('provider')::text")
		require.Contains(t, sql, "sqlc.arg('model')::text")
		require.Contains(t, sql, "sqlc.arg('prompt_version')::text")
		require.Contains(t, sql, "sqlc.arg('source_lang')::text")
		require.Contains(t, sql, "sqlc.arg('target_lang')::text")
	}
}

func TestTranslationQueueClaimUsesHomogeneousDispatchConfig(t *testing.T) {
	files := []string{
		"../../../../database/queries/brreg_translation_queue.sql",
		"../../../../database/queries/ariregister_translation_queue.sql",
	}
	for _, file := range files {
		body, err := os.ReadFile(file)
		require.NoError(t, err)
		sql := string(body)
		require.Contains(t, sql, "first_config AS")
		require.Contains(t, sql, "pending.provider = first_config.provider")
		require.Contains(t, sql, "pending.model = first_config.model")
		require.Contains(t, sql, "pending.prompt_version = first_config.prompt_version")
		require.Contains(t, sql, "RETURNING queue.company_id, queue.num_of_characters, queue.provider, queue.model, queue.prompt_version, queue.source_lang, queue.target_lang")
	}
}
```

- [ ] **Step 5: Run queue SQL tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/db ./internal/db/gen -run 'TestSourceTranslationQueueJobConfigMigrationAddsDispatchMetadata|TestTranslationQueue(PreparePersistsDispatchConfig|ClaimUsesHomogeneousDispatchConfig)' -count=1
```

Expected: FAIL because SQL files do not include config fields.

- [ ] **Step 6: Update prepare queue SQL**

In both `database/queries/brreg_translation_queue.sql` and `database/queries/ariregister_translation_queue.sql`, update `INSERT INTO ... translation_queue_entries` to include dispatch config:

```sql
  INSERT INTO brreg_source.translation_queue_entries (
    company_id, status, num_of_characters, batch_id,
    provider, model, prompt_version, source_lang, target_lang,
    status_changed_at, created_at, updated_at
  )
  SELECT
    selected.company_id,
    'pending',
    selected.num_of_characters,
    NULL,
    COALESCE(NULLIF(sqlc.arg('provider')::text, ''), 'default'),
    COALESCE(sqlc.arg('model')::text, ''),
    COALESCE(NULLIF(sqlc.arg('prompt_version')::text, ''), 'v1'),
    COALESCE(NULLIF(sqlc.arg('source_lang')::text, ''), 'no'),
    COALESCE(NULLIF(sqlc.arg('target_lang')::text, ''), 'en'),
    now(),
    now(),
    now()
  FROM limited selected
  ON CONFLICT DO NOTHING
```

Ariregister uses the same columns with the Ariregister source table and Estonian source-language default:

```sql
  INSERT INTO ariregister_source.translation_queue_entries (
    company_id, status, num_of_characters, batch_id,
    provider, model, prompt_version, source_lang, target_lang,
    status_changed_at, created_at, updated_at
  )
  SELECT
    selected.company_id,
    'pending',
    selected.num_of_characters,
    NULL,
    COALESCE(NULLIF(sqlc.arg('provider')::text, ''), 'default'),
    COALESCE(sqlc.arg('model')::text, ''),
    COALESCE(NULLIF(sqlc.arg('prompt_version')::text, ''), 'v1'),
    COALESCE(NULLIF(sqlc.arg('source_lang')::text, ''), 'et'),
    COALESCE(NULLIF(sqlc.arg('target_lang')::text, ''), 'en'),
    now(),
    now(),
    now()
  FROM limited selected
  ON CONFLICT DO NOTHING
```

- [ ] **Step 7: Update claim queue SQL**

In both queue SQL files, replace the claim query CTE with the homogeneous config shape. BRREG version:

```sql
-- name: ClaimBrregTranslationQueueBatch :many
WITH first_config AS (
  SELECT provider, model, prompt_version, source_lang, target_lang
  FROM brreg_source.translation_queue_entries
  WHERE status = 'pending'
  ORDER BY status_changed_at ASC, company_id ASC
  LIMIT 1
),
pending AS (
  SELECT pending.id, pending.company_id, pending.num_of_characters, pending.status_changed_at
  FROM brreg_source.translation_queue_entries pending
  JOIN first_config
    ON pending.provider = first_config.provider
   AND pending.model = first_config.model
   AND pending.prompt_version = first_config.prompt_version
   AND pending.source_lang = first_config.source_lang
   AND pending.target_lang = first_config.target_lang
  WHERE pending.status = 'pending'
  ORDER BY pending.status_changed_at ASC, pending.company_id ASC
  LIMIT GREATEST(sqlc.arg('max_candidate_rows')::integer, 1)
  FOR UPDATE SKIP LOCKED
),
ranked AS (
  SELECT
    id,
    company_id,
    num_of_characters,
    sum(num_of_characters) OVER (ORDER BY status_changed_at ASC, company_id ASC) AS running_chars,
    row_number() OVER (ORDER BY status_changed_at ASC, company_id ASC) AS row_number
  FROM pending
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
  RETURNING queue.company_id, queue.num_of_characters, queue.provider, queue.model, queue.prompt_version, queue.source_lang, queue.target_lang
)
SELECT company_id, num_of_characters, provider, model, prompt_version, source_lang, target_lang
FROM updated
ORDER BY company_id ASC;
```

Ariregister claim query:

```sql
-- name: ClaimAriregisterTranslationQueueBatch :many
WITH first_config AS (
  SELECT provider, model, prompt_version, source_lang, target_lang
  FROM ariregister_source.translation_queue_entries
  WHERE status = 'pending'
  ORDER BY status_changed_at ASC, company_id ASC
  LIMIT 1
),
pending AS (
  SELECT pending.id, pending.company_id, pending.num_of_characters, pending.status_changed_at
  FROM ariregister_source.translation_queue_entries pending
  JOIN first_config
    ON pending.provider = first_config.provider
   AND pending.model = first_config.model
   AND pending.prompt_version = first_config.prompt_version
   AND pending.source_lang = first_config.source_lang
   AND pending.target_lang = first_config.target_lang
  WHERE pending.status = 'pending'
  ORDER BY pending.status_changed_at ASC, pending.company_id ASC
  LIMIT GREATEST(sqlc.arg('max_candidate_rows')::integer, 1)
  FOR UPDATE SKIP LOCKED
),
ranked AS (
  SELECT
    id,
    company_id,
    num_of_characters,
    sum(num_of_characters) OVER (ORDER BY status_changed_at ASC, company_id ASC) AS running_chars,
    row_number() OVER (ORDER BY status_changed_at ASC, company_id ASC) AS row_number
  FROM pending
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
  RETURNING queue.company_id, queue.num_of_characters, queue.provider, queue.model, queue.prompt_version, queue.source_lang, queue.target_lang
)
SELECT company_id, num_of_characters, provider, model, prompt_version, source_lang, target_lang
FROM updated
ORDER BY company_id ASC;
```

- [ ] **Step 8: Regenerate sqlc**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
sqlc generate
```

Expected: generated files under `scheduler/internal/db/gen` update with new params and row fields.

- [ ] **Step 9: Update companydata commands and results**

In both `scheduler/internal/brreg/companydata/translation_queue.go` and `scheduler/internal/ariregister/companydata/translation_queue.go`, extend commands/results:

```go
type PrepareTranslationQueueCommand struct {
	IDs           []string
	Filters       map[string]string
	CompanyLimit  int32
	Provider      string
	Model         string
	PromptVersion string
	SourceLang    string
	TargetLang    string
}

type ClaimTranslationQueueBatchCommand struct {
	BatchID          string
	MaxCandidateRows int32
	MaxRequestChars  int32
	MaxSourceRunning int32
}

type ClaimTranslationQueueBatchResult struct {
	Status         string
	BatchID        string
	CompanyIDs     []string
	EstimatedChars int32
	Provider       string
	Model          string
	PromptVersion  string
	SourceLang     string
	TargetLang     string
}
```

Normalize defaults:

```go
func normalizePrepareTranslationQueueCommand(command PrepareTranslationQueueCommand, defaultSourceLang string) PrepareTranslationQueueCommand {
	command.Provider = strings.TrimSpace(command.Provider)
	if command.Provider == "" {
		command.Provider = "default"
	}
	command.Model = strings.TrimSpace(command.Model)
	command.PromptVersion = strings.TrimSpace(command.PromptVersion)
	if command.PromptVersion == "" {
		command.PromptVersion = "v1"
	}
	command.SourceLang = strings.TrimSpace(command.SourceLang)
	if command.SourceLang == "" {
		command.SourceLang = defaultSourceLang
	}
	command.TargetLang = strings.TrimSpace(command.TargetLang)
	if command.TargetLang == "" {
		command.TargetLang = "en"
	}
	return command
}
```

Change claim capacity to source-only:

```go
if command.MaxSourceRunning <= 0 {
	command.MaxSourceRunning = 2
}

func canClaimTranslationQueueBatch(counts translationQueueRunningCounts, command ClaimTranslationQueueBatchCommand) bool {
	return counts.SourceRunning < command.MaxSourceRunning
}
```

When folding returned rows, copy config from the first row:

```go
for index, row := range rows {
	if index == 0 {
		result.Provider = row.Provider
		result.Model = row.Model
		result.PromptVersion = row.PromptVersion
		result.SourceLang = row.SourceLang
		result.TargetLang = row.TargetLang
	}
	result.CompanyIDs = append(result.CompanyIDs, row.CompanyID.String())
	result.EstimatedChars += row.NumOfCharacters
}
```

- [ ] **Step 10: Update companydata tests**

In both source `workset_test.go` files, update prepare command calls that need non-default config:

```go
prepared, err := store.PrepareTranslationQueue(t.Context(), PrepareTranslationQueueCommand{
	IDs:           []string{first.CompanyID.String(), second.CompanyID.String()},
	CompanyLimit:  10,
	Provider:      "deepseek",
	Model:         "deepseek-chat",
	PromptVersion: "v2",
})
```

After the first claim, assert config:

```go
require.Equal(t, "deepseek", claimed.Provider)
require.Equal(t, "deepseek-chat", claimed.Model)
require.Equal(t, "v2", claimed.PromptVersion)
require.Equal(t, "en", claimed.TargetLang)
```

For BRREG assert:

```go
require.Equal(t, "no", claimed.SourceLang)
```

For Ariregister assert:

```go
require.Equal(t, "et", claimed.SourceLang)
```

Update the capacity test to use `MaxSourceRunning`:

```go
command := normalizeClaimTranslationQueueBatchCommand(ClaimTranslationQueueBatchCommand{
	BatchID:          "brreg-capacity-batch",
	MaxSourceRunning: 2,
})

require.True(t, canClaimTranslationQueueBatch(translationQueueRunningCounts{SourceRunning: 1}, command))
require.False(t, canClaimTranslationQueueBatch(translationQueueRunningCounts{SourceRunning: 2}, command))
```

- [ ] **Step 11: Run queue tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/db ./internal/db/gen ./internal/brreg/companydata ./internal/ariregister/companydata -run 'TestSourceTranslationQueueJobConfigMigrationAddsDispatchMetadata|TestTranslationQueue(PreparePersistsDispatchConfig|ClaimUsesHomogeneousDispatchConfig)|TestStoreTranslationQueuePreparesClaimsAndCompletes|TestTranslationQueueCapacity' -count=1
```

Expected: PASS.

- [ ] **Step 12: Commit queue metadata**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add database/migrations/000104_source_translation_queue_job_config.* database/queries/*translation_queue.sql scheduler/internal/db/gen/*translation_queue.sql.go scheduler/internal/db/*job_config*_test.go scheduler/internal/db/gen/translation_queue_query_shape_test.go scheduler/internal/brreg/companydata/translation_queue.go scheduler/internal/ariregister/companydata/translation_queue.go scheduler/internal/brreg/companydata/workset_test.go scheduler/internal/ariregister/companydata/workset_test.go
git commit -m "feat: persist translation queue dispatch config"
```

Expected: commit succeeds.

---

### Task 3: Add Source Queue Adapter API

**Files:**
- Create: `scheduler/internal/translationqueue/source.go`
- Test: `scheduler/internal/translationqueue/source_test.go`
- Modify: `scheduler/internal/brreg/actions/company_translation_workset_actions.go`
- Modify: `scheduler/internal/ariregister/actions/company_translation_workset_actions.go`

- [ ] **Step 1: Write source adapter tests**

Create `scheduler/internal/translationqueue/source_test.go`:

```go
package translationqueue

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceRegistryFindsConfiguredSources(t *testing.T) {
	registry := NewSourceRegistry([]SourceQueue{
		sourceQueueStub{name: "brreg"},
		sourceQueueStub{name: "ariregister"},
	})

	source, ok := registry.Get("ariregister")
	require.True(t, ok)
	require.Equal(t, "ariregister", source.Name())

	_, ok = registry.Get("unknown")
	require.False(t, ok)
}

func TestSourceRegistryListsSourcesInConfiguredOrder(t *testing.T) {
	registry := NewSourceRegistry([]SourceQueue{
		sourceQueueStub{name: "brreg"},
		sourceQueueStub{name: "ariregister"},
	})

	require.Equal(t, []string{"brreg", "ariregister"}, registry.Names())
}

type sourceQueueStub struct {
	SourceQueue
	name string
}

func (s sourceQueueStub) Name() string { return s.name }
```

- [ ] **Step 2: Run source adapter tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/translationqueue -run TestSourceRegistry -count=1
```

Expected: FAIL because source registry does not exist.

- [ ] **Step 3: Implement source adapter API**

Create `scheduler/internal/translationqueue/source.go`:

```go
package translationqueue

import (
	"context"

	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
)

type PrepareQueueCommand struct {
	IDs           []string
	Filters       map[string]string
	CompanyLimit  int32
	Provider      string
	Model         string
	PromptVersion string
	SourceLang    string
	TargetLang    string
}

type ClaimBatchCommand struct {
	BatchID          string
	MaxCandidateRows int32
	MaxRequestChars  int32
	MaxSourceRunning int32
}

type ClaimBatchResult struct {
	Status         string
	BatchID        string
	CompanyIDs     []string
	EstimatedChars int32
	Provider       string
	Model          string
	PromptVersion  string
	SourceLang     string
	TargetLang     string
}

type QueueBatchResult struct {
	RowsAffected int32
}

type SourceQueue interface {
	Name() string
	PrepareQueue(context.Context, PrepareQueueCommand) error
	ClaimBatch(context.Context, ClaimBatchCommand) (ClaimBatchResult, error)
	ReleaseBatch(context.Context, string) (QueueBatchResult, error)
	CompleteBatch(context.Context, string) (QueueBatchResult, error)
	ResetStale(context.Context, int32) (QueueBatchResult, error)
	LoadMissingFields(context.Context, sourcetranslation.LoadMissingFieldsCommand) ([]sourcetranslation.MissingField, error)
	LoadCachedTerms(context.Context, sourcetranslation.LoadCachedTermsCommand) (map[string]sourcetranslation.CachedTerm, error)
	SaveTerms(context.Context, sourcetranslation.SaveTermsCommand) (sourcetranslation.SaveTermsResult, error)
	ApplyTranslations(context.Context, sourcetranslation.ApplyCompanyTranslationsCommand) (sourcetranslation.ApplyCompanyTranslationsResult, error)
}

type SourceRegistry struct {
	sources map[string]SourceQueue
	names   []string
}

func NewSourceRegistry(sources []SourceQueue) SourceRegistry {
	registry := SourceRegistry{
		sources: make(map[string]SourceQueue, len(sources)),
		names:   make([]string, 0, len(sources)),
	}
	for _, source := range sources {
		if source == nil || source.Name() == "" {
			continue
		}
		if _, exists := registry.sources[source.Name()]; exists {
			continue
		}
		registry.sources[source.Name()] = source
		registry.names = append(registry.names, source.Name())
	}
	return registry
}

func (r SourceRegistry) Get(name string) (SourceQueue, bool) {
	source, ok := r.sources[name]
	return source, ok
}

func (r SourceRegistry) Names() []string {
	return append([]string(nil), r.names...)
}
```

- [ ] **Step 4: Add BRREG source adapter**

In `scheduler/internal/brreg/actions/company_translation_workset_actions.go`, add:

```go
func (a *CompanyTranslationActions) Name() string {
	return "brreg"
}

func (a *CompanyTranslationActions) PrepareQueue(ctx context.Context, command translationqueue.PrepareQueueCommand) error {
	_, err := a.store.PrepareTranslationQueue(ctx, companydata.PrepareTranslationQueueCommand{
		IDs:           command.IDs,
		Filters:       command.Filters,
		CompanyLimit:  command.CompanyLimit,
		Provider:      command.Provider,
		Model:         command.Model,
		PromptVersion: command.PromptVersion,
		SourceLang:    defaultString(command.SourceLang, "no"),
		TargetLang:    defaultString(command.TargetLang, "en"),
	})
	return errors.Wrap(err, "prepare brreg translation queue")
}

func (a *CompanyTranslationActions) ClaimBatch(ctx context.Context, command translationqueue.ClaimBatchCommand) (translationqueue.ClaimBatchResult, error) {
	result, err := a.store.ClaimTranslationQueueBatch(ctx, companydata.ClaimTranslationQueueBatchCommand{
		BatchID:          command.BatchID,
		MaxCandidateRows: command.MaxCandidateRows,
		MaxRequestChars:  command.MaxRequestChars,
		MaxSourceRunning: command.MaxSourceRunning,
	})
	if err != nil {
		return translationqueue.ClaimBatchResult{}, errors.Wrap(err, "claim brreg translation batch")
	}
	return translationqueue.ClaimBatchResult(result), nil
}

func (a *CompanyTranslationActions) ReleaseBatch(ctx context.Context, batchID string) (translationqueue.QueueBatchResult, error) {
	result, err := a.store.ReleaseTranslationQueueBatch(ctx, batchID)
	return translationqueue.QueueBatchResult(result), errors.Wrap(err, "release brreg translation batch")
}

func (a *CompanyTranslationActions) CompleteBatch(ctx context.Context, batchID string) (translationqueue.QueueBatchResult, error) {
	result, err := a.store.CompleteTranslationQueueBatch(ctx, batchID)
	return translationqueue.QueueBatchResult(result), errors.Wrap(err, "complete brreg translation batch")
}

func (a *CompanyTranslationActions) ResetStale(ctx context.Context, staleSeconds int32) (translationqueue.QueueBatchResult, error) {
	result, err := a.store.ResetStaleTranslationQueueEntries(ctx, staleSeconds)
	return translationqueue.QueueBatchResult(result), errors.Wrap(err, "reset stale brreg translation batches")
}

func (a *CompanyTranslationActions) LoadMissingFields(ctx context.Context, command sourcetranslation.LoadMissingFieldsCommand) ([]sourcetranslation.MissingField, error) {
	return a.store.LoadMissingTranslationFields(ctx, command)
}

func (a *CompanyTranslationActions) LoadCachedTerms(ctx context.Context, command sourcetranslation.LoadCachedTermsCommand) (map[string]sourcetranslation.CachedTerm, error) {
	return a.store.LoadCachedTranslationTerms(ctx, command)
}

func (a *CompanyTranslationActions) SaveTerms(ctx context.Context, command sourcetranslation.SaveTermsCommand) (sourcetranslation.SaveTermsResult, error) {
	return a.store.SaveTranslationTerms(ctx, command)
}

func (a *CompanyTranslationActions) ApplyTranslations(ctx context.Context, command sourcetranslation.ApplyCompanyTranslationsCommand) (sourcetranslation.ApplyCompanyTranslationsResult, error) {
	return a.store.ApplyCompanyTranslations(ctx, command)
}
```

Add imports for `translationqueue` and `sourcetranslation` when they are not already present in the file.

- [ ] **Step 5: Add Ariregister source adapter**

In `scheduler/internal/ariregister/actions/company_translation_workset_actions.go`, add:

```go
func (a *CompanyTranslationActions) Name() string {
	return "ariregister"
}

func (a *CompanyTranslationActions) PrepareQueue(ctx context.Context, command translationqueue.PrepareQueueCommand) error {
	_, err := a.store.PrepareTranslationQueue(ctx, companydata.PrepareTranslationQueueCommand{
		IDs:           command.IDs,
		Filters:       command.Filters,
		CompanyLimit:  command.CompanyLimit,
		Provider:      command.Provider,
		Model:         command.Model,
		PromptVersion: command.PromptVersion,
		SourceLang:    defaultString(command.SourceLang, "et"),
		TargetLang:    defaultString(command.TargetLang, "en"),
	})
	return errors.Wrap(err, "prepare ariregister translation queue")
}

func (a *CompanyTranslationActions) ClaimBatch(ctx context.Context, command translationqueue.ClaimBatchCommand) (translationqueue.ClaimBatchResult, error) {
	result, err := a.store.ClaimTranslationQueueBatch(ctx, companydata.ClaimTranslationQueueBatchCommand{
		BatchID:          command.BatchID,
		MaxCandidateRows: command.MaxCandidateRows,
		MaxRequestChars:  command.MaxRequestChars,
		MaxSourceRunning: command.MaxSourceRunning,
	})
	if err != nil {
		return translationqueue.ClaimBatchResult{}, errors.Wrap(err, "claim ariregister translation batch")
	}
	return translationqueue.ClaimBatchResult(result), nil
}

func (a *CompanyTranslationActions) ReleaseBatch(ctx context.Context, batchID string) (translationqueue.QueueBatchResult, error) {
	result, err := a.store.ReleaseTranslationQueueBatch(ctx, batchID)
	return translationqueue.QueueBatchResult(result), errors.Wrap(err, "release ariregister translation batch")
}

func (a *CompanyTranslationActions) CompleteBatch(ctx context.Context, batchID string) (translationqueue.QueueBatchResult, error) {
	result, err := a.store.CompleteTranslationQueueBatch(ctx, batchID)
	return translationqueue.QueueBatchResult(result), errors.Wrap(err, "complete ariregister translation batch")
}

func (a *CompanyTranslationActions) ResetStale(ctx context.Context, staleSeconds int32) (translationqueue.QueueBatchResult, error) {
	result, err := a.store.ResetStaleTranslationQueueEntries(ctx, staleSeconds)
	return translationqueue.QueueBatchResult(result), errors.Wrap(err, "reset stale ariregister translation batches")
}

func (a *CompanyTranslationActions) LoadMissingFields(ctx context.Context, command sourcetranslation.LoadMissingFieldsCommand) ([]sourcetranslation.MissingField, error) {
	return a.store.LoadMissingTranslationFields(ctx, command)
}

func (a *CompanyTranslationActions) LoadCachedTerms(ctx context.Context, command sourcetranslation.LoadCachedTermsCommand) (map[string]sourcetranslation.CachedTerm, error) {
	return a.store.LoadCachedTranslationTerms(ctx, command)
}

func (a *CompanyTranslationActions) SaveTerms(ctx context.Context, command sourcetranslation.SaveTermsCommand) (sourcetranslation.SaveTermsResult, error) {
	return a.store.SaveTranslationTerms(ctx, command)
}

func (a *CompanyTranslationActions) ApplyTranslations(ctx context.Context, command sourcetranslation.ApplyCompanyTranslationsCommand) (sourcetranslation.ApplyCompanyTranslationsResult, error) {
	return a.store.ApplyCompanyTranslations(ctx, command)
}
```

- [ ] **Step 6: Run source adapter tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/translationqueue ./internal/brreg/actions ./internal/ariregister/actions -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit source adapter API**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/translationqueue/source.go scheduler/internal/translationqueue/source_test.go scheduler/internal/brreg/actions/company_translation_workset_actions.go scheduler/internal/ariregister/actions/company_translation_workset_actions.go
git commit -m "feat: add source translation queue adapter"
```

Expected: commit succeeds.

---

### Task 4: Add Scheduler JetStream Client

**Files:**
- Create: `scheduler/internal/translationqueue/jetstream.go`
- Test: `scheduler/internal/translationqueue/jetstream_test.go`

- [ ] **Step 1: Write JetStream client unit tests around payload encoding**

Create `scheduler/internal/translationqueue/jetstream_test.go`:

```go
package translationqueue

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeTranslationJobRejectsMissingBatchID(t *testing.T) {
	_, err := encodeTranslationJob(TranslationJob{JobID: "job-1", Source: "brreg"})
	require.ErrorContains(t, err, "batch id is required")
}

func TestEncodeTranslationJobReturnsJSONPayload(t *testing.T) {
	body, err := encodeTranslationJob(TranslationJob{
		JobID:         "job-1",
		BatchID:       "batch-1",
		Source:        "brreg",
		SourceLang:    "no",
		TargetLang:    "en",
		Provider:      "default",
		PromptVersion: "v1",
		CompanyIDs:    []string{"company-a"},
		Terms: []TranslationJobTerm{{
			TermKey:              "a",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
		}},
	})
	require.NoError(t, err)

	var decoded TranslationJob
	require.NoError(t, json.Unmarshal(body, &decoded))
	require.Equal(t, "batch-1", decoded.BatchID)
}

func TestJetStreamPublisherPublishesJobSubject(t *testing.T) {
	publisher := fakePublisher{}
	client := NewJetStreamClientFromPublisher(publisher)

	err := client.PublishJob(context.Background(), TranslationJob{
		JobID:         "job-1",
		BatchID:       "batch-1",
		Source:        "brreg",
		SourceLang:    "no",
		TargetLang:    "en",
		Provider:      "default",
		PromptVersion: "v1",
		CompanyIDs:    []string{"company-a"},
		Terms: []TranslationJobTerm{{
			TermKey:              "a",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
		}},
	})
	require.NoError(t, err)
	require.Equal(t, JobsSubject, publisher.subject)
	require.NotEmpty(t, publisher.payload)
}

type fakePublisher struct {
	subject string
	payload []byte
}

func (f fakePublisher) Publish(context.Context, string, []byte) error {
	return nil
}
```

- [ ] **Step 2: Run JetStream tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/translationqueue -run 'TestEncodeTranslationJob|TestJetStreamPublisher' -count=1
```

Expected: FAIL because JetStream client helpers do not exist.

- [ ] **Step 3: Implement JetStream client**

Create `scheduler/internal/translationqueue/jetstream.go`:

```go
package translationqueue

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
)

type jetStreamPublisher interface {
	Publish(context.Context, string, []byte) error
}

type JetStreamClient struct {
	publisher jetStreamPublisher
	conn      *nats.Conn
}

func NewJetStreamClient(ctx context.Context, url string) (*JetStreamClient, error) {
	conn, err := nats.Connect(url, nats.Timeout(10*time.Second))
	if err != nil {
		return nil, errors.Wrap(err, "connect translation jetstream nats")
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, errors.Wrap(err, "create translation jetstream context")
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     StreamName,
		Subjects: []string{JobsSubject, ResultsSubject},
		Storage:  nats.FileStorage,
	}); err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		conn.Close()
		return nil, errors.Wrap(err, "ensure translation jetstream stream")
	}
	return &JetStreamClient{publisher: natsJetStreamPublisher{js: js}, conn: conn}, nil
}

func NewJetStreamClientFromPublisher(publisher jetStreamPublisher) *JetStreamClient {
	return &JetStreamClient{publisher: publisher}
}

func (c *JetStreamClient) PublishJob(ctx context.Context, job TranslationJob) error {
	body, err := encodeTranslationJob(job)
	if err != nil {
		return err
	}
	if err := c.publisher.Publish(ctx, JobsSubject, body); err != nil {
		return errors.Wrap(err, "publish translation jetstream job")
	}
	return nil
}

func encodeTranslationJob(job TranslationJob) ([]byte, error) {
	if strings.TrimSpace(job.BatchID) == "" {
		return nil, errors.New("translation job batch id is required")
	}
	if strings.TrimSpace(job.Source) == "" {
		return nil, errors.New("translation job source is required")
	}
	if len(job.Terms) == 0 {
		return nil, errors.New("translation job terms are required")
	}
	body, err := json.Marshal(job)
	if err != nil {
		return nil, errors.Wrap(err, "encode translation jetstream job")
	}
	return body, nil
}

func (c *JetStreamClient) Close() {
	if c != nil && c.conn != nil {
		c.conn.Close()
	}
}

type natsJetStreamPublisher struct {
	js nats.JetStreamContext
}

func (p natsJetStreamPublisher) Publish(ctx context.Context, subject string, payload []byte) error {
	_, err := p.js.Publish(subject, payload, nats.Context(ctx))
	return err
}
```

Adjust the fake publisher in the test to use pointer receiver:

```go
func (f *fakePublisher) Publish(_ context.Context, subject string, payload []byte) error {
	f.subject = subject
	f.payload = payload
	return nil
}
```

And instantiate:

```go
publisher := &fakePublisher{}
```

- [ ] **Step 4: Run JetStream tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/translationqueue -run 'TestEncodeTranslationJob|TestJetStreamPublisher' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit JetStream client**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/translationqueue/jetstream.go scheduler/internal/translationqueue/jetstream_test.go
git commit -m "feat: add translation jetstream client"
```

Expected: commit succeeds.

---

### Task 5: Implement Dispatcher Buffer Refill

**Files:**
- Create: `scheduler/internal/translationqueue/dispatcher.go`
- Test: `scheduler/internal/translationqueue/dispatcher_test.go`

- [ ] **Step 1: Write dispatcher tests**

Create `scheduler/internal/translationqueue/dispatcher_test.go`:

```go
package translationqueue

import (
	"context"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
	"github.com/stretchr/testify/require"
)

func TestDispatcherPublishesClaimedBatchWithUncachedTerms(t *testing.T) {
	source := &dispatcherSourceStub{
		name: "brreg",
		claim: ClaimBatchResult{
			Status:        "claimed",
			BatchID:       "batch-1",
			CompanyIDs:    []string{"company-a"},
			Provider:      "default",
			PromptVersion: "v1",
			SourceLang:    "no",
			TargetLang:    "en",
		},
		fields: []sourcetranslation.MissingField{{
			CompanyID:            "company-a",
			SourceTable:          "brreg_source.companies",
			SourceRowID:          "company-a",
			SourceColumn:         "organization_form_label",
			TargetColumn:         "organization_form_label_en",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TermKey:              "a",
		}},
	}
	publisher := &jobPublisherStub{}
	dispatcher := NewDispatcher(SourceRegistry{}, publisher, DispatcherConfig{
		SourceBufferTarget: 2,
		MaxCandidateRows:   25,
		MaxRequestChars:    6000,
	})

	err := dispatcher.RefillSource(context.Background(), source)
	require.NoError(t, err)
	require.Len(t, publisher.jobs, 1)
	require.Equal(t, "batch-1", publisher.jobs[0].BatchID)
	require.Equal(t, "brreg", publisher.jobs[0].Source)
	require.Equal(t, []string{"company-a"}, publisher.jobs[0].CompanyIDs)
	require.Equal(t, "Aksjeselskap", publisher.jobs[0].Terms[0].SourceText)
}

func TestDispatcherCompletesCachedOnlyBatchWithoutPublishing(t *testing.T) {
	source := &dispatcherSourceStub{
		name: "brreg",
		claim: ClaimBatchResult{
			Status:        "claimed",
			BatchID:       "batch-1",
			CompanyIDs:    []string{"company-a"},
			Provider:      "default",
			PromptVersion: "v1",
			SourceLang:    "no",
			TargetLang:    "en",
		},
		fields: []sourcetranslation.MissingField{{
			CompanyID:            "company-a",
			SourceTable:          "brreg_source.companies",
			SourceRowID:          "company-a",
			SourceColumn:         "organization_form_label",
			TargetColumn:         "organization_form_label_en",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TermKey:              "a",
		}},
		cached: map[string]sourcetranslation.CachedTerm{
			"a": {TermKey: "a", TranslatedText: "Limited liability company"},
		},
	}
	publisher := &jobPublisherStub{}
	dispatcher := NewDispatcher(SourceRegistry{}, publisher, DispatcherConfig{
		SourceBufferTarget: 2,
		MaxCandidateRows:   25,
		MaxRequestChars:    6000,
	})

	err := dispatcher.RefillSource(context.Background(), source)
	require.NoError(t, err)
	require.Empty(t, publisher.jobs)
	require.Equal(t, int32(1), source.applied)
	require.Equal(t, int32(1), source.completed)
}
```

Add source/publisher stubs in the same test file with concrete method implementations:

```go
type dispatcherSourceStub struct {
	SourceQueue
	name      string
	claim     ClaimBatchResult
	fields    []sourcetranslation.MissingField
	cached    map[string]sourcetranslation.CachedTerm
	applied   int32
	completed int32
	released  int32
}

func (s *dispatcherSourceStub) Name() string { return s.name }
func (s *dispatcherSourceStub) ClaimBatch(context.Context, ClaimBatchCommand) (ClaimBatchResult, error) {
	return s.claim, nil
}
func (s *dispatcherSourceStub) LoadMissingFields(context.Context, sourcetranslation.LoadMissingFieldsCommand) ([]sourcetranslation.MissingField, error) {
	return s.fields, nil
}
func (s *dispatcherSourceStub) LoadCachedTerms(context.Context, sourcetranslation.LoadCachedTermsCommand) (map[string]sourcetranslation.CachedTerm, error) {
	if s.cached == nil {
		return map[string]sourcetranslation.CachedTerm{}, nil
	}
	return s.cached, nil
}
func (s *dispatcherSourceStub) ApplyTranslations(_ context.Context, command sourcetranslation.ApplyCompanyTranslationsCommand) (sourcetranslation.ApplyCompanyTranslationsResult, error) {
	s.applied += int32(len(command.Bindings))
	return sourcetranslation.ApplyCompanyTranslationsResult{BindingsApplied: int32(len(command.Bindings))}, nil
}
func (s *dispatcherSourceStub) CompleteBatch(context.Context, string) (QueueBatchResult, error) {
	s.completed++
	return QueueBatchResult{RowsAffected: 1}, nil
}
func (s *dispatcherSourceStub) ReleaseBatch(context.Context, string) (QueueBatchResult, error) {
	s.released++
	return QueueBatchResult{RowsAffected: 1}, nil
}

type jobPublisherStub struct {
	jobs []TranslationJob
}

func (p *jobPublisherStub) PublishJob(_ context.Context, job TranslationJob) error {
	p.jobs = append(p.jobs, job)
	return nil
}
```

- [ ] **Step 2: Run dispatcher tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/translationqueue -run TestDispatcher -count=1
```

Expected: FAIL because dispatcher does not exist.

- [ ] **Step 3: Implement dispatcher**

Create `scheduler/internal/translationqueue/dispatcher.go`:

```go
package translationqueue

import (
	"context"
	"log/slog"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
)

type JobPublisher interface {
	PublishJob(context.Context, TranslationJob) error
}

type DispatcherConfig struct {
	SourceBufferTarget int32
	MaxCandidateRows   int32
	MaxRequestChars    int32
}

type Dispatcher struct {
	registry  SourceRegistry
	publisher JobPublisher
	config    DispatcherConfig
}

func NewDispatcher(registry SourceRegistry, publisher JobPublisher, config DispatcherConfig) *Dispatcher {
	if config.SourceBufferTarget <= 0 {
		config.SourceBufferTarget = 2
	}
	if config.MaxCandidateRows <= 0 {
		config.MaxCandidateRows = 25
	}
	if config.MaxRequestChars <= 0 {
		config.MaxRequestChars = 6000
	}
	return &Dispatcher{registry: registry, publisher: publisher, config: config}
}

func (d *Dispatcher) RefillOnce(ctx context.Context) error {
	for _, name := range d.registry.Names() {
		source, ok := d.registry.Get(name)
		if !ok {
			continue
		}
		if err := d.RefillSource(ctx, source); err != nil {
			return errors.Wrapf(err, "refill %s translation source buffer", name)
		}
	}
	return nil
}

func (d *Dispatcher) RefillSource(ctx context.Context, source SourceQueue) error {
	claimed, err := source.ClaimBatch(ctx, ClaimBatchCommand{
		BatchID:          uuid.NewString(),
		MaxCandidateRows: d.config.MaxCandidateRows,
		MaxRequestChars:  d.config.MaxRequestChars,
		MaxSourceRunning: d.config.SourceBufferTarget,
	})
	if err != nil {
		return errors.Wrap(err, "claim translation batch")
	}
	if claimed.Status == "blocked" || claimed.Status == "drained" || len(claimed.CompanyIDs) == 0 {
		return nil
	}

	fields, err := source.LoadMissingFields(ctx, sourcetranslation.LoadMissingFieldsCommand{
		PromptVersion: claimed.PromptVersion,
		CompanyIDs:    claimed.CompanyIDs,
	})
	if err != nil {
		_, _ = source.ReleaseBatch(ctx, claimed.BatchID)
		return errors.Wrap(err, "load translation batch missing fields")
	}
	if len(fields) == 0 {
		_, err := source.CompleteBatch(ctx, claimed.BatchID)
		return errors.Wrap(err, "complete empty translation batch")
	}

	cached, err := source.LoadCachedTerms(ctx, sourcetranslation.LoadCachedTermsCommand{
		PromptVersion: claimed.PromptVersion,
		TermKeys:      sourcetranslation.TranslationTermKeys(fields),
	})
	if err != nil {
		_, _ = source.ReleaseBatch(ctx, claimed.BatchID)
		return errors.Wrap(err, "load cached translation terms")
	}
	built := sourcetranslation.BuildTranslationQueueTerms(fields, cached)
	if len(built.CachedBindings) > 0 {
		applied, err := source.ApplyTranslations(ctx, sourcetranslation.ApplyCompanyTranslationsCommand{
			CompanyID: claimed.CompanyIDs[0],
			Bindings:  built.CachedBindings,
		})
		if err != nil {
			_, _ = source.ReleaseBatch(ctx, claimed.BatchID)
			return errors.Wrap(err, "apply cached translation bindings")
		}
		slog.DebugContext(ctx, "applied cached translation bindings before dispatch",
			"source", source.Name(),
			"batch_id", claimed.BatchID,
			"bindings_applied", applied.BindingsApplied,
		)
	}
	if len(built.UncachedTerms) == 0 {
		_, err := source.CompleteBatch(ctx, claimed.BatchID)
		return errors.Wrap(err, "complete cached-only translation batch")
	}

	job := buildTranslationJob(source.Name(), claimed, built.UncachedTerms)
	if err := d.publisher.PublishJob(ctx, job); err != nil {
		_, _ = source.ReleaseBatch(ctx, claimed.BatchID)
		return errors.Wrap(err, "publish translation job")
	}
	return nil
}

func buildTranslationJob(source string, claimed ClaimBatchResult, terms []sourcetranslation.TranslationTerm) TranslationJob {
	job := TranslationJob{
		JobID:         uuid.NewString(),
		BatchID:       claimed.BatchID,
		Source:        source,
		SourceLang:    claimed.SourceLang,
		TargetLang:    claimed.TargetLang,
		Provider:      claimed.Provider,
		Model:         claimed.Model,
		PromptVersion: claimed.PromptVersion,
		CompanyIDs:    claimed.CompanyIDs,
		Terms:         make([]TranslationJobTerm, 0, len(terms)),
	}
	for _, term := range terms {
		job.Terms = append(job.Terms, TranslationJobTerm{
			TermKey:              term.TermKey,
			SourceText:           term.SourceText,
			SourceTextNormalized: term.SourceTextNormalized,
		})
	}
	return job
}
```

- [ ] **Step 4: Run dispatcher tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/translationqueue -run TestDispatcher -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit dispatcher**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/translationqueue/dispatcher.go scheduler/internal/translationqueue/dispatcher_test.go
git commit -m "feat: add translation queue dispatcher"
```

Expected: commit succeeds.

---

### Task 6: Implement Result Collector

**Files:**
- Create: `scheduler/internal/translationqueue/result_collector.go`
- Test: `scheduler/internal/translationqueue/result_collector_test.go`
- Modify: `scheduler/internal/translationqueue/jetstream.go`

- [ ] **Step 1: Write result collector tests**

Create `scheduler/internal/translationqueue/result_collector_test.go`:

```go
package translationqueue

import (
	"context"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
	"github.com/stretchr/testify/require"
)

func TestResultCollectorSavesAppliesAndCompletesSuccessfulResult(t *testing.T) {
	source := &collectorSourceStub{
		name: "brreg",
		fields: []sourcetranslation.MissingField{{
			CompanyID:            "company-a",
			SourceTable:          "brreg_source.companies",
			SourceRowID:          "company-a",
			SourceColumn:         "organization_form_label",
			TargetColumn:         "organization_form_label_en",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TermKey:              "a",
		}},
	}
	registry := NewSourceRegistry([]SourceQueue{source})
	collector := NewResultCollector(registry)

	err := collector.HandleResult(context.Background(), TranslationResult{
		BatchID:       "batch-1",
		Source:        "brreg",
		Status:        "succeeded",
		Provider:      "default",
		PromptVersion: "v1",
		Results: []TranslationResultTerm{{
			TermKey:              "a",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TranslatedText:       "Limited liability company",
			Status:               "succeeded",
		}},
	})

	require.NoError(t, err)
	require.Equal(t, int32(1), source.saved)
	require.Equal(t, int32(1), source.applied)
	require.Equal(t, int32(1), source.completed)
}

func TestResultCollectorReleasesWholeBatchFailure(t *testing.T) {
	source := &collectorSourceStub{name: "brreg"}
	registry := NewSourceRegistry([]SourceQueue{source})
	collector := NewResultCollector(registry)

	err := collector.HandleResult(context.Background(), TranslationResult{
		BatchID: "batch-1",
		Source:  "brreg",
		Status:  "failed",
	})

	require.NoError(t, err)
	require.Equal(t, int32(1), source.released)
	require.Zero(t, source.completed)
}
```

Add the collector source stub:

```go
type collectorSourceStub struct {
	SourceQueue
	name      string
	fields    []sourcetranslation.MissingField
	saved     int32
	applied   int32
	completed int32
	released  int32
}

func (s *collectorSourceStub) Name() string { return s.name }
func (s *collectorSourceStub) SaveTerms(_ context.Context, command sourcetranslation.SaveTermsCommand) (sourcetranslation.SaveTermsResult, error) {
	s.saved += int32(len(command.Terms))
	return sourcetranslation.SaveTermsResult{TermsSaved: int32(len(command.Terms))}, nil
}
func (s *collectorSourceStub) LoadMissingFields(context.Context, sourcetranslation.LoadMissingFieldsCommand) ([]sourcetranslation.MissingField, error) {
	return s.fields, nil
}
func (s *collectorSourceStub) LoadCachedTerms(context.Context, sourcetranslation.LoadCachedTermsCommand) (map[string]sourcetranslation.CachedTerm, error) {
	return map[string]sourcetranslation.CachedTerm{
		"a": {TermKey: "a", TranslatedText: "Limited liability company"},
	}, nil
}
func (s *collectorSourceStub) ApplyTranslations(_ context.Context, command sourcetranslation.ApplyCompanyTranslationsCommand) (sourcetranslation.ApplyCompanyTranslationsResult, error) {
	s.applied += int32(len(command.Bindings))
	return sourcetranslation.ApplyCompanyTranslationsResult{BindingsApplied: int32(len(command.Bindings))}, nil
}
func (s *collectorSourceStub) CompleteBatch(context.Context, string) (QueueBatchResult, error) {
	s.completed++
	return QueueBatchResult{RowsAffected: 1}, nil
}
func (s *collectorSourceStub) ReleaseBatch(context.Context, string) (QueueBatchResult, error) {
	s.released++
	return QueueBatchResult{RowsAffected: 1}, nil
}
```

- [ ] **Step 2: Run collector tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/translationqueue -run TestResultCollector -count=1
```

Expected: FAIL because result collector does not exist.

- [ ] **Step 3: Implement result collector**

Create `scheduler/internal/translationqueue/result_collector.go`:

```go
package translationqueue

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
)

type ResultCollector struct {
	registry SourceRegistry
}

func NewResultCollector(registry SourceRegistry) *ResultCollector {
	return &ResultCollector{registry: registry}
}

func (c *ResultCollector) HandleResult(ctx context.Context, result TranslationResult) error {
	source, ok := c.registry.Get(strings.TrimSpace(result.Source))
	if !ok {
		return errors.Newf("translation result source %q is not registered", result.Source)
	}
	if strings.TrimSpace(result.BatchID) == "" {
		return errors.New("translation result batch id is required")
	}
	if result.Status == "failed" && len(result.Results) == 0 && len(result.Failures) == 0 {
		_, err := source.ReleaseBatch(ctx, result.BatchID)
		return errors.Wrap(err, "release failed translation batch")
	}

	terms := translationTermsFromResult(result)
	if len(terms) > 0 {
		if _, err := source.SaveTerms(ctx, sourcetranslation.SaveTermsCommand{
			PromptVersion: result.PromptVersion,
			Terms:         terms,
		}); err != nil {
			return errors.Wrap(err, "save translation result terms")
		}
	}

	fields, err := source.LoadMissingFields(ctx, sourcetranslation.LoadMissingFieldsCommand{
		PromptVersion: result.PromptVersion,
		CompanyIDs:    resultCompanyIDs(result),
	})
	if err != nil {
		return errors.Wrap(err, "load missing fields for translation result")
	}
	if len(fields) > 0 {
		cached, err := source.LoadCachedTerms(ctx, sourcetranslation.LoadCachedTermsCommand{
			PromptVersion: result.PromptVersion,
			TermKeys:      sourcetranslation.TranslationTermKeys(fields),
		})
		if err != nil {
			return errors.Wrap(err, "load cached terms for translation result")
		}
		built := sourcetranslation.BuildTranslationQueueTerms(fields, cached)
		if len(built.CachedBindings) > 0 {
			if _, err := source.ApplyTranslations(ctx, sourcetranslation.ApplyCompanyTranslationsCommand{
				Bindings: built.CachedBindings,
			}); err != nil {
				return errors.Wrap(err, "apply translation result bindings")
			}
		}
	}

	_, err = source.CompleteBatch(ctx, result.BatchID)
	return errors.Wrap(err, "complete translation result batch")
}

func translationTermsFromResult(result TranslationResult) []sourcetranslation.TranslationTermResult {
	terms := make([]sourcetranslation.TranslationTermResult, 0, len(result.Results)+len(result.Failures))
	for _, item := range result.Results {
		terms = append(terms, sourcetranslation.TranslationTermResult{
			TermKey:              item.TermKey,
			SourceText:           item.SourceText,
			SourceTextNormalized: item.SourceTextNormalized,
			TranslatedText:       item.TranslatedText,
			Status:               defaultString(item.Status, "succeeded"),
			Provider:             result.Provider,
			Model:                result.Model,
			PromptVersion:        result.PromptVersion,
		})
	}
	for _, failure := range result.Failures {
		terms = append(terms, sourcetranslation.TranslationTermResult{
			TermKey:              failure.TermKey,
			SourceText:           failure.SourceText,
			SourceTextNormalized: failure.SourceTextNormalized,
			Status:               defaultString(failure.Status, "failed_retryable"),
			Provider:             result.Provider,
			Model:                result.Model,
			PromptVersion:        result.PromptVersion,
			Error:                failure.Error,
			ErrorCode:            failure.ErrorCode,
		})
	}
	return terms
}

func defaultString(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func resultCompanyIDs(result TranslationResult) []string {
	return result.CompanyIDs
}
```

- [ ] **Step 4: Verify company ids are present in result contracts**

Confirm `scheduler/internal/translationqueue/contracts.go` includes:

```go
type TranslationResult struct {
	JobID         string                     `json:"job_id"`
	BatchID       string                     `json:"batch_id"`
	Source        string                     `json:"source"`
	Status        string                     `json:"status"`
	Provider      string                     `json:"provider"`
	Model         string                     `json:"model,omitempty"`
	PromptVersion string                     `json:"prompt_version"`
	CompanyIDs    []string                   `json:"company_ids"`
	DurationMS    int                        `json:"duration_ms"`
	Results       []TranslationResultTerm    `json:"results"`
	Failures      []TranslationFailureResult `json:"failures"`
}
```

Confirm `resultCompanyIDs` returns the batch company ids:

```go
func resultCompanyIDs(result TranslationResult) []string {
	return result.CompanyIDs
}
```

Confirm Python `JetStreamTranslationResult` in `models.py` includes:

```python
class JetStreamTranslationResult(BaseModel):
    job_id: str = Field(min_length=1)
    batch_id: str = Field(min_length=1)
    source: str = Field(min_length=1)
    status: JetStreamResultStatus
    provider: str = Field(default="default", min_length=1)
    model: str | None = None
    prompt_version: str = Field(default="v1", min_length=1)
    company_ids: list[str] = Field(default_factory=list)
    duration_ms: int = Field(ge=0)
    results: list[JetStreamTranslationResultItem] = Field(default_factory=list)
    failures: list[JetStreamTranslationFailureItem] = Field(default_factory=list)
```

- [ ] **Step 5: Run collector tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/translationqueue -run TestResultCollector -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit result collector**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/translationqueue/contracts.go scheduler/internal/translationqueue/result_collector.go scheduler/internal/translationqueue/result_collector_test.go ../data-pipelines/services/translation-service/src/corpscout_translation_service/models.py
git commit -m "feat: add translation result collector"
```

Expected: commit succeeds.

---

### Task 7: Wire Scheduler Background Service

**Files:**
- Create: `scheduler/internal/translationqueue/service.go`
- Test: `scheduler/internal/translationqueue/service_test.go`
- Modify: `scheduler/internal/config/config.go`
- Modify: `scheduler/internal/config/config_test.go`
- Modify: `scheduler/internal/app/server.go`
- Modify: `scheduler/internal/app/temporal.go`

- [ ] **Step 1: Write config tests**

Append to `scheduler/internal/config/config_test.go`:

```go
func TestLoadReadsTranslationJetStreamBufferConfig(t *testing.T) {
	t.Setenv("CORPSCOUT_DATABASE_URL", "postgres://example")
	t.Setenv("CORPSCOUT_S3_ACCESS_KEY", "access")
	t.Setenv("CORPSCOUT_S3_SECRET_KEY", "secret")
	t.Setenv("CORPSCOUT_NATS_URL", "nats://companycollect:4222")
	t.Setenv("CORPSCOUT_TRANSLATION_JETSTREAM_ENABLED", "true")
	t.Setenv("CORPSCOUT_TRANSLATION_SOURCE_BUFFER_TARGET", "2")
	t.Setenv("CORPSCOUT_TRANSLATION_DISPATCH_INTERVAL_SECONDS", "3")
	t.Setenv("CORPSCOUT_TRANSLATION_BATCH_LEASE_SECONDS", "1800")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.TranslationJetStreamEnabled {
		t.Fatal("expected translation jetstream to be enabled")
	}
	if cfg.TranslationSourceBufferTarget != 2 {
		t.Fatalf("want source buffer target 2, got %d", cfg.TranslationSourceBufferTarget)
	}
	if cfg.TranslationDispatchInterval != 3*time.Second {
		t.Fatalf("want dispatch interval 3s, got %s", cfg.TranslationDispatchInterval)
	}
	if cfg.TranslationBatchLeaseSeconds != 1800 {
		t.Fatalf("want lease 1800s, got %d", cfg.TranslationBatchLeaseSeconds)
	}
}
```

- [ ] **Step 2: Run config test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/config -run TestLoadReadsTranslationJetStreamBufferConfig -count=1
```

Expected: FAIL because config fields do not exist.

- [ ] **Step 3: Implement config**

In `scheduler/internal/config/config.go`, add fields:

```go
TranslationJetStreamEnabled  bool
TranslationSourceBufferTarget int32
TranslationDispatchInterval   time.Duration
TranslationBatchLeaseSeconds  int32
```

Add parsing helpers:

```go
func parseBoolEnv(key string, fallback bool) (bool, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	switch strings.ToLower(value) {
	case "true", "1", "yes":
		return true, nil
	case "false", "0", "no":
		return false, nil
	default:
		return false, errors.Newf("%s must be true or false", key)
	}
}

func parseInt32Env(key string, fallback int32) (int32, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, errors.Newf("%s must be a positive integer", key)
	}
	return int32(parsed), nil
}
```

In `Load`, parse and return:

```go
translationJetStreamEnabled, err := parseBoolEnv("CORPSCOUT_TRANSLATION_JETSTREAM_ENABLED", true)
if err != nil {
	return Config{}, err
}
translationSourceBufferTarget, err := parseInt32Env("CORPSCOUT_TRANSLATION_SOURCE_BUFFER_TARGET", 2)
if err != nil {
	return Config{}, err
}
translationDispatchInterval, err := parseSecondsEnv("CORPSCOUT_TRANSLATION_DISPATCH_INTERVAL_SECONDS", 2*time.Second)
if err != nil {
	return Config{}, err
}
translationBatchLeaseSeconds, err := parseInt32Env("CORPSCOUT_TRANSLATION_BATCH_LEASE_SECONDS", 1800)
if err != nil {
	return Config{}, err
}
```

Set the fields in the returned `Config`.

- [ ] **Step 4: Implement service lifecycle**

Create `scheduler/internal/translationqueue/service.go`:

```go
package translationqueue

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Service struct {
	dispatcher *Dispatcher
	collector  *ResultCollector
	interval   time.Duration
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func NewService(dispatcher *Dispatcher, collector *ResultCollector, interval time.Duration) *Service {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &Service{dispatcher: dispatcher, collector: collector, interval: interval}
}

func (s *Service) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			if err := s.dispatcher.RefillOnce(runCtx); err != nil {
				slog.ErrorContext(runCtx, "refill translation jetstream buffer", "error", err)
			}
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *Service) Stop() {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}
```

- [ ] **Step 5: Wire app server**

In `scheduler/internal/app/server.go`, add field:

```go
translationQueue *translationqueue.Service
```

After Temporal workers start, construct the service when enabled:

```go
var translationQueueService *translationqueue.Service
if cfg.TranslationJetStreamEnabled {
	jsClient, err := translationqueue.NewJetStreamClient(ctx, cfg.NATSURL)
	if err != nil {
		stopTemporalWorkers(temporalWorkers)
		temporalClient.Close()
		temporalDeps.Close()
		_ = riverClient.Stop(ctx)
		pool.Close()
		return nil, errors.Wrap(err, "create translation jetstream client")
	}
	registry := translationqueue.NewSourceRegistry([]translationqueue.SourceQueue{
		temporalDeps.companyTranslation,
		temporalDeps.ariregisterCompanyTranslation,
	})
	dispatcher := translationqueue.NewDispatcher(registry, jsClient, translationqueue.DispatcherConfig{
		SourceBufferTarget: cfg.TranslationSourceBufferTarget,
		MaxCandidateRows:   25,
		MaxRequestChars:    6000,
	})
	collector := translationqueue.NewResultCollector(registry)
	translationQueueService = translationqueue.NewService(dispatcher, collector, cfg.TranslationDispatchInterval)
	translationQueueService.Start(ctx)
}
```

Add it to returned `Server` and stop it first in `Shutdown`:

```go
if s.translationQueue != nil {
	s.translationQueue.Stop()
}
```

- [ ] **Step 6: Run scheduler app/config tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/config ./internal/translationqueue ./internal/app -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit scheduler service wiring**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/config/config.go scheduler/internal/config/config_test.go scheduler/internal/translationqueue/service.go scheduler/internal/translationqueue/service_test.go scheduler/internal/app/server.go scheduler/internal/app/temporal.go
git commit -m "feat: start translation jetstream buffer service"
```

Expected: commit succeeds.

---

### Task 8: Convert Translation Workflows To Queue Preparation

**Files:**
- Modify: `scheduler/internal/brreg/workflow/company_translation.go`
- Modify: `scheduler/internal/ariregister/workflow/company_translation.go`
- Modify: `scheduler/internal/brreg/workflow/company_translation_test.go`
- Modify: `scheduler/internal/ariregister/workflow/company_translation_test.go`
- Modify: `scheduler/internal/brreg/actions/company_translation_workset_actions.go`
- Modify: `scheduler/internal/ariregister/actions/company_translation_workset_actions.go`

- [ ] **Step 1: Update workflow tests for queued status**

In `scheduler/internal/brreg/workflow/company_translation_test.go`, replace the old "completes queue batch" test with:

```go
func TestTranslateBrregSourceCompaniesPreparesQueueAndReturnsQueued(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateBrregSourceCompanies)

	var buildInput BuildBrregTranslationWorksetInput
	env.RegisterActivityWithOptions(func(input BuildBrregTranslationWorksetInput) (BuildBrregTranslationWorksetResult, error) {
		buildInput = input
		return BuildBrregTranslationWorksetResult{
			FieldsExported:    3,
			TermsExported:     3,
			CompaniesExported: 2,
			CompaniesQueued:   2,
		}, nil
	}, activity.RegisterOptions{Name: buildBrregTranslationWorksetActivity})

	env.ExecuteWorkflow(TranslateBrregSourceCompanies, TranslateBrregSourceCompaniesInput{
		Provider:      "deepseek",
		Model:         "deepseek-chat",
		PromptVersion: "v1",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "deepseek", buildInput.Provider)
	require.Equal(t, "deepseek-chat", buildInput.Model)
	require.Equal(t, "v1", buildInput.PromptVersion)

	var result TranslateBrregSourceCompaniesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "queued", result.Status)
	require.EqualValues(t, 2, result.StatusRowsInserted)
	require.EqualValues(t, 3, result.FieldsSeen)
}
```

In `scheduler/internal/ariregister/workflow/company_translation_test.go`, add the Ariregister version:

```go
func TestTranslateAriregisterSourceCompaniesPreparesQueueAndReturnsQueued(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateAriregisterSourceCompanies)

	var buildInput BuildAriregisterTranslationWorksetInput
	env.RegisterActivityWithOptions(func(input BuildAriregisterTranslationWorksetInput) (BuildAriregisterTranslationWorksetResult, error) {
		buildInput = input
		return BuildAriregisterTranslationWorksetResult{
			FieldsExported:    3,
			TermsExported:     3,
			CompaniesExported: 2,
			CompaniesQueued:   2,
		}, nil
	}, activity.RegisterOptions{Name: buildAriregisterTranslationWorksetActivity})

	env.ExecuteWorkflow(TranslateAriregisterSourceCompanies, TranslateAriregisterSourceCompaniesInput{
		Provider:      "deepseek",
		Model:         "deepseek-chat",
		PromptVersion: "v1",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "deepseek", buildInput.Provider)
	require.Equal(t, "deepseek-chat", buildInput.Model)
	require.Equal(t, "v1", buildInput.PromptVersion)

	var result TranslateAriregisterSourceCompaniesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "queued", result.Status)
	require.EqualValues(t, 2, result.StatusRowsInserted)
	require.EqualValues(t, 3, result.FieldsSeen)
}
```

- [ ] **Step 2: Run workflow tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/brreg/workflow ./internal/ariregister/workflow -run 'TestTranslate(Brreg|Ariregister)SourceCompaniesPreparesQueueAndReturnsQueued' -count=1
```

Expected: FAIL because build input lacks provider/model and workflow still expects to claim/translate batches.

- [ ] **Step 3: Add provider/model to build inputs**

In both source action files, extend build input:

```go
type BuildBrregTranslationWorksetInput struct {
	Path          string            `json:"path"`
	Provider      string            `json:"provider,omitempty"`
	Model         string            `json:"model,omitempty"`
	PromptVersion string            `json:"prompt_version,omitempty"`
	IDs           []string          `json:"ids,omitempty"`
	Filters       map[string]string `json:"filters,omitempty"`
	CompanyLimit  int32             `json:"company_limit,omitempty"`
	FieldLimit    int32             `json:"field_limit,omitempty"`
}
```

Pass into `PrepareTranslationQueueCommand`:

```go
prepared, err := a.store.PrepareTranslationQueue(ctx, companydata.PrepareTranslationQueueCommand{
	IDs:           input.IDs,
	Filters:       input.Filters,
	CompanyLimit:  input.CompanyLimit,
	Provider:      input.Provider,
	Model:         input.Model,
	PromptVersion: input.PromptVersion,
})
```

- [ ] **Step 4: Simplify workflows to prep-only**

In both workflow files, after successful queue preparation set:

```go
result.FieldsSeen = prepared.FieldsExported
result.StatusRowsInserted += prepared.CompaniesQueued
if prepared.CompaniesQueued == 0 && prepared.FieldsExported == 0 {
	result.Status = "drained"
	return result, nil
}
result.Status = "queued"
return result, nil
```

Remove the claim/translate/complete loop from the workflow path. Keep the release and complete activity definitions registered because the background result collector uses the same source action methods for recovery and completion.

When executing build activity, pass:

```go
Provider:      input.Provider,
Model:         input.Model,
PromptVersion: input.PromptVersion,
```

- [ ] **Step 5: Run workflow tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/brreg/workflow ./internal/ariregister/workflow -count=1
```

Expected: PASS after removing or updating old synchronous translation tests.

- [ ] **Step 6: Commit workflow conversion**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/brreg/workflow/company_translation.go scheduler/internal/ariregister/workflow/company_translation.go scheduler/internal/brreg/workflow/company_translation_test.go scheduler/internal/ariregister/workflow/company_translation_test.go scheduler/internal/brreg/actions/company_translation_workset_actions.go scheduler/internal/ariregister/actions/company_translation_workset_actions.go
git commit -m "feat: make source translation workflows prepare queues"
```

Expected: commit succeeds.

---

### Task 9: Add Python JetStream Pull Worker

**Files:**
- Modify: `../data-pipelines/services/translation-service/src/corpscout_translation_service/nats_worker.py`
- Test: `../data-pipelines/services/translation-service/tests/test_nats_worker.py`

- [ ] **Step 1: Write Python JetStream handler tests**

Append to `../data-pipelines/services/translation-service/tests/test_nats_worker.py`:

```python
import json

from corpscout_translation_service.models import TermTranslationResponse, TermTranslationResultItem
from corpscout_translation_service.nats_worker import handle_jetstream_translation_job


class FakeJetStreamMessage:
    def __init__(self, payload: dict):
        self.data = json.dumps(payload).encode("utf-8")
        self.acked = False

    async def ack(self) -> None:
        self.acked = True


class FakeResultPublisher:
    def __init__(self) -> None:
        self.results: list[dict] = []

    async def publish_result(self, result: object) -> None:
        self.results.append(result.model_dump(exclude_none=True))


class FakeTermService:
    async def translate_brreg_terms(self, request):
        return TermTranslationResponse(
            request_id=request.request_id,
            source=request.source,
            source_lang=request.source_lang,
            target_lang=request.target_lang,
            provider=request.provider,
            model=request.model,
            prompt_version=request.prompt_version,
            results=[
                TermTranslationResultItem(
                    term_key=request.terms[0].term_key,
                    source_text=request.terms[0].source_text,
                    source_text_normalized=request.terms[0].source_text_normalized,
                    translated_text="Limited liability company",
                )
            ],
        )


async def test_jetstream_translation_job_acks_before_publishing_result() -> None:
    message = FakeJetStreamMessage(
        {
            "job_id": "job-1",
            "batch_id": "batch-1",
            "source": "brreg",
            "source_lang": "no",
            "target_lang": "en",
            "provider": "default",
            "prompt_version": "v1",
            "company_ids": ["company-a"],
            "terms": [
                {
                    "term_key": "a" * 64,
                    "source_text": "Aksjeselskap",
                    "source_text_normalized": "aksjeselskap",
                }
            ],
        }
    )
    publisher = FakeResultPublisher()

    await handle_jetstream_translation_job(message, FakeTermService(), publisher)

    assert message.acked is True
    assert publisher.results[0]["batch_id"] == "batch-1"
    assert publisher.results[0]["company_ids"] == ["company-a"]
    assert publisher.results[0]["results"][0]["translated_text"] == "Limited liability company"
```

- [ ] **Step 2: Run Python handler test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/data-pipelines/services/translation-service
uv run pytest tests/test_nats_worker.py::test_jetstream_translation_job_acks_before_publishing_result -q
```

Expected: FAIL because `handle_jetstream_translation_job` does not exist.

- [ ] **Step 3: Implement JetStream handler**

In `nats_worker.py`, import the new models:

```python
from corpscout_translation_service.models import (
    JetStreamTranslationFailureItem,
    JetStreamTranslationJob,
    JetStreamTranslationResult,
    JetStreamTranslationResultItem,
)
```

Add constants:

```python
JETSTREAM_JOB_SUBJECT = "source.translation.jobs"
JETSTREAM_RESULT_SUBJECT = "source.translation.results"
JETSTREAM_STREAM = "SOURCE_TRANSLATION"
JETSTREAM_DURABLE = "translation-service"
```

Add publisher protocol and handler:

```python
class ResultPublisher(Protocol):
    async def publish_result(self, result: JetStreamTranslationResult) -> None: ...


async def handle_jetstream_translation_job(
    message: Any,
    service: BrregTranslationService,
    publisher: ResultPublisher,
) -> None:
    payload: Any | None = None
    try:
        payload = json.loads(message.data.decode("utf-8"))
        job = JetStreamTranslationJob.model_validate(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
        ack = getattr(message, "ack", None)
        if ack is not None:
            await ack()
        LOGGER.exception("Invalid JetStream translation job payload")
        return

    await message.ack()

    request = TermTranslationRequest(
        request_id=job.job_id,
        source=job.source,
        source_lang=job.source_lang,
        target_lang=job.target_lang,
        provider=job.provider,
        model=job.model,
        prompt_version=job.prompt_version,
        terms=[
            TermTranslationRequestTerm(
                term_key=term.term_key,
                source_text=term.source_text,
                source_text_normalized=term.source_text_normalized,
            )
            for term in job.terms
        ],
    )
    try:
        response = await service.translate_brreg_terms(request)
        result = JetStreamTranslationResult(
            job_id=job.job_id,
            batch_id=job.batch_id,
            source=job.source,
            status="succeeded" if not response.failures else "partial",
            provider=response.provider,
            model=response.model,
            prompt_version=response.prompt_version,
            company_ids=job.company_ids,
            duration_ms=0,
            results=[
                JetStreamTranslationResultItem(
                    term_key=item.term_key,
                    source_text=item.source_text,
                    source_text_normalized=item.source_text_normalized,
                    translated_text=item.translated_text,
                    status=item.status,
                )
                for item in response.results
            ],
            failures=[
                JetStreamTranslationFailureItem(
                    term_key=item.term_key,
                    source_text=item.source_text,
                    source_text_normalized=item.source_text_normalized,
                    status=item.status,
                    error_code=item.error_code,
                    error=item.error,
                )
                for item in response.failures
            ],
        )
    except Exception as exc:
        LOGGER.exception("JetStream translation job failed")
        result = JetStreamTranslationResult(
            job_id=job.job_id,
            batch_id=job.batch_id,
            source=job.source,
            status="failed",
            provider=job.provider,
            model=job.model,
            prompt_version=job.prompt_version,
            company_ids=job.company_ids,
            duration_ms=0,
            failures=[
                JetStreamTranslationFailureItem(
                    term_key=term.term_key,
                    source_text=term.source_text,
                    source_text_normalized=term.source_text_normalized,
                    error_code="translation_worker_error",
                    error=str(exc),
                )
                for term in job.terms
            ],
        )
    await publisher.publish_result(result)
```

- [ ] **Step 4: Implement JetStream result publisher and pull loop**

Add:

```python
class JetStreamResultPublisher:
    def __init__(self, js: Any) -> None:
        self._js = js

    async def publish_result(self, result: JetStreamTranslationResult) -> None:
        await self._js.publish(JETSTREAM_RESULT_SUBJECT, result.model_dump_json(exclude_none=True).encode("utf-8"))
```

In `run_worker`, after `nc = await nats.connect(nats_url)`:

```python
    js = nc.jetstream()
    await js.add_stream(name=JETSTREAM_STREAM, subjects=[JETSTREAM_JOB_SUBJECT, JETSTREAM_RESULT_SUBJECT])
    pull_subscription = await js.pull_subscribe(
        JETSTREAM_JOB_SUBJECT,
        durable=JETSTREAM_DURABLE,
        stream=JETSTREAM_STREAM,
    )
    result_publisher = JetStreamResultPublisher(js)
```

Add a pull loop that fetches one message at a time:

```python
    async def jetstream_loop() -> None:
        while True:
            messages = await pull_subscription.fetch(batch=1, timeout=1)
            for message in messages:
                async with semaphore:
                    await handle_jetstream_translation_job(message, service, result_publisher)
```

Start it together with existing core NATS subscriptions:

```python
        jetstream_task = asyncio.create_task(jetstream_loop())
        await nc.subscribe(subject, queue=queue, cb=callback)
        await nc.subscribe(TERM_REQUEST_SUBJECT, queue=queue, cb=term_callback)
        while True:
            await asyncio.sleep(3600)
```

In `finally`, cancel the task before drain:

```python
        jetstream_task.cancel()
        await nc.drain()
```

- [ ] **Step 5: Run Python NATS worker tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/data-pipelines/services/translation-service
uv run pytest tests/test_nats_worker.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit Python worker**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add ../data-pipelines/services/translation-service/src/corpscout_translation_service/models.py ../data-pipelines/services/translation-service/src/corpscout_translation_service/nats_worker.py ../data-pipelines/services/translation-service/tests/test_models.py ../data-pipelines/services/translation-service/tests/test_nats_worker.py
git commit -m "feat: process translation jobs from jetstream"
```

Expected: commit succeeds.

---

### Task 10: Add Result Pulling And Ack Behavior In Scheduler

**Files:**
- Modify: `scheduler/internal/translationqueue/jetstream.go`
- Modify: `scheduler/internal/translationqueue/service.go`
- Test: `scheduler/internal/translationqueue/service_test.go`

- [ ] **Step 1: Write service result-loop test with fake result source**

Create `scheduler/internal/translationqueue/service_test.go`:

```go
package translationqueue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServiceHandlesResultAndAcksAfterCollectorSuccess(t *testing.T) {
	message := &fakeResultMessage{
		result: TranslationResult{
			BatchID:       "batch-1",
			Source:        "brreg",
			Status:        "failed",
			PromptVersion: "v1",
		},
	}
	source := &collectorSourceStub{name: "brreg"}
	service := NewResultService(NewResultCollector(NewSourceRegistry([]SourceQueue{source})), &fakeResultConsumer{
		messages: []ResultMessage{message},
	})

	err := service.DrainOnce(context.Background())

	require.NoError(t, err)
	require.True(t, message.acked)
	require.Equal(t, int32(1), source.released)
}

type fakeResultConsumer struct {
	messages []ResultMessage
}

func (c *fakeResultConsumer) FetchResults(context.Context, int) ([]ResultMessage, error) {
	messages := c.messages
	c.messages = nil
	return messages, nil
}

type fakeResultMessage struct {
	result TranslationResult
	acked  bool
}

func (m *fakeResultMessage) Result() TranslationResult { return m.result }
func (m *fakeResultMessage) Ack(context.Context) error {
	m.acked = true
	return nil
}
```

- [ ] **Step 2: Run service result-loop test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/translationqueue -run TestServiceHandlesResultAndAcksAfterCollectorSuccess -count=1
```

Expected: FAIL because result consumer/service types do not exist.

- [ ] **Step 3: Implement result consumer interfaces**

In `scheduler/internal/translationqueue/jetstream.go`, add:

```go
type ResultMessage interface {
	Result() TranslationResult
	Ack(context.Context) error
}

type ResultConsumer interface {
	FetchResults(context.Context, int) ([]ResultMessage, error)
}
```

Add a JetStream-backed implementation:

```go
type natsResultConsumer struct {
	sub *nats.Subscription
}

func (c natsResultConsumer) FetchResults(ctx context.Context, batch int) ([]ResultMessage, error) {
	if batch <= 0 {
		batch = 1
	}
	msgs, err := c.sub.Fetch(batch, nats.Context(ctx))
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) {
			return nil, nil
		}
		return nil, err
	}
	results := make([]ResultMessage, 0, len(msgs))
	for _, msg := range msgs {
		var decoded TranslationResult
		if err := json.Unmarshal(msg.Data, &decoded); err != nil {
			_ = msg.Ack()
			continue
		}
		results = append(results, natsResultMessage{msg: msg, result: decoded})
	}
	return results, nil
}

type natsResultMessage struct {
	msg    *nats.Msg
	result TranslationResult
}

func (m natsResultMessage) Result() TranslationResult { return m.result }
func (m natsResultMessage) Ack(context.Context) error { return m.msg.Ack() }
```

- [ ] **Step 4: Implement result service**

In `scheduler/internal/translationqueue/service.go`, add:

```go
type ResultService struct {
	collector *ResultCollector
	consumer  ResultConsumer
}

func NewResultService(collector *ResultCollector, consumer ResultConsumer) *ResultService {
	return &ResultService{collector: collector, consumer: consumer}
}

func (s *ResultService) DrainOnce(ctx context.Context) error {
	messages, err := s.consumer.FetchResults(ctx, 1)
	if err != nil {
		return err
	}
	for _, message := range messages {
		if err := s.collector.HandleResult(ctx, message.Result()); err != nil {
			return err
		}
		if err := message.Ack(ctx); err != nil {
			return err
		}
	}
	return nil
}
```

Update `Service.Start` to run both dispatcher and result loops:

```go
// Add resultService *ResultService to Service.
// In Start, launch a second goroutine:
s.wg.Add(1)
go func() {
	defer s.wg.Done()
	for {
		if err := s.resultService.DrainOnce(runCtx); err != nil {
			slog.ErrorContext(runCtx, "drain translation result messages", "error", err)
		}
		select {
		case <-runCtx.Done():
			return
		default:
		}
	}
}()
```

- [ ] **Step 5: Run service tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/translationqueue -run 'TestService|TestResultCollector' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit result consumer**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/translationqueue/jetstream.go scheduler/internal/translationqueue/service.go scheduler/internal/translationqueue/service_test.go
git commit -m "feat: consume translation jetstream results"
```

Expected: commit succeeds.

---

### Task 11: Final Verification And Operational Checks

**Files:**
- Modify: `.env.example`
- Modify: `docker-compose.yml`
- Test: full scheduler and translation-service checks

- [ ] **Step 1: Document environment defaults**

Add to `.env.example`:

```dotenv
# Source translation JetStream buffer
CORPSCOUT_TRANSLATION_JETSTREAM_ENABLED=true
CORPSCOUT_TRANSLATION_SOURCE_BUFFER_TARGET=2
CORPSCOUT_TRANSLATION_DISPATCH_INTERVAL_SECONDS=2
CORPSCOUT_TRANSLATION_BATCH_LEASE_SECONDS=1800
TRANSLATION_WORKER_MAX_CONCURRENT_REQUESTS=1
```

- [ ] **Step 2: Pass translation worker concurrency in compose**

Modify the `translation-service` service in `../data-pipelines/services/docker-compose.yml` so its environment includes:

```yaml
environment:
  TRANSLATION_WORKER_MAX_CONCURRENT_REQUESTS: ${TRANSLATION_WORKER_MAX_CONCURRENT_REQUESTS:-1}
```

Keep the NATS server config with JetStream enabled:

```conf
jetstream {
  store_dir: "/data"
}
```

- [ ] **Step 3: Run scheduler tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/db/gen ./internal/sourcetranslation ./internal/translationqueue ./internal/brreg/companydata ./internal/ariregister/companydata ./internal/brreg/actions ./internal/ariregister/actions ./internal/brreg/workflow ./internal/ariregister/workflow ./internal/app ./internal/httpapi -count=1
```

Expected: PASS.

- [ ] **Step 4: Run UI typecheck**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
pnpm typecheck
```

Expected: PASS. A Node deprecation warning for `module.register()` is acceptable if it remains unchanged.

- [ ] **Step 5: Run translation-service tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/data-pipelines/services/translation-service
uv run pytest -q
```

Expected: PASS.

- [ ] **Step 6: Run local smoke flow**

Start services:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
docker compose up -d nats postgres temporal scheduler
cd /Users/graovic/pulsarpoint/ppoint/companycollect/data-pipelines/services
docker compose up -d translation-service
```

Trigger a small translation queue from the Corpscout UI or API with BRREG and Ariregister. Verify:

```sql
SELECT source, count(*)
FROM source_translation.running_queue_batches
GROUP BY source
ORDER BY source;
```

Expected while work is buffered:

```text
ariregister | 2
brreg       | 2
```

Verify JetStream has jobs/results:

```bash
nats stream info SOURCE_TRANSLATION --server nats://localhost:4222
```

Expected: stream exists with subjects `source.translation.jobs` and `source.translation.results`.

- [ ] **Step 7: Commit operational config**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add .env.example docker-compose.yml
git commit -m "chore: document translation jetstream buffer config"
```

Expected: commit succeeds.

---

## Self-Review

Spec coverage:

- JetStream as buffer only: covered by Tasks 4, 5, 9, and 10.
- Postgres source of truth: covered by Tasks 2, 5, 6, and 8.
- Two buffered batches per source: covered by Tasks 2, 5, and 11.
- Translation service one batch at a time: covered by Task 9 and Task 11 config.
- Early input ack: covered by Task 9.
- Result ack after Postgres write: covered by Task 10.
- Stale Postgres recovery: existing reset APIs remain in source adapters; Task 7 config preserves 1800 second lease.
- No Postgres in translation service: covered by Task 9, which only uses JetStream and service models.

Placeholder scan:

- This plan contains no `TBD`, no implementation placeholders, and no references to undefined task outputs without defining them in earlier tasks.

Type consistency:

- Go payload names are `TranslationJob`, `TranslationResult`, `TranslationJobTerm`, `TranslationResultTerm`, and `TranslationFailureResult`.
- Python payload names are `JetStreamTranslationJob`, `JetStreamTranslationResult`, `JetStreamTranslationJobTerm`, `JetStreamTranslationResultItem`, and `JetStreamTranslationFailureItem`.
- Queue adapter names are `SourceQueue`, `SourceRegistry`, `ClaimBatchCommand`, and `ClaimBatchResult`.
