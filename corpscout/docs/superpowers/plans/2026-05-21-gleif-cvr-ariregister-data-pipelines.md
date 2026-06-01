# GLEIF CVR Ariregister Data Pipelines Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move GLEIF, CVR Denmark, and Ariregister Estonia into source-specific Temporal data-pipeline modules and wire those modules back into corpscout raw-input review, translation, contact extraction, ownership preservation, domain signals, and financial suggestions.

**Architecture:** CorpScout remains the control plane and review system. Its scheduler maps each data source to a Temporal workflow and records execution state. The sibling `data-pipelines` service owns source-specific workflows, Python download activities, and Go import/translation activities. Each source writes one company-level raw-input row per source-native company identifier, and CorpScout approval turns those raw rows into resolved companies plus official contacts, website/domain signals, financial rows, and ownership evidence.

**Tech Stack:** Go, `log/slog`, `github.com/cockroachdb/errors`, sqlc, pgx, pgxmock, Temporal Go SDK, Python `temporalio`, `httpx`, `respx`, `pytest`, PostgreSQL migrations, existing CorpScout scheduler and raw-input review APIs.

---

## Scope

This plan covers only:

- `gleif`
- `cvr`
- `ariregister`

OpenCorporates is intentionally excluded from implementation here. It remains an enrichment/reconciliation source unless the project has licensed bulk access.

The intended source order is:

1. Shared CorpScout schema, source mapping, and review foundation.
2. GLEIF bulk module.
3. Ariregister bulk module.
4. CVR Datafordeler bulk module.
5. Shared translation, approval, contact, ownership, domain, and financial extraction.
6. End-to-end verification.

## Current Gaps To Close

- CorpScout `DataTaskWorker` maps only `companies_house` and `brreg`.
- `handleTriggerSource` currently treats empty default country as "not Temporal", which breaks global sources such as GLEIF.
- `data-pipelines` registers only `PullCompaniesHouse`, `PullBrreg`, domain enrichment, and Brreg translation workflows.
- `WriteRawInputs` supports only `companies_house` and `brreg`.
- `cvr` and `ariregister` exist in `data_sources`, but their raw input tables are missing.
- `gleif_company_raw_inputs` exists, but it was created for the legacy source-pull path and still needs Temporal-safe insert semantics.
- Raw-input approval supports only GLEIF, Companies House, and Brreg.
- Raw-input list/detail/retry endpoints are hardcoded around the current tables.
- Translation exists only for Brreg.
- CVR and Ariregister contacts, ownership, and financial data are not persisted into the resolved company model.

## File Map

### CorpScout Files

| File | Change |
|---|---|
| `database/migrations/000040_source_pipeline_modules.up.sql` | Add CVR and Ariregister raw input tables, update GLEIF Temporal compatibility, update source metadata, extend `v_source_raw_inputs`. |
| `database/migrations/000040_source_pipeline_modules.down.sql` | Drop new tables and restore the previous raw input view shape. |
| `database/queries/raw_inputs.sql` | Add CVR and Ariregister upsert, retry, ignore, approval, and translation-aware claim queries. |
| `scheduler/internal/db/gen/*` | Regenerate sqlc after query changes. |
| `scheduler/internal/workers/data_task.go` | Add source workflow configuration for GLEIF, CVR, and Ariregister. |
| `scheduler/internal/workers/data_task_test.go` | Cover source workflow mapping and bulk-first mode selection. |
| `scheduler/internal/httpapi/sources.go` | Trigger Temporal workflows using an explicit `ok` result, not a non-empty country check. Add source-specific translation endpoints if kept in source routes. |
| `scheduler/internal/httpapi/sources_test.go` | Cover global GLEIF trigger and translation route behavior. |
| `scheduler/internal/httpapi/raw_inputs.go` | Include CVR and Ariregister in list, detail, retry, and ignore handlers. |
| `scheduler/internal/httpapi/raw_inputs_test.go` | Cover new source rows and translation filters. |
| `scheduler/internal/service/raw_input_approval.go` | Load and approve CVR and Ariregister rows; persist official contacts, ownership evidence, and financial suggestions. |
| `scheduler/internal/service/raw_input_approval_test.go` | Cover approval, translation gating, contacts, ownership, and financial extraction. |
| `scheduler/internal/workers/brreg_processor_query_test.go` | Rename or extend to cover translation-gated claim queries for CVR and Ariregister. |

### Data-Pipelines Go Files

| File | Change |
|---|---|
| `services/go-worker/contracts/contracts.go` | Add source-specific workflow, download, import, and translation contracts. |
| `services/go-worker/cmd/worker/main.go` | Register new workflows and activities. |
| `services/go-worker/workflows/pull_gleif.go` | Implement GLEIF bulk and delta workflow. |
| `services/go-worker/workflows/pull_gleif_test.go` | Temporal workflow tests. |
| `services/go-worker/workflows/pull_ariregister.go` | Implement Ariregister daily file refresh workflow. |
| `services/go-worker/workflows/pull_ariregister_test.go` | Temporal workflow tests. |
| `services/go-worker/workflows/pull_cvr.go` | Implement CVR Datafordeler file workflow. |
| `services/go-worker/workflows/pull_cvr_test.go` | Temporal workflow tests. |
| `services/go-worker/workflows/translate_source_raw_inputs.go` | Generalize Brreg translation workflow for CVR and Ariregister while preserving Brreg compatibility. |
| `services/go-worker/workflows/translate_source_raw_inputs_test.go` | Translation workflow tests for each table. |
| `services/go-worker/activities/gleif_import.go` | Stream GLEIF Golden Copy files into `gleif_company_raw_inputs`. |
| `services/go-worker/activities/gleif_import_test.go` | GLEIF import tests with fixture files. |
| `services/go-worker/activities/ariregister_import.go` | Import Estonia open-data datasets into `ariregister_company_raw_inputs`. |
| `services/go-worker/activities/ariregister_import_test.go` | Ariregister import tests with fixture files. |
| `services/go-worker/activities/cvr_import.go` | Import Datafordeler CVR file sets into `cvr_company_raw_inputs`. |
| `services/go-worker/activities/cvr_import_test.go` | CVR import tests with fixture files. |
| `services/go-worker/activities/source_translation_activity.go` | Source-generic claim, cache, payload build, and write translation activity. |
| `services/go-worker/activities/source_translation_payload.go` | Deterministic CVR and Ariregister payload normalization plus LLM term extraction. |
| `services/go-worker/activities/source_translation_payload_test.go` | Unit tests for Danish and Estonian normalized payloads. |
| `services/go-worker/testdata/*` | Small source fixtures for GLEIF, CVR, and Ariregister. |

### Data-Pipelines Python Files

| File | Change |
|---|---|
| `services/python-worker/contracts.py` | Add download request/result contracts. |
| `services/python-worker/main.py` | Register new Python activities. |
| `services/python-worker/activities/download_gleif_golden_copy.py` | Download GLEIF latest full or delta file into worker storage. |
| `services/python-worker/activities/download_ariregister_dataset.py` | Download configured Estonia open-data datasets. |
| `services/python-worker/activities/download_cvr_file_set.py` | Download configured Datafordeler CVR file set with API key or OAuth header support. |
| `services/python-worker/test_download_sources.py` | `respx` tests for source download behavior and credential failures. |

## Data Contracts

Add these Go contracts in `services/go-worker/contracts/contracts.go` and mirror the download contracts in `services/python-worker/contracts.py`.

```go
type PullGLEIFInput struct {
	CorpscoutRunID  string              `json:"corpscout_run_id"`
	RunID           string              `json:"run_id,omitempty"`
	Mode            string              `json:"mode,omitempty"` // bulk or delta
	DeltaWindow     string              `json:"delta_window,omitempty"`
	OutputDir       string              `json:"output_dir,omitempty"`
	Force           bool                `json:"force,omitempty"`
	Accumulated     PullCompaniesResult `json:"accumulated,omitempty"`
}

type PullAriregisterInput struct {
	CorpscoutRunID string              `json:"corpscout_run_id"`
	RunID          string              `json:"run_id,omitempty"`
	Mode           string              `json:"mode,omitempty"` // bulk or refresh
	OutputDir      string              `json:"output_dir,omitempty"`
	Force          bool                `json:"force,omitempty"`
	Accumulated    PullCompaniesResult `json:"accumulated,omitempty"`
}

type PullCVRInput struct {
	CorpscoutRunID string              `json:"corpscout_run_id"`
	RunID          string              `json:"run_id,omitempty"`
	Mode           string              `json:"mode,omitempty"` // bulk or incremental
	OutputDir      string              `json:"output_dir,omitempty"`
	Force          bool                `json:"force,omitempty"`
	Accumulated    PullCompaniesResult `json:"accumulated,omitempty"`
}

type DownloadedSourceFile struct {
	Source     string `json:"source"`
	Dataset    string `json:"dataset"`
	FilePath   string `json:"file_path"`
	SnapshotID string `json:"snapshot_id"`
	SHA256     string `json:"sha256"`
	Format     string `json:"format"`
}

type DownloadSourceFilesResult struct {
	Source     string                 `json:"source"`
	SnapshotID string                 `json:"snapshot_id"`
	Files      []DownloadedSourceFile `json:"files"`
}

type ImportSourceBulkParams struct {
	Files          []DownloadedSourceFile `json:"files"`
	RunID          string                 `json:"run_id"`
	CorpscoutRunID string                 `json:"corpscout_run_id"`
	Force          bool                   `json:"force,omitempty"`
}
```

Use source-specific type aliases if that keeps workflow signatures clearer:

```go
type ImportGLEIFGoldenCopyParams = ImportSourceBulkParams
type ImportAriregisterBulkParams = ImportSourceBulkParams
type ImportCVRBulkParams = ImportSourceBulkParams
```

## Task 1: CorpScout Source Workflow Mapping

**Files:**

- `scheduler/internal/workers/data_task.go`
- `scheduler/internal/workers/data_task_test.go`
- `scheduler/internal/httpapi/sources.go`
- `scheduler/internal/httpapi/sources_test.go`

- [ ] Write a failing test in `scheduler/internal/workers/data_task_test.go`.

The test must assert these mappings:

```go
cases := []struct {
	source       string
	workflow     string
	country      string
	firstMode    string
	nextMode     string
	bulkFirst    bool
}{
	{"companies_house", "PullCompaniesHouse", "GB", "", "", false},
	{"brreg", "PullBrreg", "NO", "bulk", "incremental", true},
	{"gleif", "PullGLEIF", "", "bulk", "delta", true},
	{"cvr", "PullCVR", "DK", "bulk", "incremental", true},
	{"ariregister", "PullAriregister", "EE", "bulk", "refresh", true},
}
```

Expected red result:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/workers -run TestTemporalWorkflowForSource
```

The failure should show that GLEIF, CVR, and Ariregister are not mapped.

- [ ] Replace the current string maps in `data_task.go` with an explicit config.

Use this shape:

```go
type TemporalSourceWorkflow struct {
	WorkflowType string
	Country      string
	FirstMode    string
	NextMode     string
	BulkFirst    bool
}

func TemporalWorkflowForSource(source string) (TemporalSourceWorkflow, bool) {
	cfg, ok := sourceWorkflows[source]
	return cfg, ok
}
```

The data task worker should:

- start `FirstMode` when the source is bulk-first and no checkpoint exists;
- start `NextMode` when a checkpoint exists;
- pass `country`, `mode`, `cursor`, `incremental_from`, `corpscout_run_id`, `run_id`, and `force` to the workflow input map;
- keep Companies House behavior unchanged.

- [ ] Update `handleTriggerSource` in `sources.go` to use the `ok` return value.

This fixes GLEIF because its country is intentionally empty.

- [ ] Add a handler test proving `POST /sources/gleif/trigger` starts a Temporal workflow.

Expected green command:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/workers ./internal/httpapi
```

## Task 2: CorpScout Raw Table Migrations

**Files:**

- `database/migrations/000040_source_pipeline_modules.up.sql`
- `database/migrations/000040_source_pipeline_modules.down.sql`

- [ ] Write the migration.

The `up` migration must:

- make `gleif_company_raw_inputs.source_pull_run_id` nullable for Temporal-written rows;
- add missing GLEIF helper columns when absent:
  - `legal_jurisdiction TEXT`
  - `legal_form_code TEXT`
  - `legal_form_name TEXT`
  - `registration_authority_id TEXT`
  - `entity_category TEXT`
  - `entity_creation_date DATE`
- create `cvr_company_raw_inputs`;
- create `ariregister_company_raw_inputs`;
- add processing, run, translation, and payload hash indexes for both new tables;
- set `data_sources.pull_task_type = 'data_task'` for `gleif`, `cvr`, and `ariregister`;
- update CVR config away from `cvrapi.dk` and toward Datafordeler file download configuration;
- update Ariregister config toward official open-data file configuration;
- set `data_sources.requires_translation = true` for `cvr` and `ariregister`;
- keep `data_sources.requires_translation = false` for `gleif`;
- extend `v_source_raw_inputs` with CVR and Ariregister rows.

Use this CVR table shape:

```sql
CREATE TABLE cvr_company_raw_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_pull_run_id UUID REFERENCES source_pull_runs(id),
    source_native_id TEXT NOT NULL,
    cvr_number TEXT NOT NULL,
    company_name TEXT,
    registration_status TEXT,
    company_type TEXT,
    website TEXT,
    email TEXT,
    phone TEXT,
    country_iso2 TEXT DEFAULT 'DK',
    source_updated_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL,
    raw_payload_en JSONB,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_status TEXT NOT NULL DEFAULT 'pending',
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    processing_error TEXT,
    processing_lease_by TEXT,
    processing_lease_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    run_id TEXT,
    translation_status TEXT NOT NULL DEFAULT 'pending',
    translation_attempts INTEGER NOT NULL DEFAULT 0,
    translation_error TEXT,
    translation_model TEXT,
    translation_prompt_version TEXT,
    translated_at TIMESTAMPTZ,
    translation_lease_by TEXT,
    translation_lease_until TIMESTAMPTZ,
    translation_fx_source TEXT,
    translation_fx_rate_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_cvr_raw_source_native CHECK (source_native_id = cvr_number),
    CONSTRAINT chk_cvr_raw_status CHECK (processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')),
    CONSTRAINT chk_cvr_translation_status CHECK (translation_status IN ('pending', 'translating', 'translated', 'failed')),
    CONSTRAINT chk_cvr_raw_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_cvr_translation_attempts CHECK (translation_attempts >= 0),
    CONSTRAINT chk_cvr_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT chk_cvr_raw_payload_en_object CHECK (raw_payload_en IS NULL OR jsonb_typeof(raw_payload_en) = 'object'),
    CONSTRAINT uq_cvr_company_raw_inputs_payload UNIQUE (cvr_number, payload_hash)
);
```

Use the same shape for Ariregister with source-specific columns:

```sql
source_native_id TEXT NOT NULL,
registry_code TEXT NOT NULL,
legal_name TEXT,
registration_status TEXT,
legal_form TEXT,
vat_number TEXT,
website TEXT,
email TEXT,
phone TEXT,
country_iso2 TEXT DEFAULT 'EE',
CONSTRAINT chk_ariregister_raw_source_native CHECK (source_native_id = registry_code),
CONSTRAINT uq_ariregister_company_raw_inputs_payload UNIQUE (registry_code, payload_hash)
```

The `down` migration must drop the two new tables and restore `v_source_raw_inputs` to the Brreg-era union. It should not force GLEIF `source_pull_run_id` back to `NOT NULL`, because Temporal-written rows can legitimately have `NULL` in that legacy column.

- [ ] Verify migration SQL syntax with the repository migration command.

Use the repo command if available. Otherwise run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git diff -- database/migrations/000040_source_pipeline_modules.up.sql database/migrations/000040_source_pipeline_modules.down.sql
```

Review for balanced `CREATE OR REPLACE VIEW`, constraints, and indexes.

## Task 3: CorpScout SQLC Queries

**Files:**

- `database/queries/raw_inputs.sql`
- generated files under `scheduler/internal/db/gen/`

- [ ] Add failing query-level tests first.

Create or extend `scheduler/internal/workers/brreg_processor_query_test.go` so it verifies:

- `ClaimPendingBrregRawInputs` gates processing on `raw_payload_en IS NOT NULL`;
- `ClaimPendingCVRRawInputs` gates processing on `raw_payload_en IS NOT NULL`;
- `ClaimPendingAriregisterRawInputs` gates processing on `raw_payload_en IS NOT NULL`;
- the status predicate is grouped so an expired translation lease cannot bypass the payload gate.

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/workers -run TestClaimPending
```

- [ ] Add sqlc queries for both new tables.

Add these query families:

- `UpsertCVRRawInput`
- `ClaimPendingCVRRawInputs`
- `MarkCVRRawInputProcessed`
- `MarkCVRRawInputFailed`
- `RetryCVRRawInput`
- `IgnoreCVRRawInput`
- `GetCVRRawInputForCompanyApproval`
- `UpsertAriregisterRawInput`
- `ClaimPendingAriregisterRawInputs`
- `MarkAriregisterRawInputProcessed`
- `MarkAriregisterRawInputFailed`
- `RetryAriregisterRawInput`
- `IgnoreAriregisterRawInput`
- `GetAriregisterRawInputForCompanyApproval`

Each claim query for translated sources must include:

```sql
WHERE (
    processing_status = 'pending'
    OR (processing_status = 'processing' AND processing_lease_until < now())
)
AND raw_payload_en IS NOT NULL
```

- [ ] Regenerate sqlc.

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
make sqlc-generate
```

- [ ] Run focused tests.

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/workers ./internal/service ./internal/httpapi
```

## Task 4: CorpScout Raw Input APIs

**Files:**

- `scheduler/internal/httpapi/raw_inputs.go`
- `scheduler/internal/httpapi/raw_inputs_test.go`
- `scheduler/internal/httpapi/sources.go`
- `scheduler/internal/httpapi/sources_test.go`

- [ ] Add failing tests for new source list rows.

The list handler must include:

- `cvr` rows with name from `company_name`, native ID from `cvr_number`, and `translation_status`;
- `ariregister` rows with name from `legal_name`, native ID from `registry_code`, and `translation_status`;
- `gleif` rows still without translation fields.

- [ ] Add failing tests for detail, retry, and ignore.

Expected behavior:

- CVR detail includes `raw_payload_en` and translation metadata.
- Ariregister detail includes `raw_payload_en` and translation metadata.
- Retry and ignore dispatch to the new sqlc methods.
- Unsupported source still returns the existing safe client error.

- [ ] Implement minimal source switch additions.

Keep the hardcoded SQL style used by the current handler, but isolate source-specific fragments in small helper structs to avoid duplicating query assembly across more tables.

- [ ] Add source-generic translation endpoints.

Keep existing `/sources/brreg/translate` working. Add:

- `POST /sources/cvr/translate`
- `GET /sources/cvr/translation-stats`
- `POST /sources/ariregister/translate`
- `GET /sources/ariregister/translation-stats`

All three source translation starts should call a single workflow name, `TranslateSourceRawInputs`, with source/table/language parameters.

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/httpapi
```

## Task 5: CorpScout Raw Approval And Enrichment

**Files:**

- `scheduler/internal/service/raw_input_approval.go`
- `scheduler/internal/service/raw_input_approval_test.go`
- `database/queries/company_profile.sql`
- `database/queries/company_financials.sql`
- `database/queries/company_relationships.sql`
- generated sqlc files

- [ ] Add failing approval tests for CVR.

Cover:

- `translation_status != 'translated'` returns `ErrRawInputRequiresTranslation`;
- translated CVR row creates or finds a company by `cvr_number` and `DK`;
- official `website` is copied to `companies.website`;
- official `email` is upserted into `company_emails` with source `cvr`;
- official `phone` is upserted into `company_phones` with source `cvr`;
- annual report metrics in `raw_payload_en.financials` create `company_financials` rows with status `suggested`;
- ownership fragments are preserved in `companies.ownership` evidence when they cannot be resolved to a company relationship.

- [ ] Add failing approval tests for Ariregister.

Cover the same behavior with:

- registry code as registration number;
- country `EE`;
- official contacts;
- annual-report indicator extraction;
- beneficial owner evidence preservation.

- [ ] Extend `rawCompanyCandidate`.

Use explicit nested structs:

```go
type rawCompanyContact struct {
	Kind        string
	Value       string
	Description string
	Source      string
}

type rawCompanyFinancial struct {
	Year            int
	EmployeeCount   *int32
	RevenueAmount   *int64
	RevenueCurrency string
	ProfitAmount    *int64
}

type rawCompanyOwnership struct {
	Source string
	Data   map[string]any
}
```

Extend `rawCompanyCandidate` with:

- `emails []rawCompanyContact`
- `phones []rawCompanyContact`
- `financials []rawCompanyFinancial`
- `ownership []rawCompanyOwnership`

- [ ] Add candidate loaders for CVR and Ariregister.

Both loaders must read `raw_payload_en` after translation and fall back to raw scalar columns for identifiers and display names.

CVR fields:

- `displayName` from `company_name`;
- `registrationNumber` from `cvr_number`;
- `countryISO2` default `DK`;
- `website`, `email`, `phone` from table columns first, then normalized payload;
- `registrationStatus` from table column or normalized payload.

Ariregister fields:

- `displayName` from `legal_name`;
- `registrationNumber` from `registry_code`;
- `countryISO2` default `EE`;
- `website`, `email`, `phone` from table columns first, then normalized payload;
- `registrationStatus` from table column or normalized payload.

- [ ] Persist enrichment after company create or match.

Inside the same transaction, after a company is inserted or found:

- call `UpsertCompanyEmail` for official emails;
- call `UpsertCompanyPhone` for official phone numbers;
- call `CreateCompanyFinancial` for annual financial rows;
- merge unresolved ownership metadata into `companies.ownership`;
- add the raw input ID and source name into all evidence JSON.

Do not log errors in the helper functions. Wrap and return them. The approval handler boundary logs once.

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/service
```

## Task 6: Data-Pipelines Python Download Activities

**Files:**

- `services/python-worker/contracts.py`
- `services/python-worker/activities/download_gleif_golden_copy.py`
- `services/python-worker/activities/download_ariregister_dataset.py`
- `services/python-worker/activities/download_cvr_file_set.py`
- `services/python-worker/main.py`
- `services/python-worker/test_download_sources.py`

- [ ] Add failing tests with `respx`.

Test cases:

- GLEIF downloads a mocked Golden Copy file, computes SHA256, writes to output dir, and returns one `DownloadedSourceFile`.
- GLEIF delta mode chooses a delta URL when `mode == "delta"`.
- Ariregister downloads multiple configured dataset URLs and returns a stable `snapshot_id`.
- CVR fails with a clear error when neither `CVR_FILEDOWNLOAD_API_KEY` nor `CVR_FILEDOWNLOAD_BEARER_TOKEN` is set.
- CVR sends the API key or bearer token only in headers, never logs it, and writes files to output dir.

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/python-worker
pytest -q test_download_sources.py
```

- [ ] Implement download activities.

Configuration:

- `GLEIF_GOLDEN_COPY_BASE_URL`, default `https://goldencopy.gleif.org/api/v2/golden-copies/publishes`
- `ARIREGISTER_DATASETS_JSON`, optional JSON array of `{dataset,url,format}`
- `CVR_FILEDOWNLOAD_BASE_URL`, required for CVR
- `CVR_FILEDOWNLOAD_DATASETS`, comma-separated dataset names
- `CVR_FILEDOWNLOAD_API_KEY`, optional
- `CVR_FILEDOWNLOAD_BEARER_TOKEN`, optional

CVR must not use `cvrapi.dk`. If Datafordeler configuration is missing, return a configuration error.

- [ ] Register activities in `services/python-worker/main.py`.

Activity names:

- `download_gleif_golden_copy`
- `download_ariregister_dataset`
- `download_cvr_file_set`

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/python-worker
pytest -q
```

## Task 7: Data-Pipelines Go Import Activities

**Files:**

- `services/go-worker/activities/gleif_import.go`
- `services/go-worker/activities/gleif_import_test.go`
- `services/go-worker/activities/ariregister_import.go`
- `services/go-worker/activities/ariregister_import_test.go`
- `services/go-worker/activities/cvr_import.go`
- `services/go-worker/activities/cvr_import_test.go`
- `services/go-worker/testdata/gleif_lei2_sample.json`
- `services/go-worker/testdata/ariregister_basic_sample.json`
- `services/go-worker/testdata/ariregister_financials_sample.json`
- `services/go-worker/testdata/cvr_company_sample.jsonl`

- [ ] Write failing import tests first.

GLEIF test expectations:

- inserts into `gleif_company_raw_inputs`;
- sets `source_native_id` and `lei`;
- sets `legal_name`, `registration_status`, `headquarters_country_code`;
- sets `run_id`;
- uses `(lei, payload_hash)` conflict handling;
- skips records without LEI.

Ariregister test expectations:

- assembles one company payload keyed by registry code;
- captures legal name, status, legal form, VAT number, website, email, phone;
- includes annual-report indicators under `raw_payload.financials`;
- sets `translation_status = 'pending'`;
- uses `(registry_code, payload_hash)` conflict handling.

CVR test expectations:

- assembles one company payload keyed by CVR number;
- captures company name, status, legal form, website, email, phone;
- preserves roles, owners, beneficial owners, and financial fragments in raw payload;
- sets `translation_status = 'pending'`;
- uses `(cvr_number, payload_hash)` conflict handling.

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go test ./activities -run 'TestImport(GLEIF|Ariregister|CVR)'
```

- [ ] Implement import activities with streaming parsers.

Implementation constraints:

- Do not read very large source files fully into memory.
- Accept gzip and zip files where the source download returns compressed files.
- Use batch inserts with `pgx.Batch`.
- Use `activity.RecordHeartbeat` after each batch.
- Wrap lower-level errors with source and dataset context.
- Log only source, dataset, file path, record counts, and run ID. Do not log CVR credentials or raw payloads.

- [ ] Register Go activities in `services/go-worker/cmd/worker/main.go`.

Register:

- `goActs.ImportGLEIFGoldenCopy`
- `goActs.ImportAriregisterBulk`
- `goActs.ImportCVRBulk`

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go test ./activities
```

## Task 8: Data-Pipelines Source Workflows

**Files:**

- `services/go-worker/workflows/pull_gleif.go`
- `services/go-worker/workflows/pull_gleif_test.go`
- `services/go-worker/workflows/pull_ariregister.go`
- `services/go-worker/workflows/pull_ariregister_test.go`
- `services/go-worker/workflows/pull_cvr.go`
- `services/go-worker/workflows/pull_cvr_test.go`
- `services/go-worker/cmd/worker/main.go`

- [ ] Write failing workflow tests first.

GLEIF workflow tests:

- calls `download_gleif_golden_copy`;
- calls `ImportGLEIFGoldenCopy`;
- calls `MarkExecutionComplete` with `Source: "gleif"` and empty country;
- stores final cursor `bulk:<snapshot_id>` or `delta:<snapshot_id>`.

Ariregister workflow tests:

- calls `download_ariregister_dataset`;
- calls `ImportAriregisterBulk`;
- calls `MarkExecutionComplete` with `Source: "ariregister"` and `Country: "EE"`;
- stores final cursor `refresh:<snapshot_id>`.

CVR workflow tests:

- calls `download_cvr_file_set`;
- calls `ImportCVRBulk`;
- calls `MarkExecutionComplete` with `Source: "cvr"` and `Country: "DK"`;
- stores final cursor `bulk:<snapshot_id>` or `incremental:<snapshot_id>`.

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go test ./workflows -run 'TestPull(GLEIF|Ariregister|CVR)'
```

- [ ] Implement workflows using the Brreg bulk workflow pattern.

Activity timeout guidance:

- Python download activity: 20 minutes start-to-close, heartbeat 2 minutes.
- Go import activity: 60 minutes start-to-close, heartbeat 2 minutes.
- Mark complete activity: 2 minutes start-to-close.

Retry guidance:

- Download retries: 3 attempts, exponential backoff.
- Import retries: 3 attempts. Imports must be idempotent because they upsert by source ID plus payload hash.
- Mark complete retries: 5 attempts.

- [ ] Register workflows in `services/go-worker/cmd/worker/main.go`.

Register:

- `workflows.PullGLEIF`
- `workflows.PullAriregister`
- `workflows.PullCVR`

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go test ./workflows
```

## Task 9: Source Translation And Normalization

**Files:**

- `services/go-worker/contracts/contracts.go`
- `services/go-worker/workflows/translate_source_raw_inputs.go`
- `services/go-worker/workflows/translate_source_raw_inputs_test.go`
- `services/go-worker/activities/source_translation_activity.go`
- `services/go-worker/activities/source_translation_payload.go`
- `services/go-worker/activities/source_translation_payload_test.go`
- `services/go-worker/activities/brreg_translation_activity.go`
- `services/go-worker/workflows/translate_brreg.go`
- `services/python-worker/activities/llm_translation.py`

- [ ] Write failing tests for a source-generic translation workflow.

Expected behavior:

- Brreg route can still run through `TranslateBrregRawInputs` or a wrapper with the same workflow name.
- CVR uses `source_lang = "da"` and categories:
  - `legal_form`
  - `status`
  - `industry`
  - `role`
  - `ownership_type`
  - `purpose`
  - `signing_rule`
  - `financial_note`
- Ariregister uses `source_lang = "et"` and categories:
  - `legal_form`
  - `status`
  - `activity`
  - `role`
  - `ownership_type`
  - `financial_indicator`
- deterministic field mapping runs before any LLM call;
- company names, registration numbers, LEIs, URLs, email addresses, and phone numbers are never translated;
- failed individual terms mark the row failed with safe error text.

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go test ./activities ./workflows -run 'Test(SourceTranslation|BuildCVR|BuildAriregister|Translate)'
```

- [ ] Generalize translation activity internals.

Keep the existing Brreg behavior working, but move source-specific pieces into a config:

```go
type SourceTranslationConfig struct {
	SourceName       string
	TableName        string
	SourceLang       string
	TargetLang       string
	PromptVersion    string
	Model            string
	ClaimSQL         string
	WriteSuccessSQL  string
	WriteFailureSQL  string
	BuildPayloadEn   func(context.Context, json.RawMessage, SourceTranslationSet, FXRateSet) (json.RawMessage, error)
	ExtractTerms     func(json.RawMessage) ([]SourceTranslationTerm, error)
}
```

Use allowlisted table names only:

- `brreg_company_raw_inputs`
- `cvr_company_raw_inputs`
- `ariregister_company_raw_inputs`

Do not concatenate untrusted table names into SQL.

- [ ] Build normalized English payloads.

Normalized CVR payload should include:

- `identity`
- `legal_form`
- `status`
- `addresses`
- `contacts`
- `industries`
- `roles`
- `owners`
- `beneficial_owners`
- `financials`
- `source_fragments`

Normalized Ariregister payload should include:

- `identity`
- `legal_form`
- `status`
- `addresses`
- `contacts`
- `activities`
- `registry_card_persons`
- `shareholders`
- `beneficial_owners`
- `financials`
- `source_fragments`

- [ ] Register the generic workflow and keep compatibility wrappers.

Register `TranslateSourceRawInputs`.

Keep `TranslateBrregRawInputs` registered so existing UI and operational scripts keep working.

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go test ./...
```

## Task 10: Domain Signals From Official Registry Contacts

**Files:**

- `scheduler/internal/service/raw_input_approval.go`
- `scheduler/internal/service/raw_input_approval_test.go`
- `database/queries/domain_import.sql`
- generated sqlc files

- [ ] Add failing tests for registry website extraction.

Expected behavior:

- CVR website creates or updates the company `website`.
- CVR website creates a domain import candidate or domain crawl signal with signal `registry_website`.
- Ariregister website does the same.
- Email domains are preserved as lower-confidence domain candidates only when the email domain is not a public email provider.

- [ ] Implement a small domain-signal helper.

Use existing domain import/domain crawl queries if they support this flow. If they do not, add the smallest query needed to persist a registry website signal.

Do not infer a company website from an owner email address as a high-confidence signal. Owner emails can be stored as contact evidence, but only company-level website fields should get registry-website confidence.

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/service
```

## Task 11: Financial Extraction

**Files:**

- `scheduler/internal/service/raw_input_approval.go`
- `scheduler/internal/service/raw_input_approval_test.go`
- `database/queries/company_financials.sql`
- generated sqlc files

- [ ] Add failing tests for Ariregister financial indicators.

The extractor should map annual-report indicator fields into:

- year;
- employee count;
- revenue amount and `EUR`;
- profit amount and `EUR`.

- [ ] Add failing tests for CVR financial fragments.

For the first version, map only metrics explicitly present in normalized payload fields. Preserve complete financial fragments in raw evidence when complete revenue/profit fields are not available.

- [ ] Implement extraction.

Rules:

- Create `company_financials` rows with status `suggested`.
- Source names must be `ariregister` and `cvr`.
- Do not overwrite approved financials directly.
- Keep raw evidence with source input ID, source native ID, source snapshot, and original field names.

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/service -run 'TestApproveCompanyRawInput_.*Financial'
```

## Task 12: End-To-End Verification

- [ ] Run CorpScout scheduler tests.

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./...
```

- [ ] Run data-pipelines Go tests.

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go test ./...
```

- [ ] Run data-pipelines Python tests.

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/python-worker
pytest -q
```

- [ ] Run SQL and formatting checks.

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git diff --check
```

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git diff --check
```

- [ ] Run one local Temporal smoke test with fixture-sized files.

Start workers and run:

```bash
temporal workflow start \
  --namespace corpscout \
  --task-queue corpscout-pipelines \
  --type PullGLEIF \
  --input '{"corpscout_run_id":"local-smoke-gleif","mode":"bulk","force":true}'
```

Repeat for:

- `PullAriregister`
- `PullCVR`

Use test fixture URLs or local file server URLs for smoke tests. Do not hit Datafordeler production without configured credentials and explicit operator intent.

## Implementation Notes

- Keep GLEIF as a global source with empty country. Empty country must not mean unsupported.
- Do not use `cvrapi.dk` for the production CVR workflow.
- CVR activity must require Datafordeler configuration and fail clearly when it is absent.
- Do not translate legal names, identifiers, URLs, emails, phone numbers, or raw financial amounts.
- Translation should produce an operator-facing normalized English payload while preserving the original raw payload.
- Official registry websites are high-confidence website/domain signals.
- Email domains are lower-confidence signals and must be filtered for public mail providers.
- Ownership data should be preserved even when it cannot be resolved to another CorpScout company.
- New Go code in CorpScout must wrap errors with `github.com/cockroachdb/errors` and log once only at boundary layers.
- Data-pipelines import activities must be idempotent by source-native ID plus payload hash.
- Large file imports must stream or batch records and heartbeat to Temporal.

## Commit Plan

Commit after each verified group:

1. `feat(corpscout): add source pipeline raw tables`
2. `feat(corpscout): map source workflows for registry pipelines`
3. `feat(corpscout): approve cvr and ariregister raw inputs`
4. `feat(data-pipelines): add gleif pipeline`
5. `feat(data-pipelines): add ariregister pipeline`
6. `feat(data-pipelines): add cvr pipeline`
7. `feat(data-pipelines): generalize source translation`
8. `feat(corpscout): persist registry contacts and financials`

Each commit should have passing focused tests for the files changed in that commit.
