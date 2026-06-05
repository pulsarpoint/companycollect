# Shared Source Translation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Ariregister company translation using a shared SQLite/cached translation core and identical source `companydata` APIs across BRREG and Ariregister.

**Architecture:** Introduce `scheduler/internal/sourcetranslation` for source-neutral workset and workflow mechanics. Keep source-specific SQL, cache tables, and `_en` column application inside each source's `companydata.Store`. Adapt BRREG carefully so its current workflow behavior remains unchanged, then implement Ariregister using the same API.

**Tech Stack:** Go, Temporal, PostgreSQL, sqlc, `modernc.org/sqlite`, NATS translation client, React/Remix UI, TypeScript.

---

## File Structure

- Create `scheduler/internal/sourcetranslation/types.go`: shared command/result types, source config, source adapter interface.
- Create `scheduler/internal/sourcetranslation/text.go`: term normalization and SHA-256 key generation.
- Create `scheduler/internal/sourcetranslation/workset.go`: SQLite workset build, claim, save, and apply orchestration.
- Create `scheduler/internal/sourcetranslation/workset_test.go`: source-neutral SQLite behavior tests.
- Create `scheduler/internal/sourcetranslation/workflow.go`: reusable Temporal workflow helper or source-neutral workflow implementation.
- Modify `scheduler/internal/translationclient/client.go`: add `TranslateTerms` and keep `TranslateBrregTerms` as compatibility wrapper.
- Modify `scheduler/internal/brreg/companydata/workset.go`: move source-neutral workset code to `sourcetranslation`, keep BRREG missing-field/cache/apply logic in `companydata`.
- Modify `scheduler/internal/brreg/companydata/store.go`: expose identical source translation API methods.
- Modify `scheduler/internal/brreg/actions/company_translation_workset_actions.go`: call shared translation client method and shared workset APIs through BRREG store.
- Create `database/migrations/000097_ariregister_source_translation_terms.up.sql`: Ariregister translation cache and translation status materialized view.
- Create `database/migrations/000097_ariregister_source_translation_terms.down.sql`: drop Ariregister translation status and cache.
- Modify `database/queries/ariregister_source_profile.sql`: add Ariregister cache upsert and translation status refresh queries.
- Create `scheduler/internal/ariregister/companydata/store.go`: source-level Ariregister companydata store.
- Create `scheduler/internal/ariregister/companydata/types.go`: Ariregister companydata translation API types if source-local row helpers are needed.
- Create `scheduler/internal/ariregister/companydata/translation.go`: Ariregister missing-field/cache/apply implementation.
- Create `scheduler/internal/ariregister/db/term_translation.go`: Ariregister gateway method for cache upsert.
- Create `scheduler/internal/ariregister/actions/company_translation_actions.go`: Ariregister translation action resource.
- Create `scheduler/internal/ariregister/actions/company_translation_workset_actions.go`: Temporal activities for build/claim/translate/save/apply.
- Create `scheduler/internal/ariregister/workflow/company_translation.go`: Ariregister workflow constants and wrapper.
- Create `scheduler/internal/ariregister/workflow/company_translation_test.go`: Ariregister workflow behavior tests.
- Create `scheduler/internal/app/ariregister_company_translation_temporal.go`: Temporal worker registration.
- Modify `scheduler/internal/app/temporal.go`: construct Ariregister company translation resources and worker.
- Modify `scheduler/internal/httpapi/workflow_triggers.go`: add Ariregister company translation trigger request/handler.
- Modify `scheduler/internal/httpapi/handlers.go`: add `POST /api/v1/workflows/ariregister/company-translation`.
- Modify `ui/app/lib/api.ts`: add Ariregister translation API call.
- Modify `ui/app/types/api.ts`: add Ariregister translation request type.
- Create `ui/app/components/app/AriregisterTranslationActionForm.tsx`: BRREG-style translation action form with Ariregister labels.
- Create `ui/app/components/app/AriregisterSourceEntryActionSheet.tsx`: action sheet that exposes translation action.
- Modify `ui/app/components/app/AriregisterSourceEntriesTable.tsx`: add selection and action button behavior matching BRREG style.

## Task 1: Shared Translation Types

**Files:**
- Create: `scheduler/internal/sourcetranslation/types.go`
- Create: `scheduler/internal/sourcetranslation/text.go`
- Create: `scheduler/internal/sourcetranslation/text_test.go`

- [ ] **Step 1: Write text normalization tests**

Create `scheduler/internal/sourcetranslation/text_test.go`:

```go
package sourcetranslation

import "testing"

func TestTermKeyNormalizesTrimAndCase(t *testing.T) {
	first := TermKey("  Aktsiaselts  ")
	second := TermKey("aktsiaselts")
	if first != second {
		t.Fatalf("expected normalized keys to match: %s != %s", first, second)
	}
	if len(first) != 64 {
		t.Fatalf("expected sha256 hex key length 64, got %d", len(first))
	}
}

func TestNormalizeTextTrimsLowercasesAndKeepsInternalSpacing(t *testing.T) {
	got := NormalizeText("  OSA  ÜHING  ")
	want := "osa  ühing"
	if got != want {
		t.Fatalf("NormalizeText() = %q, want %q", got, want)
	}
}

func TestTermKeyNormalizesUnicodeComposition(t *testing.T) {
	composed := TermKey("ÜHING")
	decomposed := TermKey("U\u0308HING")
	if composed != decomposed {
		t.Fatalf("expected unicode-normalized keys to match: %s != %s", composed, decomposed)
	}
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
go test ./internal/sourcetranslation -run 'TestTermKey|TestNormalizeText' -count=1
```

Expected: package or symbols are missing.

- [ ] **Step 3: Implement shared types and text helpers**

Create `scheduler/internal/sourcetranslation/text.go`:

```go
package sourcetranslation

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"golang.org/x/text/unicode/norm"
)

func NormalizeText(sourceText string) string {
	return norm.NFC.String(strings.ToLower(strings.TrimSpace(sourceText)))
}

func TermKey(sourceText string) string {
	sum := sha256.Sum256([]byte(NormalizeText(sourceText)))
	return hex.EncodeToString(sum[:])
}
```

Create `scheduler/internal/sourcetranslation/types.go`:

```go
package sourcetranslation

import "context"

type SourceConfig struct {
	Source               string
	SourceLang           string
	TargetLang           string
	DefaultPromptVersion string
}

type LoadMissingFieldsCommand struct {
	PromptVersion string
	CompanyIDs    []string
	Filters       map[string]string
	CompanyLimit  int32
	FieldLimit    int32
}

type MissingField struct {
	CompanyID            string
	SourceTable          string
	SourceRowID          string
	SourceColumn         string
	TargetColumn         string
	SourceText           string
	SourceTextNormalized string
	TermKey              string
	Priority             int32
}

type LoadCachedTermsCommand struct {
	PromptVersion string
	TermKeys      []string
}

type CachedTerm struct {
	TermKey        string
	TranslatedText string
}

type TranslationTerm struct {
	TermKey              string
	SourceText           string
	SourceTextNormalized string
}

type TranslationTermResult struct {
	TermKey              string
	SourceText           string
	SourceTextNormalized string
	TranslatedText       string
	Status               string
	Provider             string
	Model                string
	PromptVersion        string
	Error                string
	ErrorCode            string
	Metadata             map[string]any
}

type SaveTermsCommand struct {
	PromptVersion string
	Terms         []TranslationTermResult
}

type SaveTermsResult struct {
	TermsSaved int32
}

type TranslationBinding struct {
	ID             int64
	CompanyID      string
	SourceTable    string
	SourceRowID    string
	SourceColumn   string
	TargetColumn   string
	TranslatedText string
}

type ApplyCompanyTranslationsCommand struct {
	CompanyID string
	Bindings  []TranslationBinding
}

type ApplyCompanyTranslationsResult struct {
	BindingsApplied  int32
	AppliedBindingIDs []int64
}

type SourceStore interface {
	LoadMissingTranslationFields(context.Context, LoadMissingFieldsCommand) ([]MissingField, error)
	LoadCachedTranslationTerms(context.Context, LoadCachedTermsCommand) (map[string]CachedTerm, error)
	SaveTranslationTerms(context.Context, SaveTermsCommand) (SaveTermsResult, error)
	ApplyCompanyTranslations(context.Context, ApplyCompanyTranslationsCommand) (ApplyCompanyTranslationsResult, error)
	RefreshTranslationStatus(context.Context) error
}
```

- [ ] **Step 4: Run the tests**

Run:

```bash
go test ./internal/sourcetranslation -run 'TestTermKey|TestNormalizeText' -count=1
```

Expected: pass.

## Task 2: Shared SQLite Workset

**Files:**
- Create: `scheduler/internal/sourcetranslation/workset.go`
- Create: `scheduler/internal/sourcetranslation/workset_test.go`
- Modify: `scheduler/internal/brreg/companydata/workset.go`

- [ ] **Step 1: Add source-neutral workset tests**

Create tests covering:

```go
func TestBuildTranslationWorksetWritesCachedAndPendingTerms(t *testing.T)
func TestClaimTranslationWorksetBatchPacksTermsByBudget(t *testing.T)
func TestSaveTranslationWorksetBatchUpdatesBindings(t *testing.T)
func TestApplyTranslationWorksetGroupsBindingsByCompany(t *testing.T)
```

Use a fake `SourceStore` in the test file with in-memory slices for missing fields, cached terms, saved terms, and applied company commands. Use `t.TempDir()` for SQLite paths.

- [ ] **Step 2: Run the failing tests**

Run:

```bash
go test ./internal/sourcetranslation -run 'TestBuildTranslationWorkset|TestClaimTranslationWorkset|TestSaveTranslationWorkset|TestApplyTranslationWorkset' -count=1
```

Expected: missing workset symbols.

- [ ] **Step 3: Move BRREG's SQLite schema and batch logic into shared package**

Implement these exported functions in `scheduler/internal/sourcetranslation/workset.go`:

```go
type BuildWorksetCommand struct {
	Path          string
	PromptVersion string
	CompanyIDs    []string
	Filters       map[string]string
	CompanyLimit  int32
	FieldLimit    int32
}

type BuildWorksetResult struct {
	Path              string
	FieldsExported    int32
	TermsExported     int32
	CompaniesExported int32
	CachedFields      int32
}

type ClaimBatchCommand struct {
	Path            string
	MaxRequestChars int32
	MaxTerms        int32
	MaxAttempts     int32
}

type ClaimBatchResult struct {
	Status         string
	BatchID        int64
	Terms          []TranslationTerm
	EstimatedChars int32
}

type SaveBatchCommand struct {
	Path          string
	BatchID       int64
	Provider      string
	Model         string
	PromptVersion string
	Results       []TranslationTermResult
}

type SaveBatchResult struct {
	TermsSucceeded int32
	TermsFailed    int32
}

type ApplyWorksetCommand struct {
	Path          string
	PromptVersion string
}

type ApplyWorksetResult struct {
	TermsSaved      int32
	BindingsApplied int32
}

func BuildWorkset(ctx context.Context, store SourceStore, config SourceConfig, command BuildWorksetCommand) (BuildWorksetResult, error)
func ClaimBatch(ctx context.Context, command ClaimBatchCommand) (ClaimBatchResult, error)
func SaveBatch(ctx context.Context, command SaveBatchCommand) (SaveBatchResult, error)
func ApplyWorkset(ctx context.Context, store SourceStore, command ApplyWorksetCommand) (ApplyWorksetResult, error)
```

The implementation should preserve BRREG behavior from `scheduler/internal/brreg/companydata/workset.go`: same SQLite table names, same statuses, same claim packing logic, same retry handling, same cached-binding behavior.

- [ ] **Step 4: Run shared workset tests**

Run:

```bash
go test ./internal/sourcetranslation -count=1
```

Expected: pass.

## Task 3: Adapt BRREG Companydata API

**Files:**
- Modify: `scheduler/internal/brreg/companydata/workset.go`
- Modify: `scheduler/internal/brreg/companydata/store.go`
- Modify: `scheduler/internal/brreg/companydata/workset_test.go`
- Modify: `scheduler/internal/brreg/companydata/companydata_test.go`

- [ ] **Step 1: Add adapter tests for BRREG source API**

Add tests that assert:

```go
func TestStoreLoadMissingTranslationFieldsUsesBRREGFiltersAndNormalizedKeys(t *testing.T)
func TestStoreLoadCachedTranslationTermsReturnsSucceededBRREGTerms(t *testing.T)
func TestStoreApplyCompanyTranslationsRejectsUnsupportedBRREGTarget(t *testing.T)
```

Use existing BRREG test helpers and fixtures from `companydata_test.go` and `workset_test.go`.

- [ ] **Step 2: Run BRREG companydata tests and confirm failures**

Run:

```bash
go test ./internal/brreg/companydata -run 'LoadMissingTranslationFields|LoadCachedTranslationTerms|ApplyCompanyTranslations' -count=1
```

Expected: new methods are missing.

- [ ] **Step 3: Implement BRREG source translation API**

In `scheduler/internal/brreg/companydata/workset.go`, replace source-neutral workset functions with thin wrappers:

```go
func (s *Store) BuildTranslationWorkset(ctx context.Context, command BuildTranslationWorksetCommand) (BuildTranslationWorksetResult, error) {
	return sourcetranslation.BuildWorkset(ctx, s, brregTranslationConfig(), sourcetranslation.BuildWorksetCommand{
		Path:          command.Path,
		PromptVersion: command.PromptVersion,
		CompanyIDs:    command.IDs,
		Filters:       command.Filters,
		CompanyLimit:  command.CompanyLimit,
		FieldLimit:    command.FieldLimit,
	})
}

func ClaimTranslationWorksetBatch(ctx context.Context, command ClaimTranslationWorksetBatchCommand) (ClaimTranslationWorksetBatchResult, error) {
	return sourcetranslation.ClaimBatch(ctx, sourcetranslation.ClaimBatchCommand(command))
}

func SaveTranslationWorksetBatch(ctx context.Context, command SaveTranslationWorksetBatchCommand) (SaveTranslationWorksetBatchResult, error) {
	return sourcetranslation.SaveBatch(ctx, sourcetranslation.SaveBatchCommand(command))
}

func (s *Store) ApplyTranslationWorkset(ctx context.Context, command ApplyTranslationWorksetCommand) (ApplyTranslationWorksetResult, error) {
	return sourcetranslation.ApplyWorkset(ctx, s, sourcetranslation.ApplyWorksetCommand(command))
}
```

Add BRREG source methods:

```go
func (s *Store) LoadMissingTranslationFields(ctx context.Context, command sourcetranslation.LoadMissingFieldsCommand) ([]sourcetranslation.MissingField, error)
func (s *Store) LoadCachedTranslationTerms(ctx context.Context, command sourcetranslation.LoadCachedTermsCommand) (map[string]sourcetranslation.CachedTerm, error)
func (s *Store) SaveTranslationTerms(ctx context.Context, command sourcetranslation.SaveTermsCommand) (sourcetranslation.SaveTermsResult, error)
func (s *Store) ApplyCompanyTranslations(ctx context.Context, command sourcetranslation.ApplyCompanyTranslationsCommand) (sourcetranslation.ApplyCompanyTranslationsResult, error)
func (s *Store) RefreshTranslationStatus(ctx context.Context) error
```

Move the existing `loadTranslationWorksetRows`, `applyTranslationWorksetBinding`, and `SaveTranslationTerms` behavior behind these methods.

- [ ] **Step 4: Run all BRREG companydata tests**

Run:

```bash
go test ./internal/brreg/companydata -count=1
```

Expected: pass with no behavior changes.

## Task 4: Generic Term Translation Client

**Files:**
- Modify: `scheduler/internal/translationclient/client.go`
- Modify: `scheduler/internal/translationclient/client_test.go`

- [ ] **Step 1: Add client tests**

Add:

```go
func TestTranslateTermsRequestsDefaultTermSubjectAndDecodesResponse(t *testing.T)
func TestTranslateBrregTermsDelegatesToTranslateTerms(t *testing.T)
```

The tests should assert that the NATS subject remains `brreg.translation.terms.request` when the client was created with the default record translation subject.

- [ ] **Step 2: Run failing tests**

Run:

```bash
go test ./internal/translationclient -run 'TestTranslateTerms|TestTranslateBrregTermsDelegates' -count=1
```

Expected: `TranslateTerms` is missing.

- [ ] **Step 3: Implement `TranslateTerms`**

Rename the body of `TranslateBrregTerms` to:

```go
func (c *Client) TranslateTerms(ctx context.Context, request TermTranslationRequest) (TermTranslationResult, error)
```

Then make:

```go
func (c *Client) TranslateBrregTerms(ctx context.Context, request TermTranslationRequest) (TermTranslationResult, error) {
	return c.TranslateTerms(ctx, request)
}
```

Update slog messages from `brreg term translation` to `term translation`, while keeping error wrapping clear and not logging source text.

- [ ] **Step 4: Run client tests**

Run:

```bash
go test ./internal/translationclient -count=1
```

Expected: pass.

## Task 5: Ariregister Translation Cache Schema

**Files:**
- Create: `database/migrations/000097_ariregister_source_translation_terms.up.sql`
- Create: `database/migrations/000097_ariregister_source_translation_terms.down.sql`
- Modify: `scheduler/internal/db/ariregister_source_profile_migration_test.go`
- Modify: `database/queries/ariregister_source_profile.sql`

- [ ] **Step 1: Add migration shape tests**

Extend Ariregister migration tests to assert:

```go
require.Contains(t, sql, "CREATE TABLE ariregister_source.translation_terms")
require.Contains(t, sql, "CONSTRAINT chk_ariregister_source_translation_terms_source CHECK (source = 'ariregister')")
require.Contains(t, sql, "CREATE MATERIALIZED VIEW ariregister_source.mv_company_translation_status AS")
require.Contains(t, sql, "FROM ariregister_source.v_missing_translations")
```

- [ ] **Step 2: Run failing migration tests**

Run:

```bash
go test ./internal/db -run AriregisterSourceProfileMigration -count=1
```

Expected: migration assertions fail.

- [ ] **Step 3: Add migration**

Create `000097_ariregister_source_translation_terms.up.sql` with the BRREG cache table shape, replacing schema/source names with Ariregister values:

```sql
CREATE TABLE ariregister_source.translation_terms (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source text NOT NULL DEFAULT 'ariregister',
  source_lang text NOT NULL,
  target_lang text NOT NULL,
  source_text_normalized text NOT NULL,
  source_text text NOT NULL,
  term_key text NOT NULL,
  translated_text text,
  status text NOT NULL,
  attempt_count integer NOT NULL DEFAULT 0,
  provider text,
  model text,
  prompt_version text NOT NULL DEFAULT 'v1',
  error text,
  error_code text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  translated_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_translation_terms_source CHECK (source = 'ariregister'),
  CONSTRAINT chk_ariregister_source_translation_terms_status CHECK (status IN ('pending', 'queued', 'succeeded', 'failed_retryable', 'failed_terminal')),
  CONSTRAINT chk_ariregister_source_translation_terms_attempt_count CHECK (attempt_count >= 0),
  CONSTRAINT chk_ariregister_source_translation_terms_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  CONSTRAINT chk_ariregister_source_translation_terms_source_text CHECK (btrim(source_text) <> ''),
  CONSTRAINT chk_ariregister_source_translation_terms_normalized CHECK (btrim(source_text_normalized) <> ''),
  CONSTRAINT chk_ariregister_source_translation_terms_key CHECK (term_key ~ '^[0-9a-f]{64}$')
);

CREATE UNIQUE INDEX uq_ariregister_source_translation_terms_key
  ON ariregister_source.translation_terms(source, source_lang, target_lang, prompt_version, term_key);

CREATE INDEX idx_ariregister_source_translation_terms_status
  ON ariregister_source.translation_terms(status, updated_at);

CREATE INDEX idx_ariregister_source_translation_terms_lookup
  ON ariregister_source.translation_terms(source_lang, target_lang, prompt_version, source_text_normalized);
```

Add `mv_company_translation_status` using the BRREG shape and Ariregister fields `registry_code`, `legal_name`, `lifecycle_status`, and `registration_status`.

- [ ] **Step 4: Add down migration**

Create `000097_ariregister_source_translation_terms.down.sql`:

```sql
DROP MATERIALIZED VIEW IF EXISTS ariregister_source.mv_company_translation_status;
DROP TABLE IF EXISTS ariregister_source.translation_terms;
```

- [ ] **Step 5: Add sqlc queries**

Add to `database/queries/ariregister_source_profile.sql`:

```sql
-- name: RefreshAriregisterSourceCompanyTranslationStatus :exec
REFRESH MATERIALIZED VIEW ariregister_source.mv_company_translation_status;

-- name: UpsertAriregisterTranslationTermResult :exec
INSERT INTO ariregister_source.translation_terms (
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
  'ariregister',
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
    metadata = ariregister_source.translation_terms.metadata || EXCLUDED.metadata,
    translated_at = CASE WHEN EXCLUDED.status = 'succeeded' THEN now() ELSE ariregister_source.translation_terms.translated_at END,
    updated_at = now();
```

- [ ] **Step 6: Regenerate sqlc and test**

Run the repo's sqlc generation command used in this project, then:

```bash
go test ./internal/db -run AriregisterSourceProfileMigration -count=1
```

Expected: pass.

## Task 6: Ariregister Companydata Store

**Files:**
- Create: `scheduler/internal/ariregister/companydata/store.go`
- Create: `scheduler/internal/ariregister/companydata/translation.go`
- Create: `scheduler/internal/ariregister/companydata/workset_test.go`
- Create: `scheduler/internal/ariregister/db/term_translation.go`
- Modify: `scheduler/internal/ariregister/db/types.go`

- [ ] **Step 1: Add Ariregister companydata tests**

Create tests:

```go
func TestStoreLoadMissingTranslationFieldsUsesAriregisterFilters(t *testing.T)
func TestStoreLoadCachedTranslationTermsReturnsSucceededAriregisterTerms(t *testing.T)
func TestStoreApplyCompanyTranslationsUpdatesSupportedColumns(t *testing.T)
func TestStoreApplyCompanyTranslationsRejectsUnsupportedTargetColumn(t *testing.T)
```

Use source profile fixture data that creates one active Ariregister company with missing `registration_status_label_en`, `legal_form_label_en`, and address `country_label_en`.

- [ ] **Step 2: Run failing tests**

Run:

```bash
go test ./internal/ariregister/companydata -count=1
```

Expected: package or methods are missing.

- [ ] **Step 3: Implement store constructor**

Create `scheduler/internal/ariregister/companydata/store.go`:

```go
package companydata

import (
	"context"

	"github.com/cockroachdb/errors"

	ariregisterdb "github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
)

const (
	defaultPromptVersion = "v1"
	sourceName           = "ariregister"
	sourceLang           = "et"
	targetLang           = "en"
)

type Store struct {
	pool    ariregisterdb.TxPool
	gateway *ariregisterdb.Gateway
}

func New(pool ariregisterdb.TxPool) *Store {
	return &Store{pool: pool, gateway: ariregisterdb.New(pool)}
}

func translationConfig() sourcetranslation.SourceConfig {
	return sourcetranslation.SourceConfig{
		Source:        sourceName,
		SourceLang:    sourceLang,
		TargetLang:    targetLang,
		DefaultPromptVersion: defaultPromptVersion,
	}
}

func (s *Store) RefreshTranslationStatus(ctx context.Context) error {
	if s == nil || s.gateway == nil {
		return errors.New("ariregister companydata store not available")
	}
	return s.gateway.RefreshCompanyTranslationStatus(ctx)
}
```

- [ ] **Step 4: Implement missing/cache/apply methods**

In `translation.go`, implement:

```go
func (s *Store) LoadMissingTranslationFields(ctx context.Context, command sourcetranslation.LoadMissingFieldsCommand) ([]sourcetranslation.MissingField, error)
func (s *Store) LoadCachedTranslationTerms(ctx context.Context, command sourcetranslation.LoadCachedTermsCommand) (map[string]sourcetranslation.CachedTerm, error)
func (s *Store) SaveTranslationTerms(ctx context.Context, command sourcetranslation.SaveTermsCommand) (sourcetranslation.SaveTermsResult, error)
func (s *Store) ApplyCompanyTranslations(ctx context.Context, command sourcetranslation.ApplyCompanyTranslationsCommand) (sourcetranslation.ApplyCompanyTranslationsResult, error)
```

`LoadMissingTranslationFields` should select from `ariregister_source.mv_company_translation_status`, join `ariregister_source.v_missing_translations`, and compute normalized text and term key in SQL with `lower(btrim(...))` and `digest(...)`.

`ApplyCompanyTranslations` should group only by company command and update supported target columns in:

- `ariregister_source.companies`
- `ariregister_source.company_statuses`
- `ariregister_source.legal_forms`
- `ariregister_source.addresses`
- `ariregister_source.contacts`
- `ariregister_source.industries`
- `ariregister_source.capital`
- `ariregister_source.annual_reports`
- `ariregister_source.articles`
- `ariregister_source.registry_notes`

Every update should preserve existing non-blank translations:

```sql
SET target_column_en = COALESCE(NULLIF(btrim(target_column_en), ''), $2), updated_at = now()
```

- [ ] **Step 5: Run companydata tests**

Run:

```bash
go test ./internal/ariregister/companydata -count=1
```

Expected: pass.

## Task 7: Ariregister Translation Actions and Workflow

**Files:**
- Create: `scheduler/internal/ariregister/actions/company_translation_actions.go`
- Create: `scheduler/internal/ariregister/actions/company_translation_workset_actions.go`
- Create: `scheduler/internal/ariregister/workflow/company_translation.go`
- Create: `scheduler/internal/ariregister/workflow/company_translation_test.go`

- [ ] **Step 1: Add workflow tests**

Mirror the BRREG workflow tests for:

```go
func TestTranslateAriregisterSourceCompaniesCachedWorksetCompletes(t *testing.T)
func TestTranslateAriregisterSourceCompaniesPipelinesNextBatchBeforeSavingCurrent(t *testing.T)
func TestTranslateAriregisterSourceCompaniesContinuesAsNewForAllRecords(t *testing.T)
func TestTranslateAriregisterSourceCompaniesFailsWhenAllTermsFail(t *testing.T)
func TestTranslateAriregisterSourceCompaniesDrainsWhenNothingClaimed(t *testing.T)
```

- [ ] **Step 2: Run failing workflow tests**

Run:

```bash
go test ./internal/ariregister/workflow -run TranslateAriregisterSourceCompanies -count=1
```

Expected: workflow symbols are missing.

- [ ] **Step 3: Implement Ariregister action resource**

Create `company_translation_actions.go`:

```go
package actions

import (
	"github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/companydata"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

const defaultTranslationPromptVersion = "v1"

type CompanyTranslationActions struct {
	store      *companydata.Store
	translator *translationclient.Client
}

func NewCompanyTranslationActions(store *companydata.Store, translator *translationclient.Client) *CompanyTranslationActions {
	return &CompanyTranslationActions{store: store, translator: translator}
}
```

- [ ] **Step 4: Implement workset activities**

Use the same activity shape as BRREG with Ariregister names. The translate activity must build:

```go
translationclient.TermTranslationRequest{
	RequestID:     uuid.NewString(),
	Source:        "ariregister",
	SourceLang:    "et",
	TargetLang:    "en",
	Provider:      input.Provider,
	Model:         input.Model,
	PromptVersion: input.PromptVersion,
	Terms:         terms,
}
```

Call `a.translator.TranslateTerms(ctx, request)`.

- [ ] **Step 5: Implement workflow**

Create constants:

```go
const (
	TranslateAriregisterSourceCompaniesTaskQueue    = "ariregister-company-translation"
	TranslateAriregisterSourceCompaniesWorkflowName = "TranslateAriregisterSourceCompanies"
)
```

Use BRREG workflow behavior and Ariregister activity names:

```go
BuildAriregisterTranslationWorkset
ClaimAriregisterTranslationWorksetBatch
TranslateAriregisterTranslationWorksetBatch
SaveAriregisterTranslationWorksetBatch
ApplyAriregisterTranslationWorkset
```

Default workset path:

```go
filepath.Join("/var/lib/corpscout/worksets", "ariregister-translation-"+workflowID+".sqlite")
```

- [ ] **Step 6: Run workflow tests**

Run:

```bash
go test ./internal/ariregister/workflow -count=1
```

Expected: pass.

## Task 8: Temporal App and HTTP Trigger

**Files:**
- Create: `scheduler/internal/app/ariregister_company_translation_temporal.go`
- Modify: `scheduler/internal/app/temporal.go`
- Modify: `scheduler/internal/httpapi/workflow_triggers.go`
- Modify: `scheduler/internal/httpapi/handlers.go`
- Modify: `scheduler/internal/httpapi/workflow_triggers_test.go`

- [ ] **Step 1: Add HTTP trigger tests**

Add tests that submit:

```json
{
  "all_records": true,
  "max_request_chars": 12000,
  "max_companies_per_batch": 500,
  "max_attempts": 3,
  "trigger": "manual"
}
```

Assert the Temporal workflow name is `TranslateAriregisterSourceCompanies` and task queue is `ariregister-company-translation`.

- [ ] **Step 2: Run failing HTTP tests**

Run:

```bash
go test ./internal/httpapi -run Ariregister.*Translation -count=1
```

Expected: route/handler missing.

- [ ] **Step 3: Wire Temporal worker**

Add resource field:

```go
ariregisterCompanyTranslation *ariregisteractions.CompanyTranslationActions
```

Construct it with:

```go
ariregisterCompanyData := ariregistercompanydata.New(pool)
ariregisterCompanyTranslation: ariregisteractions.NewCompanyTranslationActions(ariregisterCompanyData, translator)
```

Register worker activities directly with `RegisterActivityWithOptions`.

- [ ] **Step 4: Add HTTP handler and route**

Add:

```go
r.Post("/workflows/ariregister/company-translation", h.handleStartAriregisterCompanyTranslationWorkflow)
```

Use the same request fields and validation as BRREG, but start `ariregisterworkflow.TranslateAriregisterSourceCompanies`.

- [ ] **Step 5: Run app and HTTP tests**

Run:

```bash
go test ./internal/app -run Temporal -count=1
go test ./internal/httpapi -run 'Ariregister.*Translation|StartAriregister' -count=1
```

Expected: pass.

## Task 9: Ariregister UI Action

**Files:**
- Modify: `ui/app/types/api.ts`
- Modify: `ui/app/lib/api.ts`
- Create: `ui/app/components/app/AriregisterTranslationActionForm.tsx`
- Create: `ui/app/components/app/AriregisterSourceEntryActionSheet.tsx`
- Modify: `ui/app/components/app/AriregisterSourceEntriesTable.tsx`

- [ ] **Step 1: Add API type and call**

Add:

```ts
export type AriregisterCompanyTranslationRequest = BrregCompanyTranslationRequest;
```

Add API method:

```ts
async translateAriregisterSourceCompanies(body: AriregisterCompanyTranslationRequest) {
  return this.post<WorkflowStartResponse>(
    "/api/v1/workflows/ariregister/company-translation",
    body,
  );
}
```

- [ ] **Step 2: Add Ariregister form**

Create `AriregisterTranslationActionForm.tsx` by using the BRREG form structure with:

- description: `Starts the Temporal workflow that exports missing Ariregister source translations to a local workset, translates uncached Estonian terms, and writes English values back to ariregister_source.`
- API call: `api.translateAriregisterSourceCompanies(body)`
- success toast: `Ariregister company translation workflow started.`
- failure toast: `Failed to start Ariregister translation.`

- [ ] **Step 3: Add action sheet**

Create `AriregisterSourceEntryActionSheet.tsx` with one action:

```ts
const actions = [
  {
    id: "translation",
    label: "Translation",
    description: "Translate missing source values to English.",
  },
];
```

Render `AriregisterTranslationActionForm` when selected.

- [ ] **Step 4: Add table selection and action button**

Update `AriregisterSourceEntriesTable.tsx` to match BRREG table behavior:

- row checkboxes
- selected ID tracking
- all-filtered vs selected-row action scope
- action button opening the Ariregister action sheet
- pass current filters into the action form

- [ ] **Step 5: Run UI verification**

Run:

```bash
pnpm typecheck
```

Expected: pass.

## Task 10: End-to-End Verification

**Files:**
- No new files.

- [ ] **Step 1: Run focused Go tests**

Run:

```bash
go test ./internal/sourcetranslation -count=1
go test ./internal/translationclient -count=1
go test ./internal/brreg/companydata -count=1
go test ./internal/ariregister/companydata -count=1
go test ./internal/ariregister/workflow -count=1
go test ./internal/httpapi -run 'Ariregister.*Translation|StartAriregister' -count=1
```

Expected: pass.

- [ ] **Step 2: Run UI typecheck**

Run:

```bash
pnpm typecheck
```

Expected: pass.

- [ ] **Step 3: Browser smoke**

Open:

```text
http://localhost:8094/sources/ariregister/source_entries
```

Verify:

- action button is visible
- translation action form opens
- submitting all-records translation returns a workflow started toast
- API request body uses `all_records: true` and route `/api/v1/workflows/ariregister/company-translation`

- [ ] **Step 4: Database smoke**

After one successful test translation, query:

```sql
SELECT count(*) FROM ariregister_source.translation_terms WHERE status = 'succeeded';
SELECT count(*) FROM ariregister_source.v_missing_translations;
```

Expected: translation terms increase and missing translation count decreases for applied companies.

## Self-Review

- Spec coverage: shared core, identical companydata API, Ariregister cache schema, Ariregister workflow/action/UI, and BRREG preservation are all covered by tasks.
- Placeholder scan: no placeholder implementation steps are left open; source-specific SQL is constrained to concrete files and methods.
- Type consistency: shared API names are consistent across spec and plan.
