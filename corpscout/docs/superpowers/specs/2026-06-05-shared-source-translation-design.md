# Shared Source Translation Design

## Goal

Build Ariregister source translation with the BRREG SQLite workset and cache behavior, while introducing a shared translation core that can be reused by future sources without hiding source-specific data rules.

## Current Context

BRREG is the only source with a complete translation workflow. Its working path is:

1. UI starts a Temporal workflow.
2. The workflow builds a bounded SQLite workset from missing source fields.
3. The workset preloads already translated persistent cache terms.
4. Only uncached deduped terms are sent to the NATS translation service.
5. Results are saved in SQLite, persisted to `brreg_source.translation_terms`, and applied into `_en` columns.

Ariregister already has source profile tables, `_en` columns, and `ariregister_source.v_missing_translations`, but it does not yet have a persistent translation-term cache, a BRREG-style translation status materialized view, or a translation action/workflow.

## Architecture

Use a shared `sourcetranslation` package for translation mechanics and keep source rules in each source's `companydata` package.

The shared package owns:

- SQLite workset schema and lifecycle.
- Term normalization and key generation.
- Batch claim and save behavior.
- Generic NATS term translation request handling.
- Workflow input/result types and workflow execution shape where practical.

Each source `companydata.Store` owns:

- Loading missing translation fields for selected companies.
- Reading and writing that source's persistent translation cache.
- Applying translated bindings into supported `_en` columns.
- Refreshing source-specific translation status materialized views.

This makes the source-specific API identical across sources without moving dynamic table and column update logic into the shared package.

## Source Companydata API

Each source store should expose the same concrete methods:

```go
LoadMissingTranslationFields(ctx context.Context, command sourcetranslation.LoadMissingFieldsCommand) ([]sourcetranslation.MissingField, error)
LoadCachedTranslationTerms(ctx context.Context, command sourcetranslation.LoadCachedTermsCommand) (map[string]sourcetranslation.CachedTerm, error)
SaveTranslationTerms(ctx context.Context, command sourcetranslation.SaveTermsCommand) (sourcetranslation.SaveTermsResult, error)
ApplyCompanyTranslations(ctx context.Context, command sourcetranslation.ApplyCompanyTranslationsCommand) (sourcetranslation.ApplyCompanyTranslationsResult, error)
RefreshTranslationStatus(ctx context.Context) error
```

The shared core calls these methods through a source-store interface because there are now multiple real implementations: BRREG and Ariregister. The command uses `CompanyIDs` for selected company IDs and keeps filter semantics source-specific.

`ApplyCompanyTranslationsResult` returns both an applied count and applied SQLite binding IDs so the shared workset can mark only the bindings that the source store actually applied.

## Ariregister Scope

Ariregister translation should use:

- source: `ariregister`
- source language: `et`
- target language: `en`
- workset path prefix: `/var/lib/corpscout/worksets/ariregister-translation-`
- task queue: `ariregister-company-translation`
- workflow name: `TranslateAriregisterSourceCompanies`

The first implementation should apply translations to existing `_en` fields declared in `ariregister_source.v_missing_translations`. Legal names are included because the current schema marks `legal_name_en` as translatable; this can be revised later if legal-name copying is preferred.

## Database

Add `ariregister_source.translation_terms` with the same cache semantics as BRREG:

- unique by `(source, source_lang, target_lang, prompt_version, term_key)`
- source constrained to `ariregister`
- status constrained to `pending`, `queued`, `succeeded`, `failed_retryable`, `failed_terminal`
- normalized text and 64-character SHA-256 term key checks

Add `ariregister_source.mv_company_translation_status` that joins `v_missing_translations` to `translation_terms` using `sha256(lower(btrim(source_text)))`. Existing source-entry queries can continue using `mv_company_explorer`, but asset-state and workflow workset selection should use the richer translation status view.

## Error Handling

Lower-level source stores wrap errors with source context and return them. Temporal actions and HTTP handlers log boundary failures once. The shared core must not expose internal SQL or SQLite paths to external HTTP responses.

## Testing

Required test coverage:

- Shared workset package: SQLite build, cache preload, claim, retry, save, and grouped apply behavior.
- BRREG adapter: existing behavior preserved.
- Ariregister adapter: missing fields loaded, cache terms loaded, terms saved, and bindings applied to supported `_en` columns.
- Workflow: Ariregister mirrors the BRREG success, cached-only, partial failure, and continue-as-new behaviors.
- HTTP/UI: Ariregister translation trigger starts the expected workflow and UI action submits the expected payload.

## Non-Goals

- Do not refactor France, Sweden, CVR, or older experimental implementations.
- Do not introduce fully dynamic SQL updates for arbitrary table and column names.
- Do not change BRREG user-facing behavior except for moving reusable translation mechanics behind the shared boundary.
