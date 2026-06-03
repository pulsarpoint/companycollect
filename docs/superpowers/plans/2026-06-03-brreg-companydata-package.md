# BRREG Companydata Package Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create `internal/brreg/companydata` as the concrete public API for loading, claiming, translating, and saving normalized BRREG source company data.

**Architecture:** Temporal actions call `companydata.Store`. `companydata.Store` hides `brreg_source.*` table shape and delegates low-level status claiming to the existing BRREG DB gateway/sqlc code. `CompanyData` owns graph-level behavior such as collecting missing English terms and applying translations before persistence.

**Tech Stack:** Go, pgx, sqlc-generated DB gateway, Temporal actions, PostgreSQL test database.

---

### Task 1: Companydata Core API

**Files:**
- Create: `corpscout/scheduler/internal/brreg/companydata/companydata_test.go`
- Create: `corpscout/scheduler/internal/brreg/companydata/types.go`
- Create: `corpscout/scheduler/internal/brreg/companydata/translation.go`

- [x] **Step 1: Write failing tests**

Add tests proving `CompanyData.TranslationTerms()` returns unique missing Norwegian terms and `ApplyTranslations()` fills the matching `_en` fields without exposing table/column bindings to callers.

- [x] **Step 2: Run failing tests**

Run: `GOWORK=off go test ./internal/brreg/companydata -count=1`

Expected: FAIL because package/functions do not exist.

- [x] **Step 3: Implement minimal types and translation methods**

Add `CompanyData`, `Company`, `Capital`, `TranslationTerm`, `TermTranslation`, and methods `TranslationTerms`, `ApplyTranslations`, and `TranslationComplete`.

- [x] **Step 4: Run package tests**

Run: `GOWORK=off go test ./internal/brreg/companydata -count=1`

Expected: PASS.

### Task 2: Companydata Store

**Files:**
- Modify: `corpscout/scheduler/internal/brreg/companydata/companydata_test.go`
- Create: `corpscout/scheduler/internal/brreg/companydata/store.go`

- [x] **Step 1: Write failing database tests**

Add tests for `Store.Load`, `Store.Save`, and `Store.ClaimForTranslation` using the existing test database helpers.

- [x] **Step 2: Run failing tests**

Run: `GOWORK=off go test ./internal/brreg/companydata -count=1`

Expected: FAIL because store methods do not exist.

- [x] **Step 3: Implement store methods**

Use pgx transactions and the existing `brregdb.Gateway` status-claim API internally. Do not expose sqlc row structs from the package.

- [x] **Step 4: Run package tests**

Run: `GOWORK=off go test ./internal/brreg/companydata -count=1`

Expected: PASS.

### Task 3: Wire Company Translation Actions

**Files:**
- Modify: `corpscout/scheduler/internal/brreg/actions/company_translation_actions.go`
- Modify: `corpscout/scheduler/internal/app/brreg_company_translation_temporal.go`

- [x] **Step 1: Update actions to use `companydata.Store`**

Replace direct use of `brregdb.Gateway` plus `brreg/translation` helpers with `companydata.Store`.

- [x] **Step 2: Run focused tests**

Run: `GOWORK=off go test ./internal/brreg/actions ./internal/brreg/workflow ./internal/app -count=1`

Expected: PASS.

### Task 4: Full Verification

- [x] **Step 1: Run scheduler tests**

Run: `GOWORK=off go test ./...`

Expected: PASS.
