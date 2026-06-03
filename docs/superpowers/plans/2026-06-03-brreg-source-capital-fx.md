# BRREG Source Capital FX Conversion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a BRREG source action that converts `brreg_source.capital.original_amount` values to `amount_usd_cents` using locally synced exchange rates.

**Architecture:** FX sync stores provider-native rates in public `exchange_rate_*` tables. The BRREG capital FX workflow is a local Temporal workflow with one SQL-backed activity; it does not call NATS or external services. The source-entry action sheet starts this workflow for selected, filtered, or next eligible source companies.

**Tech Stack:** Go, PostgreSQL/sqlc, Temporal Go SDK, React Router, shadcn-style local components.

---

### Task 1: SQL/sqlc Conversion Query

**Files:**
- Modify: `corpscout/database/queries/brreg_source_profile.sql`
- Regenerate: `corpscout/scheduler/internal/db/gen/*`
- Test: `corpscout/scheduler/internal/brreg/db/source_capital_fx_test.go`

- [ ] Add `ConvertBrregSourceCapitalToUSD` sqlc query that selects eligible capital rows by ids/filters/limit, joins the requested or latest ECB sheet, converts via `amount * usd.rate_per_base / source.rate_per_base`, and updates `amount_usd_cents`, `fx_source`, `fx_rate_date`, and `fx_metadata`.
- [ ] Add gateway command/result methods that call the generated query directly.
- [ ] Test NOK, USD, missing rate, and force reprocess behavior against a real test database.

### Task 2: Workflow, Action, Worker, HTTP Trigger

**Files:**
- Create: `corpscout/scheduler/internal/brreg/actions/source_capital_fx_actions.go`
- Create: `corpscout/scheduler/internal/brreg/workflow/source_capital_fx.go`
- Create: `corpscout/scheduler/internal/app/brreg_source_capital_fx_temporal.go`
- Modify: `corpscout/scheduler/internal/app/temporal.go`
- Modify: `corpscout/scheduler/internal/httpapi/workflow_triggers.go`
- Modify: `corpscout/scheduler/internal/httpapi/handlers.go`

- [ ] Add `ConvertBrregSourceCapitalToUSD` workflow on task queue `brreg-source-capital-fx`.
- [ ] Register workflow and activity directly in app wiring.
- [ ] Add `POST /api/v1/workflows/brreg/source-capital-fx` with request validation.
- [ ] Add workflow trigger tests and workflow tests.

### Task 3: Source Entries UI Action

**Files:**
- Create: `corpscout/ui/app/components/app/BrregSourceCapitalFXActionForm.tsx`
- Modify: `corpscout/ui/app/components/app/BrregSourceEntryActionSheet.tsx`
- Modify: `corpscout/ui/app/lib/api.ts`

- [ ] Add `Convert capital to USD` action with selected/filtered/eligible scope, record limit, optional rate date, and force reprocess.
- [ ] Add API client method for the new workflow endpoint.
- [ ] Verify `pnpm typecheck` and `pnpm build`.
