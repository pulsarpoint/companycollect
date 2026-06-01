# BRREG DB Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the BRREG workflow persistence boundary into Corpscout as a Go `brregdb` module so Temporal can own orchestration without Dagster owning database logic.

**Architecture:** Add a `brreg_workflow` Postgres schema for raw records, task states, task attempts, translation/domain/financial/enhanced artifacts, and live read views. Generate sqlc methods for the write/read models, then wrap transactional task actions in `scheduler/internal/brregdb`.

**Tech Stack:** PostgreSQL migrations, sqlc, Go, pgx, `github.com/cockroachdb/errors`.

---

### Task 1: BRREG Workflow Schema

**Files:**
- Create: `database/migrations/000053_brreg_workflow_store.up.sql`
- Create: `database/migrations/000053_brreg_workflow_store.down.sql`
- Create: `scheduler/internal/db/brreg_workflow_store_migration_test.go`

- [x] Write a failing migration test that requires `brreg_workflow`, not `dagster_brreg`.
- [x] Add the migration with raw records, task state, artifact tables, and live state views.
- [x] Run `GOWORK=off go test ./internal/db -run TestBrregWorkflowStoreMigration -v`.

### Task 2: sqlc Queries

**Files:**
- Create: `database/queries/brreg_workflow.sql`
- Create: `scheduler/internal/db/gen/brreg_workflow_query_shape_test.go`
- Regenerate: `scheduler/internal/db/gen/*.go`

- [x] Write query-shape tests that require claim, submit, retry, and read-view queries.
- [x] Add sqlc queries for ingest, claim, task attempt finish, artifact insert, retry, and asset state reads.
- [x] Run `make sqlc-generate`.

### Task 3: Go Gateway Package

**Files:**
- Create: `scheduler/internal/brregdb/types.go`
- Create: `scheduler/internal/brregdb/gateway.go`
- Create: `scheduler/internal/brregdb/gateway_test.go`

- [x] Write tests for transactional submit success/failure and typed task mapping.
- [x] Add `Gateway` methods that hide transactions and task-state updates from callers.
- [x] Run `GOWORK=off go test ./internal/brregdb -v`.

### Task 4: Verification

**Files:**
- Regenerated sqlc files as needed.

- [x] Run `GOWORK=off go test ./...` in `scheduler`.
- [x] Run `git diff --check`.
- [x] Commit the Corpscout changes on `main`.
