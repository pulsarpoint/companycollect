# BRREG Temporal Translation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run BRREG translation from Corpscout Temporal using the new `brregdb` gateway and the external translation service.

**Architecture:** Add a translation-service HTTP client, a `brregtemporal` workflow/activity package, and register the workflow in the scheduler server's Temporal worker. The workflow repeatedly asks one activity to claim and process a batch until `brregdb` returns no claimable rows.

**Tech Stack:** Go, Temporal SDK, pgx/sqlc, `brregdb`, `net/http`, `github.com/cockroachdb/errors`.

---

### Task 1: Translation Service Client

**Files:**
- Create: `scheduler/internal/translationclient/client.go`
- Create: `scheduler/internal/translationclient/client_test.go`
- Modify: `scheduler/internal/config/config.go`

- [x] Write failing tests for `POST /v1/translate/brreg-records`, structured errors, and config defaults.
- [x] Implement request/response DTOs and the HTTP client.
- [x] Add `CORPSCOUT_TRANSLATION_SERVICE_URL` and BRREG translation defaults to config.

### Task 2: BRREG Temporal Workflow And Activity

**Files:**
- Create: `scheduler/internal/brregtemporal/translation.go`
- Create: `scheduler/internal/brregtemporal/translation_test.go`

- [x] Write failing tests for batch processing success, service failure marking rows retryable, and workflow drain loop.
- [x] Implement `TranslateBrregRawInputs` workflow and `TranslateNextBrregBatch` activity.
- [x] Map translation-service per-record statuses to `brregdb` artifact/task submissions.

### Task 3: Worker Registration

**Files:**
- Modify: `scheduler/internal/app/app.go`
- Create/modify: `scheduler/internal/app/temporal.go`
- Modify: `scheduler/internal/app/app_test.go`

- [x] Write a failing test that scheduler registers `TranslateBrregRawInputs` on the Corpscout Temporal task queue.
- [x] Start a Temporal worker in the scheduler server and stop it on shutdown.
- [x] Keep the existing `StartTranslation` API contract working.

### Task 4: Verification

**Files:**
- All above.

- [x] Run targeted tests for `translationclient`, `brregtemporal`, and `app`.
- [x] Run `GOWORK=off go test ./...` in `scheduler`.
- [x] Run `git diff --check`.
- [x] Commit on `main`.
