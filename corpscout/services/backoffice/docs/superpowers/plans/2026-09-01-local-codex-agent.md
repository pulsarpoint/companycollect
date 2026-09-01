# Local Codex Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Codex threads runnable from inside the backoffice — create/resume/send/list/history/remove — persisted in the settings SQLite, exposed as resource APIs, with a demo chat subpage at `/admin/settings/llms/local`.

**Architecture:** One server lib (`codex-agent.server.ts`) owns the SDK calls and the SQLite mirror; resource routes and the subpage's actions both call it. Threads always run sandboxed read-only in `data/codex-workspace/`.

**Tech Stack:** `@openai/codex-sdk` ^0.148.0 (drives the local `codex` CLI), `node:sqlite`, React Router resource routes, vitest.

**Spec:** docs/superpowers/specs/2026-09-01-local-codex-agent-design.md — the binding authority; this plan implements it 1:1.

## Global Constraints

- Run from `corpscout/services/backoffice`: `npx vitest run <file>`, `npm run typecheck` clean before each commit.
- Mock `@openai/codex-sdk` in every test via `vi.mock` — tests never spawn the real CLI.
- Thread options are exactly `{ workingDirectory: CODEX_WORKSPACE_DIR, sandboxMode: "read-only", skipGitRepoCheck: true }`; `CODEX_WORKSPACE_DIR` defaults to `join(process.cwd(), "data", "codex-workspace")`, overridable via `BACKOFFICE_CODEX_WORKSPACE_DIR`, `mkdirSync`'d on demand. Verify `data/` is gitignored (it is for `data/settings`); extend `.gitignore` if the new dir isn't covered.
- SDK facts (from dist/index.d.ts): `new Codex()`; `startThread(options)`; `resumeThread(id, options)`; `thread.id` is `string | null` — null until the first `run()` completes; `run(input)` → `{ items, finalResponse, usage }`.
- Same SQLite conventions as `llm-settings.server.ts` (busy_timeout, WAL, `databasePath` defaulting param on every exported function for test isolation).
- Storage schema, service signatures, API shapes: exactly as the spec defines. `CodexAgentError` for operator-readable failures; per-thread in-flight lock via a module-level `Map<string, true>` keyed by threadId (and one `"new"` slot per page for first turns).
- Conventional Commits; commit trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`; stage only touched files.

### Task 1: dependency + storage + service lib (TDD)

**Files:** Modify `package.json` (pnpm add `@openai/codex-sdk`); Create `app/lib/codex-agent.server.ts`; Create `tests/codex-agent.server.test.ts`.

**Interfaces produced:**
```ts
export interface CodexThreadSummary { threadId: string; page: string; title: string; createdAt: string; updatedAt: string; }
export interface CodexMessage { messageId: string; threadId: string; role: "user" | "agent"; content: string; createdAt: string; }
export class CodexAgentError extends Error {}
export function runCodexTurn(input: { page: string; input: string; threadId?: string }, databasePath?: string): Promise<{ threadId: string; response: string }>;
export function listCodexThreads(page: string, databasePath?: string): CodexThreadSummary[];
export function getCodexThreadHistory(threadId: string, databasePath?: string): { thread: CodexThreadSummary; messages: CodexMessage[] } | null;
export function deleteCodexThread(threadId: string, databasePath?: string): void;
```

- [ ] Steps: failing tests first (fake SDK via `vi.mock("@openai/codex-sdk")` — fake `Codex` whose `startThread` returns a thread with `id: null` until `run` resolves, then a canned id/finalResponse; `resumeThread` asserts the id passed): new-thread turn persists thread+2 messages with title from truncated prompt; resume appends 2 messages and bumps `updated_at`; resume of unknown threadId throws `CodexAgentError`; list is page-scoped, newest-updated first; history returns ordered messages; delete cascades; concurrent second send to same thread rejects (start a hanging run, then assert the second call throws). RED → implement → GREEN → typecheck → commit.

### Task 2: resource API routes (TDD)

**Files:** Modify `app/routes.ts` (top-level, outside the admin layout): `admin/api/codex/threads` → `routes/admin-api-codex-threads.ts`; `admin/api/codex/threads/:threadId` → `routes/admin-api-codex-thread.ts`; `admin/api/codex/threads/:threadId/messages` → `routes/admin-api-codex-thread-messages.ts`. Create those three + `tests/admin-api-codex.test.ts` (mock the service lib).

- [ ] Contracts to pin: `GET threads?page=` 400 without page, else `{threads}`; `POST threads` JSON `{page, input}` (both required → 400) → `{threadId, response}`; `GET :threadId` → `{thread, messages}` or 404; `POST :threadId/messages` `{input}` → `{threadId, response}`; `DELETE :threadId` → `{ok:true}`; `CodexAgentError` → 400 `{error}`, unknown errors rethrown. RED → implement → GREEN → typecheck → commit.

### Task 3: subpage + settings-tab link (TDD)

**Files:** Modify `app/routes.ts` (inside admin layout, after `settings/llms`): `route("settings/llms/local", "routes/admin-settings-llms-local.tsx")`. Create that route; Modify `app/components/admin/llm-settings-workspace.tsx` (Local tab = switch + link "Open local codex workspace →" to `/admin/settings/llms/local`); Create/extend SSR tests (`tests/admin-settings-llms-local.test.tsx`, existing `tests/admin-llm-settings.test.tsx`).

- [ ] Subpage per spec §4: loader → `{localCodexEnabled, threads: listCodexThreads(PAGE), history?}` (`?thread=` query selects one; `PAGE = "/admin/settings/llms/local"`); actions by intent: `set_local_codex` (reuse), `new-thread` `{input}` → runCodexTurn no threadId → redirect `?thread=<id>`; `send` `{thread_id, input}`; `remove` `{thread_id}` → redirect bare. Component: switch card; enable-prompt when off; thread list (links `?thread=`), New conversation form, Remove button per thread, history panel (user right/agent left, plain text), input+Send bound to the selected thread. Export a pure `LocalCodexWorkspaceView` for SSR tests (memory-router wrapped). RED → implement → GREEN → typecheck → commit.

### Task 4: live verification (operator machine)

- [ ] With the dev server running and `codex` CLI authenticated: enable the toggle, open `/admin/settings/llms/local`, run one real turn, resume it, remove a thread. Record outcomes; no commit.

## Self-Review
Spec coverage: storage/service (T1), five APIs (T2), subpage+tab link (T3), live smoke (T4) — complete. Names consistent across tasks (`runCodexTurn`/`listCodexThreads`/`getCodexThreadHistory`/`deleteCodexThread`; `PAGE` constant). No placeholders; mechanical UI details follow sibling-page idioms per Global Constraints.
