# Local Codex Agent — Design Spec

Approved in chat 2026-09-01 (owner). Step 2 of the local-LLM track: step 1 (settings
toggle + ESEF picker entry, commits 74851c10/459d3a3c) is live; step 3 (an
OpenAI-compatible endpoint for the dagster enrichment worker, built on these thread
APIs) is explicitly out of scope here.

## Purpose

Run OpenAI Codex threads from inside the backoffice, triggered locally: the
`@openai/codex-sdk` drives the operator's locally installed, ChatGPT-authenticated
`codex` CLI on the machine that runs the backoffice dev server. Threads are
persistent conversations: created on demand, resumable, listable per originating
page, removable, with history.

## Decisions (owner-approved)

- **Sandbox posture:** every thread runs with `workingDirectory` pointed at a
  dedicated scratch dir (`data/codex-workspace/`, gitignored, created on demand),
  `sandboxMode: "read-only"`, `skipGitRepoCheck: true`. The agent cannot modify the
  repo or machine; pure chat/text use works fully.
- **New-thread default:** a turn without a chosen `threadId` always starts a fresh
  thread (`codex.startThread(...)`); a turn with a `threadId` resumes it
  (`codex.resumeThread(id, ...)`) only after the id is verified against our store.
- **History source:** our own SQLite mirror, not the SDK (which resumes by id but
  exposes no history read-back). Every turn records the user input and the agent's
  `finalResponse`.
- **Synchronous turns:** `thread.run(input)` runs inside the HTTP request with a
  generous timeout; a per-thread in-flight lock (in-memory Map) rejects a second
  concurrent send to the same thread with a clear error. No streaming in this step.
- **Removal is hard delete** of the thread row and its messages (the codex CLI's own
  session files on disk are untouched).

## Components

### 1. Storage (`app/lib/codex-agent.server.ts`, same `settings.sqlite`)

Tables (created idempotently, same pattern as `llm_profile`):

```sql
CREATE TABLE IF NOT EXISTS codex_thread (
  thread_id  TEXT PRIMARY KEY,
  page       TEXT NOT NULL CHECK (trim(page) != ''),
  title      TEXT NOT NULL,          -- first prompt, truncated to 120 chars
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS codex_message (
  message_id TEXT PRIMARY KEY,
  thread_id  TEXT NOT NULL REFERENCES codex_thread(thread_id) ON DELETE CASCADE,
  role       TEXT NOT NULL CHECK (role IN ('user', 'agent')),
  content    TEXT NOT NULL,
  created_at TEXT NOT NULL
);
```

### 2. Service functions (same module)

- `runCodexTurn({ page, input, threadId? }) → { threadId, response }` — start-or-
  resume, run, persist thread row (on first turn) + both messages, bump
  `updated_at`. SDK note: `thread.id` is null until the first run completes.
- `listCodexThreads(page) → CodexThreadSummary[]` (newest-updated first).
- `getCodexThreadHistory(threadId) → { thread, messages[] } | null`.
- `deleteCodexThread(threadId)`.
- Errors surface as a typed `CodexAgentError` with an operator-readable message
  (codex CLI missing/unauthenticated included).

### 3. Resource APIs (top-level routes, outside the admin layout chrome)

- `GET  /admin/api/codex/threads?page=…` — threads for a page
- `POST /admin/api/codex/threads` `{page, input}` — new thread + first turn
- `GET  /admin/api/codex/threads/:threadId` — thread + full history
- `POST /admin/api/codex/threads/:threadId/messages` `{input}` — send to thread
- `DELETE /admin/api/codex/threads/:threadId` — remove

JSON in/out; non-2xx carries `{error}`. The demo subpage uses the same service
functions via its own route actions (not fetch) — the APIs exist for programmatic
use and step 3.

### 4. Subpage `/admin/settings/llms/local`

- The settings page's Local tab shrinks to the `local_codex` switch + a link
  "Open local codex workspace →".
- The subpage: switch card at top; when the toggle is OFF the chat area is replaced
  by an enable prompt. When ON: thread list for page `/admin/settings/llms/local`
  (click = resume + show history), "New conversation", per-thread "Remove",
  message history panel, input + Send. Plain form posts; the page revalidates when
  the turn completes.

## Testing

- Storage + service unit tests with `@openai/codex-sdk` mocked (fake Codex whose
  threads echo canned turns; id assigned after first run).
- Resource-route tests (mock service) pinning JSON contracts and error statuses.
- SSR tests for the subpage (toggle-off prompt; thread list; history render).
- Live smoke (manual): one real turn on the operator's machine.

## Out of scope

Streaming output, auth (backoffice is local-only), the OpenAI-compatible
`/v1/chat/completions` bridge for dagster (step 3), and any write-capable sandbox.
