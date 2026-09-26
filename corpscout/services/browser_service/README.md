# Browser service

An HTTP service that creates CloakBrowser processes on demand. A **session** owns
its persistent profile; an **execution** owns a running browser; a **request** owns
one crawl or Brave query. The crawler has no browser runtime dependency.

## Run and configure

Linux headed sessions require Xvfb, xauth, x11vnc, Openbox and Chromium system libraries.

```sh
uv sync
uv run browser-service --env-file .env --max-browsers 6 --session-retention-days 7
```

Defaults: six concurrent browsers, 120 seconds idle timeout, seven days profile
retention. Startup opens **zero browsers**. `--max-browsers` overrides
`BROWSER_MAX_BROWSERS`; `--idle-timeout-seconds` overrides
`BROWSER_IDLE_TIMEOUT_SECONDS`; `--session-retention-days` overrides
`BROWSER_SESSION_RETENTION_DAYS`. Zero capacity pauses new executions.

**Browsers → Settings** stores all three settings in SQLite. Precedence:
**SQLite → CLI → environment → defaults**. Changes apply without restart.
Reducing capacity lets current work finish. Idle-timeout changes take effect at
the next request or heartbeat; retention changes at the next use, close or pin.
`GET /v1/browser/settings` returns `source`, `startup`, `current`, and `capacity`.
`PUT` accepts all three settings; `DELETE` removes the SQLite override.

`--base-profile` / `BROWSER_BASE_PROFILE` points to a closed Chromium user-data
folder. New sessions receive an independent copy; existing sessions are never
reset from the template. The default template is an empty `base-profile/` under
the state directory. Close its browser before copying; active lock files are
rejected. The service never writes browser state into the template.

`BROWSER_API_TOKEN` is required beyond loopback. Use `Authorization: Bearer …`.
Cookies, proxy credentials, profile contents and CDP/VNC ports remain private.

## Session API

Reserve explicitly for a crawl:

```json
POST /v1/browser/sessions
{"requestId":"crawl-001","domain":"example.com","headless":true}
```

The 201 response contains `id` (saved session), `executionId`, `generation`, mode,
route, state and retention deadline. Supply `id` to reopen a saved session or retry
a capacity rejection. IDs use 32 lowercase hexadecimal characters. Capacity
exhaustion returns 503 with `Retry-After: 1` and `detail.sessionId`; the identity is
saved but no browser is launched. There is no service-side queue.

One session can have only one active request. Repeating the same reservation is
idempotent while active; another request gets 409. Every start/stop counts toward
the global maximum until cleanup completes. A closed session can reopen headed or
headless. Its proxy route cannot change.

Navigate using the returned IDs:

```json
POST /v1/browser/extract
{"session":{"id":"<session-id>","executionId":"<execution-id>"},"tab":"site","url":"https://example.com/","browserHtml":true}
```

Alternatively the first extract request can omit `session` to create one, or supply
only a saved `session.id` to reopen it. Optional `headless` and `route` apply at
startup. Subsequent extracts must include the current `executionId`. Site and search
tabs share the same profile. Omitting `url` captures the existing tab. Output includes
HTML, final URL, status, public response headers, redirects and optional screenshot.

Send `X-Browser-Execution-Id` for heartbeat, DELETE and named-tab operations:

- `GET /v1/browser/sessions/{id}`: inspect active or closed identity without launching.
- `POST …/{id}/heartbeat`: extend the active execution's idle deadline.
- `POST …/{id}/tabs/{name}`: `open`, `focus`, `close`, or `recover`.
- `DELETE …/{id}`: save cookies, close the browser, release capacity, retain profile.

A stale execution ID returns 409 and cannot close or alter a newer execution.
Recovery keeps the profile and lease, rotates the browser generation, and does not
confirm verification. Closed tabs are reopened only when explicitly requested.

Crawlers heartbeat every 20 seconds. GET/status reads and desktop connections do
not extend deadlines. In-flight operations hold ownership until complete; browser
API operations are bounded at 310 seconds. Idle expiry closes the browser, but its
saved identity remains reusable. Session retention starts again after use/close.
Expired profiles are deleted only while unowned and unpinned; reopening returns
410. Execution history and metadata remain in SQLite. Retention does not yet remove
Brave/agent diagnostic files.

State layout:

```text
sessions.sqlite3       settings, sessions, executions (plus preserved legacy history)
base-profile/          closed template
sessions/<id>/profile/ persistent Chromium state
sessions/<id>/session.json   private session-cookie snapshot
```

SQLite `BEGIN IMMEDIATE` and a unique active-owner index enforce capacity and profile
exclusivity. A process lock prevents multiple service instances sharing a state
folder. On service restart, unfinished executions become interrupted; saved sessions
stay closed until requested. Systemd owns browser child processes during shutdown.

## Backoffice and management

**Browsers → Saved sessions** lists profiles, active executions, retention and pins.
Create a headed session, reopen a closed session, stop/save a manual browser, or pin
its profile. Active crawl/Brave sessions reject disruptive manual controls.
**Connect** uses Xvfb/noVNC only for running headed sessions. Tickets are single-use,
last 30 seconds and are tied to the live browser generation.

**Browsers → Testing** lets operators edit raw requests, select headed/headless,
inspect HTML/screenshots and close/reopen the same session. It carries execution IDs
between responses and requests; no automatic heartbeats or retries run there.

Management endpoints are authenticated:

- `GET /v1/server`: readiness, desktops, settings, capacity and execution history.
- `GET /v1/browser-sessions`: saved sessions and active tabs.
- `POST /v1/browser-sessions`: create/open a manual session.
- `POST …/{id}/start`: reopen; `{ "headless": false }` enables the desktop.
- `POST …/{id}/stop`: close; supply `{ "executionId": "…" }`.
- `POST …/{id}/settings`: `{ "pinned": true }` prevents profile expiry.
- `POST …/{id}/tabs`: open a manual tab; `POST …/tabs/{tab}/focus`; `GET …/inspect`.
- `GET /healthz`: readiness.

Existing [CAPTCHA assistance](CHALLENGE_AGENT.md) runs on the same active tab.

## Deployment and migration

Ansible deploys the wheel and systemd unit on port 8081. State is
`/var/lib/browser-service`; configuration is `/etc/browser-service/browser-service.env`.
Use the ignored Ansible secrets file. `browser_service_max_browsers`,
`browser_service_idle_timeout_seconds`, `browser_service_session_retention_days`, and
`browser_service_base_profile` set startup values. SQLite still takes precedence.
`-e browser_service_activate=false` prepares a release without activating it.

Deploy the updated browser service, crawler and Backoffice together after draining
active requests. The old fixed-slot clients lack execution IDs. Also update Dagster's
Brave HTTP client to retain one session per route worker. Coordinate any pending
ClickHouse naming migration separately before activating its changed consumers.

Back up the complete state folder while the old service is stopped. First startup:

1. Renames old lease history to `legacy_sessions`, preserving it.
2. Converts saved headless + headed counts into one `max_browsers` override.
3. Copies each existing `profiles/<slot>/profile` and route-specific profile into a
   deterministic saved session, preserving cookies. Imported sessions are pinned and
   labeled with their old slot/route. Originals remain available for rollback.
4. Opens no browsers until requested.

Old lease IDs were request identities and are not reusable saved-session IDs; use
imported session IDs shown in Backoffice. Rolling back requires restoring the state
backup with the older release. Never run old/new processes against the same profiles.
Ansible refreshes CloakBrowser's compatible version at build time and records it in
`uv.lock`; running services have automatic package updates disabled.

## Validation

```sh
uv run python -m unittest discover -s tests -v
COMPANY_RESEARCH_XVFB_TEST=1 BRAVE_NATIVE_TEST=1 uv run python -m unittest discover -s tests -v
```

Native tests use isolated profiles, local pages, intercepted Brave responses and a
fixture model endpoint. They check cookies/localStorage across close/reopen and
headed/headless changes, VNC connectivity, extraction, cancellation, and challenge
handling without contacting real CAPTCHA providers.

## Brave Ask

Dagster submits a query to the browser service; the service owns navigation,
answer completion, Copy, proxy routing, and CAPTCHA assistance. It uses the
same global capacity limit and launches a headless browser by default. Headed requests use the same Xvfb/noVNC access as crawls.

`POST /v1/brave/ask` uses the usual browser API bearer token:

```json
{
  "request_id": "brave-example-001",
  "query": "What is the official website of Novelic?",
  "route": "direct",
  "answer_timeout_seconds": 180,
  "challenge_agent_max_runs": 3,
  "challenge_agent_model": "deepseek-flash"
}
```

Optional `headless: true/false` selects the mode. `session_id` reopens a saved profile;
omitting it creates a new identity. Results include `session_id` and `execution_id`.
With an explicit `session_id`, sequential queries reuse the running browser.
`max_requests_per_browser` defaults to **10**; set it to **20** to recycle after
twenty requests. The counter includes failed searches but excludes cached-result
replays and rejected submissions. The browser closes after the limit; the next
request starts a new process using the same saved profile and cookies. Broken
browsers and cancellations close earlier. Requests without `session_id` close
their browser when finished.

`browser_usage` in live status and saved result JSON records the browser generation,
`requests_started`, and `restart_after`. Session snapshots expose `brave_usage`, and
service logs record each request's count and browser closure. Counters belong to the
live browser process and reset when it restarts. Dagster retains one session per
route worker and explicitly closes it when the worker exits; idle expiry handles
lost clients. Override the service default through the Dagster browser resource's
`max_requests_per_browser` setting. Routes are `direct` and the
configured `crawl_proxy1`, `crawl_proxy2`, `crawl_proxy3`. Set the corresponding
`BROWSER_CRAWL_PROXY1/2/3` environment variables in browser-service, or the
`browser_service_crawl_proxy1/2/3` Ansible secrets. Proxy URLs are never accepted
in requests or returned to clients. Each session belongs to exactly one proxy route. Reusing its ID preserves cookies;
changing its route returns 409. Create another session to use another route.

The service detects visible verification controls during navigation, answer
waiting, and Copy. It invokes the existing screenshot/CDP agent, then checks
the page itself and resumes the exact query. `challenge_agent_max_runs` defaults
to 3; 0 disables assistance. `deepseek-flash` uses `DEEPSEEK`;
`z-ai/glm-5.3-flash` uses `OPENROUTER_API_KEY`. Exhaustion returns
`status: blocked`, `error_stage: captcha`, `error_type: AgentBudgetExhausted`.
An agent's completion claim does not make the query successful: a nonempty
answer must be copied from the requested Ask page.

Backoffice-selected models supply an optional top-level `llm` object with
`provider`, `base_url`, `model`, and `api_key_encrypted`. That profile overrides
the legacy assistant model, endpoint, and environment credential. Provider
reasoning defaults remain enabled. The credential uses the crawler's existing
AES-256-GCM `v1.<nonce>.<ciphertext-and-tag>` envelope and authenticated metadata.
Set `BROWSER_LLM_ENCRYPTION_KEY` to the same 64-character hexadecimal key as
Backoffice's and the crawler's `CRAWLER_LLM_ENCRYPTION_KEY`. Backoffice stores provider
API keys encrypted in its settings database; only encrypted credentials are stored
with browser requests. Dagster transports this profile without decrypting it.
The local `.env.example` documents the browser master-key setting. Legacy `DEEPSEEK`
and `OPENROUTER_API_KEY` variables are still needed for manual/crawler CAPTCHA controls
and Brave requests without `llm`; selected-profile Brave runs do not use those variables.

Before submitting a batch, call authenticated `POST /v1/brave/llm/verify` with
`{"llm": <encrypted-profile>}`. A single completion must inspect a harmless
static image and return a valid browser action JSON. This checks image support
as well as model availability, uses the assistant's actual request settings,
and creates no browser session or evidence files. The check has a 30-second
deadline and returns `{"ok": true}` or `{"ok": false, "error": "..."}`.

The response contains `status` (`success`, `blocked`, `error`), `answer`,
`source_url`, timestamps, duration, error stage/category, and `challenge_runs`.
The page-load budget defaults to 60 seconds, answer generation to 180 seconds,
and the whole request to 900 seconds (maximum 1800). Agent time is excluded from
the answer budget but included in the overall deadline. Each agent invocation
is limited to 12 actions and 120 seconds. Increasing the run limit does not
remove the overall deadline.

Requests and results are retained at `brave-requests/<request_id>/` in the state
directory. CAPTCHA HTML/screenshots are saved there; agent actions and screenshots
remain in `challenge-runs/`. `GET /v1/brave/requests/<request_id>` returns live
progress or the saved result. `/v1/server` also exposes the active operation so
Backoffice's browser assignments show answer generation or CAPTCHA assistance.
These local diagnostics have no automatic retention policy yet.

Authenticated `POST /v1/brave/requests/<request_id>/cancel` cancels only that
request and waits for its browser cleanup. It returns the saved terminal result
with `error_type: Cancelled`, or `status: cancelling` if cleanup takes more than
30 seconds. Completed results stay intact, including cancellation evidence.
Retry a cancelled request with a new request ID; replaying its original ID
returns the saved cancellation result.

Submitting the same request ID and payload returns the saved result. A duplicate
in-flight request returns 409 with `Retry-After`; capacity exhaustion returns 503
without recording a new request. Reusing an ID for different input returns 409.
An interrupted process requires a new request ID. Dagster polls the original ID
after a transport failure rather than repeating the search, and continues to own
company selection, adaptive answer timeouts, S3 archiving and ClickHouse publication.

Run the real browser/CDP fixture tests without contacting Brave:

```bash
BRAVE_NATIVE_TEST=1 CLOAKBROWSER_AUTO_UPDATE=false uv run python -m unittest discover -s tests -p test_brave_native.py
```

Brave's proof-of-work verification can return HTTP 429 on the original Ask URL.
The detector recognizes its “Verify” / “Switch to traditional CAPTCHA” controls
and allows a short render delay before treating a rejected document as a hard
access failure. A plain 429 without verification UI returns `RateLimited`; 401
and 403 return `Unauthorized` and `Forbidden`. All failures retain `http_status`
and best-effort `failure_evidence` (title, Retry-After, HTML and screenshot),
including failures that never started the CAPTCHA agent.

Ask can also show an “I'm not a robot” verification dialog over an answer error.
This dialog takes precedence over the answer footer. Completion checks use the
footer's explicitly labeled retry icon, because the inline error has a separate
text button with the same accessible name, “Try again”. Known browser failures
are reported as `AmbiguousElement`, `NavigationInterrupted`, `BrowserClosed`, or
`TimeoutError`; other Playwright errors use `BrowserError`. Raw exception text is
not returned because it can contain credentials or page content.

### Website redirects

Browser extraction follows redirects and returns the final `url`, HTTP
`redirects` (`url`, `status_code`, `location`), and `navigationAttempts`.
For an HTTPS root URL that fails with a TLS protocol/cipher error, navigation
makes one HTTP attempt to discover legacy redirects such as AGA → Linde.
Certificate errors, non-root paths, and query-bearing URLs do not trigger this
fallback. Certificate verification stays enabled.

`robots.txt` is fetched in a temporary Chromium tab using the same browser
context. The temporary tab is closed after reading it. The final destination's
robots policy is checked before returning redirected content for analysis.
DNS/TLS failures are returned as failed captures with Chromium's error code,
instead of being mistaken for a closed browser.

```bash
BROWSER_NAVIGATION_NATIVE_TEST=1 CLOAKBROWSER_AUTO_UPDATE=false uv run python -m unittest discover -s tests -p test_navigation.py
```

For a deployment using the already validated lockfile, run `ansible-playbook site.yml --skip-tags upgrade_browser` from `ansible/`. The inventory uses the crawler host's Tailscale/SSH name.


## Durable Brave company batches

`POST /v1/brave/batches` accepts up to 500 company inputs with a batch UUID, frozen
execution/task IDs, controller run ID, registered LLM owner and encrypted options.
It commits to `$BROWSER_STATE_DIR/brave-queue.sqlite3` (SQLite WAL, full synchronous
commits) before returning 202. Each of four route workers by default saves a result
locally before taking the next company, retaining the existing request JSON evidence.
A batch is `completed` only after **all** outcomes are confirmed in ClickHouse.
Individual search failures count as processed outcomes.

- `GET /v1/brave/executions/{execution_id}/batch`: discover unfinished work for resume.
- `GET /v1/brave/batches/{batch_id}`: safe counts/state; no credentials or result documents.
- `POST /v1/brave/batches/{batch_id}/heartbeat`: renew controller lease and read progress.
- `POST /v1/brave/batches/{batch_id}/resume`: resume saved work, preserving completed results.
- `POST /v1/brave/batches/{batch_id}/cancel`: stop workers; retain resumable results.
- `GET /v1/brave/batches/{batch_id}/result-ids`: bounded membership for independent publication verification.

Heartbeat/resume/cancel bodies contain `controller_id` and `owner_request_id` UUIDs.
Dagster heartbeats every two seconds and submits the next batch only after this one
is published. A 60-second expired lease pauses work. Service restart also pauses
unfinished batches until explicit resume. Completed SQLite batches are pruned after
seven days on new submissions; unpublished data is retained. ClickHouse and
PostgreSQL history are unaffected by local cache pruning.

Configure `BRAVE_CLICKHOUSE_URL` (HTTP endpoint), `BRAVE_CLICKHOUSE_USER`,
`BRAVE_CLICKHOUSE_PASSWORD`, and `LLM_CONTROL_PG_URL` in the service environment.
The publisher needs SELECT/INSERT on `corpscout.company_brave_search_results` and
`corpscout.se_company_brave_search_results_latest_success`. PostgreSQL uses the
existing `processing_worker` admission/ledger grants. No database credentials are
accepted in batch requests, and the destination tables are fixed in service code.

The service checks LLM admission before every browser request and every two seconds
while a batch runs; disabling a model or stopping its owner cancels active requests.
Request IDs stay compatible with existing CAPTCHA statistics and Backoffice history.
Publication failures retry the local results without repeating browser work. Dagster
reports live progress during processing; company result views update on publication.
