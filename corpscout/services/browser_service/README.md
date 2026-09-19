# Browser service

Independent HTTP service owning a fixed pool of Chromium/Xvfb desktops. The crawler
has no Chromium, Playwright, CDP, VNC, or local profile dependency at runtime.
Backoffice uses this service directly at **Browsers → Servers & sessions / Saved profiles**.

**Browsers → Testing** (`/admin/browsers/testing`) sends raw API requests through
Backoffice using its server-side service credential. Enter a URL and send the first
navigation request; the service assigns a browser automatically. Optionally choose
a specific browser before sending. Later requests use the same session ID.
Edit the JSON freely to exercise new options and validation failures. The tester
shows the API status separately from the target page status, the raw response,
HTML source, screenshots, and the last ten requests for replay. **Connect browser**
opens the assigned desktop. Finish with the release preset. No automatic requests,
retries or heartbeats run; use the heartbeat preset explicitly to extend a session,
or leave it idle to test expiry. History stays in page memory only.

Each domain attempt uses one existing browser exclusively. Site and search tabs
use the same caller-generated session ID and share that profile's login state.
The service never increases the configured pool. Explicit legacy reservations wait
FIFO. New implicit sessions return 503 without creating a reservation when capacity
is unavailable, so the same request and ID can be retried later.

## Run

Linux requires Xvfb, xauth, x11vnc, Openbox and Playwright's Chromium system libraries.

```sh
uv sync
cp .env.example .env
uv run browser-service --env-file .env
```

`BROWSER_API_TOKEN` is required beyond loopback. The API and management endpoints use
`Authorization: Bearer …`. Tokens, cookies, CDP/VNC ports and profile files stay private.
Use HTTPS when crossing an untrusted network.

## API

This is a small API inspired by Zyte's request shape, not a claim of wire compatibility.
`/docs` provides the request schemas.

1. Generate a fresh UUID (32 lowercase hexadecimal characters) for each crawl attempt.
   Persist it, then send `POST /v1/browser/extract` directly:

   ```json
   {"session":{"id":"bde287b88bf844299991028cbaf18673"},"tab":"site","url":"https://example.com/","browserHtml":true,"screenshot":false,"checkRobotsTxt":true,"timeoutSeconds":60}
   ```

   On the first URL request, the service allocates an existing browser and records
   its mapping in SQLite before navigation. The initial URL supplies the domain label.
   Concurrent first requests for the same ID share one assignment and serialize.
   No reservation call or status polling is needed.
2. Optionally include `"browserId":"browser-2"` in that first extract request to
   select a specific browser. Omit it for automatic assignment. If the selected
   browser is busy or unavailable, return 503 without falling back to another
   browser or leaving a queued reservation. Unknown browser IDs return 422.
3. Subsequent requests only need the same `session.id`; they always use its assigned
   browser, including site and search tabs. Supplying a conflicting browserId returns
   409. Responses include `session.profileId` and `generation`.

   Response: session, final URL, target statusCode, allowlisted response headers,
   raw browserHtml, optional base64 PNG screenshot, and error. Omitting URL captures
   the current document without navigation. The crawler handles HTML simplification,
   link/context extraction and company interpretation.
4. `POST /v1/browser/sessions/{id}/tabs/{name}` accepts `open`, `focus`, `close`, or
   `recover`. Recovery takes the pending URL and optional `reopenClosedTab` (false).
   Closing one tab does not reopen it automatically. Whole-browser recovery preserves
   the assigned profile and changes its generation; it never confirms human verification.
5. `DELETE /v1/browser/sessions/{id}` releases idempotently. The service saves the profile
   and cookies, then restarts that browser with a blank tab before another assignment.

Sessions expire after **120 seconds without crawler activity**, configured on the
service. Heartbeats (`POST /v1/browser/sessions/{id}/heartbeat`) and browser operations
renew the deadline. The crawler sends heartbeats every 20 seconds, including while
waiting for capacity or human assistance. GET status, management reads and desktop
connections do **not** renew it. In-flight operations are protected from idle expiry,
with a maximum operation duration of 310 seconds. If release cannot reach the service,
expiry still reclaims the browser.

SQLite (`sessions.sqlite3`, WAL) records the session ID, request, domain, profile,
generation, state, last request, deadline and completion. A process lock prevents two
services owning the same profiles. A browser-service restart marks previous assignments
`interrupted` and starts blank browsers; expired/released/interrupted IDs return **410**
and cannot be reused. Unknown IDs return 404. A new crawl attempt needs a fresh ID.
The crawler's own restart does not stop the browser service.

The existing `POST /v1/browser/sessions` endpoint remains supported for crawler
compatibility and explicit FIFO queueing. It accepts `{id, requestId, domain}`;
callers poll `GET /v1/browser/sessions/{id}` until ready, then extract as above.
Repeating the POST is idempotent; changing the request/domain for an ID returns 409.
GET, heartbeat, capture without a URL, tab controls and desktop connections never
implicitly create a session.

## Management

- `GET /v1/server`: server, desktops, configured timeout and recent assignment history.
- `GET /v1/browser-sessions`: saved profiles and open tabs.
- `POST /v1/browser-sessions/{profile}/start|stop|settings|tabs`: profile controls.
- `POST /v1/browser-sessions/{profile}/tabs/{tab}/focus`; `GET …/inspect`.
- Desktop, profile, and lease `browser-ticket` endpoints return 30-second, single-use
  WebSocket tickets. The browser connects directly to this service, not through crawler.
- `GET /healthz`: service readiness.

Disruptive profile controls reject assigned profiles. Auto-restart is independently
configurable per profile. Private Chromium profiles and saved session cookies live in
`profiles/browser-N`; they are never included in crawl archives.

## Deployment and migration

`ansible/site.yml` deploys an independent wheel, locked environment, system user and
`browser-service.service` on port 8081. State: `/var/lib/browser-service`; configuration:
`/etc/browser-service/browser-service.env`. Copy `ansible/secrets.yml.example` to the
ignored `secrets.yml` and set a separate random API token. Use
`-e browser_service_activate=false` to prepare without starting/restarting it.

Each deployment first runs `uv lock --upgrade-package cloakbrowser` to select the
latest stable CloakBrowser package compatible with the service, with no upper
version limit. The resulting `uv.lock` is exported with hashes and used for the
whole release; review and retain its changes alongside the release. The running
service does not update its Python dependencies between deployments.

For the initial extraction, stop the old crawler, copy its entire private
`results/.browser-sessions/` directory into `/var/lib/browser-service/profiles/`, and
change ownership to `browser-service`. Keep the old copy for rollback. Never run both
services against the same Chromium profile directory. Start this service, configure
`BROWSER_API_URL` and `BROWSER_API_TOKEN` in crawler and Backoffice, then deploy crawler
0.37.0. Backoffice optionally uses `BROWSER_PUBLIC_URL` for desktop WebSockets.

## Validation

```sh
uv run python -m unittest discover -s tests -v
```

The crawler's `test_browser_pool_native.py` exercises real Linux/Xvfb browsers through
a listening HTTP API: parallel manual crawls, authenticated site/search sharing,
closed-tab recovery rules, cookie/storage preservation and recycling. Tests use private
temporary profiles and a local website fixture, never real verification challenges.
