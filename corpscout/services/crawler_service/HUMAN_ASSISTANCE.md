# Failed attempts and human assistance

Since 0.37.0, all browser ownership and desktop connections live in the independent
[browser service](../browser_service/README.md). Use **Browsers** in Backoffice to
manage servers and saved profiles. The crawler retains challenge detection and human
verification state, and keeps its external lease alive while assistance is pending.


Set `CRAWL_HUMAN_ENABLED=true` on the Linux service and configure S3 storage.
The independent browser service owns Xvfb, Chromium and saved profiles. REST,
JetStream and manual scans lease a profile for their entire attempt. The noVNC
JavaScript client is embedded in Backoffice.

## Automatic CAPTCHA assistance

See the [deployment and validation report](AUTOMATIC_CAPTCHA_VALIDATION_20260919.md)
for the live JetStream, model, UI and S3 checks.
The [later-page recovery report](REPEATED_CAPTCHA_VALIDATION_20260919.md) covers
the Framework failure and the 0.37.5 budget/history correction.

Crawler 0.38.0 can invoke the existing browser-service agent before the human
assistance deadline. Set `CRAWL_CHALLENGE_AGENT_ENABLED=true`; this requires
`CRAWL_HUMAN_ENABLED=true` and `DEEPSEEK` on browser service 0.4.0 or later.
The Ansible deployment enables it with `crawler_service_challenge_agent_enabled`.

For automatic REST and JetStream website fetches, a detected CAPTCHA starts a
bounded agent run on the same tab and lease. The browser API supplies its existing
12-decision, 120-second limits. Heartbeats continue, cancellation remains available,
and the human timeout starts only after unsuccessful assistance. Plain HTTP denials,
authentication, robots restrictions, manual retries and Brave search verification
retain their existing paths. Later URLs can start another agent run, up to
`CRAWL_CHALLENGE_AGENT_MAX_RUNS` per crawl attempt (default 3, allowed 3–1000).
Ansible exposes `crawler_service_challenge_agent_max_runs`. The same requested
URL receives at most one automatic agent run, preventing retry loops.

The crawler separately captures the document and requires successful HTTP access,
nonempty content, the requested website and no challenge markers before continuing.
The agent's `appears_clear` observation alone cannot resume collection.

Status APIs and SSE include `challenge_agent_running`, the latest
`challenge_agent_result`, and all `challenge_agent_results`. Each result records
its page URL. Backoffice shows the running state, per-page results, actions, tokens
and timing. Results also appear in `crawl.challenge_agent` (latest) and
`crawl.challenge_agent_results` (all) in the portable JSON. The artifact
archive includes `human-assistance/<page>/agent-result.json` and post-agent HTML and
screenshot. The browser service retains its complete per-step screenshots locally.
Manual **Try CAPTCHA agent** remains available on interactive retries, and still
requires the operator to resume separately.

If a later challenge cannot be resolved, collected documents remain saved and the
portable crawl status is `partial`. The job stays `failed` so the existing failed
filter and interactive retry continue to work. Backoffice displays
**partial · stopped**, the saved page count and the URL that blocked progress.
The final reason distinguishes an exhausted agent budget from unsuccessful
verification on the current page.

When an automatic company-page fetch encounters a detected challenge, HTTP 401/403/429, or
anti-bot response, it publishes a `blocked` or `captcha` status notification. If
automatic CAPTCHA assistance is enabled and eligible, it runs first. Unresolved
attempts then wait **10 seconds**, fail, close their assigned browser and store failure
JSON and available HTML/screenshot evidence. Saved-profile desktops can be inspected through **Servers & sessions**, but automatic
company-page failures cannot be resumed; the operator starts a new interactive attempt after failure.
The worker can process the next request. A failed challenge is never reported as a
successful company-page capture.

Backoffice shows `/admin/crawls` with live status notifications and domain, state and
input filters. Notifications use the service's durable SSE event history and an
in-app toast. Failed attempts remain available after their browser has closed.

**Retry interactively** creates a new request linked to the failed attempt and reopens
its persistent session headed through the independent browser service. The original
request, failure and artifacts remain unchanged. A separate manual queue allows
interactive attempts alongside normal workers; all share `max_browsers`. Capacity
is reserved until the browser has stopped, then the next waiting request can launch.
They pause for up to **15 minutes**, including on the first successfully loaded page,
so the operator can inspect, navigate or authenticate before resuming. Resume checks
the actual document's HTTP status and challenge markers, then extracts the current
tab without reloading it. Subsequent requested pages use the same browser context.
Saved-session cookies and storage persist between attempts. Profiles are never
archived with crawl results. Browser
User-Agent and client-hint headers remain native; the service does not override them.

### Request budgets and retries (0.38.0)

REST and JetStream requests accept `challenge_agent_max_runs` (integer 3–1000,
otherwise the service default) and `challenge_agent_model` (`deepseek-flash` or
`z-ai/glm-5.3-flash`). The latter uses OpenRouter credentials on the browser service.
These settings affect CAPTCHA assistance, independently of the discovery LLM.
The standalone CLI accepts `--challenge-agent-max-runs` and `--challenge-agent-model`.

Backoffice offers **Retry with agent** as well as **Retry interactively**. Both
use the separate manual queue and preserve the failed attempt and its S3 archive.
An agent retry uses the automatic continuation flow and a 10-second human fallback.
Interactive retries retain the 15-minute operator window and manual agent button.

```json
{"attempt": 1, "request_id": "framework-retry-2", "interactive": false}
```

POST this body to `/v1/crawls/<failed-request-id>/retry`. When the prior attempt
stopped because the total agent budget was exhausted, the next retry doubles it:
3 → 6 → 12 → 24, capped at 1000. Other failures keep the prior budget. An explicit
`challenge_agent_max_runs` on the retry overrides the calculation, and an optional
`challenge_agent_model` selects a different agent model. Defaults remain interactive
for existing retry API callers; set `interactive: false` for automatic assistance.
This does not automatically enqueue retries or repeat a failed same-page agent run.
Reusing the retry request ID is idempotent only with the same options.

Live status and stored crawl JSON include the effective budget, selected model,
`challenge_agent_budget_exhausted`, and per-page agent results. These survive restart.

## Brave search verification

Brave searches use a separate tab in the same session as the domain scan.
All REST, JetStream and manual jobs share one search lock and durable block.
A detected validator (including HTTP 200 challenges)
or HTTP 401/403/429 stops further Brave navigation. The current crawl stays on its
pending query; jobs reaching search wait behind it. It does not plan a replacement
query, consume another search-budget slot, or automatically retry the challenge.

In `/admin/crawls`, choose **Start verification** on the paused attempt. Only this
explicit action focuses the assigned profile’s search tab and repeats the exact
pending URL. If the session is headless, retry interactively to reopen its profile with a desktop.
Complete the check in the embedded browser, then choose **Resume crawl**. Resume
requires HTTP 200, no detected challenge, ordinary result cards and the original
query on the original host/path. It captures the displayed results without another
reload and continues the same crawl attempt. The verified profile is retained for subsequent searches. Saved desktops remain
accessible in Backoffice; crawl-specific controls are available during assistance.

The pause has no automatic deadline before activation. The visible verification
session has a 15-minute deadline. Cancellation, timeout or browser failure closes
the browser but leaves Brave blocked. The next search requires manual activation
again. Jobs that need no search can use other free worker slots; a waiting crawl
continues occupying its slot and JetStream progress acknowledgements continue.

The block and pending URL persist in `<output-dir>/.brave-search/blocked.json`.
Browser profiles live in the browser service's private state directory and are
excluded from crawl/S3 archives. A service restart preserves the block;
normal crawl recovery rules still apply (an interrupted attempt may need replay).
On recovery, the blocked URL is verified before any different pending query is
sent. This is a per-service guard, not coordination across independent crawler
hosts or standalone CLI processes. With human assistance disabled, or in the
standalone CLI, a detected Brave block fails the crawl instead of continuing
additional search queries. The CLI has no interactive resume control.

## Persistent browser sessions

The independent [browser service](../browser_service/README.md) owns CloakBrowser,
Xvfb/noVNC and saved profiles. Its single `max_browsers` limit covers starting,
running and stopping browsers. Sessions are created on demand and retain cookies
and storage after close; the default retention is seven days. Backoffice can pin
sessions and reopen them headed for inspection or login.

A crawl reserves one persistent session and uses its current execution ID for every
operation. Site/search tabs share the profile; concurrent requests cannot share an
active session. Heartbeats keep paused crawls alive. Idle expiry closes abandoned
browsers without removing profiles. Manual retries reuse the failed session ID.

**Browsers → Servers & sessions** shows executions and open desktops. **Saved sessions**
shows retained identities and controls for starting/stopping manual browsers. Requests
that own a browser reject disruptive management controls; use **Cancel** on the crawl.
VNC tickets are single-use, expire after 30 seconds and are tied to the active browser.
CDP/VNC ports, cookies and proxy credentials are never returned in status.

Chromium closes gracefully at completion, failure or cancellation. There are no fixed
slots, separate headed/headless counts, blank-browser recycling or idle auto-restart
switches. During an active crawl, bounded recovery can restore a crashed browser
using the same profile; verification still requires checking the actual page.

Profiles are private at `<browser-state>/sessions/<session-id>/profile/` with a
0600 `session.json` cookie snapshot. Existing fixed-slot profiles are imported as
pinned sessions on upgrade, retaining originals for rollback. Settings and lifecycle
metadata live in the browser service's SQLite database.
