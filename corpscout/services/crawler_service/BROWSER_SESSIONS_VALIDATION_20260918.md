# Persistent browser pool and server inventory validation

Validated on 18 September 2026 against the Linux crawler at `192.168.88.132:8080`
and Backoffice at `http://localhost:5183`. Deployed version: **0.34.1**, release
`97e06d51b060e3a822af963ae9b3770d628f63bc495cbc1113c2fc94dc46af21`.

## Delivered behavior

- Two independent headed Chromium profiles under Xvfb, `browser-1` and `browser-2`.
- Backoffice **Crawler → Crawls / Servers & sessions / Saved profiles** navigation.
- Server inventory shows health, version, open desktop count, desktop kind,
  associated crawl, page, start time and a Connect action.
- Inventory includes every desktop managed by this service: saved profiles,
  interactive crawls and shared headed Brave search. It is not a machine-wide
  process scanner or multi-host discovery service.
- Inline noVNC access uses single-use 30-second tickets, scoped to a desktop's
  lifetime. Closing a desktop removes its inventory entry and invalidates access.
- Saved profiles preserve tabs, cookies (including session cookies), and Chromium
  storage across restarts. Profile directories are 0700; snapshots are 0600.
- Graceful browser shutdown flushes Chromium before terminating Xvfb. Unexpected
  browser closure leaves a restartable session with an error state.
- HTTP shutdown drains long-lived connections for at most ten seconds before
  lifespan cleanup, allowing saved-browser cleanup within the systemd deadline.

The pool remains independent from automatic crawl execution and the shared Brave
search browser. No account credentials were supplied or used by the implementation
test, and there is no automatic CAPTCHA-solving agent.

## Automated verification

- Python: **324 tests run, 36 skipped**, no failures.
- Frontend: **16 tests passed** across crawl controls, saved sessions and the server
  view. React Router type generation and TypeScript checks passed.
- Python Ruff checks and targeted `ty` checks passed.
- Real Linux/Xvfb: **2 integration tests passed** on the installed release.
  The tests verify profile isolation, shared tabs, HTTP-only session-cookie and
  local-storage restoration, tab restoration, VNC handshake, browser-close
  recovery, desktop registry lifecycle, and the existing Brave verification flow.
- API checks cover authentication, private-field exclusion, all desktop kinds,
  closed/stale ticket rejection and single-use tickets.

An initial native test exposed lost local storage during desktop termination.
Graceful Chromium shutdown fixed it; the native restart test passed on the
deployed release.

## Live UI verification

The Crawls page exposes all three tabs. **Servers & sessions** lists server
`crawler` as Online with version 0.34.1 and two running saved-profile desktops.
Clicking Connect on browser-1 reached **Connected · keyboard and mouse enabled**.
Saved-profile Stop and save / Start browser restored browser-2's recorded tab.
Existing user browsing in these profiles was retained during the final deployment.

## Melexis observation

In a fresh, unauthenticated browser-1 profile, `https://melexis.com/` returned
HTTP 403 with title **Just a moment...** and a visible Cloudflare security
verification page. Observations at 21:43:27 and 21:44:23 UTC showed the same
challenge without clicks or reloads between those observations. Browser-2 also
returned the same HTTP 403 challenge, observed at 21:46:58 UTC. No CAPTCHA was
completed by the agent. These observations establish that the fresh headed
profiles did not clear that challenge automatically during the observation;
they do not establish behavior after a real account login or manual verification.
