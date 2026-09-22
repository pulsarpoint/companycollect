# On-demand browser sessions — validation

Implemented browser service 0.8.0 with crawler 0.40.0 and matching Backoffice/Dagster clients.

## Behavior

- One `max_browsers` setting (default 6); SQLite overrides startup flags/env.
- Startup opens no Chromium processes. Starting/stopping executions consume capacity.
- Profiles belong to persistent session IDs, survive close/reopen, and expire after seven days by default. Pinned profiles survive retention.
- Each execution has a separate ID. Crawler heartbeats, tab commands, extraction and close use that ID to reject stale operations.
- Automatic crawls launch headless; interactive retries reopen their saved profile headed with Xvfb/noVNC.
- Brave closes browsers after requests and Dagster reuses a session per sequential route worker.
- Fixed-slot profiles are copied into pinned saved sessions; originals and old SQLite history are preserved.
- Backoffice provides capacity/timeout/retention settings, saved-session pins, manual headed sessions, and raw API testing.

## Checks on 2026-09-21

- Browser service: 104 tests passed, 9 subtests passed; 15 opt-in tests skipped locally.
- Crawler: 326 tests passed, 188 subtests passed; 44 optional external/native tests skipped locally. The unrelated Temporal experiment is outside the `tests/` suite.
- Backoffice: TypeScript typecheck and 35 relevant tests passed.
- Dagster Brave HTTP integration: 12 tests passed; `uv run dg check defs` passed.
- Python Ruff checks passed; Ansible playbook syntax validated.

## Isolated Linux tests on 192.168.88.132

Test source was copied to `/tmp/browser-lifecycle-check.ZlbR7A`; tests used temporary
profile directories, the installed browser dependencies and CloakBrowser binary.
No production state directory, service endpoint, S3 bucket, ClickHouse table or live
Brave query was used for these tests.

- 14 native browser/Brave/CDP tests passed (72.5 seconds).
- Crawler site/search sharing and verification recovery passed against a local fixture.
- Crawler manual concurrency and profile-reuse tests passed (28.6 seconds), including persisted execution IDs.
- Verified session-cookie/localStorage retention across headed → headless → headed, profile isolation, RFB/VNC connectivity, closing and capacity release.
- Initial manual native fixture omitted `interactive: true`; corrected the fixture and reran the affected test successfully.

## Deployment state

Deployed on 2026-09-21 to `192.168.88.132`:

- Browser 0.8.0: `c7a0c14ed25632f0b8d6c2f3e87c6614555140651e4389bd3100c8dc0b987590`
- Crawler 0.40.0: `2dc19d6f3a57e5f9ec317de31faabaeb316aa04f41395771560a119cc76330be`
- Matching Dagster source hot-synced without restarting the supervisor or unrelated runs.
- Backoffice rebuilt and verified on the running local development server.

The browser state directory, service configuration, crawler SQLite files and previous
release links were backed up under `/var/backups/browser-session-deployment-20260921`
on the crawler host before activation. Eighteen stale Chromium process locks were
removed only after confirming all referenced processes had exited. Twenty-four old
profiles were imported as pinned saved sessions, with the original folders preserved.

Production startup opened zero browsers; the migrated SQLite setting remains six
maximum browsers, 120 seconds idle timeout, and seven days retention. Headless and
headed captures returned HTTP 200. Reopening preserved the session ID and assigned
a new execution ID; stale close returned HTTP 409. The noVNC WebSocket returned the
RFB 3.8 handshake. Both service health checks passed and installed Python module
hashes matched local source (13 browser modules and 47 crawler modules).

The pending ClickHouse naming cutover was also applied: physical table UUIDs and
row counts were preserved, archive views and publisher grants updated, and obsolete
result aliases removed. Existing S3 crawl results remain readable through the new
archive mapping.

See the deployment receipts and final live-run checks in
[deployment report](../backoffice/output/session-deployment-20260921/REPORT.md).
