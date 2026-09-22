# Browser session deployment — 2026-09-21

Deployment and live verification completed.

## Releases

- `192.168.88.132`: browser service **0.8.0**, crawler **0.40.0**, both healthy systemd services with no restarts reported after activation.
- Browser release: `c7a0c14ed25632f0b8d6c2f3e87c6614555140651e4389bd3100c8dc0b987590`.
- Crawler release: `2dc19d6f3a57e5f9ec317de31faabaeb316aa04f41395771560a119cc76330be`.
- Installed source matches local source: all 13 browser modules and 47 crawler modules.
- Dagster deployed by validated Ansible content hot-sync. Supervisor PID `2232153` and restart count remained unchanged; the running Norway PDF asset continued.
- Backoffice production build passed; the existing local development server serves the new source. Settings, live crawl status and the newly saved result were verified in the browser.

## State preservation

Brave run `952c6607-49b4-4667-9572-7b10fd9471d6` was stopped gracefully. The original execution `ce521036-669b-4b96-8d1d-a71147c3bfa4` retained **15,918** completed outcomes before resumption (15,709 successes, 209 errors).

Stopped-state backups are in `/var/backups/browser-session-deployment-20260921` on the crawler host: the complete browser state tar, crawler SQLite files, service environment/unit files and previous release links. This directory is restricted to root. Eighteen stale Chromium locks were removed only after confirming their processes were absent.

All **24** existing browser profiles were imported as pinned saved sessions. Original profile directories and legacy SQLite history remain. Production startup opened zero browsers. SQLite settings override startup values: maximum **6**, idle timeout **120 seconds**, retention **7 days**.

ClickHouse names were updated in place without replaying migrations or changing the migration ledger. Physical table UUIDs and row counts were preserved: 26,896 Brave outcomes, 22,673 current successful Swedish answers, and 61 existing basic-info attempts. Archive mappings, latest views, success materialized-view target and publisher grants use the new names; obsolete result views were removed after consumer deployment.

## Live checks

- Headless and headed captures returned HTTP 200.
- Closing retained the saved session. Reopening kept its ID and created a different execution ID.
- Stale execution close was rejected with HTTP 409.
- Headed noVNC WebSocket returned `RFB 003.008`.
- Saved S3 objects remained readable through `website_crawl_results_s3_archive`.
- Dagster basic-info run [`1c147fa2-8890-466e-88f4-b31e34c86030`](http://dagster:3000/runs/1c147fa2-8890-466e-88f4-b31e34c86030) succeeded. `aga.se` followed its redirect to `https://www.linde-gas.se/shop/sv/se-ig/home`, generated a company description and stored a successful normalized row plus the S3 JSON object. Crawl time: 21.768 seconds; Dagster asset time: 26.84 seconds. One model call used 40,773 input and 1,684 output tokens. No CAPTCHA assistance was needed.
- Backoffice rendered that new result through ClickHouse, showing original/final URLs and the collected page.
- Brave resumed as [`19ac7955-07a7-46d9-beef-b92d45d06580`](http://dagster:3000/runs/19ac7955-07a7-46d9-beef-b92d45d06580) with the original execution ID and the renamed `company_brave_search_results` asset. At the continuity check, all **9** new outcomes succeeded and the latest-success table updated. Total outcomes **15927** equaled distinct inputs and the previous checkpoint plus new outcomes.
- Direct and all three proxy routes returned successful Brave answers. SQLite showed multiple executions per route's saved session, confirming reuse while browser processes close between requests.

Brave remains running on the existing selected batch. The verification crawl is complete. No git commit was made by this deployment.

Detailed receipts: `browser-smoke.json`, `provenance.json`, `schema-before.json`, `schema-cutover.json`, `schema-cleanup.json`, `final-check.json`, and the three Ansible deployment logs in this directory.
