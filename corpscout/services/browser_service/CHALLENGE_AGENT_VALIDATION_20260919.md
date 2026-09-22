# CAPTCHA agent validation — 2026-09-19

Browser service 0.4.0 was deployed to `192.168.88.132`. Backoffice's existing local
development application now has **Try CAPTCHA agent** in its waiting-browser
panel. DeepSeek V4.1 Flash is called using `deepseek-flash`; actions use the
selected page's CDP session and existing SQLite browser assignment.

## Automated validation

- Browser service suite: 87 tests run, **85 passed**, two opt-in tests skipped.
- New agent cases verify malformed actions, out-of-bounds clicks, stale page
  decisions, ended/restarted sessions, step/time limits, cancellation, provider
  errors and persisted evidence. API tests additionally check authentication,
  explicit approval, matching URL/generation, busy-session rejection and retaining
  ownership after the agent finishes.
- Native Linux/Xvfb test with a deterministic model HTTP fixture passed. A CDP
  click reached a button in a cross-origin iframe, changed the real page content,
  and preserved the browser generation and lease.
- The same native test with the actual DeepSeek API passed: **two decisions,
  3.196 seconds, 3,922 prompt tokens and 93 completion tokens**. The model selected
  coordinates from its screenshot and observed the changed page afterward.
- Backoffice: **13 tests passed**, plus TypeScript checking and production build.
- Ruff checks/formatting and `git diff --check` passed.
- Ansible deployment: **37 OK, 11 changed, 0 failed, 0 unreachable**.

The model/browser integration tests used a local page built for this test. Its
verification control is a simple button inside an iframe, not a third-party
CAPTCHA. No third-party challenge was solved or attempted by this validation.

## Backoffice → model → CDP → resumed crawl

The fixture was served on a temporary loopback HTTP server on the crawler machine.
Its title triggered the crawler's normal blocked-page detection.

1. Submitted `challenge-agent-ui-fixture-20260919` through REST with one explicit
   fixture URL. The automatic attempt failed after its normal assistance deadline.
2. Clicked **Retry interactively** in Backoffice, producing
   `manual-c984e24e-3ae2-4844-b8e4-ba8eb3b4454a`.
3. Confirmed the paused retry and connected noVNC desktop in the UI.
4. Clicked **Try CAPTCHA agent**. The model clicked the visible verification
   button through CDP and then reported `appears_clear` from the next screenshot.
5. Confirmed the crawl was still paused. Clicked **Resume crawl** separately.
6. The crawler verified access, finished with **one collected document**, and
   uploaded its JSON and artifacts. Both the failed original and completed retry
   were read through Backoffice's ClickHouse/S3 result route.

Deployed agent run: `1c403c430a8a40b38b9e82d238ce2bf0`.

| Measurement | Result |
| --- | --- |
| Model | `deepseek-flash` (V4.1 Flash) |
| Decisions | 2: click, finish |
| Agent elapsed time | 3.232 seconds |
| Prompt tokens | 3,919 |
| Completion tokens | 88 |
| Total tokens | 4,007 |
| Agent outcome | `appears_clear` |
| Crawl outcome after explicit resume | `finished`, one document |

Run screenshots and the action journal remain on the server at
`/var/lib/browser-service/challenge-runs/1c403c430a8a40b38b9e82d238ce2bf0/`.
A local evidence copy, along with both jobs and their JSON results read through
ClickHouse, is under `../company_research/data/challenge-agent-20260919/` (ignored
runtime data). The temporary HTTP fixture server was stopped after verification.

## Limits

This proves model vision, coordinate actions, CDP attachment, browser ownership,
the Backoffice controls and the existing crawl-result path. It does not establish
a completion rate for Cloudflare, Turnstile, reCAPTCHA or any other third-party
challenge. `appears_clear` remains a model observation; the crawler's separate
resume check determines whether collection can continue.

Agent diagnostics currently stay on the browser server, separately from crawler
S3 artifacts. See [operation and limits](CHALLENGE_AGENT.md).
