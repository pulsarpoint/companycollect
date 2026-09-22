# Brave answer-generation failure — 2026-09-20

## Reported failures

Task `0602f7d2-6158-48f1-9b31-0550d8383d4e` reported two fast `Error` / `answer_generation` failures through `crawl_proxy3`:

| Input | Company | Query execution |
| --- | --- | ---: |
| `SE:5560726605` | H.B.V. FÖRSÄLJNINGS AKTIEBOLAG | 1.772 s |
| `SE:5560726977` | VAROPreem Sverige AB | 2.570 s |

Both companies had also failed through `direct` and `crawl_proxy2`, for three attempts each. All six saved browser results had HTTP 200 and zero CAPTCHA-agent runs. The 60-second answer budget had not expired. The original failure artifacts showed the ordinary Ask interface while its answer was still loading.

Original request IDs on `crawl_proxy3`:

- `dagster-c0a165f2-636c-4bd5-8e85-f7e3016c89d2`
- `dagster-2354ea8c-b35a-46f3-86a8-59f4c8b01f99`

## Reproduced cause

Running the deployed `BraveAsk.copy_answer()` against H.B.V. in an exclusively reserved browser on the same server reproduced a Playwright strict-mode error at the answer-completion wait. The locator for a button named `Try again` resolved to two elements:

1. An inline error retry button with `data-sveltekit-reload="true"` and visible text.
2. The answer footer's icon button with `aria-label="Try again"`.

The screenshot taken at that failure also showed a “Prove you are human” dialog with an “I'm not a robot” button over the answer error. The old code could throw before checking this dialog; its fallback text detection also skipped completed-looking pages.

The controlled reproduction used a headed browser on `direct`. VAROPreem succeeded in a separate controlled retry there. The original six exception messages were not retained, so the exact exception is directly confirmed for the H.B.V. reproduction, while the original failures have the matching stage, timing, and UI behavior.

Local diagnostic screenshots and original artifacts are under `output/playwright/brave-answer-errors/` (ignored by Git).

## Fix

- Completion and access checks target `button[aria-label="Try again"]`, distinguishing the footer from the inline text button.
- The visible “I'm not a robot” button is checked before answer-completion state, allowing automatic CAPTCHA assistance when the dialog overlays an answer error.
- Known Playwright failures now receive safe categories: `AmbiguousElement`, `NavigationInterrupted`, `BrowserClosed`, or `TimeoutError`; other failures use `BrowserError`. Exception messages containing URLs or page content are not returned.
- Existing query identity checks, CAPTCHA budgets, answer deadlines, and saved failure artifacts remain in force.

## Verification

- Native Brave suite: 12 tests passed. New fixtures cover an inline retry appearing before answer completion and a late verification dialog over both retry buttons, including actual CDP agent interaction and resuming the requested query.
- Browser regression suite: 113 tests, 99 passed and 14 opt-in tests skipped.
- Ruff and `git diff --check`: passed.

## Deployment and live retries

Version 0.7.3 was deployed using Ansible: 38 successful tasks, 10 changed, zero failures or unreachable hosts. Health verification passed. SQLite settings remain four headless and two headed browsers.

The two exact company queries were submitted concurrently using the existing Dagster `BraveBrowserResource.ask()` client through `crawl_proxy3`, with the original 60-second answer budget. This exercised the production client without modifying historical processing results.

| Company | Browser | Result | Query execution | Client wall time | Agent runs |
| --- | --- | --- | ---: | ---: | ---: |
| H.B.V. FÖRSÄLJNINGS AKTIEBOLAG | `headless-2` | success, HTTP 200 | 24.952 s | 29.58 s | 0 |
| VAROPreem Sverige AB | `headless-1` | success, HTTP 200 | 12.383 s | 17.28 s | 0 |

Saved request IDs:

- `dagster-duplicate-retry-5560726605-ea099b33340f47f09b1991481a6738d8`
- `dagster-duplicate-retry-5560726977-7fad4bd4f5e743afbf7d5da93068f9c6`

Both produced nonempty company answers, and both test leases were released. Neither post-deployment headless retry encountered a CAPTCHA; agent invocation for the overlapping dialog is covered by the native browser fixture.

A final headed H.B.V. query on `direct` also succeeded on `browser-1` in 15.237 seconds of query execution (20.11 seconds client wall time), without a CAPTCHA. Its request ID is `duplicate-retry-modal-72a7d5ac7335404ca8a75b36140a50c3`. This confirms the headed query path but does not add live verification of the dialog-solving path, because Brave did not display that dialog during the retry.

The service restart waited its configured 330-second graceful-shutdown budget. One pre-existing stalled request (`dagster-eaa0970f-a2e7-45f7-b018-c20cf2c61f1d`) had made no recorded progress for over ten minutes and was cancelled by Uvicorn when that budget expired. No force-kill was issued. The service then completed shutdown and started the new version.
