# Direct CloakBrowser validation — 18 September 2026

Version 0.32.0 removes Crawl4AI from the service and its dependency lockfile. The
locked environment drops from 133 to 69 packages. Ansible deployed this version to
the regular `company-research.service` on `192.168.88.132:8080`; REST and JetStream
health checks passed. The deployed Python environment confirms that `crawl4ai` is
not installed.

## Browser and capture behavior

`browser.py` owns one Playwright tab/context, navigation, response status tracking,
robots.txt checks and deterministic HTML cleanup. `fetch.py` launches CloakBrowser
directly in headless mode for automatic scans. Manual retries use the existing
Xvfb worker and attach Playwright to its headed browser.

Browser User-Agent and client-hint headers are left native. The previous integration
advertised Chrome 116 in HTTP headers while the browser reported another version
through JavaScript. The regression test reproducing that mismatch failed before
replacement. The replacement passes checks comparing HTTP headers with JavaScript
identity, including both headless and headed modes on the crawler server.

Simplified HTML retains readable structure, links, headings, tables and useful
labels; scripts, styles and presentation attributes are removed. Full rendered HTML
remains available for evidence and JSON-LD/microdata extraction. Main-text extraction
still uses Trafilatura. Existing discovery, source limits, analysis inputs and service
storage remain in place. Historical `crawl4ai_links` provenance values remain accepted
when loading old artifacts; new captures use `browser_links` where applicable.

Resume captures the current document without navigation and subsequent pages use
the same browser context. It preserves real HTTP status instead of generating a
synthetic success status. A small valid page is no longer rejected solely because
it has little text.

## Melexis production test

- Automatic request: `melexis-native-headless-20260918`.
- Target: `https://www.melexis.com/`, explicit page list, zero LLM calls.
- Result: Cloudflare challenge, then `failed` / `human_assistance_timeout` after
  the 10-second notification window. Zero company pages collected.
- Failure result and diagnostics were recorded in SQLite and uploaded beneath
  `s3://crawls/company-crawls/melexis-native-headless-20260918/attempts/0001/`.
- Manual retry: `manual-native-melexis-20260918`, linked to the automatic failure.
  The headed browser opened successfully through Backoffice noVNC with keyboard
  and mouse enabled. Its initial page still displayed Cloudflare verification.

The first manual session expired before completion. The operator then started
`manual-3be613a4-0e8e-4369-9c1e-73ee29a6c6a6`, completed verification and resumed.
This retry **finished successfully at 13:58:30 UTC**, collecting the requested
homepage with the title “Semiconductor Solutions - Innovation with heart | Melexis”.
The service recorded `Resumed by operator`, `crawl_status: finished`, zero LLM calls
and successful S3 delivery. The operator also confirmed that the flow worked.

The result is stored at:

```text
s3://crawls/company-crawls/manual-3be613a4-0e8e-4369-9c1e-73ee29a6c6a6/attempts/0001/result.json.gz
```

This proves successful manual verification and capture for this attempt. It does
not establish that the header mismatch was the sole cause of earlier failures or
that future attempts will avoid challenges.

Evidence is saved under `data/native-browser-20260918/`:

```text
melexis-automatic-status.json
melexis-automatic-result.json
melexis-manual-status.json
melexis-successful-manual-status.json
melexis-successful-manual-result.json
```

## Validation

- Existing Python suite: 281 tests ran successfully; 32 optional tests skipped.
- Native browser/cleanup suite: all 3 tests passed locally and on the Linux server;
  the server run includes both headless and Xvfb/headed modes.
- Existing real-browser scenarios: all 6 passed after preserving the existing
  robots-denial error wording. This includes browser restart, redirects, rendering,
  source attribution, site gating and denial before fetching a forbidden page.
- Service suite with a real NATS server: all 35 tests passed.
- Ruff, source type checking, lockfile validation and whitespace checks passed.

The controlled authentication test verifies cookie retention, successful jobs-page
capture and that Resume does not reload the already authenticated company page.
