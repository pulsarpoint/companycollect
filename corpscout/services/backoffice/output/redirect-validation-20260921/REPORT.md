# Redirect handling validation — 2026-09-21

**Deployment update:** Browser 0.7.4 and crawler 0.39.1 are now deployed. Both production site checks passed and Brave resumed. See [deployment report](../redirect-deployment-20260921/REPORT.md). The isolated-test notes below describe the earlier validation.

Implemented in the local browser-service, crawler, and Backoffice code. Verified using an isolated browser-service instance and the crawler's HTTP client on 192.168.88.132. The main browser service remains on its existing build because it is processing active Brave requests. No production browser sessions were interrupted.

## Real results

| Input | Final URL | Result | Time | Model calls |
| --- | --- | --- | --- | --- |
| https://aga.se/ | https://www.linde-gas.se/shop/sv/se-ig/home | Company description and HTML collected | 35 s | 1 |
| https://advokatsamfundet.se/ | https://www.advokatsamfundet.se/ | Swedish Bar Association described; further company crawling correctly skipped | 19 s | 1 |

AGA has a TLS protocol error on its HTTPS endpoint. HTTP returns a 301 redirect to Linde. The crawler now makes one HTTP attempt after a root-URL TLS protocol/cipher failure. Certificate failures, paths, and URLs with queries do not trigger the fallback. HTTPS certificate checks remain enabled.

Advokatsamfundet's previous failure occurred in the separate HTTP client used for robots.txt. Chromium can load both robots.txt and the homepage with normal certificate verification. Robots retrieval now uses a temporary tab in the same browser context, which is closed afterward. The final destination's robots policy is checked before returning redirected content for analysis.

## Saved identity and provenance

The original input remains the identity; following a redirect does not rewrite the queued domain or claim that the two companies are the same legal entity.

- `crawl.input_url`: original input.
- `crawl.site_url`: final landing URL used for classification/discovery.
- `crawl.site_info.source_url`: source of the description.
- `crawl.pages[].requested_url` and `source_url`: requested and final page URLs.
- `crawl.pages[].redirects`: observed HTTP redirects, including status and Location.
- `crawl.pages[].navigation_attempts`: attempted entry URLs and errors; an HTTP fallback is not mislabeled as a server redirect.

These page fields flow into the portable result, its embedded capture metadata, and the existing ClickHouse `pages` JSON section. No new database columns are required. Backoffice shows Original website and Final website for a successfully fetched landing page; this was verified with the existing acando.com → CGI archive.

## Artifacts

- [AGA portable result](aga-result.json)
- [Advokatsamfundet portable result](advokatsamfundet-result.json)
- [Real-run summary](live-summary.jsonl)

The two new test results are local artifacts, copied from the isolated server run. They have not been published as new production ClickHouse/S3 attempts. Both portable HTML hashes were checked and match their saved content.

## Verification

- Browser-service suite: 116 tests, 15 opt-in skips, no failures.
- Native browser redirect fixture on the server: 3 tests passed, including HTTPS failure → HTTP 301, destination robots enforcement, temporary-tab cleanup, and certificate-failure handling.
- Crawler suite: 366 tests, 45 opt-in skips, no failures.
- Backoffice saved-result tests: 6 passed; TypeScript check passed.
- Browser navigation lint and changed tracked-file whitespace checks passed.

The server test used a separate virtual environment and direct headless browser profile. It did not modify the systemd services, their state databases, or their deployed environments. Activating the backend change and retrying the production Basic info rows remains the deployment step.
