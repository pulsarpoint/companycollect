# Original batch failure recheck — 2026-09-21

Reran the five domains from the original 50-domain basic-info batch that still lacked successful results. `aga.se` and `advokatsamfundet.se` already had successful reruns and were excluded.

[Dagster batch](http://dagster:3000/runs/cdc00d29-c184-42fd-b2bc-65ef2c9576db) completed. **0/5 successful extractions.** Every attempt is stored in the normalized `website_site_info_results` table and its S3 JSON object was read back through ClickHouse.

Settings: headless, direct route, two concurrent requests, basic info / one page, DeepSeek Flash, CAPTCHA run budget 3, max model calls 20, forced refresh. Brave remained running.

| Domain | Result | Crawl seconds |
| --- | --- | ---: |
| [abogaranti.se](http://localhost:5183/admin/crawls/results?path=crawls%2Fcompany-crawls%2Fdagster-crawl-23f98afbb88541bc0903078aefc9e5a4ff4d5aabe53776b50ce5fc468eb3277f%2Fattempts%2F0001%2Fresult.json.gz) | DNS lookup failed: ERR_NAME_NOT_RESOLVED; also confirmed with the crawler server resolver. | 3 |
| [addage.org](http://localhost:5183/admin/crawls/results?path=crawls%2Fcompany-crawls%2Fdagster-crawl-d573341301c5740a61e696808770021d8cb46fbd856ba5cb827fee6e09662438%2Fattempts%2F0001%2Fresult.json.gz) | TLS certificate hostname mismatch: ERR_CERT_COMMON_NAME_INVALID. | 4 |
| [adlibris.com](http://localhost:5183/admin/crawls/results?path=crawls%2Fcompany-crawls%2Fdagster-crawl-f327a31c4fc34e46d6f92b8b79dbfe72b21e83911482d0097f71cf4efe7a95e1%2Fattempts%2F0001%2Fresult.json.gz) | robots.txt returned HTTP 429; the browser service declined the page fetch (reported status 403). | 3 |
| [allabolag.se](http://localhost:5183/admin/crawls/results?path=crawls%2Fcompany-crawls%2Fdagster-crawl-a2c234d76f15d10d4af246611794da5b997de1a89238a4e75e008b923aa7c32e%2Fattempts%2F0001%2Fresult.json.gz) | HTTP 403 block; the ten-second human-assistance wait expired. No CAPTCHA-agent run was recorded. | 15 |
| [alltele.se](http://localhost:5183/admin/crawls/results?path=crawls%2Fcompany-crawls%2Fdagster-crawl-54a21d4664d1f4dffd801e453eef96001f0ee37691f495472c9c02ff0f565a0c%2Fattempts%2F0001%2Fresult.json.gz) | DNS lookup failed: ERR_NAME_NOT_RESOLVED; the crawler server resolver reported no address for the hostname. | 4 |

No LLM calls or CAPTCHA-agent runs were recorded. The four unavailable-page attempts have job state `completed` but crawl status `needs_review` and `successful=false`; Allabolag has state `failed`. Dagster SUCCESS indicates the batch was processed and its outcomes persisted, not that the sites were crawled successfully.

Detailed receipts: `inspection.json`, `baseline.json`, `submission.json`, `receipt.json`, `progress.json`, and `results.json`.
