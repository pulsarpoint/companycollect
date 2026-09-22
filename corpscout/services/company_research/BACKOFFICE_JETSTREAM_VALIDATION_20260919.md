# Backoffice JetStream submission — 19 September 2026

Crawler 0.37.1 was deployed to `192.168.88.132:8080` with Ansible
(35 successful tasks, 8 changed, no failures). Independent browser service 0.2.2
remained running on port 8081. The crawler environment contains neither Crawl4AI
nor Playwright.

## Request path

Backoffice **Crawler → New test crawl** validates an editable JSON request through
authenticated `POST /v1/crawls/validate`, then publishes from its server directly to
JetStream subject `company.crawl.requests`, expecting stream `COMPANY_CRAWL`.
Validation does not submit REST work, write a job, or allocate a browser.
Backoffice displays the persistence receipt before the ordinary worker consumes
the message. The input source in crawl history is `jetstream`.

Test publishing was enabled in the ignored local Backoffice `.env`; NATS credentials
remain server-side. Stream configuration is unchanged. Retries of identical
normalized payloads use a stable message ID, and the crawler retains durable
request-ID deduplication.

## Live UI test

An invalid `config.max_pages: 0` was rejected in the dialog with a field-specific
message. The same draft was corrected and submitted through the actual browser UI:

```json
{
  "request_id": "backoffice-novelic-full-20260919-1789813689966",
  "url": "https://www.novelic.com/",
  "crawl": "full",
  "save_artifacts": true,
  "config": {"max_pages": 5, "web_search": false}
}
```

- Persistence receipt: `COMPANY_CRAWL`, sequence 1.
- Worker source: `jetstream`; one attempt.
- Browser assignment: `browser-2`, lease `7b67ff2981bb4603803bf177591c6b64`.
- Started `10:28:30 UTC`, finished `10:30:00 UTC`: 90 seconds.
- Service state `completed`; crawl status `partial`, stop reason `page_budget`.
- Five pages: homepage, contact, about us, ACAM product page, ASPER product page.
- Five LLM calls: 64,515 prompt tokens and 16,504 completion tokens.
- No CAPTCHA or operator intervention needed for this run.
- JSON and HTML artifacts uploaded to S3; browser lease subsequently `released`.
- Consumer pending 0, acknowledgement pending 0, stream acknowledgement floor 1.
- Backoffice live history visibly showed `completed`, `jetstream`, and `S3 saved`.

S3 objects in bucket `crawls`:

```text
company-crawls/backoffice-novelic-full-20260919-1789813689966/attempts/0001/result.json.gz
company-crawls/backoffice-novelic-full-20260919-1789813689966/attempts/0001/artifacts.tar.gz
```

Local evidence is in ignored `data/backoffice-jetstream-20260919/`:
`status.json`, `result.json`, and `validation.json`. This was a bounded integration
test with web search disabled, not an exhaustive company discovery benchmark.

## Automated checks

- Backoffice: 13 focused tests passed, including a real authenticated JetStream
  broker for persistence, deduplication, wrong-stream rejection, validation errors,
  and credential-safe failures; route tests cover same-origin submission controls.
- TypeScript check and production build passed.
- Crawler service tests: 8 passed, including authentication and side-effect-free
  request validation with config override preservation.
- Crawler real JetStream tests: 6 passed, including full discovery, progress acks,
  duplicate requests, lost-ack redelivery, invalid input, and restart recovery.
  Their page-acquisition fixtures were updated for the existing external-browser
  client argument and to allow the browser HTTP fixture through model stubs.
- Ruff, Python type check for the changed API, lockfile check, and diff whitespace
  checks passed.

The local Backoffice development server remains on `http://127.0.0.1:5183`.
