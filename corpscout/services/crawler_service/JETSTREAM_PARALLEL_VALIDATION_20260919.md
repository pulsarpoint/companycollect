# Five concurrent crawl requests — 2026-09-19

The live test passed its scheduling and delivery checks: five full-mode requests
submitted from Backoffice used at most two browsers, the remaining requests waited
in JetStream, and released browsers immediately became available to the next crawl.
All five terminal results were saved to S3 and read through ClickHouse in Backoffice.
Two sites yielded collected pages; three ended with access restrictions.

## Change required before testing

The existing JetStream loop awaited each message before fetching another and its
durable consumer allowed only one unacknowledged delivery. The deployment also had
one normal worker. This serialized requests despite the browser service having two
profiles.

Crawler 0.37.3 now processes deliveries concurrently, bounded by the configured
worker count. One dispatcher fetches only available delivery slots. Each delivery
keeps its progress acknowledgements until its existing result-delivery contract is
complete. Startup updates the owned durable consumer's `max_ack_pending` to the
worker count, including for an existing consumer. Shutdown cancels its delivery
tasks together and leaves unacknowledged messages eligible for redelivery.

Deployed with Ansible to `192.168.88.132` with two normal workers; the browser
service remained at 0.3.0 with two profiles. The deployment completed with no
failed or unreachable hosts. Browser ownership remains in SQLite; no allocation
queue was added to the browser service.

## Live requests and outcomes

Batch: `parallel-full-20260919-1789822026`.

All five requests were submitted through **Crawler → New test crawl → Send to
JetStream** within approximately three seconds. Each used `crawl: full`,
`save_artifacts: true`, and `config: {max_pages: 5, web_search: false}`. This
exercises full discovery with a five-page limit, not an exhaustive website crawl.
Request IDs append the slug below to the batch ID.

| Slug / domain | Stream sequence | Crawl outcome | Saved pages | Attempt duration | LLM tokens |
| --- | ---: | --- | ---: | ---: | ---: |
| novelic / novelic.com | 3 | Partial: page budget reached | 5 | 110 s | 81,331 |
| melexis / melexis.com | 4 | Failed: CAPTCHA assistance timeout | 0 | 16 s | 0 |
| infobip / infobip.com | 5 | Partial: page budget reached | 5 | 149 s | 171,989 |
| mikroe / mikroe.com | 6 | Needs review: initial page unavailable | 0 | 3 s | 0 |
| framework / frame.work | 7 | Failed: CAPTCHA assistance timeout | 0 | 15 s | 0 |

Durations use service attempt start/finish timestamps and exclude time waiting
for initial JetStream delivery. Token totals are reported prompt plus completion
tokens, including cached input; the model was `deepseek-flash`. Pricing was not
reported, so zero known cost must not be interpreted as a free run.

Melexis and Framework exhausted the automatic human-assistance grace period and
released their profiles. No human verification or manual retry was performed.
MikroE's stored page record reports HTTP 403 with
`Fetch returned HTTP 403; Access denied by robots.txt`; its result contains no
collected HTML documents. Its service state is `completed` because a result was
produced, while its crawl outcome is `needs_review`.

## Capacity and recovery evidence

The monitor collected 158 snapshots from 12:48:15 to 12:51:23 UTC. Checks on every
sample asserted at most two active assignments, no duplicate profile ownership,
and no browser-side queued assignment.

- Peak active browser assignments: **2**.
- Peak unacknowledged JetStream deliveries: **2**.
- Peak requests still pending in JetStream: **3**.
- Final pending and unacknowledged requests: **0 / 0**.
- Consumer delivered stream sequence and acknowledgement floor: **7 / 7**,
  advancing from **2 / 2** before the batch.
- Five unique request IDs, each with one attempt and source `jetstream`.
- No redelivery observed; the consumer sequence advanced by exactly five.
- All five browser assignments ended in `released`.

The stored lease intervals additionally prove that each profile was reused only
after the previous lease ended, beyond what periodic sampling alone establishes:

| Browser | Assignment order | Time from prior release to next claim |
| --- | --- | --- |
| browser-1 | Melexis → Infobip | 0.079 s |
| browser-2 | Novelic → MikroE → Framework | 0.380 s, then 0.060 s |

The batch reached its final crawl outcome approximately 166 seconds after the
first attempt started. By 12:51:23 UTC, all S3 results were readable through
ClickHouse, JetStream had drained, and both browser profiles were free.

## Stored results and Backoffice verification

Each attempt has `result.json.gz` and `artifacts.tar.gz` under:

```text
s3://crawls/company-crawls/<request_id>/attempts/0001/
```

All five JSON results were downloaded through Backoffice's
`/admin/crawls/result.json?path=<bucket/key>` route, which reads the S3 mapping in
ClickHouse. Backoffice displayed all five final attempt rows and an **S3 saved ·
View** link on every row. Both CAPTCHA failures offered **Retry interactively**.

An existing presentation issue remains: completed rows retain `Fetching page` as
their last status reason, and the top-level completed state does not distinguish
MikroE's review-needed result from the two partial results. The saved JSON provides
the precise crawl outcome. This test did not change terminal status presentation.

Local evidence is under `data/jetstream-parallel-20260919/` (ignored runtime data):

- `manifest.json`: submitted payloads and Backoffice stream receipts.
- `timeline.jsonl`: broker, browser ownership and crawl status samples.
- `summary.json`: final counters, outcomes, usage and S3 object references.
- `results/<request_id>.json`: original results read through ClickHouse.

## Automated checks

- Real JetStream suite: **9 tests passed**.
- JetStream plus S3 delivery suite: **16 tests passed**, including inherited
  JetStream cases. These suites overlap; they are not 25 distinct cases.
- New cases cover five requests with two workers and three pending, progress
  acknowledgements beyond `ack_wait`, concurrent shutdown/restart, and simultaneous
  duplicate messages sharing one crawl, attempt and completion event.
- Existing delivery cases cover upload/publish failures, slow uploads, lost
  acknowledgements and reuse of saved results after restart.
- Ruff checks and formatting passed for the changed Python files.

Commands used from this service directory:

```sh
NATS_SERVER=/tmp/company-crawl-tools/nats-server .venv/bin/python -m unittest discover -s tests -p 'test_service_nats.py' -v
NATS_SERVER=/tmp/company-crawl-tools/nats-server .venv/bin/python -m unittest discover -s tests -p 'test_service_delivery.py' -v
```
