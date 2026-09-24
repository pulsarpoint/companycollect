# Automatic CAPTCHA assistance — 2026-09-19

Crawler 0.37.4 is deployed on `192.168.88.132` with
`CRAWL_CHALLENGE_AGENT_ENABLED=true`. Browser service 0.4.0 remains in place.
The latest failed Framework JetStream attempt before this change was
`backoffice-278cdcd4-4e8f-4396-942b-2c313092b6aa`; it followed the old timeout path
because the crawler had no call to the agent endpoint.

## Behavior

Automatic REST and JetStream website fetches now invoke the existing DeepSeek
agent once per crawl attempt when a CAPTCHA is detected. The agent keeps the same
browser lease and tab, with the browser API's 12-decision/120-second limit.
The crawler separately checks HTTP access, content, domain and challenge markers
before continuing. Unsuccessful assistance falls back to the 10-second human
deadline and the existing saved-failure/manual-retry workflow.

Backoffice displays `CAPTCHA agent running`, prevents a competing manual agent run
or resume, and keeps cancellation available. Agent results persist in SQLite/SSE,
Backoffice, the main result JSON at `crawl.challenge_agent`, and crawl artifacts.
Manual retries and Brave search verification retain their existing operator flow.

## Validation

- 12 automatic-assistance tests passed, including actual browser HTTP client
  calls, false model success, domain changes, API failure, cancellation, one-run
  limits, manual/disabled behavior, and portable JSON on success and failure.
- 42 service tests passed, including real local JetStream tests using
  `/tmp/company-crawl-tools/nats-server`; no skips in that run.
- 8 browser routing, 4 human-assistance and 5 collection tests passed.
- 16 Backoffice tests passed; TypeScript checking and production build passed.
- Ruff checks/formatting, Ansible syntax and `git diff --check` passed.
- Final Ansible deployment: 35 OK, 8 changed, 0 failed, 0 unreachable.

## Deployed Backoffice → JetStream → agent → crawl → S3 test

Used a controlled local checkbox fixture inside a cross-origin iframe, with the
real browser service, DeepSeek API, CDP actions, crawler and object storage.
Submitted through Backoffice's **Send to JetStream** action. No **Try CAPTCHA
agent** or **Resume crawl** action was used.

| Measurement | Result |
| --- | --- |
| Request | `automatic-agent-final-20260919` |
| JetStream stream / sequence | `COMPANY_CRAWL` / 10 |
| Agent run | `ece552cb926b45cbbc5a17c7384e02ae` |
| Agent model | `deepseek-flash` |
| Agent duration | 2.552 seconds |
| Decisions | 2: click, finish |
| Prompt / completion / total tokens | 3,918 / 94 / 4,012 |
| Agent observation | `appears_clear` |
| Independent crawler check | `accessVerified: true` |
| Crawl outcome | `finished`, one document |
| Delivery | S3 uploaded; original JSON read through Backoffice's ClickHouse mapping |

The `crawl.challenge_agent` object read from S3 exactly matched the saved job's
agent result. The S3 artifact archive was separately read and verified to contain
`human-assistance/p0001/agent-result.json`, `agent-after.html` and `agent-after.png`.
Backoffice visibly showed the saved result, timing, tokens and S3 link.

Objects are under bucket `crawls`, prefix
`company-crawls/automatic-agent-final-20260919/attempts/0001/`.
Local evidence is in `data/automatic-challenge-20260919/` (ignored runtime data).
The temporary fixture server was stopped and no browser leases remained active.

An earlier live fixture run verified activation and continuation but exposed a
missing agent result in the portable JSON. The final deployment fixes that; the
new success/failure serialization test and final S3 read verified the correction.

This validation establishes automatic orchestration using our controlled fixture.
Framework was not rerun against a real challenge during this change. Earlier
real-domain observations remain in the browser service's
[real-domain report](../browser_service/CHALLENGE_AGENT_REAL_DOMAINS_20260919.md).
