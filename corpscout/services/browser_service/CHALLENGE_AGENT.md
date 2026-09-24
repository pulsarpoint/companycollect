# CAPTCHA assistance agent

See the [deployment and validation report](CHALLENGE_AGENT_VALIDATION_20260919.md)
for the live model, browser and Backoffice checks, and the
[real-domain report](CHALLENGE_AGENT_REAL_DOMAINS_20260919.md) for the Melexis and
Framework challenge results and subsequent crawl outcomes.

An operator can ask DeepSeek V4.1 Flash or GLM-5.3 Flash to attempt the visible challenge in an
existing browser session. The API model name is `deepseek-flash`, as documented by
[DeepSeek](https://api-docs.deepseek.com/guides/vision/).

The agent runs inside the browser service and attaches a CDP session to the selected
tab. It keeps the same Chromium process, profile, cookies, tab and SQLite lease.
CDP ports stay private. There is no new browser pool or agent framework.

## Backoffice

Crawler 0.37.4 can also invoke this endpoint automatically for website CAPTCHAs
on REST and JetStream attempts. See the crawler's
[configuration and automatic continuation checks](../crawler_service/HUMAN_ASSISTANCE.md#automatic-captcha-assistance).
The browser endpoint still only returns its observation; the crawler decides
whether the page is accessible. The manual Backoffice flow below remains available.

1. For a failed automatic attempt, choose **Retry interactively**. For a paused
   search, choose **Start verification**.
2. Open the waiting browser and choose **Try CAPTCHA agent**. This authorizes
   sending this tab's screenshots to DeepSeek and interacting with its challenge.
3. Watch through the existing noVNC desktop. Avoid manual input while the agent
   runs. The separate **Cancel** control can still cancel the crawl.
4. Inspect the agent outcome and actions. Choose **Resume crawl** to have the
   crawler check actual access; the agent never resumes or marks a crawl successful.

The default budget is 12 model decisions and 120 seconds. Each decision receives
the latest viewport screenshot plus prior actions. Allowed actions are click,
scroll, type a CAPTCHA answer, press Enter/Tab/Backspace/Escape, wait, and finish.
The model cannot call arbitrary JavaScript, shell, cookie/storage APIs, URL
navigation or browser-management tools.

The prompt restricts the agent to the challenge and asks it to stop for logins,
credentials, legal acceptance, browser security warnings and uncertain cases.
Page content is treated as untrusted observation. This is a visual heuristic,
not a guarantee of CAPTCHA completion or a substitute for the crawler's check.

`appears_clear` means the model observed normal content or a success indication.
Other terminal outcomes are `needs_human`, `step_limit`, `timeout`, `interrupted`,
`cancelled`, and `error`. Invalid actions, out-of-viewport coordinates, changed
URLs while deciding, changed origins, ended leases and restarted browsers stop
execution. Another agent request while the browser is busy returns 409.
Heartbeats continue and other profiles stay usable.

Cancelling the crawl ends its lease; the agent checks it before every next action.
A pending model call may take up to its 45-second HTTP timeout before noticing the
ended lease. Disconnecting the HTTP caller alone does not guarantee cancellation;
the server's overall time budget still applies.

## Configuration and HTTP API

Set `DEEPSEEK` on the browser service. Ansible accepts
`browser_service_deepseek_api_key` in its ignored `secrets.yml`, or `DEEPSEEK` from
the controller environment. Without a key, the agent endpoint returns 503; normal
browser operation is unaffected. Credentials are not saved in run evidence.

Read the live session and capture the selected tab first. Send the observed URL
and generation so stale requests are rejected before a model call:

```http
POST /v1/browser/sessions/<session-id>/tabs/site/challenge-agent
Authorization: Bearer <browser-service-token>
Content-Type: application/json

{
  "confirm": true,
  "expectedUrl": "https://www.example.com/",
  "expectedGeneration": "<generation-from-live-session>",
  "maxSteps": 12,
  "timeoutSeconds": 120
}
```

Use `search` instead of `site` for the existing search tab. The endpoint is also
available to command-line HTTP clients. It does not create, navigate or release
a session. `confirm: true` is mandatory, steps are capped at 20 and duration at
180 seconds. The request waits for the result; use an HTTP timeout above the
configured duration. The normal browser-service bearer token is required.

Runs are saved under `/var/lib/browser-service/challenge-runs/<run-id>/`, with
`result.json` and numbered PNG screenshots. JSON records timestamps, state,
proposed/executed actions, usage and screenshot names. Typed text is redacted from
the action journal; screenshots may contain what was visible on the page. Files
are private to the service account. These diagnostics remain local to the browser
service and are not included in crawler S3 artifacts. There is no automatic
deletion policy yet.

## Tests

`test_challenge_agent.py` exercises limits, invalid model output, provider errors,
cancellation and stale decisions. `test_browser_api.py` checks authentication,
approval, URL/generation matching, serialization and retaining the lease.
Backoffice tests cover current-lease/tab selection, same-origin actions and
leaving crawl confirmation separate.

The Linux integration test uses our own checkbox page inside a cross-origin
iframe and a real Chromium/Xvfb/CDP session:

```sh
COMPANY_RESEARCH_XVFB_TEST=1 uv run python -m unittest discover -s tests -p 'test_challenge_agent_native.py' -v
```

Set `CHALLENGE_AGENT_LIVE_MODEL_TEST=1` and `DEEPSEEK` as well to exercise the actual
model against that fixture. Default tests use a model HTTP fixture and make no
paid calls. This test does not claim success against third-party challenges.

### GLM-5.3 Flash (0.5.0)

The endpoint accepts `model: "z-ai/glm-5.3-flash"` to use the multimodal
[GLM model through OpenRouter](https://openrouter.ai/z-ai/glm-5.3-flash).
The default remains `deepseek-flash`. Set `OPENROUTER_API_KEY` on the browser service
or `browser_service_openrouter_api_key` in the ignored Ansible secrets file.
No provider substitution is performed if a requested model fails.
Both models receive the same screenshot, action schema, action limits and time limit.
DeepSeek reasoning is disabled. GLM requires reasoning, so it uses low effort and
a 4096-token response allowance (DeepSeek: 1024). Evidence records this difference. Run evidence records the actual
requested model and reported token usage. Crawler requests can select the model
with `challenge_agent_model`; the Backoffice retry form exposes the same choice.
