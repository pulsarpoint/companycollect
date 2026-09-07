# GLM 5.3 Flash versus Liquid on the same extraction pilot

`z-ai/glm-5.3-flash` matched the Codex reference on all six fields for every job
returned by its six successful windows. One window failed by exhausting the output
budget entirely on reasoning. The original pass therefore completed 6/7 windows
and matched 20/24 reference job observations, not 24/24.

## Comparison setup

The GLM run is `glm-5.3-flash-prompt-v3`; the existing Liquid run is
`liquid-explained-prompt-v3`. Both use the seven frozen windows in
`data/few-shot-pilot-v1/manifest.json`: six previously inspected job boards, 24 job
observations, and 23 distinct job URLs because one Calendly opening overlaps.

Saved settings were checked to differ only in `requested_model`. Both receive the
rewritten v3 instructions, the same six teaching examples, the same schema, and the
same Markdown. Both request strict JSON schema, temperature 0, reasoning enabled
with reasoning text excluded, and an 8,192-token maximum output. Stored input hashes
and Markdown hashes were verified for every outcome. These are direct OpenRouter
requests; there were no new crawls, Liquid requests, or Codex requests.

GLM was explicitly selected using the new `--openrouter-model` option. Every GLM
outcome reports `z-ai/glm-5.3-flash` as its actual model. The first window was a
compatibility check and was reused from cache when running the remaining six, so
the original pass made seven API requests in total. No automatic retry was needed
for transport or HTTP failures.

The reference remains the existing whole-page Codex extraction, restricted to each
window's observed job URLs. The scores measure agreement with that reference, not
independently established extraction accuracy. Missing responses remain failures;
their jobs stay in the denominator. Source validation checks literal presence and
cannot establish correct associations or complete recall.

## Original pass results

| Measure | Liquid LFM | GLM 5.3 Flash |
| --- | ---: | ---: |
| Successful windows | 7/7 | 6/7 |
| Reference job observations found | 24/24 | 20/24 |
| Title agreement | 13/24 | 20/24 |
| Location agreement | 22/24 | 20/24 |
| Department agreement | 17/24 | 20/24 |
| Employment type agreement | 24/24 | 20/24 |
| Workplace type agreement | 23/24 | 20/24 |
| URL agreement | 24/24 | 20/24 |
| All six fields agree | 7/24 | 20/24 |
| Literal-source validation flags | 0 | 0 |
| Input tokens | 22,353 | 21,899 |
| Output tokens, including reasoning | 22,545 | 14,263 |
| Reasoning tokens | 20,095 | 12,687 |
| Summed request seconds | 100.5 | 152.1 |
| OpenRouter reported charge | $0 | $0.01041635 |

GLM's 20 returned jobs all matched all six fields; that conditional 20/20 result
does not include the four jobs unavailable because of its failed window. Summed
request duration is not wall time. The reported GLM charge includes the failed
request; it is the recorded charge, not an estimate from advertised pricing.

| Window | Reference jobs | Liquid: all six fields | GLM: all six fields |
| --- | ---: | ---: | ---: |
| `ashby-zed--w001` | 2 | 2 | 2 |
| `ashby-steel--w001` | 2 | 0 | 2 |
| `ashby-hedra--w001` | 4 | 4 | 4 |
| `greenhouse-circleci--w001` | 4 | 1 | 4 |
| `greenhouse-calendly--w001` | 4 | 0 | 4 |
| `greenhouse-calendly--w002` | 4 | 0 | 4 |
| `lever-finn--w002` | 4 | 0 | Failed |

GLM separated the Steel titles from the repeated Engineering department and kept
the Calendly remote-location text out of job titles. It also preserved CircleCI's
URL query parameters and assigned its department headings correctly. These are
specific improvements over the current Liquid output on the same saved inputs.

## Output-budget failure

FINN returned `finish_reason=length` after 83.235 seconds. OpenRouter reported all
8,192 completion tokens as reasoning tokens and supplied no final JSON. The request
cost $0.0045526. It failed despite the input containing only four linked openings;
small input windows do not guarantee bounded reasoning output.

The incomplete response was not salvaged or counted as an empty successful list.
The original outcome is preserved. A separate diagnostic retry uses the identical
model, prompt, examples, schema, and output limit on this one window only; its
result is reported separately below and does not replace the original benchmark.

The diagnostic retry did not produce a final response after more than nine minutes
despite a configured HTTP timeout of 180 seconds. It was interrupted, with the
interruption recorded in `data/few-shot-pilot-v1/finn-retry/diagnostic-timeout.json`.
No extraction, usage, or charge was received for it, so its cost is unknown and is
not included in the $0.01041635 original-pass charge. This does not establish another
output-limit failure; it is an interrupted request without a final response.

The client previously relied on an HTTP read timeout, which does not bound total
duration while a connection remains active. It now also applies a total deadline
across the request and retry delays. The fix was tested with a request that never
finishes, verifying that it is cancelled and recorded as a timeout.

## Clarification of the 8K budget

The original comparison explicitly requested 8,192 output tokens for both models.
It was a benchmark setting, not evidence of GLM's native output limit. The CLI had
also rejected larger values, and that universal ceiling has now been removed.
The default remains 8,192 for compatibility with Liquid; users can choose a larger
value with `--max-tokens`, subject to the selected provider's limits.

After the user questioned the limit, live metadata was checked on 2026-09-05:

- The exact [Liquid free endpoint](https://openrouter.ai/liquid/lfm-2.5-2.6b:free)
  advertises 65,536 context tokens and 8,192 maximum completion tokens. This is an
  endpoint restriction, not a claim about every deployment of the underlying model.
- [GLM endpoint metadata](https://openrouter.ai/api/v1/models/z-ai/glm-5.3-flash/endpoints)
  advertises different completion limits by provider. For example, DeepInfra lists
  131,072 completion tokens with a 1,048,576-token context. Other providers differ;
  neither 8K nor a single larger value should be treated as universal for GLM.

The exact catalog responses are saved as `liquid-model-endpoints.json` and
`glm-model-endpoints.json` in the pilot directory. The original-pass results in this
report use the 8K budget. The subsequent larger-budget live experiment is documented
separately in [GLM_32K_RESULTS.md](GLM_32K_RESULTS.md).

## Artifacts and replay

- [Detailed original comparison](data/few-shot-pilot-v1/glm-comparison.json) includes
  settings, hash checks, per-field disagreements, and failure accounting.
- GLM outcomes: `data/few-shot-pilot-v1/runs/glm-5.3-flash-prompt-v3/openrouter/`.
- Liquid outcomes: `data/few-shot-pilot-v1/runs/liquid-explained-prompt-v3/openrouter/`.
- FINN diagnostic inputs/results: `data/few-shot-pilot-v1/finn-retry/`.
- [README command](README.md#compare-glm-53-flash-with-liquid) shows model selection
  and the complete reproducible extraction command. Use a fresh run ID for another
  full pass. The saved first pass retains its failure when resumed without
  `--retry-failed`.

The default model remains Liquid. Requests and bounded HTTP/transport retries use
the selected model without changing it. Tests cover explicit model selection,
strict schema requests, unchanged model selection on retries, and rejecting a
changed model in an existing saved run. Tests also cover larger output budgets and
the total timeout. All 32 tests, Ruff, and type checks pass.

These selected development cases support further testing of GLM, but do not establish
general accuracy or repeatability. The same output-budget cap was used deliberately;
a reasoning-budget experiment would be a separate comparison.
