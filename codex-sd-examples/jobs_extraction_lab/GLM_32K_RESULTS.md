# Three FINN trials with a 32K output budget

Increasing GLM's output budget to 32,768 tokens did not pass the reliability check.
Two trials timed out at 180 seconds. The third returned all four jobs but omitted
`Growth / E-Commerce` from one title. No trial met the full acceptance criteria,
so the conditional seven-window and 40-page stages were not started.

## Frozen inputs and acceptance criteria

All three trials used `z-ai/glm-5.3-flash`, the same 587-character FINN Markdown
window, the current v3 prompt, and the same six examples. Temperature remained 0;
reasoning was enabled and excluded from the response text. The requested output
cap was 32,768 tokens, with a hard 180-second deadline across each request and its
possible retry delays. At most two trials ran concurrently. Each made one attempt.

The saved model settings match the original 8K GLM run except for `max_tokens`.
All three new input hashes are identical and reproduce from the frozen prompt,
schema, settings, and Markdown. The total deadline was enforced by the previously
fixed client; the original 8K run preceded that fix.

The expected values were checked against the source before any of these requests
and saved in [finn-window-expected.json](fixtures/finn-window-expected.json). The
first three jobs inherit the visible `Tech` group. The fourth inherits `Account
Management`, under the current prompt's nearest-visible-group rule. The parent
`Core Functions` group is absent from this chunk and must not be guessed from the
full page. This differs from the older whole-page Codex department reference.

A passing trial had to return valid JSON with exactly four openings, all six
fields matching those annotations, and no literal-source validation issues.
Evidence could be any valid contiguous quotation containing the title; it did
not need to match the annotation's example evidence string. Three passing trials
were required before advancing to the larger pilot.

## Results

| Trial | Outcome | Returned jobs | All six fields match | Duration | Reported provider |
| --- | --- | ---: | ---: | ---: | --- |
| 1 | Total deadline exceeded | No final output | Not evaluable | 180.005 s | Unavailable |
| 2 | Total deadline exceeded | No final output | Not evaluable | 180.029 s | Unavailable |
| 3 | Valid JSON, one title mismatch | 4/4 | 3/4 | 34.971 s | Parasail |

Trial 3 reported the requested model, `z-ai/glm-5.3-flash`, and had no source-validation
flags. Its expected title was `Senior Product Manager (m/f/x) Growth / E-Commerce`;
it returned `Senior Product Manager (m/f/x)`. All five other fields on that record
matched, as did all six fields for the other three jobs. The dropped words remained
in its evidence, which illustrates why literal-source checks cannot establish that
the title field is complete.

The successful call reported:

- 3,044 input tokens.
- 2,675 completion tokens: 2,303 reasoning tokens and 372 remaining completion tokens.
- $0.0017941 in OpenRouter charges.

No final response arrived for trials 1 and 2, so their token usage, provider IDs,
and charges are unavailable. Their costs must not be treated as zero. The sum of
known charges is $0.0017941; the total charge for all three trials is unknown.

## Interpretation

The successful request consumed fewer than 8,192 completion tokens. It therefore
does not demonstrate a need for a 32K budget. Conversely, the timed-out requests
did not report token counts, so we cannot claim they exhausted 32K tokens or spent
the whole interval reasoning. The observed conclusion is narrower: raising the
budget did not produce reliable, correct extraction within the agreed deadline.

The successful provider was Parasail, but the failed requests expose no provider
metadata. These results cannot identify a failing provider or establish that routing
caused the difference. New extraction records now preserve provider and response
IDs when OpenRouter returns them, which will help future diagnosis.

The live [model catalog](https://openrouter.ai/api/v1/models), checked after these
trials, reports GLM reasoning as mandatory, with supported efforts `max`, `high`,
and `low`, and a default effort of `max`. Our requests specify `enabled=true` and
`exclude=true` but no effort. The advertised default therefore identifies a useful
next variable; it does not independently prove the effort used inside each provider
or establish the cause of either timeout.

A separate controlled test should request `reasoning.effort="low"` while keeping
the output budget and prompt fixed. If isolating provider behavior, pin one provider
and compare explicit `max` and `low` on that same provider. Do not assume reasoning
can be disabled: the current GLM catalog says it is mandatory. OpenRouter also
explains that `exclude=true` only hides reasoning text, not its computation.
[Reasoning documentation](https://openrouter.ai/docs/guides/best-practices/reasoning-tokens),
[provider-selection documentation](https://openrouter.ai/docs/guides/routing/provider-selection).
No low-effort or provider-pinned model requests were made in this test.

## Artifacts and replay

- [Summary and audit](data/glm-32k-finn/summary.json).
- [Experiment and acceptance criteria](data/glm-32k-finn/experiment.json).
- [Source manifest](data/glm-32k-finn/manifest.json).
- Outcomes and settings: `data/glm-32k-finn/runs/glm-32k-trial-{1,2,3}/openrouter/`.
- Comparisons: `data/glm-32k-finn/glm-32k-trial-{1,2,3}-comparison.json`.
- `model-reasoning-options.json` contains the public metadata checked during diagnosis.

The comparison helper retains its historical `codex` / `only_codex` field names;
in these three comparison files those keys mean the manually checked expected
values, not a new Codex extraction. The six-field comparison normalizes case and
whitespace but preserves the meaningful title difference above.

Run from `companycollect/codex-sd-examples` to resume a saved trial:

```bash
.venv/bin/python -m jobs_extraction_lab.main run \
  --backend openrouter --openrouter-model z-ai/glm-5.3-flash \
  --data-dir jobs_extraction_lab/data/glm-32k-finn \
  --run-id glm-32k-trial-1 --max-tokens 32768 --timeout 180 --attempts 2 \
  --examples-file jobs_extraction_lab/prompt_examples.md \
  --env-file jobs_extraction_lab/.env
```

Use a fresh run ID for another measurement. Existing results, including the failed
ones, remain cached unless explicitly overwritten with `--retry-failed`. The earlier
8K runs and interrupted diagnostic were preserved. All 32 unit tests, Ruff, type
checks, and the diff check passed after adding provider/response metadata.
