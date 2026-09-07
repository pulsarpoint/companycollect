# GLM with low reasoning effort

Explicit low reasoning effort with Together passed the repeated FINN check and
the seven-window pilot, but the 40-page experiment remained incomplete because
of provider failures. After bounded recovery, 184/228 windows succeeded and merged
into 542 jobs. Of the 517 jobs shared with the existing Codex reference, 419 (81.0%)
agreed on all six fields. This is reference agreement, not verified accuracy.

The experiment supports continuing with direct API extraction, while exposing
problems in provider availability, flattened Markdown, and conflict resolution.
It does not establish equivalent performance for the 2.6B Liquid model.

## Configuration and progression

All new requests used `z-ai/glm-5.3-flash`, temperature 0, the current v3
instructions, the same six teaching examples, strict JSON schema, a 32,768-token
output budget, and a hard 180-second deadline including retry delays. Reasoning
was requested as `{"enabled": true, "exclude": true, "effort": "low"}`.
At most two attempts were allowed per outcome.

The first stage changed only the explicit reasoning effort relative to the prior
32K experiment. It retained automatic provider routing. Each FINN trial had to
return four jobs, agree with the frozen manual annotations on all six fields,
and pass literal-source validation. All three trials had to pass before advancing.

| Stage | Valid outcomes | Complete field agreement | Observed result |
| --- | ---: | ---: | --- |
| Low, automatic routing: three FINN trials | 3/3 | 2/3 trials | Together passed twice; Wafer omitted a title qualifier |
| Low, Together only: three new FINN trials | 3/3 | 3/3 trials | All 12 job observations matched the manual reference |
| Low, Together only: seven-window pilot | 7/7 | 24/24 job observations | No literal-source flags |
| Full 40 pages, original pass | 183/228 | 415/513 matched jobs | 45 failed windows |
| Original pass plus bounded recovery | 184/228 | 419/517 matched jobs | 44 failed windows remain |

Automatic routing did not pass the original gate. The subsequent provider-pinned
stage was a separate diagnostic with fresh outputs, not a relaxed acceptance test.
It added `provider.only=["together"]` and `allow_fallbacks=false`, retaining
`require_parameters=true`. It passed the same gate before the larger stages began.

The pilot contains 24 observations of 23 distinct jobs because one job appears in
two overlapping windows. Its expected values combine 20 observations from the old
Codex reference and four manually annotated FINN observations. It is not a fully
independent gold set, and these previously inspected windows are a development
pilot rather than an unseen test set.

The full run made fresh requests for all 228 existing windows. No pilot responses
were reused, no pages were recrawled, and no new Codex or Liquid calls were made.
Windows retain the existing four-job target, one-job overlap, and 3,000-character
target. Each outcome preserves settings, hashes, usage, and provider/response IDs
when available. Input hashes were independently reproduced for all 248 new outcomes,
including the seven recovery outcomes.

## FINN stability and provider observations

With automatic routing, Together returned all four jobs correctly in 3.349 and
3.378 seconds. Wafer took 56.412 seconds and shortened
`Senior Product Manager (m/f/x) Growth / E-Commerce` to
`Senior Product Manager (m/f/x)`. Its source checks still passed because the shorter
title occurs in the source. Substring validation does not establish completeness.

The three new Together-only trials took 9.166, 9.576, and 3.465 seconds. All six
fields remained correct across the three trials. Reported completion tokens were
378, 411, and 378, including 0, 33, and 0 reported reasoning tokens respectively.
The seven-window pilot then reported 1,954 completion tokens and zero reasoning
tokens, with 48.264 summed call-seconds.

These observations show that the requested configuration can return correct output
quickly. They do not prove that reasoning effort alone caused the improvement:
providers differed in the earlier experiment, and the repeat sample is small.
Reported zero reasoning tokens also does not prove the absence of hidden internal
computation. `exclude=true` hides reasoning text rather than disabling reasoning.
[OpenRouter reasoning documentation](https://openrouter.ai/docs/guides/best-practices/reasoning-tokens),
[provider routing documentation](https://openrouter.ai/docs/guides/routing/provider-selection).

## Full-corpus reliability and recovery

The original full run used concurrency 2 and completed in approximately 15 minutes:

- 183 successful windows; every successful response reported Together.
- 41 HTTP 429 outcomes attributed to Together's upstream shared provider pool.
- Two 180-second total-deadline failures with no final usage or provider metadata.
- Two responses ending with `finish_reason=error`, including partial JSON.
- No `finish_reason=length` failures. The largest reported completion was 674 tokens.

The successful calls had a median duration of 3.972 seconds, a 95th-percentile
duration of 14.347 seconds, and a maximum of 38.281 seconds. These latency figures
exclude failures. The unknown token counts on timeout outcomes prevent a claim
about what they consumed. Raising the output budget further is not supported by
the completed calls in this configuration.

Recovery preserved the original directory. One serial canary still received HTTP
429. A later canary succeeded after a cooldown, so a five-window serial batch was
attempted. All five returned HTTP 429 despite bounded retries. Further batches
were stopped. Lower concurrency therefore did not resolve the observed outage.

The recovered directory combines unchanged original successes with that one
successful canary. It is not an independent full repeat. Replacement provenance,
all failed attempts, and the remaining 44 failed windows are retained. No provider
fallback was introduced to make the benchmark appear complete.

## Merged output and reference agreement

| Measure | Original pass | After bounded recovery |
| --- | ---: | ---: |
| Successful windows | 183/228 | 184/228 |
| Pages with every window successful | 16/40 | 16/40 |
| Merged jobs | 538 | 542 |
| Jobs matched to the 620-job Codex reference | 513 | 517 |
| Matches agreeing on all six fields | 415 | 419 |
| Jobs with conflicting overlap predictions | 21 | 21 |
| Merged literal-source flags | 4 | 4 |
| Rejected predictions with unrecognized/missing URLs | 0 | 0 |
| Jobs recovered by another overlapping window | 43 | 44 |

Of the 542 merged jobs, 25 are on Modal, whose earlier Codex extraction failed;
those jobs have no Codex comparison. The recovered output contains 517/620 reference
openings. Missing reference jobs largely coincide with failed windows, so this is
not a clean measurement of extraction recall. The 112 unreturned observed links
also include links that may be excluded by the extraction policy; they are not
112 independently verified missed jobs.

Among the 517 matched jobs, agreement was:

| Field | Equal values |
| --- | ---: |
| Title | 502/517 |
| Location | 507/517 |
| Department | 451/517 |
| Employment type | 517/517 |
| Workplace type | 503/517 |
| Job URL | 517/517 |

Comparison normalizes case and whitespace. URL matching also normalizes the scheme,
host, trailing slash, and fragment. Evidence is validated separately and is not one
of these six fields. All-six agreement is 419/517 among returned matches, or
419/620 when missing reference jobs remain in the denominator.

For a comparison holding job identity constant, the original GLM pass and the
historical segmented Liquid run both returned 511 Codex-reference jobs. GLM agreed
on all six fields for 413; Liquid for 30. Title agreement was 496 versus 53.
However, that Liquid run used the older prompt and different budget/routing;
this compares pipeline configurations, not the model alone. The existing
same-prompt seven-window Liquid pilot matched all six fields on 7/24 observations,
while this GLM pilot matched 24/24.

## Source checks and concrete improvements

Selected discrepancies were checked against the frozen Markdown and saved HTML.
This is a targeted review, not a complete manual annotation of the corpus.

- **Preserve field boundaries before extraction.** Resend's saved HTML separates
  `Security Engineer, Platform` from department `Engineering`; flattened Markdown
  allowed GLM to append the department to the title. Webflow similarly lost the
  boundary before `CA Remote`, and GLM moved `CA` into a title. Algolia's separate
  `New` badge became part of a title. A card-aware Markdown conversion could retain
  title, metadata, and badge boundaries using the HTML already collected.
- **Keep organizational context explicit.** Encord headings flatten to strings
  such as `GTMSalesAccount Executive`; GLM reconstructed `GTM Sales`, generating
  literal-source flags and an ambiguous group choice. FINN's `Tech` versus Codex's
  `Core Functions` is different: the parent group is absent from the window, so
  `Tech` follows the current nearest-visible-group rule. Preserve the hierarchy
  when chunking and define whether the schema means department or team.
- **Resolve overlap conflicts against the job's source.** Granola's `AI Engineer`
  has `London` in both windows. One extraction returned null, the other `London`;
  the current whole-record tie-breaker kept the earlier null. A source-supported
  field resolution would recover this value. Simply choosing any non-null value
  would still permit incorrect associations.
- **Specify page-level inheritance.** Letta states `Fully in-person` in policy
  prose. Codex applied it to the jobs, while GLM returned null. The current prompt
  should state how global work policies apply before treating this as an error.
- **Separate service recovery from extraction quality.** Use a paced queue with
  cooldown and a stop condition for sustained upstream rate limits. Evaluate any
  alternative provider as a separate configuration, retaining provider provenance.

Overlap recovered missing outputs, but its 21 conflicts still require review.
The four final source flags are not the only errors: valid substrings can still be
incomplete titles or belong to the wrong field. The detailed source review is
saved alongside the metrics. No prompt or merge selection rule was changed during
this experiment.

The next useful extraction test is to preserve card boundaries and heading context
from the saved HTML, freeze an independently checked sample spanning these failure
types, then compare Liquid and GLM on identical new inputs. That tests the small
model hypothesis more directly than increasing the output cap again.

## Cost and artifacts

| New stage | Known reported charge |
| --- | ---: |
| Automatic-routing low-effort trials | $0.00254655 |
| Together-only FINN trials | $0.00195330 |
| Together-only seven-window pilot | $0.00426185 |
| Original full run | $0.11851280 |
| Recovery, including both canaries | $0.00064395 |
| **Total known charge** | **$0.12791845** |

Forty-nine of the 248 new outcomes have no usage response. Charges for those
outcomes and any unreported intermediate attempts are unknown, not zero. The table
sums reported `usage.cost`, not internal upstream cost estimates; copied outcomes
in the recovered directory are not charged a second time in this accounting.

- [Automatic-routing trials](data/glm-32k-low-finn/summary.json).
- [Together-only FINN trials](data/glm-32k-low-together/summary.json).
- [Seven-window pilot comparison](data/glm-32k-low-together/pilot-comparison.json).
- [Original full result](data/segmented-v1/runs/glm-32k-low-together-full/segmented-comparison.md).
- [Recovered result](data/segmented-v1/runs/glm-32k-low-together-recovered/segmented-comparison.md).
- [Recovery accounting and provenance](data/glm-32k-low-together-recovery/summary.json).
- [Field and latency analysis](data/glm-32k-low-together/full-corpus-analysis.json).
- [Selected source review](data/glm-32k-low-together/source-review.json).
- [Cost and input-hash audit](data/glm-32k-low-together/experiment-audit.json).

The lab CLI now supports `--reasoning-effort` and `--openrouter-provider`.
Omitting them preserves the previous default behavior; changing them invalidates
the saved-run cache. The generic segmented-report title now works for either model.
The 32 lab tests, Ruff, and type checks passed for the implementation changes;
the final diff check and all 248 input-hash checks also passed. See the
[README](README.md) for the exact full-run command and replay behavior.
