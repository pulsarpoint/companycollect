# Native Crawl4AI HTML: validation, one retry, and repeatability

**Follow-up:** [Partial retention and generic HTML chunks](HTML_WINDOWS_RESULTS.md)
tests preserving individually valid records and splitting the same native HTML
without platform selectors. It includes a fresh full-page control.

Two fresh 40-page DeepSeek runs are complete. One correction attempt recovered
**3 of 5 pages with output-validation failures**, but did not make extraction
consistently complete. The final results were **624/650 correct titles and URLs
(96.0%)** in run 1 and **575/650 (88.5%)** in run 2. Run 2 also had three transport
or deadline failures, accounting for 28 expected jobs.

The experiment exposed two separate limitations: valid JSON can omit most of a
page, and rejecting an entire page for one persistent bad URL discards its valid
records. Validation and a full-page correction prompt alone are not sufficient
for reliable extraction.

## Results across all 40 pages

Both runs use the same 650-job reference catalog as the
[native HTML/Markdown experiment](NATIVE_CRAWL4AI_RESULTS.md). Four reviewed
general-interest/talent-pool entries are excluded. Failed pages and omitted jobs
remain in the denominator.

| Metric | Run 1 | Run 2 |
| --- | ---: | ---: |
| Pages attempted | 40 | 40 |
| Extraction calls, including correction calls | 41 | 44 |
| First responses with valid JSON/schema | 40/40 | 36/40 |
| First responses passing all validation | 39/40 | 33/40 |
| Pages receiving a correction attempt | 1 | 4 |
| Pages recovered by correction | 1 | 2 |
| Final pages passing all validation | 40/40 | 35/40 |
| Correct title + URL in initial schema-valid responses | 623/650 (95.8%) | 578/650 (88.9%) |
| Correct title + URL in final accepted outputs | 624/650 (96.0%) | 575/650 (88.5%) |
| Final returned records with expected URLs | 626 | 577 |
| Final unexpected URLs or duplicate records | 0 | 0 |
| Final altered titles among correctly linked jobs | 2 | 2 |

Initial title scoring includes correctly linked records from a schema-valid page
even when another URL on that page fails validation. Final scoring uses only
whole pages accepted by the implemented gate. This distinction explains why
run 2's final coverage is slightly lower despite two successful corrections.

In run 1, 24 omitted Braintrust jobs and two altered Browserbase titles explain
all 26 misses. In run 2, rejected Modal and Poolside pages account for 45 jobs;
transport/deadline failures account for 28; and two Browserbase title changes
account for the remaining two misses.

The original cleaned-HTML baseline scored 573/650 without correction calls.
Do not attribute the difference from that baseline to retries: run 1 already
scored 623/650 on its first responses. Within the new runs, correction recovered
41 additional correctly linked titles, while the final gate discarded 43 correct
records on two persistently invalid pages. Combined coverage changed from
1,201/1,300 in initial schema-valid responses to 1,199/1,300 in final accepted output.

## What each correction did

| Run | Page | Initial problem | Correction outcome | Correct title + URL before → final accepted |
| --- | --- | --- | --- | ---: |
| 1 | Runpod | One URL absent from page links | Recovered | 22 → 23 |
| 2 | Modal | One shortened URL | Repeated the same shortened URL; page rejected | 29 → 0 |
| 2 | Dust | Missing required field and extra field | Recovered all 24 records | 0 → 24 |
| 2 | Mintlify | 16 malformed URL schemes, including `https:://` and `https：//` | Recovered | 4 → 20 |
| 2 | Poolside | One damaged URL identifier | Replaced the identifier with an ellipsis; page rejected | 14 → 0 |

These are complete-page correction calls, not isolated record repairs. The
original response, specific validation errors, and unchanged original HTML are
included in the correction prompt. The failed Modal and Poolside corrections
demonstrate that simply pointing out a bad URL does not reliably make the model
copy it correctly. No third model attempt, manual repair, or automatic URL rewrite
was applied.

Run 2's separate service failures were Steel (connection timeout after two HTTP
attempts; two jobs), Apify (180-second total deadline; 12 jobs), and Tavus
(180-second total deadline; 14 jobs). Service errors follow the original transport
retry policy and do not trigger an additional output-correction request. These
failures do not establish a model extraction error or identify the root cause of
the network/provider delay.

## Completeness and repeatability

Braintrust is the clearest completeness failure. Run 1 returned only
`Cloud Infrastructure Engineer` and `Design Engineer`, omitting 24 other captured
jobs. The response had valid JSON, valid page URLs, and `finish_reason: stop`.
It used 4,956 completion tokens against a 32,768-token budget. It did not hit the
output limit, and the validator had no source-independent job count with which
to detect the omission. Run 2 returned all 26 jobs from the same HTML.

Both runs also removed the city suffixes from Browserbase's
`Software Engineer (Dashboard) - New York` and corresponding San Francisco title.
Schema and link membership checks cannot catch those title edits.

On the **35 pages accepted in both runs**, there are **577 expected jobs**:

| Shared-page metric | Run 1 | Run 2 |
| --- | ---: | ---: |
| Correct URL | 553/577 | 577/577 |
| Correct title + URL | 551/577 (95.5%) | 575/577 (99.7%) |

The 24-job difference is Braintrust. Among the **553 jobs returned in both runs**,
**519 (93.9%)** have the same six normalized fields: title, location, department,
employment type, workplace type, and URL. Counting the omitted jobs as unmatched,
that is **519/577 (89.9%)** of the expected shared-page jobs. This measures
repeatability, not independent metadata accuracy. All 553 matched evidence strings
agree after normalization. The complete six-field record sets match exactly,
ignoring order, on 27 of 35 pages; job URL sets match on 34 of 35.

The 34 matched records with metadata differences come from four pages:

- **Algolia, 11 jobs:** workplace type was null in run 1 and `Remote` in run 2.
  The corresponding source locations explicitly contain `Remote`.
- **Airtable, 10 jobs:** department choices differ between parent and child
  organizational groups, such as `Customer Engagement` and `Sales`.
- **Mem0, one job:** employment type was `Full time` in run 1 and null in run 2;
  the source explicitly includes `Full time`.
- **SearchAPI, 12 jobs:** run 2 inserted an extra `Remote - Lithuania` within the
  long location list. Both lists already begin with that location.

Both runs use identical settings and disjoint API response IDs. Temperature zero
did not produce identical extractions in this experiment. These results do not
isolate which implementation/provider behavior caused the variation.

## Cost and usage

| Recorded usage, including corrections | Run 1 | Run 2 |
| --- | ---: | ---: |
| Prompt tokens | 301,332 | 317,022 |
| Cached prompt tokens | 130,048 | 273,152 |
| Completion tokens | 176,149 | 204,647 |
| Reported reasoning tokens, included in completion | 121,546 | 144,905 |
| Largest completion | 9,735 | 14,369 |
| Known input cost | $0.009861 | $0.004923 |
| Known output cost | $0.017608 | $0.020457 |
| Known total cost | $0.027469 | $0.025380 |
| Of that, correction-call cost | $0.000580 | $0.004427 |
| Calls without usage/cost data | 0 | 3 |
| Median summed request time per page | 21.43 s | 23.80 s |

Combined **reported cost is $0.052848**, including **$0.005007 for correction calls**.
The three failed calls in run 2 provide no usage/cost data, so this is the known
reported amount, not an assertion that failed calls cost zero. The 85 extraction
calls involved 86 HTTP attempts; Steel accounts for the extra connection attempt.
All 82 responses with model/provider metadata report the requested DeepSeek model
and Baidu. All completed model responses report `stop`; no output-limit failure
was observed.

Run 2 had substantially more cached input, and three calls lacked billing data.
Its lower reported cost is not evidence of a cheaper or equally complete run.
Timing sums request durations and excludes the three-second spacing. The runs
overlapped on the same provider, so this is not a controlled latency comparison.

## Fixed protocol

- Frozen, complete Crawl4AI 0.9.3 `result.cleaned_html`, unchanged after Crawl4AI.
  No new crawl, site-specific selectors, input segmentation, or custom job-card
  reconstruction was used.
- The initial prompt, six synthetic examples, schema, and inference parameters
  match the original native-HTML baseline. Model:
  `deepseek/deepseek-v4-flash-0731`; provider: `baidu/fp8`, without fallback;
  temperature 0; low reasoning; 32,768 output tokens; 180-second deadline;
  up to two transport attempts and three-second spacing.
- Runtime checks cover JSON/schema, a required absolute HTTP job URL, membership
  in the page's complete link set, and duplicate normalized job URLs. Query
  parameters are preserved. The link set is built from every `<a href>` without
  classifying links as jobs. The HTML sent to the model is unchanged.
- At most one validation-correction call per page. A page still failing afterward
  has no accepted extraction. All raw attempts and their costs remain saved.
- The labelled title/URL catalog is read only by the separate evaluator. Expected
  job counts, reviewed negatives, reference titles, and previous model outputs
  from other experiments never influence runtime validation or correction.

An existing page link is not necessarily a job link. These checks also cannot
detect a valid-looking invented title, incorrect metadata, or a missing opening.
The report continues to score those against the saved source references where
available. No new Astra, Liquid, GLM, or Codex SDK inference was used.

## Recommended next experiment

Preserve individually valid records when a schema-valid page contains one bad
record, and isolate the invalid records for correction. Modal and Poolside already
contained 43 correctly linked titles that the whole-page gate discarded. Keep
unresolved records explicit rather than replacing the entire page with empty output.

For URL fidelity, test returning an ID from a sidecar table of all page links and
resolving that ID to the original URL in code. The table can be built generically
from page links while keeping the native HTML unchanged. Separately test generic
HTML sections with overlap to address omissions such as Braintrust. These changes
have not been implemented or run in this experiment. Once they are evaluated,
extend to unfamiliar career sites.

## Artifacts and validation

- [Run and report commands](README.md#validate-native-html-extraction-and-retry-once)
- [Runner and validator](validated_html.py)
- [Evaluation and repeatability report code](validated_html_report.py)
- [Complete JSON report](data/crawl4ai-html-v1/validated-comparison.json)
- [Run 1 attempts](data/crawl4ai-html-v1/runs/deepseek-validated-v1/attempts)
- [Run 2 attempts](data/crawl4ai-html-v1/runs/deepseek-validated-v2/attempts)

All 44 tests pass, along with Ruff and `ty` checks for the new modules. Tests cover
schema recovery, persistent failures, the one-correction limit, generic link and
duplicate checks, cached resumption, changed-input rejection, billing of rejected
attempts, and the evaluator retaining failed pages in coverage. The report verifies
source/input hashes, every attempt's prompt and settings hash, stored validation,
accepted output provenance, equal run settings, and distinct response IDs across
the two runs. Data and credentials remain ignored by Git.
