# Original Markdown, structured Markdown, and simplified HTML

**Follow-up without site selectors:** [Native Crawl4AI results](NATIVE_CRAWL4AI_RESULTS.md)
test DeepSeek on full pages using only Crawl4AI's own HTML and Markdown outputs.
The structured inputs below were produced by custom platform adapters.

**DeepSeek addition:** the later [DeepSeek Flash comparison](DEEPSEEK_RESULTS.md)
completed 120/120 requests. On the same 130-job cohort, both clean formats agreed
with Astra/low on all six fields for 93.8% of jobs, versus Liquid's 69.2%. The
original experiment below is preserved; the combined generated report includes
the new run and separate provider diagnostics.

Preserving the HTML job-card structure dramatically improved Liquid's extraction.
Simplified HTML and structured Markdown were close in this test: both agreed with
fresh Astra/low on all six fields for 90/130 shared jobs. HTML returned one more
correctly linked title on the paired title sample and had no request timeouts,
while Markdown was smaller. This single run does not establish a reliable winner
between the two clean formats.

## Final results

Liquid's title comparison uses the same 134 jobs from 36 groups that succeeded in
all three formats. A title counts only when returned once with its correct job URL;
missing jobs count as misses. The all-six comparison uses the same 130 jobs from
35 groups where both Liquid and Astra succeeded in all three formats.

| Input | Liquid valid requests | Correct URL + HTML title | All-six agreement with Astra/low on identical inputs | Median successful call |
| --- | ---: | ---: | ---: | ---: |
| Original Markdown | 37/40 | 30/134 (22.4%) | 23/130 (17.7%) | 14.49 s |
| Structured Markdown | 39/40 | 129/134 (96.3%) | 90/130 (69.2%) | 13.53 s |
| Simplified HTML | 40/40 | 130/134 (97.0%) | 90/130 (69.2%) | 12.46 s |

Liquid returned 132, 131, and 133 of the 134 paired job URLs respectively. Therefore
the title score includes both omissions and incorrect title text; it is not merely
accuracy conditional on a returned record. The four failed requests were total
deadline failures: three original-Markdown requests and one structured-Markdown
request. No Liquid request in this experiment ended because of the output limit.

**Astra/low completed 119/120 requests.** Original Markdown and HTML each returned
all 150 expected jobs with correct titles. Structured Markdown returned all 146
jobs from its 39 successful groups; the Dust request timed out. On the same 146
jobs shared across its three successful format arms, title correctness was
146/146 for every format. Median successful calls were 16.59, 15.92, and 15.74 seconds
for original Markdown, structured Markdown, and HTML respectively.

Astra's clean Markdown and HTML outputs agreed on all six fields for 144/146 jobs.
For two Unstructured roles, Markdown produced null location while HTML produced
`Remote`. This reinforces why model agreement is separate from independent truth.
Liquid returned `Public Sector` as location in both variants on those roles, so
the two Astra reference differences do not explain the tied Liquid all-six scores.

GLM produced no usable output: Together and Fireworks returned rate limits, and
Novita rejected the strict-output request. Those failures are retained separately;
there is no GLM quality result to compare with Liquid or Astra.

## Recommendation

Use the HTML structure to produce bounded job cards, retain the deterministic title
and URL, and pass the remaining source blocks to the extractor. Both clean formats
are viable inputs; structured Markdown is a reasonable compact default, while the
HTML variant is available for cases where explicit tags help. Do not conclude from
this sample that feeding complete raw HTML is better: complete raw HTML was not an
arm of the experiment.

Formatting alone does not make Liquid's complete records dependable. Keep checks
that values belong to the same card, distinguish employment from workplace type,
and preserve heading scope. Use the remaining disagreements to develop a manually
checked metadata benchmark before replacing the stronger extractor.

## Experiment

The existing 40 saved job-list pages contain 654 collected job links. All 654
were matched to recognized HTML cards before producing the full-page exports.
No new crawling was needed.

The model sample uses one existing group of up to four listings per page:
151 listings, comprising 150 jobs and one talent-pool negative example. Seven
groups target previously inspected mistakes; the other 33 use the middle nonempty
saved window. This is a development benchmark covering all pages, not all jobs
and not an unseen test set.

Each selected group has three input variants:

1. **Original Markdown:** the exact saved overlapping window.
2. **Structured Markdown:** separate title, badge, metadata, heading, and URL blocks.
3. **Simplified HTML:** the same source blocks in headings, paragraphs, an aside for
   a badge, an article per card, and a separate job link.

The clean variants preserve organizational nesting from the DOM and keep recognized
board-description prose that appears in the original window. All 120 model-input
hashes and URL sets were checked. After removing syntax and normalizing whitespace,
the clean variants have identical visible text on all 40 groups.

The original windows total 35,887 characters; clean Markdown 32,080; clean HTML
39,757. HTML is about 24% longer than the equivalent Markdown in characters. These
are content sizes, not token counts or complete request sizes.

The cleaner conversion uses explicit adapters for Ashby, Greenhouse, Lever, and
CircleCI's custom Greenhouse layout. It separates a wrapping card link from the
heading and metadata, moves a badge outside the title, and retains the URL. It
does not ask an LLM to rewrite the source or infer missing field values. This is
not a universal converter: unknown layouts fail visibly instead of silently
dropping jobs. Because these adapters already identify titles, production code
could retain the deterministic titles directly.

## Models and controls

- **Codex SDK:** explicitly `gpt-6-astra`, reasoning effort `low`, set on both the
  session and turn. Existing account configuration was not edited. Its model
  request is recorded in settings; the wrapper does not expose response-model
  metadata. This is a fresh baseline, separate from the old xhigh results.
- **Liquid:** `liquid/lfm-2.5-2.6b:free`, 8,192 output tokens, reasoning enabled with
  its default effort. The exact free endpoint advertised that completion maximum
  when checked; it is not a universal local-code ceiling.
- **GLM:** `z-ai/glm-5.3-flash`, low effort, 32,768 output tokens. Provider attempts
  are saved as separate runs, with provider fallbacks disabled.

All variants use the same extraction schema, instructions, and six examples. A
common addition explains HTML boundaries, badges, heading paths, and evidence as
visible text. The original-Markdown baseline was rerun with that same prompt, so
improvements are not compared against an old prompt run. Format order rotates by
page, identically for every model. Temperature is 0 for OpenRouter; the Codex SDK
does not receive that setting or the OpenRouter output budget.

Each model processes one request at a time, with a three-second gap. OpenRouter
allows at most two attempts inside a 180-second total deadline. Codex uses the
existing 180-second turn deadline followed by bounded interruption/cleanup.

## Measurement

Title accuracy uses the title element from the saved HTML, with the separate New
badge removed. Case and whitespace are normalized; title qualifiers, punctuation,
and geographic restrictions remain significant. One manually reviewed talent-pool
listing should produce no opening. URL coverage, duplicates, unexpected URLs, and
values absent from the corresponding card/context are recorded separately.

The main six-field comparison uses fresh Astra/low output for the identical input
format. It measures agreement, not independent gold accuracy. It is restricted to
page groups where both the candidate and Astra succeeded in all three formats,
holding the job set constant across the format comparison. Title comparisons use
each model's groups that succeeded in all three formats. Failed and unsent requests
are reported separately and are not counted as successful empty extractions.

The old Codex comparison is retained only as a secondary diagnostic. Source review
found a concrete reference error: Encord's HTML title is `Special Projects, GTM`;
`Account Executive` or `Account Manager` belongs to its metadata. An old reference
that appends those labels to the title must not decide title correctness here.

## Operational issues

Together rejected the first three GLM requests with HTTP 429, triggering the stop
condition. A separately pinned Fireworks run also stopped after three HTTP 429
outcomes. Novita rejected the strict-output parameter combination with HTTP 404;
15 rejected attempts were saved before that diagnostic was stopped. The runner
was then updated to stop immediately on non-retryable HTTP request/routing errors.
No GLM provider returned a usable extraction in these diagnostics, so they cannot
support a comparison of GLM extraction quality.

Timeouts in the Liquid and Codex runs remain part of the experiment. Original
outcomes are preserved; results are not silently repaired or replaced. The Codex
SDK exposes token usage but no dollar charge. OpenRouter outcomes without usage
have unknown charges, not an assumed zero cost. SDK input tokens include runtime
overhead and use a different tokenizer, so cross-model token totals are not a
pure comparison of the three text formats.

## Source-reviewed remaining problems

- Liquid returned literal `&amp;` in a Calendly HTML title and sometimes `&gt;`
  in an HTML department value. HTML introduces entity-decoding work.
- In the clean Markdown for Contentsquare, Liquid shortened `UKI` to `UK` and
  assigned `Hybrid` to employment type when the employment value was absent.
- On PlanetScale's HTML, Liquid copied `Remote - NA, APAC, EMEA` from Enterprise
  Support Engineer to Customer Support Engineer, whose location is `EMEA`.
- On Railway, some Liquid outputs included `Engineering` in location or returned
  the full heading path as department instead of the nearest group.
- In Encord's clean inputs, Liquid sometimes **replaced** `Special Projects, GTM`
  with `Account Executive` or `Account Manager` from metadata. This differs from
  the older flattened-input failure that appended metadata to the title.
- Letta's global `Fully in-person` policy is present in both clean inputs. Astra
  applied it to all jobs; Liquid left workplace type null. Policy inheritance
  needs an explicit rule before this becomes an independently graded field error.

These cases show why a correct title does not imply a correct complete record.

## Usage and verification

The experiment saved 261 new outcomes, including the stopped provider diagnostics:
235 successful extractions, five timeouts, and 21 routing/rate-limit rejections.
Liquid's 116 successful responses all reported provider `Liquid` and a zero dollar
charge. Dollar charges for its four timed-out requests and the 21 GLM rejection
outcomes are unavailable. Codex dollar charges are not exposed by the SDK.

Reported Liquid input/output tokens were 120,757/120,704 for original Markdown,
127,075/116,593 for structured Markdown, and 134,595/122,013 for HTML. These totals
cover different numbers of successful requests and exclude usage unavailable on
timeouts, so they should not be used as a controlled cost-per-job comparison.

The 32 existing lab tests and seven new format/model/routing tests passed. Ruff,
type checking, and the diff check passed. The final audit reproduced input hashes
for all 261 outcomes, including the 15 Novita diagnostics. All 120 frozen input
hashes, all selected URL sets, and visible
text parity across the two clean formats were verified.

## Files and replay

- [Generated comparison](data/format-comparison-v1/comparison.md) and
  [complete outcomes and metrics](data/format-comparison-v1/comparison.json).
- [Frozen inputs, selections, and source audit](data/format-comparison-v1/manifest.json).
- [Conversion checks](data/format-comparison-v1/conversion-audit.json).
- [Reviewed differences](data/format-comparison-v1/reviewed-differences.json).
- [Final audit and Astra format differences](data/format-comparison-v1/final-audit.json).
- Example Webflow inputs: [original](data/format-comparison-v1/inputs/original_markdown/greenhouse-webflow.md),
  [structured Markdown](data/format-comparison-v1/inputs/clean_markdown/greenhouse-webflow.md),
  [simplified HTML](data/format-comparison-v1/inputs/clean_html/greenhouse-webflow.html).

Full-page exports are under `data/format-comparison-v1/full-pages/`; the benchmark
uses the smaller groups under `inputs/`. The [README](README.md) contains preparation,
run, and report commands. A new model setting or prompt requires a new run ID.
