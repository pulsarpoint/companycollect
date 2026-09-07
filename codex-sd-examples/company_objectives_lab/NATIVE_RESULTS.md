# Native Crawl4AI comparison — 6 September 2026

Native Crawl4AI extraction works with DeepSeek and can recover the people and jobs
our quotation validator discarded. It does not itself verify evidence, enforce the
schema, merge overlapping records, or track completion of seven separate research
objectives. Its adaptive crawler found useful pages, but this pilot does not support
using its sufficiency score as a signal that company research is complete.

All runs are saved separately under `data/native-v1/`. The original 40 snapshots,
reference, prompts and custom benchmark results remain unchanged.

## Extraction on the same 40 pages

The runner calls the real `LLMExtractionStrategy.arun` with each frozen, complete
native `cleaned_html` snapshot. Crawl4AI builds the prompt, calls its bundled
LiteLLM client and parses the response. This avoids recrawling changing pages.
The same seven-objective schema and extraction instructions are supplied, except
the instruction to return only schema JSON is changed to respect Crawl4AI's native
output envelope. Its default `<blocks>` prompt/parser is used; API-level strict
JSON-schema response mode is not enabled. No site-specific selectors are supplied.

| Configuration | Requests | Usable JSON responses¹ | Checkpoint matches before quotation/URL gate | Matches after our gate | Known API cost |
|---|---:|---:|---:|---:|---:|
| Earlier custom extraction, first pass only | 40 | 32 | 118/143 | 87/143 | $0.068398 |
| Native whole-page extraction | 40 | 31 | 108/143 | 93/143 | $0.064990 |
| Earlier custom extraction, including corrections | 64 | 33 pages with a usable response | 122/143 | 103/143 | $0.085990 |

¹ Custom first pass: 34 HTTP responses, two invalid JSON documents. Native:
32 HTTP responses, one invalid schema envelope. The other eight native calls
timed out. “Before gate” still requires individually schema-valid records under
the correct objective key; it does not count malformed or unparseable output.

These are observational runs, not a controlled ranking of model quality. On the
**26 pages with usable documents in both first-pass runs**, native extraction
matched **95/96** checkpoints and custom extraction matched **90/96**. Different
request failures, wrappers and response formats materially affect the overall
totals. The reference checks selected fields, not every field of every record;
it is not an exhaustive precision test or independent human gold standard.

| Objective | Frozen checkpoints | Custom first pass, before gate | Native whole page, before gate | Native after our gate |
|---|---:|---:|---:|---:|
| Company profile | 11 | 8 | 9 | 9 |
| Contacts | 24 | 23 | 19 | 18 |
| Locations | 11 | 11 | 10 | 8 |
| Products/services | 27 | 16 | 21 | 16 |
| People | 45 | 43 | 39 | 32 |
| Company relationships | 14 | 6 | 9 | 9 |
| Jobs | 11 | 11 | 1 | 1 |

The native whole-page Vestas jobs request timed out, explaining ten missing job
checkpoints. Other timeouts were p011, p012, p013, p026, p028, p029 and p039.
No completed response hit the output-token limit. Three negative controls passed;
the Polarbröd careers control was unassessable because its request timed out.

The Tobii software-partners output used the misspelled key
`comapny_relationships`. Crawl4AI marked its block `error: false`. The report flags
the bad envelope without silently repairing it or discarding individually valid
records from other objectives in the before-gate metric. This is direct evidence
that supplying a schema to the native prompt is not schema enforcement.

## Native chunking on the quotation failures

A separate diagnostic used native defaults: 2,048 estimated tokens, 10% overlap,
and 1.3 estimated tokens per whitespace-delimited word. This produced eight calls
across the three pages selected because of the earlier quotation failures.

| Page | Native chunks | Checkpoints before our gate | After our gate |
|---|---:|---:|---:|
| Pricer contacts/locations, p016 | 3 | 8/8 | 2/8 |
| Vestas management, p036 | 3 | 7/7 | 1/7 |
| Vestas job list, p040 | 2 | 10/10 | 0/10 |
| Total | 8 | 25/25 | 3/25 |

All eight calls completed, costing **$0.010723**. This small, deliberately selected
diagnostic is not a full-corpus chunking benchmark.

For example, the whole-page native output again quoted
`Group President & CEO Henrik Andersen`, while the source includes
`Group President & CEO File title: Henrik Andersen`. Chunking recovered the checked
people and jobs but did not make generated quotations reliably contiguous. For
Pricer's eight checkpoints, our gate retained six from the whole-page output and
only two from the chunked output. Smaller input alone is not an evidence fix.

Crawl4AI concatenated chunk results. The diagnostic contained four duplicate
records when compared by all fields except evidence. Deduplication in the report
is diagnostic only; saved native blocks are unchanged.

Even with chunking disabled, Crawl4AI's internal merge normalizes whitespace:
3,368,582 source characters became 2,233,043 characters before prompt escaping.
The original HTML files are unmodified. These native transformations should not
be confused with the earlier custom selector-based HTML simplification. Native
word-based size estimates also do not guarantee 2,048 actual model tokens for HTML.

## Adaptive crawling with the objective prompt

Both strategies received the same prompt requesting all seven objectives and
explicitly saying that one person, office or job does not complete a collection.
Each started at the existing site's base URL, with a limit of five rendered-page
attempts, `top_k_links=1` and four expansion rounds. Other scoring/stopping settings
use installed defaults. The single-link batch also avoids the installed library's
known batch budget overshoot and mixed-failure bookkeeping problems.

The statistical strategy made no LLM calls and fetched **29 pages** across eight
sites. Its choices included useful contact and vacancy pages, but also recipes,
account pages and sustainability content.

The unmodified embedding strategy failed before crawling on **all eight sites**:
its prompt requests a JSON array of strings, then its code accesses
`variations['queries']`. DeepSeek returned the requested array in every case.
Those original responses and failures are preserved in `adaptive-embedding/`.

A separately labelled diagnostic adapts only that array to `{"queries": [...]}`
before native parsing. It changes no query text, link ranking, embedding logic or
stopping rule. With this repair, seven sites ran and **27 pages** were fetched.
Vaisala failed because query expansion returned malformed JSON. No correction was
silently applied to that response.

| Site | Statistical pages | Embedding + envelope repair pages | Observations |
|---|---:|---:|---|
| Handelsbanken | 5 | 5 | Both found branches/contact information; repaired embedding also fetched a corporate login page. |
| Kongsberg | 5 | 4 | Statistical reached vacancies and a job detail; embedding followed investor/share pages. |
| Polarbröd | 5 | 5 | Statistical followed a sustainability initiative; embedding found careers and foodservice, then a recipe and account page. |
| Pricer | 3 | 5 | Statistical stopped on saturation; embedding added investor/AGM pages, without visiting the contact or executive page. |
| Tobii | 3 | 2 | Both reached the careers subdomain. Embedding declared sufficient coverage after home + careers. |
| TRUMPF | 1 | 1 | The seed had 188 internal links and zero successful metadata previews; native filtering left no candidates. |
| Vaisala | 5 | 0 | Statistical found services, investors and partners; repaired embedding failed during JSON parsing. |
| Vestas | 2 | 5 | Statistical declared sufficient coverage after home + press office. Embedding added investor pages and a careers-information page. |

The Vestas statistical score was **0.812**, yet the crawl never reached the known
management or job-list pages. Its press-office snapshot contains real named people,
their contacts and a Brazilian address, and links onward to management. It is useful
for several objectives, but it does not complete those collections.

Likewise, the Polarbröd sustainability article is not simply irrelevant: source
review found the MTF Labs collaboration and named professionals in a dated article.
Tobii's careers page contains job listings, company background and a Women in Tech
relationship. These are concrete reasons to examine every fetched page for all
objectives rather than treating the discovery category as an extraction filter.

The installed embedding implementation also reports final confidence zero on six
of its seven successful site runs while retaining nonzero coverage scores. Its
display reads `learning_score`, while the active calculation stores
`coverage_score`. Tobii's validated run displays the minimum boosted value, 0.7.
These scores should not be interpreted as calibrated probabilities of completeness.

This crawl comparison uses fresh live link graphs, not the old 200-candidate
sitemap shortlists. Its budget is five pages per site, while the earlier selector's
20-page schedule was only an offline illustration. New adaptive pages were saved
and spot-reviewed; they were not assigned exhaustive gold labels or put through a
second extraction benchmark. The extraction tables above concern the frozen 40.

## What this changes in the design

1. Keep Crawl4AI for rendering and native cleaned HTML. Its native extractor is a
   viable small component if we want its chunking and prompt/parser integration;
   this test does not establish that it outperforms the existing direct client.
2. Keep a small explicit scheduler for the seven objectives. Use link assessments
   for priority, extract all objectives on every fetched page, and track unresolved
   objectives/collections separately. Native adaptive sufficiency is not enough.
3. Replace the single synthesized evidence quote with source references or several
   exact fragments from the same record/container. Validate each fragment and its
   relationship to the entity. Preserve a supported record for review when its
   quotation formatting fails; do not accept unsupported relationships merely
   because scattered words occur somewhere on the page.
4. Retain schema validation, exact URL checks, retry/correction accounting and
   record deduplication around either extraction client. Prefer schema-constrained
   API output where supported, including for native query expansion.
5. For broad company discovery, combine sitemap candidates and page links, retain
   uncertain metadata candidates, filter login/account actions, and deliberately
   allow relevant career/group domains. Native discovery currently previews at
   most 50 internal links and drops those without head metadata; “internal” can
   include subdomains, as the Tobii run demonstrated.

## Provenance, cost and reproduction

Runtime: Crawl4AI 0.9.3, bundled `unclecode-litellm` 1.81.13, Python 3.12.
Chat calls used `deepseek/deepseek-v4-flash-0731`, Baidu FP8 only, no provider fallback,
low reasoning, temperature zero, and a 65,536-token output budget. All returned
responses reported Baidu. This token budget is not the model's maximum.
Embedding used the local `sentence-transformers/all-MiniLM-L6-v2` model, not DeepSeek
chat as an embedding model. `sentence-transformers` 6.0.1 was added to the existing
test virtual environment; production dependency manifests were not edited.

The recorder observes real LiteLLM requests/responses, saves prompts and usage,
omits credentials, and caps extraction calls at three concurrent requests per run.
It supplies routing and timeout parameters at that boundary because native adaptive
query expansion does not forward them from `LLMConfig`. Native inference and parsing
remain in place. There are no semantic correction calls in the native runs.

The requested SDK timeout was 180 seconds; observed native call durations reached
231 seconds. It is not equivalent to the custom client's total-deadline handling.
No native rate-limit retries occurred. Costs from the eight timeouts are unknown.
Metadata previews make additional network requests beyond the rendered-page budget.
Both crawl strategies use the same browser setup, fresh fetches, robots checks,
45-second page navigation timeout and two-second render delay.

Total known API cost for this work was **$0.077390**, across 65 chat calls, including
the failed native embedding arm, the repaired arm, and one setup diagnostic. The
setup diagnostic completed five Handelsbanken fetches, then our recorder failed
to serialize a NumPy metric. Its raw calls/snapshots are preserved under
`adaptive-embedding-repaired-setup1/`; the recorder was fixed and the full repaired
arm was rerun separately. This runner error is not counted as a native algorithm
failure. Local browser/embedding compute is not included in API cost.

From `codex-sd-examples`, use new output directories for new measurements:

```sh
.venv/bin/python -m company_objectives_lab.native extract \
  --run-dir company_objectives_lab/data/native-v2/extraction-full

.venv/bin/python -m company_objectives_lab.native extract --chunked \
  --page p016 --page p036 --page p040 \
  --run-dir company_objectives_lab/data/native-v2/extraction-chunked

.venv/bin/python -m company_objectives_lab.native adaptive --strategy statistical \
  --run-dir company_objectives_lab/data/native-v2/adaptive-statistical

.venv/bin/python -m company_objectives_lab.native adaptive --strategy embedding \
  --run-dir company_objectives_lab/data/native-v2/adaptive-embedding

.venv/bin/python -m company_objectives_lab.native adaptive --strategy embedding \
  --repair-query-envelope \
  --run-dir company_objectives_lab/data/native-v2/adaptive-embedding-repaired

.venv/bin/python -m company_objectives_lab.native_report \
  --run-dir company_objectives_lab/data/native-v1/extraction-full
```

Pass any other completed native run directory to the reporter to regenerate its
report. Completed outcomes are reused; interrupted calls are never overwritten.
The runner sends saved cleaned HTML directly to `arun`; when using the strategy
inside `CrawlerRunConfig`, select `input_format="cleaned_html"` explicitly because
`"html"` selects raw HTML in the installed version.

Validation: **65 tests passed**, including two local HTTP tests through real
Crawl4AI/LiteLLM. They verify request recording without credentials, native retention
of quotation failures, the query-array crash and the isolated envelope adapter.
Ruff and type checks passed. Full reports rechecked the frozen source/reference hashes.

Native feature descriptions were checked against the official
[adaptive crawling documentation](https://docs.crawl4ai.com/core/adaptive-crawling/)
and [LLM extraction documentation](https://docs.crawl4ai.com/extraction/llm-strategies/).
The live measurements and implementation-specific findings above come from the
saved runs and installed 0.9.3 source, rather than documentation claims of coverage.
