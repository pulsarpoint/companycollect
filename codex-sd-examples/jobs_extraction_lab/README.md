# Job-list extraction experiment

Compare the existing Codex SDK extraction approach with OpenRouter models, using
`liquid/lfm-2.5-2.6b:free` by default and the same frozen Markdown,
instructions, and output schema. This folder reuses the existing Python environment
and Crawl4AI browser setup; it does not change the other examples.

The completed corpus is in `data/manifest.json`, with 40 Markdown files and their
rendered HTML. Results for the full experiment are in
[`data/runs/jobs-v2/comparison.md`](data/runs/jobs-v2/comparison.md); detailed
disagreements, failures, and usage are in the adjacent `comparison.json` and backend
directories. See [RESULTS.md](RESULTS.md) for interpretation and checked examples.

The current extraction instructions have since been rewritten to explain flattened
Markdown, title boundaries, inherited fields, overlap, and a final completeness
check. Historical results retain their original prompts in `settings.json`. Use a
fresh run ID with the revised prompt; reusing an old ID correctly rejects changed
settings. The examples section below contains the current seven-window pilot command.

## What this measures

This experiment isolates extraction from job-list pages. Source discovery and
sitemap selection are deliberately outside this comparison: both extractors receive
the same saved page and cannot obtain missing fields by visiting job details.

Each job has a title, location, department, employment type, workplace type, job URL,
and a short source quotation. Missing values must be `null`. General applications
and talent pools are excluded. Two different openings with the same title remain
separate when their URLs differ.

The collector rendered public boards with Crawl4AI and CloakBrowser, respected
robots.txt, and accepted pages with at least two observed job links and useful
Markdown. It saved the whole page without truncating it. The corpus contains:

- 40 pages: 31 Ashby, 7 Greenhouse, and 2 Lever boards.
- 154,772 Markdown characters and 654 observed job links. Link count is a collection
  signal, not a verified count of eligible openings.
- Mostly English technology-company boards, with 2–35 observed links per page.

This is a platform-biased pilot, not a representative benchmark of every career
website. The candidate list includes unavailable, oversized, and inaccessible
boards; `data/collection-attempts.json` retains rejection reasons. Some Ashby sources
were discovered using their public posting API, but extraction inputs are the
browser-rendered Markdown, not API responses.

## Run

Run commands from `companycollect/codex-sd-examples`, using its existing Python 3.12
environment. For a fresh environment, install the project's dependencies with
`uv sync` first.

The authorized OpenRouter key has been copied into this folder's ignored `.env` as
`OPENROUTER_API_KEY`. The file has owner-only permissions. It is not part of the
corpus, model prompts, or result files. For a new checkout, copy `.env.example` to
`.env` and provide your own key.

Collect a new corpus into a separate directory, preserving the existing snapshots:

```bash
.venv/bin/python -m jobs_extraction_lab.main collect \
  --data-dir jobs_extraction_lab/data-new --target 40
```

Run both backends against the existing 40-page corpus:

```bash
.venv/bin/python -m jobs_extraction_lab.main run \
  --backend codex --run-id jobs-explained-v3 --concurrency 4 \
  --codex-bin /Applications/ChatGPT.app/Contents/Resources/codex

.venv/bin/python -m jobs_extraction_lab.main run \
  --backend openrouter --run-id jobs-explained-v3 --concurrency 2 \
  --env-file jobs_extraction_lab/.env

.venv/bin/python -m jobs_extraction_lab.main compare --run-id jobs-explained-v3
```

These commands start runs using the revised prompt and resume matching outcomes on
subsequent invocations. Use a new `--run-id` for a new experiment or
changed settings. `--limit 1` provides a smoke test. `--retry-failed` explicitly
overwrites failed outcomes in that run, so preserve the original run before using
it for a published comparison. Output-limit failures are valid benchmark outcomes;
the recorded full run does not silently repair or retry them.

The current Codex baseline uses the account's configured default, `gpt-6-astra`
with `xhigh` reasoning. The installed SDK's pinned CLI is too old for this model,
so the successful run uses the existing app CLI 0.153.1 via `CodexConfig(codex_bin=...)`.
No dependencies or account configuration were changed. On another machine, supply
a compatible CLI path or omit the override if the SDK's pinned runtime supports
your configured model. The configured model is recorded; the SDK adapter does not
claim independently reported actual-model metadata.

OpenRouter requests use temperature 0, strict JSON schema, supported-parameter
routing, and a default budget of 8,192 output tokens. The exact free endpoint requires
reasoning. `reasoning.exclude=true` hides reasoning text, but reasoning still
consumes the output budget. Requests never silently switch models. Select a different
model explicitly with `--openrouter-model`; changing the model requires a new run ID.
The CLI no longer imposes an 8,192-token ceiling: `--max-tokens 16384`, for example,
can request a larger budget on a provider that supports it. On 2026-09-05, the
[Liquid free endpoint](https://openrouter.ai/liquid/lfm-2.5-2.6b:free) advertised a
65,536-token context window and an 8,192-token completion maximum. These are distinct
limits. GLM limits vary by provider; the same ceiling must not be assumed for it.

## Outputs and interpretation

- `manifest.json`: source URLs, capture times, Markdown hashes, and observed links.
- `markdown/` and `html/`: immutable inputs for replay and source inspection.
- `runs/<id>/<backend>/settings.json`: model label, instructions, schema, SDK
  version, and relevant backend settings.
- `runs/<id>/<backend>/<page>.json`: success/failure, parsed jobs, original OpenRouter
  response text, literal-source flags, duration, and usage when available. The reused
  Codex adapter returns parsed data rather than raw response text.
- `runs/<id>/comparison.json` and `.md`: all-page coverage, paired-page agreement,
  per-field differences, unmatched jobs, and request usage.

Each page is checkpointed independently. Input hashes detect changed Markdown;
settings hashes prevent mixing different prompts in one backend run. Transient HTTP
and transport failures have bounded retries. Non-JSON, invalid-schema, and incomplete
outputs remain failures rather than being counted as empty successful extractions.
OpenRouter's `--timeout` now also enforces a total deadline per page across requests
and retry delays, so an active connection cannot keep a request running indefinitely.
If no final response arrives, usage and charge are unavailable, not known to be zero.

Codex output is a reference, not ground truth. Compare successful-page coverage
before interpreting agreement on the remaining paired pages. Equal `null` values
contribute to field agreement. Source validation checks literal presence anywhere
in the page; it cannot establish that a value belongs to the correct opening or
that every job was found. Quotation formatting changes can also trigger flags.

Durations include SDK startup and request handling; summed concurrent request time
is not wall time. Output tokens include reasoning where reported. OpenRouter's
reported charge is retained; no dollar cost is inferred for Codex subscription
usage. Free-model availability and limits can change.

The repository ignores `data/`, so downloaded content and raw run artifacts stay
local unless explicitly exported. Source code and the human-readable findings are
separate from those generated files. The `initial` and `jobs-v1` directories retain
earlier compatibility smoke tests; `jobs-v2` is the full-corpus experiment.

## Repeat the Liquid run

The second full Liquid pass is saved separately as `jobs-v2-repeat`. It uses the same
40 snapshots, prompt, schema, model ID, temperature, reasoning settings, and output
limit. The first run remains intact. To perform another pass, choose a fresh run ID:

```bash
.venv/bin/python -m jobs_extraction_lab.main run \
  --backend openrouter --run-id jobs-explained-v3-repeat --concurrency 2 \
  --timeout 300 --attempts 3 --max-tokens 8192 \
  --env-file jobs_extraction_lab/.env

.venv/bin/python -m jobs_extraction_lab.repeat \
  --first-run jobs-explained-v3 --second-run jobs-explained-v3-repeat
```

The repeat comparison verifies identical settings and input hashes before comparing
the outcomes. It distinguishes raw-response equality, parsed-record equality,
ordering changes, evidence changes, and changes to the six extracted fields. Failed
responses remain failures; two failed pages are not counted as matching empty lists.
The comparison requires both passes to have an outcome for every corpus page.

Historical results are in `data/runs/jobs-v2-repeat/repeat-comparison.md` and the adjacent JSON
file. [REPEAT_RESULTS.md](REPEAT_RESULTS.md) explains the failure patterns and
repeatability findings. Codex was not rerun for this repeatability test.

## Validation

```bash
.venv/bin/python -m unittest \
  jobs_extraction_lab.tests jobs_extraction_lab.format_tests \
  jobs_extraction_lab.crawl4ai_html_tests jobs_extraction_lab.validated_html_tests \
  jobs_extraction_lab.html_windows_tests
uvx ruff check jobs_extraction_lab
uvx ty check jobs_extraction_lab
```

Tests exercise strict output validation, source grounding, exact OpenRouter request
parameters, credential redaction, bounded retries, changed-input detection, and
comparison coverage when a backend fails.

## Teach the model with examples

[`prompt_examples.md`](prompt_examples.md) contains six synthetic demonstrations
with expected JSON: title/metadata boundaries, repeated titles at different URLs,
remote locations, joined employment/city text, a title followed by a description,
and chunks containing no identifiable opening. Examples also cover missing values,
department headings, relative URLs, and verbatim evidence. The output schema
is unchanged; it does not include a description field.

Pass the file explicitly to add these examples to every page or window request:

```bash
.venv/bin/python -m jobs_extraction_lab.main run \
  --backend openrouter \
  --data-dir jobs_extraction_lab/data/few-shot-pilot-v1 \
  --run-id liquid-explained-prompt-v3 --concurrency 2 --timeout 180 --attempts 2 \
  --examples-file jobs_extraction_lab/prompt_examples.md \
  --env-file jobs_extraction_lab/.env
```

The pilot directory contains seven copied windows from six previously inspected
boards. The command resumes the saved pilot; choose a new run ID to make fresh
requests. Use a different data directory to apply the same examples to another
corpus. [FEW_SHOT_RESULTS.md](FEW_SHOT_RESULTS.md) records the prompt revisions and
their limitations.

The revised base instructions apply with or without `--examples-file`; the flag
adds demonstrations to those instructions. Full instruction and example text is
saved in `settings.json` and included in the input hash. Editing either requires a
new run ID. Both backends support the option;
the ordinary paired-backend comparison requires the same examples on each side.
The pilot instead explicitly compares different prompt variants with the earlier
Liquid results and the existing Codex reference. It makes no new Codex requests.

## Compare GLM 5.3 Flash with Liquid

The model option can reuse the same seven-window pilot, current instructions, and
six examples. This command preserves Liquid as the default for other invocations:

```bash
.venv/bin/python -m jobs_extraction_lab.main run \
  --backend openrouter --openrouter-model z-ai/glm-5.3-flash \
  --data-dir jobs_extraction_lab/data/few-shot-pilot-v1 \
  --run-id glm-5.3-flash-prompt-v3 --concurrency 2 --timeout 180 --attempts 2 \
  --max-tokens 8192 --examples-file jobs_extraction_lab/prompt_examples.md \
  --env-file jobs_extraction_lab/.env
```

The requested model is saved in settings and each outcome alongside the model ID
reported by OpenRouter. Model selection is passed explicitly to every request and
preserved during retries. GLM's reported usage and charge are recorded separately.
[GLM_RESULTS.md](GLM_RESULTS.md) compares this run with `liquid-explained-prompt-v3`
and the existing Codex reference; no fresh Codex or Liquid calls are needed.

[GLM_32K_RESULTS.md](GLM_32K_RESULTS.md) records the subsequent three FINN trials at
32,768 output tokens, graded against manually checked window values. Two timed out
and one returned a truncated title, so that experiment did not advance to the
seven-window or 40-page stages. New outcomes preserve OpenRouter's provider and
response ID when available.

The next experiment explicitly requested low reasoning effort, then restricted
routing to Together after observing different outputs from different providers.
[GLM_LOW_RESULTS.md](GLM_LOW_RESULTS.md) records the repeated FINN check, the
seven-window pilot, and the full 228-window experiment, including provider failures
and separately saved recovery attempts.

```bash
.venv/bin/python -m jobs_extraction_lab.main run \
  --backend openrouter --openrouter-model z-ai/glm-5.3-flash \
  --reasoning-effort low --openrouter-provider together \
  --data-dir jobs_extraction_lab/data/segmented-v1 \
  --run-id glm-32k-low-together-full --concurrency 2 --timeout 180 --attempts 2 \
  --max-tokens 32768 --examples-file jobs_extraction_lab/prompt_examples.md \
  --env-file jobs_extraction_lab/.env
```

This command resumes the original experiment from its saved outcomes. Choose a
fresh run ID for fresh requests. `--reasoning-effort` is optional; omitting it
preserves provider-default behavior. `--openrouter-provider` restricts requests to
that provider slug and disables provider fallback. Omitting it permits the existing
automatic routing. Both choices are recorded in settings and the input hash, so
changing either requires a new run ID. Provider support and availability still
determine whether a request succeeds. The first full run encountered substantial
upstream rate limiting even at concurrency 2.

## Compare original Markdown, structured Markdown, and simplified HTML

`format_inputs.py` reads the saved rendered HTML and preserves each card's title,
metadata blocks, badges, URL, and organizational headings. It handles the three
platforms in this corpus plus CircleCI's custom Greenhouse layout, and fails if a
collected job URL has no recognized card. It does not call an LLM or recrawl a site.

```bash
.venv/bin/python -m jobs_extraction_lab.format_inputs \
  --output-dir jobs_extraction_lab/data/format-comparison-new
```

The output includes complete exports of all 40 pages and one selected group of up
to four jobs per page for model testing. Seven selections target previously
reviewed failures; the others use the middle nonempty saved window. Original
Markdown stays unchanged. The clean variants contain the same source blocks in
different syntax, including the restored heading hierarchy. This is a platform
adapter, not a universal HTML-to-Markdown converter. Title identification is
already deterministic, so production code could retain that title directly.

The completed preparation is `data/format-comparison-v1`: 654 cards across the
full exports, 151 selected listings, and 150 expected jobs after excluding one
talent-pool listing. `conversion-audit.json` verifies all input hashes and URL sets,
and equal visible text between the clean formats on every page.

```bash
.venv/bin/python -m jobs_extraction_lab.format_benchmark \
  --data-dir jobs_extraction_lab/data/format-comparison-v1 \
  --run-id formats-v1 --env-file jobs_extraction_lab/.env \
  --codex-bin /Applications/ChatGPT.app/Contents/Resources/codex \
  --timeout 180 --attempts 2 --interval 3
```

The default comparison uses Liquid on OpenRouter, GLM with low reasoning through
Together, and the existing Codex SDK explicitly configured with `gpt-6-astra` and
`low` effort on both the session and turn. Those Codex settings apply to this
experiment; ordinary lab commands retain their previous defaults. OpenRouter
budgets are 8,192 tokens for the exact Liquid free endpoint and 32,768 for GLM.
The SDK does not receive either OpenRouter budget or its temperature setting.

Use `--model liquid`, `--model codex`, `--model glm`, or `--model deepseek` to select one backend;
repeat the option to select more than one. `--glm-provider` explicitly pins a
different GLM provider. Each model runs sequentially with a pause between requests;
format order rotates by page. Three consecutive service failures stop that model.
Non-retryable HTTP request/routing errors stop it immediately. Changed settings
require a new run ID. Replays reuse saved outcomes, including failures.

DeepSeek is opt-in and uses `deepseek/deepseek-v4-flash-0731` with low reasoning,
a 32,768-token output budget, and one endpoint pinned without provider fallback.
`--deepseek-provider` selects that endpoint (default `deepinfra/fp8`). The full
comparison below uses `baidu/fp8` after a separate DeepInfra run encountered an
upstream rate limit and a slow request. DeepSeek receives the same inputs, prompt,
examples, and JSON schema:

```bash
.venv/bin/python -m jobs_extraction_lab.format_benchmark \
  --data-dir jobs_extraction_lab/data/format-comparison-v1 \
  --run-id formats-deepseek-baidu-v1 --env-file jobs_extraction_lab/.env \
  --codex-bin /Applications/ChatGPT.app/Contents/Resources/codex \
  --model deepseek --deepseek-provider baidu/fp8 \
  --timeout 180 --attempts 2 --interval 3
```

Include `--run-id formats-deepseek-baidu-v1` in the report command to compare its
saved outputs with the existing Astra/low and Liquid results. The partial
`formats-deepseek-v1` run and `formats-deepseek-baidu-pilot` can also be included as
separate diagnostic rows.

See [DEEPSEEK_RESULTS.md](DEEPSEEK_RESULTS.md) for the completed run, the direct
130-job comparison with Liquid, provider diagnostics, costs, and source-reviewed
differences.

```bash
.venv/bin/python -m jobs_extraction_lab.format_report \
  --data-dir jobs_extraction_lab/data/format-comparison-v1 \
  --run-id formats-v1 --run-id formats-fireworks-v1
```

The report verifies every response's input hash before aggregating results. Title
accuracy uses the saved HTML title elements. Field comparisons use fresh Astra/low
outputs on identical inputs and remain agreement measures, not independent gold.
Scores on paired groups require successful responses in all three formats so a
provider outage does not silently change the compared job set. The JSON also
retains historical Codex comparisons and exposes failed and unrun requests.
See [FORMAT_RESULTS.md](FORMAT_RESULTS.md) for the source review and interpretation.

## Test native Crawl4AI outputs without site selectors

See [NATIVE_CRAWL4AI_RESULTS.md](NATIVE_CRAWL4AI_RESULTS.md) for the 40-page
comparison, input sizes, failures, costs, and source-reviewed title differences.

`crawl4ai_html.py` replays each saved rendered page through Crawl4AI's own
`AsyncWebCrawler.aprocess_html` with `CrawlerRunConfig(verbose=False)`. It saves
`result.cleaned_html` and `result.markdown.raw_markdown` unchanged. Preparation and
inference do not use platform selectors, known job links, labelled titles, or
reference outputs. Every model request receives a complete page; no cropping,
card reconstruction, truncation, or segmentation is applied.

```bash
.venv/bin/python -m jobs_extraction_lab.crawl4ai_html prepare \
  --source-dir jobs_extraction_lab/data \
  --output-dir jobs_extraction_lab/data/crawl4ai-html-v1

.venv/bin/python -m jobs_extraction_lab.crawl4ai_html run \
  --data-dir jobs_extraction_lab/data/crawl4ai-html-v1 \
  --run-id deepseek-v1 --env-file jobs_extraction_lab/.env

.venv/bin/python -m jobs_extraction_lab.crawl4ai_html run \
  --data-dir jobs_extraction_lab/data/crawl4ai-html-v1 \
  --run-id deepseek-markdown-v1 --env-file jobs_extraction_lab/.env \
  --input-format markdown

.venv/bin/python -m jobs_extraction_lab.crawl4ai_html_report \
  --data-dir jobs_extraction_lab/data/crawl4ai-html-v1 \
  --reference-dir jobs_extraction_lab/data/format-comparison-v1
```

Default inference uses DeepSeek V4 Flash 0731, low reasoning, a 32,768-token output
budget, and Baidu FP8 pinned without provider fallback. A new preparation requires
a new output directory. Reusing an inference run ID reads its saved outcomes;
choose a new run ID to send fresh requests.

The separate report reads existing labelled artifacts only for evaluation. It
checks complete-page title/URL coverage, four reviewed non-job listings, and the
150-job subset with existing Astra/low and DeepSeek references. Those references
used cropped, custom HTML, so complete-record agreement is a diagnostic rather
than independent accuracy. Both native outputs retain full-page context and can
resolve some department/team distinctions differently.

## Validate native HTML extraction and retry once

See [VALIDATED_HTML_RESULTS.md](VALIDATED_HTML_RESULTS.md) for both completed
40-page runs, retry outcomes, coverage, repeatability, and costs.

The validation experiment uses the same frozen native HTML and inference settings
as `deepseek-v1`. It verifies the JSON schema, requires an absolute HTTP job URL,
checks that each URL appears among the page's links, and rejects duplicate job
URLs. Link checking examines every `<a href>` without identifying job cards or
using site-specific selectors. It preserves query parameters when normalizing URLs.

Each page gets one initial call and, only after an output-validation failure, at
most one correction call containing the original prompt/HTML, previous output,
and specific validation errors. The model must return the complete corrected
page. The first-call prompt is identical to the original baseline's prompt.
HTTP retries retain the existing baseline policy; HTTP errors do not trigger
an additional validation-correction call.

```bash
.venv/bin/python -m jobs_extraction_lab.validated_html \
  --data-dir jobs_extraction_lab/data/crawl4ai-html-v1 \
  --baseline-run deepseek-v1 --run-id deepseek-validated-v1 \
  --env-file jobs_extraction_lab/.env

.venv/bin/python -m jobs_extraction_lab.validated_html \
  --data-dir jobs_extraction_lab/data/crawl4ai-html-v1 \
  --baseline-run deepseek-v1 --run-id deepseek-validated-v2 \
  --env-file jobs_extraction_lab/.env

.venv/bin/python -m jobs_extraction_lab.validated_html_report \
  --data-dir jobs_extraction_lab/data/crawl4ai-html-v1 \
  --reference-dir jobs_extraction_lab/data/format-comparison-v1 \
  --run-id deepseek-validated-v1 --run-id deepseek-validated-v2
```

Every attempt, raw response, validation error, request hash, and usage record is
saved under `runs/<run-id>/attempts/<page-id>/`. Reusing a run ID resumes those
attempts, including failures; it does not spend again on completed attempts.
Final accepted outputs live in `responses/`. A page still failing after its
correction attempt has no accepted extraction; partial records remain available
in the saved attempts for inspection and scoring.

Evaluation reports first-pass title/URL coverage, final accepted coverage,
recovery and remaining failures, all billed attempts, and repeatability across
the two runs. Schema/link checks do not establish title or metadata correctness
and cannot detect an omitted opening or a non-job page link. Labelled references
remain confined to the report and never influence retries.

## Extract overlapping windows with the small model

For the newer native-HTML experiment, see
[Partial records and generic HTML windows](#partial-records-and-generic-html-windows).
The older Markdown experiment below uses job-link boundary heuristics.

The segmented experiment is a small Python workflow: prepare windows, call the
existing direct OpenRouter extractor for every window, then merge the results.
It uses the same Liquid model, prompt, schema, temperature, and output limit as the
whole-page experiment. No agent SDK or larger-model fallback is involved in these
window requests.

```bash
.venv/bin/python -m jobs_extraction_lab.segment prepare \
  --window-dir jobs_extraction_lab/data/segmented-v1 \
  --max-jobs 4 --overlap-jobs 1 --max-chars 3000

.venv/bin/python -m jobs_extraction_lab.main run \
  --backend openrouter \
  --data-dir jobs_extraction_lab/data/segmented-v1 \
  --run-id liquid-windows-explained-v3 --concurrency 3 \
  --env-file jobs_extraction_lab/.env

.venv/bin/python -m jobs_extraction_lab.segment merge \
  --window-dir jobs_extraction_lab/data/segmented-v1 \
  --run-id liquid-windows-explained-v3 --baseline-run jobs-v2
```

Preparation refuses to overwrite an existing window manifest. To replay the saved
windows, omit `prepare` and choose a fresh extraction run ID. The extraction command
resumes existing outcomes when the same ID is used. Merge requires an outcome for
each window and can be rerun without making API calls.
The command above compares the revised prompt with the historical whole-page Codex
reference. This measures agreement across prompt versions, not a same-prompt backend
comparison; provide a newly collected baseline run to compare the revised prompt
on both backends.

The current 40-page corpus produces 228 windows. The segmenter preserves all source
lines and all recognized Markdown job links. It adds separating whitespace between
concatenated job headings, keeps each listing line intact, and repeats preceding
heading/plain-section context. Adjacent windows share one complete listing where
the character budget allows; a single oversized atomic line is rejected explicitly
instead of being silently truncated.

This boundary detection is designed for the downloaded Ashby, Greenhouse, and Lever
job lists, where each listing is represented by one Markdown link line. Arbitrary
documents or job descriptions spanning multiple paragraphs need different atomic
blocks. The code does not claim to be a universal Markdown parser.

The merger:

- Accepts only job URLs recognized in the same window. Missing URLs and filter
  links are retained as rejected predictions for inspection; this rule is specific
  to this corpus of linked job lists.
- Deduplicates by normalized job URL and preserves separate openings with identical
  titles and different URLs.
- Votes on complete field sets, counting distinct windows. It keeps a provisional
  representative for each job, with every conflicting alternative and its window
  IDs. Ties keep the earliest window; a majority is not proof of correctness.
- Flags conflicts, failed windows, rejected predictions, and source-check issues
  for review. Successful windows from partial pages remain available. A false
  `requires_review` flag means no automated issue was detected; it does not certify
  that the field values are correct.
- Measures overlap recovery: a job returned by one window but omitted by another
  window that contained the same job. Unreturned links can be intentional exclusions
  such as general applications, so link coverage is not itself accuracy.

Window inputs and provenance are in `data/segmented-v1/manifest.json` and
`windows.json`. Line ranges refer to the prepared text after separating concatenated
headings; parent hashes identify the unchanged original snapshots. Original responses are under
`data/segmented-v1/runs/liquid-windows-v1/openrouter/`; merged records, alternatives,
and comparisons are alongside them under `merged/`, `segmented-comparison.json`,
and `segmented-comparison.md`.

[SEGMENTED_RESULTS.md](SEGMENTED_RESULTS.md) summarizes completion, overlap recovery,
conflicts, and the remaining field-quality problems from the full run.

## Partial records and generic HTML windows

[HTML_WINDOWS_RESULTS.md](HTML_WINDOWS_RESULTS.md) contains the completed comparison,
retention benefits, overlap recovery, transport failures, costs, and metadata conflicts.

This experiment keeps valid jobs when another record fails validation. It compares
40 unchanged native Crawl4AI HTML pages with 98 overlapping HTML fragments from
the same pages, using the same DeepSeek model, base prompt, examples, schema, and
inference settings. Both arms use partial retention and at most one correction
call per input.

```bash
.venv/bin/python -m jobs_extraction_lab.html_windows \
  --data-dir jobs_extraction_lab/data/crawl4ai-html-v1 \
  --output-dir jobs_extraction_lab/data/html-windows-v1 \
  --core-chars 6000 --atom-chars 2000 --overlap-chars 1200

.venv/bin/python -m jobs_extraction_lab.partial_html \
  --data-dir jobs_extraction_lab/data/html-windows-v1 \
  --run-id deepseek-partial-v1 --baseline-run deepseek-v1 \
  --env-file jobs_extraction_lab/.env --concurrency 3

.venv/bin/python -m jobs_extraction_lab.html_windows_report \
  --data-dir jobs_extraction_lab/data/html-windows-v1 \
  --reference-dir jobs_extraction_lab/data/format-comparison-v1 \
  --run-id deepseek-partial-v1
```

The splitter tracks source character ranges with Python's HTML parser. It groups
generic subtrees, keeps anchors, table rows, and headings whole, and partitions
every original character into one core range. Neighboring inputs repeat at least
the requested overlap where preceding source exists; whole atoms can make the
overlap larger. Oversized indivisible elements are preserved, so character targets
are not hard limits. Up to 1,500 characters of preceding heading context are copied
from the source, excluding headings inside links and table rows. Nothing is
rewritten or selected using platform CSS, known job URLs, or reference labels.
The fragments can start/end inside enclosing containers; they are lossless source
ranges rather than reserialized, balanced HTML documents.

Every response must be complete JSON with a `jobs` array. Jobs are validated
individually against the schema and links in that input. Rejected records and
errors are retained separately; malformed JSON is not partially repaired. A
correction uses the original input and recorded errors. Valid observations from
the initial attempt survive even if correction fails or omits them.

Merging deduplicates normalized URLs. For competing values it prefers a window
whose core contains that link, then an initial attempt, then source order. This
is a deterministic selection rule, not a correctness guarantee. Every conflicting
alternative and its provenance remain in the output. Distinct URLs with the same
title remain distinct jobs. All page links are used for ownership and validation;
there is no job-link classifier in preprocessing.

Preparation refuses an existing output directory. Reusing a run ID resumes saved
attempts without resending them. After a transient-service stop, rerun the same
command to continue pending inputs; failed completed calls remain failures in the
benchmark. `status.json` records current completion; `stopped.json` preserves the
latest stop event. Runs stop after three consecutive service failures or a
nonretryable routing/configuration response.

The report checks source-range coverage, link retention, hashes, requests,
validation, and merge provenance. It measures first-attempt and retained title/URL
coverage, conflicts, jobs found only by neighboring chunks, and total recorded
cost. It also evaluates the same responses under a rule that discards an invalid
final response, to isolate what retention changes without extra inference. The
source title/URL catalog is used only by evaluation. Metadata agreement between
formats is not independent accuracy, and validation does not prove completeness.

## Crawler review

[COMPANY_RESEARCH_OBJECTIVES.md](COMPANY_RESEARCH_OBJECTIVES.md) defines the broader
company-research objectives, candidate assessments, extraction attribution rules,
coverage tracking, and prompt drafts. Jobs remain one objective within that design.

[CRAWL4AI_LINK_SELECTION_ANALYSIS.md](CRAWL4AI_LINK_SELECTION_ANALYSIS.md) explains
native URL seeding, link previews, adaptive ranking, the role of a chat model,
verified implementation issues, and the proposed DeepSeek page-selection test.

[DESIGN_NOTES.md](DESIGN_NOTES.md) covers the existing sitemap-selection and crawl
code, preserving complete snapshots, handling external career boards, and moving
routine extraction to direct inference requests.

## References

The separate [company objective benchmark](../company_objectives_lab/README.md)
extends direct DeepSeek evaluation to company profiles, contacts, locations,
offerings, people, relationships and jobs, with independent link-selection and
extraction tests. See its [results](../company_objectives_lab/RESULTS.md).

- [OpenRouter model](https://openrouter.ai/liquid/lfm-2.5-2.6b:free)
- [Structured outputs](https://openrouter.ai/docs/guides/features/structured-outputs)
- [Reasoning token accounting](https://openrouter.ai/docs/guides/best-practices/reasoning-tokens)
- [Codex SDK and runtime configuration](https://learn.chatgpt.com/docs/codex-sdk)
