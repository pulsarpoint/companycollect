# Job-list extraction experiment

Compare the existing Codex SDK extraction approach with exactly
`liquid/lfm-2.5-2.6b:free` through OpenRouter, using the same frozen Markdown,
instructions, and output schema. This folder reuses the existing Python environment
and Crawl4AI browser setup; it does not change the other examples.

The completed corpus is in `data/manifest.json`, with 40 Markdown files and their
rendered HTML. Results for the full experiment are in
[`data/runs/jobs-v2/comparison.md`](data/runs/jobs-v2/comparison.md); detailed
disagreements, failures, and usage are in the adjacent `comparison.json` and backend
directories. See [RESULTS.md](RESULTS.md) for interpretation and checked examples.

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
  --backend codex --run-id jobs-v2 --concurrency 4 \
  --codex-bin /Applications/ChatGPT.app/Contents/Resources/codex

.venv/bin/python -m jobs_extraction_lab.main run \
  --backend openrouter --run-id jobs-v2 --concurrency 2 \
  --env-file jobs_extraction_lab/.env

.venv/bin/python -m jobs_extraction_lab.main compare --run-id jobs-v2
```

These commands resume saved outcomes. Use a new `--run-id` for a new experiment or
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
routing, and a maximum of 8,192 output tokens. The exact free endpoint requires
reasoning. `reasoning.exclude=true` hides reasoning text, but reasoning still
consumes the output budget. There is no fallback to another or paid model.

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
  --backend openrouter --run-id jobs-v2-repeat --concurrency 2 \
  --timeout 300 --attempts 3 --max-tokens 8192 \
  --env-file jobs_extraction_lab/.env

.venv/bin/python -m jobs_extraction_lab.repeat \
  --first-run jobs-v2 --second-run jobs-v2-repeat
```

The repeat comparison verifies identical settings and input hashes before comparing
the outcomes. It distinguishes raw-response equality, parsed-record equality,
ordering changes, evidence changes, and changes to the six extracted fields. Failed
responses remain failures; two failed pages are not counted as matching empty lists.
The comparison requires both passes to have an outcome for every corpus page.

Results are in `data/runs/jobs-v2-repeat/repeat-comparison.md` and the adjacent JSON
file. [REPEAT_RESULTS.md](REPEAT_RESULTS.md) explains the failure patterns and
repeatability findings. Codex was not rerun for this repeatability test.

## Validation

```bash
.venv/bin/python -m unittest jobs_extraction_lab.tests
uvx ruff check jobs_extraction_lab
uvx ty check jobs_extraction_lab
```

Tests exercise strict output validation, source grounding, exact OpenRouter request
parameters, credential redaction, bounded retries, changed-input detection, and
comparison coverage when a backend fails.

## Crawler review

[DESIGN_NOTES.md](DESIGN_NOTES.md) covers the existing sitemap-selection and crawl
code, preserving complete snapshots, handling external career boards, and moving
routine extraction to direct inference requests.

## References

- [OpenRouter model](https://openrouter.ai/liquid/lfm-2.5-2.6b:free)
- [Structured outputs](https://openrouter.ai/docs/guides/features/structured-outputs)
- [Reasoning token accounting](https://openrouter.ai/docs/guides/best-practices/reasoning-tokens)
- [Codex SDK and runtime configuration](https://learn.chatgpt.com/docs/codex-sdk)
