# Independent page research and JSON output

Version 0.19.0 accepts a local crawl folder with `--crawl`; see
[crawl and analysis](CRAWL_AND_ANALYZE.md) for the two-stage workflow.
It returns JSON without object-store integration and supports model
selection for saved-page comparisons. The mention-first
flow is reusable package code with a saved-page command. It has not replaced
the existing URL crawler's controller.
The page units return extracted data and scored links; the saved-page runner does
not follow links or claim complete site coverage.

The v0.17.0 first-page eligibility gate belongs to the URL crawl controller. This
saved-page command remains an explicit extraction/replay tool: it does not infer
company eligibility from an isolated job, contact or third-party evidence page.
When integrating the page unit into autonomous discovery, admit the target site
once before scheduling these units; retain company-linked external job evidence.

```text
saved Crawl4AI captures
  → MentionPageAgent: nine fact objectives + raw technology mentions + scored links
  → classify_mentions: original sections → eligibility and relationship arrays
  → pinned central catalog lookup
  → complete JSON object returned to the caller
```

`captures.py` contains the shared HTML inventory and crawl manifest reader.
`page_agent.py` and `page_prompts.py` contain the original tested
objective rules, record validation and baseline agents. The lab imports these
through compatibility exports. `mentions.py` adds the new page/classification
flow, `mention_models.py` owns its schemas/prompts, and `analysis.py` processes saved
captures. `page_run.py` supplies the CLI and emits JSON. There is no object-store or
Dagster integration.

## Run

From `corpscout/services/crawler_service`, install `crawler_service` in the existing environment:

```sh
uv pip install --python .venv/bin/python -e crawler_service
.venv/bin/crawler-service-pages \
  --page page_agent_lab/data/one-pass-v2-20260916/fixtures/team_job \
  --page page_agent_lab/data/one-pass-v2-20260916/fixtures/data_job \
  --env-file .env \
  --catalog page_agent_lab/data/mention-flow-20260916/catalog.json \
  --offline-catalog > result.json
```

Each `--page` directory contains `input.json`, immutable `page.html`, and optional
`link-page.html` for the rendered capture. `PageInput.load` verifies the native hash;
the runner verifies the rendered hash. Pages group by their explicit `target_url`,
including job advertisements on external domains. Different companies never share
a classification batch. Page and classification requests share a concurrency cap
of three. The direct model is `deepseek-flash`, with high reasoning, a run budget
of 150 calls and an explicit output allowance of 65,536 tokens.

`DEEPSEEK` supplies the API key. Environment variables override supplied dotenv
files; later files override earlier files. Secrets are never copied into results.
No storage credentials or bucket are needed.

To compare another model, use `--api openrouter --model z-ai/glm-5.3-flash` with
`OPENROUTER_API_KEY` in the environment or supplied dotenv file. Direct DeepSeek
Flash remains the default. `--reasoning-effort` defaults to `high` for either API;
effort labels do not guarantee equal computation across model families.
`--timeout SECONDS` changes the per-request deadline (default 300); record any
changed deadline separately when comparing reliability and wall time.
`--provider PROVIDER` pins an OpenRouter provider without fallback; when omitted,
OpenRouter sorts by latency and allows fallback. Each call records the responding
provider. Both APIs use JSON-object mode with the same schema in the system prompt
and host validation, so a model comparison does not also change schema delivery.

The command writes exactly one JSON object to stdout; progress and library logs go
to stderr. One company produces the complete `company-research-result/1.0` object.
Multiple target companies produce `{ "results": [ ... ] }`, with one complete
object per company. Each company's evidence and classification remain separate.
The Python runner returns the same dictionary.

`--output DIRECTORY` optionally retains local captures, model-call diagnostics,
page results, code snapshots and a manifest. Without it, working files use a
session-local temporary directory and are removed when the command finishes. All
original captures and evidence sections needed to interpret the result are included
in the returned JSON. Redirect stdout to a file when persistence is wanted.

By default the existing catalog loader refreshes a read-only ClickHouse publication
snapshot. `--catalog snapshot.json --offline-catalog` explicitly pins an existing
snapshot. A catalog outage preserves results with a failed lookup, rather than
reporting the names as absent. Exact, case-insensitive and accepted-alias matches
return the central identity. Fuzzy candidates remain ambiguous for review; they
are not automatically equated with the mention. No proposal or observation writes
occur. Candidate metadata is useful for a later identity-resolution improvement.

## Evidence and interpretation

The page stage does not decide whether a technology is eligible or used. It
collects names as written, original section references, actor/job context and a
short contextual summary. The host stores complete original captures and visible
paragraph/list/table text, headers and headings. IDs and hashes are host-owned.
Native and rendered text have separate provenance. Supporting headings and
neighboring source passages are retained even when a model omits them from its
context references. Context is not itself proof of the claimed relationship.
The final pass also receives original page sections cited by company, customer
relationship and described-product records. This preserves client-project context
even when it is far from a technology paragraph. Repeated sections are sent once
per classification batch. The host records these added context references without
rewriting the raw mentions.

Each mention receives a final disposition and an array of relationships. This
distinguishes plans, requirements, actual use, expertise, offered products,
compatibility, product components, client work and partnerships. A job page does
not automatically force role scope. Formats/protocols and generic technical
context remain in the JSON with their excluded disposition. Uncertain or invalid
mentions remain reviewable. Classification never rewrites source text or names.

Classification batches contain at most 20 mentions and 64,000 serialized input
characters. A single oversized context is retained for review rather than silently
truncated. Both stages allow at most two requests for malformed responses; page
collection also checks that technical names occur in the cited sections before
accepting the response. Original failed attempts remain in diagnostics. Unknown,
duplicate or cross-page classification references are held for review. Every
relationship must cite its own primary mention section; additional original
context supplied from the same page is allowed (for example, a list's “Required”
introduction shared by several technology mentions).

## JSON contract

Each company returns an object with schema `company-research-result/1.0`:

- `pages`: all original captures/sections, facts, raw mentions, links and diagnostics.
- `technology_classification`: decisions, explanations, relationships, catalog
  outcomes and original batch attempts, including rejected/partial results.
- `records`: nontechnology facts concatenated across pages, retaining source IDs.
- `coverage`, `missing_information`, `unvisited_links`: explicit processing status;
  an empty result never proves company-wide absence.
- Run/revision IDs, target URL, configuration, model usage and catalog version.

The runner freezes inputs before model calls. `--output` retains those diagnostics.
Entity
deduplication is deliberately lossless at this stage: source records are retained,
not collapsed into one unsupported global claim. Schema/source validation is not
independent semantic verification. Partial results are expected when quotations,
attribution or model output still need review.

Model budget/transport/parse failures are represented as partial page or classifier
results in the JSON. With `--output`, an externally interrupted process checkpoints
its completed page files and manifest; automatic assembly/resume of an interrupted
company run is still future work.

The current URL crawl scheduler, downstream ClickHouse ingestion, technology
proposal workflows, DNS detections and PDF/OCR processing are outside this change.
