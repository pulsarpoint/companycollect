# Independent page research and complete S3 results

Version 0.16.0 implements the mention-first flow as reusable package code and a
saved-page command. It has not replaced the existing URL crawler's controller.
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
  → complete result.json per company/revision → verified RustFS object
```

`page_agent.py` and `page_prompts.py` contain the original tested page inventory,
objective rules, record validation and baseline agents. The lab imports these
through compatibility exports. `mentions.py` adds the new page/classification
flow, `mention_models.py` owns its schemas/prompts, `page_run.py` runs saved captures,
and `s3_results.py` uploads complete results. There is no dependency on Dagster.

## Run

From `codex-sd-examples`, install `company_research` in the existing environment:

```sh
uv pip install --python .venv/bin/python -e company_research
.venv/bin/company-research-pages \
  --page page_agent_lab/data/one-pass-v2-20260916/fixtures/team_job \
  --page page_agent_lab/data/one-pass-v2-20260916/fixtures/data_job \
  --output company_research/data/mention-run-example \
  --env-file jobs_extraction_lab/.env \
  --env-file ../corpscout/services/dagster_v3/.env \
  --bucket crawls
```

Each `--page` directory contains `input.json`, immutable `page.html`, and optional
`link-page.html` for the rendered capture. `PageInput.load` verifies the native hash;
the runner verifies the rendered hash. Pages group by their explicit `target_url`,
including job advertisements on external domains. Different companies never share
a classification batch. Page and classification requests share a concurrency cap
of three. The direct model is `deepseek-flash`, with high reasoning, a run budget
of 150 calls and an explicit output allowance of 65,536 tokens.

`DEEPSEEK` supplies the API key. RustFS uses the existing `CORPSCOUT_S3_ENDPOINT`,
`CORPSCOUT_S3_ACCESS_KEY`, `CORPSCOUT_S3_SECRET_KEY`, optional `CORPSCOUT_S3_REGION`,
and path-style addressing. Environment variables override supplied dotenv files;
later files override earlier files. Secrets are never copied into result settings.
The runner does not create buckets. Omit `--bucket` for local output.

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

## Durable output

Each company gets `result.json` with schema `company-research-result/1.0`:

- `pages`: all original captures/sections, facts, raw mentions, links and diagnostics.
- `technology_classification`: decisions, explanations, relationships, catalog
  outcomes and original batch attempts, including rejected/partial results.
- `records`: nontechnology facts concatenated across pages, retaining source IDs.
- `coverage`, `missing_information`, `unvisited_links`: explicit processing status;
  an empty result never proves company-wide absence.
- Run/revision IDs, target URL, configuration, model usage and catalog version.

The runner freezes input files and Python source before model calls. Entity
deduplication is deliberately lossless at this stage: source records are retained,
not collapsed into one unsupported global claim. Schema/source validation is not
independent semantic verification. Partial results are expected when quotations,
attribution or model output still need review.

Uploads use `company-research/{run_id}/{revision_id}/result.json` in the selected
existing bucket. A conditional create prevents overwrites. The client reads the
object back and verifies its SHA-256 before issuing `upload-receipt.json`. An
identical retry verifies the existing object; a different payload at the same key
fails. A changed result must get a new revision. Local results remain available
when an upload fails:

```sh
.venv/bin/company-research-upload /absolute/path/result.json \
  --env-file ../corpscout/services/dagster_v3/.env --bucket crawls
```

Model budget/transport/parse failures are represented as partial page or classifier
results and can be uploaded. An externally interrupted process checkpoints its
completed page files and manifest; automatic assembly/resume of an interrupted
company run is still future work. Completed company submissions remain durable.

The current URL crawl scheduler, downstream ClickHouse ingestion, technology
proposal workflows, DNS detections and PDF/OCR processing are outside this change.
