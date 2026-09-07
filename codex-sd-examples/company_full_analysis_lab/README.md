# Optional full company analysis experiment

**Paused:** see the [research checkpoint](../RESEARCH_CHECKPOINT.md) for saved
PDF/OCR findings and the return to core website extraction. The experiments below
are preserved for later use; they are not prerequisites for the crawler work.

This lab adds broad company research alongside the existing Crawl4AI/DeepSeek
extraction work. The core crawler still owns structured technologies, jobs,
services, contacts and company observations. This experiment produces an
additional narrative report; it does not insert claims into ClickHouse or alter
the crawler's extraction policy.

PDF follow-ups: [local OCR comparison](PDF_BENCHMARK_RESULTS.md) and
[Mistral OCR through OpenRouter with DeepSeek](PDF_OPENROUTER_RESULTS.md).

The initial comparison is NOVELIC. `prompt.txt` preserves the exact user prompt
from the completed task **Analyze Novelic company**
(`01a07c8e-ebaf-7fa2-8dfc-f935cc279dd8`). `reference/` freezes its report and
source index, with hashes and provenance. It is an Astra reference attributed
by the user, not verified ground truth. The task export did not supply reliable
model effort, token usage or price.

## Run

From the `codex-sd-examples` directory:

```sh
.venv/bin/python -m company_full_analysis_lab.run --effort high
```

The runner uses the existing `openai_codex` SDK, the desktop CLI runtime, and
`deepseek/deepseek-v4-flash-0731` through OpenRouter's Responses API. Credentials
come from `OPENROUTER_API_KEY` or `jobs_extraction_lab/.env`; a local forwarding
process holds the key instead of adding it to the research agent's environment.
No global Codex configuration is edited. Unrelated integrations, memories,
project instructions, hooks and delegation are disabled for this invocation.

The user prompt is unchanged. Additional developer instructions establish public
research scope, citation requirements, and writing `report.md`. They do not
specify research stages, topics, preferred sources, or Astra's discoveries. The
agent works in a fresh temporary directory and gets no reference files.

Web search is requested through the Responses API and executed by OpenRouter.
The model can use the SDK's shell tools to fetch and parse web pages/PDFs, inspect
cached artifacts, and calculate values. The lab does not prescribe Crawl4AI for
this exploratory run. This is an end-to-end research-system comparison, not a
controlled model-only comparison with identical desktop tools. In particular,
Astra used the desktop web tools and visual PDF inspection.

For a future matched SDK Astra run:

```sh
.venv/bin/python -m company_full_analysis_lab.run \
  --provider codex --model gpt-6-astra --effort low
```

That command is available but is not part of the initial completed-reference
comparison unless its output is explicitly recorded. For another company,
provide a new `--prompt-file`. A compatibility probe uses `--smoke-prompt` and
must be excluded from benchmark results.

## Evidence and limits

Each `data/<provider>-<timestamp>/` contains:

- `prompt.txt`, `developer-instructions.txt`, and `run.json`: exact prompt,
  effective run configuration, completion state, duration, and usage.
- `report.md`, `final-answer.md`, and `workspace/`: final report and source
  artifacts the agent chose to save. A missing file falls back to the final
  answer and is explicitly flagged; inspect this before treating it as a report.
- `actions.jsonl`: public SDK actions and tool results. Private reasoning is
  excluded, including reasoning nested inside completed-turn payloads.
- `requests.jsonl`: request metadata and available tool names, without API keys.
- `provider-events.jsonl`: public provider tool actions, citations and usage.
  This captures OpenRouter searches that the SDK's older item schema omits.

Reported OpenRouter `usage.cost` already includes server-tool charges; do not
add search charges to it again. Missing cost is unknown, not zero. Calls ending
without a completed response may have unreported charges. The one-hour wall-time
guard is an operational limit, not an output-token limit. The runner does not
set `max_output_tokens`. A timed-out or failed run is never a completed report.

Evaluate claim coverage and source support separately from length or URL count.
Compare entity identity, ownership scope, dated financials, subsidiary versus
parent measures, product evidence, certifications, jobs, named tools, customers
versus partners, risks, and explicit unknowns. Verify disagreements against
primary sources. One company and one run cannot establish a general win rate.

```sh
.venv/bin/python -m unittest company_full_analysis_lab.tests
uvx ruff check company_full_analysis_lab
uvx ty check company_full_analysis_lab --python .venv/bin/python
```
