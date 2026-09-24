# NOVELIC compact-selector model comparison — 17 September 2026

This experiment compares model performance for custom-instruction page selection.
It does not fetch pages, run the first-page company/site-info classifier, or extract
facts from saved pages. Runtime defaults and selection prompts are unchanged.

## Method

Each model receives the same ten frozen NOVELIC candidate batches: **187 candidate
occurrences across 167 unique URLs**, including one repeated correction batch from
the original crawl. The instruction is:

> Collect current job listings and full job descriptions. Skip employee stories and other employers.

The compact DeepSeek reference selects Careers and the 16 listed job URLs: 17
unique selected URLs and 20 positive occurrences. Agreement measures decisions
against that saved reference, not independently verified destination accuracy.
The original crawl and compact crawl separately verified all 16 captured job
sections; see [the compact-selector benchmark](COMPACT_SELECTOR_RESULTS_20260917.md).

All runs request `high` reasoning, JSON-object output, a 65,536-token output limit,
and a 180-second deadline per batch. Batches run serially within each model; the
three model runs start together. OpenRouter uses temperature zero; direct
DeepSeek thinking mode uses its API default. No schema-correction rounds are
performed in this replay. The normal HTTP retry policy remains enabled.

Models/providers:

- Direct DeepSeek `deepseek-flash`, matching the existing compact benchmark.
- [GLM‑5.3](https://openrouter.ai/z-ai/glm-5.3), `z-ai/glm-5.3`, pinned to
  `parasail/fp8`. This is GLM‑5.3, not the GLM Flash model in the earlier fact
  extraction experiment.
- [Qwen3.8 Flash](https://openrouter.ai/qwen/qwen3.8-flash),
  `qwen/qwen3.8-flash`, pinned to `alibaba`.

The public OpenRouter model and endpoint metadata was saved before execution.
Messages, response-format mode and output limits are compared directly across
saved requests; the audit records SHA-256 hashes of every message batch.

## Results

**GLM‑5.3 was fastest in this replay: 37.77% less elapsed time than a fresh
DeepSeek run.** All three models returned valid output for all ten batches and
agreed on all 187 eligibility decisions, selecting Careers plus all 16 job URLs.
There were no extra selections, missed reference selections, HTTP errors or
retries. This is agreement on the frozen inputs, not a new end-to-end crawl.

| Model and provider | Total time | Median API call | Input tokens | Output tokens | Reported cost | Valid batches / agreement |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| DeepSeek Flash, direct | 94.716 s | 8.915 s | 44,715 | 25,485 | Not returned | 10/10; 187/187 |
| GLM‑5.3, Parasail FP8 | **58.939 s** | **5.940 s** | 43,484 | **12,606** | $0.09576928 | 10/10; 187/187 |
| Qwen3.8 Flash, Alibaba | 355.901 s | 35.429 s | 45,242 | 27,949 | **$0.017623962** | 10/10; 187/187 |

GLM used 50.54% fewer completion tokens than fresh DeepSeek. Qwen took 3.76 times
as long as DeepSeek and 6.04 times as long as GLM, while its reported cost was
81.60% lower than GLM. Total reported OpenRouter cost was **$0.113393242**;
the ten direct DeepSeek calls have unknown monetary cost.

Reported reasoning tokens were 14,342 for DeepSeek, 1,178 for GLM and 13,511 for
Qwen. Cached input tokens were 42,880, 18,048 and 17,152 respectively. These
differences help explain why equal requested reasoning effort is not equal work.
Full timing ranges and token/cache accounting are saved in
[`summary.json`](data/selector-model-comparison-20260917/summary.json).

Individual relevance ratings and follow scopes were not identical, even though
eligibility agreed. GLM assigned `target_navigation` instead of `single_page` to
two selected job pages; Qwen and fresh DeepSeek each did so for three. Those
changes can permit further discovery from the job pages, so the replay does not
establish identical downstream crawl paths or page counts. Some excluded partner
pages also received different company-relevance labels. The report records
field-change counts separately.
The earlier compact DeepSeek replay took 87.208 seconds versus 94.716 seconds for
this fresh run, illustrating normal variation between runs.

GLM‑5.3 is the strongest candidate here when selection latency matters. Qwen is
an option when the lower reported cost matters more than latency. Broader
company/instruction checks should precede changing the production default; no
default was changed by this experiment.

## Interpretation and limits

These are observed service response times for one NOVELIC replay, including
provider/network conditions. They do not isolate model architecture or establish
performance on other companies, custom instructions, multilingual navigation,
site classification, or downstream extraction. There are no repeated trials or
cold-cache controls. Tokenizers differ, and reported completion tokens include
reasoning tokens. The same requested effort does not guarantee the same reasoning
budget across providers.

The historical `baseline_usage` inside each per-run `comparison.json` refers to
the original broad-schema calls that supplied the candidates. Use the **fresh
compact DeepSeek run** in this report for model speed/token comparisons; the
historical broad-schema timing would conflate model and prompt changes.

Direct DeepSeek does not return a monetary cost. Its `known_cost_usd: 0` is not a
claim that the calls were free. OpenRouter costs are the totals reported by the
actual responses, including any cache savings.

## Reproduce and inspect

From `corpscout/services/crawler_service`, use a new empty output directory:

```bash
.venv/bin/python benchmarks/compare_selector.py \
  data/novelic-jobs-instructions-20260917/crawl \
  --output-dir data/my-selector-comparison \
  --env-file .env \
  --api openrouter --model z-ai/glm-5.3 --provider parasail/fp8 \
  --reasoning-effort high --timeout 180
```

For Qwen, substitute `--model qwen/qwen3.8-flash --provider alibaba`.
For direct DeepSeek, use `--api deepseek --model deepseek-flash` without `--provider`.
The benchmark requires the appropriate API key, saves every request/response,
validates both the compact schema and exact candidate ID coverage, and keeps
invalid/unavailable batches in the planned candidate denominator. Any invalid
batch causes exit code 2 after saving the comparison.

Local artifacts are under
[`data/selector-model-comparison-20260917/`](data/selector-model-comparison-20260917):
model/endpoint metadata, source snapshots, three run directories, `summary.json`
and the request-parity audit `summarize.py`. Existing benchmark inputs are preserved.

Validation: three benchmark HTTP-boundary tests pass, covering API/provider
routing with message preservation, malformed output accounting and permanent HTTP
failure reporting. The existing package suite passes: 194 tests, six skipped.
Ruff check/format, benchmark type checking and Git whitespace checks pass.
