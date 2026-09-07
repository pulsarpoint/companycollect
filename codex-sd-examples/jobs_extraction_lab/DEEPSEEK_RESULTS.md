# DeepSeek Flash extraction comparison

**Follow-up without site selectors:** [Native Crawl4AI results](NATIVE_CRAWL4AI_RESULTS.md)
compare DeepSeek on complete `cleaned_html` and `markdown.raw_markdown` outputs.
The experiment below used custom platform adapters and cropped job groups; its
scores do not describe native Crawl4AI preprocessing on full pages.

DeepSeek V4 Flash 0731 completed all 120 requests and substantially improved
complete-record agreement over Liquid on the same jobs. With either clean input
format, DeepSeek matched Astra/low on all six fields for **122/130 jobs (93.8%)**,
compared with **90/130 (69.2%)** for Liquid. This is model agreement, not independently
labelled metadata accuracy.

## Direct comparison on identical jobs

These scores use the same **130 jobs from 35 page groups** where Astra/low, Liquid,
and the full DeepSeek run all succeeded in all three formats. The reference is the
saved `gpt-6-astra` / `low` response for the identical format. Titles are checked
against the HTML title elements, with the correct URL and one returned record
required. Missing jobs count as misses.

| Input | DeepSeek correct titles | Liquid correct titles | DeepSeek all-six agreement | Liquid all-six agreement |
| --- | ---: | ---: | ---: | ---: |
| Original Markdown | 118/130 (90.8%) | 26/130 (20.0%) | 107/130 (82.3%) | 23/130 (17.7%) |
| Structured Markdown | 128/130 (98.5%) | 126/130 (96.9%) | 122/130 (93.8%) | 90/130 (69.2%) |
| Simplified HTML | 130/130 (100%) | 126/130 (96.9%) | 122/130 (93.8%) | 90/130 (69.2%) |

The earlier Liquid title table used 134 jobs; this table also requires a usable
Astra reference, so its title denominator and percentages differ. Its all-six
cohort remains the same 130 jobs. DeepSeek returned every expected URL in this
cohort in every format.

## Full DeepSeek run

The frozen sample contains 151 listings from 40 saved pages: 150 actual jobs and
one talent-pool negative example. Each page contributes a group of up to four
listings, not the entire page. All 654 collected cards still have full-page exports.

| Input | Valid requests | Correct URL + HTML title | All-six agreement on the 146 jobs shared with Astra across formats | Median call | Reported cost |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original Markdown | 40/40 | 134/150 (89.3%) | 119/146 (81.5%) | 13.83 s | $0.012207 |
| Structured Markdown | 40/40 | 148/150 (98.7%) | 138/146 (94.5%) | 7.55 s | $0.007469 |
| Simplified HTML | 40/40 | 150/150 (100%) | 138/146 (94.5%) | 7.51 s | $0.007476 |

All 120 requests succeeded on the first attempt, with no missing jobs, unexpected
URLs, duplicate predictions, or output-limit failures. Granola's talent-pool entry
was correctly excluded in every format. The one historical Astra timeout removes
Dust from the 146-job field-agreement cohort; HTML title checking still includes it.

The full run cost **$0.027152** in reported OpenRouter usage. Every response included
cost. It used 436,592 prompt tokens and 200,791 completion tokens, including 162,079
reported reasoning tokens. The largest completion used 6,427 tokens. These are
observed charges with caching, not a price guarantee for a future run.

## Remaining differences checked against source

- **Browserbase, structured Markdown:** two titles lost their advertised city
  suffixes: `Software Engineer (Dashboard) - New York` and the corresponding
  `- San Francisco` role both became `Software Engineer (Dashboard)`. The suffixes
  occur inside the source title headings. HTML preserved them. Retaining the DOM
  title directly would prevent these edits.
- **Airtable, simplified HTML:** two cards with locations `Remote-US` and
  `Remote - US` retained their locations but returned null workplace type. Astra
  returned `Remote`; DeepSeek's Markdown response did too.
- **Letta, both clean formats:** DeepSeek left workplace type null for four jobs;
  Astra inherited `Fully in-person` from the preserved board description. Liquid
  also left it null. Make the intended inheritance policy explicit before treating
  this as a fully labelled correctness test.
- **Unstructured, both clean formats:** two cards read
  `Public Sector • Remote • Full time`. DeepSeek returned location `Remote` in
  Markdown and null in HTML; Astra did the reverse. Both DeepSeek variants therefore
  disagree with their same-format reference. This illustrates why reference
  agreement should not be presented as independent accuracy.

These account for all eight record disagreements in each clean format. The clean
outputs have no literal per-card grounding flags, but that check cannot detect
missing fields, title truncation, or every assignment mistake.

DeepSeek handled two earlier Liquid problem cases well in both clean formats:
Encord's `Special Projects, GTM` titles stayed separate from Account Executive /
Account Manager metadata, and PlanetScale's neighboring support roles retained
their different locations. Original Markdown still caused title-boundary errors,
including appended department text on Railway and metadata on Resend and Encord.

Both clean formats again tie on complete-record agreement. HTML preserves two more
titles; Markdown correctly supplies the two Airtable workplace values. This single
development run does not establish a universal format winner.

## Controls and provider diagnostics

- Model: **`deepseek/deepseek-v4-flash-0731`**, pinned to **`baidu/fp8`**, with provider
  fallback disabled. All 120 responses reported Baidu and the requested model ID.
- Requested reasoning: enabled, `low`, with reasoning text excluded from the
  returned content. Excluding the text does not disable reasoning computation.
- Output budget: 32,768 tokens; temperature zero; strict JSON schema; 180-second
  total request deadline; at most two attempts; sequential calls with a three-second
  gap; the first format rotates by page, as in the earlier experiment.
- Identical saved inputs, prompt, examples, and schema. No new crawling or prompt
  tuning during this run. Existing Astra/low and Liquid outputs were reused.

The initial `formats-deepseek-v1` run used `deepinfra/fp8`. It saved **39 outcomes:
38 successes and one upstream HTTP 429**. An Airtable request also took 159.2 seconds.
After a separate three-request Baidu pilot succeeded, DeepInfra was stopped between
requests. The completed Baidu run starts from the first page; provider results are
not merged. DeepInfra also returned the full Contentsquare department path from
HTML where the Baidu response used the requested nearest group. Provider and run
variation remain relevant even with temperature zero.

All **162 new DeepSeek outcomes** are retained. Their combined reported charge is
**$0.044262**; the failed DeepInfra request has no usage, so its charge is unknown.
The partial DeepInfra run and Baidu pilot appear as separate diagnostic rows in the
generated comparison. Historical GLM routing failures remain separate too.

OpenRouter's [model page](https://openrouter.ai/deepseek/deepseek-v4-flash-0731) and
[endpoint catalog](https://openrouter.ai/api/v1/models/deepseek/deepseek-v4-flash-0731/endpoints)
were checked before the run and saved locally. The catalog describes a mixture of
experts with 13B active parameters out of 284B total; this is a different model scale
from Liquid's 2.6B despite the low measured API charge.

## Reproduce and inspect

```bash
.venv/bin/python -m jobs_extraction_lab.format_benchmark \
  --data-dir jobs_extraction_lab/data/format-comparison-v1 \
  --run-id formats-deepseek-baidu-v1 --env-file jobs_extraction_lab/.env \
  --codex-bin /Applications/ChatGPT.app/Contents/Resources/codex \
  --model deepseek --deepseek-provider baidu/fp8 \
  --timeout 180 --attempts 2 --interval 3

PYTHONPATH=. .venv/bin/python \
  jobs_extraction_lab/data/format-comparison-v1/deepseek-audit.py
```

Reusing the run ID reads the saved results. Choose a new run ID for fresh inference.
The audit rebuilds the combined report, verifies response/input hashes and prompt
parity, and writes the shared-model cohort and differences.

- [Shared-cohort audit](data/format-comparison-v1/deepseek-audit.json)
- [Combined report](data/format-comparison-v1/comparison.md)
- [Full outcomes and evaluations](data/format-comparison-v1/comparison.json)
- [Frozen inputs and source audit](data/format-comparison-v1/manifest.json)
- [Original format experiment and limitations](FORMAT_RESULTS.md)

Validation: 39 existing tests passed; Ruff, type checking, and diff whitespace
checks passed. The report verified all 408 included outcome hashes, including the
162 new DeepSeek outcomes. The 120 frozen inputs were reused without changes.
