# Novelic: Jev decisions and DeepSeek Flash extraction

Measured 26 September 2026 using the immutable full crawl of `novelic.com` from 18 September. This is an offline v2 prototype, not a deployed change or a new live crawl.

## Recommendation

Start with **Jev 1.13 for site scope and fetched-page usefulness**, and **DeepSeek Flash for link selection and extraction**. The fixed 68-page comparison costs approximately **$0.149**, versus **$0.191** for non-thinking Flash throughout: about **22% less**.

Using Jev for every routing decision lowers the fixed-input total to **$0.142**, but its link selector rejected useful service pages. That configuration needs more work before replacing the existing selector.

## Site scope and page decisions

1. Read the homepage and classify its primary purpose: company, online shop, news/media, entertainment, streaming, forum/community, content site, marketplace, directory/search, nonprofit/public, personal, parked, mixed or unknown.
2. For a requested full research crawl, a clearly identified company presentation site permits **whole-site research**. Excluded types receive **first-page information only**. Mixed/unknown sites retain the first-page information and require review. An explicit `full_crawl_all` flag can override exclusions. A basic-only request must remain one page regardless of eligibility for a full crawl.
3. For each fetched page, decide **process / skip / review**, with structured reasons: company identity/purpose, business activities, products/services, contacts/locations, people/ownership, jobs, financials, technologies/credentials and useful navigation. Record negative reasons too: editorial-only, legal boilerplate, wrong operator or unusable capture.
4. Extract facts with Flash after the decision. A navigation-only page can supply links without requiring fact extraction. Empty/error captures retain failed/blocked status rather than invented information.

Jev returns typed reason flags and probabilities, not written explanations or evidence quotations. Exact supporting quotations belong in the extraction output. Confidence is not a calibrated correctness guarantee.

## Method

- 68 complete captured pages; no new browser requests.
- 39 frozen link-selection batches: 215 candidate occurrences, 204 unique candidate URLs.
- The site/page gate comparison uses identical questions and the same complete visible captured text for both models. HTML tags/scripts/styles/templates were removed; visible text was not truncated.
- Flash gates were tested in both high-reasoning and non-thinking modes.
- One common non-thinking Flash extraction was executed on all 68 pages. Its measured cost is included once in each hypothetical configuration; extraction was not repeated for each configuration.
- The legacy homepage classification calls are recorded separately, excluded from v2 totals. The new homepage gate supplies category/scope, and the shared homepage extraction supplies description/purpose.
- Jev's response model was `typesafe/jev-1.13-20260917`; Flash's API model was `deepseek-flash`.
- Enabled saved credentials were read privately. Profiles were not changed and API keys are absent from artifacts.

## Fixed-coverage totals

All configurations below retain the same 68-page extraction corpus. The all-Jev total is **not proof that a live crawl would discover those same pages**.

| Configuration | Calls | Input tokens | Output tokens | USD, off-peak |
|---|---:|---:|---:|---:|
| Flash throughout, thinking disabled | 175 | 778,949 | 162,183 | $0.191460 |
| Jev site/page gates; Flash links and extraction | 175 | 759,312 | 187,797 | $0.149307 |
| Jev all routing; Flash extraction — coverage risk | 175 | 887,764 | 216,120 | $0.141500 |
| Flash high-reasoning decisions; non-thinking extraction | 175 | 781,624 | 315,324 | $0.274903 |

Jev output tokens appear in usage but are free. Token counts use each model's tokenizer: more tokens need not mean higher cost. These totals exclude browser, proxy, compute and storage costs.

## Measured components

| Component | Calls | Input tokens | Output tokens | USD | Sum of request seconds |
|---|---:|---:|---:|---:|---:|
| Flash links, non-thinking | 39 | 122,295 | 13,726 | $0.018338 | 69.7 |
| Jev links | 39 | 250,747 | 42,049 | $0.010531 | 17.5 |
| Flash site/page gates, high reasoning | 68 | 379,528 | 130,272 | $0.127867 | 637.4 |
| Flash site/page gates, non-thinking | 68 | 377,828 | 12,913 | $0.057197 | 87.0 |
| Jev site/page gates | 68 | 358,191 | 38,527 | $0.015044 | 24.7 |
| Shared Flash extraction, non-thinking | 68 | 278,826 | 135,544 | $0.115925 | 415.1 |

Request durations exclude local validation and include overlapping calls; these are not whole-crawl wall-clock times. Average page-gate request: Jev **0.36 s**, non-thinking Flash **1.28 s**, high-reasoning Flash **9.37 s**.

## Quality findings

- Both Flash modes and Jev classified Novelic as a company eligible for whole-site research and selected all 68 captured pages for processing. These were already-selected positive pages from one company. This does **not** demonstrate rejection accuracy for real shops, news or streaming sites.
- Against high-reasoning Flash, Jev rejected **16 of 76** accepted candidate occurrences. Eleven point to pages actually present in the saved crawl; all three page gates found those pages worth processing after reading them.
- Against non-thinking Flash, Jev rejected **23 of 83** accepted candidate occurrences: 60 shared accepts, 132 shared rejects, 23 disagreements, out of 215.
- Useful rejected links included automotive cybersecurity, embedded testing, quality management, R&D activities, antenna design and electromagnetic simulations. Thin URL metadata and the experimental typed prompt may contribute; this is not proof the model cannot improve.
- Jev priority uses five levels, unlike the original integer priority. Its source-navigation choices cannot supply the exact ownership/filing quotations required by the crawler. Those cases need evidence extraction/validation or fallback before production use.
- All 68 successful non-thinking extraction calls produced schema-valid JSON. **3,012 candidate facts** passed exact-substring evidence checks. **41 were rejected** because their quotation was absent or altered.
- Some outputs contain multiple description/purpose entries despite requesting one. They require normalization and semantic/attribution review before enrichment. Schema and quotation checks do not establish semantic correctness, completeness or current ownership/employment.

## Cost accounting and diagnostic attempts

Jev amounts are provider-reported `usage.cost`. Direct DeepSeek does not return dollar charges: its costs were calculated from actual cache-hit/cache-miss input and completion counts using verified Flash prices: off-peak **$0.003/M cached input, $0.15/M uncached input, $0.60/M output**. The test ran on Saturday, which is off-peak. Peak Flash prices are twice these values. Reasoning tokens are included in completion tokens and were not double counted.

Sources: [DeepSeek pricing](https://api-docs.deepseek.com/quick_start/pricing/), [Jev Decisions API](https://openrouter.ai/docs/guides/community/jev-tutorial), [Jev 1.13](https://openrouter.ai/typesafe/jev-1.13/).

Actual recorded experiment spend, including all comparison runs and diagnostics, is approximately **$0.8755**, plus unknown billing for **eight attempts without returned usage**. This is separate from the per-configuration totals above.

Two diagnostics were stopped: the legacy rich extractor sent much larger HTML/observation inputs; compact extraction with high reasoning repeatedly exhausted its 16,384 output-token limit. Seven requests were in flight at cancellation, and one other legacy attempt failed without usage. All returned usage and attempt artifacts were preserved. The final non-thinking extraction completed all 68 pages.

## Artifacts and reproducibility

Artifacts are under this service's `data/` directory:

- [novelic-jev-benchmark-summary-20260926.json](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/crawler_service/data/novelic-jev-benchmark-summary-20260926.json): complete metrics and spend ledger.
- [novelic-page-gates-20260926/cases.json](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/crawler_service/data/novelic-page-gates-20260926/cases.json): all page decisions and detailed reason flags.
- [novelic-jev-deepseek-20260926/candidate-comparison.json](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/crawler_service/data/novelic-jev-deepseek-20260926/candidate-comparison.json): candidate-level disagreements.
- [novelic-compact-extraction-fast-20260926/pages/](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/crawler_service/data/novelic-compact-extraction-fast-20260926/pages): accepted/rejected extracted facts per page.
- [novelic-compact-extraction-fast-20260926/completion.json](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/crawler_service/data/novelic-compact-extraction-fast-20260926/completion.json): completion and quality counts.

Benchmark modules: `benchmarks.compare_jev`, `benchmarks.page_gate_jev`, `benchmarks.replay_deepseek_non_thinking`, `benchmarks.compact_page_extraction`. Run from `crawler_service` using `python -m <module> <source> <new-output-directory>`. Supply credentials as private JSON on stdin, with `deepseek` and `jev` entries containing `api_key` and relevant model/base URL/profile metadata. Never put keys in command arguments, committed files or artifacts. Exact requests/responses are preserved without authorization headers.

Validation: **10 focused tests passed**, covering scope policy, mandatory basic information, explicit overrides, reason flags, billing arithmetic, incomplete responses and secret-free error artifacts. Ruff checks passed. Production pipelines, LLM settings and databases were not changed.
