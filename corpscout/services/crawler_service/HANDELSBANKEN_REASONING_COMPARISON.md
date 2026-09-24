# Handelsbanken: DeepSeek low versus high reasoning

16 September 2026. **Completed. High improved selected technology retention, but did not improve link-assessment reliability and still admitted unsuitable technology proposals.**

For this workload, high is promising for the technology workflow. I would keep low for routine link assessment and fix the remaining scope and taxonomy rules before adopting high as a global default. The experiment uses high explicitly; application defaults are unchanged.

## Test design

All model requests use `deepseek-flash` directly through `api.deepseek.com`, with thinking enabled. The API documents this alias as DeepSeek V4.1 Flash and supports `reasoning_effort="low"` and `"high"`. [Model and pricing](https://api-docs.deepseek.com/quick_start/pricing/), [reasoning settings](https://api-docs.deepseek.com/guides/thinking_mode/).

The comparison has three parts:

1. **Technology workflow:** two fresh runs over the same 16 IT job advertisements, using the same native Crawl4AI HTML, frozen technology catalog, version 0.15.2 implementation, prompts, stable page order and budgets. Each runs extraction, description validation, normalization, source review, catalog tools and proposal review. Both use a 300-second request deadline, one HTTP attempt, the existing bounded application corrections, 65,536 maximum output tokens and batches of eight statements. The historical low run is retained separately; it is not the fresh comparator because its scope-validation implementation and grouping differed.
2. **Company extraction and crawling decisions:** replay 44 original requests with only the request body's reasoning effort changed: one site classification, 18 HTML extraction windows from 12 pages, and 25 link-assessment requests. These cover 750 candidate occurrences and 658 distinct candidate URLs. The original context, including any corrective feedback, is frozen. This tests individual decisions, not what a new autonomous crawl would visit. Historical low used a 120-second deadline; high uses 300 seconds.
3. **External links:** assess the same 507 saved occurrences in the same 14 batches. The historical low run and high replay receive identical prompts and contexts. They cover 100 full destination URLs and 27 destination domains.

No company or job pages were fetched again for the model runs, and no catalog proposals or company data were submitted to the backend. The catalog is the same saved 7,981-entry snapshot, not a fresh ClickHouse sync. Documentation was checked separately during the audit.

## Technology workflow

| Measure | Low | High |
|---|---:|---:|
| Captured IT pages processed | 16 | 16 |
| Extracted statements, before description review | 240 | 248 |
| Retained source-read technology identity controls | **31/38** | **36/38** |
| Strict signal/scope controls | 29/38 | 33/38 |
| Pipeline-accepted observations | 107 | 113 |
| Distinct identities after catalog/name normalization | 69 | 73 |
| Unaccepted descriptions | 20 | 9 |
| Pending statement IDs | 13 | 8 |
| API requests | 136 | 141 |
| Completion tokens, including reasoning | 591,643 | 854,694 |
| Reasoning tokens | 472,939 | 720,218 |
| Median metered response time | 11.5 seconds | 16.8 seconds |
| End-to-end workflow time | 33.8 minutes | 54.5 minutes |
| Estimated known cost | **$0.500** | **$0.720 + unknown timeout cost** |

The pending categories overlap and should not be added together. These are pipeline acceptance and selected retention measures, not precision scores or counts of deployed products. Both runs produce zero company-held certificate observations from these job ads; candidate credentials are not promoted to the bank.

High recovers six controls missing in fresh low: **SAS**, **Java and Jakarta EE as separate identities** in the fullstack ad, **Maven**, **DB2**, and **Microsoft SQL Server** on the second-page Data Platform Developer ad. It retains separate usage and experience observations for Microsoft 365 and Exchange. Its one regression within the controls is **Ab Initio**.

Two high controls still fail:

- **Azure Pipelines:** the model finds the team-expertise statement. Job normalization forces role scope, then source review correctly identifies team scope and rejects the mismatch. This is a conflicting pipeline rule; high reasoning does not resolve it automatically.
- **Ab Initio:** extraction and source review succeed. The proposal is held because it retains an unverified vendor URL and product characterization after correction. The source-derived identity should survive independently of incomplete global catalog metadata.

Higher reasoning also misses valid names outside the 38 controls that low retained, including OneLake, Power BI and Data Factory. It incorrectly accepts **DORA** as a technology even while its own proposal describes it as a regulation. DORA is an EU regulation, not a software product. [Official text](https://eur-lex.europa.eu/eli/reg/2022/2554/oj). High also admits generic VPN; both admit WCAG and Private Endpoints. Low admits SOAP and a combined Java/Jakarta EE proposal. These illustrate why larger observation counts do not establish better precision. The comparison's raw outputs remain unchanged, and `manual-review.json` records these qualifications.

High hit one 300-second timeout during source validation; its application-level retry succeeded in 68.6 seconds. No output-token-limit stops occurred. The reported high wall time includes the timeout; the cost estimate excludes that request because no usage was returned.

| Model stage | Low calls / estimated cost | High calls / estimated cost |
|---|---:|---:|
| Page statements and statement evidence repair | 18 / $0.1242 | 19 / $0.2018 |
| Description review | 46 / $0.1411 | 44 / $0.1829 |
| Additional evidence repair | 1 / $0.0033 | 0 / $0.0000 |
| Normalization | 24 / $0.0520 | 24 / $0.0732 |
| Source/interpretation review | 7 / $0.0758 | 10 / $0.1087 + unknown timeout |
| Catalog tool conversations | 34 / $0.0570 | 37 / $0.0749 |
| Proposal review and metadata repair | 6 / $0.0462 | 7 / $0.0786 |

Most post-extraction batches currently run sequentially. Parallelizing independent reviews is a separate runtime optimization; it was not introduced halfway through this comparison.

## Company-page and navigation replays

| Measure | Low | High |
|---|---:|---:|
| Valid site classifications | 1/1 | 1/1 |
| Extraction windows passing the full response schema | 18/18 | 16/18 |
| Records passing source-presence checks before repairs | 156 | 199 |
| Distinct source-matched contact values | 2 | 7 |
| Source-matched phone values | 0 | 0 |
| Link-assessment requests passing the full schema | 22/25 | 21/25 |
| Valid candidate assessments | 689/750 | 659/750 |
| Completion tokens, including reasoning | 316,521 | 557,855 |
| Reasoning tokens | 148,384 | 375,733 |
| Median response time | 23.7 seconds | 49.7 seconds |
| Estimated cost for these 44 requests | $0.245 | $0.392 |

The source-presence counts include repeated facts, page windows and explicit negative statements. They are not distinct verified company facts. Four of high's five additional contact values are social profiles. The remaining addition is a contact/support landing page classified as a form, which needs semantic review. Neither setting fixes the telephone attribution bottleneck in these initial outputs.

Both classify Handelsbanken as a company providing banking services. Both recognise the Swedish vacancies page as valuable for jobs; high raises the tech-careers page's technology potential from medium to high. These results do not establish improved autonomous navigation. Group ownership pages absent from the supplied candidate batches cannot benefit from stronger reasoning on those batches.

High's EFN ownership statement is grounded in the source, but it fills `ownership_scope` with `unspecified` for an `owns_brand` relationship. The application requires that field to be null for that relationship, so it rejects the record. This illustrates the difference between losing a valid fact to an application rule and inventing a fact. High also produces two invalid JSON responses in the navigation replay; raw failures are preserved.

## External-link interpretation

| Measure | Low | High |
|---|---:|---:|
| Occurrences passing assessment checks | 507/507 | 507/507 |
| Contextual hints | 457 | 386 |
| Explicit-text assessments | 50 | 108 |
| Unknown-basis assessments | 0 | 13 |
| Repeated-context groups with inconsistent relationship labels | 10/39 | 18/39 |
| Completion tokens, including reasoning | 127,251 | 152,957 |
| Median response time | 29.0 seconds | 42.4 seconds |
| Estimated cost | $0.091 | $0.107 |

High changes 77 relationship labels. One useful change removes six speculative customer hints from Jobylon-to-employer website links. Both runs correctly keep their assessments separate from verified company relationships.

Other changes are ambiguous. Country and global-site footer links move among `group_company`, `other_business` and `unknown`; developer documentation can move to `technology_provider`. Some uncertainty comes from the current schema mixing **link purpose** with **business relationship** in one label. A careers page can be both a recruitment destination and a same-group website.

The consistency diagnostic groups links with the same source registrable domain, destination URL, anchor, heading, nearby text and page region. Source page URLs and other supplied fields may still differ. It measures stability under repeated contexts, not correctness. Likewise, schema/evidence success for all 507 occurrences is not a claim of perfect accuracy. Map credits on Jobylon pages describe that hosting page's map providers, not the bank's technology stack.

## Interpretation and reproducibility

One run per effort is insufficient for a statistical conclusion. Application validation rules, record grouping, catalog coverage and subjective taxonomy choices all affect acceptance. The 38 frozen positive controls measure selected technology retention; they are not a complete precision/recall benchmark.

Signal controls also need interpretation. The Azure Pipelines source describes team expertise, while its frozen control allows only usage or required experience; high's normalization label is defensible, although its observation later fails the role/team scope conflict. Fabric/Databricks are described in a target-architecture implementation context, which does not establish bank-wide production maturity. High retains TeamCity only as mentioned on the fullstack ad, below the stronger signal expected by its control. The controls are preserved unchanged.

Costs are estimates from returned token usage and the documented off-peak tariff, not provider bills. A request without returned usage has unknown cost. Wall times come from simultaneously run job arms; request timing can vary with endpoint load. The original full company analysis and its costs are in [HANDELSBANKEN_ANALYSIS.md](HANDELSBANKEN_ANALYSIS.md).

Artifacts are under `data/handelsbanken-20260916-reasoning/`. `comparison.json` contains both final technology outputs, control results, usage by task, all decision comparisons and changed external-link assessments. The executed harness and application snapshot are frozen there. `low/` and `high/` contain native HTML, statements, reviews, tool calls and final results. `decisions-high/` preserves every historical request and high reply; `links-high/` preserves the complete low and high inventories. `manual-review.json` records the source-audit qualifications above. All paired input, configuration and prompt-integrity checks pass.

To reproduce on the saved dataset, use a new output directory from the service's `corpscout/services/crawler_service` directory:

```sh
.venv/bin/python benchmarks/compare_reasoning.py --part jobs --output data/handelsbanken-reasoning-repeat
.venv/bin/python benchmarks/compare_reasoning.py --part decisions --output data/handelsbanken-reasoning-repeat
.venv/bin/python benchmarks/compare_reasoning.py --part links --output data/handelsbanken-reasoning-repeat
.venv/bin/python benchmarks/audit_reasoning.py data/handelsbanken-reasoning-repeat
```

The harness reads only `DEEPSEEK` from the service's `.env` (override with `--env-file`). It does not save credentials in the output.
