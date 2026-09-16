# Direct DeepSeek on three additional websites

Completed 12 September 2026, Europe/Belgrade (11 September UTC).

Direct `deepseek-flash` retained more usable observations than the OpenRouter route
on these inputs, with no request timeouts. However, both runs exposed extraction,
review and catalog-completion problems. This supports further testing of the direct
API; it does not establish general model superiority or production readiness.

## What was tested

Six newly fetched pages, two from each company:

| Company | Pages | Native cleaned HTML characters |
| --- | --- | ---: |
| Memgraph | [Homepage](https://memgraph.com/), [careers](https://memgraph.com/careers) | 101,516 |
| Oxide | [Homepage](https://oxide.computer/), [Networking Software Engineer](https://oxide.computer/careers/sw-networking) | 115,822 |
| RT-RK | [Open positions](https://www.rt-rk.com/open-positions/), [industrial IoT gateway case study](https://www.rt-rk.com/one-stop-developer-shopindustrial-iot-gateway-development/) | 68,715 |

All six fetches succeeded with HTTP 200 on the first attempt. Each page was fetched
once with the existing Crawl4AI settings, without site-specific selectors or HTML
postprocessing. Both endpoints received identical saved native `cleaned_html`.
Navigation, cookie text and related links remain in that input.

The test uses package 0.14.4's optional page-statement workflow: extract descriptions
page by page, review against HTML, normalize technology relationships, review source
meaning, then resolve the frozen local catalog or prepare proposals. It tests selected
page extraction, **not autonomous link selection, complete job export, or all company
objectives**. The Memgraph careers snapshot says no positions are open; it remains
useful for interview-tool and company-context extraction.

Both arms used low reasoning, common JSON object mode with the same schema in the
prompt, 65,536 maximum output tokens, 120-second request deadlines, statement batches
of eight, two review attempts and one correction. The per-arm budget was 60 model
calls, with three arms allowed to run concurrently. No arm reached that budget and
no response reported an output-token-limit finish.

- Direct: `https://api.deepseek.com/chat/completions`, `deepseek-flash`, existing
  `DEEPSEEK` key in `jobs_extraction_lab/.env`.
- OpenRouter: `deepseek/deepseek-v4-flash-0731`, Wafer, existing `OPENROUTER_API_KEY`.

These are different model versions and serving stacks. Common JSON object mode also
differs from the crawler's normal OpenRouter strict-schema mode. There was one run
per arm, with no inference about repeatability. Runtime prompts/defaults were unchanged.

## Results

“Source-supported” means the automated source/evidence checks passed. “Accepted” also
requires catalog/proposal validation. Neither means independently verified company use
or administrator approval of a new technology. Counts are **observations**, including
multiple relationships, neutral mentions and some duplicate descriptions.

| Company | API | Extracted descriptions | Reviewed descriptions | Source-supported tech | Accepted tech | After manual specificity holds | Targeted controls |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Memgraph | Direct | 30 | 30 | 12 | 12 | 12 | 1/4 |
| Memgraph | OpenRouter | 12 | 10 | 3 | 0 | 0 | 0/4 |
| Oxide | Direct | 35 | 31 | 21 | 20 | 15 | 5/7 |
| Oxide | OpenRouter | 35 | 33 | 20 | 6 | 6 | 4/7 |
| RT-RK | Direct | 14 | 13 | 6 | 6 | 4 | 3/4 |
| RT-RK | OpenRouter | 12 | 2 | 1 | 0 | 0 | 0/4 |
| **Total** | **Direct** | **79** | **74** | **39** | **38** | **31** | **9/15** |
| **Total** | **OpenRouter** | **59** | **45** | **24** | **6** | **6** | **4/15** |

The 15 controls were selected from the saved source before model calls and never
included in the prompts. They require a particular identity, actor, relationship,
scope and page, sometimes the job title. They are a small diagnostic set, not exhaustive
recall or an accuracy percentage. Before catalog checks, OpenRouter satisfies 6/15.
For example, its AMD EPYC record exists but uses `offers`, whereas that control requires
product incorporation as `stated_use`; a failed control does not always mean a missed name.

Direct has five complete pages and one partial page: Oxide's homepage retained a
source-name/evidence issue. OpenRouter has two complete pages and four partial pages.
These are processing states, separate from a claim that a fact is absent.

### Memgraph

Direct correctly retains Google Meet for interviews, separates customer use of
Memgraph from Memgraph's own use, and keeps competitor names as mentions. It misses
separate NASA Python/R observations and a self-attributed Memgraph product offering,
although those facts remain in descriptions. Developer support for NetworkX/Python/
Cypher must not become proof of internal adoption.

Direct also accepts **ACID as company compliance**. This is a database transaction
property and belongs in product capabilities. The record does not invent an issued
certificate, but its classification is wrong; the manual review holds it. It is the
only automatically accepted credential across these runs.

OpenRouter finds Google Meet, LlamaIndex and Cypher source-supported records, but
returns no complete catalog resolutions for them. Its zero accepted count does not
mean it extracted no facts. Two homepage deadlines, one invalid JSON response and two
empty answer responses reduced usable output.

### Oxide

Direct preserves Rust, C and P4 use under the Networking Software Engineer role.
OpenRouter does better on the experience sentence: it retains both Rust and C with
the alternative group; direct omits the C preference. Direct reduces OPTE to a mention;
OpenRouter identifies its development relationship but catalog processing blocks it.

Direct still accepts broad NVMe, DDR5, IPsec, BGP and VMware identities. The manual
review holds those five from the narrow named-product catalog. Their source claims
are useful hardware capabilities, protocols, job skills or migration context; these
are specificity issues, not fabricated facts. OpenRouter's catalog checks also reject
some broad identities, but drop valid named products and leave 14 supported observations
pending. Pending records cannot all be accepted blindly.

Both models preserve product context for Kubernetes orchestration and AMD processors.
The frozen compatibility control overflags direct's separate Kubernetes product feature;
manual review explicitly qualifies that flag. Product support/incorporation should have
its own relationship, rather than becoming internal deployment or product authorship.

Three direct company descriptions also remain held because extraction leaves the actor
null while review names Oxide. This is a concrete reconciliation gap to fix with evidence.

### RT-RK

Direct retains C++, Linux and OBLO in the client-project context. SGC200 development
remains in company prose instead of a named `develops` observation. Broad “Atlassian
Cloud Tools” is held without guessing a particular application. Bluetooth and an
**unnamed E+H protocol** still become proposals; both are held from the specific-product
catalog. The latter lacks a usable technology identity regardless of taxonomy scope.

OpenRouter times out twice on the jobs list. It recovers 12 case-study statements after
a schema-error retry, but review accepts only two. It actually extracts C++ and Linux:
the reviewer expands their identities to phrases such as “C++ OBLO core” and “Linux based
framework,” causing the structured identity check to reject them. This is a review/
reconciliation failure, not missing source information. Its only supported technology
record is the unnamed protocol, still catalog-pending and unsuitable for promotion.

## Reliability and cost

| Metric | Direct | OpenRouter |
| --- | ---: | ---: |
| HTTP attempts | 60 | 61 |
| Responses with token usage | 60 | 56 |
| 120-second deadlines | 0 | 5 |
| Invalid JSON / empty answer responses | 0 / 0 | 1 / 2 |
| Input tokens | 687,706 | 496,809 |
| Output tokens, including reasoning | 224,386 | 231,396 |
| Reported reasoning tokens | 188,294 | 200,877 |
| Median first-attempt response with usage | 9.85 s | 17.87 s |
| Observed-usage cost | **$0.22055 estimated** | **$0.10603 reported** |

JSON/schema/semantic problems inside later workflow stages are additional to the API
error rows. A response with usage is not necessarily a usable extraction. Latency samples
also contain different mixes of downstream tasks, so this is not a pure speed benchmark.

Direct requests ran during the documented off-peak window. The cost is calculated from
reported cache-hit, cache-miss and output tokens at $0.003/$0.15/$0.60 per million;
the same usage at peak rates would be $0.44110. Direct does not return billed dollars.
OpenRouter's five timed-out calls have unknown usage/cost. Combined observed usage is
approximately **$0.32658**, plus any unknown timeout charges. Pricing was checked on
11 September against [DeepSeek's official pricing](https://api-docs.deepseek.com/quick_start/pricing/).
The inherited metric helper's `pricing_date` is 10 September; the saved live check
confirms the same rates on the run date.

Direct cost more in this run, but it also completed more downstream work. Lower spend
from an incomplete run is not an equivalent-output cost comparison.

## What to do next

1. Replay the saved failed descriptions with bounded actor/identity reconciliation.
   Separate a technology's identity from the longer source phrase that describes it.
   Keep company-attribution checks and source evidence requirements.
2. Add a completeness pass over accepted descriptions: emit each named technology and
   each supported relationship, including alternatives. Use NASA Python/R, Oxide C/P4/
   OPTE and RT-RK SGC200 as regression cases.
3. Separate product support/incorporation, company usage, technical capabilities and
   credentials. ACID and unnamed protocols should not reach the wrong catalog.
4. Retry only catalog-pending valid items, preserving completed resolutions. After those
   corrections, compare direct non-thinking with low reasoning on these frozen pages
   to measure output quality and cost without recrawling.

## Reproduce and inspect

All six runs finished. No backend records were submitted, no technologies were approved,
and no deployment or runtime configuration change was made.

Artifacts are in [data/deepseek-more-websites-20260911](data/deepseek-more-websites-20260911/):
native sources, frozen controls/catalog/code, request and raw-response logs, page and
description reviews, final JSON, `comparison.json`, `manual-review.json` and derived
`reviewed-records.json`. Original outputs remain intact. Manual inspection is targeted,
not an exhaustive independent audit of every quotation.

Offline re-audit (no network calls):

```sh
.venv/bin/python company_research/benchmarks/audit_company_websites.py \
  company_research/data/deepseek-more-websites-20260911
```

For a new experiment, use `compare_company_websites.py fetch --output <new-folder>`,
inspect the saved pages and create `controls.json`, then run `compare` with the same
output path. Existing experiment folders are protected from accidental overwrite.

The audit verifies frozen controls/catalog hashes, both arms' HTML hashes, all 15
control quotations and identical initial prompts. Validation commands and credential
scan results are saved in `verification.json`.
