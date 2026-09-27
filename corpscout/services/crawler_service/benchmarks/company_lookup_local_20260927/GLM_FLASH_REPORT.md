# GLM 5.3 Flash — Swedish website-to-company benchmark

27 September 2026. **25 websites attempted; $0.0697447675 in reported LLM charges.** The separate 12-request concurrency pilot cost $0.013119529, bringing the measured benchmark total to **$0.0828642965** (about 8.3 US cents). A single model-verification preflight is excluded; its cost was not captured. Browser, server and database infrastructure costs are excluded.

## Method

The exact same frozen 25-site cohort was submitted to the live read-only company-lookup endpoint, with GLM for classification, identity extraction, matching and industry assessment. No Jev was used. Two sites ran concurrently, with up to four pages and 20 model calls per site. The saved model was `z-ai/glm-5.3-flash`, profile revision 1, with reasoning effort left at its saved default (`null`). OpenRouter routing retained the production settings: latency preference, fallbacks allowed, required-parameter support.

This tests website ownership matching, not an unrestricted deep crawl. The current operator-lookup policy includes shops and content sites and limits discovery to company/contact/legal evidence. The older local-Qwen baseline predated this policy and excluded Byggmax. Live page content, provider routing and load also varied, so the full-run comparison is not a controlled model-only experiment.

All registry queries were read-only. No company/domain associations were saved in ClickHouse or PostgreSQL, and no lookup results were uploaded to S3. The crawler retained its normal local request history and debug artifacts.

## Tokens and cost

| Metric | 25-site run | Separate concurrency pilot |
|---|---:|---:|
| Model requests | 80 | 12 |
| Input tokens | 255,730 | 79,488 |
| Cached input tokens, included above | 640 | 15,808 |
| Output tokens | 74,451 | 10,817 |
| Reported reasoning tokens, included in output | 35,392 | 0 |
| Reported cost, USD | $0.0697447675 | $0.013119529 |
| Requests missing cost metadata | 0 | 0 |

The average is **$0.002790 per attempted website**, or **$2.79 per 1,000** at this sample's mix. Three websites failed before any model request; among the 22 that reached GLM, the mean is $0.003170, or $3.17 per 1,000. These are sample extrapolations, not a forecast for arbitrary sites.

Costs sum each response’s `usage.cost`, including paid work from failed or unfinished requests. Lifco’s completed model output was recovered from its debug trace before cancellation, preserving its $0.0010824325 charge. Corem’s failed generation reported a zero charge. Two authenticated generation-record checks confirmed the zero charge and one successful charge; all 79 charged calls reconcile with the captured provider rates. The 80th call is the uncharged Corem error.

The endpoint price snapshot came from [OpenRouter’s GLM endpoint API](https://openrouter.ai/api/v1/models/z-ai/glm-5.3-flash/endpoints). Cost verification uses its [generation metadata endpoint](https://openrouter.ai/docs/api/api-reference/generations/get-generation). Prices below are the routes actually observed, per million tokens; they are not a claim that all GLM routes have these prices.

| Provider | Calls | Input / M | Output / M | Cached input / M | Run cost | Reported reasoning tokens |
|---|---:|---:|---:|---:|---:|---:|
| Modal | 28 | $0.15 | $0.5 | $0.03 | $0.04062670 | 35,392 |
| Decart | 52 | $0.1275 | $0.425 | $0.0255 | $0.02911807 | 0 |

Decart’s snapshot includes a 15% discount. Modal reported reasoning tokens; Decart reported zero and returned no separate reasoning text in the inspected responses. The saved default therefore does not yield uniform observed reasoning behavior across routes. All concurrency-pilot responses used Decart.

| Stage | Calls | Input tokens | Output tokens | Reported reasoning tokens | Cost, USD | Sum of model request seconds |
|---|---:|---:|---:|---:|---:|---:|
| Classification | 22 | 60,808 | 13,374 | 5,678 | $0.01464950 | 172.4 |
| Identity | 22 | 134,111 | 32,237 | 10,578 | $0.03291247 | 430.4 |
| Match | 20 | 40,496 | 6,977 | 3,869 | $0.00884456 | 112.3 |
| Industry | 16 | 20,315 | 21,863 | 15,267 | $0.01333824 | 451.2 |

## Outcomes and observed problems

**15 requests completed with accepted matches:** 14 reference IDs and the independently supported Mangold operating company. **One additional matching result, Lifco, was computed correctly but its request timed out during cleanup.** Four cases remained unresolved and five failed. Three of the failures happened before the model was called. No incorrect accepted ID was identified in the reviewed sample; this is not a population-level accuracy estimate.

- **Stendörren:** false negative in the final GLM answer. The saved request included the exact organisation number, matching registry candidate and address, but the response incorrectly claimed matching registry data was absent.
- **Avanza:** both the operating bank and holding-company IDs were extracted. GLM proposed the bank, and the production conflict guard required review.
- **NCC:** proposed match lacked sufficient accepted legal-name/address evidence; the guard rejected it.
- **Brinova:** same-name company ambiguity lacked an organisation number or corroborating address; the guard rejected automatic acceptance.
- **Norion / Studsvik / Elon Group:** homepage fetch failures. Norion returned a DNS-resolution error, Studsvik a certificate-name error, and Elon Group failed before any model call.
- **Corem:** OpenRouter/Modal returned HTTP 200 with `finish_reason: error`; identity extraction correctly failed.
- **Latour:** the name-search query returned HTTP 408 after 15.15 seconds. GLM classification and identity extraction had completed.
- **Lifco:** result `5564653185` was computed at 14:49:15 UTC in 86.115 seconds. The outer request continued heartbeats after the final lookup result. At the existing 600-second test deadline it was cancelled; the browser lease release returned HTTP 200. Its original computed output, cancellation result and both traces are retained. The exact cleanup wait still needs diagnosis.
- **Byggmax:** classified as an online store and matched to the company explicitly named in its footer, `5566563531`. This verifies the revised operator-lookup policy for this shop.

There were 52 database queries: 17 ID searches, 15 name searches and 20 industry queries. All but Latour’s name query succeeded.

## Per-site results

Time below is lookup execution time. Lifco’s separate request lifetime was 601 seconds. Each website link opens its Backoffice trace; the Lifco final API result is the cancellation record, while the earlier debug event contains the matching result.

| Website / trace | Outcome | Accepted or computed ID | Seconds | Input tokens | Output tokens | Cost, USD |
|---|---|---|---:|---:|---:|---:|
| [addtech.se](http://localhost:5183/admin/crawls?tab=attempts&domain=addtech.se&source=rest&debug_request=lookup-local-563edc45cd60-01&debug_attempt=1) | Reference matched | 5563029726 | 44.9 | 12,291 | 3,876 | $0.0037817 |
| [jm.se](http://localhost:5183/admin/crawls?tab=attempts&domain=jm.se&source=rest&debug_request=lookup-local-563edc45cd60-02&debug_attempt=1) | Reference matched | 5560452103 | 32.7 | 7,422 | 3,529 | $0.0028778 |
| [stendorren.se](http://localhost:5183/admin/crawls?tab=attempts&domain=stendorren.se&source=rest&debug_request=lookup-local-563edc45cd60-03&debug_attempt=1) | Unresolved | — | 98.1 | 14,044 | 8,246 | $0.0062296 |
| [neobo.se](http://localhost:5183/admin/crawls?tab=attempts&domain=neobo.se&source=rest&debug_request=lookup-local-563edc45cd60-04&debug_attempt=1) | Reference matched | 5565802526 | 146.3 | 21,332 | 9,187 | $0.0077933 |
| [norion.se](http://localhost:5183/admin/crawls?tab=attempts&domain=norion.se&source=rest&debug_request=lookup-local-563edc45cd60-05&debug_attempt=1) | Failed | — | 1.4 | 0 | 0 | $0.0000000 |
| [instalco.se](http://localhost:5183/admin/crawls?tab=attempts&domain=instalco.se&source=rest&debug_request=lookup-local-563edc45cd60-06&debug_attempt=1) | Reference matched | 5590158944 | 181.6 | 19,184 | 9,266 | $0.0075106 |
| [studsvik.se](http://localhost:5183/admin/crawls?tab=attempts&domain=studsvik.se&source=rest&debug_request=lookup-local-563edc45cd60-07&debug_attempt=1) | Failed | — | 1.8 | 0 | 0 | $0.0000000 |
| [corem.se](http://localhost:5183/admin/crawls?tab=attempts&domain=corem.se&source=rest&debug_request=lookup-local-563edc45cd60-08&debug_attempt=1) | Failed | — | 57.2 | 5,102 | 954 | $0.0006174 |
| [axfood.se](http://localhost:5183/admin/crawls?tab=attempts&domain=axfood.se&source=rest&debug_request=lookup-local-563edc45cd60-09&debug_attempt=1) | Reference matched | 5565420824 | 222.0 | 18,270 | 5,946 | $0.0057135 |
| [spiltan.se](http://localhost:5183/admin/crawls?tab=attempts&domain=spiltan.se&source=rest&debug_request=lookup-local-563edc45cd60-10&debug_attempt=1) | Reference matched | 5562885417 | 254.3 | 8,976 | 9,513 | $0.0061029 |
| [humlegarden.se](http://localhost:5183/admin/crawls?tab=attempts&domain=humlegarden.se&source=rest&debug_request=lookup-local-563edc45cd60-11&debug_attempt=1) | Reference matched | 5566821202 | 30.9 | 10,308 | 1,352 | $0.0018889 |
| [elongroup.se](http://localhost:5183/admin/crawls?tab=attempts&domain=elongroup.se&source=rest&debug_request=lookup-local-563edc45cd60-12&debug_attempt=1) | Failed | — | 1.5 | 0 | 0 | $0.0000000 |
| [avanza.se](http://localhost:5183/admin/crawls?tab=attempts&domain=avanza.se&source=rest&debug_request=lookup-local-563edc45cd60-13&debug_attempt=1) | Unresolved | — | 48.1 | 10,579 | 1,855 | $0.0021372 |
| [ncc.se](http://localhost:5183/admin/crawls?tab=attempts&domain=ncc.se&source=rest&debug_request=lookup-local-563edc45cd60-14&debug_attempt=1) | Unresolved | — | 45.2 | 10,395 | 1,981 | $0.0021673 |
| [fabege.se](http://localhost:5183/admin/crawls?tab=attempts&domain=fabege.se&source=rest&debug_request=lookup-local-563edc45cd60-15&debug_attempt=1) | Reference matched | 5560491523 | 45.9 | 13,524 | 1,999 | $0.0025412 |
| [arise.se](http://localhost:5183/admin/crawls?tab=attempts&domain=arise.se&source=rest&debug_request=lookup-local-563edc45cd60-16&debug_attempt=1) | Reference matched | 5562746726 | 42.5 | 9,146 | 1,537 | $0.0017867 |
| [mangold.se](http://localhost:5183/admin/crawls?tab=attempts&domain=mangold.se&source=rest&debug_request=lookup-local-563edc45cd60-17&debug_attempt=1) | Supported operator | 5565851267 | 38.0 | 15,944 | 2,021 | $0.0028918 |
| [byggmax.se](http://localhost:5183/admin/crawls?tab=attempts&domain=byggmax.se&source=rest&debug_request=lookup-local-563edc45cd60-18&debug_attempt=1) | Reference matched | 5566563531 | 102.6 | 14,532 | 2,363 | $0.0028571 |
| [lifco.se](http://localhost:5183/admin/crawls?tab=attempts&domain=lifco.se&source=rest&debug_request=lookup-local-563edc45cd60-19&debug_attempt=1) | Matched; cleanup timeout | 5564653185 | 86.1 | 4,663 | 1,148 | $0.0010824 |
| [brinova.se](http://localhost:5183/admin/crawls?tab=attempts&domain=brinova.se&source=rest&debug_request=lookup-local-563edc45cd60-20&debug_attempt=1) | Unresolved | — | 64.5 | 14,118 | 2,700 | $0.0029475 |
| [genova.se](http://localhost:5183/admin/crawls?tab=attempts&domain=genova.se&source=rest&debug_request=lookup-local-563edc45cd60-21&debug_attempt=1) | Reference matched | 5568648116 | 59.4 | 14,057 | 1,687 | $0.0025092 |
| [latour.se](http://localhost:5183/admin/crawls?tab=attempts&domain=latour.se&source=rest&debug_request=lookup-local-563edc45cd60-22&debug_attempt=1) | Failed | — | 40.8 | 7,825 | 688 | $0.0012901 |
| [hufvudstaden.se](http://localhost:5183/admin/crawls?tab=attempts&domain=hufvudstaden.se&source=rest&debug_request=lookup-local-563edc45cd60-23&debug_attempt=1) | Reference matched | 5560128240 | 31.9 | 6,939 | 1,305 | $0.0014393 |
| [indutrade.se](http://localhost:5183/admin/crawls?tab=attempts&domain=indutrade.se&source=rest&debug_request=lookup-local-563edc45cd60-24&debug_attempt=1) | Reference matched | 5560179367 | 30.6 | 9,533 | 1,679 | $0.0019290 |
| [medivir.se](http://localhost:5183/admin/crawls?tab=attempts&domain=medivir.se&source=rest&debug_request=lookup-local-563edc45cd60-25&debug_attempt=1) | Reference matched | 5562384361 | 38.8 | 7,546 | 1,619 | $0.0016502 |

Mangold returned `5565851267` (Mangold Fondkommission AB), explicitly identified by the contact page, rather than the cohort’s reporting parent `5566285408`. This is a supported operator match, not counted as an exact reference-ID match.

## Timing and concurrency

The 25-site request lifecycle took **21.3 minutes**, including the Lifco cleanup timeout. All lookup results/errors had been produced after **17.8 minutes**. Median lookup execution was **45.2 seconds**. The earlier local-Qwen run took 32.7 minutes and returned 18 accepted matches, under the older policy described above.

The separate pilot replayed the same four saved identity prompts and schemas used in the local-model concurrency test (Addtech, JM, Neobo, Arise), with GLM/profile routing substituted. No website fetches or database queries were part of this pilot. No full-run model calls were still active when it started; Lifco was waiting only on outer cleanup. Each level ran once, with no retries and a 180-second deadline.

| Concurrent LLM requests | Valid and supported | Batch seconds | Median request seconds | Supported extractions / min | Output tokens / second | Cost, USD |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4/4 | 36.04 | 6.97 | 6.66 | 103.3 | $0.0049540 |
| 2 | 4/4 | 30.01 | 8.24 | 8.00 | 142.6 | $0.0040409 |
| 4 | 4/4 | 10.50 | 9.96 | 22.85 | 268.1 | $0.0041246 |

Four concurrent GLM requests worked in this small pilot. The local Qwen pilot had completed 4/4 at concurrency 1 and 2, but only 2/4 within the deadline at concurrency 4. Do not treat either pilot as a measured maximum capacity: there were only four requests per level, existing caches were retained, output lengths varied, and hosted provider load was not observable. GLM’s four-request batch generated fewer output tokens than its sequential batch. A cap of four simultaneous GLM calls is a reasonable setting for a larger follow-up trial; full-site worker capacity still depends on browser availability and cleanup.

Before a large run, investigate the cleanup wait and name-search timeout, address the Stendörren final-answer contradiction, and explicitly compare reasoning settings with consistent routing. The current null/default reasoning setting mixed materially different provider behavior.

## Artifacts and reproduction

- [Machine-readable benchmark results](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/crawler_service/benchmarks/company_lookup_local_20260927/glm-results.json)
- [Raw 25-site manifest](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/crawler_service/data/company-lookup-glm-20260927/full/manifest.json)
- [Raw results and debug traces](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/crawler_service/data/company-lookup-glm-20260927/full)
- [Captured endpoint prices](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/crawler_service/data/company-lookup-glm-20260927/pricing-endpoints.json)
- [Billing verification](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/crawler_service/data/company-lookup-glm-20260927/billing-check.json)
- [All 80 rate checks](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/crawler_service/data/company-lookup-glm-20260927/rate-check.json)
- [Concurrency results](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/crawler_service/data/company-lookup-glm-20260927/concurrency/summary.json)
- [Frozen crawler source](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/crawler_service/data/company-lookup-glm-20260927/source)

The private encrypted LLM transport remains outside the repository. No API keys are included in these artifacts. Benchmark code checks passed with Ruff; count, unique-response, token/cost-completeness, terminal-state and read-only-result checks passed. No production settings were changed, and no deploy was performed for this benchmark.

```sh
uv run --no-sync python benchmarks/company_lookup_local_20260927/run.py \
  --cohort benchmarks/company_lookup_local_20260927/rerun-cohort.json \
  --output data/company-lookup-glm-20260927/full \
  --llm-envelope /tmp/company-lookup-glm-envelope.json

uv run --no-sync python benchmarks/company_lookup_local_20260927/concurrency.py \
  --source data/company-lookup-industry-v2-20260927 \
  --output data/company-lookup-glm-20260927/concurrency \
  --envelope /tmp/company-lookup-glm-envelope.json --adapt-profile

uv run --no-sync python benchmarks/company_lookup_local_20260927/summarize_glm.py
```

Use a new output directory for a new paid run. The cohort runner resumes existing results; the concurrency runner requires a new directory.
