# Local-model Swedish company lookup benchmark — 27 September 2026

Tested **25 distinct Swedish company-associated websites**. **22 homepages were fetched**; 3 failed before classification. The initial 21 cases are retained, with four additional cases selected under the same rule to compensate for unreachable domains.

The module is conservative about returning a company, but currently misses too many supported matches to use this local-model configuration for unattended bulk association. The returned proposals must be distinguished from abstentions, fetch failures and category exclusions.

| Reviewed outcome | Count |
|---|---:|
| Exact reference match | 7 |
| Supported operator; different reporting company | 1 |
| Correct shop exclusion | 1 |
| No match / abstained | 10 |
| Technical failure | 6 |

No returned match was identified as a wrong company in this sample. This is a small, selected sample; it does not establish population-level precision. A no-match result is not automatically an incorrect decision: Avanza contains several plausible group operators.

The six technical failures comprise three homepage fetch failures, two model timeouts, and one malformed identity response. No candidate-search database request failed.

## Method and model

- Initial cohort: Addtech regression case plus 20 distinct active Swedish companies with suggested-primary `.se` domains explicitly recorded in 2024 ESEF filings. Stable domain-hash selection; names and expected IDs were withheld from lookup requests.
- Additional cohort: next four distinct DNS-resolving companies using the same reference selection. Failed initial attempts remain in the totals.
- Four-page budget, two concurrent crawls, 180-second model deadline, saved profile defaults, no Jev. Classification, extraction and matching all use the local endpoint. No changes to production matching logic during the benchmark.
- Configured model: `RedHatAI/Qwen3.6-35B-A3B-NVFP4`; actual model reported by responses: `nvidia/Qwen3.8-27B-NVFP4`. This is an endpoint/model-configuration mismatch, so these findings cannot be attributed to the configured model name.
- Company/domain associations were not written to a database; results and debug traces are local crawler artifacts. No S3 publication. PostgreSQL/ClickHouse company enrichment was not modified.
- ESEF identifies the reporting company, which can differ from the legal website operator. Mangold was reviewed against its actual contact-page and copyright evidence before scoring it as supported.

## Per-website results

Each website link opens its exact Backoffice attempt and debug trace. Expected IDs are reference associations, not values provided to the model.

| Website / trace | Reference ID | Returned ID | Reviewed outcome | Seconds | Review |
|---|---|---|---|---:|---|
| [addtech.se](http://localhost:5183/admin/crawls?tab=attempts&domain=addtech.se&source=rest&debug_request=lookup-local-f6e553a27a52-01&debug_attempt=1) | 5563029726 | — | Technical failure | 230.3 | Identity model timed out (180 s). Original VAT-conflict bug fixed separately. |
| [jm.se](http://localhost:5183/admin/crawls?tab=attempts&domain=jm.se&source=rest&debug_request=lookup-local-f6e553a27a52-02&debug_attempt=1) | 5560452103 | 5560452103 | Exact reference match | 143.6 | Website organisation number matches the registry |
| [stendorren.se](http://localhost:5183/admin/crawls?tab=attempts&domain=stendorren.se&source=rest&debug_request=lookup-local-f6e553a27a52-03&debug_attempt=1) | 5568254741 | — | No match / abstained | 174.9 | Correct candidate rejected by basis-dependent guard; model altered an ID. |
| [neobo.se](http://localhost:5183/admin/crawls?tab=attempts&domain=neobo.se&source=rest&debug_request=lookup-local-f6e553a27a52-04&debug_attempt=1) | 5565802526 | 5565802526 | Exact reference match | 193.2 | Website organisation number matches the registry |
| [norion.se](http://localhost:5183/admin/crawls?tab=attempts&domain=norion.se&source=rest&debug_request=lookup-local-f6e553a27a52-05&debug_attempt=1) | 5565970513 | — | Technical failure | 1.3 | DNS failure (ERR_NAME_NOT_RESOLVED). |
| [instalco.se](http://localhost:5183/admin/crawls?tab=attempts&domain=instalco.se&source=rest&debug_request=lookup-local-f6e553a27a52-06&debug_attempt=1) | 5590158944 | — | No match / abstained | 134.4 | Correct candidate rejected by basis-dependent guard; address formatting differs. |
| [studsvik.se](http://localhost:5183/admin/crawls?tab=attempts&domain=studsvik.se&source=rest&debug_request=lookup-local-f6e553a27a52-07&debug_attempt=1) | 5565010997 | — | Technical failure | 1.7 | TLS certificate name mismatch. |
| [corem.se](http://localhost:5183/admin/crawls?tab=attempts&domain=corem.se&source=rest&debug_request=lookup-local-f6e553a27a52-08&debug_attempt=1) | 5564639440 | 5564639440 | Exact reference match | 195.1 | Website organisation number matches the registry |
| [axfood.se](http://localhost:5183/admin/crawls?tab=attempts&domain=axfood.se&source=rest&debug_request=lookup-local-f6e553a27a52-09&debug_attempt=1) | 5565420824 | — | Technical failure | 241.4 | Identity model timed out (180 s); correct ID is visible in saved page evidence. |
| [spiltan.se](http://localhost:5183/admin/crawls?tab=attempts&domain=spiltan.se&source=rest&debug_request=lookup-local-f6e553a27a52-10&debug_attempt=1) | 5562885417 | 5562885417 | Exact reference match | 166.2 | Explicit legal name uniquely matches the returned registry candidates |
| [humlegarden.se](http://localhost:5183/admin/crawls?tab=attempts&domain=humlegarden.se&source=rest&debug_request=lookup-local-f6e553a27a52-11&debug_attempt=1) | 5566821202 | 5566821202 | Exact reference match | 161.3 | Website organisation number matches the registry |
| [elongroup.se](http://localhost:5183/admin/crawls?tab=attempts&domain=elongroup.se&source=rest&debug_request=lookup-local-f6e553a27a52-12&debug_attempt=1) | 5560654054 | — | Technical failure | 1.7 | Homepage HTTP response failure. |
| [avanza.se](http://localhost:5183/admin/crawls?tab=attempts&domain=avanza.se&source=rest&debug_request=lookup-local-f6e553a27a52-13&debug_attempt=1) | 5562748458 | — | No match / abstained | 230.9 | Several group-company IDs; operator ambiguous. Abstention is defensible. |
| [ncc.se](http://localhost:5183/admin/crawls?tab=attempts&domain=ncc.se&source=rest&debug_request=lookup-local-f6e553a27a52-14&debug_attempt=1) | 5560345174 | — | No match / abstained | 112.2 | No supported facts; model explanation unsupported by saved pages. |
| [fabege.se](http://localhost:5183/admin/crawls?tab=attempts&domain=fabege.se&source=rest&debug_request=lookup-local-f6e553a27a52-15&debug_attempt=1) | 5560491523 | — | No match / abstained | 107.5 | One exact legal-name candidate, but model falsely called it non-unique. |
| [arise.se](http://localhost:5183/admin/crawls?tab=attempts&domain=arise.se&source=rest&debug_request=lookup-local-f6e553a27a52-16&debug_attempt=1) | 5562746726 | — | No match / abstained | 56.6 | Model missed the explicit legal name and address in all four input pages. |
| [mangold.se](http://localhost:5183/admin/crawls?tab=attempts&domain=mangold.se&source=rest&debug_request=lookup-local-f6e553a27a52-17&debug_attempt=1) | 5566285408 | 5565851267 | Supported operator; different reporting company | 183.2 | Contact and copyright identify Mangold Fondkommission AB, 5565851267. |
| [byggmax.se](http://localhost:5183/admin/crawls?tab=attempts&domain=byggmax.se&source=rest&debug_request=lookup-local-f6e553a27a52-18&debug_attempt=1) | 5566563531 | — | Correct shop exclusion | 92.3 | Online store: stopped after homepage; no company queries. |
| [lifco.se](http://localhost:5183/admin/crawls?tab=attempts&domain=lifco.se&source=rest&debug_request=lookup-local-f6e553a27a52-19&debug_attempt=1) | 5564653185 | 5564653185 | Exact reference match | 121.9 | Website organisation number matches the registry |
| [brinova.se](http://localhost:5183/admin/crawls?tab=attempts&domain=brinova.se&source=rest&debug_request=lookup-local-f6e553a27a52-20&debug_attempt=1) | 5568403918 | — | No match / abstained | 134.6 | Correct candidate, confidence 0.97, but model marked evidence insufficient; historical same-name candidate also returned. |
| [genova.se](http://localhost:5183/admin/crawls?tab=attempts&domain=genova.se&source=rest&debug_request=lookup-local-f6e553a27a52-21&debug_attempt=1) | 5568648116 | — | Technical failure | 115.8 | Malformed identity fact with an empty source URL failed whole-response validation. |
| [latour.se](http://localhost:5183/admin/crawls?tab=attempts&domain=latour.se&source=rest&debug_request=lookup-local-c14d21833c4b-01&debug_attempt=1) | 5560263237 | — | No match / abstained | 156.6 | Name normalization/ranking omitted the correct candidate; validator rejected the model's unrelated proposal. |
| [hufvudstaden.se](http://localhost:5183/admin/crawls?tab=attempts&domain=hufvudstaden.se&source=rest&debug_request=lookup-local-c14d21833c4b-02&debug_attempt=1) | 5560128240 | — | No match / abstained | 81.6 | Model invented an ID and HTML quotes; evidence validation correctly rejected them. |
| [indutrade.se](http://localhost:5183/admin/crawls?tab=attempts&domain=indutrade.se&source=rest&debug_request=lookup-local-c14d21833c4b-03&debug_attempt=1) | 5560179367 | — | No match / abstained | 100.0 | Verified exact ID match rejected because model marked its basis insufficient. |
| [medivir.se](http://localhost:5183/admin/crawls?tab=attempts&domain=medivir.se&source=rest&debug_request=lookup-local-c14d21833c4b-04&debug_attempt=1) | 5562384361 | 5562384361 | Exact reference match | 88.9 | Explicit legal name uniquely matches the returned registry candidates |

## Timing, usage and query audit

- Wall time: 28.4 minutes; median duration among fetched websites: 139.1 seconds.
- 58 model calls; 296,502 reported input tokens and 15,291 reported output tokens. Timed-out calls may consume unreported tokens. The endpoint did not report a monetary price.
- 33 read-only company searches during the benchmark: 10 by exact ID and 23 by name; all completed successfully. A separate read-only diagnostic after the benchmark confirmed Latour's retrieval-ranking problem.
- Median query duration: **622 ms by exact ID**, **6,646 ms by name**. SQL, parameters, returned rows and timing are retained in each debug trace.

## Main fixes to prioritize

1. Extract labelled organisation/VAT IDs directly from contact/legal-page evidence, normalize them, and query exact IDs before relying on general-model extraction. Keep ownership verification and conflicting-ID checks. Existing ID searching works; reliable discovery of the IDs is the missing piece.
2. Make deterministic acceptance check all supported evidence routes instead of depending solely on the model’s chosen basis. Stendörren, Instalco and the exact-ID Indutrade case expose false-negative paths. Preserve conflicting-ID and unsupported-evidence rejection.
3. Improve legal-form normalization and name retrieval. Latour's correct registry row was outside the top ten candidates because `Investmentaktiebolaget` was not normalized to the website's abbreviated legal form.
4. Fix model/profile identity, reduce cookie/navigation boilerplate, and improve extraction validation. Arise was missed despite clear text; Genova’s malformed fact invalidated the entire response. Do not silently accept unsupported facts.
5. Add optional Jev ranking for multiple candidates, with per-candidate confidence/evidence and an explicit no-match choice. Treat scores as uncalibrated model confidence; preserve independent evidence checks. Jev was not tested in this local-model baseline.

The original Addtech VAT-normalization fix is deployed and covered by focused regression tests. Its saved original evidence/assessment now accepts `5563029726` in replay. The new local-model Addtech run still timed out, so a successful fresh local-model crawl has **not** been demonstrated by this benchmark.

## Reproduction and artifacts

- [Detailed review notes](REVIEW_NOTES.md), [machine-readable results](results.json), [initial cohort](cohort.json), [additional cohort](extra-cohort.json), [benchmark runner](run.py).
- Initial raw artifacts: `data/company-lookup-local-20260927/`; additional raw artifacts: `data/company-lookup-local-extra-20260927/`. Each folder contains `status.json`, `result.json`, `trace.jsonl` and `summary.json` per website. Manifests include source hashes and model-profile revision.
- `run.py` resumes existing request IDs and keeps completed failures. `--cohort` and `--output` select another frozen cohort/output directory. It uses the existing private verified profile envelope; credentials are not included in report artifacts.
