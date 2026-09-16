# NOVELIC crawler controller recheck — 8 September 2026

The controller changes improve job recovery, but the run still exposes important technology-validation and discovery gaps. Package **0.9.2 / schema 1.7** is implemented and tested. The completed benchmark reached its **24-page budget** (23 successful fetches, one broken URL) and produced a reviewed 21-statement overview. Its result remains **partial**, with three extraction chunks pending. No database submission or deployment was performed.

## What changed

- **Known posting URLs are bound in code.** A source-matched listing, an unchanged fetched URL and one unambiguous H1 establish the posting context. Matching job/technology titles receive that exact URL, with `job_url_binding` provenance. Related jobs retain their own observed links. This does not bypass quotation or meaning checks.
- **Engineering coverage has a reserve.** Up to three target-company service/engineering detail attempts receive priority inside the page budget. The queue also favors direct evidence after navigation and reduces priority for repetitive news/navigation pages with no new records. A zero-yield penalty now requires completed extraction of the objective; incomplete processing is not evidence of low usefulness.
- **Saved extraction can retry without fetching.** Native Crawl4AI HTML is retained and SHA-256 checked. Each chunk attempt has separate artifacts and call IDs. Completed chunks and semantic rejections are not repeated. Defaults are two attempts per chunk and five saved retries per run. Pending status includes chunks that never started.
- **Review IDs are short and exact.** Source/proposal reviews use short request IDs, mapped back to canonical IDs in code. Unknown IDs are rejected. This fixed an observed response that changed six of 17 canonical IDs; the first live short-ID review returned all 17 correctly in 9.632 seconds.
- **Credential versions compare correctly.** The validator accepts an exact `ISO 9001:2015` reconstruction when the record stores `ISO 9001` and `2015` separately. Different standard numbers or versions still fail.

No custom job CSS selector or HTML cleanup was introduced. The LLM receives windows of Crawl4AI's native `cleaned_html`; all objectives remain applicable to every fetched page.

## Results compared with the previous audited run

“Accepted” below means passed the current pipeline checks, not independently verified or approved by an administrator. These are selected previously checked facts, not exhaustive recall.

| Check | Previous 0.8.1 result | This run |
|---|---:|---:|
| Successful page fetches | 24 | 23 of 24 attempts |
| Checked Careers listings retained | 16 / 16 | 16 / 16 |
| Job descriptions fetched | 5 | 5 |
| Fetched descriptions yielding accepted job records | 3 / 5 | **5 / 5** |
| Checked Management names retained | 16 / 16 | **15 / 16**; all 16 extracted |
| Technology observations passing pipeline checks | 10 | 47 |
| Technology entities after grouping | 9 | 35 |
| Mechanical / AMS engineering pages fetched | 0 | 2 |
| Checked Antenna Design tools from that source | 0 / 6 | **0 / 6**; page unvisited |
| Unrelated general Analog Devices pages fetched | 0 | 0 |

The formerly failing **Senior Data Engineer/Data Architect** posting now retains its actual `Data-Engineer-k3vrrny` URL, one accepted job and **11 accepted technology observations**. Across the five ads, the controller bound five jobs and 46 raw technology observations. Of the technology observations from those ads, **28 passed pipeline checks**; duplicate requirements/signals mean this is not a count of unique tools.

Mechanical Engineering yielded 25 tool observations, of which three passed all checks. AMS/RF/mmWave IC Design yielded 19, of which 16 passed. These 19 accepted engineering observations need a semantic qualification: the tables advertise engineering skills/expertise, while the model labels them `stated_use` at company scope. Their presence is useful evidence of capability, but does not establish a currently installed company stack.

## What remains wrong

1. **Proposal metadata discards useful observations.** There are 32 rejected proposal-review records. Mechanical CAD and programming languages were assigned unsuitable EDA categories. Some reviewer decisions also incorrectly require a suggested new category to already exist in the catalog. Category repair should be separate from source extraction. Vendor descriptions remain model-authored drafts requiring administrator verification.
2. **Expertise and usage are not consistently distinguished.** The 19 engineering observations above should retain advertised expertise/mention semantics unless explicit use is established. Increasing the accepted count does not by itself demonstrate improved precision.
3. **Evidence repair still loses valid records.** Dušan Manić / Head Of Finance was extracted with name and role, but the quotation omitted NOVELIC. All 16 checked Management names were returned; only 15 survived validation.
4. **Antenna Design remains a discovery gap.** Its sitemap URL was eventually assessed as useful, but target relevance remained unknown. The three engineering attempts also included the broken `/mechanical-enginering/` URL. The reserve improved general engineering coverage but did not recover this specific source.
5. **Credential/document output remains incomplete in the comparison JSON.** Earlier 0.9.1 credential rejections are preserved in the live records. The separate unchanged-response replay through 0.9.2 recovers ISO 9001:2015, ISO 14001:2015 and IATF 16949:2016 `working_toward`; it was not substituted into the comparison. Nine document records were extracted and none passed all checks. Raw document candidates and links remain available. Historical operations announcements were fetched; PDFs were not parsed.
6. **The ordinary summary path still fails an entire batch over a bad statement.** After two responses cited site classification as service evidence, the summary stopped. A benchmark recovery excluded those two statements using the unchanged validator, then consolidated and reviewed the saved valid batches in two further model calls. The final reviewer rejected an unsupported jump from Sona Comstar partnership to group membership. The resulting overview has 21 retained statements; it still contains attributed marketing language that should not be treated as verified rankings.

Three chunks remain pending: DSP internship (`p0011/0`), Automotive (`p0015/1`) and 2022 review (`p0021/0`). The global saved-retry allowance was exhausted. Historical errors and rejected records remain in the artifacts.

## Saved-source checks and verification

Before the live crawl, replaying the saved Data Engineer HTML produced one accepted job and 12 source-matched technology observations with correct URLs; only three technology observations passed catalog resolution in that replay. Separately, deterministic revalidation fixed all **17 previously URL-rejected records** (one job and 16 technology observations) using unchanged facts and quotations. These are different checks from the fresh live result above.

The frozen queue check selected Antenna Design and Mechanical Engineering after the five job follow-ups, but used richer saved assessments. It did not prove autonomous discovery; the live result demonstrates that limitation.

**89 Python tests pass**, including three actual browser recovery tests and nine controller tests. Ruff, ty, wheel/source builds and `git diff --check` pass. Controller provenance, source hashes and accepted-entity references have **no integrity issues**. Integrity checks do not establish semantic correctness. The manual findings and affected record IDs are saved in `source-audit-issues.json`.

## Protocol, recoveries and cost

The only page seed was the NOVELIC homepage. The run used `deepseek/deepseek-v4-flash-0731`, reasoning disabled, the saved 7,981-entry technology catalog, a cumulative 24-page / 200-call budget, eight external pages, and 60,000-character HTML windows with 4,000-character overlap.

The original automatic-routing run used a 120-second request deadline. Saved-page recovery increased it to 360 seconds, followed by the short-ID fix. Repeated long timeouts then led to a preserved recovery pinned to **Wafer**, returning to a 120-second deadline. Its explicit recovery allowances were four chunk attempts and eight saved retries, counting interrupted attempts. All previous calls, page fetches, queue state and code snapshots were retained. No new page seeds were added. Summary recovery fetched no pages.

The completed collection and summary made **170 calls**, with **$0.2014627934 reported charges plus 19 calls of unknown cost**. Wall time was **99 minutes 27 seconds**, including interruptions and manual recovery. The previous run reported $0.16767575 plus 32 unknown-cost calls. Provider routing, deadlines and recovery changed, so this is **not a controlled cost or latency comparison**. The earlier isolated Data Engineer replay adds $0.00706444 outside the collection total. Deterministic URL and credential replays made no new model calls.

## Recommended next work

Separate source evidence, technology identity and proposal metadata into independently repairable stages. Add an explicit advertised-expertise interpretation; repair category/description failures without re-extracting HTML. Then address unknown-relevance engineering candidates and failed reserve attempts, and make summary statement exclusion a normal bounded recovery path. Validate these changes on frozen records and several other companies before treating NOVELIC improvements as general accuracy.

## Artifacts

- [Completed result](data/novelic-autonomous-v17-completed/result.json)
- [Company overview](data/novelic-autonomous-v17-completed/company-overview.json)
- [Manual quality issues](data/novelic-autonomous-v17-completed/source-audit-issues.json)
- [Controller audit](data/novelic-autonomous-v17-completed/controller-audit.json)
- [Baseline comparison](data/novelic-autonomous-v17-completed/scope-comparison.json)
- [Summary recovery provenance](data/novelic-autonomous-v17-completed/summary-recovery.json)
- [Verification and source hashes](data/novelic-autonomous-v17-completed/verification.json)
- [Credential validation replay](data/novelic-controller-v092-credentials/result.json)
- [Saved Data Engineer replay](data/novelic-controller-v09-replay/result.json)
- [Deterministic URL revalidation](data/novelic-controller-v09-binding/result.json)
- [Unmodified crawl before summary recovery](data/novelic-autonomous-v17-wafer/result.json)
- [Previous audited run](data/novelic-autonomous-v16-validated/result.json)
