# NOVELIC scope and validation recheck — 8 September 2026

The test is complete. Target scope and job-ad follow-up improved, while engineering page coverage regressed. The output remains **partial**. No database submission or deployment was performed. PDF/OCR remains paused.

Package **0.8.1**, output schema **1.6**. Live collection used 0.8.0; the final saved-source audit applied 0.8.1 validation fixes without fetching more pages. This experiment does not establish accuracy on unseen companies.

## Results at the same 24-page budget

| Selected check | Previous autonomous run | New run plus saved-source validation |
|---|---:|---:|
| Pages fetched | 24 | 24 |
| Checked Careers listings retained | 16 | 16 |
| Checked Management names retained | 16 | 16 |
| NOVELIC job descriptions fetched | 0 | 5 |
| Job description pages yielding accepted job records | 0 | 3 |
| Checked DSP internship technologies | 0 | 5 |
| Unrelated Analog Devices pages fetched | 6 | 0 |
| Six checked Antenna Design tools recovered autonomously | 6 | 0 |

The DSP internship technologies are VHDL, Verilog, Matlab, Simulink and Python. They remain **preferred experience for that role**, not proof of company-wide deployment. Other accepted job signals include Cadence Virtuoso and Siemens Calibre requirements from Analog IC Layout, and Windows/Linux requirements plus preferred Python from Junior Data Acquisition.

Final accepted technology output contains **10 observations across nine identities**, all from job descriptions. Earlier raw artifacts also contain CI/CD and an overly broad AUTOSAR observation, which the final audit withheld.

There are 19 accepted job records consolidated into 16 job entities, and 25 accepted person records consolidated into 21 person entities. These include the 16 checked Careers listings and 16 checked Management names. Records and entities are different measures. Likewise, 90 accepted offering records are overlapping descriptions, not 90 distinct services.

## Changes implemented

- Link assessment records target-company relevance and whether an external link permits one page or further target-specific navigation. Observed target facts are required before expanding external navigation. A partner profile cannot authorize its entire domain.
- Target-employer job links observed in listings become prioritized follow-ups, with a five-page reserve inside the total page budget. `/job/` paths are recognized.
- Company identity, relationship direction, credential holder, document type and technology signal receive structured source-meaning review before technology catalog resolution. Legal-name equivalence requires explicit evidence; regional expansion does not establish the primary company's legal name.
- Proposed technology identities/categories receive a separate audit. Vendor names, uncertain spelling corrections and inappropriate category prefixes are challenged. Proposals remain administrator-review drafts.
- Duplicate records share source/proposal review decisions. Required-review flags prevent missing or failed reviews from entering accepted exports. An unreviewed duplicate cannot resurrect a rejected interpretation in either merge order.
- Summaries use accepted target-company facts and typed citations. Short internal IDs are validated and mapped back to canonical IDs in code. Certification names must match cited structured facts exactly. A final model review supplies an additional, fallible meaning check.
- Generic CI/CD is excluded from technology names; actual tools such as GitLab CI, GitHub Actions and Jenkins remain allowed.

Every fetched page is still examined for all objectives. Input remains native Crawl4AI `cleaned_html`, without custom job CSS selectors or hand-cleaned HTML.

## What the final audit caught

**Duplicate review bypass.** An unreviewed duplicate allowed the Quality Management landing page to survive as a certificate document after another copy was rejected. The code now closes this path. The final saved-source review rejects it; original data and decisions remain saved.

**Generic method accepted by both reviews.** CI/CD passed both source and proposal checks. A deterministic name guard now excludes it. A second model opinion alone was insufficient.

**Mistyped certification number.** DeepSeek wrote ISO 14000:2015 while the cited record says ISO 14001:2015. Its statement reviewer approved the mistake. The exact-name check excluded that sentence, retaining 23 other overview statements. The correct structured credential and certificate URL remain in `records`. The original generated summary is preserved in `summaries/before-standard-name-audit.json`.

**Product evidence attributed too broadly.** ACAM's specification lists AutoSAR Classic. The model recorded company scope and proposed an Operating systems category despite describing a broader software architecture. The source audit moved this observation to `needs_review`. The schema has no product technology scope; explicit product attribution is a remaining model change.

Accepted credentials preserve ISO 9001:2015 and ISO 14001:2015 as website certification claims, IATF 16949:2016 as **working toward**, and IP51 as **ACAM product compliance**. These do not independently verify current certificate validity. Actual ISO certificate PDF URLs were discovered; files were not downloaded.

One document-link record passed all checks: an ADAS Domain Control Unit case-study PDF, classified as `product_documentation`. No NOVELIC financial report passed extraction checks. The 2017/2018 operations announcements are not treated as verified financial statements. Absence from accepted output does not prove no report exists.

## Remaining defects and next implementation order

1. **Engineering pages lose to high-rated news.** Antenna Design and Mechanical Engineering were discovered and assessed `medium/direct` for technologies but never fetched. Six antenna tools pass frozen review; the live failure is page selection. Reserve direct service/engineering coverage and penalize repetitive news/navigation value as coverage accumulates. Do not insert NOVELIC-specific seeds.
2. **The model rewrites known posting URLs.** The fetched URL ends in `Data-Engineer-k3vrrny`; its heading is Senior Data Engineer/Data Architect. DeepSeek constructed a new URL from the title. Validation withheld one job-detail record and 16 technology records. Bind the known fetched posting URL in code on detail pages; retain observed links for listing pages. Do not weaken URL validation.
3. **Recover fetched-page extraction separately from discovery.** Five fetched pages failed extraction during provider timeouts/429s. Reprocess saved native HTML before spending more crawl budget. Keep retries bounded and record routing changes.
4. **Preserve product-level technology attribution.** Add product scope and a source-backed product reference. Audit proposal categories independently from website labels.
5. **Improve consolidation.** Group repeated offerings, shorten summary statements and preserve historical qualifiers. Render exact identifiers such as credential names from structured facts where possible.

The Indian expansion page was fetched without accepting its local entity name as NOVELIC's primary legal name. A source-supported Sona Comstar relationship was retained. The ISO 26262 experts page was not fetched live: rejection of the erroneous company certification is validated only by frozen regression tests.

## Validation and experimental limits

The expanded frozen-source test matched **39/39 known regression cases**: 23 expected rejections and 16 expected acceptances. Positives include overlapping observations of six antenna tools, one explicit Sona parent relationship, a trading name, ISO 9001 and IATF working-toward claims. These are development examples, not an unseen test set. Earlier iterations remain saved; they exposed omitted reconstruction fields, an overly broad positive label, and confusion between `mentioned` and proven deployment.

The partner guard was replayed against 35 actual anchors from the previous Analog Devices profile HTML. It allowed the target's single profile and blocked 26 external navigation attempts; remaining links were social/invalid. None of the six previous unrelated destinations was admitted. The new live crawl did not visit the profile, so this replay is separate evidence for the scope rule.

All **80 Python tests passed**, including three actual browser recovery tests. Ruff, ty, wheel/source builds and `git diff --check` passed. Logs are saved in the final artifact's `verification/`. Artifact audits found no source hash mismatches or unaccepted record references in exported entities. Integrity checks do not establish semantic completeness or independent accuracy.

Configuration: DeepSeek `deepseek/deepseek-v4-flash-0731`, reasoning none, saved 7,981-entry catalog, 24 pages, up to eight external pages, 200 model calls, 60,000-character HTML windows with 4,000-character overlap. The crawl began with only the homepage URL.

This was not one uninterrupted controlled run. Together stopped after 21 pages with rate limits. Saved-queue recovery fetched three more pages using the same model/provider and two HTTP attempts instead of one. Overview recovery used automatic OpenRouter routing, which selected Wafer. An earlier two-page development attempt was interrupted and is excluded from the 24-page result.

| Stage | Model calls, cumulative | Reported cost, cumulative | Calls with unknown cost |
|---|---:|---:|---:|
| Previous autonomous baseline | 124 | $0.20520180 | 8 |
| New 24-page collection including recovery | 136 | $0.15736450 | 32 |
| Failed long-ID summary recovery | 138 | $0.16250285 | 32 |
| Final missing review + short-ID summary/review | 141 | $0.16767575 | 32 |

The final three calls added $0.00517290; subsequent deterministic/source audits made no model calls. Frozen regression checks and earlier development attempts have separate costs. **These incomplete billing figures do not establish cost savings**: the new run had more failed/unknown-cost requests and different coverage.

## Saved artifacts

- [Final validated JSON](data/novelic-autonomous-v16-validated/result.json)
- [Company overview](data/novelic-autonomous-v16-validated/company-overview.json)
- [Source audit and remaining issues](data/novelic-autonomous-v16-validated/source-audit-issues.json)
- [Integrity and selected-source audit](data/novelic-autonomous-v16-validated/audit.json)
- [Baseline comparison](data/novelic-autonomous-v16-validated/scope-comparison.json)
- [Validation provenance](data/novelic-autonomous-v16-validated/validation.json)
- [Unsubmitted technology preview](data/novelic-autonomous-v16-validated/technology-submission-preview.json)
- [Expanded frozen checks](data/novelic-scope-frozen-v08-identity/result.json)
- [Partner-link replay](data/novelic-autonomous-v16-final/frozen-link-replay.json)
- [Original live attempt](data/novelic-autonomous-v16-final/result.json) and [saved-queue recovery](data/novelic-autonomous-v16-recovered/result.json)

Native HTML, requests, raw responses, failed reviews and implementation snapshots remain saved. The final JSON is a derived validation artifact; not every fix was present during the original collection.
