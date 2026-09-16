# NOVELIC autonomous crawl — 8 September 2026

Browser recovery, entity consolidation and the submission acceptance boundary are
implemented. The homepage-only test recovered the selected Careers, Management and
Antenna Design facts. It also demonstrated that the crawler still needs tighter
company scope and broader semantic validation before its output can be ingested
without review.

[Raw crawler JSON](data/novelic-autonomous-v15/result.json),
[automated audit](data/novelic-autonomous-v15/audit.json),
[source-audit issues](data/novelic-autonomous-v15/source-audit-issues.json), and
[unsubmitted technology payload](data/novelic-autonomous-v15/technology-submission-preview.json).
The raw result is unchanged by the audit and contains the errors described below.
The payload excludes pipeline-rejected claims but still contains draft identities
requiring administrator review; it is not an approved import.

## What was tested

Package 0.7.0 / schema 1.5 started with only `https://www.novelic.com/`. It read
138 sitemap URLs and discovered subsequent links through fetched pages. There were
no supplied Careers, Management, engineering or job-ad page seeds.

The run used native Crawl4AI `cleaned_html`, DeepSeek
`deepseek/deepseek-v4-flash-0731` through Together, reasoning `none`, 60,000-character
windows with 4,000-character overlap, and the saved 7,981-entry catalog from the MCP
test. The catalog was not refreshed during this benchmark. Limits were 24 pages,
eight external pages, 200 model calls, one concurrent extraction request, a
120-second request deadline and one HTTP attempt. These are benchmark settings;
the package defaults allow more extraction concurrency and HTTP retries.

All 24 pages fetched successfully: 17 NOVELIC pages and seven Analog Devices pages.
The run stopped at its page budget with status `partial`. No browser recovery was
needed during this particular live run. Recovery was tested separately by closing
an actual Playwright context: the same page succeeded after restart, both attempt
files remained, and exhaustion stopped the run without spending the remaining page
budget on a dead browser.

The run took 49 minutes 23 seconds and made 124 model calls. There were 115 complete
model responses, one incomplete response with reported usage, and eight deadline
failures with unknown cost. Reported charges were **$0.20520180**, plus any unreported
charges for those eight calls. This is one run's measured cost, not a provider price
estimate. The older `usage.successful_responses` field counts responses carrying usage,
including an incomplete response; `audit.complete_model_responses` checks completion.

## Useful results

| Selected check | Autonomous result |
|---|---|
| Careers page | 16 listed openings, all passing source checks |
| Management page | All 16 names from the earlier checked list recovered |
| Antenna Design page | All six previously checked tools recovered |
| Antenna overlap | 12 accepted observations grouped into six technology identities |
| Sona Comstar relationship | The India announcement explicitly calls it NOVELIC's parent; no ownership percentage invented |
| Source integrity | No source-hash mismatches or rejected record references in entity summaries |

The six antenna tools are CST Studio Suite, Ansys HFSS, WIPL-D Pro CAD, ADS Momentum,
ADS and AWR Microwave Office. Their accepted signal is `mentioned`: advertised
experience, not confirmed internal deployment. Generic XML/JSON are absent from the
accepted technology entities. The Automotive Annotation page was not visited, so
this run does not independently repeat that page's earlier negative test.

The complete output has 33 pipeline-accepted technology observations grouped into
27 proposed identities, all attributed to NOVELIC and all with `mentioned` signals.
Every proposal contains a description and category suggestion. These are not 27
independently verified technologies: the source audit found identity/metadata errors.
Fifty-two other raw technology findings remain reviewable and are excluded from the
entity summary and submission preview.

Across all companies, 36 accepted person records group into 35 entities. Those
include 21 under `NOVELIC`, one under `Novelic d.o.o.`, 12 under Analog Devices name
variants, and one under Firefly. The 16 Management people are the useful checked
NOVELIC subset. Sava Smiljanic / Sava Smiljanić under different company-name variants
remain separate; company aliases and personal-name variants are not resolved by
the conservative grouping logic. All 16 job entities are attributed to NOVELIC.

Earlier saved data was also checked: 17 management-role records consolidate into
16 people; 17 Careers/internship records consolidate into 16 openings. These are
frozen-data checks, separate from the new run, which did not reach a job description.

## Remaining errors

1. **External company scope is too broad.** Visiting NOVELIC's Analog Devices partner
   profile was useful. The controller then allowed traversal into Analog Devices'
   investor homepage, overview, contacts, board, careers and quarterly-results pages.
   Those six additional pages consumed 25% of the entire crawl budget. The narrative
   even includes an unrelated Analog Devices acquisition, although it identifies ADI
   as a separate company. Most accepted financial-document links belong to ADI or
   have unknown attribution; no supported NOVELIC financial report was recovered.

2. **A jobs list did not lead to NOVELIC job descriptions.** All 16 external posting
   URLs were discovered, but none was fetched. The jobs selection instead visited
   Analog Devices Careers. The five preferred skills from the earlier checked
   internship therefore were not reproduced. Navigation state only advances when a
   selected link was predicted to be navigation; Careers was reached through fallback
   exploration. Actual extracted openings should update that state. The nomination
   hints also recognize `/jobs/` but omit the common singular `/job/` ATS path.

3. **Expert credentials became company certification.** The ISO 26262 article says
   NOVELIC has a pool of certified experts. Record `7daa6677c1e1a5379ea99a9b` instead
   says NOVELIC holds certification, and the overview repeats it. This is unsupported.
   The ISO 9001 and IATF working-toward records preserve useful distinctions, but the
   ISO 14001 claim was not successfully accepted. Certification meaning needs review,
   not only quotation matching.

4. **Company identity and narrative relationships bypassed semantic review.** Record
   `aeb99c85e95e6c3b7bce5807` assigns `NOVELIC India Private Ltd` as NOVELIC's legal name.
   The page describes the Indian operation separately. The overview further calls it
   a subsidiary using that record and a location record; those citations do not prove
   the parent/subsidiary relationship. By contrast, the same article explicitly calls
   Sona Comstar NOVELIC's parent, so that separate relationship has direct support.

5. **Proposal identity and category quality still vary.** The source lists
   `Dlubal (RSTAB, RFEM)`; the extractor also proposes Dlubal as a separate technology,
   while its own description identifies a software company. The source spelling
   `Abacus` becomes a description asserting it is Abaqus without verifying that alias.
   Many mechanical CAD/structural-analysis tools receive suggestions beginning
   “Electronic design automation,” which is an inappropriate broad category for them.
   These drafts need correction before catalog approval.

6. **A certificate navigation page is counted as a document.** Record
   `499cc6f0547e16fbf9452b62` identifies `/quality-management/` itself as a certificate.
   It should lead to the actual certificate links. The ASPER product PDF is useful
   product documentation, but it must not be counted as a financial report. PDFs
   were not downloaded or interpreted in this test.

Source matching did not catch errors 3–6. The current structured semantic review
covers relationships and technology signals, and even that model review can make
mistakes. It does not yet protect company legal names, certification holders or
document classification. The audit is a selected source check, not exhaustive truth
validation of every raw record or narrative sentence.

## Recommended next change

Keep the next test on NOVELIC before adding unseen companies:

1. Require target-company relevance before expanding an external site. A partner
   profile should not authorize a crawl of the partner's whole company. Preserve
   narrow access to employer-linked ATS postings and genuinely relevant ownership
   sources.
2. Promote job URLs extracted from the target Careers page into the detail-page
   queue. Update navigation coverage from observed page contents and cover singular
   ATS `/job/` paths. Measure descriptions fetched separately from openings listed.
3. Extend structured semantic review to company identity, credential holders and
   document type. A relationship sentence in the overview must be supported by an
   accepted relationship claim, not inferred from a name/location record.
4. Strengthen vendor-versus-product and proposal-category checks. Treat spelling
   corrections as uncertain alias candidates until resolved. Keep transient model
   review failures distinct from semantic rejection and eligible for bounded retry.

The next acceptance check should include the same 16 openings and Management names,
at least one employer-linked job description and its supported technology signals,
the six antenna tools, no partner investor/careers crawl, and no company-wide
ISO 26262 certification or Indian legal-name/subsidiary assertion from these sources.

## Artifacts and validation

`result.json`, `queue.json`, `sitemaps.json`, native HTML, fetch attempts, model
requests/responses, catalog searches, corrections and usage remain together in the
run directory. `implementation/` and `implementation.json` preserve the Python code
used by the run. No credentials are included in those snapshots.

Reproduce with `benchmarks/recheck_novelic.py live`, the settings above and a new
output directory; the `--snapshot` argument supplies only the catalog in live mode,
and `--catalog` can override that file. Run `benchmarks/audit_autonomous_novelic.py`
against the completed directory to regenerate the automated audit and unsubmitted
payload. The source-audit issue list records the additional checks described here.

Validation passed: **67 Python tests**, including the real-browser recovery tests
and MCP protocol tests; **34 backoffice tests**; Ruff, ty, and wheel/source builds.
No database submission, deployment, new company crawl, or PDF/OCR work was performed.
