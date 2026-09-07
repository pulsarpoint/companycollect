# NOVELIC extraction and discovery recheck

Saved 2026-09-07/08, package 0.5.0 / schema 1.4. PDF/OCR work remains paused.

The extraction improved substantially after separating catalog matching from HTML
extraction and adding focused evidence repair and structured interpretation review.
This is a regression investigation on NOVELIC, with prompts revised against its
failures. It is not a held-out accuracy benchmark or a demonstration of exhaustive
website coverage.

## Results to inspect

| Artifact | What it establishes | Result |
|---|---|---|
| [Company JSON](data/novelic-live-v14-final/result.json) | Twelve-target live crawl, followed by saved-source evidence repair and bounded interpretation corrections | 16 distinct management people; 17 role records. One job ad; five preferred-experience technologies. Six corrected customer relationships plus five supported partner relationships. |
| [Engineering-page JSON](data/novelic-replay-v14-reviewed/result.json) | Frozen antenna-design and annotation HTML, with current extraction and focused interpretation review/correction | All six expected engineering tools retained as `mentioned`, with experience context. No accepted XML, JSON or CVAT application-use claim. |
| [Careers and application-page JSON](data/novelic-job-extraction-v14/result.json) | Guided discovery follow-up, using unmodified Crawl4AI HTML | All 16 Careers listings extracted. The external internship detail produces a seventeenth record for an already listed opening; these are not 17 distinct vacancies. |
| [Guided fetch probe](data/novelic-job-links-v14/result.json) | Careers and the known external application link both load in Crawl4AI | Both returned usable HTML. The external internship page displays an application action. Other application pages were not individually checked. |

The company JSON remains `partial`. Its original crawl reached eight usable pages
out of twelve attempted targets. The last four fetches failed because the browser
context had closed, **not because these URLs were proved missing**. Three model calls
also timed out. Original page errors and coverage remain in the audited result;
postprocessing did not turn incomplete crawling into complete coverage.

The revised queue fairness/navigation ordering was added after that live pilot.
It passes the package tests, including a case where finding one job previously
allowed missing people records to starve Careers. The guided Careers probe proves
that the useful page is accessible and extractable; it is not an autonomous rerun
of the entire website with the final queue.

## What changed in the package

1. **Discovery and queue ordering.** Sitemap URLs and ordinary HTML links remain
   complementary inputs. Assessment batches nominate candidates across objectives;
   failed assessments get bounded retries and internal exploration. Crawl attempts
   are shared across objectives, with navigation pages receiving an initial turn.
   Social profiles remain contact information without consuming external crawl
   slots. LinkedIn regional/tracking variants normalize without collapsing job IDs.
2. **Extraction and catalog lookup are separate requests.** DeepSeek extracts
   source names from native cleaned HTML. Exact/case-insensitive/alias matches are
   resolved locally, and a small focused request searches the pinned local catalog
   for the remaining names. New technology proposals remain administrator-review
   candidates. Catalog failure excludes a technology from the accepted summary.
3. **Evidence repair changes quotations, not facts.** The model gets fixed records
   and their original HTML window and returns separate exact fragments. Company,
   role and employer attribution are checked per record. HTML line breaks and
   punctuation in composite roles/addresses no longer cause unnecessary field-anchor
   failures. Quotation presence itself still requires a contiguous source match.
4. **Interpretation review returns structured meaning.** A yes/no critic approved
   all six reversed customer claims in the first diagnostic. The replacement review
   identifies the source-supported parties, specific-technology eligibility and
   usage signal; Python checks these against the proposed record. Partnership is
   symmetric; customer/supplier and ownership predicates are directional.
5. **One bounded interpretation correction.** When review identifies a reversed
   pair or a different technology signal, the code can construct a narrowly changed
   candidate from the same verified source. That candidate is reviewed again. The
   original stays `needs_review`, linked to the correction; missing ownership facts
   cannot be invented by this step. This remains model-assisted checking, not
   independent verification.
6. **Documents and ownership fields.** `document_links` records report/certificate
   URLs and discovery context without fetching PDFs. Unclassified document
   candidates are also retained. Relationships distinguish people, companies and
   brands and can carry explicitly supported ownership percentage, scope and date.
   Routine purchase conditions and policies do not qualify as company reports.
7. **Failure reporting.** An empty extraction despite a visible email link triggers
   a recheck. Identical duplicated JSON payloads can be unwrapped; conflicting
   documents remain invalid. Page/model-budget stops are explicitly partial.

## Checked facts and distinctions

- **People:** the saved Management page contains 16 distinct named professionals.
  Four role records passed the revised local formatting checks immediately; focused
  evidence repair recovered the remaining people. The seventeenth record is another
  source's wording of Raffaele Soloperto's role, not another person.
- **Jobs:** the Careers source lists 16 openings. The external DSP internship ad
  independently names VHDL, Verilog, Matlab, Simulink and Python as advantages. These
  five signals are `preferred_experience` for that role, not evidence that all five
  are deployed throughout NOVELIC. The board's cookie/analytics tools were not
  accepted as NOVELIC's stack.
- **Engineering technologies:** CST Studio Suite, Ansys HFSS, WIPL-D Pro CAD,
  ADS Momentum, ADS and AWR Microwave Office are explicitly listed under experience
  with design tools. The corrected records preserve that distinction using
  `mentioned`; the old `stated_use` records remain reviewable. These tool identities
  are proposals against the pinned catalog, not automatically approved catalog entries.
- **Services:** antenna design, electromagnetic simulation and automotive annotation
  are supported offerings with descriptions. Formats such as JSON/CVAT XML can be
  description details without becoming technology observations. The broader company
  result has 92 source-matched offering records, **not 92 distinct services**: product,
  page and description variants still need consolidation.
- **Relationships:** VBG Coupling, ZF, Fernride, Interventional Systems, Liebherr and
  Firefly are described as NOVELIC clients; the corrected direction is client
  `customer_of` NOVELIC. The unsupported claim that NOVELIC is a subsidiary of
  NOVELIC India stays reviewable. No verified current ownership stake was found in
  these inspected pages. Sona Comstar's partnership mention does not establish its
  ownership interest.
- **Certifications:** the quality page supports website claims for ISO 9001:2015
  and ISO 14001:2015 and a separate `working_toward` IATF 16949:2016 claim. Certificate
  PDF URLs survive in certification records and document candidates. No PDF was
  opened, so certificate validity/issuer/current status is not independently verified.
- **Financial reports:** no supported financial-statement or annual-report link was
  found in this bounded site inspection. A purchase-conditions PDF was a false
  positive, now flagged. This does not establish that financial reports do not exist.

## Remaining problems

- **Browser lifecycle:** the long pilot lost its browser context after page eight.
  Recovery/restart is still needed; retrying against that same dead context wastes
  the remaining page budget. Treat these outcomes as infrastructure failures.
- **Entity and record consolidation:** the same person, job, service or contact can
  have different descriptions/source URLs. Exact-record merging is insufficient
  for analytical counts. AURIX also has dated/undated observations which must not be
  counted as different technologies.
- **Attribution/name variants:** several Serbian contacts use the extracted name
  `NOVELIC d.o.o.` while the supporting source names NOVELIC differently. These remain
  reviewable; evidence-only repair deliberately cannot rewrite legal identity. The
  latest address-format and form-URL checks are covered by code validation but were
  added after the saved company audit; the published audit does not claim those
  records have all been recovered.
- **Document labels:** invented labels such as `ISO 9001 certificate` did not match
  the exact link label. The links are still available through certification records
  and `discovery.document_candidates`, but those duplicated `document_links` rows
  remain reviewable. A quality-management HTML index can also be misclassified as a
  certificate document; do not assume every discovered reference is a PDF.
- **Review is fallible:** a bare yes/no critic failed, and the first structured
  critic confused specific-tool eligibility with usage support. Few-shot examples
  corrected that regression. Check other companies before treating this prompt as
  generally reliable. Generic-name guards supplement this review; they are not a
  complete technology classifier.
- **External research/coverage:** ownership and financial gaps may need parent-company
  disclosures, registries or targeted web search. The package follows linked pages;
  it does not yet run an independent external research/search workflow.

Downstream consumers must use each finding's **overall** `evidence_status` and review
metadata. A source can match its quotations while the resulting claim is rejected.
Do not submit every technology proposal found in the raw retained records; use the
accepted, attributed technology output and preserve its provenance.

## Configuration, costs and reproducibility

Reported successful stages used `deepseek/deepseek-v4-flash-0731`, provider `together`,
reasoning `none`, native Crawl4AI cleaned HTML, and a pinned 7,981-entry catalog from
the earlier NOVELIC snapshot. Catalog refresh was intentionally disabled for these
comparisons. Raw requests, provider identity, token usage, evidence and source hashes
are saved alongside each result. Request JSON omits authorization headers.

| Reported artifact chain | Known model cost, USD | Calls with unknown cost |
|---|---:|---:|
| Live company crawl + evidence audit + interpretation corrections | 0.10147923 | 3 |
| Frozen engineering-page replay + refreshed review/corrections | 0.01678180 | 0 |
| Careers and internship extraction | 0.00993695 | 0 |

These are measured artifact-chain costs, not the total cost of the whole investigation.
Earlier Parasail/OpenInference/automatic-routing probes, reviews and the interrupted
live run are separate diagnostics. They include empty JSON, HTTP 429 and timeouts;
unknown charges must not be counted as zero. The original failed/interrupted artifacts
remain under `data/novelic-*`, including `novelic-live-v14/run-interrupted.json`.
These observations do not establish a general provider ranking.

Reproduction entry points are [recheck_novelic.py](benchmarks/recheck_novelic.py),
[audit_saved.py](benchmarks/audit_saved.py), [correct_saved.py](benchmarks/correct_saved.py)
and [probe_job_links.py](benchmarks/probe_job_links.py). Use a new output directory.
The source snapshots are local, Git-ignored artifacts, not a remote backup. The
original [acceptance fixture](tests/fixtures/novelic_acceptance.json) identifies the
frozen engineering sources and selected manually checked facts.

Validation: 55 tests passed, including a real-browser website-to-JSON test with the
paid model boundary simulated. Ruff and ty passed; the 0.5.0 wheel and source archive
built successfully. The live/replay model results above are separate from those tests.
No deployment, database write, PDF/OCR experiment or external message was performed.
