# DeepSeek company objectives — 6 September 2026

DeepSeek extracted most of the checked data when requests returned usable JSON.
The immediate problems are request timeouts and evidence handling: the model
returned all 45 checked people and all 11 checked vacancies, but the strict
quotation gate discarded seven people and ten vacancies. Link assessment is useful
as an initial hypothesis, with weaker detection of unexpected information and
material variation between repeated decisions.

## What was tested

The experiment separates link assessment from extraction. Both use the same seven
objectives, with direct DeepSeek requests through OpenRouter. Extraction receives
the entire native Crawl4AI `cleaned_html` and its source URL, independently of link
scores. No company-specific selectors or custom HTML simplification are used.

The corpus contains Handelsbanken, Kongsberg, Polarbröd, Pricer, Tobii, TRUMPF,
Vaisala, and Vestas: 1,600 saved candidate links and 40 destination pages. Full
cleaned HTML totals 3,368,582 characters, ranging from 17,027 to 458,645 per page.
Native markdown totals 715,791 characters, but is not used in this experiment.
The Vestas customer URL returns HTTP 404 and remains in the corpus as a control.
The jobs page redirects to an external careers host; relative links use that host.

This is a pilot over an **existing upstream shortlist**, not a new end-to-end crawl
from raw sitemaps. None of the candidates has anchor text; most have titles, but
all 200 Vaisala titles are missing. Extraction pages were selected by source review,
not selected by the model's ranking. That separation makes the two tasks assessable
independently, but does not establish end-to-end crawl performance.

The reference was frozen before extraction inference and before inspecting
selection responses. It has 143 positive extraction checkpoints, 72 known-useful
page/objective pairs, ten negative page/objective labels, and four negative
extraction controls. Codex reviewed the saved source; these are not independent
human annotations or exhaustive truth labels.

Settings: `deepseek/deepseek-v4-flash-0731`, provider `baidu/fp8`, no fallback, low
reasoning, temperature zero, concurrency three, and a 65,536-token output budget.
This budget is not a model maximum. Each logical request has a 180-second total
deadline and up to two HTTP attempts within it. One validation correction is allowed;
service errors do not receive a correction. Valid peers survive a failed correction.

## Link selection

The full pass attempted all 40 batches. It retained complete assessments for
**1,120/1,600 candidates (70%)**. Twelve batches timed out. Three additional
corrections fixed two malformed JSON responses and one unknown candidate ID.
Known reported cost was **$0.042411159**; costs for the 12 timeouts are unknown.

“Identified” means high or medium potential for the objective. “With assessment”
excludes labelled pages whose entire batch failed; all known-useful labels remain
in the last column so reliability is not hidden.

| Objective | Identified / with assessment | Identified / all known useful |
|---|---:|---:|
| Company profile | 8/9 | 8/11 |
| Company contacts | 8/10 | 8/12 |
| Locations | 4/5 | 4/7 |
| Products/services | 11/11 | 11/16 |
| People | 4/8 | 4/12 |
| Company relationships | 4/5 | 4/6 |
| Jobs | 7/7 | 7/8 |
| Total | 46/55 | 46/72 |

There were no useful classifications among the eight assessed negative labels;
two negative labels had no assessment. This tiny negative sample is insufficient
to estimate precision over all 1,600 links. Three detected positive labels had a
different direct/navigation role from the source reference.

Most observed misses concern information outside the obvious page purpose:

- Handelsbanken's contact page contains registration and LEI identifiers.
- Kongsberg's vacancy contains recruiter contacts and an office address.
- Polarbröd's contact and careers pages contain named employees.
- Polarbröd's locations page mentions another company in the group.
- Vaisala's services page has a helpdesk email; its careers page has employee stories.

This supports extracting all enabled objectives from each fetched page. A low
people score should not suppress people extraction on a contact or careers page.
The Vestas customer URL scored high for relationships but returned 404: plausible
metadata is a hypothesis, and fetching must establish whether evidence exists.

A fixed offline schedule of 20 unique pages per site covered 30 of the 72 reviewed
useful page/objective pairs. It cycles across objectives, ranks high before medium
and direct before navigation, and breaks ties by URL hash. This is an illustration
of a shared crawl budget, not an optimized scheduler; unreviewed selected pages
could also satisfy the objectives, and navigation destinations were not followed.

## Extraction

All 40 pages were attempted. There were 64 logical requests, including 24 correction
requests, with **$0.085989850** in known reported cost. Nine requests exceeded the
180-second client deadline; their costs are unknown. Seven pages ultimately had
no usable extraction: `p010`, `p011`, `p023`, `p024`, `p025`, `p034`, and `p035`.
Six had an initial timeout; `p034` had malformed JSON followed by a correction
timeout. Two other correction timeouts preserved their initial valid records.

There are **657 retained observations**, including repeated facts across pages and
some alternate spellings after corrections. This is not a count of unique correct
facts. The raw-record diagnostic below was added during analysis to distinguish
model output from losses at the evidence/URL validation boundary; it does not
change prompts, labels, or retained results.

“With response” means the checkpoint's page produced at least one usable JSON
envelope. “Raw matches” satisfy the record schema and checkpoint fields before
evidence/URL validation. They are diagnostic matches, not independently proven
record precision. The all-checkpoints column includes failed pages.

| Objective | Raw matches / with response | Retained / with response | Retained / all checkpoints |
|---|---:|---:|---:|
| Company profile | 8/8 | 8/8 | 8/11 |
| Company contacts | 23/24 | 22/24 | 22/24 |
| Locations | 11/11 | 10/11 | 10/11 |
| Products/services | 17/17 | 17/17 | 17/27 |
| People | 45/45 | 38/45 | 38/45 |
| Company relationships | 7/10 | 7/10 | 7/14 |
| Jobs | 11/11 | 1/11 | 1/11 |
| Total | 122/126 | 103/126 | 103/143 |

The 19 checkpoints lost at the evidence gate were ten Vestas vacancies, seven
Vestas executives, one Pricer phone and one Pricer address. For example, the
management source renders `Group President & CEO File title: Henrik Andersen`;
the model quotes `Group President & CEO Henrik Andersen`. Job excerpts similarly
omit repeated labels, dates and intervening text. The checked fields were present,
but the requested contiguous quotation was not. A second full-page request did
not resolve these cases. The untouched source and rejected records remain saved.

The other four raw checkpoint misses comprise three legal-name aliases discussed
below and the Kongsberg support email attributed to `Sales` rather than the
reference's support/company owner. The email itself and its `Global support hotline`
purpose are correct; the department association is ambiguous in the layout and
should not be silently converted into a confidently identified department.

Three negative controls were assessable and passed: Polarbröd employee testimonials
were not vacancies, the 404 page produced no customer relationship, and LEI issuers
were not classified as owned companies. The Vaisala employee-story control was
unassessable because its extraction failed; it is not counted as a pass.

Across attempts, validation recorded 105 evidence-absence issues, 12 URL-absence
issues and three malformed JSON responses. Counts include repeated issues on the
same record after correction. No response reported an output-length finish; the
largest reported extraction completion used 16,770 tokens. Timeouts cannot be
diagnosed as output-limit failures from the available responses.

## Repeatability

A separate repeat uses the first shuffled batch from each company: 320 candidates
across eight sites. Decisions are compared only where both runs return valid
assessments; coverage and failures are reported separately. This is a stratified
repeatability probe rather than a second full selection benchmark.

The repeat completed 8/8 planned tasks, with valid assessments for 240/320 links
and two timeouts. Known cost was **$0.008759235**. Within its 320-link plan, the
original run assessed 200 and the repeat assessed 240. Only **120 links** have
valid decisions in both: Handelsbanken, Polarbröd and Tobii, 40 each. The remaining
80 first-only and 120 repeat-only assessments are excluded from agreement metrics.
This limits the strength and company diversity of the repeatability conclusion.

| Objective | Useful/not-useful agreement | Shared useful links / useful in either run |
|---|---:|---:|
| Company profile | 114/120 | 6/12 (50.0%) |
| Company contacts | 115/120 | 11/16 (68.8%) |
| Locations | 117/120 | 5/8 (62.5%) |
| Products/services | 100/120 | 59/79 (74.7%) |
| People | 118/120 | 2/4 (50.0%) |
| Company relationships | 118/120 | 5/7 (71.4%) |
| Jobs | 120/120 | 2/2 (100%) |

Overall agreement can look high because most links are low potential for an
objective. Agreement on the smaller useful sets is less stable. The two shared
job links are too small a sample to establish job-selection repeatability broadly.

## Cost and implementation checks

| Run | Logical requests | Timeouts with unknown cost | Known cost |
|---|---:|---:|---:|
| Selection, 1,600 candidates | 43 | 12 | $0.042411159 |
| Selection repeat, 320 candidates | 8 | 2 | $0.008759235 |
| Extraction, 40 pages | 64 | 9 | $0.085989850 |
| Total | 115 | 23 | **$0.137160244** |

This is a known-cost sum, not a verified total bill. No extra HTTP retries occurred
within these logical requests. Stop/resume cycles continued pending work and
preserved all recorded failures. These client-deadline failures must be investigated
separately from extraction quality; the responses do not identify their root cause.

The implementation is isolated in `company_objectives_lab`. The shared OpenRouter
client gained an optional response schema, preserving its original job schema by
default. Validation covers candidate coverage, record retention, real HTTP payload
shape, cached resumes, tampered inputs/results, source freezing, scoped repeat
comparison and the distinction between raw and retained checkpoints. The full
relevant suite passes 63 tests; Ruff and type checks pass.

## Recommended next step

Keep DeepSeek for the next iteration and fix the evidence contract before
integrating the crawler. Use separately verifiable source spans or DOM-backed
field evidence so a correct record does not require reconstructing one long quote.
Evaluate targeted repair of failed records with nearby source context, rather than
regenerating all records and repeating the whole page for every correction.

Then run an end-to-end pilot from fresh sitemaps plus homepage/navigation context,
with an explicit page budget and per-objective progress. Preserve unclassified
links as pending, investigate the deadline/provider behavior, and test a compact
selection output schema. Fetch a multipurpose page once and extract all enabled
objectives, including those its metadata did not predict.

Extend company relationships with explicit joint-venture/associate types,
ownership percentages, and date semantics. The current schema forces Kongsberg's
49.9% interest in Patria and 50% joint arrangements into broad `partner_of` records;
the source evidence retains the detail, but the structured fields lose it. Add
entity-alias reconciliation while preserving legal entities, branches and brands
as distinct concepts. These schema limitations should be resolved before judging
relationship completeness in a production crawl.

## Interpretation limits

The checkpoint matcher requires selected fields within the same retained record,
including contact owner, person role/company, or relationship direction. It allows
documented alternatives and inverse relationships. Most text fields use normalized
substring matching; phone digits ignore punctuation and enum/job URLs match exactly.
It is not a universal exact-field accuracy score. Unlabelled records are not counted
as false positives, and schema/source validation alone does not prove attribution.

For example, correct relationships using “Handelsbanken Fonder,” “Stadshypotek,” and
“EFN Ekonomikanalen” fail checkpoints that require the corresponding full legal name
with “AB.” The same output supplies those legal names as company facts. These are
entity-alias reconciliation issues in the evaluator, not evidence that the three
relationships were absent from the output. Frozen labels are preserved.

Strict quotation validation can also reject supported content. In Handelsbanken's
HTML, a tag boundary renders “1 ,800 advisors”; the model writes “1,800 advisors.”
The validator rejects that excerpt, and repeating the full request does not always
fix it. Correcting only the failed record with its nearby source context is worth
testing, alongside DOM-aware evidence matching that preserves factual values.

No result here establishes exhaustive company coverage. This test does not follow
pagination or discovery links, reconcile entities across pages, measure changes
over time, or evaluate related-company crawl expansion. Service failures and empty
arrays are not evidence that information does not exist.

## Reproducibility

See [README.md](README.md) for commands, [reference-v1.json](reference-v1.json) for
frozen checks, and the ignored `data/v1/report.json` for per-check outcomes and
request totals. The report audits source, prompt and request hashes, revalidates
the raw responses and verifies the retained results before computing metrics.
