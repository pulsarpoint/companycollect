# NOVELIC: DeepSeek through Codex SDK versus the existing Astra report

Completed 7 September 2026. **DeepSeek discovers useful company evidence at low
reported cost, but this run does not match Astra's reliability.** Its company,
service, product and subsidiary descriptions are useful. Its financial synthesis
contains material errors that flow into the outlook. Keep this as an optional
report layer alongside the core technologies/jobs/services extraction, with claim
validation before publication or conversion into company facts.

## What was tested

The exact user prompt from **Analyze Novelic company** was replayed unchanged:

> Can you make full analysis of the novelic.com as a company, everything that we can get about it

DeepSeek ran through the existing Python Codex SDK and desktop CLI, using
`deepseek/deepseek-v4-flash-0731` on OpenRouter with `high` effort. It chose its
own searches, pages, downloads, OCR attempts and report structure. No Astra
findings or source pack were supplied. A separate developer instruction required
public research, citations, and `report.md`; it prescribed no research stages.

This compares two research systems using the same user prompt. Their system
instructions, web tools and PDF capabilities differ. Astra used the desktop web
tools and visual inspection. DeepSeek used OpenRouter search and shell tools.
The completed Astra task is reused, not rerun; its effort and cost were not
available from the task export. No core crawler extraction or database was changed.

## Measurements

| Measurement | Existing Astra task | DeepSeek SDK run |
|---|---:|---:|
| Elapsed time | 46.9 minutes | 20.7 minutes, including retry |
| Report words, whitespace count including tables/source list | 5,002 | 2,988 |
| Unique URLs in report | 51 | 29 |
| Reported API charges | Unavailable | $0.301277373, plus one interrupted request with unknown charges |
| Provider requests | Unavailable | 108; 107 completed with reported usage |
| Public command actions | 21 | 106 |
| PDF sources | 8 preserved in the reference pack | 7 downloaded, recovered after the run |

DeepSeek's recorded searches total 35: 12 in the interrupted request and 23
reported by completed responses. Its known usage is 6,631,685 input tokens
(5,898,752 cached) and 44,039 output tokens. These totals exclude the interrupted
request's unreported usage. OpenRouter's reported charge already includes search;
do not add a separate search estimate to it. Compatibility probes are excluded.

No output-token limit was set: recorded `max_output_tokens` is null. The initial
stream exceeded Codex's default five-minute idle timeout and retried. Subsequent
runner configuration allows a longer idle interval; the completed result above
retains its original configuration and includes that incident.

## What DeepSeek did well

- Identified the company, the legal-versus-operating start dates, founders,
  majority shareholder, downstream entities and international locations.
- Described the main offerings and linked named radar products to applications.
- Distinguished stated ISO certifications from work toward IATF 16949.
- Followed the parent-company connection into annual reports and subsidiary
  accounts. It downloaded documents, attempted OCR, and sought alternative
  disclosures when scanned tables were hard to read.
- Found useful additional research leads, including acquisition-accounting
  notes, impairment testing, historic growth announcements and sample production.
  Their presence does not validate every interpretation in the report.

These are reusable research behaviors. The failed financial synthesis does not
invalidate the earlier clean-HTML jobs/technology extraction results; those are
different tasks with different evidence and output constraints.

## Verified problems

**1. Cash paid to founders became revenue.** DeepSeek reports INR 2,109.62 million
as FY24 revenue. In the source, that is cash paid to founders in the acquisition
table. The adjacent results table gives INR 484.31 million revenue for the
specified post-acquisition period. The report also mixes consideration and
acquired assets in its deal explanation. The relevant two-page spread was
visually checked, so this is a confirmed row-association error, not just a
disagreement with Astra. See printed pages 388–389 of the
[FY2025 annual report](https://sonacomstar.com/annual-report-24-25/assets/pdf/Sona-Comstar-AR-24-25.pdf).

**2. Net assets became an invented revenue estimate.** DeepSeek correctly reads
2.39% and INR 1,464.32 million under net assets, then treats the percentage as a
share of group revenue and uses an approximately EUR16 million revenue scale in
its outlook. That inference is invalid. It also confuses net worth below an
investment's carrying value with net liabilities. Positive net assets and an
investment-valuation gap are different facts. The report's INR186.35 million
loss contribution does exist; the error is not that every number is fabricated.
See printed pages 350–351 of the
[FY2026 consolidated statements](https://sonacomstar.com/annual-report-25-26/pdf/Sona%20Comstar%20AR%202025-26_Consolidated%20Financial%20Statements.pdf).

**3. An extraction failure became “not public.”** The report says post-2022
financials are not public, while admitting it downloaded scanned statements it
could not OCR reliably. Those are different statuses. It also overlooked the
AOC-1 turnover disclosure in material it retrieved. “Found but not reliably
parsed” must remain explicit; it must never become a claim that a source does
not exist. The reference separately handles entity accounts, group schedules and
parent disclosures.

**4. Interpretations became unsupported facts.** The report states the cause of
the FY26 loss as verified and offers a most-probable growth trajectory based on
the invalid revenue estimate. Finding a loss during a product-development period
does not establish its causes. Astra is more careful about causality, unavailable
unit economics and production expectations.

**5. Customers, partners and unknowns are inconsistent.** DeepSeek groups partner
and innovation-program names under customers, then says exact customer names
are not disclosed. It nevertheless mentions Firefly itself. The official
[Firefly case study](https://www.novelic.com/blog/case-study-powering-seamless-smart-lighting-control-with-human-presence-detection-for-firefly/)
is a named customer example; Astra also found VBG. Partnership, customer and
program-participation relations must remain separate.

**6. Citation quality is weaker.** Most citations sit in a source list rather
than next to claims. A GET check found 26 of 29 URLs returned 200 and three
returned 404—all three annual-report directory URLs. Working PDF URLs appear in
the action trace, but the final report replaces them with broken directory links.
An HTTP 200 response only establishes reachability, not claim support.

**7. Conflicts and coverage remain unresolved.** DeepSeek uses the annual-report
acquisition date of 6 September without discussing the 4 September date in the
[contemporaneous closing notice](https://nsearchives.nseindia.com/corporate/SONACOMS_04092023225204_Intimation.pdf).
The same notice gives adjusted consideration of EUR40.097 million, avoiding the
report's uncertain currency conversion. DeepSeek misses current job listings,
grant allocations, several current executives, counterpart-verified partnerships,
and much of Astra's product/safety nuance. Its mass-production language also lacks
the reference's reconciliation of dated launch guidance.

## Claim review

The 24 reference criteria yielded **6 covered, 8 partial, 5 missing, and 5 with
material errors**. These are overlapping topic assessments, not an accuracy
percentage and not five independent mistakes. The detailed notes distinguish
omissions from contradictions and source-access failures. The source-checked
financial errors above carry more significance than a missed historical topic.

The reference is not treated as truth by definition. For example, DeepSeek's
INR186.35 million contribution figure is present in the source even though Astra
uses a different AOC-1 measure. A different number is not automatically wrong;
its field, consolidation scope and period determine whether it is comparable.

## Recommended next experiment

Keep the same broad user prompt and DeepSeek model. Improve the evidence tools
and validation around them:

1. Give the agent a reusable page reader using Crawl4AI cleaned HTML, preserving
   observed URLs, page titles, dates and source IDs. The agent should choose the
   pages; it should not repeatedly write ad-hoc curl/HTML-cleaning scripts.
2. Give PDFs a separate reader that preserves page and table structure. Return
   row labels, headers, units and periods together. If extraction fails, return
   `extraction_failed` and the source reference; route the small uncertain table
   to visual extraction or review.
3. Extract evidence-backed claims before drafting. A financial claim needs
   company/entity, metric, value, currency/unit, reporting period, consolidation
   scope and source/table reference. Validate calculations in code and require
   compatible metrics before combining them.
4. Draft only from accepted claims. Keep interpretation separate. Require an
   actual retrieved source URL near each material claim and check citations.
5. Repeat NOVELIC first, then several different company types. Use Astra for
   unresolved or high-impact claims if needed, while retaining DeepSeek for the
   broad collection and draft work.

This preserves the two priorities: core structured extraction for technologies,
jobs, services and contacts, plus an optional deeper company report. It does not
promote narrative assertions directly into the company database.

## Artifacts

- [Unmodified DeepSeek report](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/openrouter-20260907T165736Z/report.md)
- [Frozen Astra report](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/reference/novelic-company-analysis.md)
- [Measured comparison](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/openrouter-20260907T165736Z/comparison.json)
- [Detailed claim review](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/openrouter-20260907T165736Z/claim-review.json)
- [Citation audit](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/openrouter-20260907T165736Z/citation-audit.json)
- [Public SDK actions](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/openrouter-20260907T165736Z/actions.jsonl)
- [Provider search/actions and usage](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/openrouter-20260907T165736Z/provider-events.jsonl)
- [Run configuration and usage](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/openrouter-20260907T165736Z/run.json)
- [Recovered source-cache manifest](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_full_analysis_lab/data/openrouter-20260907T165736Z/cache-recovery.json)

The agent saved PDFs under `/tmp/novelic_pdfs` despite the workspace instruction.
The reviewer recovered the exact files referenced in its commands after
completion. They are separately marked, not presented as artifacts the runner
captured automatically. The original report remains unchanged, including errors.

Validation: four tests cover HTTP stream forwarding, public-only trace capture,
unknown billing, and rejecting smoke probes as benchmark runs. Ruff and ty pass.
