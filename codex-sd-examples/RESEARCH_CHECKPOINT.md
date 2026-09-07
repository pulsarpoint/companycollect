# Company extraction checkpoint — 7 September 2026

## Resume here

**Follow-up completed:** package 0.5.0 / schema 1.4 and the
[NOVELIC recheck report](company_research/NOVELIC_RECHECK_RESULTS.md) now supersede
the extraction/discovery baseline discussed below. The report links the company
JSON, frozen engineering replay and guided Careers/application extraction.
They recovered 16 distinct management people, 16 Careers listings, five preferred
job technologies and six specific engineering tools with qualified experience
signals. Six reversed customer relationships were corrected with original records
retained. Fifty-five tests, Ruff, ty and package builds passed.

Keep the PDF/OCR work paused. Next priorities are browser recovery after context
closure, entity/record consolidation, company-name variants, and external evidence
for ownership/financial gaps. The final queue fairness changes have test coverage;
the Careers follow-up was guided, not a complete autonomous rerun of the final queue.
Source matching and structured model review remain distinct from independent
verification. All retained raw records are not automatically safe to ingest.

Current stages: native cleaned HTML → extraction without catalog tools → exact
source checks/evidence-only repair → local/focused catalog resolution → structured
relationship/technology review → one bounded correction and re-review → sourced
summary. Original HTML, unsuccessful diagnostics and correction lineage remain saved.

The user paused PDF/OCR experimentation and returned the priority to accurate
website extraction: jobs, specific named technologies, owners and connected
companies, contacts, people, company activities and described services. Discover
financial-report links as useful sources without reopening PDF interpretation.
Keep the optional full-company narrative report as a separate capability.

This checkpoint saves decisions, evidence and unfinished work. It does not claim
that the crawler issues below have been fixed. No live crawl, paid model request,
deployment, RustFS upload or database migration was performed for this checkpoint.
Existing benchmark originals and outputs remain unchanged.

## Decisions to retain

- Use **Crawl4AI native `cleaned_html`** for website extraction. The production
  approach must work without advance knowledge of job CSS selectors. Retain exact
  snapshots; generic overlapping windows can bound input, with merged results.
- Use **DeepSeek Flash through OpenRouter** for bounded selection/extraction.
  Record the actual model, provider, reasoning settings, failures and reported
  cost. Provider routing and timeouts materially affected our experiments.
- Assess each candidate link for potential across objectives, including whether
  it is navigation to useful content. Examine **all objectives on every fetched
  page**, even when one objective caused its selection.
- The Python controller owns the queue, retries, deduplication and budgets.
  Crawl4AI fetches/renders HTML; the LLM makes semantic assessments. Native
  Crawl4AI extraction/adaptive crawling did not remove our validation needs.
- A jobs list is navigation to detail pages. Job descriptions supply technology
  evidence; a job title alone does not establish a deployed company technology.
- Technologies must be specific named tools, applications, platforms, libraries,
  languages or hardware products. XML/JSON, generic radar/AI/FPGA capabilities,
  architectures and general engineering disciplines are not technology identities.
  Radar-system development, outsourced hardware design and custom embedded-device
  development belong in **services with meaningful descriptions** when offered.
- Preserve technology statement type, company/team/role scope, employer, source
  job, date and alternatives. Required experience is not confirmed internal use;
  a staffing agency's client technology belongs to the named client.
- Match a local ClickHouse-synced technology catalog, case-insensitively, with
  aliases for real spelling/name variants. Search the catalog before proposing a
  new technology. Keep proposals auditable; administrator acceptance links them
  to the canonical catalog. Do not create a second permanent technology identity.
- Preserve source URLs and evidence fragments. `source_matched` means source
  presence, **not semantic verification**. Failed checks remain `needs_review`;
  do not silently discard a correctly discovered person or job because a generated
  quotation omitted intervening text.
- Preserve `not_found` versus `not_assessed`, partial extraction and remaining
  promising links. Finding one record does not complete an objective.
- Owners, subsidiaries, partners, customers and suppliers are distinct relations.
  Website certification claims are not independent certificate verification.

The [package README](company_research/README.md) describes the implemented API,
CLI, defaults and limits. Current source has nine objectives, package version
0.4.0 and result schema 1.3. Historical benchmark outputs use older versions;
their accepted categories must not override the policy above.

## What the NOVELIC evidence says about discovery

The completed reference task is
[Analyze Novelic company](codex://threads/01a07c8e-ebaf-7fa2-8dfc-f935cc279dd8).
Its [frozen public actions](company_full_analysis_lab/reference/public-actions.json)
contain 76 items: 24 web events, 21 commands, 17 image inspections and other task
events. The web events contain 11 search batches, five page-open events, one
find event and seven events without exposed targets. Neither `sitemap` nor
`robots.txt` appears in this export. Missing targets and omitted command outputs
mean this is **no visible sitemap request**, not proof that every internal action
is known. It is not an export of private reasoning.

Observable actions include:

1. Search for NOVELIC and ownership, then targeted `site:` searches.
2. Open the company About page and Sona's subsidiary-financial-statements page.
3. Follow related-company disclosures and counterparty partner sources.
4. Parse Careers links to the external `novelic.oneassessment.com` job board.
5. Download documents, search cached text and inspect difficult PDF pages.
6. Search again for specific gaps and conflicting dates before writing the report.

The first query already names Sona. The trace does not establish how that initial
hypothesis arose, so do not hardcode or accept such relationships without evidence.
It also demonstrates job-link discovery, not exhaustive job-detail extraction.

OpenAI documents model-directed search, page opening and in-page searching, with
the model deciding whether more searching is needed. This supports an adaptive
research loop; it does not document a mandatory company-specific sitemap-first
algorithm. Tool availability and configuration vary between environments.
Sources checked on this checkpoint date: [web search guide](https://developers.openai.com/api/docs/guides/tools-web-search)
and [Codex CLI](https://learn.chatgpt.com/docs/codex/cli).

Our crawler already reads `/sitemap.xml`, sitemap declarations in `robots.txt`,
nested sitemap indexes and links from fetched pages in
[discovery.py](company_research/src/company_research/discovery.py). A sitemap is
an inventory of candidate URLs; it does not establish which information is present
or identify every external recruiting/parent-company source. The missing capability
to evaluate next is targeted source discovery driven by unresolved objectives.

The [earlier pattern analysis](company_research/reference_analysis/novelic-01a07c8e/analysis.md)
and [pattern definitions](company_research/reference_analysis/novelic-01a07c8e/patterns.json)
remain useful. Their 66-item snapshot was taken while the task was running;
use the completed export above when discussing completed action counts.

## Concrete crawler issues to revisit

The preserved [NOVELIC run](company_research/data/novelic-profile-20260907T152619Z/result.json)
ended `partial` at an eight-target page budget. Six pages fetched, two failed;
three of the fetched pages had partial extraction. This was an older prompt/run,
not a measurement of the current prompt's accuracy.

| Observed result or current code gap | Next verification or change |
|---|---|
| No careers page or individual ad fetched; zero job records | Trace the careers link through the external board to detail pages; check navigation scoring, pagination and available source content |
| Three targets spent on LinkedIn variants, two failing | Deduplicate equivalent external targets and avoid repeatedly spending the external budget on the same inaccessible source; preserve distinct job/detail URLs |
| Link-assessment deadlines and provider errors | Keep failures distinct from low relevance; inspect batch size and retry handling before interpreting missing objectives |
| 39 technology candidates, all `needs_review`, including broad categories | Re-extract cached HTML with the current policy; separate eligibility, catalog-search success, source matching and company attribution failures |
| Technology samples have missing company evidence and unsuccessful catalog lookup | Verify attribution context and required tool execution without accepting every technology on the company's domain as company usage |
| 87 service/product observations, 74 source matched | Audit overlap duplicates and headings without adequate descriptions; record count is not a count of distinct verified services |
| Six of eight location records and all four certification records needed review | Inspect exact failed anchors/fragments and entity/scope attribution before changing validation |
| `Relationship` has direction/date but no typed percentage or person-owner classification | Add a precise ownership contract when implementing owner extraction: person/company, stake if stated, date, direct/indirect scope and source evidence |
| `crawlable_url()` excludes PDFs and other documents from the page queue; no dedicated report-link inventory | Retain useful document candidates in a separate output collection instead of trying to crawl them as HTML |

The current prompt already excludes generic technology categories and asks for
described services. Those edits still need measured end-to-end validation.
Earlier [technology results](company_research/TECHNOLOGY_RESULTS.md) include
categories now excluded; use their fixtures for attribution behavior, not as the
final eligibility standard. Likewise, smoke-test counts are not recall scores.

## Next bounded work

1. **Build a source-backed NOVELIC acceptance set.** Reuse saved HTML, queues and
   model responses; label missing fetches, extraction errors, validation errors,
   catalog failures and duplicates separately. Read source pages to establish
   expected facts; neither Astra's narrative nor previous LLM output is ground truth.
2. **Repair and measure core extraction first.** Jobs plus job-derived named
   technologies, described services, owners, contacts, people and certification
   scope. Use existing frozen corpora to check regressions. Do not recrawl merely
   to compare a prompt against unchanged input.
3. **Strengthen discovery around unresolved objectives.** Start with homepage,
   sitemap and navigation candidates; extract; then choose useful follow-ups.
   Follow official recruitment destinations and supported parent/subsidiary
   relationships. Add bounded targeted web search when those sources leave gaps.
   This search extension is proposed, not currently part of `company_research`.
4. **Collect document links without parsing.** Suggested fields: document URL,
   discovery-page URL, link label/context, probable type, associated entity,
   stated period/date if visible, evidence, and `content_examined=false`.
   Use unknown values when the HTML does not establish them. A filename containing
   a year is a discovery hint, not verification of the reporting period.
5. **Run a bounded live NOVELIC crawl after the fixes**, then sites with different
   layouts. Compare unique supported records, correct attribution, missed expected
   facts, rejected candidates, remaining coverage, requests, latency and known cost.

Proposed control flow: classify site → discover candidates → score for all
objectives → fetch with Crawl4AI → extract all objectives → validate/merge →
identify specific gaps → follow another candidate or perform targeted search →
return attributed JSON, discovered documents and explicit gaps. Keep queue state
and budgets in code; ask the model only for bounded semantic decisions.

## PDF/OCR research paused: findings worth keeping

The complete [local PDF benchmark](company_full_analysis_lab/PDF_BENCHMARK_RESULTS.md),
[OpenRouter OCR benchmark](company_full_analysis_lab/PDF_OPENROUTER_RESULTS.md)
and [proposed PDF service](company_full_analysis_lab/PDF_SERVICE_DESIGN.md) preserve
the detail. Do not rerun these as a prerequisite for fixing website extraction.

- Docling is a document-layout/table pipeline with selectable OCR; it is not
  simply a Tesseract wrapper. The tests used standard Docling with Tesseract or
  RapidOCR, **not** Granite-Docling or another OCR VLM.
- Six selected pages from three NOVELIC/Sona PDFs exposed orientation, table
  association, wrong entity/date and numeric normalization failures. Neither
  successful conversion nor plausible prose established correct financial facts.
- Mistral OCR plus DeepSeek recovered 7/22 exact normalized targets, versus 11/22
  for the completed local-output comparison. These are small diagnostic output
  checks, not general OCR rankings; one local page used a different provider.
- Manually splitting a two-page spread and rotating the sideways table improved
  three AOC-1 values from 0/3 to 3/3. This did not implement automatic orientation.
- Mistral preserved all six checked raw cash-flow amounts/signs, but DeepSeek
  interpreted grouping dots as decimals, making normalized values 1,000× too small.
  Arithmetic reconciliation cannot detect a uniform scale error. Preserve raw
  strings, units, locale, headers, signs, entity and period before normalization.
- Known experiment charges totalled $0.037014465; failed/cancelled calls had
  additional unreported costs. Provider rate limits and reasoning-only output
  were execution failures, separate from extraction accuracy.
- The proposed asynchronous service would accept a PDF URL, return a durable job
  ID, expose status/read, archive originals on RustFS/S3 before extraction, and
  retain original URL plus S3 reference/hash/page provenance in reports. Ordinary
  polling does not need an LLM. A later document-analysis agent remains optional.
- Existing Dagster Norway extraction uses PyMuPDF native text and a Tesseract
  fallback. Its raw-PDF cleanup after normalized storage must **not** be copied
  into a report archive that needs enduring original sources. No new PDF service
  or RustFS archive was deployed during these experiments.

The reviewed OCR-VLM shortlist from
[AWESOME-OCR-LLM](https://github.com/yuliang-liu/awesome-ocr-llm) was
[PaddleOCR-VL 1.6](https://huggingface.co/PaddlePaddle/PaddleOCR-VL-1.6),
[MinerU 2.5 Pro](https://huggingface.co/opendatalab/MinerU2.5-Pro-2604-1.2B),
[Granite-Docling 258M](https://huggingface.co/ibm-granite/granite-docling-258M),
[DeepSeek-OCR 2](https://huggingface.co/deepseek-ai/DeepSeek-OCR-2) and
[HunyuanOCR](https://github.com/Tencent-Hunyuan/HunyuanOCR). These were researched,
not tested. None was found as an OpenRouter model in the 7 September catalog
check; that is a dated availability snapshot. Mistral's file-parser service was
available and was tested as described above. Recheck availability only when this
work resumes.

## Artifact map

| Topic | Starting point |
|---|---|
| Standalone crawler, run configuration and current limitations | [company_research/README.md](company_research/README.md) |
| Site classification and conditional objectives | [SITE_PROFILE_FLOW.md](company_research/SITE_PROFILE_FLOW.md) |
| Crawl4AI link-selection behavior | [CRAWL4AI_LINK_SELECTION_ANALYSIS.md](jobs_extraction_lab/CRAWL4AI_LINK_SELECTION_ANALYSIS.md) |
| HTML versus Markdown, repeatability, generic chunks | [NATIVE_CRAWL4AI_RESULTS.md](jobs_extraction_lab/NATIVE_CRAWL4AI_RESULTS.md), [VALIDATED_HTML_RESULTS.md](jobs_extraction_lab/VALIDATED_HTML_RESULTS.md), [HTML_WINDOWS_RESULTS.md](jobs_extraction_lab/HTML_WINDOWS_RESULTS.md) |
| All-objective extraction and quotation problems | [company_objectives_lab/RESULTS.md](company_objectives_lab/RESULTS.md), [native comparison](company_objectives_lab/NATIVE_RESULTS.md) |
| Technology identity, company/domain observations and DB plan | [TECHNOLOGY_DB_IMPLEMENTATION_PLAN.md](company_research/TECHNOLOGY_DB_IMPLEMENTATION_PLAN.md), [mapping notes](company_research/technology_mapping/README.md) |
| Exact prompt, frozen Astra reference and DeepSeek SDK comparison | [company_full_analysis_lab/README.md](company_full_analysis_lab/README.md), [RESULTS.md](company_full_analysis_lab/RESULTS.md) |
| PDF service design and both OCR benchmarks | Links in the paused-research section above |

The report files and code are saved in the workspace. Large `data/` artifacts are
locally preserved and Git-ignored; they are not a remote backup. Do not delete them
when resuming. In particular, retain `company_full_analysis_lab/data/pdf-parser-20260907/`,
`data/pdf-openrouter-20260907/` under that lab, and the NOVELIC crawler run linked
above. The OCR folder includes `comparison.json`, `attempt-ledger.json` and
`integrity-check.json` alongside raw requests, responses and extracted text.

Technology alias/proposal migrations, loader changes and backoffice review code
also exist in the working tree. Consult those changes and their tests before
implementing them again; the broader observations design remains a plan. No
deployment is requested. Credentials remain in existing environment files and
must not be copied into reports or checkpoint artifacts.
