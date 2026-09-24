# DSPy RLM crawler-service experiment

16 September 2026. **Postponed by the user: implementation and model runs have not started.**
Retain this design for future reference. Continue improving the existing crawler;
do not launch the RLM experiment unless the user resumes it. If revisited, consider
first testing a limited evidence-review/consolidation module against a simple
batched review with the same sources and budget before changing the crawler.

The following is the original proposed experiment, not active implementation work.
Target: repeat the Handelsbanken crawler-service task with direct DeepSeek Flash,
using DSPy RLM to explore evidence and choose semantic subtasks. Preserve the
0.15.2 baseline and its completed low/high results before making runtime changes.

## What RLM changes

The complete source corpus lives in Python variables in an interpreter. The root
model initially sees metadata and can use code to select sections, ask a sub-model
focused questions and combine results. DSPy exposes `llm_query`,
`llm_query_batched`, custom tools and a saved execution trajectory. This is useful
for adaptive evidence exploration; it does not establish factual correctness.
See the [versioned RLM documentation](https://github.com/stanfordnlp/dspy/blob/3.3.1/docs/docs/api/modules/RLM.md).

Pin the initial experiment to [DSPy 3.3.1](https://github.com/stanfordnlp/dspy/releases/tag/3.3.1)
and lock dependencies in a separate environment. The RLM API is experimental.
Its built-in subqueries call an LM directly; they do not each start another RLM
with an independent recursive crawl. Start with one root coordinator and bounded
semantic subqueries. Keep the existing Crawl4AI fetcher, source files, catalog and
output concepts. A DSPy dependency does not belong in the main package yet.

## Division of work

| Stage | Inputs and responsibility | Output |
|---|---|---|
| 1. Identify the site | A small structured classification call reads the homepage and known metadata. Identify site type, operator, target company and objective priorities. | Site profile; relevant objectives; uncertain identity questions. |
| 2. Plan discovery | An RLM invocation examines the sitemap/link inventory and coverage ledger. Select objective-specific hubs and follow-ups. Ordinary Python owns fetching, queue state, deduplication and limits. | Candidate URLs with expected objectives and reasons; new saved pages. |
| 3. Extract page evidence | RLM chooses page or section batches and makes semantic subqueries. Every fetched page receives an all-objective pass; long pages are covered section by section. | Source-grounded statements and typed records, with page and section references. |
| 4. Consolidate company evidence | RLM groups statements by entity and subject, rereads ambiguous sources and uses the catalog search tool. It reconciles repeated observations and asks focused follow-up questions. | Company facts, services, people, contacts, relationships, jobs, technology observations and proposal drafts. |
| 5. Validate and report | Python checks schema, source references and coverage; focused semantic checks resolve ambiguous attribution. Assemble output from saved records. | JSON findings, source links, concise company summary, gaps and review items. |

Discovery and extraction form a loop: extraction may reveal a jobs board, a
second listing page, a group website or a report archive worth fetching next.
Stages 2 and 4 have different inputs and outputs and should be separate bounded
RLM invocations, sharing durable host state. We do not prescribe every internal
search, chunk or subquery; those choices are the behavior the experiment tests.

Classification can remain an ordinary DSPy structured prediction. A single short
page does not need its own RLM interpreter. Page extraction is a worker operation
inside evidence analysis, with a reusable prompt and typed response contract.
Avoid multiplying every page into ten independent calls, one per objective.

## Objectives and source context

Retain all ten existing objectives: company profile, company contacts, locations,
products/services, people, company relationships, jobs, technology signals,
certifications/compliance, and document links. External-link occurrences remain a
separate complete inventory with optional relationship assessments.

Store `pages[page_id]` with URL, fetched time, content hash, unchanged native
Crawl4AI cleaned HTML, title, headings and available source metadata. Build a
reversible section index for navigation; record offsets into the saved HTML.
Keep a mapping from any derived visible-text evidence to its source blocks.
The model can inspect the original page whenever an indexed section is ambiguous.
Do not replace the corpus with summaries alone.

Long-page requests should carry the page title, heading ancestry, identified
company/employer, job identity when applicable and neighboring blocks. Revisit
boundaries with overlap when a claim crosses sections. A coverage ledger records
which sections were actually examined. Merely finding technology keywords does
not count as an all-objective page pass. Missing sections remain unprocessed.

Each source statement preserves its original wording and also describes **how**
the subject is connected to the company. Retain the actor, company/team/role or
product scope, current/planned/historical timing, required/preferred/used/offered
relationship, alternatives, and evidence references. A normalized identity is an
additional field, not a replacement for this context.

For example, an advert saying its team has Azure Pipelines expertise yields a
team-expertise observation with the advert as source. It does not become a
requirement for that particular role or proof of company-wide deployment. A job
hosted on Jobylon keeps Handelsbanken as employer when the source establishes it;
Jobylon and its map providers remain distinct website operators/providers.

## Small tool surface

The first offline experiment needs only source inspection, catalog search and
validated checkpoint functions. Keep the page inventory and intermediate records
in interpreter variables instead of creating tools for every dictionary access.

- `read_source(page_id, section_ids)` returns source blocks and provenance.
- `search_technologies(query, category)` reuses the frozen local catalog and
  existing search behavior; category lookup can share the same catalog boundary.
- `save_findings(records, coverage)` validates and checkpoints records in the run
  directory, returning IDs and per-record errors. It never submits backend data.

In the later live experiment, add `fetch_page(url, objectives)` through the host
controller. The host enforces target scope, public URL checks, page/time budgets,
cache and Crawl4AI rendering, then updates the source inventory. Sitemap discovery,
HTML navigation, pagination and relevant external boards are complementary leads.
An objective score ranks exploration; it never suppresses other objectives on a
page that has been fetched.

## Validation changes required by the baseline findings

1. Preserve actual company/team/role scope. Remove the assumption that everything
   on a job page must describe the advertised role.
2. Separate evidence acceptance from catalog readiness. A supported Ab Initio
   observation survives while its proposed vendor URL or description awaits review.
   Do not invent a canonical technology ID or auto-approve a proposal.
3. Enforce entity types. DORA is a regulation, WCAG a standard and generic VPN a
   category; they must not enter the specific-product technology catalog. Preserve
   useful compliance or capability context in the relevant objective. Company-held
   certification still needs explicit attribution; a regulation mention is not one.
4. Store multiple exact evidence fragments when text intervenes. Match each to
   the same source and inspect their relationship; do not fabricate a contiguous
   quotation or accept unrelated keyword matches as support.
5. Separate an external link's purpose from a corporate relationship. A recruitment
   link can point to a group's subsidiary; these are independent attributes.
6. Keep raw records, accepted records and review items separately. Report per-record
   failures without discarding an otherwise useful batch.

The experiment can propose extended record fields, but its mapping back to current
output must flag unsupported semantics instead of silently forcing legacy enums.
Use the same independently audited acceptance criteria to evaluate both approaches.
Do not silently repair the historical baseline and attribute that gain to RLM.

Coverage should distinguish found, not found in examined sources, not examined,
blocked and partial. An empty array alone must not imply the company lacks that
attribute. Financial-report links are in scope; PDF downloading/OCR and report
content extraction remain paused for this experiment.

## First test: frozen sources, then live discovery

Start in a separate `crawler_service_rlm_lab/` directory beside the main package.
Use its own environment, pinned DSPy dependency, prompts, runs and benchmark code.

1. **Integration smoke:** a few frozen pages check direct DeepSeek request routing,
   high reasoning, source tools, schema failures, durable output and token accounting.
   Inspect the outgoing request metadata to confirm DSPy passes the intended model
   and reasoning options. Credentials stay in the host client.
2. **Technology comparison:** use the same 16 Handelsbanken IT HTML snapshots and
   frozen 7,981-entry catalog as the completed low/high experiment. Compare against
   both baselines, principally the high run. Start with high reasoning for root and
   workers so a mixed-effort policy is not an additional variable. Score source
   statements before catalog review and accepted records afterward.
3. **Other objectives and links:** test the 12 saved company pages and all 507 link
   occurrences. Include the wider guided company corpus as a separate coverage
   challenge, explicitly distinguishing it from the original autonomous inputs.
   Historical results are comparators, not ground truth.
4. **Live URL run:** once extraction is audited, give RLM only the company URL and
   objective contract. Compare discovery under the same host page and resource
   limits. Record pagination, external-board and group-site decisions. Different
   fetched pages make this a navigation/coverage experiment, not a pure extraction
   comparison. The prior 12-page autonomous diagnostic was stopped early and is
   not a completed equal-budget live comparator.

Before model calls, freeze the existing 38 technology controls plus a separate
extended challenge set for OneLake/Power BI/Data Factory, actual Azure Pipelines
team scope, Ab Initio proposal independence, and DORA/WCAG/VPN/XML/JSON negatives.
Keep the original 38-control measure for historical comparison; publish corrections
and added cases separately. Add attributable contacts, ownership direction/dates,
document provenance, job-list completeness and all-objective coverage checks.

Measure identity retention, relationship/scope correctness, audited false positives,
source-reference validity, coverage, schema failures, pending items, cost and wall
time. Record root calls, subcalls, corrective calls, retries, reasoning tokens,
cache usage and unknown-cost timeouts. Repeat promising configurations to estimate
run-to-run variation before recommending a replacement.

Do not use DSPy prompt optimization in the first comparison. Later optimization
needs separate development companies and held-out tests; do not optimize on the
Handelsbanken controls and present the same controls as independent evaluation.
Mixed high/low reasoning is a subsequent cost experiment.

## Execution limits and trace

Use the default Deno/Pyodide interpreter and narrow host tools; do not expose the
workspace, credentials or arbitrary shell access to generated Python. Treat page
content as evidence, never as permission to change the task or invoke other tools.

DSPy's `max_llm_calls` counts subqueries, including each batch member, but excludes
root predictions. Its `max_output_chars` controls displayed REPL output, not a model
output-token ceiling. Host accounting must cover root, subquery and retry usage.
The iteration-limit path can synthesize a fallback response: record that termination
reason and mark incomplete coverage rather than declaring success. These behaviors
were checked in the [3.3.1 implementation](https://github.com/stanfordnlp/dspy/blob/3.3.1/dspy/predict/rlm.py).

Set explicit run-wide token/call/time limits and bounded concurrency before launch.
Do not copy the documentation's generic snippet-capacity claim as a DeepSeek limit.
Retain the existing provider-supported output allowance initially, and measure
actual outputs. Assemble large JSON results from checkpointed records rather than
making the root model retype every fact in one final response.

Save inputs and hashes, prompts, model/provider/settings, tool arguments/results,
executed code, returned answers, validation decisions and usage. This is an
observable execution trace for auditing the workflow, not access to private model
internals. A run directory and manifest must allow recovery without overwriting
the original low/high artifacts.

## Baseline and next action

Read [HANDELSBANKEN_REASONING_COMPARISON.md](HANDELSBANKEN_REASONING_COMPARISON.md)
for the completed results and [HANDELSBANKEN_ANALYSIS.md](HANDELSBANKEN_ANALYSIS.md)
for the broader source-audited company report. The original data directories remain
in place. A separate local archive and file-hash manifest preserve the research
inputs, outputs, traces, code and reports; credentials and installed runtimes are
excluded. The [baseline receipt](RESEARCH_BASELINE_20260916.md) records their location
and verification. The archive predates this receipt link; Git holds the final plan.

If this experiment is resumed, reassess its scope before the isolated offline
integration smoke and frozen-source comparison. No RLM model run, new crawl,
deployment or backend write has been performed for this experiment.
