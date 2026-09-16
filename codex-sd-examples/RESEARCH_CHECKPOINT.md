# Company extraction checkpoint — 16 September 2026

## Resume here

**Current scope: website crawler only.** Work on URL/page discovery, Crawl4AI,
independent page extraction and link scoring, final technology classification,
and complete JSON submission to RustFS/S3. DNS detection and integration with
existing infrastructure detection pipelines are outside this task. The broader
database plan has been moved to historical material; do not bring those branches
back into the crawler workflow. ClickHouse parsing/storage remains deferred.

**Central-system interaction is limited to technology catalog lookup.** Determine
whether a discovered technology is already defined and retain its canonical
identity when matched. Use the existing catalog search/local synced snapshot;
keep source names and lookup results in the submitted JSON. A confirmed no-match
can be marked not found; ambiguous/failed lookups remain unresolved, not new.
Catalog membership does not establish company use. No central observation writes,
catalog creation, proposal submission or other system integration is part of the
current crawler task.

**Latest correction: submit ALL research results as JSON to RustFS/S3.**
Read [the submission contract](company_research/TECHNOLOGY_MENTION_STORAGE.md).
The crawler's durable output is a complete, versioned result.json containing
all objective results, raw mentions, original source/context text, final
classifications, links, provenance, coverage and errors. Include known, unknown,
ambiguous and excluded mentions. Preserve partial outcomes explicitly.
ClickHouse parsing, tables, importer and proposal routing are deferred until the
user resumes that work. Do not implement the previously suggested four-table
schema or a database ingestion endpoint now. The existing proposal-only endpoint
is separate and does not block complete S3 submissions. Use existing RustFS
configuration; no uploader or upload has been implemented by this design update.

**Latest user direction: defer technology relationships to a final agent.**
Collect source-name mentions with original text sections, headings, source URLs
and actor/job context per page. After crawling, classify eligibility and all
supported relationships (use, requirements, expertise, plans, product/client
context, vendor partnerships), then resolve catalog identities. Keep raw mentions
immutable; unknown use is not false. This changes the earlier plan to tighten
technology filtering in the page request: retain candidate context there and
apply catalog eligibility in the final pass. Read
[the detailed contract and prompt examples](company_research/TECHNOLOGY_MENTION_DESIGN.md).
Design saved; no runtime/schema change or new model test yet. Reuse the older
statement pipeline's concepts without its full review chain or forced role scope.
Next test must collect mentions from saved pages afresh, not only reclassify the
previously accepted signals, which would hide collection misses.

### Last completed evaluation

**Revised one-pass evaluation completed; integration gate not passed.** Read
[the full results](page_agent_lab/ONE_PASS_V2_RESULTS.md). Direct DeepSeek Flash/
high, 13 frozen pages and one separate retry: 14 calls, estimated $0.26395.
The seven-page regression retained 40/40 source-reviewed facts (frozen exact
score 39/40 because Stadshypotek's short name is valid), all 30 listing jobs,
278/278 links and 19/19 negative checks. Evidence passed for 118/238 records.

Six additional Memgraph/Oxide/RT-RK pages initially returned 21/32 facts. One
RT-RK response had an extra closing brace; its independent unchanged-prompt
retry recovered all seven jobs and 11 checks. With that retry: 32/32 facts,
19/21 negatives, 507/507 links and 74/159 source-matched records. Preserve the
initial parse failure. The auditor now marks negatives on failed pages unavailable.

Remaining work: bounded response/evidence recovery, stable evidence references
with native-versus-rendered provenance, specific-technology policy aligned with
the schema, client/product scope, application email versus job-page URLs, and
positive controls for planned-adoption/expertise recall. Many records omit actor
quotes; two valid rendered link labels failed native-only evidence validation.
Protocol/storage standards and broad suites still leak into technology signals.
Main runtime/queue unchanged; do not claim a new autonomous crawl. Integrate only
after these page-level gaps are addressed. No prompt changes were made after the
new tests began. Seven lab tests, Ruff/type/format checks and all 14 actual request
payload checks passed; all 146 original pilot files remain unchanged.

Artifacts: page_agent_lab/data/one-pass-v2-20260916,
one-pass-holdout-20260916, one-pass-holdout-retry-20260916,
one-pass-review-20260916. The runner now defaults to one-pass; `--mode compare`
explicitly runs both arms; `--dataset page_agent_lab/holdout.json` selects the
additional source snapshots/controls. See the
[archive receipt](page_agent_lab/ONE_PASS_V2_RECEIPT.json).

### Prior stage: preparation and phone validation

**One-pass baseline accepted; phone validation fixed in 0.15.3.**
The user agreed to keep one page as the independent unit and use one combined
request as the integration baseline. Targeted specialists remain a later test.
Read [the follow-up](page_agent_lab/FOLLOW_UP.md). The main crawler's page-agent
integration, queue rewrite and final merge are not yet implemented.

Numeric phone matching now applies only to the value, not the owner's name.
Positive/negative regression tests reproduce both false rejection of valid names
and false acceptance of owner names containing digits. Replaying the original
outputs removed 19 erroneous holds: six one-pass and thirteen routed records.
All record data, quotations and IDs remained identical; all 146 original run
files passed unchanged-hash checks. Source matching is not semantic approval.
146 package tests plus six lab tests passed; four browser tests skipped.
Ruff/type/format checks and 0.15.3 source/wheel builds passed.

The lab prompt now specifies objective boundaries, explicit claim dates, actor/job
quotes, planned adoption versus deployment, services versus product sales,
navigation versus documents and source hints versus corporate relationships.
Controls 0.2 correct two known name-matching expectations and add negative cases
(40 positives, 19 negatives). Against the original outputs, the new negative
checks pass 15/19 for one-pass and 4/19 for routed; this is a post-hoc baseline.
The revised prompt has not had a model run. Next evaluate it with a bounded
one-pass test before crawler integration. Email invisible-character matching and
missing actor quotations remain separate work. No new paid calls or site fetches.
Artifacts: page_agent_lab/data/phone-validation-20260916/.

**Completed standalone page-agent comparison, using runtime 0.15.2.**
Read [page-agent results](page_agent_lab/RESULTS.md) and [runner instructions](page_agent_lab/README.md).
Seven saved pages, direct DeepSeek Flash/high: one pass used 7 calls, 3.18 minutes,
estimated $0.10676; router plus specialists used 67 calls, 8.96 minutes, $0.48565.
Both matched 38/40 frozen exact-name controls and retained all 40 checked facts
after two explicitly documented source-name matching corrections. Both retained
30/30 listing titles/URLs and assessed all 278 observed navigation occurrences.
No API errors, truncations or automatic correction calls. Six boundary tests,
Ruff, type checks, 74 request-payload checks and credential scan passed.

Routing dispatched all 14 selected positive page/objective pairs but selected
60/70 possible workers; 18 returned empty. It added useful product names and
more complete quotations, alongside false technologies (iframe host/element name),
geographical markets as office addresses, person records under company profiles,
unsupported supplier claims, capture dates as claim dates and AURIX service/product
confusion. Both variants still need better technology signals and document taxonomy.
Positive-control retention and source matching are not overall precision.

Recommendation: keep the page boundary; use one-pass as the next integration
baseline and test specialists selectively after correcting the newly recorded
failures. The hybrid and autonomous coordinator are not implemented. The existing
phone validator incorrectly digit-matches the owner name as well as the number;
fix it separately and preserve actor/field evidence. Raw review items remain saved.
Artifacts: `page_agent_lab/data/two-pass-20260916/`, including frozen code/inputs,
calls, comparison.json, source-review.json and verification.json. Do not overwrite
the completed run. No new site fetches, catalog/backend submissions or deployment.
The [run receipt](page_agent_lab/RUN_RECEIPT.json) points to a verified 146-file
archive under `/Users/graovic/pulsarpoint/company-research-snapshots/`.

**Design direction: one page agent returns page data and scored visit targets.**
Read [PAGE_AGENT_DESIGN.md](company_research/PAGE_AGENT_DESIGN.md). The coordinator
persists page results, updates the existing queue and dispatches pending targets
by priority. Each agent's factual context is its one page; company merge and catalog
resolution follow. The standalone unit and comparison are implemented; the main
crawler is unchanged. Integrate scheduling only after the remaining validation
and objective-boundary gaps are addressed, then run an uninterrupted comparison.
**Tested variant:** [two-pass page analysis](company_research/PAGE_AGENT_TWO_PASS.md):
multi-label routing over all objectives, then selected specialists with their own
prompts/examples and the original page as evidence. Compare against one-pass
extraction under the same outer contract. Measure routing misses separately from
extractor errors, and count all worker calls. See the completed pilot above.

**DSPy RLM postponed by the user.** No implementation or model run has started.
Keep [DSPY_RLM_PLAN.md](company_research/DSPY_RLM_PLAN.md) as a saved proposal;
do not begin it unless the user resumes it. Current focus is the existing Crawl4AI
and direct DeepSeek workflow. The Handelsbanken results support a supervised pilot,
but do not establish reliable, complete research from a URL without intervention.
The high-reasoning comparison reused saved sources, not a new autonomous crawl.
Clarification from the original queue: the vacancies page had been discovered and
rated highly. The analyst stopped the run after about 41 minutes, with 12/30 pages
and 112/320 calls used. This does not establish failure to find jobs within budget.

Next priorities: improve early coverage of jobs/company/contact/ownership hubs,
confirmed company domains, pagination and embedded archives; fix team/role scope,
retention of source facts with pending catalog metadata, technology taxonomy and
contact attribution; then run a fresh autonomous regression against the saved
source-audited expectations. Measure elapsed time and calls as well as quality.
These are next steps, not fixes already implemented. High reasoning remains a
promising explicit setting for technology work; global defaults are unchanged.
The [baseline receipt](company_research/RESEARCH_BASELINE_20260916.md) records the
Git branches, local archive and verified manifest of the saved research.

**Handelsbanken source-audited analysis and high/low comparison completed. Runtime 0.15.2.**
Read [HANDELSBANKEN_ANALYSIS.md](company_research/HANDELSBANKEN_ANALYSIS.md) and
`company_research/data/handelsbanken-20260916-review/company-analysis.json`.
The original low-reasoning analysis uses direct `deepseek-flash` (DeepSeek V4.1 Flash),
not OpenRouter. The public Python API accepts `api="deepseek"`; the CLI still uses
OpenRouter. No backend submission or deployment.

The autonomous diagnostic was explicitly stopped after 12 fetched pages / 112 model
calls, before it reached the jobs list and group ownership. It is marked partial with
interruption metadata and an unchanged pre-stop snapshot. Do not report it as a
completed 30-page crawl. Separate guided sources supply 32 Swedish vacancy entries
(16 IT, two open applications), all 32 captured ads, eight executives, five explicitly
wholly-owned subsidiaries, 15 shareholders with voting percentages dated 2025-12-31,
and 58 document-link occurrences. Report contents remain unparsed. The sitemap has
1,018 URLs, of which the crawler admitted 1,000. Pagination and iframe follow-ups
were auditor guided and are preserved separately.

Original IT workflows plus targeted scope recovery retained 34/38 source-read identity
controls (32 strict signal/scope checks). Pipeline acceptance: 103 observations / 64
raw labels. Additional manual holds remove five observations; explicit aliases group
the remaining 98 observations into 58 identities. Six source-audit supplements are
separate from automated acceptance. Eight descriptions / 15 statement IDs remain
unresolved in the first 14-ad run. These counts are not overall precision/accuracy.
All 507 external occurrences were assessed (100 URLs, 27 domains); 457 hints / 50
explicit-text claims. Jobylon map providers must not become the bank's technology
stack. Original usage totals 280 calls, 278 with usage, estimated $1.151 off peak;
two interrupted requests have unknown cost. See `model-usage.json`.

Fixed numeric tel: URL misclassification in 0.15.1. Fixed duplicate observations
created by job-scope conversion in 0.15.2, retaining genuine duplicate-output rejection.
The saved Microsoft 365/Exchange replay recovered four observations. 144 regular tests
and all four browser integration tests passed; source Ruff and type checks passed.

**Completed reasoning experiment:** read
[HANDELSBANKEN_REASONING_COMPARISON.md](company_research/HANDELSBANKEN_REASONING_COMPARISON.md)
and `company_research/data/handelsbanken-20260916-reasoning/comparison.json`.
Fresh paired low/high runs over the same 16 IT snapshots used 0.15.2, identical
prompts, stable page ordering, local catalog and budgets. Identity controls: **31/38
low, 36/38 high**; strict signal/scope: 29/38 and 33/38. Accepted observations: 107/113.
Unaccepted descriptions: 20/9; pending statement IDs: 13/8 (not additive categories).
Low: 136 calls, 33.8 minutes, estimated $0.500. High: 141 calls, 54.5 minutes,
estimated $0.720 plus one unknown-cost timeout; its 300-second source-review timeout
recovered on application retry. No output-token-limit stops. All runs finished.

High gained SAS, separate Java/Jakarta EE in j0029, Maven, DB2 and Microsoft SQL
Server controls, but lost Ab Initio (proposal metadata rejection). Azure Pipelines
still fails because normalization forces role scope and source review sees team
scope. High incorrectly accepts DORA as technology despite describing it as a
regulation, plus generic VPN. Both still need taxonomy/granularity checks. Low also
retains valid OneLake/Power BI/Data Factory names missed by high, outside the controls.
Do not call this a precision score or automatically approve the proposals.

The 44 frozen classification/extraction/selection requests and 507 external-link
occurrences were also replayed at high. Candidate validity: 689/750 low vs 659/750
high. External label changes: 77; repeated-context inconsistency 10/39 low vs 18/39
high. Six speculative customer hints became other_business. No new autonomous
crawl was run. Pairing/input/prompt checks passed. New requests total 335; returned
usage implies about $1.719 plus one unknown timeout. Historical low replay baselines
were reused, not rebilled as new calls. See `new-run-usage.json` and `verification.json`.

Recommendation: high is promising for technology work; retain low for routine link
assessment. Fix role/team attribution, separate retained source identity from proposal
metadata readiness, and explicitly exclude regulations/standards/generic categories
from the specific-product technology catalogue. Defaults were not globally changed.
The benchmark harness and audit are in `company_research/benchmarks/compare_reasoning.py`
and `audit_reasoning.py`; executed code and all artifacts are frozen in the run folder.
Do not rerun or overwrite this completed experiment unless asked.

## Previous external-link implementation

**External-link context implemented — 0.15.0, research schema 1.10.** Read
[EXTERNAL_LINKS.md](company_research/EXTERNAL_LINKS.md). Every successfully fetched
page now produces a separate external-link inventory before model work and crawl
exclusions. Full URLs retain query strings/fragments; observations preserve source
page, fetch time, HTML hash/file, anchor/logo text, DOM region, heading and nearby text.
Repeated destinations in separate contexts remain separate occurrences. New fetches
inspect saved rendered HTML before cleanup; company extraction still receives unchanged
native cleaned HTML. Metadata-only fallbacks expose missing context explicitly.

The crawl selector receives up to five observed contexts per candidate. The entire
inventory remains uncapped by queue limits, including social, document and account
destinations that are never crawled. External means a different registrable domain
from the fetched source page; private suffixes distinguish hosted tenants. Subdomains
of one registered domain are internal. `Page.external_link_count=0` means observed none;
null means no inventory was produced.

`ResearchResult.external_links` and `external-links.json` expose observations with
separate optional LLM relationship assessments. The final assessment stage runs after
core extraction/summary using remaining model budget, default three batches of 20.
Unprocessed links remain `not_assessed`. Missing/duplicate/invalid replies are retained
as failures; unsupported evidence/entity names need review. Assessments distinguish
source text from hints and always say `independently_verified=false`. They do not
create `company_relationships`, summary facts or backend mappings. A header alone
does not prove a business affiliation; a partner is not automatically a subsidiary.

Saved replay: `company_research/data/external-links-v015/`, six prior cleaned-HTML
pages, no recrawl. 49 occurrences (Memgraph 13, Oxide 24, RT-RK 12). Direct DeepSeek
smoke: three calls, all 49 ID/schema/evidence checks passed, including 14 unknown and
30 contextual-hint assessments. This is not an accuracy score; generic footer social
hints remain uncertain. Existing cleaned inputs cannot restore removed header structure;
real browser fixtures verify the rendered-HTML capture. No backend or deployment work.
Validation: 143 regular tests plus three real-browser tests pass; nine new link tests.
Ruff, type checks, whitespace checks and 0.15.0 wheel/source builds pass. The new replay
artifacts contain no API key values. Full verification is saved alongside the replay.

**Next:** use these observations as reviewable leads for related-company/domain
resolution, preserving source-operator direction. Do not infer ownership from placement
or promote the inference inventory into company facts. The earlier technology identity,
completeness and catalog-recovery gaps below remain outstanding.

## Previous additional website comparison

**Three additional websites completed — unchanged runtime 0.14.4.** Read
[DEEPSEEK_MORE_WEBSITES.md](company_research/DEEPSEEK_MORE_WEBSITES.md). Memgraph,
Oxide and RT-RK, two selected pages each, fetched once as native Crawl4AI cleaned HTML
and paired across direct `deepseek-flash` and OpenRouter
`deepseek/deepseek-v4-flash-0731`/Wafer. Same low reasoning, JSON object/schema prompt,
65,536 output tokens, 120-second deadline and frozen catalog. This is selected-page
statement extraction and normalization, not autonomous navigation or all-objective export.

All six runs finished. Direct: 79 descriptions, 74 reviewed, 39 source-supported
technology observations, 38 automatically accepted, **31 after seven manual specificity
holds**. OpenRouter: 59 descriptions, 45 reviewed, 24 source-supported observations,
six accepted. These are observations including neutral mentions and proposals, not
unique technologies or confirmed internal usage. Frozen targeted controls: direct
9/15 versus OpenRouter 4/15 final (6/15 before catalog). Not exhaustive recall.

Direct correctly preserves client/project attribution and role use, but misses NASA
Python/R, C preference, OPTE development and SGC200 as separate relationships. OpenRouter
retains both Rust and C preferences with the OR alternative group, better than direct
for that case. Valid observations are also lost at catalog resolution. RT-RK/OpenRouter
extracts C++ and Linux but review expands their identities to longer phrases and the
structured name check rejects them. Three direct Oxide company descriptions have null
actors where review names Oxide. Fix bounded reconciliation; do not remove actor checks.

Manual holds: direct Oxide NVMe/DDR5/IPsec/BGP/VMware, direct RT-RK Bluetooth and unnamed
E+H protocol; preserve useful capability/skill/service context outside the narrow named
product catalog. **ACID company-compliance record is also held** as a database property.
The frozen negative Kubernetes control overflags an actual product feature; manual audit
qualifies it rather than changing controls. Product incorporation/support needs clearer
relationships. Unnamed protocol identity also slips through OpenRouter source review
but remains catalog-pending.

Direct: 60 HTTP calls, no timeouts or malformed/empty response errors; five complete
pages and one partial evidence/name case. OpenRouter: 61 calls, five deadline errors,
one invalid JSON and two empty answers; two complete/four partial pages. Direct cost
**$0.220552 estimated at actual off-peak rates**, OpenRouter **$0.106032 reported** plus
five unknown-cost deadline calls. Observed total about $0.326584. Direct completed more
downstream work; this is not equal-output cost or isolated model-version comparison.

Artifacts: `company_research/data/deepseek-more-websites-20260911/`, including frozen
sources/code/catalog/controls, raw calls, comparison, manual review and derived reviewed
records. The folder uses UTC run date; local completion is 12 September. New reusable
fetch/compare and offline-audit harnesses are in `company_research/benchmarks/`.
Ruff, type checking and whitespace checks pass. Offline audit verifies unchanged
sources/controls/catalog and matching initial prompts; no credential values appear in
the 315 scanned artifact/code/report files. Validation is saved in `verification.json`.

**Next:** replay saved identity/actor failures, complete individual relationships from
reviewed descriptions, tighten named-product/credential scope, then recover valid pending
catalog items. Test direct non-thinking versus low reasoning after those fixes. No runtime
defaults changed, backend submissions, admin approvals, deployment or PDF work.

## Previous direct API comparison

**Direct DeepSeek comparison completed — 0.14.4.** Read
[DEEPSEEK_DIRECT_RESULTS.md](company_research/DEEPSEEK_DIRECT_RESULTS.md).
The `DEEPSEEK` credential in `jobs_extraction_lab/.env` is used with
`https://api.deepseek.com/chat/completions`, model `deepseek-flash` (documented V4.1
Flash). `ModelClient` supports both API dialects; normal crawler/CLI defaults remain
OpenRouter. Benchmarks use common JSON object mode with the schema in the prompt,
low reasoning, 65,536 output tokens, 120-second deadlines and unchanged native HTML.

The paired 31-record replay produced 13/16 accepted DMC descriptions on direct versus
0/16 on OpenRouter; 11 DMC observations remain after holding broad AVEVA, including
one Zephyr mention that must not become usage. thoughtbot: 12 versus seven accepted
observations; both retained 9/12 old controls before catalog review, and direct still
misses three old relationships. Both recovered four Plausible records and both MCAP
relationships. No wrong actors or unsupported sale/negative signals survived.

An additional fresh extraction from one saved NOVELIC job HTML produced 19 statements,
17 reviewed descriptions and 12 accepted technology observations on direct. It retained
12/18 source-read controls. Five AWS services (S3/IAM/Lambda/Glue/Athena) remained grouped
inside AWS; Protobuf was held by overly restrictive proposal metadata review. A generic
job-summary description was also incorrectly held over nullable source-name handling.
Cookie vendors, generic hardware categories and vague candidate certification requests
were not promoted to accepted specific technologies/company credentials. OpenRouter
failed both extraction attempts at the 120-second deadline: pending extraction, not
negative findings. This was the optional statement path, not the regular jobs export.

Artifacts: `company_research/data/deepseek-direct-20260910/`,
`deepseek-direct-20260910-job/`, and `deepseek-direct-20260910-smoke/`. Preserve all
frozen source/HTML, requests, raw responses, controls, summaries and manual audits.
All runs finished. 110 HTTP attempts total; direct estimated observed-usage cost
$0.34102 at peak rates, OpenRouter reported $0.034735, plus unknown usage/cost for six
deadline calls per endpoint. Direct does not return billed cost; a zero legacy
`known_cost_usd` sum is not free usage. 134 regular tests pass, three browser tests
skipped; Ruff, source/new-test/new-benchmark type checks and 0.14.4 builds pass.

**Next:** compare direct non-thinking versus low reasoning on these saved inputs; fix
individual identities in parenthetical lists, distinguish alias/metadata verification
from evidence of company use, and handle generic job/company description attribution.
Do not silently modify the completed comparison or assume general superiority from
one run. No backend submissions, admin approvals, deployment, recrawl or PDF work.

## Previous attribution/evidence replay

**Saved attribution/evidence fixes implemented and tested — 0.14.3**, optional schema
**page-statements/1.3**, regular schema 1.9. Read
[ATTRIBUTION_REPLAY_RESULTS.md](company_research/ATTRIBUTION_REPLAY_RESULTS.md) and
[the final audit](company_research/data/attribution-audit-v0143/summary.json).

The package checks reconstructed actors separately from prose, allows bounded
source-supported actor corrections, repairs their quotations without changing facts,
and rechecks corrected descriptions. Development services no longer imply selling the
technology. Failed extraction pages are explicit even with zero statement IDs. Strict
review schemas require nullable fields too; duplicate JSON keys are rejected. Original
HTML is unchanged; quotation matching tolerates typographic quote punctuation.

31 selected saved inputs; no recrawl. Final DMC audit: nine correctly attributed
expertise observations, seven retained and two manual identity holds (AVEVA, Beckhoff
Motion Control); seven descriptions remain pending. thoughtbot: no incorrect `offers`,
14 source-reviewed observations, ten accepted and four catalog/proposal-pending.
All four Plausible company/person evidence cases recovered. MCAP preserved role use
but omitted preferred experience; the prior successful two-relationship result did
not repeat. thoughtbot retained 11/12 earlier valid relationships before catalog checks.
Do not claim exhaustive recall or production readiness.

Artifacts: full four-case replay `attribution-replay-v0141`, DMC recovery
`attribution-recovery-v0142`, focused five-description evidence recovery
`attribution-recovery-v0143`, and derived audited JSON in `attribution-audit-v0143`,
all under `company_research/data/`. The interrupted `attribution-replay-v014` pilot is
preserved separately. Total this turn: **62 calls, $0.09172215 reported, 13 unknown-cost
calls**. All runs ended; no deployment, DB submission, or PDF work. The final focused
run matches its 0.14.3 source; older phases retain their own frozen implementations.
130 regular + three browser tests, Ruff, source/new-harness type checks, whitespace
checks and package builds pass.

**Next:** a bounded relationship-completeness check on reviewed descriptions (MCAP
preference and Rails history), pending-only recovery, and stricter vendor/product
identity review. Continue exposing processing failures separately from absent facts.
Do not rerun the five-site crawl. Fresh job/employer, ownership and document-discovery
coverage remains untested.

## Previous fresh-company benchmark

**Fresh five-company benchmark complete — 0.13.0**, optional statement schema
**page-statements/1.2**; regular research schema remains 1.9. Read
[FRESH_COMPANY_BENCHMARK.md](company_research/FRESH_COMPANY_BENCHMARK.md) and the
[audited output index](company_research/data/fresh-company-statements-v1/README.md).
The optional workflow is **not ready for default use**.

Multiple relationships are implemented. The isolated saved MCAP regression returned
stated_use/role AND preferred_experience/role and both passed source review: two calls,
$0.00176125, no unknown costs. It did not rerun catalog metadata or change the historical
NOVELIC benchmark score. Artifacts: `company_research/data/mcap-multiple-relationships-v013/`.

The fresh frozen run used DMC, Plausible, thoughtbot, OnLogic and IC Resources, starting
from homepages only, two sites at a time. Same native Crawl4AI HTML, frozen catalog,
DeepSeek `deepseek/deepseek-v4-flash-0731`, Wafer, low reasoning, 65,536 output tokens,
120-second deadline. Ten pages / 100 direct-crawl calls / 120 statement calls per site.
All five stopped after repeated request errors before reaching their page budgets.
Only 11 pages were fetched (1, 4, 2, 3, 1 respectively); no job details were fetched.
This does not validate recruiter-versus-client job attribution, ownership or financial
report coverage. The controller still chooses pages using direct findings.

Fresh statement output: 136 descriptions, 98 accepted description reviews; 31 technology
observations accepted automatically. Manual inspection held **all 14 DMC observations**
for wrong company attribution (tool/vendor names used as the company), and **five of 17
thoughtbot observations** for confusing development services with offering the technology
itself. **Twelve thoughtbot observations remain**, across eight source-named technologies.
Plausible and OnLogic statement extraction failed entirely. IC Resources generic sectors
were excluded. Original direct results still contain three Plausible technology mentions
and an OnLogic ISO 9001:2015 claim that the experimental statement paths failed to recover.

Use per-site `audited-company.json`, not the unqualified original `accepted-company.json`.
The offline auditor corrects processing coverage, applies explicit manual holds and
recomputes technology summaries. It preserves all frozen outputs. Original pending-ID
lists can be empty despite whole-page extraction failure; audited coverage exposes that.
`manual-controls.json` contains 26 source-derived checks, never provided to the LLM.
`manual-findings.json`, `automatic-audit.json` and per-site `manual-holds.json` document
errors and remaining limits. Zero provenance-integrity errors is not zero semantic errors.

Runtime 85 minutes, **162 calls, $0.21019995 reported**, plus **53 unknown-cost calls**
(51 deadlines and two HTTP 429 responses). Costs include two extraction paths. The
separate MCAP regression and historical NOVELIC experiments are not included in this
five-site cost. Source implementation and prompts were not tuned during the run.
Suite SHA256: `6115f1abbb366466b6ffc1718de8355f6380f64a9124cb8583daf06f312e1254`.
All 11 HTML snapshots/copies and 21 frozen source files were verified; source matches
final 0.13.0 implementation. 123 regular tests + three browser tests, Ruff, source/focused
type checks and wheel/source builds pass. Historical broad benchmark/test type diagnostics
remain outside the source-package check. No active model run, deployment, database writes,
submissions or PDF work remains. Do not silently replay or promote this benchmark.

**Next recommendation:** first fix/check structured actors separately from prose; then
service-versus-technology relationships and evidence anchoring/request complexity. Use
DMC and thoughtbot saved failures plus MCAP as regressions. Expose failed pages directly
in the package result and prevent long-page failures from blocking useful navigation.
Only then run a focused fresh job-detail and ownership/document-discovery test. Do not
repeat this entire five-site crawl without a new instruction.

## Previous completed description-review checkpoint

**Page description review and individual recovery completed:** package **0.12.3**,
regular research schema **1.9**, optional output schema **page-statements/1.1**.
Read [PAGE_STATEMENTS_REVIEW_RESULTS.md](company_research/PAGE_STATEMENTS_REVIEW_RESULTS.md).

Descriptions are checked against each page's native cleaned HTML; minimal corrections
need another source check. Names, company/holder, job and provenance are supplied by
code during normalization. Required per-item decisions isolate incomplete responses.
Wrong signal/scope interpretations can receive one source-reviewed correction, with
job scope preserved. Catalog resolution also uses required per-item keys and retains
good decisions. Final acceptance validates generic names and credential document
metadata when loading saved findings as well as fresh model output.

Final artifacts: `company_research/data/novelic-page-statements-v2-validated/`.
Start with `accepted-observations.json`; full records and accepted IDs are in
`statement-result.json`. Read `comparison.json`, `manual-audit.json` and `manifest.json`
for controls, remaining holds, cost and implementation provenance. Final result SHA256:
`a28d5c7f13e27818519d365d0c9ce20ec3ee64e24585cae82174a1a6c9b473cf`.

Six unchanged saved NOVELIC pages, zero new page fetches or source extractions.
113 descriptions; **104 accepted source reviews**, including **three corrected and
rechecked descriptions**; nine original evidence failures remain pending. Final
validated output has **53 accepted technology observations**, **31/32 technology
controls** (previously 20/32), and **3/3 company credential controls** (previously 0/3).
Company credentials preserve ISO 9001:2015 and ISO 14001:2015 certification claims and
IATF 16949:2016 **working_toward**. Two product-compliance records remain held due to
document-type metadata without a document URL; their issuer/assessor also needs support.

All control technology names are present. The remaining MCAP control expects preferred
experience; the accepted record preserves stated use within the role. Its reviewed
description contains both meanings, but normalization currently chooses one relationship.
AutoPLANT's draft no longer names the wrong vendor. Norasoft is retained with application
context, but internal catalog commentary in its definition still needs administrator
cleanup. Accepted descriptions/metadata are LLM-reviewed source claims, not independently
verified deployment or certification facts.

Live phases are saved as `novelic-page-statements-v2` (0.12.0, deliberately stopped),
`novelic-page-statements-v2-resumed` (0.12.1), and `novelic-page-statements-v2-final`
(0.12.2). Final 0.12.3 validation made no additional calls. Across these current phases:
**73 calls, $0.09917655 reported, nine unknown-cost calls** (eight deadlines and one
intentional interruption), below the 80-call cap. DeepSeek
`deepseek/deepseek-v4-flash-0731`, Wafer, low reasoning, 65,536 output tokens,
120-second deadline. Historical 0.11.1 costs below are separate. This is an incremental
recovery experiment, not a fresh production crawl or controlled model comparison.

Verification covers **123 Python tests including three browser tests** (120 regular
tests in the final run; browser tests passed in the preceding full run), Ruff,
source/focused type checks, 0.12.3 wheel/source builds, diff checking and six saved-source
hashes. Broad `ty check company_research` reports 19 diagnostics in benchmark scripts
and tests; do not describe that broad check as clean.
The optional workflow is not enabled in default extraction;
shared catalog handling and saved-finding acceptance guards did change. No active
model run, database writes, submissions, deployment or PDF/OCR work remains.

**Next recommendation:** preserve multiple supported relationships from one statement
(MCAP), repair the nine pending evidence anchors, then test fresh descriptions on other
company websites before default integration. Do not start another experiment without
the user's next instruction.

## Previous page statement checkpoint (retained for provenance)

**Page statement prototype completed, optional:** package **0.11.1 / research schema
1.9** adds page-local company/technology/certification descriptions, followed by
normalization, raw-source meaning checks and catalog/proposal processing. The default
crawler extraction is unchanged. The dedicated experimental JSON uses schema
`page-statements/1.0`. Added `develops` / `offers` relationships to Python and the
backoffice boundary. Read
[PAGE_STATEMENTS_EXPERIMENT.md](company_research/PAGE_STATEMENTS_EXPERIMENT.md).

Prompts: `company_research/src/company_research/statement_prompts.py`.
Implementation: `company_research/src/company_research/statements.py`.
Artifacts: `company_research/data/novelic-page-statements-v1/` and
`company_research/data/novelic-page-statements-v1-recovered/`.
The recovery result SHA256 is
`01a1dd12ad2f232500ec4f60581aa18fa5874354b495bdeff8edb47486805259`.
`manual-audit.json` records findings and final source verification; original run
snapshots and `final-verified-implementation/` distinguish tested live code from
subsequent prompt/guard fixes.

Six unchanged saved NOVELIC pages, zero new fetches. First attempt: 221 statement
versions, ten mechanically source-matched, no accepted normalization. Recovery reused
113 descriptions unchanged and repaired only quotations/anchors: 104 now have matching
quotations. Descriptions shrink native HTML from 308,738 to 43,590 characters without
provenance (159,130 including stored sources and repair history).
Final: 58 normalized technology records, 25 automatically accepted, **20/32 technology
controls** (18/19 expertise, 2/13 jobs), **0/3 credential controls**, 46 primary statements
pending. The credentials were present in page descriptions, then held with failed
normalization batches. Generic Cadence collection excluded; Norasoft normalization
pending; AutoPLANT catalog resolution pending. Do not call this an accuracy win.

Main failures: model assigning a different item to a statement ID, repeated credential
versions, invented alternative-group labels and incomplete catalog responses. One
unsupported "likely" application appears in a Python job description: quotation
presence is not independent verification of generated prose. Final output marks page
descriptions accordingly and keeps them out of source-meaning review inputs. Final
prompt now forbids inferred applications, explains version splitting and literal
alternative clauses. These latest prompt changes have not had fresh live extraction.

Across initial and recovery phases: **48 calls, $0.0898446 reported, two unknown-cost
deadlines**; 60-call cap not reached. DeepSeek `deepseek/deepseek-v4-flash-0731`, Wafer;
reasoning disabled for extraction/quotation repair, low for recovery normalization and
review; 65,536 output tokens and 120-second call deadline. This was exploratory recovery,
not a controlled model comparison. **110 Python tests (including three browser tests),
38 backoffice tests**, Ruff, ty, builds, diff and saved-source checks passed.
No active run, database writes, submissions, deployment or PDF/OCR work remains.

**Next recommendation:** independently validate page descriptions against raw sources;
isolate failed normalization/metadata items so one mistake does not hold a whole batch;
then retry these saved failures before fresh extraction or default integration. The
user approved page-by-page context and later normalization, not deployment. Do not
launch another experiment without the next instruction.

## Previous metadata checkpoint (retained for provenance)

**Technology metadata replay completed:** package **0.10.2 / schema 1.8** separates
source/meaning support from catalog metadata, adds `advertised_expertise`, and repairs
proposal descriptions/categories with bounded review. Metadata failure no longer
causes HTML re-extraction. Required review gates and metadata hashes are enforced by
the Python submission filter and backoffice boundary; administrator approval remains.
Source review no longer sees generated technology context or proposed definitions.
Read [the completed metadata replay report](company_research/NOVELIC_METADATA_REPLAY.md).

Final artifacts: `company_research/data/novelic-metadata-v18-final/`.
Frozen replay: 86 original observations from the previous completed NOVELIC run,
123 versions after linked corrections, zero new crawler fetches or HTML extractions.
All **19 known engineering overstatements** now pass as advertised expertise, and all
**28 accepted job controls** preserve their original signal/scope. Automated gates
recover **28/32** previously metadata-blocked observations. There are 75 automatically
accepted records, including a generic Cadence tool-collection false positive; do not
interpret these totals as verified accuracy. AutoPLANT's draft still incorrectly names
Autodesk as vendor. Manual holds and remaining metadata/source failures are saved in
`manual-audit.json`; the preview was not submitted. Simulink/Plan3D/E3D expertise drafts
remain metadata-blocked; Norasoft has a likely source-review false negative. Seven
quotation failures were intentionally left untouched.

Three preserved phases used 0.10.0, 0.10.1 and 0.10.2 against the same saved sources.
Total **28 calls, $0.0285846 reported**, plus one HTTP 429 and one 120-second deadline
with unknown cost. The 60-call cap was not reached. All model calls used DeepSeek
`deepseek/deepseek-v4-flash-0731`, Wafer, reasoning disabled. Runtime was about ten
minutes including manual diagnosis and code changes. This was not a new full crawl.
**96 Python tests** (93 regular + 3 real browser tests), **36 backoffice tests**, Ruff,
ty, wheel/source builds, `git diff --check`, and saved-source integrity checks passed.
A final duplicate-merge guard was tested after the replay; snapshots distinguish the
benchmark implementation from final verified source. No active run remains. No database
writes, deployment, administrator approvals or PDF/OCR experiments occurred.

**Next recommendation:** improve specific-product versus generic vendor-collection
classification and draft vendor/description checks, then share valid metadata decisions
across observations of the same proposed identity and exact draft. Use the remaining
frozen failures as regressions before testing other company websites. Do not redo the
NOVELIC crawl or launch new work without the user's next instruction.

## Previous controller checkpoint (retained for provenance)

**Controller follow-up completed:** package **0.9.2 / schema 1.7** binds known job URLs,
reserves three engineering detail attempts and retries stored extraction chunks without
refetching. Source/proposal reviews use short exact IDs. Final hardening prevents
incomplete pages from creating zero-yield penalties, exposes never-attempted chunks
as pending, and compares separately stored credential versions correctly.
**89 tests** (including three actual browser recovery tests), Ruff, ty, builds and
`git diff --check` pass. Integrity/controller audits have no issues. No submissions,
deployment or backoffice changes in this turn. PDF/OCR remains paused.

Read [the completed controller report](company_research/NOVELIC_CONTROLLER_RECHECK.md).
Final artifacts: `company_research/data/novelic-autonomous-v17-completed/`.
The 24-page budget yielded 23 successful fetches and one broken mechanical URL.
All 16 checked Careers listings remain; all five fetched descriptions yield accepted
jobs (previously three). The Data Engineer posting retains its actual URL and 11
accepted technology observations. All 16 checked Management names were extracted;
15 survive validation because Dušan Manić's evidence omitted NOVELIC.
47 technology observations / 35 grouped identities pass current pipeline checks,
including 28 job observations and 19 engineering observations. **These counts are not
verified accuracy:** the 19 engineering observations overstate advertised skills as
company stated_use. There are 32 proposal-review rejections, often category metadata
losses, and Antenna Design remains unvisited. Read `source-audit-issues.json` before
consuming the result or the unsubmitted technology preview.

Three chunks remain pending after exhausted saved retries: p0011/0, p0015/1, p0021/0.
Earlier credential rejections remain in the comparison JSON; the separate zero-call
replay `novelic-controller-v092-credentials/result.json` recovers ISO 9001:2015,
ISO 14001:2015 and IATF 16949:2016 working_toward through the fixed validator.
Nine document records were extracted; none passed all checks. Raw links remain saved.

The ordinary summary stopped on invalid service citations. The benchmark-only
`benchmarks/finish_saved_summary.py` removed two rejected statements from the saved
batch, then consolidated and reviewed it in two calls. The final overview retains
21 statements and excludes an unsupported partnership-to-group-membership claim.
This recovery is not yet an automatic package behavior. It fetched no new pages.
Total: 170 calls, $0.2014627934 reported plus 19 unknown-cost calls, 99m27s including
manual recovery. The separate initial Data Engineer replay adds $0.00706444.
Do not claim a controlled cost/speed comparison: the preserved run phases used
0.9.0 automatic routing, a 360-second recovery, 0.9.1 short review IDs, then 0.9.2
pinned to Wafer with a 120-second deadline. Global limits stayed 24 pages / 200 calls;
recovery allowances were four chunk attempts / eight saved retries. Originals remain
under `novelic-autonomous-v17`, `-recovered`, `-final`, and `-wafer`.
Code snapshots and all interruption/summary-recovery provenance are preserved.
There is no active run to resume.

**Historical recommendation (partly completed above):** repair source evidence, technology identity and proposal
metadata independently; add advertised-expertise semantics; repair category failures
without repeating HTML extraction; address unknown-relevance engineering candidates
and failed reserve attempts; make bounded summary statement exclusion automatic.
Validate on frozen records and several additional companies. Do not begin new work
without the user's next instruction.

**Scope/meaning test completed, 8 September:** package **0.8.1 / schema 1.6** adds
target-specific external scope, observed-job follow-ups, source review before catalog
resolution, separate proposal checks, mandatory duplicate-review propagation and
constrained summaries with short citation IDs and exact credential names.
The homepage-only 24-page NOVELIC run preserved 16 checked listings and 16 Management
names, fetched five job descriptions and recovered all five checked DSP internship
skills with preferred-experience/role attribution. It fetched zero unrelated ADI pages,
but missed the six Antenna Design tools because engineering pages lost in prioritization.
The frozen regression review passes 39/39 known cases; this is not unseen accuracy.
Final validation: **80 Python tests passed**, including three actual browser recovery
tests; Ruff, ty, wheel/source builds and `git diff --check` passed. Source/record
integrity audits returned no issues. Logs and code hashes are saved in the final
artifact's `verification/` and `verification.json`. Backoffice was not changed or
retested during this final scope/meaning pass.

Read [the completed report](company_research/NOVELIC_SCOPE_RECHECK.md) and
`company_research/data/novelic-autonomous-v16-validated/source-audit-issues.json`.
The final derived JSON and 23-statement overview are in that directory. It includes
10 accepted technology observations across nine identities, all from job descriptions.
CI/CD, a quality landing page misidentified as a certificate, and an overly broad
product-specific AUTOSAR observation are withheld. A summary typo from ISO 14001 to
ISO 14000 was excluded by code after the LLM reviewer approved it; the correct
structured credential remains. Do not infer accuracy from a model review alone.

The live collection used 0.8.0/Together. It stopped at 21 pages; recovery reached 24
with five extraction failures. Two long-ID summary attempts failed; three subsequent
same-model calls with automatic routing (Wafer) completed missing review and summary.
All original artifacts remain under `novelic-autonomous-v16-final`, `-recovered`, and
`-overview`. The final `-validated` output applies 0.8.1 checks to saved HTML without
another crawl. It is still partial: 141 calls, $0.16767575 reported plus 32 unknown-cost
calls, excluding earlier development/frozen runs. No submissions or deployment.

Next: reserve direct engineering/service coverage; bind known posting URLs in code
(one ad and 16 technology records were withheld after the LLM rewrote its URL);
reprocess saved pages after provider failures; add product-level technology scope;
consolidate service records and render exact identifiers from structured facts.
The first two-page 0.8 attempt in `novelic-autonomous-v16/` remains interrupted, not
part of the final 24-page collection. PDF/OCR remains paused.


**Browser recovery and entity consolidation, 8 September:** package 0.7.0 emits
schema 1.5 and adds bounded browser restarts, cumulative fetch-attempt artifacts,
and accepted person/job/technology entity summaries with raw-record references.
Technology submission now excludes unaccepted claims and evidence; the backoffice
enforces the same acceptance boundary. All 67 Python tests (including actual browser
closure/recovery and exhaustion) and 34 backoffice tests passed, along with Ruff,
ty and wheel/source builds. The homepage-only NOVELIC run is saved under
`company_research/data/novelic-autonomous-v15/`; it finished partial at the 24-page
budget. Read the [autonomous report](company_research/NOVELIC_AUTONOMOUS_RESULTS.md)
and its source-audit issues before consuming any output. It starts without guided page seeds and uses native Crawl4AI HTML,
DeepSeek/Together, and the saved 7,981-entry catalog from the MCP test.

The live run recovered 16 Careers listings, all 16 previously checked
Management people and the six Antenna Design tools. It has also exposed external
scope drift into Analog Devices investor/careers pages and vendor/product and
category-quality issues in technology proposals. It fetched no NOVELIC job descriptions.
The final source audit also found an unsupported company-wide ISO 26262 certification,
incorrect Indian legal-name attribution, and an inferred subsidiary relationship in
the narrative. Those are remaining defects,
not resolved by the browser or deduplication changes. No submission or deployment
has been performed. Reported model charges were $0.20520180 plus eight calls with
unknown charges; the run made 124 calls and took 49 minutes 23 seconds.

**Catalog MCP follow-up, 8 September:** package 0.6.0 now provides a
[technology catalog MCP server](company_research/TECHNOLOGY_MCP.md). It refreshes
from ClickHouse at startup, exposes search/details/categories, and prepares new
technology drafts with mandatory description and category ID/suggestion. The
existing submission/backoffice review flow remains responsible for persistence and
approval. The live MCP test read all 7,981 technologies and prepared an ADS draft
without a database write. Read-only stdio, the shared DeepSeek tool loop and proposal
validation passed 60 package tests and 28 backoffice tests. Deployment remains out
of scope. Artifacts: `company_research/data/catalog-mcp-live-v06-final/`.

**Follow-up completed:** package 0.5.0 / schema 1.4 and the
[NOVELIC recheck report](company_research/NOVELIC_RECHECK_RESULTS.md) now supersede
the extraction/discovery baseline discussed below. The report links the company
JSON, frozen engineering replay and guided Careers/application extraction.
They recovered 16 distinct management people, 16 Careers listings, five preferred
job technologies and six specific engineering tools with qualified experience
signals. Six reversed customer relationships were corrected with original records
retained. Fifty-five tests, Ruff, ty and package builds passed.

Keep the PDF/OCR work paused. The historical 0.5.0 Careers follow-up was guided;
the 0.7.0 run above is the homepage-only follow-up. Remaining priorities include
external company relevance, progression from job lists to their descriptions,
proposal metadata quality and company-name variants.
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
| Direct DeepSeek versus pinned OpenRouter replay and job-page extraction | [DEEPSEEK_DIRECT_RESULTS.md](company_research/DEEPSEEK_DIRECT_RESULTS.md) |
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
