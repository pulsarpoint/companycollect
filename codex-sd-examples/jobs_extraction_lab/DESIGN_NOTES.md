# Crawler and extraction design notes

The split in `ex3` is the right place to build on: discover candidate URLs, select
pages, persist the crawl, and run extraction against saved content. The separate
`ex4` selection experiment is also useful because bad page selection and bad field
extraction should be measured independently.

## Changes I would prioritize

1. **Save complete source content before applying model budgets.**
   `ex3/crawler.py`, around line 1200, truncates Markdown before saving it, and around
   line 1585 it applies another analysis-time limit. The first truncation permanently
   removes evidence from a snapshot. For job boards, openings in the middle can
   disappear. Save full Markdown/HTML with a content hash, then create bounded model
   inputs from that snapshot. This experiment already saves complete inputs.

2. **Split job lists into intact listing blocks.**
   The captured Ashby Markdown often runs headings together and joins title,
   department, location, and employment metadata in one link label. A compact model
   can copy the whole label as the title, and a long response can consume its output
   budget. Preserve the job link and inherited section heading, label the fields
   where the DOM makes them explicit, and send small batches of intact listings.
   Record which blocks succeeded and deduplicate by job URL. Compare this as a new
   experiment against the same 40 frozen pages.

3. **Use deterministic structured extraction for known platforms.**
   For Ashby, Greenhouse, and Lever, inspect available public structured data or
   stable DOM fields before using a model. Keep source provenance and capture time.
   A small LLM is useful for unfamiliar layouts and ambiguous text; a larger model
   can handle only cases that fail validation. This is a recommendation for the
   architecture, not something the current Markdown-only benchmark measures.

4. **Keep sitemap ranking bounded and measure its candidate recall.**
   `ex3/seeding.py` already combines sitemap inventory with the homepage and its
   links, and `ex3/llm_selection.py` validates that model picks belong to the supplied
   candidates. Retain those safeguards. `candidate_shortlist()` in
   `ex3/candidates.py` deterministically truncates the candidate list before the LLM
   sees it; a relevant URL outside that shortlist cannot be recovered by a better
   selection prompt. Reserve candidate coverage for the requested categories and
   evaluate shortlist recall separately from final page-choice precision.

5. **Treat external career boards and pagination as discovery concerns.**
   A company's sitemap may only contain `/careers`, which links to a different
   recruitment host. Allow discovered career-board links through an explicit,
   bounded crawl policy. Record pagination/load-more coverage separately from
   extraction success: extracting every visible listing does not prove all jobs
   on the site were collected.

6. **Evaluate field correctness, not just valid JSON or model agreement.**
   The experiment records schema failures, source quotation flags, unmatched jobs,
   and field differences. Next, manually label a fixed subset across platforms,
   including empty boards, multilingual pages, general applications, repeated job
   titles, and pagination. Use some pages to improve the prompt and untouched pages
   for the final comparison. Codex is a useful reference but can also be wrong.

7. **Use a direct inference client for routine extraction.**
   `ex3/llm.py` creates an SDK client and an ephemeral Codex turn for each operation.
   That supports the existing application, but extraction from supplied text needs
   no agent tools. Keep orchestration in ordinary Python, use direct model requests
   for extraction, and reserve an agent runtime for tasks requiring investigation
   or tools. The lab uses two concrete adapters so this choice can be measured
   without introducing a general provider framework.

The recommended flow is: sitemap plus homepage links → bounded goal-specific
candidate selection → Crawl4AI snapshots → deterministic job blocks → small-model
extraction where needed → validation → selective larger-model fallback. Each stage
should retain enough evidence to rerun the next stage without crawling again.

No production crawler behavior was changed by this experiment.
