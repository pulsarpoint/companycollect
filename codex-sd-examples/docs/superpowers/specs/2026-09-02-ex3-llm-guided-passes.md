# ex3: LLM-guided page selection and a configurable second pass

**Status:** approved algorithm (user, 2026-09-02); assumptions below are the
implementer's and are open to correction.

## Goal

Extract as much company information as possible from a company website of any
country, language or industry, with the LLM deciding *which* pages to read and
deterministic code deciding *what is allowed*.

## Algorithm (as specified)

1. Try to fetch `sitemap.xml` (robots.txt fallback) and parse its links.
2. **Sitemap found:** send the sitemap URLs to the LLM with our information
   requirements; it picks the pages to crawl. Crawl them with Crawl4AI and run
   the LLM analysis on the stored Markdown.
3. **No sitemap:** run a breadth-first crawl from the base URL within the page
   and depth budget, then run the LLM analysis.
4. After the analysis the LLM suggests the next round of links to crawl, chosen
   from URLs we discovered but did not process, based on what is still missing
   (for example jobs).
5. Crawl that round, analyze it, and merge the results with the first round
   using the LLM.
6. The second pass is a configurable parameter; more than one extra pass is
   allowed but bounded.

## Assumptions locked in by this spec

- **Deterministic guardrails stay in front of every LLM decision.** External
  domains, binary files, images, duplicate URLs and already processed pages are
  removed before any candidate list reaches the model. The model can only pick
  URLs from the list it was given; unknown or duplicate picks are dropped and
  recorded as warnings (same pattern as the related-domain step).
- **Sitemaps are capped.** A sitemap with tens of thousands of product URLs is
  not sent whole. Candidates are ordered by the existing structural scorer
  (homepage, depth, linked from the base page, vocabulary as tie-break) and
  capped by `--llm-candidates` (default 200). The base URL and the base page's
  links always join the candidate list because sitemaps often omit them.
- **Candidates carry title and declared language.** The bounded `<head>`
  fetch already in place runs for the capped candidate list so the model sees
  URL path, title, anchor text and language.
- **Language policy is a prompt sentence, not a constant.** Prefer English or
  the site's own language; take another language only when it is the only
  source for a target field. The deterministic penalty remains only in the
  fallback ordering.
- **Fallback.** If the LLM selection call fails (timeout, invalid output), the
  existing deterministic selection runs and the manifest records why.
- **No sitemap means plain BFS**, as specified. `--discovery best_first` stays
  available for comparison.
- **The information requirements are one shared list** (`TARGET_FIELDS`) used
  by the selection prompt, the gap check and the suggestion prompt: company
  name, legal name, identifiers (registration, VAT, LEI), headquarters
  address, phone, email, description, industries, founded year, employee
  count, management, jobs, products/services, social profiles, group
  structure.
- **Gaps are computed deterministically** from the consolidated result; the
  LLM only decides which candidate URLs are likely to fill them.
- **Pass two analysis is incremental.** Only new pages go to the extraction
  LLM; page-level results from earlier passes are reused. Deterministic
  consolidation runs over all pages first, then one LLM merge call produces
  the final profile from the previous consolidated result and the new round.
  Every evidence URL in the merged output must belong to a processed page;
  items that fail this check are dropped and counted. If the merge call fails
  the deterministic consolidation is the result.
- **Phase artifacts stay durable.** `crawl`, `analyze`, `suggest` and `extend`
  are separate commands with JSON artifacts between them; a `research`
  command orchestrates them with `--max-passes` (default 2, `1` disables the
  second pass).
- **Related domains are out of scope for suggestions** in this iteration. The
  suggestion step only proposes URLs on the crawled site. Allowing confirmed
  related domains (group site, investor relations) is a later flag.
- **The Codex harness floor (~18k tokens per call) is accepted** for the PoC.
  Selection is an easy task and is the first candidate for a smaller model via
  the OpenAI-compatible bridge later.

## Artifacts (in the work directory)

- `crawl-manifest.json` — one manifest; pages carry `pass_number`,
  `selection` (`selected` | `discovery` | `suggested`) and `selection_reason`.
- `url-inventory.json` — every scored inventory URL with reasons.
- `report-pass-1.json`, `suggestions-pass-2.json`, `report-pass-2.json`, …
- `report.json` — copy of the last pass report.

## Cost expectation (handelsbanken.se, 20 + 8 pages)

Pass one: one selection call (~25k tokens) + extraction (~220k). Pass two: one
suggestion call (~25k) + one or two extraction batches (~40–80k) + one merge
call (~30k). Roughly 350k tokens and 25–30 minutes end to end.

## Non-goals

Boilerplate stripping, deterministic fact extraction, typed schema changes,
concurrent batches and the eval fixture set remain separate work items (see
the 2026-09-02 analysis notes).
