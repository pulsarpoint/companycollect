# External links with source context

Version 0.15.0 adds `external_links` to research JSON schema 1.10. Link observations
are collected on every successfully fetched page before company extraction, queue
filtering or model assessment. They remain available if later processing fails.

Each observation contains:

- `url`: the resolved full HTTP(S) URL, including query parameters and fragment.
  `raw_href` preserves the parsed HTML attribute before URL resolution. HTML entities
  are decoded by the parser; the original markup remains in the source artifact.
- Destination hostname and registrable domain, source page URL/domain/ID, fetch time,
  source HTML filename/hash and anchor index.
- Anchor text, title, accessibility label, image alt text and `rel` values.
- Page region, ancestor DOM path, section heading and nearby text. The nearby-text
  excerpt is limited to 1,600 characters, with `context_truncated` indicating truncation.
- A separate optional relationship assessment with evidence, a description, the named
  destination entity and a basis of `explicit_text`, `contextual_hint` or `unknown`.
  `independently_verified` is always false.

Two links to the same URL in different places remain two observations. A header may
describe another business while an article describes a partnership. Observations on
different source pages also remain separate. Queue URL normalization and its bounded
context samples do not alter or cap the complete observation inventory.

## Where context comes from

New fetches inspect the browser's rendered HTML before cleanup and save it under
`link-html/<page-id>.html`. This retains header/footer structure and logo labels that
cleanup may remove. Simplified HTML is stored separately for later extraction.
Semantic HTML elements and ARIA roles identify regions; unknown structure
is not guessed from a destination URL. DOM occurrences may include responsive/hidden
navigation; visibility is not independently measured.

If rendered HTML is unavailable, extraction uses cleaned HTML and says so. Additional
links returned by the browser capture but absent from the chosen HTML receive
`extraction_method=browser_links` and unknown location/context. Historical
`crawl4ai_links` values remain readable. The original browser
link metadata is saved under `fetches/<page-id>-links.json`. Its normalized duplicate
of an already captured DOM destination is not counted as another occurrence.

HTTP(S) anchors and image-map links on a **different registrable domain from the fetched
source page** are external. Subdomains of the same registered site are internal;
private-suffix tenants such as separate `github.io` sites remain distinct. Without a
recognized public suffix, hostnames are compared. Relative and protocol-relative links
use the fetched URL and any HTML `<base>` element. Email/telephone/JavaScript links and
credential-bearing URLs are excluded. No destination is fetched merely to record it.

Social links, documents and account links remain in the inventory even when crawl
policy excludes them. External pages themselves can contribute observations: relationship
direction then refers to that source page's operator, not automatically the original
research company.

## Assessment and crawl selection

The existing link-selection prompt now receives sampled source contexts from observed
external links. This improves prioritization without treating guessed relationships as
facts. A candidate receives up to five distinct contexts; every occurrence is still
stored in the output.

A separate final assessment stage uses remaining model budget **after core extraction
and company summary**. Defaults allow three batches of 20 observations, configured by
`max_external_link_assessment_calls` and `external_link_batch_size`. Setting the former
to zero disables inference while retaining collection. HTTP retries remain governed by
the shared client settings and total run budget. Larger inventories retain explicit
`not_assessed` observations beyond the assessment budget.

Relationship categories include partners, customers, suppliers, group companies,
parents/subsidiaries, brands/other businesses, recruitment, social profiles,
documentation, technology providers and references. Header position alone establishes
none of these. A neighboring “Partners” menu item must not classify an unrelated logo.
“Our businesses” does not prove a particular ownership percentage or legal parent.

Model assessments are bound to observed IDs. Duplicate/missing responses fail for those
observations; unsupported quotations or entity names become `needs_review`. All raw
observations remain. Matching a quote is not an independent semantic verification.
These assessments are **not promoted into `records.company_relationships`**, company
overviews, or backend company mappings.

For example, these are two distinct source observations (illustrative):

```json
[
  {
    "url": "https://nova.example/?utm_source=group#labs",
    "source_url": "https://source.example/",
    "page_region": "header",
    "section_heading": "Our businesses",
    "image_alt": ["Nova Labs"],
    "assessment": {
      "relationship": "other_business",
      "basis": "explicit_text",
      "description": "Listed under the source's businesses; ownership unspecified."
    }
  },
  {
    "url": "https://nova.example/?utm_source=group#labs",
    "source_url": "https://source.example/",
    "page_region": "main",
    "surrounding_text": "Our implementation partner Nova Labs supports deployment.",
    "assessment": {
      "relationship": "partner",
      "basis": "explicit_text",
      "description": "The source names Nova Labs as an implementation partner."
    }
  }
]
```

## Output and validation

- `result.json`: complete `external_links`, plus inventory/assessment counts under
  `discovery.external_links`.
- Each page's `external_link_count`: zero means collection completed with no external
  links; null means no inventory was produced. `external_links_file` locates its capture.
- `external-links/<page-id>.json`: original per-page observations, before LLM inference.
- `external-links.json`: combined inventory with final assessment statuses.
- `external-link-assessments/` and `calls/`: assessment artifacts and model traces.

Tests cover full URLs, repeated contexts, source redirects, base URLs, private tenants,
internal subdomains, skipped crawl targets, logo/header context, metadata fallback,
malformed model output, invented evidence/entities and exhausted assessment budgets.
Browser tests exercise actual CloakBrowser fetching and final JSON, including browser recovery.

An offline replay of the six saved Memgraph/Oxide/RT-RK pages found **49 occurrences**:
13 Memgraph, 24 Oxide and 12 RT-RK. Inputs and results are saved under
`data/external-links-v015/`. Those old inputs are cleaned HTML, so missing original
header structure cannot be reconstructed; the new browser tests cover rendered HTML.

A direct `deepseek-flash` smoke run assessed all 49 in three calls: 14 unknown,
eight recruitment, 11 media references, ten social profiles, three documentation,
one reference, one brand and one possible technology provider. All responses passed
ID/schema/quotation checks; this is not a semantic accuracy score. Fourteen blank or
ambiguous links remained unknown, and 30 assessments explicitly use contextual hints.
Some social-profile hints rely on a platform URL plus generic footer text and should
retain that uncertainty. No ownership/partnership records were promoted. The original
calls, frozen implementation and assessment-run manifest are saved with the replay.

Validation completed: 143 regular tests and three real-browser tests pass, including
nine new external-link unit tests. Ruff, type checks, whitespace checks and version
0.15.0 wheel/source builds pass. Details are in the replay's `verification.json`.
