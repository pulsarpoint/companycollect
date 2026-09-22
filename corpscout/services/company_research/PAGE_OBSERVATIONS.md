# Information collected from each page

Version 0.28 collects deterministic observations from every successfully captured
page before HTML cleanup, including site-info-only and rejected-site first pages.
It runs automatically for CLI, REST and JetStream, without extra fetches, LLM
calls. Default page selection focuses on contacts, jobs, company information and
financial-information links. Job interpretation and technology analysis are deferred.

The portable result is `company-crawl-result/1.2`. Each
`documents[].input.observations` has schema `company-page-observations/1.1`:

Version 0.30 adds source URL and context to document/financial links: nearby row
text, section heading and page title. Generic “Download” anchors can be recognized
using that context; document links on financial-titled pages are also candidates.
The recorded `basis` distinguishes URL/anchor, surrounding-context and page-title
matches. These are discovery hints, not verified financial facts or entity matches.

| Section | Contents |
| --- | --- |
| `metadata` | Title, declared language/charset, meta tags including Open Graph/Twitter/generator, canonical URL, alternate languages, icons and other head links. Duplicate declarations stay in `meta_entries`; `meta` uses the first value. |
| `text` | DOM text and main readable text, with extraction method recorded. |
| `structured_data` | Complete JSON-LD blocks, independently addressable typed/identified nodes, type inventory, and all microdata scopes/properties: jobs, products/offers, people, organizations, addresses, dates, employee counts, logos and other published fields. |
| `contacts` | Email, phone/fax and social/profile-link occurrences with source and locator. Valid phones use E.164; extensions and original values remain. |
| `identifiers` | Published LEI, VAT, tax, DUNS, NAICS, ISIC and GLN properties; valid standalone LEI/VAT tokens in DOM text, with validation scope and source locator. |
| `technology_analysis` | `deferred`. The tracker/resource collector is preserved but not called. |
| `trackers`, `resources` | `null`, meaning not collected; these are not empty search results. Older observation schema 1.0 bundles can contain these arrays. |
| `document_links` | PDF/office/data-document and explicit download links, without downloading contents. |
| `financial_links` | Likely financial/investor/report HTML or document links, based on URL paths, anchor text or titles; includes source anchor index and `content_examined=false`. This is a keyword heuristic, not complete discovery or a content classification. |
| `response_headers` | Selected content, caching, server/technology and security-policy headers. Cookies and session/authentication headers are excluded; missing input is marked unavailable. |

Each set carries page ID, final source URL, HTML representation and SHA-256.
Page metadata also hashes the observation JSON for replay verification. Malformed
JSON-LD stays as an invalid raw block and makes observation `status` partial;
other sections and the HTML remain available. Main-text failures fall back to DOM
text with an error recorded. DOM text is parsed text, not a browser visibility test.

JSON-LD `script_index` counts JSON-LD scripts from zero; `entity_path` is an RFC 6901
pointer inside that block. `@context` definitions are excluded from entity discovery.
Sibling/nested entities are never merged. Microdata scopes have document-order
indices; nested properties reference `entity_index`, and `itemref` is supported.
Contact sources are `jsonld`, `microdata`, `html`, `link` or `visible_text`. Offsets
refer to the corresponding stored string; anchor indices count anchors with href.
Occurrences stay separate across pages and sources.

An observation does not prove company ownership: a publisher email, provider link
can belong to a third party. Valid phones passed numbering-plan checks.
VAT uses the Common Crawl country formats, DE/IT checksums and format checks
elsewhere; LEI uses MOD 97. Neither checks registry membership. Other identifier
types explicitly have `validation=not_checked` and `valid=null`.

## Common Crawl relationship

The implementation follows `cc-processor/cc-enrich-worker/internal/parse` and
`internal/extract`: email false-positive filtering, social-host boundaries, tracker
patterns (now deferred), LEI/VAT rules, country-hinted phones and JSON-LD provenance. It uses Python
[libphonenumber](https://github.com/daviddrysdale/python-phonenumbers) and
[Trafilatura](https://trafilatura.readthedocs.io/en/latest/corefunctions.html);
library/version differences can change phone or main-text output relative to Go.

It additionally preserves profile URLs, all microdata scopes, duplicate metadata
and invalid JSON-LD. It retains selected live headers rather than a full WARC map.
Raw HTML, declared generator metadata and selected headers support later technology
analysis. No technology inventory or job interpretation runs during collection.
The collector does **not** run Common Crawl's Wappalyzer engine,
embeddings or NACE classifier; those remain processing steps.

## Storage and querying

Observations are part of `result.json` even with `save_artifacts=false`; no separate
observation file is required. With artifacts enabled (the default), the same object
is in `pages/<page_id>/input.json`. S3 uploads the full result. Older 1.1 bundles and
capture folders still replay, with observations unavailable rather than fabricated.

Saved-page analysis preserves observations per page and in `page_observations`.
They are not automatically promoted to LLM records or added to prompts; existing
cleaned/rendered evidence remains the model input.

Migration 422 adds the validated JSON-array column `page_observations` to ClickHouse
results, the latest view and the direct S3 view. The importer projects observations
from document inputs. Old results have SQL NULL; new 1.2 crawls without captured
pages have `[]`. Raw-crawl `jobs` and other company-record columns stay NULL.

```sql
SELECT domain, JSONExtractString(observation, 'source_url') AS page_url,
       JSONExtractString(observation, 'metadata', 'title') AS title,
       JSONExtractRaw(observation, 'contacts') AS contacts,
       JSONExtractRaw(observation, 'structured_data') AS structured_data
FROM corpscout.website_crawl_results_latest
ARRAY JOIN JSONExtractArrayRaw(ifNull(page_observations, '[]')) AS observation
WHERE domain = 'novelic.com' AND result_kind = 'crawl';
```

Use the same expressions with `website_crawl_results_s3_archive` and an exact `_path` filter
to read an uploaded result directly.

## Collection-only validation (v0.28)

An offline replay of the 26 previously captured NOVELIC pages preserved all HTML
and structured data, including 16 JobPosting objects with full descriptions, three
distinct emails and three phones. Technology extraction was disabled and no LLM
or network requests were made. No likely financial links were present in those
captured pages; this does not establish that the site has none. Portable replay and
ClickHouse row mapping passed. This test did not upload or import the new result.
See `data/novelic-collection-only-replay-20260918/summary.json` and its `run.py`.

The 250-test package suite passed (14 optional/live checks skipped), including
default discovery, custom instructions, all crawl CLI entry points, REST/JetStream,
artifact retention and financial-link extraction. Ruff, changed-module type checks
and the locked dependency check passed.

## Historical v0.27 NOVELIC validation, 18 September 2026

A fresh homepage/Careers crawl used zero model calls and `save_artifacts=false`.
Both observation sets reported `collected`, with 21 and 18 JSON-LD nodes (including
references and nested entities, not 39 companies). The homepage exposed
`info@novelic.com`; both pages exposed the same Google Tag Manager container.
The only local file, `data/novelic-observations-20260918/result.json`, passed replay
and hash verification. Counts describe only those two captures.

The same JSON was uploaded to RustFS and imported into live ClickHouse after
migration 422. The S3 round-trip, S3-view projection and stored/latest projection
all matched the original observations exactly. The receipt is in
`data/page-observations-20260918/summary.json`. This does not deploy the updated
systemd crawler on `192.168.88.132`.

Validation passed: the 247-test package suite (14 optional/live checks skipped),
all 13 ClickHouse mapping/integration checks against an isolated live database,
all eight observation tests on Python 3.12, the saved-crawl-to-analysis preservation
check, 132 shared migration checks, Dagster definition loading, Ruff, changed-module
type checks and the locked dependency check. Live ClickHouse tests cleaned up their
isolated database; the NOVELIC result remains available for inspection.
