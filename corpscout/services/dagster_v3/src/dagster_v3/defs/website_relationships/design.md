# Website domain relationship evidence

This extends the ESEF free-text relationship presentation to saved website captures.
No relationship buckets or ownership decisions are produced. Registry matching for
counterparties is deliberately separate: a name/domain in a description is not a
verified legal entity.

## Source and storage

The existing crawler persists immutable `company-crawl-result/1.2` bundles in
RustFS, including original/cleaned HTML and `documents[].input.links` with local
text, headings, source URL, link IDs, DOM paths and capture metadata. The explicit
`company-research-clickhouse import-s3`/`import-file` importer now projects these
occurrences into the existing `website_crawl_results.external_links` JSON column.
Older research results' `external_links` are also accepted. Original page bundles
remain intact in `documents` and S3. No recrawl or LLM is needed to reimport them.

`website-domain-context-v1` identifies this deterministic projection. Domains use
the crawler's offline public-suffix list including private suffixes; internal
same-domain links are excluded, while distinct hosted tenants remain distinct.
Only HTTP(S) links without URL credentials are eligible. Duplicate occurrences
retain their individual page/link IDs. Empty and unavailable inventories differ.

Migration 000425 owns `website_domain_relationship_analysis` and two views:

- `website_domain_relationship_inputs`: groups saved occurrences by result,
  actual source host, company and destination domain. It uses the latest imported
  result per crawl hostname and kind, as defined by the existing latest-results
  view. A new partial crawl can have narrower coverage; absence is not evidence
  that an earlier relationship ended. Historical captures/attempts remain stored.
- `website_domain_relationships_current`: latest successful interpretation that
  still matches that input's evidence and company identity. A failed retry does
  not erase a still-current success. A new capture or changed association hides
  the old interpretation from this current projection without deleting history.

Attribution requires an **active exact source-host association** in
`company_domains_resolved`, normalized for case, trailing dot and `www`. It must
identify one `(country_code, company_id)` across all associations. A shared host,
unknown subdomain or inactive association is retained as raw evidence but is not
assigned to an arbitrary company. Followed external pages are attributed using
those pages' own hosts, never the crawl's initial target. This first publication
adapter resolves Swedish company names from `se_company_basic_info`; other
countries' captures remain stored until their company identity adapters exist.

This is an interpretation stage over an existing persisted source, so it uses
ClickHouse directly instead of adding a duplicate dlt/DuckDB copy. Immutable LLM
attempts intentionally retain exact inputs/raw output for audit and paid-response
reuse. English prose is generated here; original-language citations are unchanged.
There are no new monetary fields or currency conversions.

## Operation

Apply migrations before deploying. Import a saved result with the crawler CLI:

```sh
uv run --frozen --no-sync company-research-clickhouse --env-file ../../.env \
  import-s3 --path crawls/company-crawls/REQUEST/attempts/0001/result.json.gz
```

Run the deployed `website_domain_relationships_job` after import:

```yaml
ops:
  website_domain_relationships:
    config:
      execute: true
      result_ids: [RESULT_CONTENT_SHA256]
      max_domains: 25
```

Omit `execute` for a preview. Work is bounded and serial in a dedicated Dagster
pool, using the existing DeepSeek profile with 12,000 output tokens. Successful
answers are reused only for identical source evidence, company, prompt and
semantic model settings. Every attempted answer is persisted synchronously.
Invalid quotations, missing/oversized context and provider errors fail the job
visibly after preserving the attempts. No automatic crawler importer or paid
analysis sensor is added; these remain explicit operations.

Website and report adapters share response validation and the model call, with
separate source prompts. Quotations must occur in their cited context or recorded link destination, and named
entities must occur in cited evidence. Recorded link attributes are displayed separately
from page prose; a URL is evidence of a link or embed, not a commercial agreement. This checks text provenance, not semantic
entailment or independent truth. Ambiguous labels/headings must remain cautious.

The Swedish company Domains page displays both sources in the relationship area,
separate from active company websites and website-candidate review history.
Website evidence has capture dates, page titles, exact quotes, surrounding text,
page/link locations, source-page links and the referenced destination URL. A
capture timestamp is not a publication date or a relationship start date.

## Live pilot — 18 September 2026

Migration 000425 is applied. The deployed Dagster run
`5759bdef-a316-481b-b5f1-9773672373dc` completed successfully with prompt v2.
Scandic Hotels Group AB (`SE:5567031702`) contributed 13 occurrences across four
domains from its homepage capture at `2026-09-18T17:44:48Z`:

- `scandichotels.com`: linked hotel booking destination.
- `inderes.com`: linked Q2 2026 presentation/webcast page.
- `whistleb.com`: linked whistleblowing channel.
- `cloudflarestream.com`: observed embedded video sources.

The archived bundle is
`crawls/company-crawls/scandic-domain-context-20260918/attempts/0001/result.json.gz`;
its imported result ID is
`2a47c1b35d82f9e70e941bba723dce8555463a22afeb942df70852242f9c6197`.
All four current interpretations reference the successful run. Initial v1
responses that quoted recorded destinations before attributes were eligible
citations remain in the attempt history; they were withheld from publication.

Both this website input and Öresund's existing 2024 ESEF input returned zero
pending analyses under their current profiles. Öresund's 14 ESEF interpretations
remain published. Its attempted website crawl was refused by robots rules, so no
website context or website interpretation was fabricated for Öresund.

Validation included the actual ClickHouse migration and cache/attribution queries,
Dagster definitions, Python quotation/projection tests, Backoffice TypeScript and
48 focused UI tests, plus browser checks of capture dates, citations, original
source-page links, link attributes and surrounding text. Analysis remains a
bounded explicit job, without a corpus-wide automatic paid backfill.
