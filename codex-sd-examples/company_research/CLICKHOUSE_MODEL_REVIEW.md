# Company/domain technology model review — 6 September 2026

Reviewed the live `corpscout` ClickHouse metadata and bounded reads of the company,
catalog and technology rollup tables, plus migration, publishing and serving code.
No schema, stored data or production code was changed. The multi-billion-row page
detection table was inspected through system metadata, not scanned.

## Conclusion

The company-to-domain model supports multiple domains correctly. The existing
technology model describes website/DNS detections and their association with company
domains. It does not yet represent independent company technology observations from
job ads, with optional domain applicability, statement type and evidence.

The new `company_research` package writes JSON. Its technology records use company
names rather than resolved country/company IDs and are not published to ClickHouse.

## What already exists

| Table | Current purpose | Relevant finding |
|---|---|---|
| `company_domains` | Company/domain associations, provenance and review state | Key includes country, company and domain; multiple domains are supported |
| `commoncrawl_page_technologies` | Technologies detected on crawled pages | Has page URL and WARC reference; no separate subject company |
| `domain_signal_technologies` | DNS detections with matching evidence and seen dates | `signal_type` is the detection channel, e.g. `dns_mx`, not an employment/usage relationship |
| `technology_companies` | Company/domain/technology rollup | Contains only technology, country, company, root domain and computation time |
| `se_jobtech_links_job_ads` | External job advertisements | Retains employer name/URL, canonical source URL, provider and lifecycle |
| `se_jobtech_links_job_ad_company_matches` | Versioned job-ad/company matching and review | Suitable existing connection point; live table currently has zero rows |

Live deduplicated `company_domains` contains 8,391 associations for 8,179 companies.
101 companies have more than one domain; the largest observed domain count is 30.
All current associations are active and unreviewed.

## Gaps and defects

1. **There is no independent company technology observation table.**
   `technology_companies` is derived by joining page/DNS detections to
   `company_domains` on `root_domain`. It cannot faithfully carry an employer's
   Kubernetes statement on an external job board without confusing the source
   domain with the subject. It also lacks signal/scope, quotations, job references,
   attribution status and observation time. Its `computed_at` is a rollup build
   timestamp. Adding rows manually is not an ingestion path: the publishing asset
   reconstructs and exchanges the whole table on refresh.

2. **The Companies tab counts company/domain pairs rather than distinct companies.**
   Both pagination and its count include `root_domain`. Live verification for
   Google Search Console returned 3,557 domain associations but only 3,459 distinct
   country/company pairs. Microsoft 365 returned 4,220 versus 4,138. Company-level
   counts should deduplicate by `(country_code, company_id)` and list domains within
   each company; domain counts remain a separate metric.

3. **The company technology publisher ignores domain review and active state.**
   The final join reads every `company_domains FINAL` row without filtering
   `is_active` or `review_status`. Rejected or inactive associations would therefore
   continue attributing detections to companies. None currently exist in the live
   table, so this is a confirmed code defect with no observed rejected/inactive
   contamination at inspection time. Treatment of unreviewed associations should be
   explicit and retain their attribution uncertainty.

4. **Technology identity needs an ingestion mapping.**
   The catalog uses exact detector names as keys. Job-ad extraction preserves source
   spelling. Live examples include catalog `git` versus extracted `Git`, and
   `Amazon Web Services` versus `AWS`; several observed job technologies lack an
   exact catalog entry. Preserve the raw mention, resolve to a catalog key where
   supported, and retain unresolved names for curation rather than fuzzy-merging
   them or dropping them from analytics.

## Recommended extension

Add an evidence-level `technology_observations` table alongside the current detection
tables. Each row represents one supported statement, not a company/domain aggregate.
The required information is:

- Stable observation ID and source version/run references.
- Subject company `(country_code, company_id)` when resolved, original company name
  and attribution status/basis. Unresolved entities must remain representable.
- Optional applicable domain: populated only when the evidence establishes domain
  applicability, not copied to every domain belonging to the company.
- Resolved technology catalog key, original technology mention and mapping status.
- Relationship signal (`stated_use`, `required_experience`, etc.), scope, alternatives
  and explicitly stated dates.
- Discovery method, source URL, source domain, quotation fragments and a retrievable
  source artifact reference. Source domain is independent of applicable domain.
- Job-ad ID/version/URL when available, and employer separately from a client whose
  technology is described.
- Observation time, source-check status and any review issues.

Build company and domain serving rollups from qualifying observations. Preserve
relationship and scope dimensions so candidate experience, current usage, migration
plans and explicit non-use do not become one undifferentiated adoption count.
Keep raw HTML and full model payloads in the existing artifact/object storage;
ClickHouse needs the analytical columns and cheap, retrievable lineage.

For the existing Swedish external-ad source, reuse accepted records in
`se_jobtech_links_job_ad_company_matches`. The general crawler needs an equivalent
company-resolution step; a company name or the input website alone is not a resolved
legal entity, especially for subsidiary and staffing/client descriptions.

## Code anchors

- [Company/domain key and provenance](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse/migrations/000269_corpscout_company_domains.up.sql:3).
- [Current technology rollup schema](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse/migrations/000354_corpscout_technology_adoption_tabs.up.sql:12).
- [Domain-derived company publishing join](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3/src/dagster_v3/defs/technology_catalog/assets.py:672).
- [Companies list grain](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice/app/lib/technologies.server.ts:396).
- [Companies count](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice/app/lib/technologies.server.ts:511).
- [External-ad matching](/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse/migrations/000363_corpscout_se_jobtech_links_jobs.up.sql:226).
- [Extractor's current company-name schema](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_research/src/company_research/models.py:174).
- [Extractor's JSON output boundary](/Users/graovic/pulsarpoint/ppoint/companycollect/codex-sd-examples/company_research/src/company_research/research.py:489).
