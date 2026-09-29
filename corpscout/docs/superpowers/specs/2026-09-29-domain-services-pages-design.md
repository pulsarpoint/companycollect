# Domain services pages: service intervals, provider counts, backoffice

Status: DRAFT for owner review (2026-09-29)
Builds on: `docs/superpowers/specs/2026-09-28-dns-detect-slice-4-storage-design.md`
(the resolver, `dns_record_services`, `dns_record_resolutions` and the
per-domain views `domain_services_history` / `domain_services_now`, migration
000468).

## Owner decisions (2026-09-29)

- **Providers are the model for DNS evidence (option A).** "Which domains use
  X" lives on provider pages built from the resolver's results. The technology
  pages (`/admin/technologies`) keep only webtech's web technologies; their
  DNS side (`technology_adoption`, `technology_top_domains`,
  `technology_companies`, all fed by `domain_signal_technologies`) goes when the
  old pipeline is retired. There is no service → catalog-technology mapping.
- **Provider pages read a table that Dagster maintains per bucket** (approach 1).
  A refreshable view over the whole results table and querying
  `dns_record_services` by provider were rejected: the table has 682M rows
  (30 GiB) sorted by domain.
- **The domain page reads the existing per-domain views.**
- **Company ↔ domain stays out of scope.**

## Data (migration: next free number, checked against main and the prod ledger before merge)

### `domain_service_intervals`

One row per period a provider's service was in use on a domain. It holds the
same thing `domain_services_history` returns, stored for every domain.

```
root_domain String, service_type LowCardinality(String), provider_key String,
provider_slug LowCardinality(String), service_keys Array(String),
first_seen Date, last_seen Date, is_current UInt8,
evidence UInt32, analyzers Array(LowCardinality(String)),
record_types Array(LowCardinality(String)), confidence Float32,
bucket UInt8, computed_at DateTime64(3, 'UTC')
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY (provider_slug, service_type, root_domain, first_seen)
```

- `bucket` = `cityHash64(root_domain) % 128`, the resolver asset's partition.
- `provider_slug` is `''` for a provider key no definition names yet (an
  unmapped key).
- `is_current` is the `domain_services_now` rule: the period reaches the
  domain's latest scan of one of its record types.

### `provider_service_counts`

```
bucket UInt8, provider_slug LowCardinality(String), provider_key String,
service_type LowCardinality(String), domains_now UInt32, domains_ever UInt32,
computed_at DateTime64(3, 'UTC')
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY (provider_slug, provider_key, service_type)
```

Pages sum the 128 buckets, which is a few thousand rows. Filtered to
`provider_slug = ''`, it is the list of unmapped provider keys.

### One history definition

- The bucket SQL reproduces the rules of `domain_services_history`:
  - latest resolution per record only;
  - fallback rows dropped where a non-fallback row of the same service type
    overlaps them;
  - windows of one service less than 45 days apart merged.
- A test checks that both give identical intervals on the same fixture.

## The Dagster asset: `domain_service_intervals_clickhouse`

- **Location:** `defs/dns_detect`.
- **Partitions:** the same 128 (`hash_000`…`hash_127`), depending on
  `dns_record_services_clickhouse`.
- **Job:** it joins `dns_record_services_job`, so each partition run resolves
  and then rebuilds its intervals. The daily schedule and the version sensor
  cover it unchanged.
- **Pool:** its own, `dns_detect_intervals`, so a slow ClickHouse step never
  holds a resolver slot.

**Per bucket:**
1. Create two empty stage tables shaped like the targets.
2. `INSERT … SELECT` the bucket's intervals from `dns_record_services` and
   `dns_record_resolutions`.
3. `INSERT … SELECT` the counts from the staged intervals.
4. `REPLACE PARTITION` both targets from their stages, then drop the stages.
   A failure before this step leaves the live tables untouched, and a re-run
   is safe.

**Guards:**
- **Empty bucket:** refuse to swap when the stage is empty while
  `dns_record_services` has rows for the bucket.
- **Memory:** capped through query settings. Bucket 3 (about 4M result rows)
  is measured first. Only if one query is too heavy does the SQL split the
  bucket into sub-slices by a second hash.

**Metadata:** intervals, current intervals, mapped providers, unmapped keys,
seconds.

## Pages (backoffice, admin only)

Queries go in `app/lib/domain-services.server.ts` (parameterized ClickHouse
queries), types in `app/types/api/`, and query parameters are validated with
Zod, falling back to defaults.

### Domain page: Services tab (`/admin/domains/:domain/services`)

This is a new tab in `TechnologySectionTabs`. It reads `domain_services_history`
and `domain_services_now`.

- **Now:** current providers grouped by service type: DNS, mail, mail
  filtering, outbound mail, hosting/CDN, DMARC reporting, verification.
  - Each shows the provider (linked to its provider page), services, since,
    and confidence.
  - An unmapped key is shown bare with an "unmapped" badge.
- **History:** every period, including ended ones. Columns: service type,
  provider, first seen, last seen, current/ended, evidence count, record types.
- **Evidence:** a period expands to its records, read from
  `dns_record_services_current` for the domain: record name, type, value, rule,
  window.
- **Empty states:** "not resolved yet" (no resolutions) and "resolved, no
  providers found".

### Providers list (`/admin/providers`, new nav entry)

It reads `provider_service_counts`.

- **Providers:** one row per provider with its category and domains now/ever
  by service type. Sortable and searchable.
- **Unmapped provider domains:** the most frequent unmapped keys with domain
  counts. This is the worklist for new provider-recon definitions.

### Provider page: Domains tab (`/admin/provider-feeds/providers/:slug/domains`)

The current provider page content becomes the "Definition & feeds" tab.

- **Summary:** counts per service type and per service.
- **Domain table:** paged, 50 per page, as a range read on
  `domain_service_intervals`.
  - Filters: service type, service, now/ever.
  - Columns: root domain (linked to its Services tab), service types, first
    seen, last seen, current.

### Errors

- **ClickHouse failures** show the page's error card, like the provider-feeds
  pages.
- **An unknown provider slug** is a 404.
- **An unknown domain** gets the "not resolved yet" state.

## Rollout

1. Apply the migration.
2. Merge the watermark branch (`dns-detect-watermark`) and this work. Deploy
   dagster_v3 from a pristine tree **after the running knowledge refresh
   finishes**: a deploy cancels in-flight runs.
3. Backfill `domain_service_intervals_clickhouse` alone for all 128
   partitions, timing `hash_003` first.
4. Enable `dns_record_services_daily`.
5. Merge the backoffice pages to main. The owner runs the backoffice locally,
   and new route files need a restart of the long-running dev server.

## Testing

Ordinary per-case tests, no harness.

- **Migration:** a contract test and the `EXPECTED_MIGRATIONS` entry.
- **Bucket SQL** (clickhouse-local, fixture rows):
  - identical to `domain_services_history` (fallback suppression, the 45-day
    merge, a real gap kept, older resolutions hidden);
  - `is_current` equal to `domain_services_now`;
  - counts consistent with the intervals;
  - an unmapped key kept with an empty slug.
- **Asset:**
  - partition → bucket;
  - the swap only after both inserts;
  - the refusal to swap in an empty bucket;
  - metadata;
  - job membership;
  - SQL rendered through clickhouse-driver's real substitution (`%%`).
- **Backoffice:**
  - Vitest for query-module mapping and parameter handling;
  - route tests for the Services tab states (now, history, evidence, empty)
    and the Domains tab paging and filters;
  - `npm run typecheck`.
- **After rollout:**
  - `hash_003` timing;
  - hubspot.com and loopia.se Services tabs against `domain_services_history`;
  - the IONOS provider page counts against a direct count.

## Out of scope

- Adoption trends over time: a later precomputed table.
- CSV export.
- Company linkage.
- Retiring `domain_signal_technologies` and the `technology_*` DNS rollups:
  owner go-ahead after the refresh completes and coverage is checked.
