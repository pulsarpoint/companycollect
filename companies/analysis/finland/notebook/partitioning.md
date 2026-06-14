# Partition Strategy — canonical ClickHouse tables

Refines the tentative partition choices in `companies/analysis/_canonical/canonical_schema.md`
with evidence from the Finland reference run, and projects them across ~150 countries.

## Measured sample (Finland, bounded reference run)

500 companies, one-week XBRL window (`2025-01-01..2025-01-08`):

| Canonical table | Rows | Distinct keys | Notes |
|---|---:|---|---|
| `company` | 500 | 500 `company_uid` | 178 active / 322 inactive |
| `registrations` | 500 | 500 `registration_uid` | 1 registration per company (single-key country) |
| `financials` | 3,540 | 15 `period_end` | ~20 metric rows per statement, 178 statements |
| `company_websites` | 63 | — | ~12.6% of companies (Finland publishes few) |
| `persons`, `company_people`, `company_contacts`, `company_relationships` | 0 | — | known-absent in Finland open data |

**Full-Finland extrapolation** (rough, for sizing — not measured): registrations and
company ≈ **819k** each (full YTJ); financials grows with digital filers (~5% of
companies file digitally) × periods × ~20 metrics — order of **10^5–10^6 rows/year**;
websites ≈ 6% ≈ **49k**. Finland is single-registration, so `company` ≈ `registrations`.

## ClickHouse partitioning principles applied here

- **Partitions are for data management and coarse pruning, not query tuning.** Row-level
  query speed comes from `ORDER BY` (the sorting key), not the partition. So partition by
  the unit you *rebuild/drop/retain as a whole*, and let `ORDER BY` carry lookup speed.
- **The conformance boundary is a country.** Each country's conformance run replaces that
  country's rows wholesale, so `PARTITION BY country` makes a rebuild a single
  `ALTER TABLE ... REPLACE/DROP PARTITION` — no cross-country contention.
- **Keep partition count bounded and parts large.** 150 countries → ~150 partitions per
  country-partitioned table: well within limits. Avoid the "too many parts" problem by
  loading each country **in bulk** (one insert per partition replace), never row-by-row.
- **Every table is `ReplacingMergeTree(updated_at)`** — re-runs supersede; query with
  `FINAL` (or `argMax(updated_at)`) at the serving edge.

## Per-table recommendation

| Table | PARTITION BY | ORDER BY | Rationale |
|---|---|---|---|
| `registrations` | `country` | `(country, registration_number)` | ~150 partitions; per-country rebuild boundary; national PK lookups |
| `company` | `home_country` (`'XX'` if unknown) | `company_uid` | entity is country-independent; partition coarsely by domicile for per-country prune + rebuild; point lookups via `company_uid` |
| `financials` | `country` (start) → `(country, toYear(period_end))` if a country grows large | `(company_uid, period_end, metric_code, basis, period_reference)` | tall, time-series, the growth table; country-only is simplest; add year-subpartition only when a single country partition exceeds tens of GB |
| `company_websites` | `country` | `(country, company_uid, normalized_url)` | sparse; per-country boundary; `host`/`normalized_url` lowercased so dedup/joins are case-stable |
| `company_relationships` | `reporting_country` | `(reporting_country, from_registration_uid, relationship_type)` | the registry that asserts an edge owns it → rebuilds with that country |
| `company_people` | `reporting_country` | `(reporting_country, registration_uid, role)` | personal data; same reporting-country boundary |
| `company_contacts` | `country` | `(country, company_uid, contact_type)` | per-country boundary |
| `persons` | `cityHash64(person_uid) % 32` | `person_uid` | country-independent + **GDPR**: partition by uid hash so right-to-erasure is a keyed delete, not personal-attribute partitioning |

## Partition-count budget across 150 countries

| Table | Partitions | Within limits? |
|---|---:|---|
| `registrations`, `company_websites`, `company_contacts` | ~150 each | yes |
| `company` | ~151 (home countries + `XX`) | yes |
| `company_relationships`, `company_people` | ~150 each | yes |
| `financials` | 150 (country) → ~1,500 (× ~10 years) | yes |
| `persons` | 32 (hash buckets) | yes |

All comfortably below ClickHouse's practical partition ceiling (low thousands). The risk
is **not** partition count — it's part proliferation from drip inserts. The per-country
bulk-replace load pattern keeps parts few and large.

## The `company` partition tradeoff (home_country vs hash)

- **`home_country` (recommended):** readable, single-valued, prunes "companies in country X",
  and aligns with rebuilding a country's entity rows. Caveat: a company that redomiciles
  changes partition — rare; handle via a `succeeded_by` relationship rather than re-keying.
- **`cityHash64(company_uid) % N`:** stable under redomiciliation, but opaque and gives no
  country pruning. Choose only if redomiciliation churn proves material (it won't at first).

## Small-country skew

A microstate with a few hundred companies still gets its own ~few-hundred-row partition.
At 150 partitions this is harmless (the "too many small parts" problem starts in the
thousands). No special handling needed; do **not** merge small countries into a shared
partition — it would break the per-country rebuild boundary.

## Serving with ReplacingMergeTree

Supersede is keyed by the sorting key, versioned by `updated_at`. For correctness at read
time use `... FINAL` on point/aggregate queries, or schedule `OPTIMIZE ... FINAL` per
partition after a country reload so most reads hit already-merged parts.

## Open items (revisit with full-scale data)

- Confirm the `financials` country-only vs `(country, toYear)` threshold once a large
  country (e.g. a full national financials feed) is loaded.
- Validate `persons` hash-bucket count (32) against real person volumes and erasure cadence.
- Decide `OPTIMIZE FINAL` cadence vs `FINAL`-on-read for the serving layer.
