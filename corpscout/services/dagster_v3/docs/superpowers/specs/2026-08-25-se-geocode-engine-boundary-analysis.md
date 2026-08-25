# SE geocoding: DuckDB↔ClickHouse engine-boundary analysis

Analysis note (not a plan). Captures how the Sweden address-geocoding pipeline splits work
between ClickHouse and DuckDB, which splits are *necessary* vs *incidental*, and two concrete
optimization opportunities. Written 2026-08-25 from reading the live assets; nothing changed.

## The pipeline, stage by stage (verified against the assets)

```
ClickHouse   se_company_addresses_current            4.67M raw per-company address rows
                (one row per company × address_type × source)
     │
     ▼  sweden_company_canonical_addresses_duckdb  [DuckDB]
     │     load raw from CH → normalize (lower/trim/strip-accents/regexp) →
     │     address_fingerprint → GROUP BY + window (row_number/first) to pick a
     │     representative and collapse duplicates
     ▼
ClickHouse   se_addresses_current  2.09M identities (WITH normalized_street /
                normalized_postal_code / normalized_post_town) + members bridge
                (published by sweden_company_canonical_addresses_clickhouse)

ClickHouse   sweden_osm_pbf_s3   Geofabrik SE PBF synced to S3
     │
     ▼  sweden_osm_addresses_duckdb  [DuckDB]  st_readosm + spatial → address_points + street_segments
     │     (also now published to CH: se_osm_address_points / se_osm_street_segments)
     ▼
DuckDB   sweden_address_geocode_demand_duckdb   → se_address_pending_identities (what needs matching)
     │
     ▼  sweden_address_resolution_shadow_duckdb  [DuckDB]  THE MATCHER
     │     score pending identities vs OSM index: street-variant generation (incl. v6
     │     abbreviation expansion) + damerau_levenshtein fuzzy + scoring policy
     ▼
DuckDB   sweden_address_resolution_current_duckdb  promote shadow outcomes → serving/hand-off table (DuckDB)
     │
     ▼  sweden_address_geocode_store_clickhouse  [ClickHouse]
     ▼
ClickHouse   se_address_geocodes   versioned geocode-match store (append-only)
                (serving view se_address_geocodes_current = refreshable MV over it)
```

## Which engine placement is NECESSARY vs INCIDENTAL

| Stage | Engine today | Necessary? | Why |
|---|---|---|---|
| Raw addresses | ClickHouse | — | source of record |
| **Normalization + canonicalization** | **DuckDB** | **NO — incidental** | plain SQL (normalize + hash-fingerprint + GROUP BY/window); ClickHouse does all of it natively. It's in DuckDB only for *locality* — it's the first stage of the DuckDB resolver pipeline and feeds the next DuckDB stage without round-tripping through CH. Output already lands in CH (se_addresses_current). |
| PBF → OSM index | DuckDB | **YES** | st_readosm + spatial geometry; ClickHouse cannot read a PBF |
| Demand scan | DuckDB | mostly | reads/writes DuckDB working tables; loads prior outcomes from CH (now keyset-chunked) |
| **The matcher (shadow)** | **DuckDB** | **arguable** | fuzzy street matching (damerau_levenshtein + variant generation + scoring). ClickHouse HAS edit-distance functions, so not strictly impossible — but this is the complex, iterated core (v6/v7 policy). DuckDB-vs-CH here is a real engineering decision, not a slam-dunk. THIS is the asset we iterate. |
| Match store | ClickHouse | — | versioned store + serving MV |

Fixed-where-they-are: raw addresses (CH), PBF/OSM parse (DuckDB), match store (CH).
Movable in principle: normalization (easy) and the matcher (hard).

## Optimization opportunity 1 — policy-only rematch skips the canonical rebuild (cheap, high value)

A policy bump (v6→v7 …) changes ONLY the matcher. It does NOT change address canonicalization —
`se_addresses_current` is byte-identical before and after. Yet a full rematch today re-derives those
2.09M identities from the 4.67M raw rows in DuckDB (~25 min observed on run dce200db), purely to have
them in the DuckDB working file for the matcher.

Fix: for a policy-only / `rematch_all` run, BULK-LOAD the existing `se_addresses_current` (and the
OSM index) from CH into DuckDB instead of re-deriving. ~25 min → ~1-2 min. Same spirit as the
`rematch_all` demand-skip already shipped. Precondition to verify: the shared-address/demand/shadow
steps must consume the canonical layer purely as data (no hidden dependency on the run recomputing it).

## Optimization opportunity 2 — move normalization/canonicalization into ClickHouse (deeper, structural)

Because canonicalization needs nothing DuckDB-specific, it could be a ClickHouse-native transform:
maintain the canonical identities (normalized fields + fingerprint + representative pick) as a CH table,
incrementally, and let DuckDB do ONLY what it must (PBF parse + the fuzzy matcher, reading canonical
identities from CH). Benefits: canonical layer becomes a queryable/incrementally-maintained CH table
instead of an ephemeral 25-min rebuild every run; subsumes opportunity 1. Costs/risks: the
address_fingerprint + representative-pick logic must stay BYTE-IDENTICAL across the two engines (a
divergence silently changes address_ids and breaks the store's key join); splitting the pipeline across
two engines adds a serialization seam; window/first() semantics must match exactly. Worth a proper
spec if pursued.

## Relationship to what already shipped
- OSM index is now queryable in CH (se_osm_address_points/street_segments) — enables measuring matcher
  yield in SQL (the manual analysis workflow that replaced the removed agent).
- The demand + canonical loads are now keyset-chunked + timeout-hardened (incident fix), so the current
  pipeline is robust even without these optimizations.
