# SE geocoding: persistent-workbench target architecture

Design doc. Owner-driven (2026-08-25). Supersedes the analysis note
`2026-08-25-se-geocode-engine-boundary-analysis.md` with a concrete target. This restructures how
address→geocode matching is developed and run; it does NOT change what a geocode *is*.

## Motivation

Today a matcher change (policy bump: v6 abbreviation expansion, a future v7 punctuated-suffix, …) forces a
full ~1-hour pipeline run: rebuild canonical identities from 4.67M raw rows (~25 min, output byte-identical
to the prior run), re-parse the OSM PBF (~5 min, same index), rebuild shared addresses, THEN the actual
matching (demand → shadow → store). Only the matching changed; ~30 min is spent rebuilding unchanged
upstream just to stage it in an ephemeral DuckDB for the matcher to read.

Matcher improvements have diminishing returns (v6 ≈ a couple %, v7 maybe a fraction of a %). Re-running an
hour-long production pipeline to test each incremental idea is the wrong cost curve. The iteration loop and
the production run should be decoupled: iteration should be a cheap local loop; production should be a
scheduled invocation of the same function.

Two earlier decisions led here and are consistent with this target: the LLM analysis agent was removed
(too much machinery for occasional analysis), and the OSM-in-ClickHouse mirror was reverted (a CH exact-key
join cannot reflect the real similarity/fuzzy matcher, so analysis cannot live in CH). The missing piece
those removals imply is a reachable place where normalized addresses and the OSM index coexist and the real
matcher can be run cheaply — the workbench below.

## Target architecture

```
ClickHouse (source of record)          DuckDB workbench (persistent, on the Dagster host)
  se_company_addresses_current  ──┐      ┌──────────────────────────────────────────────┐
    (4.67M raw rows)              │      │  normalized_addresses  (2.09M identities)     │
                                  ├─────▶│  osm_address_points / osm_street_segments     │
  OSM PBF (Geofabrik snapshot) ───┘      │  = the reference substrate, refreshed on a    │
                                         │    SOURCE-change clock (new companies / new    │
                                         │    OSM snapshot), NOT per matcher iteration    │
                                         └───────────────────┬──────────────────────────┘
                                                             │
                                        match(workbench, policy) -- THE iterated function
                                         - runs LOCALLY against the workbench (fast loop, minutes)
                                         - identical function invoked by a SCHEDULED Dagster asset
                                             for the authoritative production run
                                                             │
                                                    matched (address_id → geocode)
                                                             ▼
ClickHouse   se_address_geocodes  (versioned store: policy_version + reference_md5, append-only)
                serving view se_address_geocodes_current = refreshable MV over it  (KEEP: attribution + serving)
```

### Components

1. **Workbench (persistent DuckDB, Dagster host).** Holds `normalized_addresses` (the canonical identities
   with normalized fields, keyed by `address_id`) and the OSM reference (`address_points`, `street_segments`).
   A durable file (~few hundred MB), not the ephemeral per-run DuckDB. Reachable for local iteration (SSH +
   `duckdb` CLI, or a thin read API).

2. **Refresh (Dagster asset, source-change clock).** Repopulates the workbench when SOURCE data changes:
   pull `se_company_addresses_current` from CH → normalize + fingerprint + representative-pick →
   `normalized_addresses`; parse the OSM PBF → the reference tables. This is the ONLY place canonicalization
   runs, and it runs on the addresses/OSM cadence (weekly-ish), decoupled from matcher iteration. Uses the
   keyset-chunked, timeout-hardened CH→DuckDB load already shipped.

3. **The match function (the iteration surface).** A pure function `match(workbench, policy) -> outcomes`:
   for each pending identity, score against the OSM reference (street-variant generation incl. abbreviation
   expansion, damerau_levenshtein similarity, spatial context, scoring policy) → an outcome per identity.
   This is the ONLY thing that changes between v6 / v7 / … It is:
   - **iterated locally** against the standing workbench (edit function → re-run → measure yield in minutes,
     using the same diagnostics the real matcher uses — extend `address_resolution/diagnostics.py`), and
   - **run in production** by a scheduled Dagster asset against the freshest workbench.

4. **Result copy to CH.** The production run writes matched outcomes to `se_address_geocodes` (unchanged
   versioned store: keyed by `policy_version` + `reference_md5`, append-only, with the refreshable-MV serving
   view). This preserves attribution, reproducibility, and the serving path exactly as today.

### CH's role shrinks to two things
Source (`se_company_addresses_current`) and versioned output (`se_address_geocodes` + serving MV). Everything
between — normalization, the OSM index, the matching substrate — lives in the workbench.

## Safety / invariants
- **Canonicalization determinism.** The workbench refresh must produce the same `address_id`s the store keys
  on. `address_fingerprint` + representative-pick logic is the identity contract; changing it is a data
  migration, not a matcher iteration. (This is exactly why a policy bump can reuse existing identities: a
  matcher change provably does not touch canonicalization.)
- **Store versioning stays.** Outcomes remain attributable to `(policy_version, reference_md5)`; the two-stage
  versioned read + refreshable MV serving are unchanged.
- **Production reproducibility.** The scheduled production run stamps which workbench snapshot (reference_md5)
  and policy it used; nothing about reproducibility weakens by moving the substrate to a persistent file.

## What this removes / changes vs today
- Canonical rebuild + OSM parse stop being per-matcher-run costs; they become workbench-refresh on the source
  clock. A policy-only iteration touches neither.
- The full weekly geocoding job splits conceptually into (a) workbench refresh and (b) the match+store run,
  which can run on different cadences.
- The matcher becomes locally runnable against a durable substrate — the "few passes" workflow gets a real,
  fast home, faithful to production (same function, same engine, same data).

## Open decisions (resolve during planning)
1. **Workbench durability & placement.** A single DuckDB file on the Dagster host vs a small dedicated volume;
   single-writer discipline between the scheduled production run and ad-hoc local iteration (iteration is
   read-mostly, writing only to scratch outcome tables — but concurrency needs a rule, e.g. iteration works on
   a copy/attach read-only).
2. **Refresh trigger.** Time-based (weekly) vs change-detected (new OSM md5 / raw-address delta). Start
   time-based; add change-detection later.
3. **Local iteration ergonomics.** SSH + duckdb CLI is the zero-build option; a thin "run this candidate policy,
   report yield" harness/asset is the nicer one. Decide in planning; the diagnostics module is the seed.
4. **Production run shape.** Keep it a Dagster asset reading the workbench (not rebuilding it); confirm the
   demand→shadow→store assets repoint from the ephemeral DuckDB to the persistent workbench cleanly.

## Out of scope
- Multi-country generalization (the workbench pattern should generalize, but design SE first).
- Any change to what a geocode/address IS, to the store schema, or to the serving MV.
- Overture/national-register reference sources (separate track; the workbench can hold additional reference
  tables later without changing this design).

## Next step
Planning (writing-plans) → task breakdown → build, AFTER the in-flight v6 rematch and the OSM-CH revert land.
This is the last v6-style hour-long rematch; the redesign's whole point is that v7+ happen in the workbench in
minutes.
