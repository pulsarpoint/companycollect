# SE Geocode Lab — design

Owner-approved in chat 2026-08-25 ("Do it", exact-only caching, terminal-first). Goal: iterate on
address-augmentation ideas in minutes and let confirmed full matches reach serving without a policy
bump + full rematch per idea.

## Components

1. **Augmentation hook** — `sweden_company/geocode_lab.py`: one pluggable pure function
   `augment(street, postal_code, city) -> tuple[AugmentedQuery, ...]` (candidate rewrites; empty
   tuple = no idea for this address). Iterated by editing this module only.

2. **Lab harness (fast loop, local, no deploy)** — a script/pytest-style harness that:
   - pulls ONLY the unmatched pool (latest servable outcome per identity = unmatched; read-only
     prod SELECT) and the current OSM reference snapshot (reuse the weekly's DuckDB artifact or
     rebuild locally from the Geofabrik download);
   - runs the REAL resolver engine (`defs/address_resolution`) with augmentation applied to the
     query side — same scoring, same policy thresholds, no simplified copy;
   - reports: would-match counts by outcome class, confidence histogram, N samples per class for
     eyeballing. Writes nothing anywhere.

3. **Cache-on-full-match path** — the "cache table" IS the store `se_address_geocodes`
   (legacy-adoption precedent, policy label `lab_augmentation_v1`):
   - only EXACT full matches (score 1.0: street+house+postcode) are eligible; everything else is
     preview-only;
   - preview-first, execute-gated Dagster job (adoption pattern: bare materialization previews,
     `execute: true` appends); append rows carry is_adopted-family provenance so the versioned
     read serves them, and any later real policy version that geocodes the same address OUTRANKS
     them (existing two-stage read semantics — no cleanup ever needed);
   - writer invariant preserved: appends never back-dated.

4. **Graduation** — a proven augmentation class moves into the resolver as a proper policy bump
   (golden-corpus gated, full rematch), and its lab rows retire by being outranked. The lab is an
   experimentation fast path, never a second permanent matcher.

## Constraints
- The two-stage versioned read in `geocode_store.py` is the single source of truth; the lab never
  adds read paths.
- Terminal-first: no backoffice surface in this iteration.
- Exact-only into the store; the golden corpus is NOT bypassed for policy changes — only for
  lab-labeled adoptions, which are individually exact and outrankable.
