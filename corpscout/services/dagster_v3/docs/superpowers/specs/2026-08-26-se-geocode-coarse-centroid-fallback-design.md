# SE geocoding: coarse-centroid fallback tier (v8) — design

Design doc. Owner-driven (2026-08-26). A NEW precision tier: for addresses the precise matcher
(v6/v7) leaves unmatched/ambiguous but which carry a postcode or city, assign a COARSE area
coordinate — honestly labeled by precision, never conflated with a building match. Measured to
rescue 98.5% of the residual unmatched. This does NOT change the precise matcher; it adds a
fallback that fires only where precise matching fails.

## Motivation (measured, not assumed)
The v7 exploration converged: the residual ~490k unmatched is dominated by streets ABSENT from OSM
(a coverage gap no matcher fixes — e.g. STAVSTENSV 3, postcode 23100, whose street isn't in OSM).
But those addresses still carry a valid postcode and/or city. The centroid-coverage experiment
(scripts/geocode_centroid_coverage_experiment.py) measured, on the 488,356 unmatched:
- 481,198 (98.53%) get a coarse coordinate — 196,454 at postcode precision, 284,744 at city
  precision, only 7,158 unreachable.
- STAVSTENSV 3 reproduces: postcode 23100 has 0 OSM points, but Trelleborg city (570 OSM points)
  yields a centroid ~600m from town center.
So the fallback turns "no location" into "coarse but useful location" for the coverage-gap tail —
the single largest remaining coverage win, and the only one precise matching cannot deliver.

## Non-negotiable principle: honest precision labeling
A postcode/city centroid is an AREA coordinate, categorically different from a building match. It
MUST carry an explicit precision (`postcode` / `locality`|`city`) and a distinct provenance
(geocode_provider='centroid_fallback'), and it MUST be RANKED BELOW any precise match — a precise
building match always wins; the centroid fires only for unmatched/ambiguous. Consumers (serving
view, backoffice, exports) surface the precision so a coarse coord is never mistaken for a building
geocode. This labeling is what makes the tier safe to add.

## The centroid ladder (finest available wins)
1. Postcode centroid — robust centroid of OSM address points in that postcode. Tight (usually a few
   hundred m). Fails the worst gaps (a postcode with 0 OSM points, like 23100).
2. City centroid — robust centroid of OSM address points in that city (post_town). Broader; catches
   the postcode-gap cases (23100 → Trelleborg). Coarser spread (km-scale), labeled accordingly.
3. (skip) External postcode dataset — the experiment showed the internal-source residual is tiny and
   dominated by garbage `invalid_address` rows, not a real gap. Not worth a new external dependency.

## Quality bar (from the experiment's findings)
- Require N>=3 OSM points behind a centroid; report/enforce a coordinate-spread threshold (a postcode
  with a km-scale spread is unreliable — either downgrade to city or drop).
- Use a ROBUST centroid (median / geometric-median style), NOT a plain mean — ~0.8% of postcode
  centroids were skewed by a single outlier OSM point; robust aggregation neutralizes it.
- Prefer matched-geocode coordinates (from se_address_geocodes) over raw OSM points as the city-source
  where available (cleaner sample).
- Scope: fire ONLY for match_status IN ('unmatched','ambiguous'). Never override postal_box /
  invalid_address / foreign_address / property_identifier (structural non-addresses).

## PREREQUISITE bug fix (blocks the tier)
`normalized_post_town` strips Swedish diacritics under ClickHouse's ASCII-only regex
(`'GÖTEBORG' -> 'g teborg'`), so a city-name join matches only ~16% of distinct town strings. This is
a latent data-quality bug in the canonical addresses (anything joining on normalized city hits it),
not just a fallback problem. Fix the normalization to preserve Swedish letters (å ä ö) before the
city-centroid join can work. The experiment routed around it with a local `upper(trim(post_town))`
key; production needs the real fix.

## Correctness (wrong-AREA risk, different from wrong-building)
The failure mode is assigning a coordinate in the WRONG area, not the wrong building. Guard by:
- Ambiguous city names: distinct real places sharing a post_town string → a city centroid would
  average across towns. Detect and either disambiguate by postcode region or withhold the city
  centroid for ambiguous names.
- Validate a sample lands in the right area (the experiment's 15-address eyeball + the Trelleborg
  reproduction); add an automated check that a rescued coord is within a sane distance of the
  postcode/city it claims.

## Architecture — the ONE key open decision
Two placements; the spec recommends deciding this in planning:
- **A. Centroid reference table + serving overlay (recommended).** A Dagster asset maintains a CH
  table `se_postcode_centroids` / `se_city_centroids` (key -> robust coord + point_count + spread +
  precision), refreshed on the OSM/matched-data clock. The serving layer (the se_address_geocodes_current
  MV or a sibling view, and the backoffice) LEFT JOINs it to fill unmatched/ambiguous identities with
  a coarse coord + precision at read time. Pro: the geocode STORE stays purely precise-match outcomes
  (no ranking entanglement); the fallback evolves independently; cheap to change thresholds.
- **B. Post-resolution store append.** A Dagster asset writes fallback outcomes into se_address_geocodes
  (match_status e.g. 'matched_area', precision, provider='centroid_fallback') for unmatched identities,
  ranked below precise matches by the versioned read. Pro: one unified store. Con: mixes coarse + precise
  in the store and complicates the versioned-read ranking (must guarantee a later precise match always
  outranks an earlier centroid).
Recommendation: A (serving overlay) — keeps the store's meaning clean (precise matches only) and matches
the "store precise outcomes; derive coarse at read" separation. Decide in planning.

## Scope
- SE first (the centroid derivation + city keys are SE-specific); the pattern generalizes per-country.
- unmatched/ambiguous identities only.
- Precision bands: postcode, city. (No street-level here — that's the matcher's job.)

## Out of scope
- Changing the precise matcher (v6/v7) or the store schema for precise outcomes.
- External postcode-centroid datasets (measured unnecessary).
- Multi-country rollout (design SE; generalize later).

## Next step
Planning (writing-plans) → tasks → build, in order: (1) the diacritic normalization fix (prerequisite),
(2) the centroid reference asset(s) with the quality bar, (3) the serving overlay + precision surfacing,
(4) the correctness checks. Ship behind the precision label so a coarse coord is always distinguishable.
