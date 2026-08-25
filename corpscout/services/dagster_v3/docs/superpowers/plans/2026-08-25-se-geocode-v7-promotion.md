# SE Geocode v7 Promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Promote the empirically-validated `g8_v7_recommended` matcher variant to production: strictly-additive exact-only street variants (punctuated v6 suffixes + separate-word definite forms) and the `se-address-resolution-policy-v6` → `-v7` version bump that triggers the store-driven rematch.

**Architecture:** v6 behavior is untouched — parsed/libpostal/glued-`suffix_expansion` variants stay fuzzy-eligible. A new variant kind `suffix_exact` (rank 3) carries the additive variants; it participates in the EXACT match paths (which gate on `!= 'parsed'`) but is excluded from fuzzy postings at the single choke point `resolution.py:_replace_fuzzy_street_postings`. The existing dedup (`qualify row_number() ... order by variant_rank`) already guarantees a v6 form wins any collision with a `suffix_exact` duplicate, making v7 a strict superset of v6. The policy-version bump makes `geocode_demand`'s pending rule mark all ~2.09M identities `policy_changed`, routing the full rematch through the golden gate exactly as the v5→v6 rematch (`dce200db`) did.

**Evidence (2026-08-25 exploration, /tmp/v7_run4.log, real matcher engine):** control pool 49,461 matched, yield pool 20,513 abbreviation-scoped unmatched. `g8_v7_recommended`: **+1,909 new, 0 lost, 0 regressions → ACCEPT**. Candidates with fuzzy-eligible additions regressed (1 control flip each: `strandbergsg.` matched_corrected→ambiguous); extra abbreviations (gg/all/stg/tg/…) confirmed harmful and are EXCLUDED from v7.

**Tech Stack:** DuckDB matcher engine (`defs/address_resolution/`), Sweden policy wiring (`defs/sweden_company/`), Dagster weekly job `sweden_company_address_geocoding_weekly_job`, ClickHouse store `se_address_geocodes` (append-only, keyed `(address_id, policy_version, reference_md5)`).

**Spec:** This plan + the exploration driver `se-geocode-v7-exploration:corpscout/services/dagster_v3/scripts/geocode_v7_exploration.py` (the `g8_v7_recommended` candidate + `build_additive_variants` define the exact semantics; read it via `git show`). Background: `docs/superpowers/specs/2026-08-25-se-geocode-workbench-architecture.md`, ledger `.superpowers/sdd/2026-08-24-se-geocode-simplification/progress.md`.

## Global Constraints

- `uv run` for everything from `corpscout/services/dagster_v3`; `uv run dg check defs` and `uv run ruff check src/dagster_v3/defs` clean before each commit.
- No `from __future__ import annotations` in Dagster-asset modules.
- Commits by explicit path (shared tree carries other sessions' WIP), Conventional Commits, trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- TDD: failing test first, run to see the stated failure, then implement.
- **The engine module (`defs/address_resolution/`) is shared**: every change is additive and inert unless the new optional maps are passed — no behavior change for any caller that does not opt in.
- **No policy-knob changes**: `minimum_fuzzy_street_length=6`, `maximum_street_edit_distance=1`, score ladder — all unchanged. v7 = variants + version string only.
- **NO new migrations** — the store schema already versions by `policy_version`.
- Production steps (deploy, rematch launch) are controller-only (Task 5).

## Key facts from recon (file:line as of main 775b1301)

- Variant kinds today: `parsed`/0, `libpostal_expansion`/1 (`search_documents.py:52-53`), `suffix_expansion`/2 (`:54-55`); column contract `STREET_VARIANT_COLUMNS` `:41-50`; dedup prefers lower rank `:360-367`.
- `expanded_street_suffix_variants` (`:178-209`): glued last-token, longest-first, stem ≥3 (`MINIMUM_GLUED_SUFFIX_STEM_LENGTH`, `:60`), case-preserving, ≤1 variant. Punctuated map keys (`"v."`) work through this same function (the abbreviation match is longest-first suffix match on the last token).
- Fuzzy choke point: `_replace_fuzzy_street_postings` (`resolution.py:558-595`), query side only — add `street_variant_kind != 'suffix_exact'` to the WHERE at `:588-594`. Exact paths gate on `!= 'parsed'` (`:92`, `:114`, `:331`, `:336`, `:356`) and must INCLUDE the new kind (no change needed).
- Sweden policy: `sweden_company/address_resolution_policy.py` — `SWEDEN_STREET_SUFFIX_EXPANSIONS` `:21-27` = `{gr→gränd, v→vägen, g→gatan}`; `SWEDEN_ADDRESS_RESOLUTION_POLICY.version = "se-address-resolution-policy-v6"` `:31` (the ONLY version constant; demand/promotion/store all read `.version`).
- Single production suffix-map call site: `address_resolution_shadow.py:97-103`.
- Pending rule: `geocode_demand.py:97-98` (Python twin) and `:215` (SQL) — `policy_version != current → 'policy_changed'`.
- Tests pinning literals: `tests/test_address_resolution.py:172` (exact `(variant_kind, variant_rank)` tuples), `:109`, `:293`; shadow fixture `:36-38` passes policy + maps.

## Task 1: Engine — `suffix_exact` variant kind + additive emission

**Files:**
- Modify: `src/dagster_v3/defs/address_resolution/search_documents.py`
- Test: `tests/test_address_resolution.py`

**Interfaces produced (later tasks rely on these exact names):**
- `SUFFIX_EXACT_VARIANT_KIND = "suffix_exact"`, `SUFFIX_EXACT_VARIANT_RANK = 3` (module constants next to the existing kind constants).
- `separate_definite_variant(street_name: str, separate_map: Mapping[str, str]) -> str | None` — pure function: if the LAST whitespace token (lowercased) is a key in `separate_map`, return the street with that token replaced by the mapped definite form, uppercased whole-word iff the original token `isupper()`; else `None`. Never returns the input street.
- `replace_address_street_variants(..., exact_suffix_expansions_by_country: Mapping[str, Mapping[str, str]] | None = None, separate_definite_by_country: Mapping[str, Mapping[str, str]] | None = None)` — two NEW keyword-only optional params, default `None` (inert). When provided, emit per-document additional rows with `variant_kind='suffix_exact'`, `variant_rank=3`, computed per distinct `(country_code, street_name)` as the ADDITIVE set: `{expanded_street_suffix_variants(street, exact_map[cc])} ∪ {separate_definite_variant(street, separate_map[cc])}` MINUS the variants v6 already produced for that street via `expanded_street_suffix_variants(street, suffix_expansions_by_country[cc])` MINUS `{street}` — mirroring `build_additive_variants` in the exploration driver. Empty/None results skipped. Emission mirrors the existing `suffix_expansion` emission block (`:279-285`) including normalized-variant computation and the deletion-signature rebuild; the existing dedup (`:360-367`) needs no change (rank 3 loses collisions by construction).

- [ ] **Step 1: Write the failing tests** (in `tests/test_address_resolution.py`, following the file's existing fixture style at `:172`):

```python
def test_separate_definite_variant_expands_last_token_case_preserving():
    from dagster_v3.defs.address_resolution.search_documents import separate_definite_variant
    m = {"väg": "vägen", "gata": "gatan"}
    assert separate_definite_variant("Norra Villa Väg", m) == "Norra Villa Vägen"
    assert separate_definite_variant("NORRA VILLA VÄG", m) == "NORRA VILLA VÄGEN"
    assert separate_definite_variant("Norra Villavägen", m) is None
    assert separate_definite_variant("", m) is None

def test_exact_suffix_variants_are_additive_and_tagged_suffix_exact(...):
    # Build a variant table with BOTH the v6 maps AND the new exact maps for a doc
    # with street "Villav." (punctuated, v6 cannot expand it) and one with
    # "Norra Villa Väg" (separate-word). Assert:
    #  - rows ('VILLAVÄGEN', 'suffix_exact', 3) and ('NORRA VILLA VÄGEN', 'suffix_exact', 3) exist
    #  - every v6 row from the same input WITHOUT the new maps is still present, byte-identical
    #    (strict-superset property)
    #  - a street the v6 glued map already expands (e.g. 'Stavstensv') gains NO suffix_exact
    #    duplicate of its v6 expansion (additive-minus-v6)
    ...

def test_exact_suffix_variants_absent_when_maps_not_passed(...):
    # Call with the new params omitted; assert zero rows with variant_kind='suffix_exact'
    # and the full variant table identical to before this change (inertness).
    ...
```

(The `...` test bodies are written against the file's existing duckdb-fixture helpers at `tests/test_address_resolution.py:109-291` — reuse the same table-construction helpers those tests use; the pinned-literal test at `:172` stays UNTOUCHED in this task because the new params are not passed there.)

- [ ] **Step 2:** Run: `uv run pytest tests/test_address_resolution.py -k "separate_definite or suffix_exact" -v` → FAIL (names not defined).
- [ ] **Step 3:** Implement the constants, `separate_definite_variant`, and the additive emission in `replace_address_street_variants` per the interface above, mirroring the adjacent `suffix_expansion` emission code path.
- [ ] **Step 4:** `uv run pytest tests/test_address_resolution.py -v` → ALL pass (including the untouched pinned-literal tests — proof of inertness).
- [ ] **Step 5:** `uv run ruff check src/dagster_v3/defs && uv run dg check defs` → clean. Commit: `feat(address-resolution): additive exact-only street variant tier (suffix_exact)`

## Task 2: Engine — exclude `suffix_exact` from fuzzy postings

**Files:**
- Modify: `src/dagster_v3/defs/address_resolution/resolution.py:558-595` (`_replace_fuzzy_street_postings`)
- Test: `tests/test_address_resolution.py`

**Interfaces:** Consumes `SUFFIX_EXACT_VARIANT_KIND` from Task 1. Produces: fuzzy postings never contain a `suffix_exact` variant; exact paths still do.

- [ ] **Step 1: Write the failing test**: build a resolution run (reuse the shadow-style fixtures at `tests/test_address_resolution.py:310+`) where a `suffix_exact` variant is 1 edit away from a WRONG reference street (the `strandbergsg.` failure shape: exact target absent, near-miss present). Assert the result is NOT matched via fuzzy on the exact-only variant (stays unmatched or matches only via its own exact path), and a control case where the same street's v6 `suffix_expansion` variant still fuzzy-matches as before.
- [ ] **Step 2:** Run it → FAIL (the exact-only variant currently enters fuzzy_pairs and matches).
- [ ] **Step 3:** Add `and street_variant_kind != '{SUFFIX_EXACT_VARIANT_KIND}'` to the query-side WHERE in `_replace_fuzzy_street_postings` (`reference_documents=False` branch only — the reference side is hardcoded `'parsed'`).
- [ ] **Step 4:** `uv run pytest tests/test_address_resolution.py -v` → PASS.
- [ ] **Step 5:** Commit: `feat(address-resolution): suffix_exact variants are exact-only (excluded from fuzzy postings)`

## Task 3: Sweden policy v7 — maps, version bump, shadow wiring

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/address_resolution_policy.py`, `src/dagster_v3/defs/sweden_company/address_resolution_shadow.py:97-103`
- Test: `tests/test_address_resolution.py`, `tests/test_sweden_geocode_demand.py` (version literals only if any pin the string)

**Interfaces produced:**

```python
# address_resolution_policy.py — derive punctuated from glued so they can never drift:
SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS: dict[str, dict[str, str]] = {
    country: {f"{abbreviation}.": expansion for abbreviation, expansion in glued.items()}
    for country, glued in SWEDEN_STREET_SUFFIX_EXPANSIONS.items()
}
SWEDEN_SEPARATE_DEFINITE_EXPANSIONS: dict[str, dict[str, str]] = {
    "SE": {
        "väg": "vägen", "gata": "gatan", "torg": "torget", "allé": "allén",
        "backe": "backen", "gränd": "gränden", "plan": "planen", "stig": "stigen",
        "led": "leden", "gång": "gången", "park": "parken",
    }
}
```

plus `SWEDEN_ADDRESS_RESOLUTION_POLICY.version = "se-address-resolution-policy-v7"`, and a comment block recording the 2026-08-25 evidence (+1,909/0/0; fuzzy-eligible punctuated and extra abbreviations rejected — extend the existing measured-rejections comment at `:7-20`, do not replace it).

Shadow wiring: pass `exact_suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS, separate_definite_by_country=SWEDEN_SEPARATE_DEFINITE_EXPANSIONS` at the single call site `address_resolution_shadow.py:97-103`.

- [ ] **Step 1: Failing test:** assert `SWEDEN_ADDRESS_RESOLUTION_POLICY.version == "se-address-resolution-policy-v7"`, the derived punctuated map equals `{"SE": {"gr.": "gränd", "v.": "vägen", "g.": "gatan"}}`, and a shadow-fixture run (the `:310+` harness) on a punctuated street (`villav.` → expects `Villavägen`) now matches with status `matched_corrected`.
- [ ] **Step 2:** Run → FAIL. **Step 3:** implement. **Step 4:** `uv run pytest tests/test_address_resolution.py tests/test_sweden_geocode_demand.py tests/test_sweden_geocode_store_append.py -v` → PASS (version propagates via `.version` reads; fix any test that pinned the literal v6 string — recon says most bind `.version` dynamically).
- [ ] **Step 5:** Commit: `feat(se-geocode): promote v7 — punctuated + separate-definite exact-only variants, policy v7`

## Task 4: Golden gate + full sweep

- [ ] **Step 1:** `uv run pytest tests/test_address_resolution.py::test_sweden_golden_address_resolution_corpus -v`. The golden corpus (`sweden-address-resolution-golden-v5`) runs the REAL matcher: if v7 changes any golden outcome, STOP and report the diff to the controller — an outcome change here is owner-visible evidence, not a fixture to silently regenerate (expected: no change; v7 additions only touch abbreviation streets the corpus may not contain).
- [ ] **Step 2:** Full sweep: `uv run pytest tests/test_address_resolution.py tests/test_sweden_geocode_demand.py tests/test_sweden_geocode_store.py tests/test_sweden_geocode_store_append.py tests/test_sweden_geocode_checks.py -v` and `uv run dg check defs` → all green.
- [ ] **Step 3:** Commit any test-literal fixes: `test(se-geocode): align pinned literals with v7`

## Task 5 (controller-only): merge, deploy, rematch

- [ ] **Step 1:** Merge the v7 branch → main (fast-forward or merge commit; run the Task 4 sweep once on merged main).
- [ ] **Step 2:** Deploy dagster via the established pristine-worktree light_sync (worktree at merged main; `uv sync`, `dbt parse` per dbt project, `dg check defs`, then ansible light_sync). This deploy also ships the already-merged OSM-CH revert, closing the mirror-check inconsistency BEFORE the Tuesday 04:05 weekly.
- [ ] **Step 3:** Launch the weekly job manually (`sweden_company_address_geocoding_weekly_job`, no special config — the version bump makes demand mark everything `policy_changed`; `rematch_all` not needed since the chunked demand load is deployed). Monitor: demand scan seconds-not-minutes; golden gate passes; shadow → promotion → store append.
- [ ] **Step 4:** Post-verify against v6 baseline (geocoded 1,129,204 / unmatched 492,637 after dce200db): expect geocoded to rise by roughly the exploration's yield (~+1.9k scoped, plausibly more over the full unmatched pool), unmatched to fall accordingly, ambiguous ~flat, and ZERO servable v6→v7 regressions via the store comparison (`se_address_geocodes` keeps v6 rows beside v7 — compare per address_id). The `sweden_company_address_exact_match_rate_check` must stay green.
- [ ] **Step 5:** Record outcome in this plan + ledger; update memory.
