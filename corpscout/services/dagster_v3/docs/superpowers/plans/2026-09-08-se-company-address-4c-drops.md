# SE Company Address Slice 4c: Drops and Ledger Cleanup — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the last code that reads the old SE address chain, take the dropped objects' DDL out of the historical migration files under the dev-phase ledger policy, and hand the owner one committed script that drops the twelve remaining ClickHouse objects in dependency order — so nothing in the repo or the database is left of the address model that shipped on 2026-08-24.

**Architecture:** Four code deletions and one owner-run script. First the two modules that still name a dropped object go: the one-off `se_address_geocodes_adopt_keys` (it reads `se_addresses_current`) and `sweden_company/geocode_serving_overlay.py` (it *is* the `se_address_geocodes_served` view), after the centroid-fallback policy constants the new chain imports from the overlay move into the address package as `se_company/address/constants.py`. Then a keep-rule pass takes the library modules that survived slice 4b only because something still imported them — `geocode_demand.py`, `shared_addresses.py`, `address_canonicalization.py`, `address_parsing.py`, the now-unrunnable shadow driver inside `address_resolution_shadow.py`, and the store-side helpers in `geocode_store.py` that only deleted code used. Then the migration files: sixteen files are emptied to `CREATE DATABASE IF NOT EXISTS corpscout;` under the 2026-09-03 precedent and two lose one statement each, with the tests that pinned that DDL deleted or narrowed and three keeper tests re-homed. Finally three SQL scripts (precheck, drops, postcheck) in `corpscout/clickhouse/operations/`, with a test that pins the drop order and asserts no kept object is named. **No numbered migration drops anything**: the owner runs the drop script by hand (ledger policy, owner ruling 2026-08-25).

**Tech Stack:** Python 3.14 / Dagster 1.13.9 (`uv run --frozen --no-sync`), ClickHouse 26.5 (golang-migrate ledger, files only in this slice), dbt (`company_serving`, `company_domain_suggestions` — untouched here), React Router v7 backoffice (comment-only edits).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md`, section 9 (the Retirement paragraph and the slice-4b shipped record, which carries the ordered drop list) and section 10 (names). Predecessor plan: `2026-09-08-se-company-address-4b-retire.md` (its Task 8 step 8 is this slice's handoff and its Global Constraints apply verbatim).

## Global Constraints

- Dagster commands from `corpscout/services/dagster_v3` with `uv run --frozen --no-sync ...` and `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix`; `uv run --frozen --no-sync dg check defs` before every commit touching `src/`, and **after every module deletion inside a task**, not only at its end.
- Backoffice commands from `corpscout/services/backoffice`: `npx vitest run <files>` and `npm run typecheck`.
- **The entity's table name is a prefix of five others.** `corpscout.se_company_address` is a prefix of `_suggestion`, `_normalized`, `_history`, `_rule`, `_precedence` — and of `se_company_address_legacy`, `se_company_address_scb`, `se_company_address_bolagsverket`, `se_company_address_correction`, `se_company_addresses`. Every string match on a table name — a test assertion, an `rg` check, the drop-script guard test — must compare whole names (split the statement and compare tokens, or anchor with `([^_a-zA-Z0-9]|$)`), never a bare `in` on the qualified string. A guard written as `"se_company_address" not in script` would fire on `se_company_address_legacy` and a guard written as `"se_company_address" in kept` would pass on it.
- **Nothing in this slice executes DDL against a server.** The drops are hand-run by the owner from a committed script (ledger policy, memory `clickhouse-ledger-squash-planned`; the session's command classifier blocks bulk DROPs). No numbered migration is added, and `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py` is **unchanged** — only `EMPTIED_MIGRATIONS` grows.
- **Editing a historical migration file is only legal when the object is gone from the server.** The file edits and the drops therefore land in this order on prod: merge → deploy dagster → precheck → owner runs the drops. The repo may sit for a short while with the DDL removed and the objects still on the server; that is harmless because nothing replays the ledger on prod (it is forward-only, at 393).
- **Every edited migration file must still satisfy the four file guards** (`tests/test_clickhouse_migrations.py`): the up file creates/alters/drops something *or* its name is in `EMPTIED_MIGRATIONS`; the down file undoes something *or* its name is in `EMPTIED_MIGRATIONS`; no `;` inside a `--` comment anywhere under `corpscout/clickhouse/migrations`; every file ends with a statement, not prose. An emptied file's *only* statement line must be exactly `CREATE DATABASE IF NOT EXISTS corpscout;` — `_statement_lines()` strips comment lines and blank lines and compares the rest to that one-element list.
- **A fresh full replay of the whole ledger was already broken before this slice, and a replay failure is not this slice's regression.** 000298 grants `INSERT ON corpscout.se_company_info_correction` although 000297 (emptied on 2026-09-03) no longer creates it, and 000326/000335/000338/000344/000347 create views whose bodies read `se_address_geocodes_served`, which after this slice no migration creates. Production is forward-only at ledger 393 and nothing replays it; the requirement on an edited file is that it still **parses** as a whole, which is exactly what `tests/test_clickhouse_migrations.py` checks by reading every file.
- **Leave `000391`, `000392` and `000393` untouched.** 000392's first statement, `DROP TABLE IF EXISTS corpscout.se_companies_serving_retired`, may stay: it is harmless and it is the history of the swap that parked that view.
- **Leave `000308` (the correction-table writer grant) untouched.** House precedent: `000298_corpscout_se_company_info_writer_grants` still grants `INSERT ON corpscout.se_company_info_correction` although 000297 no longer creates that table and 000372 dropped it. Access migrations are not emptied when their target leaves.
- **Kept for good — never name any of these in a drop script, and never delete their code:** `corpscout.se_address_geocodes` (the entity's geocode cache), `corpscout.se_postcode_centroids`, `corpscout.se_city_centroids`, the matcher (`defs/address_resolution/`, `defs/sweden_address_osm/`), `sweden_company/address_resolution_policy.py`, the reference-document and posting builders in `address_resolution_shadow.py` (`replace_reference_documents`, `reference_documents_md5`, `reference_documents_built_at`, `ensure_reference_documents`, `replace_reference_postings`, `reference_postings_key`, `ensure_reference_postings`, `INDEX_SCOPE`, `QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE`, `QUALIFIED_REFERENCE_POSTINGS_TABLE`, and the private `_replace_building_reference_documents` / `_replace_street_reference_documents` / `_replace_address_point_street_inputs` / `_replace_road_search_documents` / `_replace_road_street_inputs` / `_spread_sql` / `_log` they call), `geocode_store.py` minus the four helpers Task 2 names, `centroid_keys.py`, `centroid_assets.py`, the OSM workbench.
- **Another session merges to main daily** on branch `se-basic-info-6-economic-activity`, and the main checkout may be on that branch. Merge through a worktree that checks main out (memory `se-worktree-deploy-recipe`). Nothing here takes a migration number, so no renumber is possible — but re-run `pytest tests/test_clickhouse_migrations.py -q` after the merge in case the other branch added a migration that names one of the dropped objects.
- Never `from __future__ import annotations` in a module that defines Dagster assets.
- Delete a Python module only when `rg` shows no importer outside the deleted set; otherwise move the imported symbol to the importer's side and then delete.
- Commit by explicit path only; trailers in this order at the end of every commit message:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`
- Pre-existing unrelated failures, not regressions of this slice: `tests/test_se_company_address_extractors_clickhouse_local.py` (broken since main's 000390 — the fixture lacks `se_ratsit_company_translated`), `tests/test_schedule_cron_contracts.py` (four `(minute, hour)` collisions), `tests/test_sweden_address_geocoding.py::test_lantmateriet_credentials_are_documented_without_values`; backoffice `admin-se-company-esef.test.tsx` and the live-ClickHouse timeouts under the full suite.

## The drop list, in order

Each object is dropped after everything that reads it. `se_address_geocodes_served` LEFT JOINs `se_addresses_current`, so it goes first of that trio; `se_address_geocodes_current` is the view's precise base, so it goes last.

| # | object | statement | why it can go |
| --- | --- | --- | --- |
| 1 | `se_companies_serving_retired` | `DROP TABLE IF EXISTS` | 000391's serving render, parked by 000392's RENAME; the live view is the address-entity render |
| 2 | `se_company_address_legacy` | `DROP TABLE IF EXISTS` | the old final table, renamed aside by 000393; no reader since slice 4a |
| 3 | `se_company_address_scb` | `DROP TABLE IF EXISTS` | its writer left in 4b |
| 4 | `se_company_address_bolagsverket` | `DROP TABLE IF EXISTS` | its writer left in 4b |
| 5 | `se_company_address_correction` | `DROP TABLE IF EXISTS` | the queue, the sensor and the writer left in 4b |
| 6 | `se_company_addresses` | `DROP TABLE IF EXISTS` | the register-address publish left in 4b |
| 7 | `se_company_addresses_current` | `DROP TABLE IF EXISTS` | same publish; last reader (`countries.ts`) switched in 4b |
| 8 | `se_company_address_members_current` | `DROP TABLE IF EXISTS` | the canonical publish left in 4b |
| 9 | `se_address_geocodes_served` | `DROP VIEW IF EXISTS` | a plain VIEW (000325/000327); its last reader is the module Task 1 deletes |
| 10 | `se_addresses_current` | `DROP TABLE IF EXISTS` | read only by #9 and by the adoption asset Task 1 deletes |
| 11 | `se_company_address_links_current` | `DROP TABLE IF EXISTS` | its writer and its last check left in 4b |
| 12 | `se_address_geocodes_current` | `DROP TABLE IF EXISTS` | a refreshable MATERIALIZED VIEW since 000320 — `DROP TABLE` is the statement 000392 already used successfully on a refreshable MV (`se_companies_serving_retired`); confirm with `system.tables.engine` in the precheck |

Already gone, so **not** in the script: `se_company_addresses_canonical_current` (`CANONICAL_RETIREMENT_DROP_SQL` ran), `se_address_geocodes_current_retired` (verified absent on prod 2026-09-08), `se_company_address_geocodes` / `se_company_address_geocode_results` (`LEGACY_PAIR_RETIREMENT_DROP_SQL` ran).

## File structure

| file | change |
| --- | --- |
| `src/dagster_v3/defs/se_company/address/constants.py` | **create** — the centroid-fallback policy constants, moved off the overlay |
| `src/dagster_v3/defs/se_company/address/adoption.py` | **delete** — the one-off, ran 2026-09-06 |
| `src/dagster_v3/defs/sweden_company/geocode_serving_overlay.py` | **delete** — the served view's builder |
| `src/dagster_v3/defs/sweden_company/geocode_demand.py` | **delete** — after `fresh_reference_md5` moves into the shadow module |
| `src/dagster_v3/defs/sweden_company/shared_addresses.py` | **delete** — last importer goes with the shadow driver |
| `src/dagster_v3/defs/sweden_company/address_canonicalization.py` | **delete** — last importers go |
| `src/dagster_v3/defs/sweden_company/address_parsing.py` | **delete** — only `address_canonicalization` imported it |
| `src/dagster_v3/defs/sweden_company/address_resolution_shadow.py` | **trim** — the unrunnable shadow driver goes, the reference builders stay |
| `src/dagster_v3/defs/sweden_company/geocode_store.py` | **trim** — four helpers only deleted code used |
| `src/dagster_v3/defs/sweden_company/tables.py` | **trim** — the four `se_company_addresses*` constants |
| `src/dagster_v3/defs/se_company/address/{fold,warm,geocode}.py`, `sweden_company/companies_current.py` | import the moved constants |
| `corpscout/clickhouse/migrations/*` | 16 files emptied, 2 files lose one statement |
| `corpscout/clickhouse/operations/se_address_retirement_{precheck,drops,postcheck}.sql` | **create** — owner-run, beside the ledger they retire from |
| `tests/test_se_address_retirement_drops.py` | **create** — pins the order and the kept set |
| `tests/test_sweden_address_geocoding_weekly.py` | **create** — re-homes the weekly-shape guard |

---

### Task 1: The adoption asset and the served-overlay builder go; the fallback constants move into the address package

**Files:**
- Create: `src/dagster_v3/defs/se_company/address/constants.py`
- Delete: `src/dagster_v3/defs/se_company/address/adoption.py`, `src/dagster_v3/defs/sweden_company/geocode_serving_overlay.py`
- Delete: `tests/test_se_company_address_adoption.py`, `tests/test_geocode_serving_overlay.py`, `tests/test_geocode_serving_overlay_base_sql.py`, `tests/test_se_address_geocodes_served_view.py`
- Modify: `src/dagster_v3/defs/se_company/address/fold.py:25`, `src/dagster_v3/defs/se_company/address/warm.py:18`, `src/dagster_v3/defs/se_company/address/geocode.py:39,72,267,274-307,820-851`, `src/dagster_v3/defs/sweden_company/companies_current.py:69-71`
- Modify: `tests/test_se_company_address_warm.py:14`
- Modify: `src/dagster_v3/defs/se_company/address/docs/address-design.md:14,179,199,204`
- Modify: `../backoffice/app/lib/se-company-geocoding-filters.ts:23-24`, `../backoffice/app/components/admin/se-company-geocoding-table.tsx:159`

**Interfaces:**
- Produces: module `dagster_v3.defs.se_company.address.constants` exporting `POSTCODE_CENTROIDS_TABLE: str`, `CITY_CENTROIDS_TABLE: str`, `FALLBACK_ELIGIBLE_STATUSES: tuple[str, ...]`, `POSTCODE_SPREAD_MAX_METERS: float`, `GEOCODE_FALLBACK_PROVIDER: str`, `GEOCODE_FALLBACK_COORDINATE_METHOD: str`, `POSTCODE_PRECISION: str`, `CITY_PRECISION: str`, `GEOCODE_FALLBACK_STATUS: str`. Every value is byte-identical to the overlay's.
- Consumes: `dagster_v3.defs.sweden_company.geocode_store.GEOCODED_STATUSES` (for the membership assert) and `dagster_v3.defs.se_company.address.tables.DATABASE`.

- [ ] **Step 1: Write the failing pins**

Append to `tests/test_se_company_address_geocode.py`:

```python
def test_the_fallback_policy_constants_live_in_the_address_package() -> None:
    """Slice 4c: geocode_serving_overlay.py went with the served view it built. The
    centroid-fallback policy it also held is the address entity's, so it moved here --
    same values, one importable place, no dependency on the retired module."""
    from dagster_v3.defs.se_company.address import constants

    assert constants.FALLBACK_ELIGIBLE_STATUSES == ("unmatched", "ambiguous", "postal_box")
    assert constants.POSTCODE_SPREAD_MAX_METERS == 3000.0
    assert constants.GEOCODE_FALLBACK_PROVIDER == "centroid_fallback"
    assert constants.GEOCODE_FALLBACK_COORDINATE_METHOD == "centroid_median"
    assert constants.POSTCODE_PRECISION == "postcode"
    assert constants.CITY_PRECISION == "city"
    assert constants.GEOCODE_FALLBACK_STATUS == "matched_area"
    assert constants.POSTCODE_CENTROIDS_TABLE == "corpscout.se_postcode_centroids"
    assert constants.CITY_CENTROIDS_TABLE == "corpscout.se_city_centroids"


def test_no_module_imports_the_retired_serving_overlay() -> None:
    """The served view is dropped in this slice, and its builder with it."""
    import subprocess
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    hits = subprocess.run(
        ["rg", "-l", "geocode_serving_overlay", "src", "tests"],
        cwd=root, capture_output=True, text=True,
    ).stdout.split()
    assert hits == [], hits


def test_the_one_off_adoption_asset_is_gone() -> None:
    """`se_address_geocodes_adopt_keys` ran once on 2026-09-06 (2,019,120 adopted) and
    read `se_addresses_current`, which slice 4c drops. Its history is the spec's."""
    from dagster_v3.definitions import defs as load_defs

    keys = {key.path[-1] for key in load_defs().get_repository_def().asset_graph.get_all_asset_keys()}
    assert "se_address_geocodes_adopt_keys" not in keys
    assert "se_address_geocodes_warm" in keys
```

- [ ] **Step 2: Run them and watch them fail**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix \
  uv run --frozen --no-sync pytest tests/test_se_company_address_geocode.py -q \
  -k "fallback_policy_constants or retired_serving_overlay or one_off_adoption"
```

Expected: three failures — `ModuleNotFoundError: dagster_v3.defs.se_company.address.constants`, a non-empty `rg` hit list, and `se_address_geocodes_adopt_keys` present in the graph.

- [ ] **Step 3: Write the constants module**

Create `src/dagster_v3/defs/se_company/address/constants.py`:

```python
"""The centroid-fallback policy the address entity's geocode function applies on the way out.

These values were `sweden_company/geocode_serving_overlay.py`'s until slice 4c (2026-09-08):
that module existed to build the `se_address_geocodes_served` VIEW (migrations 000325/000327),
which is dropped with the rest of the old chain. The POLICY it also held is not the view's --
`geocode.py` applies it per outcome, in Python, and stores nothing -- so it moves here.

It is a leaf on purpose. `fold.py`, `warm.py`, `geocode.py` and `sweden_company/companies_current.py`
all need `GEOCODE_FALLBACK_PROVIDER`, and `companies_current.py` is a pure SQL builder the
serving migration renders: it must not have to import `geocode.py`, which pulls in DuckDB, the
matcher and the OSM workbench for one string.
"""

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.sweden_company.geocode_store import GEOCODED_STATUSES

# The centroid reference tables (migrations 000323 / 000324). Spelled here rather than
# imported from centroid_assets.py to keep this module free of Dagster asset definitions.
POSTCODE_CENTROIDS_TABLE = f"{tables.DATABASE}.se_postcode_centroids"
CITY_CENTROIDS_TABLE = f"{tables.DATABASE}.se_city_centroids"

# The precise outcomes the fallback is allowed to fill. `postal_box` joined 2026-08: a box
# postcode is a dedicated range tied to a postal town, so the coarse centroid is exactly as
# honest for a box as for an unmatched street. Everything else -- geocoded, invalid_address,
# foreign_address, property_identifier -- passes through untouched.
FALLBACK_ELIGIBLE_STATUSES = ("unmatched", "ambiguous", "postal_box")

# A postcode centroid looser than this is demoted to the city centroid: past a few km a
# "postcode" centroid no longer means the postcode.
POSTCODE_SPREAD_MAX_METERS = 3000.0

GEOCODE_FALLBACK_PROVIDER = "centroid_fallback"
GEOCODE_FALLBACK_COORDINATE_METHOD = "centroid_median"
POSTCODE_PRECISION = "postcode"
CITY_PRECISION = "city"

# The status a fallback row reports. `matched_area` is the store's coarsest GEOCODED status,
# so a consumer filtering on GEOCODED_STATUSES still counts a rescued identity;
# `geocode_precision` ('postcode'/'city') is what marks it coarse. Asserted a member of the
# store vocabulary here so a status rename cannot silently make the fallback serve a status
# no consumer recognizes.
GEOCODE_FALLBACK_STATUS = "matched_area"
assert GEOCODE_FALLBACK_STATUS in GEOCODED_STATUSES
```

- [ ] **Step 4: Point the four importers at it**

In `src/dagster_v3/defs/se_company/address/fold.py` replace line 25:

```python
from dagster_v3.defs.se_company.address.constants import GEOCODE_FALLBACK_PROVIDER
```

In `src/dagster_v3/defs/se_company/address/warm.py` replace line 18 with the same line (keep the surrounding import order: it sorts before `from dagster_v3.defs.se_company.address.geocode import ...`, so move it to the top of that group).

In `src/dagster_v3/defs/sweden_company/companies_current.py` replace lines 69-71 with:

```python
from dagster_v3.defs.se_company.address.constants import GEOCODE_FALLBACK_PROVIDER
```

In `src/dagster_v3/defs/se_company/address/geocode.py` change line 72 to `from dagster_v3.defs.sweden_company import geocode_store`, add `from dagster_v3.defs.se_company.address import constants` above it, then rewrite the thirteen attribute reads:

```bash
cd corpscout/services/dagster_v3
sed -i '' 's/geocode_serving_overlay\./constants./g' \
  src/dagster_v3/defs/se_company/address/geocode.py
rg -n "geocode_serving_overlay" src/dagster_v3/defs/se_company/address/geocode.py
```

The `rg` leaves exactly two prose mentions, lines 39 and 267 (they have no trailing `.` so the `sed` skipped them). Rewrite both by hand: line 39 `... exactly as the served overlay did it for the shared-address chain (retired in slice 4c)`; line 267 `... keys are constants.py's, reused rather than restated: this is the same`.

- [ ] **Step 5: Delete the two modules and their four tests**

```bash
cd corpscout/services/dagster_v3
rm src/dagster_v3/defs/se_company/address/adoption.py \
   src/dagster_v3/defs/sweden_company/geocode_serving_overlay.py \
   tests/test_se_company_address_adoption.py \
   tests/test_geocode_serving_overlay.py \
   tests/test_geocode_serving_overlay_base_sql.py \
   tests/test_se_address_geocodes_served_view.py
```

Then re-point the one surviving test import — `tests/test_se_company_address_warm.py:14` becomes:

```python
from dagster_v3.defs.se_company.address.constants import GEOCODE_FALLBACK_PROVIDER
```

- [ ] **Step 6: Update the design doc and the two backoffice comments**

In `src/dagster_v3/defs/se_company/address/docs/address-design.md`:
- delete the `adoption.py` row of the module table (line 14);
- line 179, replace `The \`adopted:<old address_id>\` run-id prefix the adoption asset stamps is provenance, not a` with `The \`adopted:<old address_id>\` run-id prefix on the imported rows is provenance, not a`;
- line 199, drop `, and so do \`adoption.py\`'s two` and end the sentence at the settings;
- delete the paragraph at line 204 that describes `se_address_geocodes_adopt_keys` walking `se_addresses_current`, and put in its place: `The one-off \`se_address_geocodes_adopt_keys\` (slice 2a) copied 2,019,120 outcomes from the old identities onto location keys on 2026-09-06 and was deleted in slice 4c with \`se_addresses_current\`, the table it read. The adopted rows stay in \`se_address_geocodes\` under the \`legacy_adopted_v1\` family.`

In `../backoffice/app/lib/se-company-geocoding-filters.ts` lines 23-24, replace `see FALLBACK_ELIGIBLE_STATUSES in geocode_serving_overlay.py` with `see FALLBACK_ELIGIBLE_STATUSES in se_company/address/constants.py`, and make the sentence past tense about the view: `... but the served overlay (corpscout.se_address_geocodes_served, migrations 000325/000327, retired in slice 4c) filled it ... -- the entity now stores the same coarse outcome on the row`.

In `../backoffice/app/components/admin/se-company-geocoding-table.tsx:159`, replace `(from the served overlay, corpscout.se_address_geocodes_served)` with `(from the address entity's stored geocode outcome)`.

- [ ] **Step 7: Run everything this touched**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix \
  uv run --frozen --no-sync pytest \
  tests/test_se_company_address_geocode.py tests/test_se_company_address_warm.py \
  tests/test_se_company_address_fold.py tests/test_se_companies_serving_sql.py \
  tests/test_se_companies_serving_mv.py tests/test_se_companies_current_asset.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix \
  uv run --frozen --no-sync dg check defs
cd ../backoffice && npm run typecheck
```

Expected: all green, `dg check defs` reports the definitions load with no `se_address_geocodes_adopt_keys`.

- [ ] **Step 8: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/constants.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/fold.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/warm.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/geocode.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/adoption.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/geocode_serving_overlay.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/companies_current.py \
        corpscout/services/dagster_v3/tests/test_se_company_address_geocode.py \
        corpscout/services/dagster_v3/tests/test_se_company_address_warm.py \
        corpscout/services/dagster_v3/tests/test_se_company_address_adoption.py \
        corpscout/services/dagster_v3/tests/test_geocode_serving_overlay.py \
        corpscout/services/dagster_v3/tests/test_geocode_serving_overlay_base_sql.py \
        corpscout/services/dagster_v3/tests/test_se_address_geocodes_served_view.py \
        corpscout/services/backoffice/app/lib/se-company-geocoding-filters.ts \
        corpscout/services/backoffice/app/components/admin/se-company-geocoding-table.tsx
git commit -m "$(cat <<'EOF'
refactor(se-address): retire the served overlay and the adoption one-off

The centroid-fallback policy moves to se_company/address/constants.py so the
entity keeps it without the module that built the dropped view.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 2: The keep-rule pass over the old chain's library modules

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/address_resolution_shadow.py` (delete the shadow driver, take in `fresh_reference_md5`, drop the three dead imports)
- Delete: `src/dagster_v3/defs/sweden_company/geocode_demand.py`, `shared_addresses.py`, `address_canonicalization.py`, `address_parsing.py`
- Modify: `src/dagster_v3/defs/sweden_company/geocode_store.py` (four helpers out)
- Modify: `src/dagster_v3/defs/common/clickhouse_checks.py:207-213` (the stale "deliberate gap" note)
- Delete: `tests/test_sweden_geocode_demand.py`, `tests/test_sweden_geocode_demand_clickhouse_local.py`, `tests/test_sweden_company_address_canonicalization_clickhouse_local.py`, `tests/test_sweden_geocode_store_current_mv.py`
- Modify: `tests/test_address_resolution.py` (four shadow tests and two fixtures out), `tests/test_sweden_geocode_store.py` (four tests out), `tests/test_sweden_address_reference_documents.py:5` (docstring)

**Interfaces:**
- Consumes: Task 1's `se_company.address.constants` (unchanged here).
- Produces: `address_resolution_shadow.fresh_reference_md5(connection: Any) -> str` — same name, same body, same `ValueError` on a snapshot without an md5; `replace_reference_documents` and `ensure_reference_documents` call it as a module-local function instead of `geocode_demand.fresh_reference_md5`.

**Why the shadow driver goes** (controller ruling, 2026-09-08: confirmed — the shadow cannot run without the dropped tables, and the keep rule closes only if it goes; the reference-document and posting builders and `fresh_reference_md5` stay in the module): `replace_sweden_address_resolution_shadow` and `replace_sweden_address_resolution_unmatched_diagnostics` cannot run any more. Both start from `sweden_company_enrichment.se_address_pending_identities` (built only by `sweden_address_geocode_demand_duckdb`) and `sweden_company_enrichment.se_addresses_current` (built only by `sweden_shared_addresses_duckdb`) — **both assets were deleted in slice 4b**. Nothing in `src/` calls either function; only `tests/test_address_resolution.py` does, through fixtures that hand-build those two tables. They are the last importers of `geocode_demand.pending_identity_count`, `geocode_demand.QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE` and `shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE`, which is why the keep-rule cannot finish without them.

- [ ] **Step 1: Write the failing guard**

Append to `tests/test_sweden_address_reference_documents.py`:

```python
def test_the_reference_builders_stand_alone_after_the_old_chain_retired() -> None:
    """Slice 4c: the shadow DRIVER is gone (its two input tables were built by assets slice
    4b deleted), but the reference-document and posting builders the address entity's
    geocode function calls stay -- and they no longer reach into geocode_demand for the
    extract md5, because that module went with the demand scan."""
    from dagster_v3.defs.sweden_company import address_resolution_shadow as shadow

    assert callable(shadow.fresh_reference_md5)
    assert callable(shadow.ensure_reference_documents)
    assert callable(shadow.ensure_reference_postings)
    assert not hasattr(shadow, "replace_sweden_address_resolution_shadow")
    assert not hasattr(shadow, "geocode_demand")
    assert not hasattr(shadow, "shared_addresses")
    assert not hasattr(shadow, "address_canonicalization")
```

And append to `tests/test_sweden_geocode_store.py`:

```python
def test_the_store_keeps_only_the_read_rule_the_entity_uses() -> None:
    """Slice 4c: the resolver-family read and the serving projection existed for the demand
    scan and the served view. Both are retired, so the store exposes one read rule --
    `build_current_geocodes_sql`, what geocode.py's cache lookup renders."""
    from dagster_v3.defs.sweden_company import geocode_store

    assert callable(geocode_store.build_current_geocodes_sql)
    for retired in (
        "SERVING_COLUMNS",
        "build_current_resolver_geocodes_sql",
        "RESOLVER_ONLY_FILTER_SQL",
        "current_resolver_outcomes_by_address",
        "GEOCODE_APPEND_TABLE",
        "QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE",
        "PREVIOUS_OUTCOMES_TABLE",
        "QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE",
    ):
        assert not hasattr(geocode_store, retired), retired
```

- [ ] **Step 2: Run them and watch them fail**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix \
  uv run --frozen --no-sync pytest \
  tests/test_sweden_address_reference_documents.py::test_the_reference_builders_stand_alone_after_the_old_chain_retired \
  tests/test_sweden_geocode_store.py::test_the_store_keeps_only_the_read_rule_the_entity_uses -q
```

Expected: two failures — `replace_sweden_address_resolution_shadow` still present, `SERVING_COLUMNS` still present.

- [ ] **Step 3: Cut the shadow driver out and take `fresh_reference_md5` in**

In `src/dagster_v3/defs/sweden_company/address_resolution_shadow.py`:

Delete these definitions and nothing else: `replace_sweden_address_resolution_shadow` (line 83), `replace_sweden_address_resolution_unmatched_diagnostics` (164), `_replace_query_documents` (400), `_replace_comparison` (784), `_assert_shadow_invariants` (822), `_shadow_counts` (866); and the constants only they used: `SHADOW_QUERY_DOCUMENTS_TABLE`, `SHADOW_QUERY_STREET_VARIANTS_TABLE`, `SHADOW_CANDIDATES_TABLE`, `SHADOW_RESULTS_TABLE`, `SHADOW_COMPARISON_TABLE`, `UNMATCHED_DIAGNOSTICS_TABLE` and their four `QUALIFIED_*` twins. Keep `SHADOW_REFERENCE_DOCUMENTS_TABLE`, `REFERENCE_MANIFEST_TABLE`, `REFERENCE_POSTINGS_TABLE`, `REFERENCE_POSTINGS_MANIFEST_TABLE`, their `QUALIFIED_*` twins, `INDEX_SCOPE`, the seven public reference helpers and the five private document builders (`_replace_building_reference_documents`, `_replace_street_reference_documents`, `_replace_address_point_street_inputs`, `_replace_road_search_documents`, `_replace_road_street_inputs`) plus `_spread_sql` and `_log`.

Replace the import block at lines 19-24 with:

```python
from dagster_v3.defs.sweden_company import geocode_store
```

and drop the now-unused `from dagster_v3.defs.address_resolution.diagnostics import replace_unmatched_address_resolution_diagnostics`, `replace_address_resolution_candidates`, `replace_address_resolution_results` and `replace_address_search_documents` / `replace_address_street_variants` imports (keep `_replace_fuzzy_street_postings`, which `replace_reference_postings` calls). Then:

```bash
cd corpscout/services/dagster_v3
sed -i '' 's/address_canonicalization\.ENRICHMENT_SCHEMA/geocode_store.ENRICHMENT_SCHEMA/g' \
  src/dagster_v3/defs/sweden_company/address_resolution_shadow.py
rg -n "address_canonicalization|geocode_demand|shared_addresses" \
  src/dagster_v3/defs/sweden_company/address_resolution_shadow.py
```

The `rg` must come back with only the two `geocode_demand.fresh_reference_md5` call sites. Add the function above `replace_reference_documents` and change both call sites to the bare name:

```python
def fresh_reference_md5(connection: Any) -> str:
    """The OSM snapshot identity this run holds, read exactly as the promotion stamps it.

    Moved here from geocode_demand.py in slice 4c: the demand scan retired with the old
    chain, and the reference-document builders below are the only remaining callers.
    """
    [(reference_md5,)] = connection.execute(
        f"""
        select coalesce(first(source_md5 order by source_record_id), '')
        from {osm_tables.QUALIFIED_ADDRESS_TABLE}
        """
    ).fetchall()
    if not str(reference_md5):
        raise ValueError(
            "The Sweden OSM reference table carries no snapshot MD5 -- refusing to "
            "build address-resolution reference documents against an unidentifiable reference"
        )
    return str(reference_md5)
```

- [ ] **Step 4: Delete the four modules that lost their last importer**

Confirm the keep-rule holds, then delete:

```bash
cd corpscout/services/dagster_v3
for m in geocode_demand shared_addresses address_canonicalization address_parsing; do
  echo "### $m ###"
  rg -n "sweden_company import [^\n]*\b$m\b|sweden_company\.$m" src tests --glob '!**/__pycache__/**'
done
```

Expected after Step 3: `geocode_demand` — nothing; `shared_addresses` — nothing; `address_canonicalization` — only `shared_addresses.py` (going) and the three doomed tests; `address_parsing` — only `address_canonicalization.py` (going) and `tests/test_sweden_company_address_geocoding.py` (deleted in Task 3).

```bash
rm src/dagster_v3/defs/sweden_company/geocode_demand.py \
   src/dagster_v3/defs/sweden_company/shared_addresses.py \
   src/dagster_v3/defs/sweden_company/address_canonicalization.py \
   src/dagster_v3/defs/sweden_company/address_parsing.py \
   tests/test_sweden_geocode_demand.py \
   tests/test_sweden_geocode_demand_clickhouse_local.py \
   tests/test_sweden_company_address_canonicalization_clickhouse_local.py
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix \
  uv run --frozen --no-sync dg check defs
```

Delete `tests/test_sweden_geocode_store_current_mv.py` in the same step. It is the drift pin
comparing migration 000320's `CREATE MATERIALIZED VIEW` body against
`build_current_geocodes_sql(columns=SERVING_COLUMNS)` — both sides of that comparison leave in
this slice (the view is drop-list entry 12 and `SERVING_COLUMNS` goes in Step 5), and Task 3
empties 000320, which would make the pin read an empty file:

```bash
rm tests/test_sweden_geocode_store_current_mv.py
```

- [ ] **Step 5: Trim `geocode_store.py`**

Delete, in `src/dagster_v3/defs/sweden_company/geocode_store.py`: `GEOCODE_APPEND_TABLE` and `QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE` (lines 61-62), `PREVIOUS_OUTCOMES_TABLE` and `QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE` (64-67), `RESOLVER_ONLY_FILTER_SQL` (71), `SERVING_COLUMNS` with the two comment lines above it (121-127), `build_current_resolver_geocodes_sql` (195-224) and `current_resolver_outcomes_by_address` (311-315).

Keep `RANK_INPUT_COLUMNS` (`_inner_columns` reads it), `current_resolver_outcome` and `current_adopted_outcome` (`current_outcome` calls both), `_by_address` (`current_outcomes_by_address` calls it) and `IS_ADOPTED_SQL` (`build_current_geocodes_sql` renders it).

Amend the `STORE_COLUMNS` comment that referred to the serving shape:

```python
# The store's own columns. `se_address_geocodes_current` used to project all but the two
# version columns; that refreshable view was dropped in slice 4c and the entity reads the
# store through build_current_geocodes_sql instead.
```

- [ ] **Step 6: Trim the tests of deleted code**

In `tests/test_sweden_geocode_store.py` delete four tests: `test_the_store_columns_are_the_serving_columns_plus_the_two_version_columns` (line 57, `SERVING_COLUMNS`), `test_the_module_agrees_with_the_canonicalization_module_on_names` (69), `test_the_resolver_only_read_is_stage_one_over_the_resolver_family` (154) and `test_the_resolver_only_read_parenthesizes_the_caller_filter` (168) and `test_the_resolver_view_ignores_adopted_rows_entirely` (250); narrow the import block at line 14 accordingly. In `test_the_read_filters_before_ranking` (144) replace the example filter's table with a live one:

```python
        address_filter_sql="address_id IN (SELECT location_key FROM corpscout.se_company_address_normalized)")
```

In `tests/test_address_resolution.py` delete five tests — `test_sweden_shadow_adapter_builds_results_without_serving_changes` (1059), `test_sweden_shadow_exact_suffix_variant_matches_punctuated_street` (1157), `test_a_partial_pending_week_scopes_the_shadow_stages_to_the_pending_set` (1205), `test_sweden_unmatched_diagnostics_explain_typo_and_osm_coverage` (1273) — and the two fixtures only they used, `_create_sweden_shadow_fixture` (1334) and `_create_sweden_shadow_exact_suffix_fixture` (1660); narrow the `address_resolution_shadow` import at line 32 to what remains. Then move in the matcher test that Task 3's deletion would otherwise take with it — append to `tests/test_address_resolution.py`:

```python
def test_street_location_key_removes_house_and_non_location_suffixes() -> None:
    """Moved here from tests/test_sweden_company_address_geocoding.py in slice 4c: the
    matcher stays, the old chain's test file does not."""
    from dagster_v3.defs.sweden_address_osm.address_matching import (
        normalized_street_location_key_sql,
    )

    key_sql = normalized_street_location_key_sql(
        street_name_sql="street_name",
        street_address_sql="street_address",
        normalized_postcode_sql="postcode",
    )
    connection = duckdb.connect()
    keys = connection.execute(
        f"""
        select {key_sql}
        from values
            ('', 'DOKTOR LIBORIUS GATA 42 B', '41323'),
            ('', 'STADSGÅRDEN 6 (+1 KOMMUNIKATIONSBYRÅ AB)', '11645'),
            ('', 'SVEAVÄGEN 53 4 tr', '10124'),
            ('Våxtorpsgränd', 'Våxtorpsgränd 26 lgh 1106', '12573')
            address(street_name, street_address, postcode)
        """
    ).fetchall()
    assert keys == [
        ("41323|doktorliboriusgata",),
        ("11645|stadsgrden",),
        ("10124|sveavgen",),
        ("12573|vxtorpsgrnd",),
    ]
```

In `tests/test_sweden_address_reference_documents.py` line 5, replace `` `geocode_demand.fresh_reference_md5` `` with `` `address_resolution_shadow.fresh_reference_md5` ``.

- [ ] **Step 7: Retire the stale freshness-leaf note**

In `src/dagster_v3/defs/common/clickhouse_checks.py`, replace the seven-line `# NO LEAF FOR se_address_geocodes_current ...` block (lines 207-213) with:

```python
    # The refreshable view se_address_geocodes_current, which had no leaf because no asset
    # published it, was dropped in slice 4c (2026-09-08). The leaf above, on the store the
    # entity actually reads and writes, is the whole of this chain's freshness signal.
```

Leave the never-contains guard in `tests/test_clickhouse_leaf_checks.py:64-71` in place — it now guards against a leaf being added for an object that no longer exists, which is still the right answer.

- [ ] **Step 8: Run everything this touched**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix \
  uv run --frozen --no-sync pytest \
  tests/test_address_resolution.py tests/test_sweden_address_reference_documents.py \
  tests/test_sweden_geocode_store.py tests/test_clickhouse_leaf_checks.py \
  tests/test_se_company_address_geocode.py tests/test_se_company_address_warm.py \
  tests/test_se_company_address_fold.py tests/test_sweden_centroid_assets.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix \
  uv run --frozen --no-sync dg check defs
```

Expected: all green.

- [ ] **Step 9: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_resolution_shadow.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/geocode_store.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/geocode_demand.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/shared_addresses.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_canonicalization.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_parsing.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/common/clickhouse_checks.py \
        corpscout/services/dagster_v3/tests/test_address_resolution.py \
        corpscout/services/dagster_v3/tests/test_sweden_address_reference_documents.py \
        corpscout/services/dagster_v3/tests/test_sweden_geocode_store.py \
        corpscout/services/dagster_v3/tests/test_sweden_geocode_demand.py \
        corpscout/services/dagster_v3/tests/test_sweden_geocode_demand_clickhouse_local.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_address_canonicalization_clickhouse_local.py \
        corpscout/services/dagster_v3/tests/test_sweden_geocode_store_current_mv.py
git commit -m "$(cat <<'EOF'
refactor(se-address): the old chain's library modules retire

The shadow driver's two input tables were built by assets slice 4b deleted, so
it cannot run; with it go geocode_demand, shared_addresses,
address_canonicalization and address_parsing. fresh_reference_md5 moves to the
reference-document builders that are its only remaining caller.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 3: The dropped objects' DDL leaves the ledger

**Files:**
- Modify (emptied, up and down): `corpscout/clickhouse/migrations/000255_corpscout_se_company_address_history`, `000256_corpscout_se_company_address_current_snapshot`, `000265_corpscout_se_company_address_normalization`, `000270_corpscout_se_company_address_geocodes`, `000271_corpscout_se_company_address_geocode_results`, `000272_corpscout_se_company_address_city_fallback`, `000273_corpscout_se_company_canonical_addresses`, `000274_corpscout_se_shared_addresses`, `000275_corpscout_se_address_geocodes_current`, `000277_corpscout_se_address_geocode_spread`, `000278_corpscout_se_address_components`, `000307_corpscout_se_company_address`, `000312_corpscout_se_company_addresses_current_uid_default`, `000320_corpscout_se_address_geocodes_current_mv`, `000325_corpscout_se_address_geocodes_served_view`, `000327_corpscout_se_address_geocodes_served_postal_box_fallback`
- Modify (one statement out): `000084_corpscout_se_company_registry`, `000244_corpscout_company_source_records`
- Modify: `tests/test_clickhouse_migrations.py` (`EMPTIED_MIGRATIONS`, two tests), `tests/se_company_ddl.py:13-15` (docstring), `src/dagster_v3/defs/sweden_company/tables.py:8-9,15-18`
- Delete: `tests/test_sweden_company_address_geocoding.py`
- Create: `tests/test_sweden_address_geocoding_weekly.py`
- Modify: `src/dagster_v3/defs/sweden_address_geocoding/docs/sweden_address_geocoding-design.md:67`, `tests/test_se_companies_serving_mv.py:11`

**Interfaces:**
- Consumes: nothing from Tasks 1-2 (this task is files and tests only).
- Produces: sixteen new names in `EMPTIED_MIGRATIONS`; three tests re-homed under the names `test_the_weekly_is_the_only_geocoding_job_and_selects_five_assets`, `test_sweden_address_geocode_store_migration_is_versioned_and_replacing` and `test_no_new_migration_drops_a_slice_4c_retirement`, which Task 4's script test does **not** duplicate.

**The legacy per-company geocode pair joins this edit** (controller ruling, 2026-09-08). `corpscout.se_company_address_geocodes` and `corpscout.se_company_address_geocode_results` were dropped by hand earlier, by `LEGACY_PAIR_RETIREMENT_DROP_SQL`, but their DDL never left the ledger. Same policy, same removal comment, same date: `000270`, `000271` and `000272` declare nothing else and are emptied. That in turn makes `000277_corpscout_se_address_geocode_spread` an emptied file rather than a partial one — its two ALTERs name `se_address_geocodes_current` (drop-list entry 12) and `se_company_address_geocode_results`, and with the pair gone neither survives. So the edit is **16 emptied files and 2 partial ones** (`000084`, `000244`), not the 12-and-3 an earlier reading of the handoff list would give.

- [ ] **Step 1: Write the failing ledger pins**

Append to `tests/test_clickhouse_migrations.py`:

```python
SLICE_4C_EMPTIED = (
    "000255_corpscout_se_company_address_history",
    "000256_corpscout_se_company_address_current_snapshot",
    "000265_corpscout_se_company_address_normalization",
    "000270_corpscout_se_company_address_geocodes",
    "000271_corpscout_se_company_address_geocode_results",
    "000272_corpscout_se_company_address_city_fallback",
    "000273_corpscout_se_company_canonical_addresses",
    "000274_corpscout_se_shared_addresses",
    "000275_corpscout_se_address_geocodes_current",
    "000277_corpscout_se_address_geocode_spread",
    "000278_corpscout_se_address_components",
    "000307_corpscout_se_company_address",
    "000312_corpscout_se_company_addresses_current_uid_default",
    "000320_corpscout_se_address_geocodes_current_mv",
    "000325_corpscout_se_address_geocodes_served_view",
    "000327_corpscout_se_address_geocodes_served_postal_box_fallback",
)

# Every name whose DDL leaves the ledger in slice 4c: the twelve this slice's script drops,
# plus se_company_addresses_canonical_current and the legacy per-company geocode pair, which
# were dropped by hand earlier and only lose their DDL now. Whole-name matching only:
# se_company_address (the entity, kept) is a prefix of six of these.
SLICE_4C_DROPPED_OBJECTS = (
    "se_companies_serving_retired",
    "se_company_address_legacy",
    "se_company_address_scb",
    "se_company_address_bolagsverket",
    "se_company_address_correction",
    "se_company_addresses",
    "se_company_addresses_current",
    "se_company_addresses_canonical_current",
    "se_company_address_members_current",
    "se_company_address_geocodes",
    "se_company_address_geocode_results",
    "se_addresses_current",
    "se_company_address_links_current",
    "se_address_geocodes_current",
    "se_address_geocodes_served",
)


def test_the_slice_4c_migrations_are_emptied_and_registered() -> None:
    """Dev-phase ledger policy: an object dropped by hand loses its DDL from the file that
    declared it, and the file stays for history with the database statement alone."""
    for migration in SLICE_4C_EMPTIED:
        assert migration in EMPTIED_MIGRATIONS, migration
        for suffix in (".up.sql", ".down.sql"):
            sql = _migration_sql(f"{migration}{suffix}")
            assert _statement_lines(sql) == ["CREATE DATABASE IF NOT EXISTS corpscout;"], (
                f"{migration}{suffix}"
            )
            assert "dropped by hand" in sql, f"{migration}{suffix} lost its removal comment"


def test_no_up_migration_declares_a_slice_4c_dropped_object() -> None:
    """No CREATE and no later ALTER may still build one of them on the way UP.

    UP FILES ONLY, deliberately. 000345's and 000348's DOWN files recreate the parked
    se_companies_serving_retired verbatim so the serving-view swap chain can walk backwards
    -- that is the same history 000391-000393 carry, and it is left alone.

    A DROP is history of a drop, not a declaration, so 000314, 000345, 000348 and 000392 pass
    on their up files by construction: the pattern matches only declarations. So does
    000308's GRANT on se_company_address_correction, which stays on purpose -- access
    migrations are not emptied when their target leaves (000298 does the same for
    se_company_info_correction).
    """
    pattern = re.compile(
        r"(CREATE TABLE IF NOT EXISTS|CREATE OR REPLACE VIEW|CREATE VIEW IF NOT EXISTS"
        r"|CREATE MATERIALIZED VIEW|ALTER TABLE)\s+(?:corpscout\.)?(\w+)",
        re.IGNORECASE,
    )
    for path in sorted(MIGRATIONS_DIR.glob("*.up.sql")):
        for _, name in pattern.findall(path.read_text(encoding="utf-8")):
            assert name not in SLICE_4C_DROPPED_OBJECTS, f"{path.name} declares {name}"
```

`re` is already imported at the top of the file; add it if a future edit removed it.

Two facts this pattern cannot see, both handled by the emptying rather than by the guard:
000320's up file creates the view under the staging name `se_address_geocodes_current_next`
and renames it at the end, and 000307's up file creates the old final table under the name
`se_company_address`, which the entity has carried since 000393 and which is KEPT. Both files
are emptied whole, so neither leaves a declaration behind.

- [ ] **Step 2: Run them and watch them fail**

```bash
cd corpscout/services/dagster_v3
uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py -q \
  -k "slice_4c_migrations_are_emptied or no_up_migration_declares_a_slice_4c"
```

Expected: two failures — the sixteen names are not in `EMPTIED_MIGRATIONS`, and 000084 (among others) still declares `se_company_addresses`.

- [ ] **Step 3: Empty the sixteen files**

Every one of the thirty-two files becomes exactly this, byte for byte (six comment lines, one statement, no semicolon inside a comment, a statement last):

```sql
-- SE address slice 4c (2026-09-08): this migration's objects -- the retired old address
-- chain (the register address history and its current snapshot, the canonical, members,
-- shared-identity and link tables, the per-source address artifacts and the correction
-- ledger, the legacy per-company geocode pair, the geocode serving view and overlay) --
-- were dropped by hand on the server and their DDL left this file per the dev-phase ledger
-- policy. The file stays for history.
CREATE DATABASE IF NOT EXISTS corpscout;
```

```bash
cd corpscout/clickhouse/migrations
cat > /tmp/emptied_4c.sql <<'EOF'
-- SE address slice 4c (2026-09-08): this migration's objects -- the retired old address
-- chain (the register address history and its current snapshot, the canonical, members,
-- shared-identity and link tables, the per-source address artifacts and the correction
-- ledger, the legacy per-company geocode pair, the geocode serving view and overlay) --
-- were dropped by hand on the server and their DDL left this file per the dev-phase ledger
-- policy. The file stays for history.
CREATE DATABASE IF NOT EXISTS corpscout;
EOF
for m in 000255_corpscout_se_company_address_history \
         000256_corpscout_se_company_address_current_snapshot \
         000265_corpscout_se_company_address_normalization \
         000270_corpscout_se_company_address_geocodes \
         000271_corpscout_se_company_address_geocode_results \
         000272_corpscout_se_company_address_city_fallback \
         000273_corpscout_se_company_canonical_addresses \
         000274_corpscout_se_shared_addresses \
         000275_corpscout_se_address_geocodes_current \
         000277_corpscout_se_address_geocode_spread \
         000278_corpscout_se_address_components \
         000307_corpscout_se_company_address \
         000312_corpscout_se_company_addresses_current_uid_default \
         000320_corpscout_se_address_geocodes_current_mv \
         000325_corpscout_se_address_geocodes_served_view \
         000327_corpscout_se_address_geocodes_served_postal_box_fallback; do
  cp /tmp/emptied_4c.sql "$m.up.sql"
  cp /tmp/emptied_4c.sql "$m.down.sql"
done
```

- [ ] **Step 4: Take one statement out of the two partial files**

`000084_corpscout_se_company_registry.up.sql`: delete lines 6-23 (the `CREATE TABLE IF NOT EXISTS corpscout.se_company_addresses` block through its `ORDER BY (company_id, address_type, source, source_record_id);`) and add above the surviving `CREATE TABLE IF NOT EXISTS corpscout.se_industries`, alongside the slice-5 note:

```sql
-- se_company_addresses (the register address history) was dropped by hand in SE address
-- slice 4c (2026-09-08) and its DDL left this file per the dev-phase ledger policy.
```

`000084_...down.sql`: delete the line `DROP TABLE IF EXISTS corpscout.se_company_addresses;`, leaving the `se_industries` drop as the file's last statement.

`000244_corpscout_company_source_records.up.sql`: delete lines 192-197 (the `ALTER TABLE corpscout.se_company_addresses ADD COLUMN ... source_record_uid ...` statement) and put in their place, above the `se_industries` ALTER:

```sql
-- se_company_addresses removed on 2026-09-08: dropped by hand in SE address slice 4c
-- (development-phase ledger policy).
```

`000244_...down.sql`: delete lines 18-19 (`ALTER TABLE corpscout.se_company_addresses` / `    DROP COLUMN IF EXISTS source_record_uid;`).

Both files keep plenty besides (`se_industries`; the ten other `source_record_uid` columns and the seven `company_source_record*` / `esef_document*` tables), so neither joins `EMPTIED_MIGRATIONS`.

- [ ] **Step 5: Register the sixteen in `EMPTIED_MIGRATIONS`**

In `tests/test_clickhouse_migrations.py`, extend the existing set (do not reorder or reformat what is there):

```python
    # SE address slice 4c (2026-09-08): the old address chain was dropped by hand and its
    # DDL left these files. 000270-000272 are the legacy per-company geocode pair, dropped
    # by hand earlier (LEGACY_PAIR_RETIREMENT_DROP_SQL) and only losing its DDL now, and
    # 000277 altered one table from each half. 000084 and 000244 are NOT here -- each still
    # declares something that stays (se_industries, the other source_record_uid columns)
    # and only lost the one statement that named a dropped object.
    "000255_corpscout_se_company_address_history",
    "000256_corpscout_se_company_address_current_snapshot",
    "000265_corpscout_se_company_address_normalization",
    "000270_corpscout_se_company_address_geocodes",
    "000271_corpscout_se_company_address_geocode_results",
    "000272_corpscout_se_company_address_city_fallback",
    "000273_corpscout_se_company_canonical_addresses",
    "000274_corpscout_se_shared_addresses",
    "000275_corpscout_se_address_geocodes_current",
    "000277_corpscout_se_address_geocode_spread",
    "000278_corpscout_se_address_components",
    "000307_corpscout_se_company_address",
    "000312_corpscout_se_company_addresses_current_uid_default",
    "000320_corpscout_se_address_geocodes_current_mv",
    "000325_corpscout_se_address_geocodes_served_view",
    "000327_corpscout_se_address_geocodes_served_postal_box_fallback",
```

Also update the comment above the set: it currently says "2026-09-03 and 2026-09-08" — leave the dates, they still hold.

- [ ] **Step 6: Delete or narrow the tests that pinned the removed DDL**

In `tests/test_clickhouse_migrations.py`:
- delete `test_sweden_company_addresses_store_a_normalized_match_key` (line 3361) — it reads 000265, now empty;
- in `test_sweden_company_registry_migration_covers_exported_columns` (1456) delete the `sweden_company_tables.COMPANY_ADDRESSES_TABLE_CH: (sweden_company_tables.SE_COMPANY_ADDRESS_BASE_COLUMNS)` entry from `expected_columns_by_table`, leaving only `INDUSTRIES_TABLE_CH`, and add to its body a line asserting the removal held:

```python
    assert "corpscout.se_company_addresses\n" not in sql
```

In `src/dagster_v3/defs/sweden_company/tables.py` delete `COMPANY_ADDRESSES_TABLE_CH` and `COMPANY_ADDRESSES_CURRENT_TABLE_CH` (lines 8-9), `QUALIFIED_COMPANY_ADDRESSES_TABLE` and `QUALIFIED_COMPANY_ADDRESSES_CURRENT_TABLE` (15-18), and `SE_COMPANY_ADDRESS_BASE_COLUMNS` (162 and its tuple) — the narrowed test above was their last reader.

In `tests/se_company_ddl.py` update the `_migration_for` docstring (lines 13-15): `000307 the address ones` becomes `000384 the address entity's` (000307 is emptied and no longer declares anything).

- [ ] **Step 7: Delete the old chain's test file, re-homing its three keepers**

Create `tests/test_sweden_address_geocoding_weekly.py` and move `test_the_weekly_is_the_only_geocoding_job_and_selects_five_assets` into it verbatim (it needs `import dagster as dg` and the in-function `from dagster_v3.definitions import defs as load_defs`), with the module docstring:

```python
"""What is left of the Sweden address-geocoding chain, as a shape guard.

Moved out of tests/test_sweden_company_address_geocoding.py when slice 4c deleted that file
with the chain it tested. The weekly is the only geocoding job, and this is the list of
assets it may select -- a re-added canonical/shared/demand/resolution step fails here first.
"""
```

Move `test_sweden_address_geocode_store_migration_is_versioned_and_replacing` verbatim into `tests/test_clickhouse_migrations.py` (it pins 000317, the KEPT store's own migration); rewrite its `migration_directory` lines to use the module's `_migration_sql` helper.

Move `test_no_new_migration_drops_a_slice_4c_retirement` into `tests/test_clickhouse_migrations.py` too, rewritten against the shared list and with its ordering premise restated for the post-drop world:

```python
def test_no_new_migration_drops_a_slice_4c_retirement() -> None:
    """The gated drops stay out of the ledger (owner ruling 2026-08-25, paid for in UNDROPs).
    Slice 4c dropped these by hand from
    corpscout/clickhouse/operations/se_address_retirement_drops.sql; a later
    migration must not re-declare a drop for them. Only migrations NEWER than 000392 are
    checked -- 000256's own rename-swap and 000345/000348/000392's serving-view swaps
    legitimately dropped objects on this list, and history is not what this guard is about."""
    names = "|".join(sorted(SLICE_4C_DROPPED_OBJECTS, key=len, reverse=True))
    pattern = re.compile(
        rf"drop\s+(?:table|view)\s+(?:if\s+exists\s+)?(?:corpscout\.)?({names})"
        r"([^_a-zA-Z0-9]|$)",
        re.IGNORECASE,
    )
    up_files = [p for p in sorted(MIGRATIONS_DIR.glob("*.up.sql")) if p.name >= "000393"]
    assert up_files, "no migration up files newer than 000392 -- this guard would pass vacuously"
    for path in up_files:
        found = pattern.search(path.read_text(encoding="utf-8"))
        assert found is None, f"{path.name} drops {found.group(1)}"
```

Then delete the file:

```bash
cd corpscout/services/dagster_v3
rm tests/test_sweden_company_address_geocoding.py
```

Everything else it held goes with it: the canonical/shared build tests, the chunked canonical-load tests, the libpostal parse test (its module was deleted in Task 2), and the migration pins for 000270/000271/000272/000273/000274/000275/000277/000278 — all of them read DDL that is gone or objects that are dropped.

- [ ] **Step 8: The two remaining doc/comment mentions**

`src/dagster_v3/defs/sweden_address_geocoding/docs/sweden_address_geocoding-design.md:67`: replace `normalize and match \`corpscout.se_company_addresses_current\` against the` with `normalize and match \`corpscout.se_company_address_normalized\` against the` and add ` (the source table was \`se_company_addresses_current\` until the address entity's slice-4 cutover)` at the end of that bullet.

`tests/test_se_companies_serving_mv.py:11`: `\`se_company_address_legacy\` until slice 4c drops it` becomes `\`se_company_address_legacy\`, dropped by hand in slice 4c`.

- [ ] **Step 9: Run the ledger and fixture suites**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix \
  uv run --frozen --no-sync pytest \
  tests/test_clickhouse_migrations.py tests/test_se_companies_serving_mv.py \
  tests/test_se_companies_serving_sql.py tests/test_sweden_company_source_tables.py \
  tests/test_sweden_company_source_tables_clickhouse_local.py \
  tests/test_se_company_address_tables.py tests/test_se_company_basic_info_tables.py \
  tests/test_se_company_address_fold_clickhouse_local.py \
  tests/test_se_company_address_normalize_clickhouse_local.py \
  tests/test_se_company_address_geocode_clickhouse_local.py \
  tests/test_se_company_person_views.py tests/test_company_serving.py \
  tests/test_company_domain_suggestions_dbt.py \
  tests/test_sweden_address_geocoding_weekly.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix \
  uv run --frozen --no-sync dg check defs
```

Expected: all green. The clickhouse-local fixtures listed here are the ones that build tables out of migration DDL; none of them reads an emptied file (verified: they read 000317, 000322, 000380-000387 and 000391-000393), so a green run is the proof that the edits touched nothing live.

- [ ] **Step 10: Commit**

```bash
git add corpscout/clickhouse/migrations \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/services/dagster_v3/tests/se_company_ddl.py \
        corpscout/services/dagster_v3/tests/test_se_companies_serving_mv.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py \
        corpscout/services/dagster_v3/tests/test_sweden_address_geocoding_weekly.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/tables.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_address_geocoding/docs/sweden_address_geocoding-design.md
git commit -m "$(cat <<'EOF'
chore(clickhouse): the old SE address chain's DDL leaves the ledger

Sixteen files keep only CREATE DATABASE, including the legacy per-company
geocode pair dropped by hand earlier; 000084 and 000244 lose one statement
each. No numbered migration drops anything -- the drops are the owner's, per
the dev-phase ledger policy.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 4: The three owner-run SQL scripts

**Files:**
- Create: `corpscout/clickhouse/operations/se_address_retirement_precheck.sql`, `corpscout/clickhouse/operations/se_address_retirement_drops.sql`, `corpscout/clickhouse/operations/se_address_retirement_postcheck.sql`
- Create: `tests/test_se_address_retirement_drops.py`

**Interfaces:**
- Consumes: `SLICE_4C_DROPPED_OBJECTS` is **not** imported from `tests/test_clickhouse_migrations.py` — that tuple is a ledger guard and includes three objects already gone (`se_company_addresses_canonical_current` and the legacy geocode pair) and carries no order. This task's test declares its own ordered `DROP_ORDER` tuple, which is the spec's slice-4b handoff list plus the parked serving view.
- Produces: `corpscout/clickhouse/operations/se_address_retirement_drops.sql`, twelve statements, one per line, in `DROP_ORDER`; the prod-run task in Task 5 pipes exactly this file.

**Location** (controller ruling, 2026-09-08): the three scripts live in `corpscout/clickhouse/operations/`, the house directory for owner-run work beside the ledger they retire from (`se_companies_retire.md`, `se_company_info_retire.md`, the DNS backfill buckets), but stay pipeable `.sql` rather than becoming fenced Markdown, so step 4 of Task 5 is one redirect and the test can parse them. `tests/test_clickhouse_migrations.py` already resolves that directory as `OPERATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "operations"`; this task's test uses the same expression.

- [ ] **Step 1: Write the failing test**

Create `tests/test_se_address_retirement_drops.py`:

```python
"""The slice-4c drop scripts say exactly what the spec's handoff list says, in its order.

The scripts live in corpscout/clickhouse/operations/ beside the ledger they retire from, and
they are owner-run: nothing in this repo executes them, so this file is the only thing
standing between a typo and a dropped production table. It reads the SQL, parses the object
names out, and compares them to the list -- as WHOLE names, because
corpscout.se_company_address (the entity, kept for good) is a prefix of five entries here.
"""

import re
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[3] / "clickhouse" / "operations"
DROPS = SCRIPTS / "se_address_retirement_drops.sql"
PRECHECK = SCRIPTS / "se_address_retirement_precheck.sql"
POSTCHECK = SCRIPTS / "se_address_retirement_postcheck.sql"

# Spec section 9, the slice-4b shipped record's handoff order, plus the serving view 000392
# parked under _retired. Each entry is dropped only after everything that reads it:
# se_address_geocodes_served LEFT JOINs se_addresses_current, and it reads
# se_address_geocodes_current as its precise base, so the view goes between them.
DROP_ORDER = (
    ("TABLE", "se_companies_serving_retired"),
    ("TABLE", "se_company_address_legacy"),
    ("TABLE", "se_company_address_scb"),
    ("TABLE", "se_company_address_bolagsverket"),
    ("TABLE", "se_company_address_correction"),
    ("TABLE", "se_company_addresses"),
    ("TABLE", "se_company_addresses_current"),
    ("TABLE", "se_company_address_members_current"),
    ("VIEW", "se_address_geocodes_served"),
    ("TABLE", "se_addresses_current"),
    ("TABLE", "se_company_address_links_current"),
    ("TABLE", "se_address_geocodes_current"),
)

# Never droppable. se_company_address is the entity itself; the three below it are the
# geocode cache and the two centroid references the spec keeps "for good".
KEPT = (
    "se_company_address",
    "se_company_address_suggestion",
    "se_company_address_normalized",
    "se_company_address_history",
    "se_company_address_rule",
    "se_company_address_precedence",
    "se_address_geocodes",
    "se_postcode_centroids",
    "se_city_centroids",
    "se_companies_serving",
    "se_company_basic_info",
    "se_scb_companies",
    "se_bolagsverket_companies",
)

_DROP = re.compile(
    r"^DROP (TABLE|VIEW) IF EXISTS corpscout\.(\w+);$", re.MULTILINE
)


def _statements(path: Path) -> list[tuple[str, str]]:
    return [(kind, name) for kind, name in _DROP.findall(path.read_text(encoding="utf-8"))]


def test_the_drop_script_drops_exactly_the_handoff_list_in_order() -> None:
    assert _statements(DROPS) == list(DROP_ORDER)


def test_the_drop_script_names_no_kept_object() -> None:
    """Whole-name comparison. A substring check would call se_company_address_legacy a hit
    on the entity and refuse a legitimate drop -- or, written the other way round, would
    call the entity safe because a longer name containing it is on the drop list."""
    dropped = {name for _, name in _statements(DROPS)}
    assert dropped.isdisjoint(KEPT)
    assert len(dropped) == len(DROP_ORDER)


def test_the_served_overlay_is_the_only_drop_view() -> None:
    """se_address_geocodes_served is a plain VIEW (000325, replaced in place by 000327).
    se_address_geocodes_current is a REFRESHABLE MATERIALIZED VIEW since 000320, and
    DROP TABLE is what removes one -- migration 000392 did exactly that to
    se_companies_serving_retired on production, successfully."""
    views = [name for kind, name in _statements(DROPS) if kind == "VIEW"]
    assert views == ["se_address_geocodes_served"]


def test_no_script_uses_sync() -> None:
    """SYNC turns each drop into a blocking wait for a full data removal. These are large
    tables and the owner runs the file as one pipe; the default async drop is what the
    480-second UNDROP window is measured against."""
    for path in (DROPS, PRECHECK, POSTCHECK):
        assert " SYNC" not in path.read_text(encoding="utf-8").upper(), path.name


def test_the_precheck_and_postcheck_cover_the_same_objects() -> None:
    names = [name for _, name in DROP_ORDER]
    for path in (PRECHECK, POSTCHECK):
        sql = path.read_text(encoding="utf-8")
        for name in names:
            assert f"'{name}'" in sql, f"{path.name} does not cover {name}"


def test_the_precheck_reports_the_engine_and_the_row_count() -> None:
    """The engine tells the owner whether a name is a table, a plain view or a refreshable
    MV before the drop statement assumes it; the row count is what goes in the ledger."""
    sql = PRECHECK.read_text(encoding="utf-8")
    assert "engine" in sql
    assert "total_rows" in sql
    assert "FROM system.tables" in sql


def test_the_precheck_counts_the_plain_view_itself() -> None:
    """system.tables.total_rows is NULL for a plain View, so the served overlay would be the
    one object whose size the ledger could not record. It gets its own SELECT count()."""
    sql = PRECHECK.read_text(encoding="utf-8")
    assert "SELECT count() AS se_address_geocodes_served_rows" in sql
    assert "FROM corpscout.se_address_geocodes_served" in sql


def test_the_postcheck_asserts_absence_rather_than_reporting_it() -> None:
    sql = POSTCHECK.read_text(encoding="utf-8")
    assert "count() = 0 AS all_dropped" in sql
    assert "groupArray(name) AS still_present" in sql
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cd corpscout/services/dagster_v3
uv run --frozen --no-sync pytest tests/test_se_address_retirement_drops.py -q
```

Expected: every test errors with `FileNotFoundError` on `corpscout/clickhouse/operations/se_address_retirement_drops.sql`.

- [ ] **Step 3: Write the precheck**

Create `corpscout/clickhouse/operations/se_address_retirement_precheck.sql`:

```sql
-- SE address slice 4c precheck. Run BEFORE se_address_retirement_drops.sql, record the
-- output in the ledger, and check the two gates below.
--
-- Gate 1: nothing left in ClickHouse reads any of these. The match is on a FROM or a JOIN,
-- so a provenance string literal in a view body cannot hold the gate open, and the trailing
-- character class stops corpscout.se_company_address (the entity, KEPT) from matching the
-- se_company_address_legacy alternative -- the alternatives are ordered longest-first for
-- the same reason.
SELECT count() = 0 AS no_readers, groupArray(name) AS readers
FROM system.tables
WHERE database = 'corpscout'
  AND engine IN ('View', 'MaterializedView')
  AND name NOT IN (
      'se_companies_serving_retired', 'se_company_address_legacy',
      'se_company_address_scb', 'se_company_address_bolagsverket',
      'se_company_address_correction', 'se_company_addresses',
      'se_company_addresses_current', 'se_company_address_members_current',
      'se_address_geocodes_served', 'se_addresses_current',
      'se_company_address_links_current', 'se_address_geocodes_current'
  )
  AND match(create_table_query,
      '(FROM|JOIN)\\s+(corpscout\\.)?(se_company_addresses_current|se_company_address_members_current|se_company_address_links_current|se_companies_serving_retired|se_company_address_bolagsverket|se_address_geocodes_current|se_address_geocodes_served|se_company_address_correction|se_company_address_legacy|se_company_address_scb|se_company_addresses|se_addresses_current)([^_a-zA-Z0-9]|$)');

-- Gate 2: engine and size of every object about to go. `engine` is what tells a
-- MaterializedView apart from a View: se_address_geocodes_served is a plain View and takes
-- DROP VIEW, se_address_geocodes_current is a refreshable MaterializedView and takes
-- DROP TABLE. total_rows is NULL for a plain View, which is correct and expected.
SELECT
    name,
    engine,
    total_rows,
    formatReadableSize(total_bytes) AS size
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_companies_serving_retired', 'se_company_address_legacy',
      'se_company_address_scb', 'se_company_address_bolagsverket',
      'se_company_address_correction', 'se_company_addresses',
      'se_company_addresses_current', 'se_company_address_members_current',
      'se_address_geocodes_served', 'se_addresses_current',
      'se_company_address_links_current', 'se_address_geocodes_current'
  )
ORDER BY name;

-- Gate 2b: the one row count Gate 2 cannot give. system.tables.total_rows is NULL for a
-- plain View, and se_address_geocodes_served is the only plain View on the list, so it is
-- counted directly -- the ledger gets a real number for every object destroyed.
SELECT count() AS se_address_geocodes_served_rows
FROM corpscout.se_address_geocodes_served;

-- Gate 3: the live serving view reads none of them and is healthy at its last refresh.
SELECT view, status, last_success_time, exception
FROM system.view_refreshes
WHERE database = 'corpscout' AND view = 'se_companies_serving';
```

- [ ] **Step 4: Write the drop script**

Create `corpscout/clickhouse/operations/se_address_retirement_drops.sql`:

```sql
-- SE address slice 4c: the old address chain leaves ClickHouse. OWNER-RUN, by hand, after
-- the slice-4c dagster deploy is live and se_address_retirement_precheck.sql is clean.
-- Nothing in this repo executes this file (dev-phase ledger policy, owner ruling
-- 2026-08-25: a drop whose gate cannot be checked at write time never goes in the ledger).
--
-- ORDER MATTERS. Each object is dropped after everything that reads it. The three in the
-- middle are the tight ones: se_address_geocodes_served LEFT JOINs se_addresses_current for
-- postcode and post_town and reads se_address_geocodes_current as its precise base, so the
-- view goes first and its two sources follow.
--
-- NO SYNC, deliberately. An async drop is what leaves the roughly 480-second UNDROP window
-- these drops are gated on. If one turns out to be wrong, run
-- UNDROP TABLE corpscout.<name> inside that window.
--
-- se_address_geocodes_current is a REFRESHABLE MATERIALIZED VIEW (migration 000320), and
-- DROP TABLE is what removes one together with its inner MergeTree -- migration 000392 ran
-- exactly that statement against se_companies_serving_retired on production and it worked.
-- se_address_geocodes_served is a plain VIEW (000325/000327) and takes DROP VIEW.
--
-- KEPT FOR GOOD, and absent from this file by construction (a test asserts it):
-- corpscout.se_address_geocodes, se_postcode_centroids, se_city_centroids and the address
-- entity's own six tables, whose names se_company_address_legacy and the rest merely
-- prefix-collide with.

DROP TABLE IF EXISTS corpscout.se_companies_serving_retired;
DROP TABLE IF EXISTS corpscout.se_company_address_legacy;
DROP TABLE IF EXISTS corpscout.se_company_address_scb;
DROP TABLE IF EXISTS corpscout.se_company_address_bolagsverket;
DROP TABLE IF EXISTS corpscout.se_company_address_correction;
DROP TABLE IF EXISTS corpscout.se_company_addresses;
DROP TABLE IF EXISTS corpscout.se_company_addresses_current;
DROP TABLE IF EXISTS corpscout.se_company_address_members_current;
DROP VIEW IF EXISTS corpscout.se_address_geocodes_served;
DROP TABLE IF EXISTS corpscout.se_addresses_current;
DROP TABLE IF EXISTS corpscout.se_company_address_links_current;
DROP TABLE IF EXISTS corpscout.se_address_geocodes_current;
```

- [ ] **Step 5: Write the postcheck**

Create `corpscout/clickhouse/operations/se_address_retirement_postcheck.sql`:

```sql
-- SE address slice 4c postcheck. Run immediately after se_address_retirement_drops.sql.
-- all_dropped must be 1 and still_present must be empty. If it is not, the UNDROP window
-- has NOT been spent on anything -- re-run the drop for the names listed.
SELECT
    count() = 0 AS all_dropped,
    groupArray(name) AS still_present
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_companies_serving_retired', 'se_company_address_legacy',
      'se_company_address_scb', 'se_company_address_bolagsverket',
      'se_company_address_correction', 'se_company_addresses',
      'se_company_addresses_current', 'se_company_address_members_current',
      'se_address_geocodes_served', 'se_addresses_current',
      'se_company_address_links_current', 'se_address_geocodes_current'
  );

-- The kept objects are all still there -- the point of the whole-name matching. Expect 7.
SELECT count() AS kept_present, groupArray(name) AS kept
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_company_address', 'se_company_address_suggestion',
      'se_company_address_normalized', 'se_company_address_history',
      'se_company_address_rule', 'se_company_address_precedence',
      'se_address_geocodes'
  );

-- And the serving view still refreshes. Re-run after the next :45.
SELECT view, status, last_success_time, exception
FROM system.view_refreshes
WHERE database = 'corpscout' AND view = 'se_companies_serving';
```

- [ ] **Step 6: Run the test**

```bash
cd corpscout/services/dagster_v3
uv run --frozen --no-sync pytest tests/test_se_address_retirement_drops.py -q
```

Expected: 8 passed.

- [ ] **Step 7: Run the whole unit suite once**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix \
  uv run --frozen --no-sync pytest tests -q -x --ignore=tests/test_se_company_address_extractors_clickhouse_local.py
cd ../backoffice && npx vitest run && npm run typecheck
```

Expected: green apart from the pre-existing failures listed in Global Constraints.

- [ ] **Step 8: Commit**

```bash
git add corpscout/clickhouse/operations/se_address_retirement_precheck.sql \
        corpscout/clickhouse/operations/se_address_retirement_drops.sql \
        corpscout/clickhouse/operations/se_address_retirement_postcheck.sql \
        corpscout/services/dagster_v3/tests/test_se_address_retirement_drops.py
git commit -m "$(cat <<'EOF'
chore(se-address): the slice-4c drop scripts, order-pinned

Precheck, twelve drops in dependency order, postcheck, in
corpscout/clickhouse/operations beside the ledger they retire from. Owner-run;
the test pins the order against the spec's handoff list and asserts no kept
object is named, matching whole names because the entity is a prefix of five.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 5: Prod run (controller)

The code retirement must be **live** before the drops: no deployed asset may name a dropped table when it goes. The migration-file edits have no server effect at all, so they ride along with the deploy.

1. [ ] Whole-branch review; merge to main through a worktree that checks main out (the main checkout may sit on `se-basic-info-6-economic-activity`). After the merge re-run `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py tests/test_se_address_retirement_drops.py -q` in case the other branch added a migration naming one of the dropped objects.
2. [ ] Deploy dagster per the worktree deploy recipe (pristine worktree, dbt-state refresh, `.env`); hot-sync the host. Confirm in the UI that `se_address_geocodes_adopt_keys` is gone, `se_address_geocodes_warm` is present, `sweden_company_address_geocoding_weekly` is RUNNING with its five-asset selection and `se_company_address_weekly` is STOPPED.
3. [ ] Run the precheck and record its three results in the ledger:
   ```bash
   ssh companycollect "docker exec -i clickhouse-clickhouse-1 clickhouse-client --multiquery" \
     < corpscout/clickhouse/operations/se_address_retirement_precheck.sql
   ```
   Gate 1 must report `no_readers = 1` with an empty array. Gate 2's `engine` column must show `View` for `se_address_geocodes_served` and `MaterializedView` for `se_address_geocodes_current` — if either differs, stop and adjust the drop statement rather than running the file. Copy the twelve `(name, engine, total_rows, size)` rows plus Gate 2b's `se_address_geocodes_served_rows` into the ledger entry: together they are the record of what was destroyed, with a real count for every object (Gate 2's `total_rows` is NULL for the plain view, which is why 2b exists).
4. [ ] Hand the owner the exact command, to be run with `!` from the main checkout:
   ```bash
   ssh companycollect "docker exec -i clickhouse-clickhouse-1 clickhouse-client --multiquery" \
     < corpscout/clickhouse/operations/se_address_retirement_drops.sql
   ```
   UNDROP is possible for about 480 seconds after each drop (`UNDROP TABLE corpscout.<name>`), so stay at the terminal until the postcheck is clean.
5. [ ] Run the postcheck the same way. `all_dropped` must be 1, `still_present` empty, `kept_present` 7.
6. [ ] Record `SELECT count() FROM system.tables WHERE database = 'corpscout' AND NOT startsWith(name, '.inner') AND NOT startsWith(name, '.tmp.inner')` before step 4 and after step 5; the difference must be exactly 12. Two of the twelve drops (`se_companies_serving_retired`, `se_address_geocodes_current`) are refreshable materialized views, and each keeps its own implicit storage table in `system.tables` under a `.inner_id.*` name (plus a `.tmp.inner_id.*` staging table while a refresh is swapping in) — an unfiltered count would show those inner tables disappearing too and report a delta of 14, not 12.
7. [ ] Wait for the next `:45` and confirm the serving view still refreshes: `SELECT view, status, last_success_time, exception FROM system.view_refreshes WHERE database='corpscout' AND view='se_companies_serving'` reports a success with an empty exception, and `SELECT count() FROM corpscout.se_companies_serving` is around 3.5M with `countIf(address_count > 0)` around 3.49M. It reads none of the dropped objects (000393's render joins `se_company_address` alone), so this is a regression check, not a gate.
8. [ ] Backoffice smoke (local, `npm run dev`): the company Address tab (published rows, history, the reviewer's edit sheet, a targeted fold launch), the companies list, the geocoding list with each filter, the address-quality queue, the same-building lookup, and the generic company detail plus the generic list's Place column for SE.
9. [ ] Record in spec section 9 (slice 4c shipped: what was deleted in code, the twelve objects dropped with their recorded row counts, the sixteen emptied migrations and the two partial ones — including the legacy per-company geocode pair, whose DDL left the ledger long after its own hand-drop — and the ruling that `000308` and `000391`-`000393` were left alone); archive the ledger under `.superpowers/sdd/2026-09-08-se-company-address-4c-drops/`; update memory `se-address-entity`.

## Self-review

- **Spec coverage.** Section 9's Retirement paragraph, item by item: `se_company_address_scb`, `se_company_address_bolagsverket`, the old `se_company_address` (now `_legacy`) and `se_company_address_correction` — drop-list entries 2-5, Task 4, run in Task 5; `se_company_addresses` and `se_company_addresses_current` with the register-load steps — entries 6-7 (the steps went in 4b); `se_company_addresses_canonical_current` — already dropped, recorded in the "already gone" note under the drop table; `se_company_address_members_current`, `se_addresses_current`, `se_company_address_links_current`, the `se_address_geocodes_current` view — entries 8, 10, 11, 12; the demand-scan assets and `geocode_legacy_adoption` — deleted in 4b, their library remnants in Task 2. The 4b handoff's three extra items: the one-off `se_address_geocodes_adopt_keys` — Task 1; the migration-file edits and the fixture cleanup under the ledger policy — Task 3; `geocode_serving_overlay`'s retirement once the served view is gone — Task 1, with `se_address_geocodes_served` added to the drop list as entry 9 (the spec's own list names only `_current`). Section 10's names: `constants` joins the address package's module list, and `adoption` leaves it — recorded in Task 5 step 9. Section 9's "Kept" sentence is enforced twice: by `test_the_drop_script_names_no_kept_object` and by the postcheck's `kept_present` query.
- **Placeholders.** Every code step carries its content: the constants module in full, the two `sed` commands with their verification `rg`, the exact line ranges for both partial migration edits and the exact emptied-file body, the sixteen `EMPTIED_MIGRATIONS` entries, all three SQL scripts verbatim and all eight of their tests. The one thing not inlined is the body of the five deleted test files and the sixteen emptied migration files, which is deletion, not authorship.
- **Type consistency.** `constants.GEOCODE_FALLBACK_PROVIDER` (Task 1) is the single name imported by `fold.py`, `warm.py`, `companies_current.py` and `tests/test_se_company_address_warm.py`; `geocode.py` reads the same module under the alias `constants.`, which is what the `sed` produces from `geocode_serving_overlay.`. `address_resolution_shadow.fresh_reference_md5` (Task 2) keeps the name and signature it had in `geocode_demand`, so `replace_reference_documents` and `ensure_reference_documents` need only lose the module prefix. `SLICE_4C_EMPTIED` and `SLICE_4C_DROPPED_OBJECTS` are defined once, in `tests/test_clickhouse_migrations.py` (Task 3), and read by the two guards there and by the re-homed `test_no_new_migration_drops_a_slice_4c_retirement`; Task 4's `DROP_ORDER` is deliberately a separate, ordered tuple in `tests/test_se_address_retirement_drops.py` because the ledger guard's list includes three objects already dropped and carries no order. `corpscout/clickhouse/operations/se_address_retirement_drops.sql` is named identically in Task 4's test (`parents[3] / "clickhouse" / "operations"`, the same expression `tests/test_clickhouse_migrations.py` uses for `OPERATIONS_DIR`), in Task 4's commit and in Task 5's command.
- **Ordering.** Task 3 must not run before Tasks 1-2: emptying 000325/000327 while `tests/test_se_address_geocodes_served_view.py` still pins their rendering fails that file (deleted in Task 1), and emptying 000320 while `tests/test_sweden_geocode_store_current_mv.py` still pins it fails that one (deleted in Task 2, Step 4, together with the `SERVING_COLUMNS` half of its comparison). Task 4 is independent of Tasks 1-3 and may be done first if a reviewer prefers, but Task 5 needs all four.
- **The `sweden_company` package after this slice.** What is left of it: `assets.py`, `clickhouse.py`, `clickhouse_streaming.py`, `companies_current.py`, `companies_current_asset.py`, `centroid_assets.py`, `centroid_derivation.py`, `centroid_keys.py`, `geocode_store.py`, `address_resolution_policy.py`, `address_resolution_shadow.py` (reference builders only), `identity.py`, `normalized_duckdb.py`, `raw_duckdb.py`, `resources.py`, `tables.py`, `translation.py`, `docs/`. `address_resolution_shadow.py` keeping its name while holding only reference-document builders is a wart worth a rename in a later slice; renaming it here would touch `geocode.py`, `batch.py` and four tests for no functional gain, so it is deliberately out of scope.
