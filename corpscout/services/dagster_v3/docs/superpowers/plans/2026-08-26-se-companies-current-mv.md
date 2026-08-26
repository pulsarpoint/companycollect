# se_companies_current serving MV — plan (scope A)

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** A per-company denormalized refreshable materialized view `corpscout.se_companies_current` (one row per company, addresses as a JSON array, geocode pre-computed) so the companies/geocoding admin surfaces read a plain table sub-second instead of recomputing FINAL merges + joins on every request (currently ~20s).

**Architecture:** Refreshable MV (the migration-000320 pattern) that materializes ONCE per refresh: the FINAL merges on se_company_address + se_company_info, the primary-address pick, the LEFT JOIN to se_address_geocodes_served, and the per-company JSON address aggregation. Serving reads the plain MergeTree result.

**Spec:** owner-approved in chat 2026-08-26 (scope A: company + addresses + geocode summary; name se_companies_current; datatype-presence columns are a later scope-B extension).

## Global Constraints
- Grain: one row per company (SE only for now); ORDER BY company_id.
- addresses = JSON array via toJSONString(groupArray(map(...))) — companies have 1-2 current addresses (verified). Each address element: street_address, postal_code, city, address_type, address_id, geocode_status, geocode_precision, geocode_provider, latitude, longitude — geocode fields enriched from corpscout.se_address_geocodes_served (LEFT JOIN on address_id; provider/precision/coords).
- Company-level geocode summary columns for the list status/badge: primary_geocode_class (the coarse-aware class of the PRIMARY address — visiting_or_postal > visiting > address_key, same primary-pick rule the geocoding list uses), primary_geocode_precision, primary_latitude, primary_longitude, primary_geocode_provider.
- legal_name from corpscout.se_company_info FINAL (INNER — every addressed company has one, 0 orphans verified).
- REFRESHABLE MV, staged-swap + SYSTEM WAIT VIEW exactly per migration 000320; refresh EVERY 1 HOUR.
- Migration: next free number after 325 (→ 326; the served view is 325). No DROP in up (staging/RENAME per 000320). Register in EXPECTED_MIGRATIONS. NOTE the untracked Ratsit-322 WIP: my committed EXPECTED must NOT include 322 (already removed); a clean checkout is self-consistent.
- The refresh's coarse-aware class must match the backoffice's GEOCODE_STATUS_CLASS_EXPR semantics (provider='centroid_fallback' → 'coarse' BEFORE the geocoded-status check).

---

### Task 1: se_companies_current SELECT builder + drift-pinnable SQL
Files: create src/dagster_v3/defs/sweden_company/companies_current.py (build_se_companies_current_sql() -> str); test tests/test_se_companies_current_sql.py (clickhouse-local: seed se_company_address + se_company_info + se_address_geocodes_served; assert one row per company, addresses JSON has the right elements incl. a coarse-overlaid address, primary_geocode_class is coarse-aware and picks the primary address, legal_name joined). Reuse the primary-pick + coarse-class logic already in geocode_serving_overlay.py / the backoffice class expr — keep the class vocabulary identical (geocoded/coarse/ambiguous/unmatched/no_outcome).

### Task 2: migration — se_companies_current as a refreshable MV
Files: create corpscout/clickhouse/migrations/000326_corpscout_se_companies_current.{up,down}.sql (refreshable MV over build_se_companies_current_sql(), staged-swap + SYSTEM WAIT VIEW per 000320; ENGINE MergeTree ORDER BY company_id; REFRESH EVERY 1 HOUR); register in EXPECTED_MIGRATIONS; drift-pin test (migration body == build_se_companies_current_sql(), anti-vacuous, per 000320's pin). Verify on throwaway CH 26.5: refresh populates, one row per company, JSON addresses present. Also define a refresh-health check function on system.view_refreshes for this view (mirror sweden_address_geocodes_serving_view_refresh_check) — Task 2b attaches it to the sweden_companies_current_clickhouse asset.

### Task 2b: Dagster asset that refreshes se_companies_current after the weekly
Files: create src/dagster_v3/defs/sweden_company/companies_current_asset.py (asset sweden_companies_current_clickhouse); modify address_geocoding_assets.py to register it in defs + the weekly job selection; test tests/test_se_companies_current_asset.py (FakeClient).
The asset runs `SYSTEM REFRESH VIEW corpscout.se_companies_current` then `SYSTEM WAIT VIEW corpscout.se_companies_current` (force an immediate refresh + block until the materialization lands), so the companies view is fresh right after the upstream data updates rather than waiting up to the hourly auto-refresh. deps = the assets that produce its freshest inputs: sweden_geocode_centroids_clickhouse (centroids) + sweden_address_geocode_store_clickhouse (the store append that the se_address_geocodes_current MV refreshes from) — so this asset runs LAST, after geocode + centroid data is current. Pool = none needed (a SYSTEM statement, not the DuckDB pool). Attach the refresh-health check (from Task 2, on system.view_refreshes for se_companies_current) to THIS asset. Wire into sweden_company_address_geocoding_weekly_job. Verify dg check defs loads with the asset + check in the weekly. Test: FakeClient asserts the asset issues SYSTEM REFRESH VIEW + SYSTEM WAIT VIEW against corpscout.se_companies_current and nothing else write-mode.

### Task 3: repoint the backoffice geocoding list to se_companies_current
Files: modify corpscout/services/backoffice/app/lib/se-company-geocoding-list.server.ts + tests. Replace the published/published_companies CTE (se_company_address FINAL + LIMIT 1 BY + se_company_info FINAL + served-view join) with a plain read of corpscout.se_companies_current (no FINAL, no joins): the list reads primary geocode fields + street/postcode/city (from the primary address or the JSON), the counts strip counts primary_geocode_class. Keep the exact same class vocabulary + filters + the coarse-before-geocoded correctness. Verify the counts match the pre-repoint numbers (coarse ~976,860, unmatched ~94k). Pin SQL; gates pnpm typecheck + react-router build + client-bundle grep + vitest. MEASURE: the counts query should now be sub-second.
