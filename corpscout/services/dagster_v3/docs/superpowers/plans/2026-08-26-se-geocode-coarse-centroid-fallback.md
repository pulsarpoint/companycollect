# SE Coarse-Centroid Fallback (v8) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** For SE addresses the precise matcher (v6/v7) leaves `unmatched`/`ambiguous` but which carry a postcode or city, serve a coarse postcode/city **centroid** coordinate, honestly labeled by precision and always ranked below a precise match.

**Architecture:** A Dagster asset derives robust postcode- and city-centroids from OSM address points (+ matched-geocode coords) into two ClickHouse reference tables; the geocode SERVING read LEFT-JOINs them to fill unmatched/ambiguous identities at read time (serving-overlay — the geocode *store* stays purely precise outcomes). Coarse coords carry `geocode_precision IN ('postcode','city')` and `geocode_provider='centroid_fallback'`.

**Tech Stack:** DuckDB (derivation, mirrors the existing OSM/canonical assets), ClickHouse 26.5 (reference tables + serving), Dagster (asset + checks), Python (clickhouse_driver).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-08-26-se-geocode-coarse-centroid-fallback-design.md`

## Global Constraints

- Coarse coords MUST carry `geocode_precision` ∈ {`postcode`,`city`} and `geocode_provider='centroid_fallback'`; NEVER a `matched_*`/exact status, and always ranked BELOW any precise match (a precise match, present or later, always wins).
- Serving-overlay architecture (ruled 2026-08-26): the geocode STORE (`se_address_geocodes`) is NOT written by this tier. The centroid tier maintains SEPARATE reference tables and fills unmatched/ambiguous at READ time.
- Fire ONLY for `match_status IN ('unmatched','ambiguous')`. Never override `postal_box`/`invalid_address`/`foreign_address`/`property_identifier`.
- Quality bar: a centroid requires ≥3 source points; use a ROBUST centroid (median of lat and of lon), NOT a plain mean; carry `point_count` and `spread_meters` (max haversine to the centroid) so a poor centroid can be filtered/downgraded.
- The city join key must PRESERVE Swedish letters (å ä ö). Do NOT reuse `_compact_text_sql`/`normalized_post_town` (they strip accents to spaces under ClickHouse's ASCII regex — the measured 16%-match bug). Do NOT change `_compact_text_sql` (the resolver depends on its accent-stripping).
- Migrations: no destructive DROP in an up-file (house rule); these are pure CREATE TABLE. Register in `tests/test_clickhouse_migrations.py` EXPECTED_MIGRATIONS.
- Read-only against prod during development; no Dagster run/deploy from tasks.

---

### Task 1: City/postcode normalization keys (accent-preserving)

**Files:**
- Create: `src/dagster_v3/defs/sweden_company/centroid_keys.py`
- Test: `tests/test_sweden_centroid_keys.py`

**Interfaces:**
- Produces: `city_key_sql(expr: str) -> str` and `postcode_key_sql(expr: str) -> str` — DuckDB/ClickHouse-portable SQL fragments producing the join keys. `city_key_sql` = `upper(trim(...))` with NFC-normalization but WITHOUT accent stripping (å/ä/ö survive); `postcode_key_sql` = digits-only compaction (`regexp_replace(..., '[^0-9]', '')`). Both used identically on the address side and the reference side so keys line up.

- [ ] **Step 1: Write the failing test**
```python
# tests/test_sweden_centroid_keys.py
import duckdb
from dagster_v3.defs.sweden_company.centroid_keys import city_key_sql, postcode_key_sql

def test_city_key_preserves_swedish_letters():
    con = duckdb.connect()
    for raw, expected in [("GÖTEBORG","GÖTEBORG"), (" Göteborg ","GÖTEBORG"),
                          ("Upplands Väsby","UPPLANDS VÄSBY"), ("trelleborg","TRELLEBORG")]:
        got = con.execute(f"select {city_key_sql('?')}", [raw]).fetchone()[0]
        assert got == expected, (raw, got)

def test_postcode_key_is_digits_only():
    con = duckdb.connect()
    for raw, expected in [("231 00","23100"), ("23100","23100"), ("  114 56 ","11456")]:
        assert con.execute(f"select {postcode_key_sql('?')}", [raw]).fetchone()[0] == expected
```

- [ ] **Step 2: Run test to verify it fails**
Run: `uv run pytest tests/test_sweden_centroid_keys.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Write minimal implementation**
```python
# src/dagster_v3/defs/sweden_company/centroid_keys.py
def city_key_sql(expr: str) -> str:
    # NFC-normalize + trim + uppercase, PRESERVING accents (å ä ö). No strip_accents.
    return f"upper(trim(nfc_normalize(coalesce({expr}, ''))))"

def postcode_key_sql(expr: str) -> str:
    return f"regexp_replace(coalesce({expr}, ''), '[^0-9]', '', 'g')"
```
(If `nfc_normalize` is unavailable in the target engine, fall back to `upper(trim(coalesce({expr},'')))` — verify in the test which the workbench DuckDB and ClickHouse 26.5 both accept, and use the portable form.)

- [ ] **Step 4: Run test to verify it passes**
Run: `uv run pytest tests/test_sweden_centroid_keys.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add src/dagster_v3/defs/sweden_company/centroid_keys.py tests/test_sweden_centroid_keys.py
git commit -m "feat(se-geocode): accent-preserving city/postcode centroid join keys"
```

---

### Task 2: Robust centroid derivation (pure DuckDB SQL, ported from the experiment)

**Files:**
- Create: `src/dagster_v3/defs/sweden_company/centroid_derivation.py`
- Test: `tests/test_sweden_centroid_derivation.py`
- Reference: `scripts/geocode_centroid_coverage_experiment.py` (proven derivation logic — read it)

**Interfaces:**
- Consumes: `city_key_sql`, `postcode_key_sql` from Task 1.
- Produces:
  `replace_postcode_centroids(connection, *, source_points_table: str, out_table: str, min_points: int = 3) -> int`
  and `replace_city_centroids(connection, *, source_points_table: str, out_table: str, min_points: int = 3) -> int`.
  Each `create or replace table {out_table}` with columns `(key VARCHAR, latitude DOUBLE, longitude DOUBLE, point_count BIGINT, spread_meters DOUBLE)`, one row per key with ≥ `min_points`, using MEDIAN(lat)/MEDIAN(lon) as the robust centroid and max-haversine as `spread_meters`. Returns row count. `source_points_table` has columns `postcode`, `post_town`, `latitude`, `longitude`.

- [ ] **Step 1: Write the failing test** — assert the robust centroid ignores an outlier and the min-points gate holds.
```python
# tests/test_sweden_centroid_derivation.py
import duckdb
from dagster_v3.defs.sweden_company.centroid_derivation import replace_city_centroids

def _seed(con):
    con.execute("create table pts(postcode varchar, post_town varchar, latitude double, longitude double)")
    # Trelleborg: 3 tight points + 1 gross outlier -> median must ignore the outlier
    con.execute("""insert into pts values
      ('23100','Trelleborg',55.375,13.150),('23139','Trelleborg',55.377,13.152),
      ('23132','Trelleborg',55.373,13.148),('23100','Trelleborg',59.000,18.000),
      ('11456','Stockholm',59.339,18.05),('11457','Stockholm',59.341,18.06)""")  # 2 pts -> below N>=3

def test_city_centroid_is_robust_and_gated():
    con = duckdb.connect(); _seed(con)
    n = replace_city_centroids(con, source_points_table="pts", out_table="cc", min_points=3)
    rows = {r[0]: r for r in con.execute("select key, latitude, longitude, point_count from cc").fetchall()}
    assert "STOCKHOLM" not in rows            # 2 points < 3 -> excluded
    assert "TRELLEBORG" in rows
    lat, lon = rows["TRELLEBORG"][1], rows["TRELLEBORG"][2]
    assert 55.37 < lat < 55.38 and 13.14 < lon < 13.16   # median, outlier ignored
    assert rows["TRELLEBORG"][3] == 4
```

- [ ] **Step 2: Run test to verify it fails** — `uv run pytest tests/test_sweden_centroid_derivation.py -v` → FAIL (module missing).

- [ ] **Step 3: Write minimal implementation** — median centroid + max-haversine spread + N gate. (Port the haversine + median SQL from `scripts/geocode_centroid_coverage_experiment.py`; use DuckDB `median()`.)
```python
# src/dagster_v3/defs/sweden_company/centroid_derivation.py
from dagster_v3.defs.sweden_company.centroid_keys import city_key_sql, postcode_key_sql

_HAVERSINE = ("6371000 * 2 * asin(sqrt(pow(sin(radians(latitude - clat)/2),2)"
              " + cos(radians(clat))*cos(radians(latitude))*pow(sin(radians(longitude - clon)/2),2)))")

def _replace(connection, key_sql, source_points_table, out_table, min_points):
    connection.execute(f"""
        create or replace table {out_table} as
        with base as (
            select {key_sql} as key, latitude, longitude
            from {source_points_table}
            where latitude is not null and longitude is not null and {key_sql} != ''
        ), centroids as (
            select key, median(latitude) as clat, median(longitude) as clon, count(*) as point_count
            from base group by key having count(*) >= {int(min_points)}
        )
        select c.key, c.clat as latitude, c.clon as longitude, c.point_count,
               max({_HAVERSINE}) as spread_meters
        from base b join centroids c using(key)
        group by c.key, c.clat, c.clon, c.point_count
    """)
    return connection.execute(f"select count(*) from {out_table}").fetchone()[0]

def replace_postcode_centroids(connection, *, source_points_table, out_table, min_points=3):
    return _replace(connection, postcode_key_sql("postcode"), source_points_table, out_table, min_points)

def replace_city_centroids(connection, *, source_points_table, out_table, min_points=3):
    return _replace(connection, city_key_sql("post_town"), source_points_table, out_table, min_points)
```

- [ ] **Step 4: Run test to verify it passes** — `uv run pytest tests/test_sweden_centroid_derivation.py -v` → PASS.

- [ ] **Step 5: Commit**
```bash
git add src/dagster_v3/defs/sweden_company/centroid_derivation.py tests/test_sweden_centroid_derivation.py
git commit -m "feat(se-geocode): robust postcode/city centroid derivation (median + N>=3 + spread)"
```

---

### Task 3: ClickHouse reference tables (migrations)

**Files:**
- Create: `corpscout/clickhouse/migrations/0003XX_corpscout_se_postcode_centroids.{up,down}.sql`
- Create: `corpscout/clickhouse/migrations/0003XX_corpscout_se_city_centroids.{up,down}.sql`
- Modify: `tests/test_clickhouse_migrations.py` (EXPECTED_MIGRATIONS)

**Interfaces:**
- Produces: `corpscout.se_postcode_centroids` and `corpscout.se_city_centroids`, each
  `(key String, latitude Float64, longitude Float64, point_count UInt32, spread_meters Float64, source_snapshot_at DateTime64(3,'UTC')) ENGINE = MergeTree ORDER BY key`.

- [ ] **Step 1:** Pick the next two free migration numbers (`ls corpscout/clickhouse/migrations | tail`). Write both up-files as `CREATE DATABASE IF NOT EXISTS corpscout;` + `CREATE TABLE IF NOT EXISTS corpscout.se_postcode_centroids (...columns above...) ENGINE = MergeTree ORDER BY key;` (and city). Down-files: `DROP TABLE IF EXISTS ...`.
- [ ] **Step 2:** Add both to `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`, in order.
- [ ] **Step 3: Run the migration ledger tests** — `uv run pytest tests/test_clickhouse_migrations.py -v` → PASS (files explicit, down-files present, verbs allowed).
- [ ] **Step 4: Commit**
```bash
git add corpscout/clickhouse/migrations/0003*_se_*centroids.*.sql corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(se-geocode): se_postcode_centroids + se_city_centroids reference tables"
```
(Migrations are NOT applied by tasks — the controller applies them at deploy, per the house rule.)

---

### Task 4: Dagster asset — derive + publish centroids, wired into the weekly

**Files:**
- Create: `src/dagster_v3/defs/sweden_company/centroid_assets.py`
- Modify: `src/dagster_v3/defs/sweden_company/address_geocoding_assets.py` (add the asset to `sweden_company_address_geocoding_weekly_job` selection + `defs`)
- Test: `tests/test_sweden_centroid_assets.py` (FakeClient-style, pin SQL + publish path)

**Interfaces:**
- Consumes: Task 2 derivation fns; the OSM DuckDB tables (`sweden_address_osm.address_points`, columns include `postcode`, `city`, `latitude`, `longitude`); the staged-EXCHANGE publish helper (`export_duckdb_connection_table_to_clickhouse` / the pattern in `sweden_address_osm/…` and `sweden_company/clickhouse.py`).
- Produces: asset `sweden_geocode_centroids_clickhouse` (deps=[`sweden_osm_addresses_duckdb`]) that builds both centroid tables in DuckDB and publishes them to CH via staged EXCHANGE, stamped with the OSM snapshot time.

- [ ] **Step 1: Write the failing test** — a FakeClient asserting the asset derives from `address_points` and publishes both tables via staged EXCHANGE (mirror `tests/test_sweden_company_address_geocoding.py` fakes). Assert `city` from OSM maps to `post_town` for `replace_city_centroids` (adapt the source SELECT so `address_points.city` feeds the `post_town` column the derivation expects).
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** the asset: read `address_points` into a staging shape `(postcode, post_town:=city, latitude, longitude)`, call `replace_postcode_centroids`/`replace_city_centroids`, publish each to `corpscout.se_postcode_centroids`/`se_city_centroids` via the staged EXCHANGE helper. Add `pool=tables.DUCKDB_POOL`. Register in `defs` and add to the weekly job selection.
- [ ] **Step 4: Run** the asset test + `uv run dg check defs` → PASS / defs load.
- [ ] **Step 5: Commit**
```bash
git add src/dagster_v3/defs/sweden_company/centroid_assets.py src/dagster_v3/defs/sweden_company/address_geocoding_assets.py tests/test_sweden_centroid_assets.py
git commit -m "feat(se-geocode): centroid publish asset wired into the weekly"
```

---

### Task 5: Serving overlay — fill unmatched/ambiguous with the finest centroid

**Files:**
- Create: `src/dagster_v3/defs/sweden_company/geocode_serving_overlay.py`
- Test: `tests/test_geocode_serving_overlay.py` (clickhouse-local, seed store + centroids)

**Interfaces:**
- Consumes: `build_current_geocodes_sql` (geocode_store.py) as the precise base; the two centroid tables; `city_key_sql`/`postcode_key_sql`; `se_addresses_current` (for the address's postcode/post_town).
- Produces: `build_served_geocodes_sql() -> str` — one row per address_id = the precise current outcome if geocoded/servable, ELSE (status in unmatched/ambiguous) the finest available centroid: postcode-centroid if the address's postcode is in `se_postcode_centroids` AND its `spread_meters` ≤ `POSTCODE_SPREAD_MAX_METERS` (const, e.g. 3000), else city-centroid if present, else the original unmatched row. Overlay rows set `geocode_precision` ∈ {`postcode`,`city`}, `geocode_provider='centroid_fallback'`, and a status constant `GEOCODE_FALLBACK_STATUS='matched_area'` (existing non-GEOCODED-exact status — verify it exists in the status vocabulary; if a distinct status is wanted, that's a design note, default to `matched_area`).

- [ ] **Step 1: Write the failing test** — clickhouse-local: seed `se_address_geocodes` with (a) a geocoded identity, (b) an unmatched identity whose postcode is in `se_postcode_centroids`, (c) an unmatched identity whose postcode is absent but city is in `se_city_centroids` (the STAVSTENSV/Trelleborg case), (d) a `postal_box` identity. Assert `build_served_geocodes_sql()` yields: (a) unchanged precise, (b) postcode-precision centroid, (c) city-precision centroid, (d) unchanged (NOT overlaid). Assert precise ALWAYS wins over any centroid.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `build_served_geocodes_sql` as a LEFT-JOIN overlay over the precise current read; precise wins; the `matchStatus IN ('unmatched','ambiguous')` gate; postcode-spread threshold; finest-available selection.
- [ ] **Step 4: Run** → PASS (row-for-row).
- [ ] **Step 5: Commit**
```bash
git add src/dagster_v3/defs/sweden_company/geocode_serving_overlay.py tests/test_geocode_serving_overlay.py
git commit -m "feat(se-geocode): serving overlay fills unmatched/ambiguous with coarse centroids"
```

---

### Task 6: Backoffice — surface coarse precision distinctly

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/se-company-geocoding-list.server.ts` (+ the address tab reader) to read the served overlay and show `geocode_precision` (a "coarse: postcode"/"coarse: city" badge distinct from exact).
- Modify: `corpscout/services/backoffice/app/components/admin/se-company-geocoding-table.tsx` (badge/legend).
- Test: `corpscout/services/backoffice/tests/se-company-geocoding-list.server.test.ts` (pin the new precision column + that fallback rows read as coarse, not geocoded-exact).

- [ ] **Step 1:** Write the failing vitest pinning that the list surfaces `geocode_precision` and classifies `centroid_fallback` rows as a distinct "coarse" state (not the exact-geocoded green).
- [ ] **Step 2: Run** `npx vitest run tests/se-company-geocoding-list.server.test.ts` → FAIL.
- [ ] **Step 3:** Point the list/address reader at the served overlay (or the overlay view once published); add the coarse precision badge + legend; keep the existing exact/ambiguous/unmatched states.
- [ ] **Step 4: Gates** — `pnpm typecheck`; `npx react-router build` + `rg -l clickhouse build/client/assets` empty; the vitest file green.
- [ ] **Step 5: Commit**
```bash
git add corpscout/services/backoffice/app/lib/se-company-geocoding-list.server.ts corpscout/services/backoffice/app/components/admin/se-company-geocoding-table.tsx corpscout/services/backoffice/tests/se-company-geocoding-list.server.test.ts
git commit -m "feat(backoffice): surface coarse-centroid precision distinctly in the geocoding list"
```

---

### Task 7: Correctness check — rescued coords land in the right area

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/centroid_assets.py` (add an asset check)
- Test: `tests/test_sweden_centroid_assets.py`

**Interfaces:**
- Consumes: the centroid tables + a sample of rescued identities.
- Produces: asset check `centroid_fallback_lands_in_the_right_area` — for a sample of unmatched identities that received a postcode-centroid, assert the centroid is within a sane distance of the SAME address's city centroid (a postcode centroid wildly far from its own city centroid ⇒ bad data); fail if > `AREA_SANITY_METERS` (e.g. 50_000 = 50km) for > 1% of the sample. Also fail on any centroid with `point_count < 3` (invariant) or an empty table.

- [ ] **Step 1:** Write the failing test forcing the check's negative branch (a postcode centroid 200km from its city centroid → check fails).
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3:** Implement the check + predicate.
- [ ] **Step 4: Run** the check tests + `uv run ruff check`; `uv run dg check defs`.
- [ ] **Step 5: Commit**
```bash
git add src/dagster_v3/defs/sweden_company/centroid_assets.py tests/test_sweden_centroid_assets.py
git commit -m "feat(se-geocode): centroid fallback right-area correctness check"
```

---

## Deploy / rollout (controller, after tasks + review)
1. Deploy dagster (worktree recipe). 2. Apply the two centroid migrations (`make clickhouse-migrate-up`). 3. Materialize `sweden_geocode_centroids_clickhouse` once (populates the reference tables). 4. Point the serving MV / backoffice at `build_served_geocodes_sql` (a follow-up decision: refresh the `se_address_geocodes_current` MV definition to the overlay, or add a sibling served view — decide at deploy). No rematch needed — the overlay is a read-time fill.

## Self-review notes
- Spec coverage: precision labeling (Global Constraints + Task 5), centroid ladder (Task 5 finest-available), quality bar (Task 2 N≥3/median/spread), diacritic fix scoped as a dedicated city key not a global change (Task 1 + constraint), correctness/wrong-area (Task 7), serving-overlay architecture (Task 5, ruled). Prerequisite "diacritic fix" is realized as Task 1's accent-preserving key (the safe form — the global `normalized_post_town` bug is noted for a separate scoped fix, since `_compact_text_sql` is shared with the resolver).
- Open item for the executor to confirm at Task 5: whether `matched_area` is the right status for fallback rows or a new distinct status is preferred (default `matched_area` + `geocode_precision` disambiguates); and at deploy, MV-repoint vs sibling-view.
