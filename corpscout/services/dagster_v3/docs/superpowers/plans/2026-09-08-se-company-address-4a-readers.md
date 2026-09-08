# SE Company Address Slice 4a: Readers, Warm Schedule, Postcode-only Addresses — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the parity gap (postcode-only addresses), make the geocode cache stay warm across OSM refreshes, and switch every reader of the old address chain to the new entity table while the old chain keeps running, so slice 4b can rename and stop it.

**Architecture:** Three independent lines. (1) Normalizer v3 publishes a valid postcode with a town and no street or box as a `partial` address, keeping the care-of; the fold's twin rule merges duplicates and the centroid overlay geocodes them. (2) The warm step joins the weekly geocoding job downstream of the OSM extract with a production-proven chunk size. (3) Readers move one by one: the served company view `se_companies_serving` (a staged-swap migration), three dbt models, the publish reconciliation count, the centroid postcode-to-city source, and three backoffice modules, each reading `se_company_address_v2` active rows with an explicit column mapping; none of them joins the geocode serving view any more, so slice 4b can retire it with the old MV.

**Tech Stack:** Python 3.14 / Dagster 1.13.9 (`uv run --frozen --no-sync`), ClickHouse 26.5 (migrations via golang-migrate, `make -C corpscout clickhouse-migrate-up-one`), dbt (finland_ytj-style parse checks), React Router v7 backoffice (vitest, `npm run typecheck`).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md`, sections 4 (normalizer), 6 (warm step, amended 2026-09-07), 9 (parity, reader switch, retirement), 10. Owner decisions 2026-09-08: publish postcode-only addresses as partial rows with the postcode centroid; keep the exact hit rule and automate the warm after each OSM refresh (option 1).

## Global Constraints

- Dagster commands from `corpscout/services/dagster_v3` with `uv run --frozen --no-sync ...` and `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix`; `uv run --frozen --no-sync dg check defs` before every commit touching `src/`. The two dbt projects that the deploy parses (`finland_ytj`, `exchange_rates_v2`) are not the ones edited here; for the edited projects run `uv run --frozen --no-sync dbt parse --project-dir <project> --profiles-dir <project>` (company_serving: `src/dagster_v3/defs/company_serving/dbt`; company_domain_suggestions: `src/dagster_v3/defs/company_domain_suggestions/dbt`) and `dg check defs`, which loads the dbt manifests.
- Backoffice commands from `corpscout/services/backoffice`: `npx vitest run <files>` and `npm run typecheck`.
- The old chain keeps running throughout this slice: nothing here stops an asset, drops a table or renames one. `se_company_address_v2` keeps its name until slice 4b; every new reader names it through one constant so the rename is one edit per module.
- New-table reads: `FINAL` and `active = 1` for published rows; `FixedString(64)` keys through `toString`; nullable components through `ifNull(..., '')`; kinds/sources are arrays.
- The serving view migration is 000392 (000391 went to the basic-info re-base that landed on main first) and follows the staged-swap recipe of migration 000377 exactly (SYSTEM STOP VIEW on the live view, CREATE `_next` with the 000366 hourly cadence, SYSTEM WAIT VIEW, one atomic RENAME; it also drops the `_retired` name 000391's own swap left occupied, which 000345/000348 each did as a follow-up migration); the drift pin `tests/test_se_companies_serving_mv.py` is retargeted at 000392 and the executable suite `tests/test_se_companies_serving_sql.py` extended.
- Non-nullable String columns never receive None/null; every SELECT that binds a page of ids passes the module's query settings.
- Commit by explicit path only; trailers in this order at the end of every commit message:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`
- Pre-existing unrelated failures: `tests/test_schedule_cron_contracts.py` (four collisions), `test_sweden_address_geocoding.py::test_lantmateriet_credentials_are_documented_without_values`; backoffice `admin-se-company-esef.test.tsx`, `address-quality.test.ts` and the live-ClickHouse timeouts under the full suite.

## Column mapping (old chain → `corpscout.se_company_address_v2`)

| old | new |
| --- | --- |
| `se_company_address.is_current` / `se_company_addresses_current.has_address = 1 AND has_observation = 1` | `active = 1` |
| `address_key` / `address_fingerprint` | `address_key` (`FixedString(64)`, read through `toString`) |
| `address_type` (one row per type) | `kinds` (array on one row); primary pick `has(kinds, 'visiting_or_postal') DESC, has(kinds, 'visiting') DESC, address_key ASC` |
| `street_address` | the line's street part: `replaceRegexpOne(normalized_address, ',\\s*[0-9]{3} [0-9]{2}[^,]*$', '')` (keeps a `c/o` prefix) |
| `postal_code`, `city`, `country_code` | same names (`ifNull(...,'')` on the two nullables) |
| `sources` (array of strings) | `sources` (array of `LowCardinality(String)`, `arrayMap(x -> toString(x), sources)`) |
| `source_record_uids` | not on the row; a link goes through `se_company_address_suggestion FINAL` on `(company_id, source, slot)` from `arrayZip(sources, slots)` |
| `observed_at` / `resolved_at` | `folded_at` |
| `address_id` + served join (`latitude`, `longitude`, `geocode_precision`, `geocode_provider`) | on the row: `latitude`, `longitude`, `geocode_status`, `geocode_precision`; `geocode_provider` derived: `multiIf(geocode_status = 'matched_area', 'centroid_fallback', latitude IS NULL, '', 'osm')` |
| `normalized_address` (`street\|postcode\|city\|se`) | `normalized_address` (display line) |

---

### Task 1: Normalizer v3 publishes postcode-only addresses as partial rows

**Files:**
- Modify: `src/dagster_v3/defs/se_company/address/normalize_se.py`, `tests/fixtures/se_addresses/golden.jsonl`, `tests/test_se_company_address_normalize_se.py`, `tests/test_se_company_address_fold.py`, `tests/test_se_company_address_geocode.py`, spec section 4 (one paragraph) and `src/dagster_v3/defs/se_company/address/docs/address-design.md`.

**Interfaces:**
- Consumes: `normalize_se_address(raw) -> NormalizedAddress`, `display_line`, `_INVALID_POSTCODES`, `_UNKNOWN_TOWNS`; `fold._Candidate.partial_compatible` and the twin join (`fold.py`); `geocode._input_row`/`search_text`/`address_kind`.
- Produces: `NORMALIZER_VERSION = "se-address-normalizer-v3"`; a `partial` result for a valid postcode + known town + no street/box, with `parse_notes` containing `no street or box` and the care-of kept.

Rules:
1. In `normalize_se_address`, where `has_location` is False today: if the postcode is valid (five digits, not in `_INVALID_POSTCODES`) AND the town is present and not in `_UNKNOWN_TOWNS`, return a `partial` row with `care_of` (may be None), `postal_code`, `city`, every other component None, `normalized_address = display_line(care_of=..., box=None, street_name=None, house_number=None, unit=None, postal_code=code, city=town, care_of_display=..., city_display=...)` (so the line reads `c/o Axfast AB, 164 87 Stockholm` or `106 40 Stockholm`), `parse_notes` = the collected notes plus `no street or box`. Everything else that lacks a location stays `no_address` as today.
2. `NORMALIZER_VERSION = "se-address-normalizer-v3"`.
3. Corpus additions (append to `golden.jsonl`, each generated by running the new normalizer and checked by hand): SCB `care_of "SEB, STIFTELSER & FÖRETAG"`, `postal_code "10640"`, `post_town "STOCKHOLM"` → partial, care_of `seb, stiftelser & företag`, city `stockholm`, line `c/o Seb, Stiftelser & Företag, 106 40 Stockholm`; Bolagsverket `raw_address "$c/o AxFast AB$STOCKHOLM$16487$SE-LAND"` → partial, care_of `axfast ab`, line `c/o AxFast AB, 164 87 Stockholm`; Bolagsverket `"$$STOCKHOLM$10011$SE-LAND"` → partial, no care_of, line `100 11 Stockholm`; Bolagsverket `"$$ADRESS SAKNAS$99999$SE-LAND"` → no_address; SCB `street_address "Okänd adress"`, `postal_code "00000"`, `post_town "OKÄND"` → no_address; Ratsit `street_address ""`, `postal_code "22104"`, `post_town "Lund"` → partial. The 61 existing cases stay byte-identical.
4. Fold tests: two sources delivering the same postcode-only address publish ONE row (the twin join); a postcode-only partial never joins a street candidate; `PublishedAddress.needs_geocode()` is True for it and `as_normalized_address().parse_status == "partial"`.
5. Geocode test: `geocode_addresses` on a postcode-only partial is not rejected, `_input_row` carries an empty street and the postcode/city, and a scripted `unmatched` result plus a postcode centroid overlay yields `matched_area` with precision `postcode` (reuse the existing overlay fixtures).
6. Spec section 4: one sentence under the normalizer rules ("Amended 2026-09-08: a valid postcode with a known town and no street or box is a `partial` address; the old chain published these 27,786 companies and the new one now does too, geocoded to the postcode centroid"). Docs: the same in `address-design.md`.

- [ ] Steps: write the failing corpus/fold/geocode tests → run `pytest tests/test_se_company_address_normalize_se.py tests/test_se_company_address_fold.py tests/test_se_company_address_geocode.py tests/test_se_company_address_normalize.py -q` (FAIL) → implement → PASS → `dg check defs` → commit by explicit path: `feat(dagster): normalizer v3 publishes postcode-only addresses as partial rows`.

---

### Task 2: The warm step runs after every OSM refresh

**Files:**
- Modify: `src/dagster_v3/defs/se_company/address/warm.py` (`CHUNK_SIZE = 150_000` with the OOM note: 500,000 exhausted DuckDB's 97 GiB on 2026-09-07), `src/dagster_v3/defs/se_company/address/assets.py` (`se_address_geocodes_warm` gets `deps=[dg.AssetKey("sweden_osm_addresses_duckdb")]`), `src/dagster_v3/defs/sweden_company/address_geocoding_assets.py` (add the string `"se_address_geocodes_warm"` to `sweden_company_address_geocoding_weekly_job`'s `AssetSelection.assets(...)` list right after `GEOCODE_STORE_ASSET_KEY`; extend the schedule description with "then warms the address entity's geocode cache for the new extract"), tests `tests/test_se_company_address_assets.py` (default chunk 150,000; the warm asset's deps contain the OSM key) and `tests/test_sweden_company_address_geocoding.py` (the weekly job's selection includes `se_address_geocodes_warm`; find how that test enumerates the selection today and extend it), spec section 6 (one sentence: the warm step is part of the weekly geocoding job, downstream of the OSM extract) and `address-design.md`.
- No import from `sweden_company` into `se_company.address` and none the other way beyond the string key (the job selection accepts asset names).

- [ ] Steps: failing tests → implement → `pytest tests/test_se_company_address_assets.py tests/test_se_company_address_warm.py tests/test_sweden_company_address_geocoding.py -q` → `dg check defs` → commit: `feat(dagster): warm the address geocode cache after every weekly OSM refresh`.

---

### Task 3: The served company view reads the new table

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/companies_current.py`; Create: `corpscout/clickhouse/migrations/000392_corpscout_se_companies_serving_address_entity.up.sql` and `.down.sql`; Modify: `tests/test_se_companies_serving_mv.py` (drift pin → 000392), `tests/test_se_companies_serving_sql.py` (executable suite: a fixture row in `se_company_address_v2` reaches `addresses`, `address_count`, `primary_*`; a hidden row does not), `tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS`).

**Rules:**
- `COMPANY_ADDRESS_TABLE = f"{CLICKHOUSE_DATABASE}.se_company_address_v2"`; delete `SERVED_GEOCODES_TABLE` and the LEFT JOIN. The `company_addresses` CTE becomes:

```sql
company_addresses AS (
  SELECT
    a.company_id AS company_id,
    toString(a.address_key) AS address_key,
    arrayStringConcat(arrayMap(x -> toString(x), a.kinds), ',') AS address_type,
    toUInt8(has(a.sources, 'bolagsverket')) AS from_bolagsverket,
    replaceRegexpOne(a.normalized_address, ',\\s*[0-9]{3} [0-9]{2}[^,]*$', '') AS street_address,
    ifNull(a.postal_code, '') AS postal_code,
    ifNull(a.city, '') AS city,
    toString(a.address_key) AS address_id,
    toString(a.geocode_status) AS geocode_status,
    toString(a.geocode_precision) AS geocode_precision,
    multiIf(a.geocode_status = 'matched_area', 'centroid_fallback', a.latitude IS NULL, '', 'osm') AS geocode_provider,
    a.latitude AS latitude,
    a.longitude AS longitude,
    has(a.kinds, 'visiting_or_postal') AS kind_visiting_or_postal,
    has(a.kinds, 'visiting') AS kind_visiting
  FROM corpscout.se_company_address_v2 AS a FINAL
  WHERE a.active = 1
)
```

- `_PRIMARY_ORDER_BY` = `company_id, kind_visiting_or_postal DESC, kind_visiting DESC, address_key ASC`. `ADDRESS_ELEMENT_KEYS` unchanged (the backoffice geocoding list reads them), `address_id` now carries the address key. `_geocode_class_expr` unchanged: with the derived provider a `matched_area` row classifies `coarse`, exactly as the overlaid rows did.
- Migration 000392: copy the structure of 000377's up file (the latest staged swap) and replace the SELECT with `build_se_companies_serving_sql()`'s output (the test pins the text); the down file recreates the 000377 definition the same way.
- `tests/test_se_companies_serving_sql.py`: add `se_company_address_v2` to the schema statements (migration 000384) and one company with one active street row (kinds `['postal']`, matched_exact, lat/lon) and one hidden row: assert `address_count = 1`, `primary_geocode_class = 'geocoded'`, the JSON element's `address_id` equals the key, and a `matched_area` row classifies `coarse`.

- [ ] Steps: failing tests → implement → `pytest tests/test_se_companies_serving_mv.py tests/test_sweden_company_assets.py tests/test_clickhouse_migrations.py -q` and the clickhouse-local `tests/test_se_companies_serving_sql.py -m integration` → `dg check defs` → commit: `feat(dagster): se_companies_serving reads the address entity`.

---

### Task 4: dbt models, the publish reconciliation count, the centroid source

**Files:**
- Modify: `src/dagster_v3/defs/company_serving/dbt/models/company_section_presence_current_build.sql`, `.../company_section_item_source_links_build.sql`, the project's `sources.yml` (add `se_company_address_v2` and `se_company_address_suggestion` under the `corpscout` source), `src/dagster_v3/defs/company_domain_suggestions/dbt/models/staging/stg_se_company_match_features.sql` and its `sources.yml`, `src/dagster_v3/defs/company_serving/publish.py` (the `addresses` reconciliation count), `src/dagster_v3/defs/sweden_company/centroid_assets.py` (`POSTCODE_CITY_MAP_SQL`), their tests (`tests/test_company_serving*.py` files that pin these SQL texts, `tests/test_sweden_centroid_assets.py`).

**Rules:**
- Presence: `SELECT '{{ var("country_code") }}', company_id, 'addresses', toString(address_key), folded_at FROM {{ source('corpscout', 'se_company_address_v2') }} FINAL WHERE active = 1`.
- Source links:

```sql
addresses AS (
    SELECT
        '{{ var("country_code") }}' AS country_code, published.company_id, 'addresses' AS section,
        toString(published.address_key) AS item_key, raw.source_record_uid,
        'registry_address' AS relationship_kind,
        'registry_company_record' AS match_method,
        toFloat32(1) AS match_confidence, published.source_run_id, published.folded_at AS linked_at
    FROM (
        SELECT company_id, address_key, source_run_id, folded_at, member.1 AS source, member.2 AS slot
        FROM {{ source('corpscout', 'se_company_address_v2') }} FINAL
        ARRAY JOIN arrayZip(arrayMap(x -> toString(x), sources), slots) AS member
        WHERE active = 1
    ) AS published
    INNER JOIN {{ source('corpscout', 'se_company_address_suggestion') }} AS raw FINAL
        ON raw.company_id = published.company_id AND toString(raw.source) = published.source AND raw.slot = published.slot
    WHERE raw.source_record_uid != ''
)
```

- Match features: from `se_company_address_v2 FINAL WHERE active = 1 AND normalized_address != ''`: `normalized_value` = the existing normalization expression applied to `concat(street part, ' ', postal_code, ' ', city)` where the street part is the `replaceRegexpOne` of the mapping table, `raw_value = normalized_address`, `source_field = concat(toString(text_source), '.', arrayStringConcat(arrayMap(x -> toString(x), kinds), '|'))`.
- Reconciliation count: `SELECT countDistinct(tuple(addresses.company_id, addresses.address_key)) FROM corpscout.se_company_address_v2 AS addresses FINAL INNER JOIN (...anchors...) ON anchors.company_id = addresses.company_id WHERE addresses.active = 1`; the comment explains the switch.
- Centroid map: `FROM corpscout.se_company_address_normalized FINAL WHERE parse_status IN ('ok', 'partial') AND postal_code IS NOT NULL AND city IS NOT NULL` with `postcode_key_sql("postal_code")` and `city_key_sql("city")`.

- [ ] Steps: failing tests → implement → the two `dbt parse` commands → `pytest tests/test_company_serving*.py tests/test_sweden_centroid_assets.py -q` → `dg check defs` → commit: `feat(dagster): company serving, match features, reconciliation and centroids read the address entity`.

---

### Task 5: Backoffice readers

**Files (backoffice):**
- Modify: `app/lib/company-sections.server.ts` (`getAddressesSection`), `app/lib/address-quality.server.ts`, `app/lib/address-companies.server.ts`, and their tests (`tests/address-quality.test.ts` is pre-existing-failing: fix it as part of this task if the failure is in the SQL it pins; otherwise leave it and say so), `app/components/detail/contact-location-card.test.ts` if it pins the addresses section shape.

**Rules:**
- `getAddressesSection(companyId)`: read `se_company_address_v2 FINAL WHERE company_id = {id:String}` (active first, hidden and withdrawn after, so the detail page can show them with a badge if it already distinguishes) and map each row into the section's existing item shape: `full_address = normalized_address`, `address_id = toString(address_key)`, `address_country_code = country_code`, `address_is_foreign = geocode_status = 'foreign'`, `street_name`, `house_number`, `address_unit = unit`, `geocode_postal_code = postal_code`, members = `arrayZip(sources, slots)` with the raw text from `se_company_address_suggestion FINAL` for those (source, slot) pairs, geocode fields from the row (`geocode_status`, `geocode_provider` derived as in the mapping table, `geocode_precision`, `geocode_match_method = geocode_method`, `geocode_match_confidence = geocode_confidence`, `geocode_matched_at = geocoded_at`), and `''`/`0`/`[]` for the candidate and source-provenance fields the row does not carry. Keep the section's public type unchanged so the detail components need no edit; if a field must go, remove it from the type and the component together.
- `address-quality.server.ts`: the queue reads `se_company_address_v2 FINAL WHERE active = 1`; filters: `ambiguous` (`geocode_status = 'ambiguous'`), `unmatched`, `invalid` (`invalid_address`), `street_fallback` (`geocode_precision = 'street'`), `city_fallback` (`geocode_precision = 'city'`), `low_confidence` (`geocode_status = 'matched_exact' AND geocode_confidence < 0.8`); stats over the same table; search over `normalized_address`, `postal_code`, `city`, `toString(address_key)`; rows carry `address_id = toString(address_key)`, `display_address = normalized_address`, `representative_source = toString(text_source)`, `street_address` (the street part), `postal_code`, `post_town = city`, `address_kind = if(box != '', 'postal_box', 'physical')`, `company_count = 1`, `evidence_count = length(sources)`, `match_status = geocode_status`, `match_method = geocode_method`, `match_confidence = geocode_confidence`, `latitude`, `longitude`, `geocode_precision`, `coordinate_method = geocode_method`, `coordinate_locality = city`, `coordinate_supporting_point_count = 0`, `candidate_count = 0`, `candidate_record_urls = []`, `source_url = ''`, `source_snapshot_at = ''`, `matched_at = geocoded_at`; order as today with the new columns. Each row links to `/admin/se/company/<company_id>/address?address=<key>` (add `company_id` to the row type; update the route/component that renders the queue to link there instead of the old address id page).
- `address-companies.server.ts` (companies at the same building): the target is the company's primary active row (`has(kinds,'visiting_or_postal') DESC, has(kinds,'visiting') DESC, geocode_precision = 'building' DESC, address_key`) with `box = ''`; matches are active rows of other companies with the same `street_name`, `house_number` (unit ignored), `postal_code` and `country_code`; the result shape unchanged.

- [ ] Steps: failing tests → implement → `npx vitest run tests/address-quality.test.ts tests/address-companies.server.test.ts <the sections tests>` and `npm run typecheck` → commit: `feat(backoffice): address quality, same-building and company address section read the address entity`.

---

### Task 6: Prod run (controller)

1. [ ] Whole-branch review; merge; hot-sync the dagster host.
2. [ ] `se_company_address_normalize` (`changed_only: true`): the v3 bump selects every raw row (about 30 min). Readout: `parse_status` per source; expect about 28,000 more `partial` rows and the same fewer `no_address`.
3. [ ] Backfill `se_company_address_fold` bucket_00..bucket_63 (the normalizer bump selects every company): expect about 64 × 75 s plus the in-page matching of the roughly 28,000 new keys; readouts as in slice 2b; the postcode-only rows show `matched_area`/`postcode`.
4. [ ] Parity (spec 9) with the comparison key `replaceRegexpAll(lower(line), '[^0-9a-zåäö]', '')` on both sides, the new side built without the unit and the care-of, the old side from `normalized_address` with the trailing `|se` dropped: report identical / new superset / old superset / different per company, the old-only breakdown by parse status, and a 30-company sample of `different` reviewed by hand with each difference traced to a spec rule (units kept, hyphens and abbreviation dots preserved, trailing village words dropped, care-of separated); record the numbers in spec section 9.
5. [ ] Apply migration 000392 (`make -s -C <deploy-worktree>/corpscout clickhouse-migrate-up-one`), watch `system.view_refreshes` for the `_next` populate (the migrate client times out at about 35 s; verify, then `migrate force 392` if the ledger went dirty), confirm the backoffice companies and geocoding lists and the company detail addresses section render from the new data, and that the address-quality queue and same-building lookup work.
6. [ ] Record in the ledger and spec section 9 (slice 4a shipped); archive the ledger; update memory; hand the retirement list to slice 4b.

## Self-review

- Spec coverage: section 4 amendment (Task 1); section 6 warm scheduling (Task 2); section 9 reader switch: served company view (Task 3), the dbt and publish readers the explorer found beyond the spec's list (Task 4), the backoffice readers (Task 5); parity (Task 6). Retirement and rename stay in 4b by design.
- Placeholders: Task 1 lists the rule and the corpus cases with expected values; Tasks 3 and 4 carry the SQL; Task 5 carries the column mapping per module; the executable tests are named.
- Type consistency: `address_id` in the serving JSON now equals `toString(address_key)` everywhere (serving view, backoffice list, quality queue, detail section), and every module names the table through one constant for the 4b rename.
