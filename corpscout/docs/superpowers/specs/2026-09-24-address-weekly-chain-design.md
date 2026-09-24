# Weekly Sweden address chain: extract → normalize → OSM → warm → fold → serving refresh

**Date:** 2026-09-24
**Status:** approved in chat, awaiting spec review
**Owner decisions:** the address fold runs automatically every week (reversing the 2026-09-15
"fold stays manual" decision); geocoding happens before the fold, never after; run window
Tuesday 01:05 Europe/Stockholm; one job, not two.

## 1. Why

`se_company_address` (the published address entity, 3.9M active rows) was last folded on
2026-09-13. On 2026-09-15 the scheduled address weekly was removed in favour of backoffice-launched
jobs ("the fold itself stays manual", `se_company/address/docs/address-design.md`). Since then the
geocoding weekly (`sweden_company_address_geocoding_weekly_job`, Tuesdays 04:05 Stockholm) has
refreshed the OSM extract, re-geocoded all 2.14M location keys into the cache and forced the
companies serving view to refresh -- but the serving view reads `se_company_address`, which nothing
rewrites. Two weekly runs (09-15, 09-22) therefore changed nothing a user can see, and source
addresses collected since 09-13 were neither normalized nor published.

The fold is what writes coordinates onto published rows, reading the geocode cache. Geocoding
must therefore precede the fold: the one fold that ran without a warm cache (2026-09-07) took
4 h 18 m matching page by page; folds after a warm run buckets in minutes.

## 2. Current state (verified 2026-09-24)

| Item | Value |
|---|---|
| Weekly job | `sweden_company_address_geocoding_weekly_job` in `sweden_company/address_geocoding_assets.py`; selection: `sweden_osm_pbf_s3`, `sweden_osm_addresses_duckdb`, `sweden_geocode_centroids_clickhouse`, `se_address_geocodes_warm`, `sweden_companies_current_clickhouse`; schedule `sweden_company_address_geocoding_weekly`, cron `5 4 * * 2`, `Europe/Stockholm`, default RUNNING |
| Last run | 2026-09-22, SUCCESS, 69 min (osm 3.5 min, warm 45 min, centroids 13 s, serving refresh wait 18.5 min) |
| Address assets | `EXTRACTOR_ASSET_NAMES` = `se_company_address_suggestions_<source>` for each `EXTRACTOR_SOURCES` entry (scb, bolagsverket, ratsit, esef); `se_company_address_normalize` (pool `se_company_address_normalize`, deps = extractors); `se_address_geocodes_warm` (pool `sweden_address_osm_duckdb`, deps = `sweden_osm_addresses_duckdb` + normalize); `se_company_address_publish` (pool `sweden_address_osm_duckdb`, deps = normalize + warm, `AddressFoldConfig.changed_only` default True) |
| Centroids | `sweden_geocode_centroids_clickhouse`, pool `sweden_address_osm_duckdb`, deps = `sweden_osm_addresses_duckdb` |
| Serving refresh | `sweden_companies_current_clickhouse`, deps = centroids + warm + `se_company_domain_publish` (no dependency on the fold today) |
| Manual jobs | `se_company_address_sync_job` (extractors + normalize) and `se_company_address_refresh_job` (extractors + normalize + warm + publish), launched from the backoffice Processing page; `se_company_address_fold_companies` behind the Address tab's "Fold now" |
| Pool | `sweden_address_osm_duckdb` has the instance default limit of 1 op, so centroids, warm, publish, fold_companies and `sweden_osm_addresses_duckdb` serialize |
| Fold cost | `changed_only` marks a company stale when `geocode_reference` differs from the current OSM reference; every weekly extract changes it, so the weekly fold re-folds every company. Last full fold after a warm (2026-09-13) ran as bucket batches from 05:10 to 08:04 UTC |
| Last normalize | 2026-09-13 02:53 UTC (18 min) |

## 3. Job composition

`sweden_company_address_geocoding_weekly_job` keeps its name and selects, in dependency order:

1. `se_company_address_suggestions_scb`, `..._bolagsverket`, `..._ratsit`, `..._esef` (via `EXTRACTOR_ASSET_NAMES`)
2. `se_company_address_normalize`
3. `sweden_osm_pbf_s3`
4. `sweden_osm_addresses_duckdb`
5. `sweden_geocode_centroids_clickhouse` (the geocoder's fallback overlay reads the centroids, so they refresh before warming)
6. `se_address_geocodes_warm`
7. `se_company_address_publish` (default config: `changed_only=True`, default page size)
8. `sweden_companies_current_clickhouse`

Ordering is enforced by asset dependencies. One dependency is added: `sweden_companies_current_clickhouse`
gains `dg.AssetKey("se_company_address_publish")` so the serving refresh cannot start before the
fold has landed. All other dependencies already exist. Steps 4-7 share the single-slot DuckDB pool
and run one at a time; steps 1 and 3 may run in parallel with each other.

Expected wall time about 4 to 4.5 hours until the invalidation redesign (section 8): extract +
normalize ~30 min, OSM 4 min, warm 45 min, fold ~3 h, serving refresh wait 15-20 min.

## 4. Schedule

`sweden_company_address_geocoding_weekly` keeps its name; `cron_schedule` becomes `"5 1 * * 2"`
(Tuesday 01:05 Europe/Stockholm), `execution_timezone` unchanged, `default_status` RUNNING
unchanged. Keeping the name keeps the instance's schedule state and tick history. The job and
schedule descriptions describe the new chain.

## 5. Documentation

- `se_company/address/docs/address-design.md`: the sentence "the fold itself stays manual" and the
  note that the extract job and stopped weekly schedule were removed are replaced by the new rule:
  the weekly chain extracts, normalizes, refreshes OSM, warms, folds and refreshes the serving view
  every Tuesday 01:05 Stockholm; targeted folds and the backoffice jobs remain for on-demand work.
  The "Warm step" section's claim that the weekly "already normalizes before anything folds" becomes
  true again and is kept.
- `sweden_company/address_geocoding_assets.py` module docstring and `companies_current_asset.py`
  module docstring: describe the new chain and the new dependency.

## 6. Backoffice interplay

`se_company_address_sync_job`, `se_company_address_refresh_job` and "Fold now" stay as they are.
During the Tuesday run their DuckDB-touching steps wait for the pool rather than collide. The plan
verifies that every asset in the chain that opens the OSM DuckDB file carries
`pool=sweden_address_osm_duckdb` (the 2026-08-25 lock conflict came from two runs touching the file
outside a pool). `sweden_osm_pbf_s3` does not open DuckDB and stays unpooled.

## 7. Tests, deploy, verification

- `tests/test_sweden_address_geocoding_weekly.py`: pins the job's asset selection (the eleven asset names
  above: four extractors plus seven), the cron `5 1 * * 2`, the timezone, and that it is still the only job in that module.
- `tests/test_se_companies_current_asset.py`: the dependency test's expected parent set gains
  `se_company_address_publish`.
- A pool test: every asset in the selection whose resources include `sweden_address_osm_duckdb`
  declares `pool == "sweden_address_osm_duckdb"`.
- `uv run --frozen --no-sync dg check defs` in `services/dagster_v3`.
- Deploy: hot patch of the changed source files onto `/opt/companycollect/corpscout/dagster_v3`
  plus the GraphQL `reloadRepositoryLocation("dagster_v3")`, as done on 2026-09-21, because the tree
  carries other workstreams' uncommitted work; previous copies kept under
  `/var/backups/corpscout-dagster/`. Verify with `workspaceOrError.locationEntries` LOADED and the
  schedule's next tick.
- One manual launch of the job on the server off-hours (GraphQL `launchPipelineExecution`,
  repository `__repository__`, location `dagster_v3`) to prove the whole chain before Tuesday
  2026-09-29 01:05 Stockholm; success criteria: run SUCCESS, `max(folded_at)` on
  `corpscout.se_company_address` advances, the serving view's `last_success_time` is after the fold,
  `se_address_geocodes` gains exactly one new `reference_md5`.

## 8. Not in this change

- The cache-invalidation redesign (carry forward outcomes whose OSM neighbourhood did not change;
  fold staleness compares the cached outcome to the published row instead of the reference checksum).
  This is step two and turns the 4-hour Tuesday run into minutes.
- A sensor that relaunches a weekly root run canceled by a Dagster restart (run retries cover
  failures only).
- The hetzner01 path rename from the commoncrawl dissolve.

## 9. Risks

- A Dagster service restart between 01:05 and ~05:30 Stockholm on a Tuesday cancels the run; the
  warm step is resumable (chunks land in the cache) but the fold restarts from the first bucket.
- Reviewer "Fold now" launched during the run waits up to the remaining fold time.
- The weekly appends ~2.1M rows to `se_address_geocodes` and rewrites every `se_company_address`
  row's `geocode_reference` each week until step two lands.
