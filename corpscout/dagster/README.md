# Corpscout Dagster

Dagster owns dataset-level source refreshes and ClickHouse transformations.

## Finland PRH YTJ Jobs

Use `finland_prhytj_pipeline` only when you want a fresh full download from the
PRH YTJ API:

```text
source_system -> raw_snapshot -> normalized_tables -> code_lists
  -> industry_nace_mappings -> company_explorer_cache
```

`raw_snapshot` downloads about 819k companies from an API fixed at 100 records
per page, so a full pull can take hours. The step logs progress after page 1,
every 100 pages, and on the final page.

Do not queue repeated full materializations while one is already running. If a
full pull was started by mistake, cancel it before launching transform-only work.

Use `finland_prhytj_transform_latest` when you want to test or rerun ClickHouse
imports from the latest completed RustFS manifest without downloading again:

```text
normalized_tables -> code_lists -> industry_nace_mappings -> company_explorer_cache
```

This is the safer job for most development checks after a valid
`runs/{run_id}/manifest.json` already exists in `source-finland-prhytj`.

## Common Commands

Rebuild and restart Dagster services:

```bash
docker compose up -d --build --force-recreate
```

Run transforms from the latest manifest:

```bash
docker compose run --rm dagster-code \
  dagster job execute -m dagster_corpscout.definitions \
  -j finland_prhytj_transform_latest
```

Run a fresh full pull and transform:

```bash
docker compose run --rm dagster-code \
  dagster job execute -m dagster_corpscout.definitions \
  -j finland_prhytj_pipeline
```

## Source Package Convention

Each source lives under `dagster_corpscout/sources/<country>/<source>` and
exports a `source_bundle` from `__init__.py`. The registry imports source
packages from `registry.source_modules`; source names and asset key prefixes
must be unique.

Assets are grouped by country in Dagster, with source and layer carried as tags:

```text
group_name: country_finland
asset key:  sources/finland/prh_ytj/raw_snapshot
tags:       country=finland, source=prh_ytj, source_name=finland_prhytj, layer=raw
```

Create a new source package skeleton with:

```bash
dagster-corpscout-scaffold-source serbia apr
```

The scaffold creates `spec.py`, `jobs.py`, `schedules.py`, and the initial
`assets/external.py` and `assets/raw.py` modules. Register the package in
`dagster_corpscout/registry.py` only after the raw asset is implemented.
