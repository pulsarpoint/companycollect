# dagster_v2

Second-generation Corpscout Dagster project (design:
`../docs/superpowers/specs/2026-06-12-dagster-source-pipeline-design.md`).
Separate project from `../dagster` carrying the full Dagster stack — own venv,
own image (`dagster-corpscout-v2:latest`), own compose (code server,
webserver on :3500, daemon) — sharing v1's Dagster Postgres, network name, and
code-location name, so run history carries over. The two stacks must not run
at the same time. The Python package keeps the name `dagster_corpscout`;
`../dagster` is deleted at cutover and this directory replaces it with no
renames.

## Development

```bash
python3.12 -m venv .venv
./.venv/bin/python -m pip install -e '.[dev]'
./.venv/bin/python -m pytest tests -v
```

Deploy: stop the v1 stack first (`cd ../dagster && docker compose down`), then
`docker compose build && docker compose up -d` here. The UI stays at :3500.

## Finland PRH XBRL (window archetype reference)

- `raw_xml_documents` — partitioned by registration month. Downloads only
  statements of **eligible companies** (active + website, queried from
  `fi_prhytj_company_explorer_cache`; the asset depends on
  `sources/finland/prh_ytj/company_explorer_cache`). Ineligible statements are
  recorded under `skipped` in `windows/<partition>/listing.json` — re-materialize
  a window to catch up companies that became eligible later (already-downloaded
  objects are reused, only new ones are fetched; set `refresh_existing: true`
  in run config to force re-downloads). Backfill months from the asset page;
  the monthly schedule (`finland_prh_xbrl_pull_window_schedule`, default
  STOPPED) keeps it current. Raw XML lands at
  `source-finland-prh-xbrl/companies/<business_id>/<financial_date>.xml`.
- `statement_tables` — re-parses the partition's XML from RustFS into the
  `fi_prh_xbrl_*` ClickHouse tables. Rebuildable any time without touching the
  PRH API (re-materialize after parser changes). All tables are
  ReplacingMergeTree — re-runs supersede, never duplicate. Query with FINAL.
- `financial_metrics` — curated metrics from explicit (concept, MCY) mappings
  in `metrics.py`.
- Layer cascade is automatic via `automation_condition_sensor` (enable it per
  location). On-demand single-company pull: job `finland_prh_xbrl_pull_company`.

## Finland PRH YTJ (snapshot archetype reference)

Ported from v1; the weekly schedule now triggers only `raw_snapshot`, and
normalized/code_lists/mapping/serving cascade via eager automation.

## Adding a source

```bash
./.venv/bin/python -m dagster_corpscout.source_scaffold <country> <source> \
  --sources-root dagster_corpscout/sources --archetype snapshot|window
```

Register the bundle in `dagster_corpscout/registry.py`; the conventions suite
(`tests/test_source_conventions.py`) enforces layout, layer vocabulary, and
stopped-by-default schedules.
