# Weekly Sweden Address Chain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Sweden weekly geocoding job extract, normalize, refresh OSM, warm the geocode cache, fold (publish) all company addresses and then refresh the companies serving view, every Tuesday 01:05 Stockholm, so published coordinates actually update.

**Architecture:** The existing job `sweden_company_address_geocoding_weekly_job` grows from five to eleven assets; ordering comes from asset dependencies plus one new dependency (the serving refresh depends on the fold). The schedule keeps its name and moves to `5 1 * * 2`. No asset logic changes. Deploy is a hot patch of two source files plus a code-location reload, followed by one manual proof run on the server.

**Tech Stack:** Dagster 1.13.9 (`dg.define_asset_job`, `dg.ScheduleDefinition`, pools), Python 3.14 via `uv`, pytest, ClickHouse (verification queries), Dagster GraphQL on the dagster host.

**Spec:** `docs/superpowers/specs/2026-09-24-address-weekly-chain-design.md` (absolute: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/docs/superpowers/specs/2026-09-24-address-weekly-chain-design.md`)

## Global Constraints

- Working directory for every local command: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3`. Git root is `/Users/graovic/pulsarpoint/ppoint/companycollect`. The owner approved committing directly on `main` in this checkout. The tree holds ~290 unrelated dirty files from other workstreams: never `git add -A`, `git add .`, `git stash`, `git checkout -- .`, `git clean`. Stage only the paths each task names.
- Run Python through `uv run --frozen --no-sync ...`. Use `rg`, not `grep`. macOS `sed -i` needs `sed -i ''`; prefer the Edit tool with the exact old/new text given.
- The job selection, in this order of dependency: `se_company_address_suggestions_scb`, `se_company_address_suggestions_bolagsverket`, `se_company_address_suggestions_ratsit`, `se_company_address_suggestions_esef` (via `EXTRACTOR_ASSET_NAMES`), `se_company_address_normalize`, `sweden_osm_pbf_s3`, `sweden_osm_addresses_duckdb`, `sweden_geocode_centroids_clickhouse`, `se_address_geocodes_warm`, `se_company_address_publish`, `sweden_companies_current_clickhouse`. Eleven asset names.
- Schedule name stays `sweden_company_address_geocoding_weekly`; job name stays `sweden_company_address_geocoding_weekly_job`; cron becomes exactly `5 1 * * 2`; timezone stays `Europe/Stockholm`; default status stays RUNNING.
- The only new dependency edge: `sweden_companies_current_clickhouse` depends on `se_company_address_publish`. No other asset's `deps`, `pool`, config or body changes. `AddressFoldConfig` defaults (`changed_only=True`) are used as-is; the job passes no config.
- Every asset in the selection whose required resources include `sweden_address_osm_duckdb` must declare `pool="sweden_address_osm_duckdb"` (already true; the test pins it).
- Tests must not call `dagster_v3.definitions.defs()` (full load): it fails under pytest in this environment (a component needs env vars pytest does not load). Build module-scoped `dg.Definitions` as shown in Task 1. `uv run --frozen --no-sync dg check defs` DOES pass locally and is the full-load verification.
- Never run an Ansible playbook. Deploy = copy the two changed source files onto the host with `sudo -n install` (backup to `/var/backups/corpscout-dagster/`), run `dg check defs` on the host, then GraphQL `reloadRepositoryLocation`. Host access: `ssh dagster`, passwordless `sudo -n`; the live tree is `/opt/companycollect/corpscout/dagster_v3` (root-owned; `.env` and `.venv` readable only via sudo).
- The proof run holds the DuckDB pool for ~4.5 h. Launch it only when no backoffice address job is running and outside Stockholm office hours (before 07:00 or after 18:00 Europe/Stockholm), and before Tuesday 2026-09-29 01:05 Stockholm.
- Commit messages: Conventional Commits, ending with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.

---

### Task 1: The weekly job selects the full chain and runs at 01:05

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/address_geocoding_assets.py` (module docstring, `WEEKLY_CRON_SCHEDULE`, job selection and description, schedule description)
- Test: `tests/test_sweden_address_geocoding_weekly.py` (rewrite)

**Interfaces:**
- Consumes: `EXTRACTOR_ASSET_NAMES` from `dagster_v3.defs.se_company.address.assets` (a tuple of the four extractor asset names); `CENTROIDS_ASSET_KEY`, `COMPANIES_CURRENT_ASSET_KEY` already imported in the module.
- Produces: `WEEKLY_CRON_SCHEDULE == "5 1 * * 2"` and the eleven-asset job that Task 4 deploys and Task 5 launches. (Task 2 is independent of this task and does not import the test helper.)

- [ ] **Step 1: Rewrite the test file so it fails on the current job**

Replace the whole content of `tests/test_sweden_address_geocoding_weekly.py` with:

```python
"""The Sweden weekly address chain, as a shape guard.

Spec: docs/superpowers/specs/2026-09-24-address-weekly-chain-design.md. The weekly is the one
job that publishes Sweden addresses on a schedule: extract, normalize, refresh OSM, centroids,
warm the geocode cache, fold, then refresh the companies serving view. This file pins the
selection, the order-bearing dependency, the schedule, and that every DuckDB-touching step is
in the single-slot pool. It builds module-scoped Definitions rather than loading the whole
project: the full load needs env vars pytest does not carry (see `dg check defs` instead).
"""

import dagster as dg

from dagster_v3.defs.se_company.address import assets as address_assets
from dagster_v3.defs.sweden_address_osm import assets as osm_assets
from dagster_v3.defs.sweden_company import (
    address_geocoding_assets as weekly,
    centroid_assets,
    companies_current_asset,
)

WEEKLY_JOB = "sweden_company_address_geocoding_weekly_job"
WEEKLY_SCHEDULE = "sweden_company_address_geocoding_weekly"
DUCKDB_POOL = "sweden_address_osm_duckdb"

EXPECTED_SELECTION = {
    "se_company_address_suggestions_scb",
    "se_company_address_suggestions_bolagsverket",
    "se_company_address_suggestions_ratsit",
    "se_company_address_suggestions_esef",
    "se_company_address_normalize",
    "sweden_osm_pbf_s3",
    "sweden_osm_addresses_duckdb",
    "sweden_geocode_centroids_clickhouse",
    "se_address_geocodes_warm",
    "se_company_address_publish",
    "sweden_companies_current_clickhouse",
}


def _weekly_defs() -> dg.Definitions:
    """Only the modules the weekly touches, with mock resources so the job resolves."""
    assets = dg.load_assets_from_modules(
        [address_assets, osm_assets, centroid_assets, companies_current_asset]
    )
    return dg.Definitions(
        assets=assets,
        jobs=[weekly.sweden_company_address_geocoding_weekly_job],
        schedules=[weekly.sweden_company_address_geocoding_weekly],
        resources={
            "clickhouse": dg.ResourceDefinition.mock_resource(),
            "sweden_address_osm_duckdb": dg.ResourceDefinition.mock_resource(),
            "sweden_address_osm_object_store": dg.ResourceDefinition.mock_resource(),
        },
    )


def test_the_weekly_selects_the_eleven_step_chain() -> None:
    job = _weekly_defs().resolve_job_def(WEEKLY_JOB)
    assert {key.path[-1] for key in job.asset_layer.executable_asset_keys} == EXPECTED_SELECTION
    assert len(EXPECTED_SELECTION) == 11


def test_the_weekly_runs_tuesday_0105_stockholm() -> None:
    schedule = _weekly_defs().resolve_schedule_def(WEEKLY_SCHEDULE)
    assert schedule.job.name == WEEKLY_JOB
    assert schedule.cron_schedule == "5 1 * * 2"
    assert weekly.WEEKLY_CRON_SCHEDULE == "5 1 * * 2"
    assert schedule.execution_timezone == "Europe/Stockholm"
    assert schedule.default_status == dg.DefaultScheduleStatus.RUNNING


def test_every_duckdb_touching_step_is_in_the_single_slot_pool() -> None:
    """The 2026-08-25 lock conflict came from two runs opening the OSM DuckDB file outside a
    pool. Every step of the chain that requires the DuckDB resource must serialize on it."""
    job = _weekly_defs().resolve_job_def(WEEKLY_JOB)
    duckdb_steps = {
        node.name: node.pool
        for node in job.graph.node_defs
        if DUCKDB_POOL in node.required_resource_keys
    }
    assert duckdb_steps, "the chain must contain DuckDB-touching steps"
    assert all(pool == DUCKDB_POOL for pool in duckdb_steps.values()), duckdb_steps
    assert set(duckdb_steps) >= {
        "sweden_osm_addresses_duckdb",
        "sweden_geocode_centroids_clickhouse",
        "se_address_geocodes_warm",
        "se_company_address_publish",
    }


def test_the_retired_geocoding_chain_stays_retired() -> None:
    """Slice 4b deleted the canonical/shared/demand/resolution/store chain. None of it may
    come back into the sweden_company modules under its old names."""
    defs = _weekly_defs()
    asset_names = {key.path[-1] for key in defs.resolve_asset_graph().get_all_asset_keys()}
    job_names = {job.name for job in defs.resolve_all_job_defs()}
    for retired in (
        "sweden_company_addresses_clickhouse",
        "sweden_company_canonical_addresses_duckdb",
        "sweden_company_canonical_addresses_clickhouse",
        "sweden_shared_addresses_duckdb",
        "sweden_shared_addresses_clickhouse",
        "sweden_address_geocode_demand_duckdb",
        "sweden_address_resolution_golden_evaluation",
        "sweden_address_resolution_shadow_duckdb",
        "sweden_address_resolution_current_duckdb",
        "sweden_address_resolution_unmatched_diagnostics_duckdb",
        "sweden_address_geocode_store_clickhouse",
        "sweden_address_geocode_store_backfill_clickhouse",
        "sweden_address_geocode_legacy_adoption_clickhouse",
    ):
        assert retired not in asset_names, retired
    for retired_job in (
        "sweden_company_address_geocoding_job",
        "sweden_shared_address_identity_job",
        "sweden_shared_address_geocoding_job",
        "sweden_address_geocode_store_backfill_job",
        "sweden_address_geocode_legacy_adoption_job",
        "sweden_address_resolution_shadow_job",
        "sweden_address_resolution_publish_job",
        "sweden_address_resolution_diagnostics_job",
    ):
        assert retired_job not in job_names, retired_job
```

- [ ] **Step 2: Run the tests to verify the two that must fail do fail**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3
uv run --frozen --no-sync pytest tests/test_sweden_address_geocoding_weekly.py -q -p no:warnings --tb=short 2>&1 | tail -12
```
Expected: three FAIL, one PASS. `test_the_weekly_selects_the_eleven_step_chain` fails (the current job selects five assets); `test_the_weekly_runs_tuesday_0105_stockholm` fails (`'5 4 * * 2' != '5 1 * * 2'`); `test_every_duckdb_touching_step_is_in_the_single_slot_pool` fails on its last assertion because `se_company_address_publish` is not in the current job. `test_the_retired_geocoding_chain_stays_retired` passes.

- [ ] **Step 3: Rewrite the job module**

Replace the module docstring (lines 1-12 of `src/dagster_v3/defs/sweden_company/address_geocoding_assets.py`) with:

```python
"""The Sweden weekly address chain: extract, normalize, OSM, centroids, warm, fold, serving refresh.

The canonical/shared/demand/resolution/store chain this module used to hold retired with the
old address model (slice 4b). The SE address model is the address entity
(`corpscout.se_company_address`, built by `se_company/address`), which geocodes inside its
own warm step and fold, so nothing here computes addresses or coordinates.

This module assembles the weekly run out of assets that live elsewhere, in dependency order:
the four source extractors and the normalizer (`se_company/address/assets.py`), the OSM
snapshot and index (`sweden_address_osm`), the coarse centroids (`centroid_assets.py`), the
address entity's geocode-cache warm and the fold that publishes every company's addresses
(`se_company/address/assets.py`), and the companies serving-view refresh
(`companies_current_asset.py`), which depends on the fold so it runs last.

Geocoding precedes the fold on purpose: the fold writes coordinates onto the published rows by
reading the geocode cache, so a fold before warming would match page by page (4 h 18 m on
2026-09-07) and a warm after the fold would reach users only a week later. The fold runs with
`AddressFoldConfig` defaults (`changed_only=True`); until the cache-invalidation redesign
lands, the weekly OSM extract still marks every company stale, so the fold rewrites all 64
buckets (~3 h). The run starts Tuesday 01:05 Europe/Stockholm so it holds the single-slot
DuckDB pool (shared with the backoffice's Fold now) well before reviewers start.
Spec: docs/superpowers/specs/2026-09-24-address-weekly-chain-design.md.
"""
```

Replace the imports block and constants (current lines 14-28) with:

```python
import dagster as dg

from dagster_v3.defs.se_company.address.assets import EXTRACTOR_ASSET_NAMES
from dagster_v3.defs.sweden_company.centroid_assets import (
    CENTROIDS_ASSET_KEY,
    sweden_geocode_centroid_area_sanity_check,
    sweden_geocode_centroids_clickhouse,
)
from dagster_v3.defs.sweden_company.companies_current_asset import (
    COMPANIES_CURRENT_ASSET_KEY,
    sweden_companies_current_clickhouse,
    sweden_companies_current_refresh_check,
)

WEEKLY_CRON_SCHEDULE = "5 1 * * 2"
WEEKLY_EXECUTION_TIMEZONE = "Europe/Stockholm"
```

Replace the job definition (current lines 31-46) with:

```python
sweden_company_address_geocoding_weekly_job = dg.define_asset_job(
    name="sweden_company_address_geocoding_weekly_job",
    selection=dg.AssetSelection.assets(
        *EXTRACTOR_ASSET_NAMES,
        "se_company_address_normalize",
        "sweden_osm_pbf_s3",
        "sweden_osm_addresses_duckdb",
        CENTROIDS_ASSET_KEY,
        "se_address_geocodes_warm",
        "se_company_address_publish",
        COMPANIES_CURRENT_ASSET_KEY,
    ),
    tags={"country": "SE", "pipeline": "address_geocoding"},
    description=(
        "Weekly Sweden address chain: sync the source address extractors and normalize, "
        "refresh the Geofabrik snapshot and OSM address index, republish the centroids, warm "
        "the address entity's geocode cache against the new extract, fold and publish every "
        "company's addresses, then force the companies serving view to refresh."
    ),
)
```

Replace the schedule's `description=(...)` (current lines 54-58) with:

```python
    description=(
        "Tuesday 01:05 Stockholm: extract, normalize, OSM refresh, centroids, geocode warm, "
        "fold of all company addresses, then the companies serving view refresh. About 4.5 h "
        "while the fold rewrites every bucket; holds the sweden_address_osm_duckdb pool."
    ),
```

Leave the `defs = dg.Definitions(...)` block unchanged: the extractors, normalizer, OSM and fold assets are defined in their own modules and reach the repository through `load_from_defs_folder`; the job only names them.

- [ ] **Step 4: Run the test file and the full-load check**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3
uv run --frozen --no-sync pytest tests/test_sweden_address_geocoding_weekly.py -q -p no:warnings 2>&1 | tail -3
uv run --frozen --no-sync dg check defs 2>&1 | tail -2
```
Expected: `4 passed`; `All definitions loaded successfully.`

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3
git add src/dagster_v3/defs/sweden_company/address_geocoding_assets.py tests/test_sweden_address_geocoding_weekly.py
git commit -q -m "$(cat <<'MSG'
feat(dagster): the Sweden weekly folds addresses after geocoding

The geocoding weekly grows into the full address chain: the four source
extractors, normalize, OSM snapshot and index, centroids, geocode warm,
the fold that publishes every company's addresses, then the companies
serving refresh. Nothing had published addresses since 2026-09-13 because
the fold was manual; two weekly runs re-geocoded 2.1M keys into a cache
nobody read. The schedule moves to Tuesday 01:05 Stockholm so the ~4.5 h
run releases the DuckDB pool before reviewers arrive.

Spec: docs/superpowers/specs/2026-09-24-address-weekly-chain-design.md

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
MSG
)"
git log --oneline -1
```

---

### Task 2: The serving refresh waits for the fold

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/companies_current_asset.py` (the `deps=` line of the asset decorator and one docstring paragraph)
- Test: `tests/test_se_companies_current_asset.py` (the dependency test)

**Interfaces:**
- Consumes: nothing from Task 1 (independent edit).
- Produces: `sweden_companies_current_clickhouse.dependency_keys` == {centroids, warm, `se_company_domain_publish`, `se_company_address_publish`}.

- [ ] **Step 1: Replace the dependency test so it fails on the current code**

In `tests/test_se_companies_current_asset.py` replace the whole function `test_the_refresh_asset_runs_after_geocoding_and_domain_publication` (it currently loads the full definitions via `from dagster_v3.definitions import defs as load_defs`, which fails under pytest here) with:

```python
def test_the_refresh_asset_runs_after_geocoding_publication_and_the_fold() -> None:
    """The serving view reads se_company_address, which only the fold rewrites. Without the
    fold in its dependencies the weekly could refresh the view before the fold landed and
    serve last week's coordinates for another week. The other three dependencies are the
    centroids, the geocode-cache warm and the domain publication (has_domains)."""
    assert {key.path[-1] for key in sweden_companies_current_clickhouse.dependency_keys} == {
        "sweden_geocode_centroids_clickhouse",
        "se_address_geocodes_warm",
        "se_company_domain_publish",
        "se_company_address_publish",
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3
uv run --frozen --no-sync pytest "tests/test_se_companies_current_asset.py::test_the_refresh_asset_runs_after_geocoding_publication_and_the_fold" -q -p no:warnings --tb=short 2>&1 | tail -6
```
Expected: FAIL; the actual set lacks `se_company_address_publish`.

- [ ] **Step 3: Add the dependency and update the docstring**

In `src/dagster_v3/defs/sweden_company/companies_current_asset.py` replace
```python
    deps=[dg.AssetKey(CENTROIDS_ASSET_KEY), dg.AssetKey(WARM_ASSET_KEY), dg.AssetKey("se_company_domain_publish")],
```
with
```python
    deps=[
        dg.AssetKey(CENTROIDS_ASSET_KEY),
        dg.AssetKey(WARM_ASSET_KEY),
        dg.AssetKey(ADDRESS_PUBLISH_ASSET_KEY),
        dg.AssetKey("se_company_domain_publish"),
    ],
```
and directly below the existing line `WARM_ASSET_KEY = "se_address_geocodes_warm"` add:
```python
ADDRESS_PUBLISH_ASSET_KEY = "se_company_address_publish"
```

In the module docstring replace the paragraph
```
Both statements run against a name ClickHouse owns -- there is no DuckDB work and no pool to
take. `deps` include the centroids, the address entity's geocode-cache warm, and domain
publication, which updates the current domains counted by `has_domains`.
Selecting this asset downstream of domain publication refreshes the company list in the same run.
The store-append asset it used to name retired with the old address
chain in slice 4b; the warm step is what now puts the week's new OSM extract into
`se_address_geocodes`, so it is what this must follow. Ordered LAST in the weekly, after
everything the view reads is current.
```
with
```
Both statements run against a name ClickHouse owns -- there is no DuckDB work and no pool to
take. `deps` are the centroids, the address entity's geocode-cache warm, the address fold
(`se_company_address_publish`, which rewrites `corpscout.se_company_address` -- the table the
serving view actually reads for addresses and coordinates) and domain publication, which
updates the current domains counted by `has_domains`. The fold dependency is what makes this
asset run LAST in the weekly chain (spec
docs/superpowers/specs/2026-09-24-address-weekly-chain-design.md): without it the view could
refresh before the fold landed and serve last week's coordinates for another week. Selecting
this asset downstream of domain publication refreshes the company list in the same run.
```

- [ ] **Step 4: Run the whole asset test file and the full-load check**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3
uv run --frozen --no-sync pytest tests/test_se_companies_current_asset.py -q -p no:warnings 2>&1 | tail -3
uv run --frozen --no-sync dg check defs 2>&1 | tail -1
```
Expected: `23 passed` (22 from before plus the rewritten one, which previously failed for environmental reasons); `All definitions loaded successfully.`

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3
git add src/dagster_v3/defs/sweden_company/companies_current_asset.py tests/test_se_companies_current_asset.py
git commit -q -m "$(cat <<'MSG'
feat(dagster): the companies serving refresh depends on the address fold

sweden_companies_current_clickhouse now lists se_company_address_publish
among its deps, so in the weekly chain the serving view is rebuilt only
after the fold has rewritten se_company_address, the table it reads for
addresses and coordinates. The dependency test no longer loads the full
definitions, which need env vars pytest does not carry.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
MSG
)"
git log --oneline -1
```

---

### Task 3: Design doc records the new rule

**Files:**
- Modify: `src/dagster_v3/defs/se_company/address/docs/address-design.md` (two passages, lines ~44-45 and ~162-163)

**Interfaces:**
- Consumes: nothing.
- Produces: nothing code depends on.

- [ ] **Step 1: Write the failing check**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3
rg -n "fold\s*$|itself stays manual|stopped address weekly" src/dagster_v3/defs/se_company/address/docs/address-design.md | wc -l
```
Expected: `3` (the two sentences this task rewrites; `fold` at end of line 44 plus lines 45 and 162-163). Must be `0` after the edit.

- [ ] **Step 2: Rewrite the two passages**

Replace
```
not been recomputed yet. The weekly job already normalizes before anything folds; the fold
itself stays manual, so this ordering is on whoever launches a bucket or backfill by hand.
```
with
```
not been recomputed yet. The weekly chain (`sweden_company_address_geocoding_weekly_job`,
Tuesday 01:05 Stockholm, spec `docs/superpowers/specs/2026-09-24-address-weekly-chain-design.md`)
normalizes before it warms and folds, so the scheduled path always has this ordering; only a
bucket or backfill launched by hand needs the operator to keep it.
```

Replace
```
filters or LLM settings are sent. The redundant extract job and stopped address weekly
schedule were removed. Internal bucket and targeted correction folds remain available.
```
with
```
filters or LLM settings are sent. Since 2026-09-24 the fold also runs on a schedule again: the
weekly chain in `sweden_company/address_geocoding_assets.py` extracts, normalizes, refreshes
OSM and the centroids, warms, folds all 64 buckets with `changed_only=True`, then refreshes the
companies serving view (Tuesday 01:05 Stockholm; about 4.5 h until the cache-invalidation
redesign). The 2026-09-15 decision to keep the fold manual was reversed because nothing had
published addresses since 2026-09-13. Internal bucket and targeted correction folds remain
available; they share the DuckDB pool with the weekly and wait while it runs.
```

- [ ] **Step 3: Run the check again**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3
rg -n "fold\s*$|itself stays manual|stopped address weekly" src/dagster_v3/defs/se_company/address/docs/address-design.md | wc -l
rg -n "2026-09-24-address-weekly-chain-design" src/dagster_v3/defs/se_company/address/docs/address-design.md | wc -l
```
Expected: `0` then `1`.

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3
git add src/dagster_v3/defs/se_company/address/docs/address-design.md
git commit -q -m "$(cat <<'MSG'
docs(address): the fold runs weekly again, after geocoding

Records the 2026-09-24 reversal of "the fold itself stays manual": the
weekly chain extracts, normalizes, refreshes OSM, warms, folds and then
refreshes the serving view every Tuesday 01:05 Stockholm.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
MSG
)"
git log --oneline -1
```

---

### Task 4: Deploy the two source files and reload the code location

**Files:**
- Host: `/opt/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/sweden_company/address_geocoding_assets.py` and `.../companies_current_asset.py` (replaced with the committed versions)
- No repository files change.

**Interfaces:**
- Consumes: commits from Tasks 1 and 2 (the two source files at HEAD).
- Produces: the live code location `dagster_v3` serving the new job and schedule; Task 5 launches against it.

- [ ] **Step 1: Confirm the host copies equal the pre-change committed versions**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3
for f in address_geocoding_assets.py companies_current_asset.py; do
  echo "$f local HEAD: $(git show HEAD:corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/$f | md5)"
  echo "$f pre-change: $(git show HEAD~3:corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/$f | md5)"
  ssh dagster "md5sum /opt/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/sweden_company/$f | cut -d' ' -f1 | sed 's/^/$f host:      /'"
done
```
Expected: for each file the host md5 equals the pre-change md5 (HEAD~3 is the commit before Task 1 if Tasks 1-3 made exactly three commits; adjust the `~N` to the commit before Task 1 and say so in the report). If the host copy matches neither, STOP: someone deployed something else; report BLOCKED.

- [ ] **Step 2: Install the two files with backups**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3
STAMP=$(date -u +%Y%m%dT%H%MZ)
for f in address_geocoding_assets.py companies_current_asset.py; do
  scp -q src/dagster_v3/defs/sweden_company/$f dagster:/tmp/$f
  ssh dagster "set -e; L=/opt/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/sweden_company/$f; sudo -n mkdir -p /var/backups/corpscout-dagster; sudo -n cp -p \"\$L\" /var/backups/corpscout-dagster/$f.$STAMP-before-weekly-chain; sudo -n install -o root -g root -m 0644 /tmp/$f \"\$L\"; rm -f /tmp/$f; md5sum \"\$L\" | cut -d' ' -f1"
  echo "local: $(md5 -q src/dagster_v3/defs/sweden_company/$f)"
done
```
Expected: for each file the printed host md5 equals the local md5.

- [ ] **Step 3: Validate the full definitions on the host, then reload**

Run:
```bash
ssh dagster 'cd /opt/companycollect/corpscout/dagster_v3 && sudo -n bash -c "set -a; . ./.env; set +a; export DAGSTER_HOME=/opt/companycollect/corpscout/dagster_v3 DAGSTER_DISABLE_TELEMETRY=1; .venv/bin/dg check defs 2>&1 | tail -2"'
ssh dagster 'curl -s -m 120 -X POST localhost:3000/graphql -H "Content-Type: application/json" -d "{\"query\":\"mutation { reloadRepositoryLocation(repositoryLocationName: \\\"dagster_v3\\\") { __typename ... on WorkspaceLocationEntry { name loadStatus locationOrLoadError { __typename ... on PythonError { message } } } ... on PythonError { message } } }\"}"; echo'
```
Expected: `All definitions loaded successfully.`; then `"loadStatus":"LOADED"` with `locationOrLoadError.__typename == "RepositoryLocation"`. If the reload returns a PythonError, restore both backups with `sudo -n install` from `/var/backups/corpscout-dagster/`, reload again, and report BLOCKED with the message.

- [ ] **Step 4: Verify the schedule and job on the host**

Run:
```bash
ssh dagster 'for i in $(seq 1 12); do out=$(curl -s -m 20 -X POST localhost:3000/graphql -H "Content-Type: application/json" -d "{\"query\":\"{ workspaceOrError { ... on Workspace { locationEntries { name loadStatus updatedTimestamp } } } }\"}"); echo "$out" | grep -q LOADED && { echo "$out"; break; }; sleep 5; done'
ssh dagster 'curl -s -m 30 -X POST localhost:3000/graphql -H "Content-Type: application/json" -d "{\"query\":\"{ scheduleOrError(scheduleSelector: {repositoryLocationName: \\\"dagster_v3\\\", repositoryName: \\\"__repository__\\\", scheduleName: \\\"sweden_company_address_geocoding_weekly\\\"}) { ... on Schedule { cronSchedule executionTimezone scheduleState { status } futureTicks(limit: 1) { results { timestamp } } } ... on ScheduleNotFoundError { message } } }\"}"; echo'
ssh dagster 'curl -s -m 30 -X POST localhost:3000/graphql -H "Content-Type: application/json" -d "{\"query\":\"{ pipelineOrError(params: {repositoryLocationName: \\\"dagster_v3\\\", repositoryName: \\\"__repository__\\\", pipelineName: \\\"sweden_company_address_geocoding_weekly_job\\\"}) { ... on Pipeline { solids { name } } } }\"}" | python3 -c "import sys,json; d=json.load(sys.stdin); names=sorted(s[\"name\"] for s in d[\"data\"][\"pipelineOrError\"][\"solids\"]); print(len(names), names)"'
```
Expected: `updatedTimestamp` newer than before the reload; `cronSchedule` `5 1 * * 2`, `executionTimezone` `Europe/Stockholm`, `scheduleState.status` `RUNNING`, one future tick whose epoch converts to Tuesday 2026-09-29 01:05 Europe/Stockholm (2026-09-28 23:05 UTC); the job lists exactly the eleven asset op names of the Global Constraints (asset checks may appear as extra ops named `<asset>_<check>`; count only the eleven asset names and say so).

- [ ] **Step 5: Record**

No commit. Write the host md5s, the reload result, the schedule verification output and the backup filenames into the task report.

---

### Task 5: Prove the chain once on the server before Tuesday

**Files:**
- None. Operational verification against the live instance and ClickHouse.

**Interfaces:**
- Consumes: the reloaded code location from Task 4.
- Produces: evidence that the chain publishes addresses and refreshes the serving view; recorded in the report and the spec's section 7 criteria.

- [ ] **Step 1: Pre-flight: the pool is free and the window is right**

Run:
```bash
date -u; TZ=Europe/Stockholm date
ssh dagster 'curl -s -m 30 -X POST localhost:3000/graphql -H "Content-Type: application/json" -d "{\"query\":\"{ runsOrError(filter: {statuses: [STARTED, STARTING, QUEUED]}) { ... on Runs { results { runId pipelineName status } } } }\"}"; echo'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client --multiquery --query "SELECT max(folded_at) AS fold_before, count() AS active_rows FROM corpscout.se_company_address FINAL WHERE active = 1 FORMAT PrettyCompactMonoBlock; SELECT count(DISTINCT reference_md5) AS md5s_before FROM corpscout.se_address_geocodes FORMAT PrettyCompactMonoBlock; SELECT last_success_time AS view_before FROM system.view_refreshes WHERE view = '"'"'se_companies_serving'"'"' FORMAT PrettyCompactMonoBlock"'
```
Expected: Stockholm local time before 07:00 or after 18:00; no in-flight run whose `pipelineName` starts with `se_company_address` or is `__ASSET_JOB` with an address asset; record `fold_before`, `md5s_before`, `view_before`. If the window is wrong, wait (do not launch) and say so.

- [ ] **Step 2: Launch the job**

Run:
```bash
ssh dagster 'curl -s -m 60 -X POST localhost:3000/graphql -H "Content-Type: application/json" -d "{\"query\":\"mutation { launchPipelineExecution(executionParams: { selector: { repositoryLocationName: \\\"dagster_v3\\\", repositoryName: \\\"__repository__\\\", jobName: \\\"sweden_company_address_geocoding_weekly_job\\\" }, runConfigData: {}, mode: \\\"default\\\" }) { __typename ... on LaunchRunSuccess { run { runId status } } ... on PythonError { message } ... on RunConfigValidationInvalid { errors { message } } } }\"}"; echo'
```
Expected: `LaunchRunSuccess` with a `runId` and status `QUEUED`. Record the run id.

- [ ] **Step 3: Watch it to a terminal state**

Poll the run's status every 10 minutes for up to 6 hours (a background monitor, not a foreground sleep), emitting each status change and, on FAILURE or CANCELED, the failing step:
```bash
RUN=<runId>
ssh dagster "curl -s -m 20 -X POST localhost:3000/graphql -H 'Content-Type: application/json' -d '{\"query\":\"{ runOrError(runId: \\\"$RUN\\\") { ... on Run { status stats { ... on RunStatsSnapshot { stepsSucceeded stepsFailed } } } } }\"}'"
```
Expected: SUCCESS within about 4.5 hours, `stepsFailed` 0. If the run FAILS, fetch the failed step's error (`logsForRun` or the Postgres `event_logs` STEP_FAILURE row), fix nothing here, and report DONE_WITH_CONCERNS with the step name and message.

- [ ] **Step 4: Verify the spec's success criteria**

Run:
```bash
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client --multiquery --query "SELECT max(folded_at) AS fold_after, count() AS active_rows FROM corpscout.se_company_address FINAL WHERE active = 1 FORMAT PrettyCompactMonoBlock; SELECT count(DISTINCT reference_md5) AS md5s_after FROM corpscout.se_address_geocodes FORMAT PrettyCompactMonoBlock; SELECT status, last_success_time AS view_after, exception FROM system.view_refreshes WHERE view = '"'"'se_companies_serving'"'"' FORMAT PrettyCompactMonoBlock"'
```
Expected: `fold_after` > `fold_before` and within the run's window; `md5s_after == md5s_before + 1` (one new OSM reference; if the Geofabrik extract did not change since 09-22 it is `+ 0` and the warm reports mostly cache hits — say which); `view_after` later than the fold's end and `exception` empty. Also read the run's step timeline (STEP_START/STEP_SUCCESS per step, from Postgres `event_logs` as in earlier diagnostics) and record each step's duration.

- [ ] **Step 5: Record**

No commit. Report: run id, status, per-step durations, the three before/after values, and any concern (for example a fold longer than 3.5 h, or a warm step with near-zero cache hits, which is expected until the invalidation redesign).
