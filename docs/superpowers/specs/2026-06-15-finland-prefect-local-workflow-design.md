# Finland Prefect Local Workflow Design

## Goal

Create a self-contained Prefect experiment in `companycollect/processor` that ports the
logic from `companies/analysis/finland/notebook/finland_walkthrough.py` into a lean,
local-first workflow. The purpose is to evaluate whether Prefect is easier to reason
about than Dagster for this type of source pipeline, especially when combined later
with dbt.

This first version is not a production replacement for the existing Dagster work. It
downloads a bounded live sample, transforms it locally, validates canonical tables,
and writes Parquet outputs that dbt can consume in a later experiment.

## Scope

Included:

- Local Prefect 3 flow under `companycollect/processor`.
- Copied Finland pipeline logic inside the processor project, not imports from the
  analysis notebook package.
- Bounded PRH YTJ company download.
- Bounded PRH XBRL statement-window download.
- Structured intermediate Parquet outputs.
- Canonical Parquet outputs for:
  - `registrations`
  - `company`
  - `financials`
  - `company_websites`
- JSON run manifest stored beside the Parquet outputs.
- Prefect run metadata and a Markdown artifact for UI-visible run summaries.
- Focused validation and smoke-test workflow execution.

Excluded:

- S3, RustFS, ClickHouse, or production persistence.
- Full historical backfills.
- Prefect deployments, workers, schedules, or cloud setup.
- dbt project creation. The file layout should be dbt-ready, but dbt is a later step.
- Virre paid-document fallback.

## Architecture

The processor project will contain a normal Python package for Finland-specific
business logic and one visible Prefect flow entrypoint:

```text
companycollect/processor/
  finland_flow.py
  finland/
    __init__.py
    config.py
    download.py
    structured.py
    canonical.py
    schemas.py
    io.py
  output/
    finland/
      <run_id>/
        raw/
        structured/
        canonical/
        manifest.json
```

`finland_flow.py` owns orchestration. The `finland/*` modules own plain Python and
Polars logic. This keeps the task graph easy to read while avoiding a large single
script.

The flow uses Prefect for:

- task boundaries;
- retries around remote downloads;
- logs and run status;
- run names, parameters, and tags for lightweight partition-like organization;
- Markdown artifacts with output paths and row counts;
- parameterized local execution.

The transform code stays framework-agnostic so it can later be reused from tests,
dbt pre-processing scripts, or another orchestrator.

## Components

`finland/config.py`

- PRH YTJ and XBRL URLs.
- User agent.
- Default `max_companies=200`.
- Default XBRL registration window: `2025-01-01` through `2025-01-03`.
- Finland country constants.
- XBRL metric mapping copied from the walkthrough.

`finland/download.py`

- HTTP session setup.
- Retry/backoff helper for transient `429`, `5xx`, timeout, and connection errors.
- `download_ytj_companies(...)` writes local NDJSON.
- `download_xbrl_window(...)` writes local XML files and an XBRL listing JSON.

`finland/structured.py`

- YTJ NDJSON to structured Polars frames:
  - `fi_prhytj_statuses`
  - `fi_prhytj_names`
  - `fi_prhytj_websites`
  - `fi_prhytj_addresses`
  - `fi_prhytj_business_lines`
- XBRL XML to facts using an inline `lxml` parser copied from the walkthrough shape.

`finland/canonical.py`

- Structured frames to canonical frames:
  - `registrations`
  - `company`
  - `financials`
  - `company_websites`
- UID generation remains the walkthrough convention:
  - `registration_uid = "FI:" + business_id`
  - `company_uid = "c:" + sha1("FI:" + business_id)`

`finland/schemas.py`

- Required canonical schemas.
- Unique key rules for canonical tables.
- Validation helper that raises clear errors for missing columns, dtype mismatches,
  and duplicate keys.

`finland/io.py`

- Output directory creation.
- Parquet writes for structured and canonical frames.
- Manifest writing.

`manifest.json` remains because it travels with the generated dataset. It is the
portable record that dbt, scripts, or a human can inspect without querying the Prefect
API or UI.

## Data Flow

1. The flow receives parameters:
   - `run_id`, optional. If absent, generate a timestamped ID.
   - `max_companies`, default `200`.
   - `xbrl_start`, default `2025-01-01`.
   - `xbrl_end`, default `2025-01-03`.
   - `output_root`, default `output/finland`.
2. Create `output/finland/<run_id>/`.
3. Download PRH YTJ company records to `raw/prh_ytj_companies.ndjson`.
4. Discover PRH XBRL statements for the requested registration-date window.
5. Download XBRL XML files under `raw/xbrl/` and write `raw/xbrl_listing.json`.
6. Convert YTJ raw data to structured frames.
7. Convert XBRL raw XML to a facts frame.
8. Write structured frames as Parquet under `structured/`.
9. Build canonical frames from structured frames.
10. Validate canonical schemas and unique keys.
11. Write canonical Parquet files under `canonical/`.
12. Write `manifest.json` with parameters, raw counts, table shapes, output paths,
    validation results, and parse failures if any.
13. Create a Prefect Markdown artifact with the run ID, parameters, row counts, and
    output path.

Prefect is used for operational metadata rather than a separate `latest.json` pointer.
The flow should set a clear run name, such as:

```text
finland-local-{run_id}
```

The XBRL date window acts as the local experiment's partition-like dimension. Later,
the flow can add an explicit `partition_key`, such as `2025-01`, if monthly XBRL
windows become the main execution unit.

The walkthrough's YTJ URL placeholder is not copied. The Prefect project uses the
verified PRH endpoint:

```text
https://avoindata.prh.fi/opendata-ytj-api/v3/companies
```

## Error Handling

Remote download tasks retry transient source failures. A response with status `429`
or `5xx`, timeout, or connection error is retried with backoff. Persistent HTTP
errors fail the task.

Transform and validation tasks fail fast. These failures usually mean the source
shape changed or the copied logic is wrong, so automatic retries would add little
value.

XBRL parse failures are recorded in the manifest and fail the run for this experiment.
The first version should expose bad XML or parser assumptions immediately instead of
silently producing partial financial output.

Validation failures raise `ValueError` with table and column context. The flow does
not expose stack traces to external clients because there is no API boundary in this
local version.

## Testing And Verification

Verification should stay focused on the local experiment:

- Unit-style tests for pure helpers:
  - canonical UID generation;
  - website normalization;
  - schema validation;
  - XBRL fact extraction with a small fixture.
- Smoke execution of the Prefect flow with a tiny sample:
  - `max_companies=5`;
  - a narrow XBRL window.
- Read Parquet files back with Polars and verify:
  - all expected structured files exist;
  - all expected canonical files exist;
  - canonical schemas match;
  - unique keys are unique where required;
  - `manifest.json` exists and contains the run ID.
- Verify the Prefect flow creates a Markdown artifact summarizing the run.

The demo Prefect scripts already in `processor` should be left alone unless they
block imports or tests.

## Future dbt Direction

This design intentionally writes stable Parquet paths so a later dbt-duckdb
experiment can read:

```text
output/finland/<run_id>/structured/*.parquet
output/finland/<run_id>/canonical/*.parquet
```

The first dbt experiment can either:

- treat Prefect as raw and structured orchestration while dbt builds canonical
  models from structured Parquet; or
- use Prefect for the whole Python-heavy Finland flow and let dbt handle only
  downstream SQL marts.

That decision is intentionally deferred until the local Prefect version exists and
can be compared against the Dagster implementation.

## Acceptance Criteria

- `companycollect/processor` contains a self-contained Finland Prefect workflow.
- The workflow can run locally with a small bounded sample.
- The workflow writes raw, structured, canonical, and manifest outputs.
- The workflow creates a Prefect run summary artifact.
- The four canonical tables match the walkthrough's intended outputs.
- Validation catches schema and key failures.
- The implementation remains plain Python plus Prefect task boundaries, with no
  production storage or scheduler requirements.
