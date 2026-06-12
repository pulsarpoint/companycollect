# Finland PRH XBRL Temporal to Dagster Phase 1 Design

## Purpose

Build the first complete Temporal + Dagster integration around Finland PRH
financial statements. Phase 1 only discovers and downloads raw PRH XBRL
statement XML, writes durable RustFS ledgers, and exposes the completed run as
a Dagster source asset. It proves the cross-plane handoff before any XBRL
parsing, financial fact modeling, or ClickHouse financial tables.

This spec narrows and updates
`2026-06-10-finland-prh-xbrl-discovery-download-design.md` for the current
Dagster architecture in
`2026-06-11-finland-dagster-temporal-design.md`.

## Goals

- Reuse the existing Go PRH XBRL discovery/download code and Postgres ledger.
- Move PRH XBRL run artifacts from local run directories to RustFS.
- Write a run-level `manifest.json` that Dagster can observe.
- Add a Dagster source package and sensor for `source_finland_prh_xbrl`.
- Keep all real source assets loaded in Dagster while treating global lineage
  as a filtered/debug view.

## Non-Goals

Phase 1 does not:

- parse XBRL or iXBRL facts;
- create ClickHouse financial tables;
- derive revenue, net income, assets, equity, liabilities, employee count, or
  other financial metrics;
- update the Finland company explorer cache;
- implement Virre paid fallback;
- rebuild the old scheduler REST or React source-action UI.

## Source Identity

The source is a separate PRH Open Data API source, not a YTJ source:

```text
source_name: finland_prh_xbrl
country: finland
source: prh_xbrl
registry_key: finland/prh_xbrl
display_name: Finland PRH financial XBRL
source_url: https://avoindata.prh.fi/opendata-xbrl-api/v3
bucket: source-finland-prh-xbrl
dagster_group: source_finland_prh_xbrl
asset_key_prefix: sources/finland/prh_xbrl
```

## Boundary Rule

Temporal owns PRH XBRL download execution because the work is long-running,
rate-limited, per-statement, and needs durable retry state. Dagster owns the
dataset visibility and downstream graph. The only handoff between them is the
RustFS run manifest.

```text
Temporal workflow
  -> discover registered-date window
  -> upsert Postgres statement artifact ledger rows
  -> download XML files with per-statement retry/skip state
  -> write XML files to RustFS
  -> write statements.ndjson
  -> write manifest.json

Dagster sensor
  -> lists source-finland-prh-xbrl/runs/*/manifest.json
  -> records sources/finland/prh_xbrl/raw_statements materialization
  -> exposes run metadata in source_finland_prh_xbrl
```

Dagster does not start this workflow in phase 1. Manual triggering is through
Temporal UI or Temporal CLI with explicit registered-date window input.

## PRH API Surface

Discovery uses registration-date windows:

```text
GET /all_financial_statements?registeredDateStart=<YYYY-MM-DD>&registeredDateEnd=<YYYY-MM-DD>&page=<N>
```

Each discovered statement has:

```text
businessId
financialDate
registrationDate
```

XML download uses:

```text
GET /financial?businessId=<business_id>&financialDate=<YYYY-MM-DD>
```

PRH documents `429`, `500`, and `503`; the workflow treats these as retryable
or per-artifact failures depending on the point of failure. One bad statement
must not block all successfully downloaded statements from being exposed.

## Temporal Workflow

Workflow type:

```text
FinlandPRHXBRLDownloadWorkflow
```

Task queue:

```text
company-source
```

If implementation keeps using the generic company-source worker initially,
register the workflow directly on that worker. If the slim Temporal worker
exists first, register it there. Do not introduce a registry abstraction just
for workflow registration.

Input:

```json
{
  "registered_date_start": "2026-06-01",
  "registered_date_end": "2026-06-03",
  "max_statements": 50,
  "retry_failed": false
}
```

Input rules:

- `registered_date_start` and `registered_date_end` are required ISO dates.
- `registered_date_start <= registered_date_end`.
- `max_statements` defaults to 50 for manual tests and must be bounded.
- `retry_failed=false` skips artifacts already marked `failed`.
- `retry_failed=true` makes failed artifacts eligible for another attempt.

Workflow ID:

```text
finland-prh-xbrl:{registered_date_start}:{registered_date_end}
```

Use a conflict/reuse policy that prevents two open workflows for the same date
window. If the window completed earlier and a new retry is desired, use the same
workflow ID only when Temporal permits reuse for completed workflows; otherwise
append an explicit retry suffix and rely on the Postgres ledger for idempotency.

Run ID for RustFS:

```text
{UTC timestamp}-{Temporal workflow run id short suffix}
```

Example:

```text
20260612T120000Z-1a2b3c4d
```

This keeps bucket prefixes sortable while avoiding overwrite collisions.

## Postgres Ledger

Reuse the existing source-specific ledger tables:

```text
financial_xbrl.finland_prh_xbrl_discovery_windows
financial_xbrl.finland_prh_xbrl_statement_artifacts
```

`finland_prh_xbrl_discovery_windows` tracks coverage for a registered-date
window: Temporal workflow ID/run ID, total results, pages discovered,
statements discovered, last completed page, and `completed_at`.

`finland_prh_xbrl_statement_artifacts` tracks one row per stable statement key:

```text
source_id
business_id
financial_date
registration_date
source_url
xml_path
xml_sha256
xml_size_bytes
download_status
attempts
last_attempt_at
downloaded_at
last_error_message
first_discovered_run_id
latest_action_run_id
```

The stable idempotency key is:

```text
source_id + business_id + financial_date
```

The ledger remains the retry authority. Temporal status says what happened to
one workflow run; the ledger says which statement files still need work.

## RustFS Layout

Bucket:

```text
source-finland-prh-xbrl
```

Run layout:

```text
runs/{run_id}/manifest.json
runs/{run_id}/statements.ndjson
runs/{run_id}/xml/{business_id}/{financial_date}.xml
```

The path uses `xml/`, not the old local `statements/` directory, so the object
type is obvious when browsing the bucket.

## `statements.ndjson`

`statements.ndjson` is the per-statement artifact inventory for one Temporal
run. It contains one JSON object per discovered statement that belongs to the
requested registered-date window and is relevant to the run output.

Succeeded row:

```json
{"business_id":"0100130-4","financial_date":"2024-12-31","registration_date":"2025-04-10","source_url":"https://avoindata.prh.fi/opendata-xbrl-api/v3/financial?businessId=0100130-4&financialDate=2024-12-31","download_status":"succeeded","xml_path":"runs/20260612T120000Z-1a2b3c4d/xml/0100130-4/2024-12-31.xml","xml_sha256":"...","xml_size_bytes":12345}
```

Failed row:

```json
{"business_id":"0202020-2","financial_date":"2024-06-30","registration_date":"2025-04-11","source_url":"https://avoindata.prh.fi/opendata-xbrl-api/v3/financial?businessId=0202020-2&financialDate=2024-06-30","download_status":"failed","error_message":"status 503"}
```

Later XBRL parser assets will read this file to find XML object keys and decide
whether failed artifacts should be ignored, retried, or reported.

## `manifest.json`

`manifest.json` is the run-level completion signal. Dagster watches this file,
not every XML file.

Required shape:

```json
{
  "run_id": "20260612T120000Z-1a2b3c4d",
  "source": "finland_prh_xbrl",
  "country": "finland",
  "source_slug": "prh_xbrl",
  "workflow_id": "finland-prh-xbrl:2026-06-01:2026-06-03",
  "workflow_run_id": "1a2b3c4d...",
  "registered_date_start": "2026-06-01",
  "registered_date_end": "2026-06-03",
  "statements_discovered": 123,
  "statements_succeeded": 120,
  "statements_failed": 3,
  "artifacts": [
    {
      "key": "statements_manifest",
      "object_key": "runs/20260612T120000Z-1a2b3c4d/statements.ndjson",
      "records_written": 123,
      "content_sha256": "...",
      "content_length_bytes": 9876
    }
  ]
}
```

The workflow writes `manifest.json` after `statements.ndjson` and all successful
XML object writes are durable. The manifest may report partial success when
some statement XML downloads failed.

## Partial Success Rule

Phase 1 allows partial success. If discovery completes and at least one
statement row is written, the workflow writes `statements.ndjson` and
`manifest.json` even when some XML downloads failed.

Rationale:

- the Postgres ledger already tracks failures per artifact;
- one bad PRH response should not hide all successful XMLs;
- later retry windows can pick up failed artifacts with `retry_failed=true`;
- Dagster materialization metadata can show both succeeded and failed counts.

The workflow should fail without a manifest when discovery itself cannot
complete, the ledger cannot be updated, RustFS is unavailable, or
`statements.ndjson` cannot be written. Those failures mean the run-level ledger
is not trustworthy.

## Dagster Source Package

Add a Dagster source package:

```text
dagster_corpscout/sources/finland/prh_xbrl/
  __init__.py
  spec.py
  assets/__init__.py
  assets/external.py
  sensors.py
```

Assets:

```text
sources/finland/prh_xbrl/source_system
sources/finland/prh_xbrl/raw_statements
```

`source_system` is an `AssetSpec` for the PRH XBRL API.

`raw_statements` is represented by materialization events emitted by a Dagster
sensor after it sees a new RustFS manifest. It is not a Dagster download asset
in phase 1.

Group and tags:

```text
group_name: source_finland_prh_xbrl
tags: country=finland, source=prh_xbrl, source_name=finland_prh_xbrl, layer=raw
```

The source group appears under:

```text
/locations/dagster_corpscout/assets
/locations/dagster_corpscout/asset-groups/source_finland_prh_xbrl
```

The global `/asset-groups` page remains a filtered/debug view.

## Dagster Sensor

Sensor name:

```text
finland_prh_xbrl_manifest_sensor
```

Behavior:

1. List `runs/*/manifest.json` in `source-finland-prh-xbrl`.
2. Parse manifests in sortable run prefix order.
3. Keep a cursor of observed `run_id` values or the latest observed prefix.
4. For every unseen manifest, emit a materialization for
   `sources/finland/prh_xbrl/raw_statements`.
5. Attach metadata:
   - run ID;
   - workflow ID and workflow run ID;
   - registered-date window;
   - statements discovered/succeeded/failed;
   - statements manifest object key;
   - content SHA-256 and byte length.

The sensor must tolerate Dagster downtime. Missing a polling interval is fine
because the RustFS manifest is durable and the sensor can catch up later.

## Error Handling

Go code follows existing Corpscout rules:

- lower-level HTTP, SQL, and RustFS operations wrap and return errors;
- Temporal activity/workflow boundary logs once;
- stored `last_error_message` values are safe summaries, not stack traces or
  request bodies;
- secrets and credentials never appear in manifest metadata or logs.

PRH `429`, `500`, and `503` are retryable. A permanent per-statement failure
marks only that artifact failed and still allows partial manifest publication
if the run-level ledger is valid.

## Testing

Go tests:

- date input validation;
- discovery page decoding;
- statement artifact upsert/skip/retry behavior;
- RustFS object key generation;
- XML SHA-256 and byte metadata;
- `statements.ndjson` generation with succeeded and failed rows;
- `manifest.json` generation and partial success counts;
- workflow registration uses direct Temporal registration.

Python/Dagster tests:

- source package convention for `finland/prh_xbrl`;
- definitions include `source_system` and `raw_statements`;
- `raw_statements` group/tags match `source_finland_prh_xbrl`;
- sensor emits one materialization per unseen manifest;
- sensor cursor prevents duplicate materializations;
- malformed manifest is logged by failing the sensor tick rather than emitting
  incorrect materialization metadata.

Manual smoke test:

1. Start Temporal worker and Dagster.
2. Trigger a small window, for example 2-5 registered days with
   `max_statements=5`.
3. Verify RustFS has:
   - `runs/{run_id}/manifest.json`;
   - `runs/{run_id}/statements.ndjson`;
   - at least one `runs/{run_id}/xml/...xml` when PRH returns XML.
4. Verify Postgres ledger rows have expected statuses and attempts.
5. Verify Dagster source group `source_finland_prh_xbrl` shows
   `raw_statements` materialized with statement counts.

## Implementation Order

1. Update the Go PRH XBRL downloader to write RustFS object keys and a
   run-level manifest while preserving the existing ledger behavior.
2. Register the PRH XBRL Temporal workflow in the current or slim worker with
   deterministic workflow IDs.
3. Add the Dagster `finland/prh_xbrl` source package and manifest sensor.
4. Run the manual smoke test against a small registered-date window.

## Later Phases

After phase 1 produces real XML samples and visible Dagster materializations:

1. design XBRL fact extraction;
2. add ClickHouse tables for raw facts and selected derived metrics;
3. add Dagster assets that parse `statements.ndjson` and XML into ClickHouse;
4. decide whether Virre paid fallback is worth implementing.
