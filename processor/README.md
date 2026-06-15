# Processor

Local Prefect workflows for company collection experiments.

## Finland Raw Ingest

`finland_raw_ingest.py` downloads raw Finland PRH data from public APIs into the
existing S3/RustFS buckets:

- YTJ companies: `source-finland-prhytj`
- XBRL statements: `source-finland-prh-xbrl`

Required environment variables:

```bash
export CORPSCOUT_S3_ENDPOINT=http://localhost:9000
export CORPSCOUT_S3_ACCESS_KEY=...
export CORPSCOUT_S3_SECRET_KEY=...
```

Run once:

```bash
uv run python finland_raw_ingest.py
```

Run with explicit parameters:

```bash
uv run python - <<'PY'
from finland_raw_ingest import finland_raw_ingest_flow

finland_raw_ingest_flow(
    snapshot_date="2026-06-15",
    max_companies=200,
    xbrl_start="2025-01-01",
    xbrl_end="2025-01-03",
    refresh=False,
)
PY
```

Serve on a cron:

```bash
uv run python - <<'PY'
from finland_raw_ingest import serve_finland_raw_ingest

serve_finland_raw_ingest(cron="0 2 * * *")
PY
```

Object layout:

```text
source-finland-prhytj/
  snapshots/<YYYY-MM-DD>/source.ndjson
  snapshots/<YYYY-MM-DD>/manifest.json

source-finland-prh-xbrl/
  windows/<start>_<end>/listing.json
  windows/<start>_<end>/manifest.json
  companies/<business_id>/<financial_date>.xml
```

Existing objects are reused by default. Pass `refresh=True` to redownload and
overwrite source objects.
