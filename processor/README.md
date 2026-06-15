# Processor

Local Prefect workflows for company collection experiments.

## Finland YTJ Base Ingest

`finland_raw_ingest.py` currently implements the first Finland Prefect task:

1. Download the full PRH YTJ companies JSON from the public API.
2. Store that full JSON in S3/RustFS.
3. Filter companies with `registrationDate >= start_date` and `registrationDate < today`.
4. Store the filtered result as `base.json`.

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
    start_date="2024-01-01",
    today="2026-06-15",
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
  full/date=<today>/companies.json
  base/start_date=<start_date>/end_date=<today>/base.json
  base/start_date=<start_date>/end_date=<today>/manifest.json
```

Existing full JSON objects are reused by default. Pass `refresh=True` to redownload
and overwrite the full JSON before rebuilding `base.json`.
