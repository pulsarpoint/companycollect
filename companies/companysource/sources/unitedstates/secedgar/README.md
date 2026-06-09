# SEC EDGAR Source

SEC EDGAR downloads `https://www.sec.gov/files/company_tickers.json`,
validates the source file, and writes source-level Parquet exports into the
same run folder.

Run these commands from `companies/companysource` with `GOWORK=off`.

## Full source sync

SEC EDGAR enforces a fair-access policy and rejects generic User-Agents with
HTTP 403. Requests must send a descriptive identifier that includes a contact
email. The source ships a compliant default
(`CorpScout CountryData/1.0 (+https://pulsarpoint.com; goran.raovic@gmail.com)`),
so the sync works out of the box. Override it with your own organization and
contact email before a live SEC request:

```bash
export USA_SEC_EDGAR_USER_AGENT="<organization name and contact email>"
```

Run the source sync:

```bash
GOWORK=off go run ./cmd/companysource download --country united_states --source secedgar --run-dir ../data/united_states/sources/secedgar/runs/<run-id>
GOWORK=off go run ./cmd/companysource export-parquet --country united_states --source secedgar --run-dir ../data/united_states/sources/secedgar/runs/<run-id>
```

## Export an existing snapshot

Use this when a snapshot already exists and only the source export should be
rebuilt:

```bash
GOWORK=off go run ./cmd/companysource export-parquet --country united_states --source secedgar --run-dir ../data/united_states/sources/secedgar/runs/<run-id>
```

The exporter reads `source.json` from the run folder.

## Status

```bash
GOWORK=off go run ./cmd/companysource status --country united_states --source secedgar --run-dir ../data/united_states/sources/secedgar/runs/<run-id>
```

## Data layout

Generated outputs:

```text
source.json
companies.parquet
company_names.parquet
identifiers.parquet
source_evidence.parquet
manifest.json
```

## Environment

| Variable | Purpose |
| --- | --- |
| `USA_SEC_EDGAR_DOWNLOAD_URL` | Download URL override. Defaults to SEC `company_tickers.json`. |
| `USA_SEC_EDGAR_USER_AGENT` | User-Agent sent to SEC. Defaults to a compliant contact string; SEC rejects generic agents with HTTP 403. Override with your own organization/contact string in production. |
| `USA_SEC_EDGAR_REQUEST_TIMEOUT` | Request timeout, for example `30s` or `30`. |

No secret or token is required.

## Testing

```bash
GOWORK=off go test ./... -count=1
```

Tests use local fixtures and `httptest`; they do not make real network calls.

## Limitations

This source exports SEC EDGAR company ticker records only. The final USA export
that combines SEC EDGAR with the other implemented sources (`irseobmf` and
`coloradoentities`) is intentionally not implemented yet.
