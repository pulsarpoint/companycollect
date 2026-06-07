# United States Country Data

Standalone Go module for United States company data collection and export.

The module builds one country-level binary:

```bash
GOWORK=off go build -o ./bin/united-states-countrydata ./cmd/united-states-countrydata
```

Phase 1 implements SEC EDGAR:

```bash
GOWORK=off go run ./cmd/united-states-countrydata sync-source --source secedgar --data-dir ../data/united_states/countrydata
GOWORK=off go run ./cmd/united-states-countrydata status-source --source secedgar --data-dir ../data/united_states/countrydata
GOWORK=off go run ./cmd/united-states-countrydata build-export --data-dir ../data/united_states/countrydata
```

When `--data-dir` is omitted, the CLI uses `../data/united_states/countrydata`.
