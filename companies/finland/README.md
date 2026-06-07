# Finland Country Data

Standalone Go module for Finland company data collection and export.

The module builds one country-level binary:

```bash
GOWORK=off go build -o ./bin/finland-countrydata ./cmd/finland-countrydata
```

Run source sync and export commands from this directory:

```bash
GOWORK=off go run ./cmd/finland-countrydata sync-source --source prhytj --data-dir ../data/finland/countrydata --max-pages 2
GOWORK=off go run ./cmd/finland-countrydata status-source --source prhytj --data-dir ../data/finland/countrydata
GOWORK=off go run ./cmd/finland-countrydata build-export --data-dir ../data/finland/countrydata
```

When `--data-dir` is omitted, the CLI uses `../data/finland/countrydata`.

The module depends on `../common` for shared source-processing helpers during
local development. The Finland binary is intended to be executed by orchestration
code or a container runtime; Corpscout should consume the produced manifest and
parquet files rather than importing this module directly.
