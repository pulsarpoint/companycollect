# Finland PRH YTJ

This package imports Finnish company records from the PRH Open Data YTJ API v3.

Run commands from `companies/companysource`.

## Local Fixture Tests

Default tests use checked-in fixtures and do not call the live PRH API:

```sh
GOWORK=off go test ./sources/finland/prhytj/... -count=1
```

The live integration tests are also skipped by default, even when the `integration` tag is enabled:

```sh
GOWORK=off go test -tags=integration ./sources/finland/prhytj/... -run TestLivePRHYTJ -count=1
```

## Companysource CLI

The source is run through the unified `companysource` CLI:

```bash
GOWORK=off go run ./cmd/companysource download --country finland --source prhytj --run-dir ../data/finland/sources/prhytj/runs/<run-id> --max-pages 2
GOWORK=off go run ./cmd/companysource export-parquet --country finland --source prhytj --run-dir ../data/finland/sources/prhytj/runs/<run-id>
GOWORK=off go run ./cmd/companysource status --country finland --source prhytj --run-dir ../data/finland/sources/prhytj/runs/<run-id>
```

Build the unified binary with:

```bash
GOWORK=off go build -o ./bin/companysource ./cmd/companysource
```

Runs are written under:

```text
../data/finland/sources/prhytj/runs/<run-id>/
```

There is no country-level final export in the new source ingestion path.

## Live Smoke Test

The smoke test downloads two live pages into a temporary data directory, then processes the snapshot with a chunk size of 100:

```sh
COUNTRYDATA_PRH_YTJ_LIVE=1 GOWORK=off go test -tags=integration ./sources/finland/prhytj/... -run TestLivePRHYTJSmoke -count=1 -v
```

## Full Live Import Test

The full live test downloads the complete PRH YTJ dataset into a temporary data directory, then processes the snapshot with a chunk size of 500:

```sh
COUNTRYDATA_PRH_YTJ_LIVE_FULL=1 GOWORK=off go test -tags=integration ./sources/finland/prhytj/... -run TestLivePRHYTJFullDataset -count=1 -v
```
