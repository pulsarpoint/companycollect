# Finland PRH YTJ

This package imports Finnish company records from the PRH Open Data YTJ API v3.

Run commands from `companies/finland`.

## Local Fixture Tests

Default tests use checked-in fixtures and do not call the live PRH API:

```sh
GOWORK=off go test ./prhytj/... -count=1
```

The live integration tests are also skipped by default, even when the `integration` tag is enabled:

```sh
GOWORK=off go test -tags=integration ./prhytj/... -run TestLivePRHYTJ -count=1
```

## Finland countrydata CLI

The source can be run through the country-level CLI:

```bash
GOWORK=off go run ./cmd/finland-countrydata sync-source --source prhytj --data-dir ../data/finland/countrydata --max-pages 2
GOWORK=off go run ./cmd/finland-countrydata status-source --source prhytj --data-dir ../data/finland/countrydata
GOWORK=off go run ./cmd/finland-countrydata build-export --data-dir ../data/finland/countrydata
```

Build the standalone Finland binary with:

```bash
GOWORK=off go build -o ./bin/finland-countrydata ./cmd/finland-countrydata
```

Source exports are written under:

```text
../data/finland/countrydata/sources/prhytj/exports/<run-id>/
```

Final country exports are written under:

```text
../data/finland/countrydata/final/exports/<run-id>/
```

## Live Smoke Test

The smoke test downloads two live pages into a temporary data directory, then processes the snapshot with a chunk size of 100:

```sh
COUNTRYDATA_PRH_YTJ_LIVE=1 GOWORK=off go test -tags=integration ./prhytj/... -run TestLivePRHYTJSmoke -count=1 -v
```

## Full Live Import Test

The full live test downloads the complete PRH YTJ dataset into a temporary data directory, then processes the snapshot with a chunk size of 500:

```sh
COUNTRYDATA_PRH_YTJ_LIVE_FULL=1 GOWORK=off go test -tags=integration ./prhytj/... -run TestLivePRHYTJFullDataset -count=1 -v
```
