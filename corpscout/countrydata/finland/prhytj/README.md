# Finland PRH YTJ

This package imports Finnish company records from the PRH Open Data YTJ API v3.

Run commands from `corpscout/countrydata`.

## Local Fixture Tests

Default tests use checked-in fixtures and do not call the live PRH API:

```sh
GOWORK=off go test ./finland/prhytj/... -count=1
```

The live integration tests are also skipped by default, even when the `integration` tag is enabled:

```sh
GOWORK=off go test -tags=integration ./finland/prhytj/... -run TestLivePRHYTJ -count=1
```

## Live Smoke Test

The smoke test downloads two live pages into a temporary data directory, then processes the snapshot with a chunk size of 100:

```sh
COUNTRYDATA_PRH_YTJ_LIVE=1 GOWORK=off go test -tags=integration ./finland/prhytj/... -run TestLivePRHYTJSmoke -count=1 -v
```

## Full Live Import Test

The full live test downloads the complete PRH YTJ dataset into a temporary data directory, then processes the snapshot with a chunk size of 500:

```sh
COUNTRYDATA_PRH_YTJ_LIVE_FULL=1 GOWORK=off go test -tags=integration ./finland/prhytj/... -run TestLivePRHYTJFullDataset -count=1 -v
```
