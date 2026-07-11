# cc-dns-worker

`cc-dns-worker` is one binary containing two independent scanner packages. `dnsscan` resolves Common
Crawl root domains directly against authoritative DNS servers. `axfrscan` independently probes the
latest authoritative endpoints for zone-transfer exposure. The binary starts them as separate
top-level goroutines; neither scanner reads, throttles, enqueues, or waits for the other scanner's
work.

## Data ownership

The DNS scanner reads:

- `corpscout.commoncrawl_domains`, keyset-paged by `root_domain`
- `corpscout.commoncrawl_domain_hostnames`, queried for the exact claimed root-domain batch

The Dagster `commoncrawl_domain_hostnames` asset incrementally merges Certificate Transparency
hostnames into the registry. The scanner does not query `ctlogs.hostnames`.

The AXFR scanner independently keyset-pages `corpscout.commoncrawl_domain_dns_scan FINAL`. It
reclassifies every current endpoint address before creating work and probes only public addresses.
It does not consume DNS SQLite output or wait for the current DNS traversal.

The worker writes:

- `corpscout.commoncrawl_domain_dns_scan`
- `corpscout.commoncrawl_domain_dns_record_observations`
- `corpscout.commoncrawl_domain_hostnames`
- `corpscout.dns_axfr_latest`
- `corpscout.dns_axfr_state_changes`

`commoncrawl_domain_dns_record_observations` is the authoritative DNS-record history.
Every query or AXFR RR is retained, including unknown RFC3597 types. `record_type_code` and
`record_class_code` hold protocol identity, `value` holds complete presentation-format RDATA, and
`rdata_wire` holds uncompressed binary RDATA. AXFR rows also identify the endpoint in `name_server`
and `name_server_ip`. Zero/empty values in those columns mean a legacy observation did not capture
that metadata, not that the DNS record had type or class zero.
`commoncrawl_ip_addresses` incrementally aggregates canonical A/AAAA values for GeoIP enrichment.
`dns_axfr_observations` is retained only as legacy backfill input.

Apply ClickHouse migration `000123_corpscout_dns_observations_universal_rr` before starting a worker
built from this version. The migration is metadata-only and does not rewrite historical observation
parts.

## Independent bounded local state

The scanners never share a SQLite connection or queue. A production run maintains:

- `dns-cycle-state.json` and `dns-scan-<cycle-id>.db`, owned only by `dnsscan`
- `axfr-cycle-state.json` and `axfr-scan-<cycle-id>.db`, owned only by `axfrscan`

The DNS database contains its source cursor, cumulative health counters, `dns_work`, and
`dns_records`. A DNS result transaction stores its records, marks the job ready, and advances durable
statistics. It never creates AXFR work. The persistent DNS worker pool refills before the current
buffer drains, so a slow tail cannot leave the other resolver workers idle.

The AXFR database contains its own ClickHouse cursor, durable probe counters, current delegation
snapshots, endpoint jobs, and transferred-record outbox. Work is keyed by unique public
`(root_domain, name_server_ip)`; a shared IP is pulled once and its outcome is applied to each current
NS hostname identity. Each endpoint result is committed immediately, without a domain-batch result
barrier.

At startup, only interrupted `running` jobs return to `pending`; `ready` output is retained. A loader
deletes a ready batch only after all of its ClickHouse sinks succeed. Lost acknowledgements replay the
same logical observation identities safely.

Each scanner has its own capacity, workers, QPS limits, retry loop, completion condition, and
one-second statistics. One scanner can fail and retry while the other continues. Each completed cycle
database is deleted only after that scanner's own ClickHouse outbox drains.

## DNS and AXFR behavior

Tier 1 uses the required `--resolvers` recursive resolver list to discover NS, NS addresses, and parent
DS. Production points this at local Unbound. Tier 2 sends `RD=0` record queries directly to publicly
dialable authoritative endpoints with per-server pacing and circuit breakers.

Special-use answers are stored and can be flagged as misconfiguration. Special-use NS endpoint
addresses are also stored, but target classification is separate from observation and prevents them
from becoming network probe destinations.

AXFR uses a dedicated scheduler. One IP is probed once per domain even when several NS hostnames
share it, while the outcome is persisted for every `(root_domain, name_server, name_server_ip)`
identity. Unknown outcomes never become closed. Delegation removal marks an endpoint inactive rather
than closed. `dns_axfr_latest.updated_at` uses the stable probe/delegation observation time, so a replay
of older local work cannot replace newer state.

## Commands

```text
cc-dns-worker scan [flags]   # one bounded cycle, then exit
cc-dns-worker run [flags]    # continuous production cycles
```

`run` starts independent DNS and AXFR supervisors. Each supervisor resumes only its own state and
retries only its own failures. `--dns=false` or `--axfr=false` can intentionally disable one scanner;
both default to true.

Required/common flags:

| Flag | Default | Meaning |
|---|---:|---|
| `--resolvers` | required with DNS | recursive resolver addresses, comma separated |
| `--dns` | `true` | run the independent DNS scanner |
| `--max-domains` | `0` | durably fetched domain limit, `0` means all |
| `--workers` | `4000` | DNS domain worker limit |
| `--domain-page-size` | `5000` | ClickHouse keyset page size |
| `--dns-work-capacity` | `20000` | maximum active DNS jobs in SQLite |
| `--dns-claim-batch` | `2000` | roots fetched in one hostname query |
| `--dns-flush-batch` | `500` | ready DNS jobs per acknowledgement pass |
| `--dns-flush-interval` | `5s` | DNS output retry/poll interval |
| `--host-enrich` | `true` | query hostname registry labels |
| `--host-cap` | `100` | ranked registry labels per root |
| `--axfr` | `true` | run the independent AXFR scanner |
| `--axfr-workers` | `50` | AXFR endpoint worker limit |
| `--axfr-domain-page-size` | `1000` | latest DNS-summary roots per AXFR source page |
| `--axfr-work-capacity` | `5000` | maximum active AXFR domains in AXFR SQLite |
| `--axfr-claim-batch` | `100` | claimed endpoint probes per batch |
| `--axfr-flush-batch` | `100` | ready AXFR domains per acknowledgement pass |
| `--axfr-flush-interval` | `5s` | AXFR output retry/poll interval |

`scan` additionally accepts `--scan-id`, `--run-id`, `--dns-db`, and `--axfr-db`. `run` accepts
`--dir`; each supervisor assigns its own UTC cycle ID and removes only its own completed database.

Run `cc-dns-worker <command> -h` for DNS timeout, QPS, in-flight, circuit-breaker, and AXFR cap flags.

## Build, test, and deploy

```bash
go test ./...
go test -race ./...
go vet ./...
staticcheck ./...
```

The Ansible role builds the Linux/amd64 binary on the control machine with `CGO_ENABLED=0`, stops an
existing service before replacing the binary, installs configuration, and leaves the service stopped
and disabled:

```bash
cd deploy/ansible
ansible-playbook site.yml
```

The ClickHouse migrations must be applied before deploying a binary that uses their columns. The
Dagster hostname registry release gate is: bootstrap all 16 partitions, run one incremental
materialization successfully, and validate expected CT labels in production.

## Manual cutover and rollback

The new supervisors deliberately ignore the coupled worker's `orchestrator-state.json` and
`scan-<cycle-id>.db`. Before first start of this binary:

1. Stop the existing service.
2. Independently decide whether the old cycle is fully flushed or intentionally abandoned.
3. Archive/delete its old state and cycle DB manually; deployment does not alter them.
4. Apply migrations and complete the hostname-registry release gate.
5. Deploy and start the bounded worker.

Rollback stops the bounded service, preserves its active DB for diagnosis, restores the old binary,
and starts only a separately verified old cycle. Legacy ClickHouse tables remain during the rollback
window; removing them is a delayed migration, not part of first deployment.

## Baseline captured 2026-07-11

Read-only inspection of `hetzner01` before this rewrite found:

- `cc-dns-scan.service` stopped at 10:51:18 CEST after 28m33s
- peak memory 3.9 GB
- cycle `20260711T082245Z` still in the legacy seeding phase
- one ClickHouse input timeout followed by a reported 33,619,254 seeded rows
- no matching cycle DB present under the deploy directory
- an invalid legacy state marker (`phase` was misspelled)
- ClickHouse row counts: 32,291,477 DNS summaries, 0 retry-safe DNS observations,
  241,474,568 registry rows, 0 AXFR latest rows, and 0 AXFR state changes

That state is not a valid resume candidate. Production cutover remains an explicit operator action;
the repository does not automatically alter or abandon it.
