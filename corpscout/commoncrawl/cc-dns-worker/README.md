# Common Crawl DNS workers

This module builds two independently deployed scanners:

- `cc-dns-worker` resolves Common Crawl root domains directly against authoritative DNS servers.
- `cc-axfr-worker` probes the latest authoritative endpoints for zone-transfer exposure.

The binaries have separate processes, flags, worker pools, retry loops, local state, and systemd
services. AXFR reads the latest persisted DNS summaries from ClickHouse; it does not wait for or
coordinate with a live DNS worker process.

## Data ownership

The DNS scanner reads:

- `corpscout.commoncrawl_domains`, keyset-paged by `root_domain`
- `corpscout.commoncrawl_domain_hostnames`, queried for the exact claimed root-domain batch

The Dagster `commoncrawl_domain_hostnames` asset incrementally merges Certificate Transparency
hostnames into the registry. The scanner does not query `ctlogs.hostnames`.

The AXFR scanner independently keyset-pages `corpscout.commoncrawl_domain_dns_scan FINAL`. It
reclassifies every current endpoint address before creating work and probes only public addresses.
It does not consume DNS SQLite output or wait for the current DNS traversal.

The workers write:

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

The binaries never share a SQLite connection or queue. Production maintains:

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

Each binary has its own capacity, workers, QPS limits, retry loop, completion condition, and
five-second statistics by default. One service can fail, restart, or be deployed while the other
continues. Each completed cycle database is deleted only after that scanner's own ClickHouse outbox
drains.

The DNS health line reports only live in-memory rates for the latest five-second interval: `qps` is
the number of DNS queries sent per second and `errps` is the number of real DNS errors per second.
Real errors are timeouts, transport failures, and error RCODEs such as SERVFAIL, REFUSED, or NOTAUTH.
Valid NOERROR/NODATA and NXDOMAIN responses are excluded. These process-local counters reset on
restart and are never loaded from or checkpointed to SQLite.

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
cc-dns-worker scan [flags]    # one bounded authoritative-DNS cycle
cc-dns-worker run [flags]     # continuous authoritative-DNS cycles
cc-axfr-worker scan [flags]   # one bounded AXFR cycle
cc-axfr-worker run [flags]    # continuous AXFR cycles
```

Each `run` command resumes only its own state and retries only its own failures.

DNS flags:

| Flag | Default | Meaning |
|---|---:|---|
| `--resolvers` | required with DNS | recursive resolver addresses, comma separated |
| `--max-domains` | `0` | durably fetched domain limit, `0` means all |
| `--workers` | `4000` | DNS domain worker limit |
| `--domain-page-size` | `5000` | ClickHouse keyset page size |
| `--dns-work-capacity` | `20000` | maximum active DNS jobs in SQLite |
| `--dns-claim-batch` | `2000` | roots fetched in one hostname query |
| `--dns-flush-batch` | `500` | ready DNS jobs per acknowledgement pass |
| `--dns-flush-interval` | `5s` | DNS output retry/poll interval |
| `--host-enrich` | `true` | query hostname registry labels |
| `--host-cap` | `100` | ranked registry labels per root |

AXFR flags:

| Flag | Default | Meaning |
|---|---:|---|
| `--max-domains` | `0` | durably fetched domain limit, `0` means all |
| `--workers` | `50` | AXFR endpoint worker limit |
| `--per-server-qps` | `5` | AXFR starts per second for one NS IP |
| `--timeout` | `20s` | AXFR connection deadline |
| `--max-records` | `50000` | records retained per transfer |
| `--max-bytes` | `67108864` | bytes retained per transfer |
| `--domain-page-size` | `1000` | latest DNS-summary roots per source page |
| `--work-capacity` | `5000` | maximum active AXFR domains in SQLite |
| `--claim-batch` | `100` | claimed endpoint probes per batch |
| `--flush-batch` | `100` | ready AXFR domains per acknowledgement pass |
| `--flush-interval` | `5s` | AXFR output retry/poll interval |

DNS `scan` additionally accepts `--scan-id`, `--run-id`, and `--dns-db`. AXFR `scan` accepts
`--scan-id` and `--db`. Both `run` commands accept `--dir`, assign their own UTC cycle ID, and remove
only their own completed database.

Run either binary with `<command> -h` for its complete flag set.

## Build, test, and deploy

```bash
go test ./...
go test -race ./...
go vet ./...
staticcheck ./...
```

The Ansible roles build both Linux/AMD64 binaries on the control machine with `CGO_ENABLED=0`, stop
each corresponding service before replacing it, install separate systemd units, and leave both
services stopped and disabled:

```bash
cd deploy/ansible
ansible-playbook site.yml
```

The ClickHouse migrations must be applied before deploying a binary that uses their columns. The
Dagster hostname registry release gate is: bootstrap all 16 partitions, run one incremental
materialization successfully, and validate expected CT labels in production.

## Split-service cutover and rollback

The split preserves `dns-cycle-state.json`/`dns-scan-*.db` and
`axfr-cycle-state.json`/`axfr-scan-*.db` in the existing deployment directory. To cut over without
duplicating AXFR probes:

1. Stop the existing combined `cc-dns-scan.service`.
2. Deploy both binaries and units.
3. Start the new DNS-only `cc-dns-scan.service`.
4. Start `cc-axfr-scan.service`.
5. Confirm each service resumes its existing cycle ID before enabling either service at boot.

Never run an older combined worker and `cc-axfr-scan.service` simultaneously. Rollback must stop both
split services before restoring and starting the combined binary. Deployment does not delete or move
either scanner's active state.

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
