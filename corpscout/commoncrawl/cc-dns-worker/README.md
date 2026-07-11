# cc-dns-worker

`cc-dns-worker` continuously resolves Common Crawl root domains directly against their authoritative
DNS servers. ClickHouse owns the complete input and output datasets. SQLite contains only bounded,
restartable active work and output that ClickHouse has not fully acknowledged.

## Data ownership

The scanner reads only:

- `corpscout.commoncrawl_domains`, keyset-paged by `root_domain`
- `corpscout.commoncrawl_domain_hostnames`, queried for the exact claimed root-domain batch

The Dagster `commoncrawl_domain_hostnames` asset incrementally merges Certificate Transparency
hostnames into the registry. The scanner does not query `ctlogs.hostnames`.

The worker writes:

- `corpscout.commoncrawl_domain_dns_scan`
- `corpscout.commoncrawl_domain_dns_record_observations`
- `corpscout.commoncrawl_domain_hostnames`
- `corpscout.dns_axfr_latest`
- `corpscout.dns_axfr_state_changes`

`commoncrawl_domain_dns_record_summary` is refreshed from the retry-safe observations and the frozen
legacy baseline. `dns_axfr_observations` is retained only as legacy backfill input.

## Bounded local state

One active cycle database contains:

- `scan_state`: cycle ID, source cursor, fetched count, and source-exhaustion marker
- `dns_work`: `pending`, `running`, or `ready` DNS jobs and compact summaries
- `dns_records`: DNS record outbox belonging to active `dns_work`
- `axfr_work`: copied delegation endpoints and their stable observation time
- `axfr_probes`: endpoint outcomes and transfer metrics
- `axfr_zone_records`: zone records waiting for ClickHouse acknowledgement

The domain source transaction inserts a page into `dns_work` and advances `scan_state.domain_cursor`
together. A DNS result transaction stores its records, marks the job ready, and—when AXFR is enabled
and delegation discovery was trustworthy—creates `axfr_work` with every public and private endpoint.
Private/special endpoints remain security evidence but are never dialed.

At startup, only interrupted `running` jobs return to `pending`; `ready` output is retained. A loader
deletes a ready batch only after all of its ClickHouse sinks succeed. Lost acknowledgements replay the
same logical observation identities safely.

DNS and AXFR are independent concurrent lanes with separate capacities, workers, QPS limits, and
flush intervals. Input pauses at capacity while already active work continues. A cycle completes only
after its source is exhausted and both local lanes are empty. The completed cycle database is then
deleted rather than retained as a corpus copy.

## DNS and AXFR behavior

Tier 1 uses the required `--resolvers` recursive resolver list to discover NS, NS addresses, and parent
DS. Production points this at local Unbound. Tier 2 sends `RD=0` record queries directly to publicly
dialable authoritative endpoints with per-server pacing and circuit breakers.

Special-use answers are stored and can be flagged as misconfiguration. Special-use NS endpoint
addresses are also stored, but target classification is separate from observation and prevents them
from becoming network probe destinations.

AXFR uses a dedicated scheduler. One IP is probed once per domain batch even when several NS hostnames
share it, while the outcome is persisted for every `(root_domain, name_server, name_server_ip)`
identity. Unknown outcomes never become closed. Delegation removal marks an endpoint inactive rather
than closed. `dns_axfr_latest.updated_at` uses the stable probe/delegation observation time, so a replay
of older local work cannot replace newer state.

## Commands

```text
cc-dns-worker scan [flags]   # one bounded cycle, then exit
cc-dns-worker run [flags]    # continuous production cycles
```

Both commands invoke the same cycle engine. `run` stores the current cycle ID in
`orchestrator-state.json` and resumes its derived `scan-<cycle-id>.db` after a restart.

Required/common flags:

| Flag | Default | Meaning |
|---|---:|---|
| `--resolvers` | required | recursive resolver addresses, comma separated |
| `--max-domains` | `0` | durably fetched domain limit, `0` means all |
| `--workers` | `4000` | DNS domain worker limit |
| `--domain-page-size` | `5000` | ClickHouse keyset page size |
| `--dns-work-capacity` | `20000` | maximum active DNS jobs in SQLite |
| `--dns-claim-batch` | `2000` | roots fetched in one hostname query |
| `--dns-flush-batch` | `500` | ready DNS jobs per acknowledgement pass |
| `--dns-flush-interval` | `5s` | DNS output retry/poll interval |
| `--host-enrich` | `true` | query hostname registry labels |
| `--host-cap` | `100` | ranked registry labels per root |
| `--axfr` | `true` | enable concurrent AXFR lane |
| `--axfr-workers` | `50` | AXFR domain worker limit |
| `--axfr-work-capacity` | `5000` | maximum active AXFR jobs in SQLite |
| `--axfr-claim-batch` | `100` | claimed AXFR jobs per batch |
| `--axfr-flush-batch` | `100` | ready AXFR jobs per acknowledgement pass |
| `--axfr-flush-interval` | `5s` | AXFR output retry/poll interval |

`scan` additionally accepts `--scan-id`, `--run-id`, and `--db`. `run` accepts `--dir`; it assigns a
UTC cycle ID and removes the cycle DB only after complete drain.

Run `cc-dns-worker <command> -h` for DNS timeout, QPS, in-flight, circuit-breaker, and AXFR cap flags.

## Build, test, and deploy

```bash
go test ./...
go test -race ./...
go vet ./...
staticcheck ./...
```

The Ansible role builds the Linux/amd64 binary on the control machine with `CGO_ENABLED=0`, stops an
existing service before replacing the binary, installs configuration, and starts the service:

```bash
cd deploy/ansible
ansible-playbook site.yml
```

The ClickHouse migrations must be applied before deploying a binary that uses their columns. The
Dagster hostname registry release gate is: bootstrap all 16 partitions, run one incremental
materialization successfully, and validate expected CT labels in production.

## Manual cutover and rollback

Do not resume an old full-corpus SQLite cycle with this binary. Before first deployment:

1. Stop the existing service.
2. Independently decide whether the old cycle is fully flushed or intentionally abandoned.
3. Archive/delete its old state and cycle DB manually.
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
