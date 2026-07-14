# Common Crawl DNS scanner

`cc-dns-scan` resolves Common Crawl root domains against authoritative DNS servers and loads durable
DNS summaries and record observations into ClickHouse. It is a standalone Go module, binary, worker
pool, retry loop, and resumable SQLite pipeline.

AXFR probing is owned by the separate [`cc-dns-axfr`](../cc-dns-axfr/) project. The two processes do
not share Go packages, queues, SQLite databases, or completion conditions. Their only data-flow
boundary is ClickHouse: AXFR reads the latest DNS delegation summaries written by this scanner.

## Data flow

The scanner reads:

- `corpscout.commoncrawl_domains`, keyset-paged by `root_domain`; and
- `corpscout.domain_hostnames`, queried for each claimed root-domain batch.

It writes:

- `corpscout.commoncrawl_domain_dns_scan`;
- `corpscout.commoncrawl_domain_dns_record_observations`;
- `corpscout.commoncrawl_ip_addresses` through the corresponding ClickHouse materialized view.

`commoncrawl_domain_dns_record_observations` is the authoritative record history. Every known or
unknown RR type is retained with its numeric type/class, presentation-format RDATA, and uncompressed
wire RDATA. `domain_hostnames` is a read-only view of record owners with an observed A, AAAA, or
CNAME record; neither DNS scanner writes a separate hostname registry.

## Resolution behavior

Tier 1 uses the required `--resolvers` list to discover authoritative nameservers, their addresses,
and the parent DS response. Production points it at local Unbound. Tier 2 sends `RD=0` queries
directly to publicly dialable authoritative endpoints with per-IP pacing and circuit breakers.

Special-use answers are stored as evidence. Special-use nameserver addresses are also stored, but
the target classifier prevents them from becoming network destinations.

The durable database contains the source cursor, cumulative counters, `dns_work`, and `dns_records`.
At restart, interrupted `running` work returns to `pending`; ready output remains until every
ClickHouse sink succeeds. A completed cycle is deleted only after its outbox drains.

The supervisor owns these files:

```text
dns-cycle-state.json
dns-scan-<cycle-id>.db
dns-scan-<cycle-id>.db-wal
dns-scan-<cycle-id>.db-shm
```

Production initially keeps them in the legacy `cc-dns-worker` state directory so an in-progress
cycle resumes without copying SQLite and WAL files. New binaries and configuration are deployed only
under the `cc-dns-scan` runtime directory.

## Commands

```text
cc-dns-scan scan [flags]  # run one bounded DNS cycle
cc-dns-scan run [flags]   # continuously supervise resumable DNS cycles
```

Important flags:

| Flag | Default | Meaning |
|---|---:|---|
| `--resolvers` | required | comma-separated recursive resolver addresses |
| `--max-domains` | `0` | durable domain limit; `0` means all |
| `--workers` | `4000` | concurrently resolved domains |
| `--discovery-qps` | `50` | discovery queries/second per recursive resolver |
| `--discovery-inflight` | `500` | discovery queries in flight per resolver |
| `--per-server-qps` | `10` | direct queries/second per authoritative IP |
| `--per-server-inflight` | `3` | direct queries in flight per authoritative IP |
| `--domain-page-size` | `5000` | ClickHouse source page size |
| `--dns-work-capacity` | `20000` | maximum active domains in SQLite |
| `--dns-claim-batch` | `2000` | domains claimed per batch |
| `--dns-flush-batch` | `500` | ready domains per acknowledgement pass |
| `--dns-flush-interval` | `5s` | output retry/poll interval |
| `--host-enrich` | `true` | query ranked labels from the confirmed hostname view |
| `--host-cap` | `100` | confirmed hostname labels queried per domain |

`scan` also accepts `--scan-id`, `--run-id`, and `--dns-db`. `run` accepts `--dir` and assigns a UTC
cycle ID. Run either command with `-h` for the complete flag set.

## Build and verify

```bash
make build
make test
make vet
go test -race ./...
staticcheck ./...
```

## Deploy

Deployment is owned by [`../deploy/cc_dns_scan`](../deploy/cc_dns_scan/), including the DNS host's
Unbound and OS tuning. Deploy this package before [`cc_dns_axfr`](../deploy/cc_dns_axfr/). The
playbook installs `cc-dns-scan.service` but deliberately leaves it stopped and disabled:

```bash
cd ../deploy/cc_dns_scan
ansible-playbook site.yml
ssh root@hetzner01 'systemctl enable --now cc-dns-scan'
ssh root@hetzner01 'journalctl -u cc-dns-scan -n 100 -f'

cd ../cc_dns_axfr
ansible-playbook site.yml
ssh root@hetzner01 'systemctl enable --now cc-dns-axfr'
ssh root@hetzner01 'journalctl -u cc-dns-axfr -n 100 -f'
```

The AXFR playbook also leaves its service stopped and disabled. `cc-axfr-scan` is obsolete and is not
the unit to use for AXFR logs.

Historical design and hardening plans remain under `docs/superpowers/`; their old project and command
names describe the repository at the time those plans were executed.
