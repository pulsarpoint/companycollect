# cc-dns-axfr

`cc-dns-axfr` is the standalone AXFR exposure scanner for Common Crawl domains. It reads
authoritative nameserver endpoints from the latest summaries written by
[`cc-dns-scan`](../cc-dns-scan/), probes public endpoints, and writes endpoint state and transferred
records to ClickHouse. The two scanners are separate Go modules and have no source dependency.

The scanner deliberately permits only one active probe per nameserver IP. The worker count controls concurrency across different IPs; it does not allow concurrent transfers to the same server.

## Build and test

```sh
make build
make test
make test-race
make vet
```

The binary is written to `bin/cc-dns-axfr`.

## Run

Run one cycle with an explicit SQLite work database:

```sh
bin/cc-dns-axfr scan --scan-id 20260713 --db axfr-scan.db
```

Continuously run resumable cycles in a working directory:

```sh
bin/cc-dns-axfr run --dir /var/lib/cc-dns-axfr
```

The supervisor stores `axfr-cycle-state.json` and `axfr-scan-<cycle-id>.db` in that directory. It retains both after a failed or interrupted cycle and removes them only after successful completion.

Use `cc-dns-axfr scan -h` or `cc-dns-axfr run -h` for scanner settings, including worker count, claim batch size, transfer caps, and timeouts.

## ClickHouse configuration

The process reads these environment variables:

- `CLICKHOUSE_HOST` (default `localhost`)
- `CLICKHOUSE_NATIVE_PORT` (default `9000`)
- `CLICKHOUSE_DATABASE` (default `corpscout`)
- `CLICKHOUSE_USER` (default `default`)
- `CLICKHOUSE_PASSWORD`

Its source is `corpscout.commoncrawl_domain_dns_scan`. AXFR endpoint state is written to
`corpscout.dns_axfr_latest` and `corpscout.dns_axfr_state_changes`; transferred records are written to
`corpscout.commoncrawl_domain_dns_record_observations`. The read-only `corpscout.domain_hostnames`
view projects owners with observed A, AAAA, or CNAME records from that shared observation history.

## Deploy

Production deployment is owned by [`../deploy/cc_dns_axfr`](../deploy/cc_dns_axfr/). Deploy and verify
[`cc_dns_scan`](../deploy/cc_dns_scan/) first. Both playbooks install their units but deliberately
leave them stopped and disabled:

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

The deployment uses the legacy `cc-dns-worker` directory only for compatible resumable AXFR state.
`cc-axfr-scan` is the obsolete pre-split unit name; `journalctl -u cc-axfr-scan` will not show logs
from the standalone scanner.
