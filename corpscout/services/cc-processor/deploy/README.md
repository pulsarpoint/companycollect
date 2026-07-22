# CC processor deployment

This playbook tests, vets, and cross-compiles `cc-enrich-worker` on the control machine, then copies the
single binary to the processing server. That is the whole deployment: no release directories, no
symlinks, no version pinning. It does not copy source code and does not modify crawl catalogs, Parquet
output, logs, or markers.

See the parent [`cc-processor` runbook](../README.md) for catalog construction, the shared environment,
ClickHouse migrations, processing commands, and completion semantics. The builder has its own
[`ansible`](../cc-warc-index-builder/ansible/README.md) deployment and is not part of this playbook.

## Target

The tracked inventory deploys the Linux/AMD64 binary to `commoncrawl2` over the existing `graovic` SSH
account. Root SSH is not required; Ansible uses `sudo` only for the remote installation.

Requirements on the control machine:

- Go 1.26.1 or newer;
- GNU Make;
- Docker with BuildKit support;
- Ansible Core; and
- SSH access to `graovic@commoncrawl2`.

Before deployment, ensure `/opt/companycollect/corpscout/commoncrawl/cc-processor/.env` exists on the
server (the playbook refuses to deploy without it) and required ClickHouse migrations are applied.

## Deploy

```bash
cd corpscout/commoncrawl/cc-processor/deploy
ansible-playbook site.yml
```

Ansible runs `make release` locally (tests + vet + the pinned CGO Docker build for `linux/amd64`), then
copies the binary to its stable command path and validates it with `--help`:

```text
/opt/companycollect/corpscout/commoncrawl/cc-processor/
├── .env                                      # never touched by the playbook
└── cc-enrich-worker/bin/cc-enrich-worker
```

The copy is atomic (temp file + rename), so deploying while a worker is running is safe: the running
process keeps its already-open executable; new invocations get the new binary.

A check run performs the complete local build and the remote preflight without changing the server:

```bash
ansible-playbook site.yml --check --diff
```

## Runtime environment

The deployment does not template or transmit credentials: `.env` lives solely on the processing server
at the processor root and is never created, modified, or read by the playbook. Launch commands from the
processor root after sourcing it:

```bash
ssh graovic@commoncrawl2
cd /opt/companycollect/corpscout/commoncrawl/cc-processor
set -a; . ./.env; set +a
./cc-enrich-worker/bin/cc-enrich-worker tech \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --crawl-id CC-MAIN-2026-25 --parts 0-99 --warc-parallel 8
```

## Roll back

There is no on-server history: redeploy the desired source revision instead —

```bash
git checkout <known-good-revision>
cd corpscout/commoncrawl/cc-processor/deploy && ansible-playbook site.yml
```

Catalogs, output, and markers are never rolled back.

## Troubleshooting

- If the local release build fails, run `make test`, `make vet`, and `make release` from the parent
  `cc-processor` directory. The worker requires the provided CGO Docker build.
- If the playbook reports a missing environment file, create and protect the processor-root `.env`
  (`chmod 0600`) on the server first.
- If `--help` validation fails after copying, the binary or a shared-library dependency is broken on the
  target; fix and rerun — reruns are idempotent.
