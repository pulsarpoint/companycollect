# Common Crawl runtime deployment

This playbook tests, vets, and cross-compiles `cc-crawl` and `cc-enrich-worker` on the control machine,
then deploys them to one processing server as a single atomic release. It does not copy source code and
does not modify crawl catalogs, Parquet output, logs, or `.loaded` markers.

## Target

The tracked inventory deploys Linux/AMD64 binaries to `commoncrawl2` over the existing `graovic` SSH
account. Root SSH is not required; Ansible uses `sudo` only for the remote installation.

Requirements on the control machine:

- Go 1.26.1 or newer;
- GNU Make;
- Docker with BuildKit support;
- Ansible Core; and
- SSH access to `graovic@commoncrawl2`.

## Deploy

```bash
cd corpscout/commoncrawl/deploy/ansible
ansible-playbook site.yml --limit commoncrawl2 --ask-become-pass
```

Ansible runs `make release` locally. That target tests and vets `cc-raw`, `cc-enrich-worker`, and
`cc-crawl`, then builds the `linux/amd64` binaries under the ignored `dist/` directory. The Linux build
runs in the pinned Docker toolchain because the DuckDB Go driver requires CGO; source code is still
compiled on the control machine and only the resulting binaries are deployed.

A check run performs the complete local build and the remote preflight without changing the server:

```bash
ansible-playbook site.yml --limit commoncrawl2 --ask-become-pass --check --diff
```

## Release layout

```text
/opt/companycollect/corpscout/commoncrawl/
├── .env                              # preserved; permissions tightened to 0600
├── data/                             # never touched
├── releases/
│   └── <paired-binary-checksum>/
│       ├── bin/
│       │   ├── cc-crawl
│       │   └── cc-enrich-worker
│       └── release.json
├── current -> releases/<checksum>    # switched atomically after validation
├── previous -> releases/<checksum>   # prior release, when one exists
├── cc-crawl/bin/cc-crawl -> ../../current/bin/cc-crawl
└── cc-enrich-worker/bin/cc-enrich-worker -> ../../current/bin/cc-enrich-worker
```

The legacy command paths remain valid, so existing operator commands do not change. A running new
`cc-crawl` resolves the worker beside its own real executable and remains pinned to that paired release
even if a later deployment changes `current`.

The first migration from directly copied binaries refuses to run while an old crawl or worker process is
active. Later deployments may safely publish a new release while already-pinned jobs finish on the old
release. Old releases are never deleted automatically.

## Runtime environment

The deployment does not template or copy credentials: they remain solely on the processing server. It
requires the existing `.env`, preserves its ownership, adds or updates only the non-secret
`COMMONCRAWL_CATALOG_S3_BASE=s3://crawls/commoncrawl/catalogs` setting, and changes the file mode to
`0600`. The current production file is root-owned, so continue launching from a root shell as before:

```bash
sudo -i
cd /opt/companycollect/corpscout/commoncrawl
./cc-crawl/bin/cc-crawl -base "$OUT_BASE_DIR" -crawl CC-MAIN-2026-25 -mode tech -parts 0-100
```

## Roll back

`previous` points to the release that was active before the latest change. Switch both binaries back in
one operation:

```bash
ssh -t graovic@commoncrawl2
cd /opt/companycollect/corpscout/commoncrawl
sudo ln -sfn "$(readlink previous)" .current-rollback
sudo mv -Tf .current-rollback current
```

The compatibility command paths follow `current`, so no other files need to change.
