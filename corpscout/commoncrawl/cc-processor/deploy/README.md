# CC processor deployment

This playbook tests, vets, and cross-compiles `cc-crawl` and `cc-enrich-worker` on the control machine,
then deploys them to one processing server as a single atomic release. It does not copy source code and
does not modify crawl catalogs, Parquet output, logs, or `.loaded` markers.

See the parent [`cc-processor` runbook](../README.md) for catalog construction, the shared environment,
ClickHouse migration `000127`, processing commands, and completion semantics. This playbook deploys only
the paired processing runtime. It does not deploy or run `cc-warc-index-builder` on `wappalyzer`.

## Target

The tracked inventory deploys Linux/AMD64 binaries to `commoncrawl2` over the existing `graovic` SSH
account. Root SSH is not required; Ansible uses `sudo` only for the remote installation.

Requirements on the control machine:

- Go 1.26.1 or newer;
- GNU Make;
- Docker with BuildKit support;
- Ansible Core; and
- SSH access to `graovic@commoncrawl2`.

Before deployment:

- apply ClickHouse migration `000127_corpscout_commoncrawl_page_jsonld` and all earlier migrations;
- ensure either `/opt/companycollect/corpscout/commoncrawl/cc-processor/.env` or, for the first migration,
  the legacy `/opt/companycollect/corpscout/commoncrawl/.env` exists on `commoncrawl2`, is a regular file,
  and contains the RustFS, Common Crawl, ClickHouse, and optional embedding settings; and
- stop the legacy crawl and worker before the first versioned deployment. Later deployments can publish a
  new paired release while jobs pinned to an older release finish.

## Deploy

```bash
cd corpscout/commoncrawl/cc-processor/deploy
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

`--ask-become-pass` prompts for the remote `sudo` password. Omit it when `graovic` has passwordless sudo.
It is separate from `--ask-pass`, which would prompt for an SSH password.

## Release layout

```text
/opt/companycollect/corpscout/commoncrawl/cc-processor/
├── .env                              # preserved; permissions tightened to 0600
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

The processor command paths remain stable across releases. A running new
`cc-crawl` resolves the worker beside its own real executable and remains pinned to that paired release
even if a later deployment changes `current`.

The first migration from directly copied binaries refuses to run while an old crawl or worker process is
active. Later deployments may safely publish a new release while already-pinned jobs finish on the old
release. Old releases are never deleted automatically.

## Runtime environment

The deployment does not template or transmit credentials: they remain solely on the processing server.
On the first deployment, when the new processor-root file does not exist, it copies the regular legacy
`/opt/companycollect/corpscout/commoncrawl/.env` into the processor root without overwriting any existing
file. It then preserves the new file, adds or updates only the non-secret
`COMMONCRAWL_CATALOG_S3_BASE=s3://crawls/commoncrawl/catalogs` setting, and enforces mode `0600`. Later
deployments use only the processor-root file. The current production file is root-owned, so continue
launching from a root shell as before:

```bash
sudo -i
cd /opt/companycollect/corpscout/commoncrawl/cc-processor
./cc-crawl/bin/cc-crawl -base "$OUT_BASE_DIR" -crawl CC-MAIN-2026-25 -mode tech -parts 0-100
```

The source checkout also has exactly one ignored `cc-processor/.env`, created from the processor-root
`.env.example`. Do not place separate environment files inside `cc-crawl`, `cc-enrich-worker`, or the
builder. `cc-crawl` locates the processor-root file relative to its binary before falling back to the
working directory, so the deployed command does not depend on where it was launched.

## Roll back

`previous` points to the release that was active before the latest change. Switch both binaries back in
one operation:

```bash
ssh -t graovic@commoncrawl2
cd /opt/companycollect/corpscout/commoncrawl/cc-processor
sudo ln -sfn "$(readlink previous)" .current-rollback
sudo mv -Tf .current-rollback current
```

The compatibility command paths follow `current`, so no other files need to change.

Rollback affects new commands only. A running `cc-crawl` resolves `cc-enrich-worker` beside its own real,
versioned executable and remains pinned to the release with which it started. Catalogs, output, and markers
are not rolled back.

## Troubleshooting

- If the local release build fails, run `make test`, `make vet`, and `make release` from the parent
  `cc-processor` directory. The worker requires the provided CGO Docker build.
- If the first deployment reports active legacy processes, stop the listed crawl/worker commands and rerun.
  The playbook will not replace legacy paths underneath an active unversioned job.
- If the playbook reports a missing environment file, create and protect the new processor-root `.env` or
  retain the regular legacy file for the first migration. The playbook intentionally does not transmit
  credentials and never overwrites an existing new file with the legacy one.
- If validation fails after copying, the `current` link is not switched. Correct the reported binary or
  library problem and rerun; the active release remains unchanged.
- Inspect `releases/<checksum>/release.json` for the source revision, dirty-source flag, Go version, target,
  and both binary checksums.
