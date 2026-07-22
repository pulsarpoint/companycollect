# WARC index builder deployment

This standalone playbook tests and packages `cc-warc-index-builder` on the control machine, then deploys
it only to `commoncrawl2`. It is intentionally separate from the `cc-enrich-worker` deployment.

The deploy copies a wheel and a hash-locked production requirements file. On the server it maintains a
dedicated Python 3.14 virtual environment and exposes the command at:

```text
/opt/companycollect/corpscout/commoncrawl/cc-warc-index-builder/bin/cc-warc-index-builder
```

The playbook does not copy `.env`, catalogs, candidate Parquets, logs, or other runtime data.

## Requirements

The control machine needs Ansible Core and `uv`. The `graovic` account on `commoncrawl2` also needs `uv`,
SSH access, and permission to use `sudo` for creating the `/opt` deployment directory. `uv` obtains the
requested Python 3.14 runtime if it is not already present.

## Deploy

```bash
cd corpscout/services/cc-processor/cc-warc-index-builder/ansible

ansible-playbook site.yml --ask-become-pass --check --diff
ansible-playbook site.yml --ask-become-pass
```

Omit `--ask-become-pass` when `graovic` has passwordless sudo. The tracked inventory contains only
`commoncrawl2`, so this playbook cannot deploy the builder to the worker inventory's other hosts.

Before running the deployed command, export the required RustFS credentials and builder settings. For
example, to reuse the processor's protected environment file:

```bash
set -a
. /opt/companycollect/corpscout/commoncrawl/cc-processor/.env
set +a

/opt/companycollect/corpscout/commoncrawl/cc-warc-index-builder/bin/cc-warc-index-builder \
  --base /home/graovic/cc-warc-index-data \
  --crawl CC-MAIN-2026-25 \
  --pages-per-domain 25
```
