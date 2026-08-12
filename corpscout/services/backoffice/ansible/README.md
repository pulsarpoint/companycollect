# Backoffice PostgreSQL Ansible deployment

This package provisions a PostgreSQL instance dedicated to Corpscout backoffice
workflow state. It deliberately does not reuse or modify Dagster's primary
PostgreSQL instance.

The current inventory deploys PostgreSQL 17.10 to `192.168.88.169` with:

- service and container `corpscout-backoffice-postgres`;
- durable data under `/opt/corpscout-backoffice-postgres/data`;
- database `corpscout_backoffice` on `192.168.88.169:5432`;
- a limited migration owner, application writer, and Dagster read-only role;
- SCRAM authentication and an allowlist limited to `192.168.88.0/24`;
- server-generated credentials retained only in the root-readable
  `/opt/corpscout-backoffice-postgres/.env` file.

Docker is the installation boundary used by the other Corpscout database
services. The role requires an existing Docker Engine and Compose plugin, but
installs and supervises the PostgreSQL container itself. It is safe to rerun:
credentials and data are retained, roles and grants are reconciled, and Compose
recreates the container only when its effective configuration changes.

## Deploy

Run from this directory:

```bash
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

ansible-playbook site.yml --syntax-check
ansible-playbook site.yml
```

The deployment includes non-mutating health, network, database-isolation, and
role-permission checks. To additionally prove that a test record survives a
controlled PostgreSQL container restart, run:

```bash
ansible-playbook verify.yml
```

The persistence check creates and removes its own temporary verification table.
It does not touch application tables.

## Credentials and client configuration

Credentials must not be copied into Git or printed in logs. An operator can
inspect the root-only environment file directly on the server when configuring
the backoffice and Dagster secrets:

```bash
ssh graovic@192.168.88.169 \
  'sudo ls -l /opt/corpscout-backoffice-postgres/.env'
```

Use the application role for backoffice requests and the Dagster role for
workflow reads. Use the owner role only for versioned schema migrations. The
`postgres` superuser is intentionally not accepted by the network access policy.

## Operations

```bash
ssh graovic@192.168.88.169 \
  'sudo systemctl status corpscout-backoffice-postgres --no-pager'

ssh graovic@192.168.88.169 \
  'cd /opt/corpscout-backoffice-postgres && sudo docker compose logs -f postgres'
```

Backups and application migrations are separate concerns. They should be added
before this database becomes the sole system of record for reviewed suggestions.
