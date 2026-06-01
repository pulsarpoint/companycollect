# CompanyCollect

This repository is the monorepo for the CompanyCollect/Corpscout system.

The repo keeps deployable projects as separate folders while versioning their contracts, deployment files, and CI together:

- `corpscout/` - Corpscout scheduler, UI, database migrations, and API.
- `data-pipelines/` - enrichment services such as translation, crawling, currency, and source-specific workers.
- `corpscout_db/` - central PostgreSQL bootstrap and database-level operations.
- `deploy/` - image-only deployment compose files for remote servers.

The production direction is:

- GitHub Actions builds Docker images from changed paths only.
- Remote servers pull prebuilt images from GHCR.
- Remote servers do not build images.
- Central Postgres remains outside the service compose.
- NATS and enrichment services can run together in `deploy/services/docker-compose.yml`.

## Common Commands

```sh
make services-up
make services-logs
make services-pull
make services-down
```

Corpscout and database commands are delegated to the existing project makefiles:

```sh
make corpscout-up
make db-up
```

