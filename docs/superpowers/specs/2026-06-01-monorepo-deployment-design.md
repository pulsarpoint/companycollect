# Monorepo and Deployment Design

## Decision

Create a clean `companycollect` monorepo snapshot without preserving the old repository histories. Keep deployable systems as separate folders inside the repo:

- `corpscout/`
- `data-pipelines/`
- `corpscout_db/`
- `deploy/`

This is a monorepo for versioning, CI, deployment, and contracts. It is not a merge of all application code into one package.

## Why

Corpscout, NATS subjects, Temporal workflows, enrichment services, database bootstrap, and deployment files are now changing together. A monorepo reduces coordination cost while still allowing each service to keep its own language, runtime, tests, and Dockerfile.

## Deployment Shape

Remote servers pull prebuilt GHCR images. They do not build images.

The first production compose target is `deploy/services/docker-compose.yml`, which runs:

- NATS
- translation API and worker
- crawl API and worker
- currency service
- BRREG financial service

Central Postgres remains managed by `corpscout_db`. Corpscout can continue running locally during active development, then move into remote compose later.

## CI Shape

Root GitHub Actions build Docker images based on changed paths:

- `corpscout/scheduler/**`
- `corpscout/ui/**`
- `data-pipelines/services/translation-service/**`
- `data-pipelines/services/crawl-service/**`
- `data-pipelines/services/currency-service/**`
- `data-pipelines/services/brreg-financial-service/**`
- existing worker paths when still needed

Each image uses Buildx cache and publishes branch, SHA, and `latest` tags.

## Non-Goals

- Do not preserve old Git history.
- Do not use Git submodules.
- Do not merge Go and Python services into one application.
- Do not make the remote server build images.

