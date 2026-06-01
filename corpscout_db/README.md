# Corpscout Central Postgres

This folder is the versioned bootstrap package for the shared Postgres server used by Corpscout and Temporal.

It manages cluster-level objects only:

- the Postgres container definition
- databases
- login roles
- database ownership and grants

Corpscout schema migrations stay in `../database/migrations`. Those migrations own extensions, schemas, tables, views, `corpscout_anon`, and object grants. Temporal owns its own tables inside the `temporal` and `temporal_visibility` databases through Temporal auto-setup.

## Current Live Snapshot

Pulled from `companycollect:5432` on 2026-05-30:

- Postgres: `17.10`
- databases:
  - `postgres`, owner `corpscout`
  - `corpscout`, owner `corpscout`
- cluster roles observed:
  - `corpscout`: login, superuser, createdb, createrole, replication, bypassrls
  - `corpscout_anon`: no-login, inherit only
- memberships:
  - `corpscout` is a member of `corpscout_anon`
- Corpscout extensions:
  - `pgcrypto` in `public`
- Corpscout non-system schemas:
  - `public`
  - `brreg_workflow`
  - `dagster_brreg`
- `corpscout_anon` currently has `USAGE` on `public` and `SELECT` on 47 public tables/views used by PostgREST.

The target cluster state added by these bootstrap files is:

- `temporal` database, owner `temporal`
- `temporal_visibility` database, owner `temporal`
- `temporal` login role with no superuser/admin permissions

## Setup

Create a local `.env` from the example:

```bash
cp .env.example .env
```

Set real passwords in `.env`. Do not commit `.env`.

Start or manage the central Postgres container:

```bash
make up
```

Apply bootstrap objects and grants:

```bash
make bootstrap
```

Verify the live state:

```bash
make verify
```

## Execution Order

Use this order for a clean server:

1. `make up`
2. `make bootstrap-cluster`
3. run Corpscout migrations from `../database/migrations`
4. start Temporal with:
   - `DB=postgres12`
   - `POSTGRES_SEEDS=companycollect`
   - `POSTGRES_USER=temporal`
   - `POSTGRES_PWD=<TEMPORAL_PASSWORD>`
   - `DBNAME=temporal`
   - `VISIBILITY_DBNAME=temporal_visibility`

For an existing server, `make bootstrap` is idempotent and can be run again after updating `.env`.

`make verify` intentionally shows some Corpscout database state, including extensions and schema privileges, but this folder does not create or update those objects. They remain service migration responsibility.

## Ownership Boundary

Use the same Postgres server, but keep separate databases:

```text
corpscout             Corpscout application data and migrations
temporal              Temporal persistence
temporal_visibility   Temporal visibility persistence
```

Do not place Temporal tables in the `corpscout` database.

## Notes

The current server uses `corpscout` as both the bootstrap/admin role and app owner. The SQL codifies that cluster-level state for reproducibility. If we later split admin and app ownership, that should be a separate migration of this bootstrap package.
