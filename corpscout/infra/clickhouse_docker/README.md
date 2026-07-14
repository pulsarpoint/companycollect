# ClickHouse Runtime

This folder owns the standalone ClickHouse server runtime. It can live on a
different host from Corpscout.

Start locally:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/clickhouse
docker compose up -d
```

Corpscout migrations and imports connect to ClickHouse through
`CLICKHOUSE_MIGRATE_URL` and `CLICKHOUSE_NATIVE_URL` from
`/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/.env`.

`CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1` is required because some migrations
provision least-privilege service users, such as the localhost-only reader used
by the RDAP IP-trie dictionary. The `default` account remains protected by
`CLICKHOUSE_PASSWORD`.

After enabling access management on an existing deployment, recreate the
ClickHouse container so the image entrypoint regenerates its user configuration:

```bash
docker compose up -d --force-recreate clickhouse
```

If migration 126 previously failed at `CREATE USER`, golang-migrate left version
126 dirty after dropping the old dictionary. Reset it to the last successful
version and rerun it from the Corpscout checkout:

```bash
make clickhouse-migrate-force VERSION=125
make clickhouse-migrate-up
```
