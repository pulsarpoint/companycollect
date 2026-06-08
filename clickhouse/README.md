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
