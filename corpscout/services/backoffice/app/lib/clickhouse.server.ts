import { createClient, type ClickHouseClient } from "@clickhouse/client";

let client: ClickHouseClient | undefined;

function getClient(): ClickHouseClient {
  if (!client) {
    client = createClient({
      url: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
      username: process.env.CLICKHOUSE_USER ?? "default",
      password: process.env.CLICKHOUSE_PASSWORD ?? "",
      database: process.env.CLICKHOUSE_DATABASE ?? "corpscout",
      request_timeout: 30_000,
    });
  }
  return client;
}

/**
 * Runs a read-only SELECT against ClickHouse and returns rows as objects.
 * User-supplied values MUST be passed via `params` (ClickHouse named query
 * params, e.g. `{q:String}` in the SQL) — never interpolated into `sql`.
 */
export async function chQuery<T>(
  sql: string,
  params?: Record<string, unknown>,
): Promise<T[]> {
  const result = await getClient().query({
    query: sql,
    query_params: params,
    format: "JSONEachRow",
  });
  return result.json<T>();
}
