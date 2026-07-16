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
      // readonly=2: server rejects INSERT/ALTER/DDL/any write, while still
      // allowing named query params over HTTP (readonly=1 would reject them,
      // as query params count as settings changes).
      clickhouse_settings: { readonly: "2" },
    });
  }
  return client;
}

/**
 * Runs a read-only SELECT against ClickHouse and returns rows as objects.
 * User-supplied values MUST be passed via `params` (ClickHouse named query
 * params, e.g. `{q:String}` in the SQL) — never interpolated into `sql`.
 *
 * Read-only access is enforced server-side: the client sends
 * `readonly=2`, so ClickHouse rejects INSERT/ALTER/DDL and any other
 * write statement regardless of what SQL is passed in.
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
