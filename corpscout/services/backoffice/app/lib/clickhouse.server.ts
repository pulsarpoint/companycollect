import "dotenv/config";
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
      //
      // use_top_k_dynamic_filtering=0: ClickHouse 26.5.1's dynamic top-K
      // filter (__topKFilter, from ORDER BY <col> LIMIT n pushdown) grabs the
      // wrong constant when the query carries bound parameters — observed
      // live on the procurement register pages as "Cannot parse date: while
      // converting 'SE' to Date" and "Expected: Date. Got: Decimal128" inside
      // __topKFilter(publication_date), flaky across identical requests.
      // Re-evaluate after a server upgrade.
      clickhouse_settings: { readonly: "2", use_top_k_dynamic_filtering: 0 },
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
