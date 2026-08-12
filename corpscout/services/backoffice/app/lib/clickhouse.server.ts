import "dotenv/config";
import { createClient, type ClickHouseClient } from "@clickhouse/client";

let readClient: ClickHouseClient | undefined;
let writeClient: ClickHouseClient | undefined;

function getReadClient(): ClickHouseClient {
  if (!readClient) {
    readClient = createClient({
      url: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
      username: process.env.CLICKHOUSE_USER ?? "default",
      password: process.env.CLICKHOUSE_PASSWORD ?? "",
      database: process.env.CLICKHOUSE_DATABASE ?? "corpscout",
      request_timeout: 30_000,
      // readonly=2 rejects writes while allowing named query parameters.
      // Dynamic top-K filtering is disabled because ClickHouse 26.5.1 can
      // bind the wrong query parameter inside its generated filter.
      clickhouse_settings: {
        readonly: "2",
        use_top_k_dynamic_filtering: 0,
      },
    });
  }
  return readClient;
}

function getWriteClient(): ClickHouseClient {
  if (!writeClient) {
    const username = process.env.CLICKHOUSE_WRITE_USER?.trim() ?? "";
    const password = process.env.CLICKHOUSE_WRITE_PASSWORD ?? "";
    if (!username || !password) {
      throw new Error(
        "Backoffice writes require dedicated ClickHouse writer credentials.",
      );
    }
    writeClient = createClient({
      url: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
      username,
      password,
      database: process.env.CLICKHOUSE_DATABASE ?? "corpscout",
      request_timeout: 30_000,
      clickhouse_settings: { use_top_k_dynamic_filtering: 0 },
    });
  }
  return writeClient;
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
  const result = await getReadClient().query({
    query: sql,
    query_params: params,
    format: "JSONEachRow",
  });
  return result.json<T>();
}

/** Append reviewed decisions to the immutable person-correction ledger. */
export async function chInsertPersonCorrections<T extends object>(
  values: T[],
): Promise<void> {
  if (values.length === 0) return;
  await getWriteClient().insert({
    table: "country_person_correction",
    values,
    format: "JSONEachRow",
  });
}

/** Append a replacement version of a reviewed company/domain association. */
export async function chInsertCompanyDomains<T extends object>(
  values: T[],
): Promise<void> {
  if (values.length === 0) return;
  await getWriteClient().insert({
    table: "company_domains",
    values,
    format: "JSONEachRow",
  });
}
