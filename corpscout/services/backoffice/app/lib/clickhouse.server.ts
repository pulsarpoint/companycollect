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
    // Same account as the read client and as the Dagster pipelines (owner
    // decision 2026-08-23: one ClickHouse credential set, no separate writer
    // user). Writes still fail closed when no password is configured.
    const username = process.env.CLICKHOUSE_USER?.trim() ?? "";
    const password = process.env.CLICKHOUSE_PASSWORD ?? "";
    if (!username || !password) {
      throw new Error(
        "Backoffice writes require CLICKHOUSE_USER and CLICKHOUSE_PASSWORD.",
      );
    }
    writeClient = createClient({
      url: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
      username,
      password,
      database: process.env.CLICKHOUSE_DATABASE ?? "corpscout",
      request_timeout: 30_000,
      clickhouse_settings: {
        use_top_k_dynamic_filtering: 0,
        // One review click is one row. Automatic producers may append several
        // rows per second; async inserts let ClickHouse coalesce them into ~1
        // part per second instead of one part per INSERT. wait_for_async_insert
        // keeps read-after-write: the row is durable and visible when the
        // promise resolves.
        async_insert: 1,
        wait_for_async_insert: 1,
        async_insert_busy_timeout_ms: 1000,
      },
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

/**
 * Streams a read-only ClickHouse query without buffering the full result in
 * application memory. Every yielded value is one JSONEachRow object.
 */
export async function* chStreamQuery<T>(
  sql: string,
  params?: Record<string, unknown>,
): AsyncGenerator<T> {
  const result = await getReadClient().query({
    query: sql,
    query_params: params,
    format: "JSONEachRow",
  });

  try {
    for await (const rows of result.stream<T>()) {
      for (const row of rows) {
        yield row.json<T>();
      }
    }
  } finally {
    result.close();
  }
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

/** Append reviewed decisions to the Sweden company-person correction ledger. */
export async function chInsertSeCompanyPersonCorrections<T extends object>(
  values: T[],
): Promise<void> {
  if (values.length === 0) return;
  await getWriteClient().insert({
    table: "se_company_person_correction",
    values,
    format: "JSONEachRow",
  });
}

/** Append decided values to the Sweden company-info field-value store; the
 * newest row per (company_id, field) is what Dagster publishes. */
export async function chInsertSeCompanyInfoFieldValues<T extends object>(
  values: T[],
): Promise<void> {
  if (values.length === 0) return;
  await getWriteClient().insert({
    table: "se_company_info_field_value",
    values,
    format: "JSONEachRow",
  });
}

/** Append a reviewer-row version to the SE basic-info suggestion table; the
 * fold reads the newest version per (company_id, source) through FINAL. */
export async function chInsertSeBasicInfoSuggestions<T extends object>(
  values: T[],
): Promise<void> {
  if (values.length === 0) return;
  await getWriteClient().insert({
    table: "se_company_basic_info_suggestion",
    values,
    format: "JSONEachRow",
  });
}

/** Append reviewer decisions to the Sweden company-address correction ledger;
 * Dagster's sensor picks them up. */
export async function chInsertSeCompanyAddressCorrections<T extends object>(
  values: T[],
): Promise<void> {
  if (values.length === 0) return;
  await getWriteClient().insert({
    table: "se_company_address_correction",
    values,
    format: "JSONEachRow",
  });
}
