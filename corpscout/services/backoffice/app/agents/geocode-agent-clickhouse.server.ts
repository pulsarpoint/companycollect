/**
 * The agent's only capability: run one read-only statement against ClickHouse
 * and get rows back.
 *
 * Three locks, in the order they apply, and it is worth being precise about
 * which one carries the weight:
 *
 * 1. `assertReadOnlyQuery` checks the statement's SHAPE (one statement, a read
 *    keyword, no FORMAT clause). String matching, so treat it as fast feedback
 *    and defense in depth -- not as a boundary.
 * 2. `EXPLAIN AST` through this same read-only connection, then
 *    `assertInertTableFunctions` over ClickHouse's OWN parse: every table
 *    function in the statement must be on the inert allowlist. This is what
 *    stops `url()`, `file()`, `s3()`, `remote()` and `executable()` -- all of
 *    which `readonly=1` would run happily, since they are reads -- and it
 *    cannot be evaded by heredoc literals or backtick-quoted names, because
 *    the server has already resolved them by the time it answers.
 * 3. `readonly=1` on every request, including the EXPLAIN: ClickHouse refuses
 *    writes and DDL server-side whatever text reaches it. This one is never
 *    turned off, whatever (1) and (2) conclude.
 *
 * This is a SEPARATE client from clickhouse.server.ts's: that one runs the
 * backoffice's own hand-written SQL under `readonly=2` (which permits settings
 * changes and named parameters). The agent gets the stricter `readonly=1`, no
 * query parameters, and hard result/time caps.
 */
import "dotenv/config";
import { createClient, type ClickHouseClient } from "@clickhouse/client";
import {
  assertInertTableFunctions,
  assertReadOnlyQuery,
} from "~/agents/read-only-sql";

/** Rows returned to the agent per query. Enough to show a pattern, small
 * enough that a `SELECT *` cannot blow up the context window. */
export const AGENT_MAX_ROWS_PER_QUERY = 200;
/** Wall-clock ceiling per query: full scans of se_company_address (4.7M rows)
 * finish in seconds; anything slower is a mistake worth reporting. */
export const AGENT_QUERY_TIMEOUT_SECONDS = 90;
/** Characters of serialised result the agent is shown per query. */
export const AGENT_MAX_RESULT_CHARS = 40_000;

let agentClient: ClickHouseClient | undefined;

function getAgentClient(maxRows: number): ClickHouseClient {
  if (!agentClient) {
    agentClient = createClient({
      url: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
      username: process.env.CLICKHOUSE_USER ?? "default",
      password: process.env.CLICKHOUSE_PASSWORD ?? "",
      database: process.env.CLICKHOUSE_DATABASE ?? "corpscout",
      request_timeout: (AGENT_QUERY_TIMEOUT_SECONDS + 30) * 1000,
      clickhouse_settings: {
        // Settings are applied in order; `readonly` last so the caps above it
        // still take effect (readonly=1 forbids further settings changes).
        max_execution_time: AGENT_QUERY_TIMEOUT_SECONDS,
        max_result_rows: String(maxRows),
        // Truncate rather than throw: a wide result is still useful evidence.
        result_overflow_mode: "break",
        // The agent writes its own aggregates over multi-million-row tables;
        // progress headers keep a slow one from being cut by an idle timeout.
        send_progress_in_http_headers: 1,
        http_headers_progress_interval_ms: "10000",
        readonly: "1",
      },
    });
  }
  return agentClient;
}

export interface AgentQueryOutcome {
  purpose: string;
  sql: string;
  rows: unknown[];
  rowCount: number;
  truncated: boolean;
  elapsedMs: number;
}

/**
 * Asks ClickHouse to parse the statement and returns its AST as text. Parsing
 * only: `EXPLAIN AST` reads no data and touches no table, so this costs one
 * cheap round trip and tells us exactly what the server thinks it was sent.
 * A syntax error surfaces here, before anything runs.
 */
async function explainAst(sql: string, maxRows: number): Promise<string> {
  const result = await getAgentClient(maxRows).query({
    query: `EXPLAIN AST ${sql}`,
    format: "TabSeparatedRaw",
  });
  return result.text();
}

/**
 * Validates and runs one agent-authored statement. Throws
 * `ReadOnlyQueryError` for a refused statement and the driver's own error for
 * a failing one -- the loop turns either into feedback the agent can act on.
 *
 * The statement that is executed is character-for-character the one that was
 * parsed by `EXPLAIN AST`, so the check and the execution cannot disagree.
 */
export async function runAgentClickHouseQuery(
  request: { purpose: string; sql: string },
  options: { maxRows?: number } = {},
): Promise<AgentQueryOutcome> {
  const maxRows = options.maxRows ?? AGENT_MAX_ROWS_PER_QUERY;
  const sql = assertReadOnlyQuery(request.sql);
  assertInertTableFunctions(await explainAst(sql, maxRows));
  const startedAt = Date.now();
  const result = await getAgentClient(maxRows).query({
    query: sql,
    format: "JSONEachRow",
  });
  const rows = await result.json<unknown>();
  return {
    purpose: request.purpose,
    sql,
    rows: rows.slice(0, maxRows),
    rowCount: rows.length,
    truncated: rows.length > maxRows,
    elapsedMs: Date.now() - startedAt,
  };
}

/** Serialises one outcome for the next prompt, capped so a wide result cannot
 * crowd out the instructions. */
export function formatAgentQueryOutcome(outcome: AgentQueryOutcome): string {
  const body = JSON.stringify(outcome.rows, null, 1);
  const clipped =
    body.length > AGENT_MAX_RESULT_CHARS
      ? `${body.slice(0, AGENT_MAX_RESULT_CHARS)}\n... [result text truncated]`
      : body;
  const header = [
    `purpose: ${outcome.purpose || "(none given)"}`,
    `sql: ${outcome.sql}`,
    `rows: ${outcome.rowCount}${outcome.truncated ? " (capped)" : ""} in ${outcome.elapsedMs} ms`,
  ].join("\n");
  return `${header}\nresult:\n${clipped}`;
}

/** Closes the agent's ClickHouse connection. Tests and scripts only. */
export async function closeAgentClickHouseClient(): Promise<void> {
  const current = agentClient;
  agentClient = undefined;
  await current?.close();
}
