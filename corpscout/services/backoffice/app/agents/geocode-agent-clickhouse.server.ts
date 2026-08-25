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
 * Lock 2 reads ClickHouse's OWN output (`Function`, `TableExpression` node
 * labels), so a future server upgrade that renames those could make the gate
 * silently start ALLOWING url()/s3() again. `readonly=1` would still block
 * them -- this would be a loss of defense in depth, not reopened exfiltration
 * -- but silent is the wrong failure mode, so `assertGuardSelfCheck` runs the
 * whole path over a known-bad statement once per process and refuses to run
 * ANY agent query if the guard fails to catch it (fail closed on drift).
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
  ReadOnlyQueryError,
} from "~/agents/read-only-sql";

/** Rows returned to the agent per query. Enough to show a pattern, small
 * enough that a `SELECT *` cannot blow up the context window. */
export const AGENT_MAX_ROWS_PER_QUERY = 200;
/** Wall-clock ceiling per query: full scans of se_company_address (4.7M rows)
 * finish in seconds; anything slower is a mistake worth reporting. */
export const AGENT_QUERY_TIMEOUT_SECONDS = 90;
/** Characters of serialised result the agent is shown per query. */
export const AGENT_MAX_RESULT_CHARS = 40_000;

/**
 * The statement the guard self-check parses. A canonical exfiltration attempt:
 * if the AST gate lets THIS through, the gate is not working and no agent
 * query may run.
 */
export const GUARD_CANARY_SQL =
  "SELECT * FROM url('http://example.invalid/', 'JSONEachRow')";

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

/** The single dependency the guard and the canary share: hand ClickHouse a
 * statement, get its AST back as text. Injectable so the drift path is
 * testable without a server that has actually drifted. */
export type ExplainAst = (sql: string) => Promise<string>;

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

export class GuardSelfCheckError extends Error {
  constructor(detail: string) {
    super(
      `geocode agent SQL guard self-check failed -- ClickHouse AST shape may have changed (${detail}). Refusing to run agent queries.`,
    );
    this.name = "GuardSelfCheckError";
  }
}

/**
 * Runs the whole guard path over `GUARD_CANARY_SQL` and throws
 * `GuardSelfCheckError` unless the guard REFUSES it. Fails closed on every way
 * the guard could stop catching url(): drifted node labels, an empty/blank
 * AST, or EXPLAIN AST being unavailable.
 *
 * Pure over its injected `explain`, so a test can force each failure branch
 * without a real server.
 */
export async function assertGuardSelfCheck(explain: ExplainAst): Promise<void> {
  // The shape gate must reject it on its own; if that ever stops being true,
  // the canary is no longer canonical and must be updated deliberately.
  try {
    assertReadOnlyQuery(GUARD_CANARY_SQL);
  } catch (error) {
    if (error instanceof ReadOnlyQueryError) return; // refused early: guard holds.
    throw error;
  }

  let ast: string;
  try {
    ast = await explain(GUARD_CANARY_SQL);
  } catch (error) {
    throw new GuardSelfCheckError(
      `EXPLAIN AST is unavailable: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  try {
    assertInertTableFunctions(ast);
  } catch (error) {
    if (error instanceof ReadOnlyQueryError) return; // caught url(): guard holds.
    throw error;
  }

  // We reach here only when the guard ALLOWED a url() exfiltration probe.
  throw new GuardSelfCheckError(
    "the url() canary was not refused by the AST gate",
  );
}

let selfCheck: Promise<void> | undefined;

/**
 * Runs `assertGuardSelfCheck` at most once per process, memoized on the
 * promise. A failed check is NOT cached as failed forever: the memo is cleared
 * so a transient EXPLAIN outage can be retried on the next query, while a real
 * drift keeps failing every time.
 */
function ensureGuardSelfCheck(explain: ExplainAst): Promise<void> {
  if (!selfCheck) {
    selfCheck = assertGuardSelfCheck(explain).catch((error: unknown) => {
      selfCheck = undefined;
      throw error;
    });
  }
  return selfCheck;
}

/** Test-only: forget a prior self-check so the next query re-runs it. */
export function resetGuardSelfCheckForTests(): void {
  selfCheck = undefined;
}

/**
 * Validates and runs one agent-authored statement. Throws
 * `ReadOnlyQueryError` for a refused statement, `GuardSelfCheckError` if the
 * guard's own self-check has failed, and the driver's own error for a failing
 * query -- the loop turns each into feedback the agent can act on.
 *
 * The statement that is executed is character-for-character the one that was
 * parsed by `EXPLAIN AST`, so the check and the execution cannot disagree.
 */
export async function runAgentClickHouseQuery(
  request: { purpose: string; sql: string },
  options: { maxRows?: number; explain?: ExplainAst } = {},
): Promise<AgentQueryOutcome> {
  const maxRows = options.maxRows ?? AGENT_MAX_ROWS_PER_QUERY;
  const explain = options.explain ?? ((sql: string) => explainAst(sql, maxRows));

  // Fail closed before touching the request: if the guard cannot prove it
  // still catches url(), no agent query runs at all.
  await ensureGuardSelfCheck(explain);

  const sql = assertReadOnlyQuery(request.sql);
  assertInertTableFunctions(await explain(sql));
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
