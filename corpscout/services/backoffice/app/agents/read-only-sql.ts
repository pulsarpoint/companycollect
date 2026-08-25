/**
 * The statement gate between the analysis agent and ClickHouse.
 *
 * Threat model, stated plainly so nobody trusts the wrong layer:
 *
 * - The STRONG barrier is server-side: every agent statement is sent with
 *   `readonly=1` (geocode-agent-clickhouse.server.ts), so ClickHouse itself
 *   refuses writes and DDL no matter what text reaches it.
 * - The barrier against ClickHouse's *read-only* escape hatches -- `url()`,
 *   `file()`, `s3()`, `remote()`, `executable()` and the ~90 other table
 *   functions a server exposes, all of which `readonly=1` happily runs -- is
 *   `assertInertTableFunctions`, which reads ClickHouse's OWN parse of the
 *   statement (`EXPLAIN AST`) and allows only an allowlist of inert local
 *   table functions. It does not try to out-lex ClickHouse.
 * - `assertReadOnlyQuery` below is defense in depth and fast feedback, not a
 *   security boundary. Its string matching CAN be evaded (heredoc literals,
 *   backtick-quoted identifiers), which is exactly why the AST check exists
 *   and why `readonly=1` is never turned off.
 *
 * Pure (no imports): unit-testable without a database.
 */

/** Statement forms the agent is allowed to send. Anything else is refused. */
const ALLOWED_LEADING_KEYWORDS = ["SELECT", "WITH", "DESCRIBE", "DESC", "SHOW", "EXPLAIN"];

/**
 * Words that must not appear anywhere in the statement, as whole words.
 *
 * `readonly=1` already blocks every write and DDL; these are listed so the
 * refusal is explainable to the agent in its own turn, and because a few of
 * them (SET, SYSTEM, KILL) would otherwise burn a turn on a server error.
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
 * The only table functions the agent's statements may name, checked against
 * ClickHouse's own AST rather than against the query text.
 *
 * An ALLOWLIST, deliberately: a denylist of table functions is a losing race
 * against a server that ships around ninety of them and gains more every
 * release. Everything here is inert -- it reads this server's own data or
 * generates rows locally, and reaches no network, no filesystem, no other
 * cluster.
 */
export const INERT_TABLE_FUNCTIONS = [
  "merge",
  "view",
  "viewifpermitted",
  "numbers",
  "numbers_mt",
  "values",
  "null",
  "generaterandom",
  "generateseries",
  "generate_series",
  "zeros",
  "zeros_mt",
  "dictionary",
];

const INERT_TABLE_FUNCTION_SET = new Set(INERT_TABLE_FUNCTIONS);

export class ReadOnlyQueryError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ReadOnlyQueryError";
  }
}

/**
 * Strips string literals (including `$tag$ heredocs $tag$`), quoted
 * identifiers and comments so the cheap keyword scan below is not fooled by
 * `'insert'` in a LIKE pattern. It is NOT relied on for safety: a literal
 * that hides a call is caught by the AST check, not here.
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
    if (char === "$") {
      // Heredoc literal: $tag$ ... $tag$ (ClickHouse allows an empty tag).
      const tagMatch = /^\$[A-Za-z0-9_]*\$/.exec(sql.slice(index));
      if (tagMatch) {
        const tag = tagMatch[0];
        const end = sql.indexOf(tag, index + tag.length);
        index = end === -1 ? sql.length : end + tag.length;
        out += "''";
        continue;
      }
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
      // Quoted identifiers keep a placeholder name, so `` `url`(...) `` still
      // reads as a call to the scan below instead of collapsing to "(".
      out += quote === "`" || quote === '"' ? "quoted_identifier" : "''";
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

/**
 * First gate: the statement's SHAPE. Returns the statement to hand to
 * ClickHouse, or throws `ReadOnlyQueryError` with a message written for the
 * agent to read and correct.
 *
 * Accepts exactly ONE statement (a single trailing semicolon is tolerated):
 * multi-statement text is refused outright rather than partially executed.
 * Table functions are NOT judged here -- see `assertInertTableFunctions`.
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

  // The application owns the wire format (the driver appends its own FORMAT
  // clause, and the AST probe appends one too). A statement carrying its own
  // would produce a parse error the agent could not diagnose.
  if (hasWord(bare, "FORMAT")) {
    throw new ReadOnlyQueryError(
      "Do not add a FORMAT clause; results are returned as JSON rows automatically.",
    );
  }

  for (const keyword of FORBIDDEN_KEYWORDS) {
    if (hasWord(bare, keyword)) {
      throw new ReadOnlyQueryError(
        `The keyword ${keyword} is not allowed. This connection is read-only analysis only.`,
      );
    }
  }

  return sql;
}

/* -------------------------------------------------------------------- */
/* The AST gate                                                          */
/* -------------------------------------------------------------------- */

interface AstNode {
  depth: number;
  type: string;
  name: string;
}

/**
 * `EXPLAIN AST` prints one node per line, nesting by leading spaces:
 *
 *     TablesInSelectQueryElement (children 1)
 *      TableExpression (children 1)
 *       Function url (children 1)
 *
 * A table function is a `Function` node whose PARENT is a `TableExpression`
 * (or, defensively, a `TablesInSelectQueryElement`). Scalar calls -- `equals`,
 * `in`, `count` -- never appear in that position, so this distinguishes
 * `FROM url(...)` from `WHERE x = url_column` without any SQL lexing.
 */
export function parseAstNodes(astText: string): AstNode[] {
  const nodes: AstNode[] = [];
  for (const line of astText.split("\n")) {
    if (line.trim() === "") continue;
    const depth = line.length - line.trimStart().length;
    const tokens = line.trim().split(/\s+/);
    nodes.push({ depth, type: tokens[0] ?? "", name: tokens[1] ?? "" });
  }
  return nodes;
}

const TABLE_FUNCTION_PARENTS = new Set(["TableExpression", "TablesInSelectQueryElement"]);

/**
 * Second gate, and the real one: every table function ClickHouse itself found
 * in the statement must be inert.
 *
 * `astText` is the output of `EXPLAIN AST <statement>` run through the same
 * read-only connection the statement will use. Because the check reads the
 * server's parse, a heredoc literal (`$x$...$x$`), a backtick-quoted function
 * name, whitespace tricks and unicode escapes are all already resolved --
 * there is nothing left to hide behind.
 */
export function assertInertTableFunctions(astText: string): void {
  const nodes = parseAstNodes(astText);
  const parents: AstNode[] = [];

  for (const node of nodes) {
    while (parents.length > 0 && parents[parents.length - 1]!.depth >= node.depth) {
      parents.pop();
    }
    const parent = parents[parents.length - 1];
    if (
      node.type === "Function" &&
      parent &&
      TABLE_FUNCTION_PARENTS.has(parent.type) &&
      !INERT_TABLE_FUNCTION_SET.has(node.name.toLowerCase())
    ) {
      throw new ReadOnlyQueryError(
        `The table function ${node.name}() is not allowed: read tables in this ClickHouse instance instead. Allowed table functions: ${INERT_TABLE_FUNCTIONS.join(", ")}.`,
      );
    }
    parents.push(node);
  }
}

/** True when `assertReadOnlyQuery` would accept the statement's shape. Says
 * nothing about its table functions -- only the AST gate can. */
export function isReadOnlyQuery(sql: string): boolean {
  try {
    assertReadOnlyQuery(sql);
    return true;
  } catch {
    return false;
  }
}
