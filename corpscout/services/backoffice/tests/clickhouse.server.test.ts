import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";

// Integration test: runs against the real ClickHouse instance from .env.
describe("chQuery", () => {
  it("runs a parameterized SELECT and returns typed rows", async () => {
    const rows = await chQuery<{ answer: number }>(
      "SELECT {a:UInt8} + {b:UInt8} AS answer",
      { a: 40, b: 2 },
    );
    expect(rows).toEqual([{ answer: 42 }]);
  });

  it("reads from the corpscout database by default", async () => {
    const rows = await chQuery<{ db: string }>("SELECT currentDatabase() AS db");
    expect(rows).toEqual([{ db: "corpscout" }]);
  });
});
