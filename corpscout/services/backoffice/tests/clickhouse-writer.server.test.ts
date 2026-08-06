import { afterEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({
  createClient: vi.fn(),
  insert: vi.fn(),
}));

vi.mock("@clickhouse/client", () => ({
  createClient: clickhouse.createClient,
}));

import { chInsertPersonCorrections } from "~/lib/clickhouse.server";

describe("person correction ClickHouse writer", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    clickhouse.createClient.mockReset();
    clickhouse.insert.mockReset();
  });

  it("fails closed without dedicated writer credentials", async () => {
    vi.stubEnv("CLICKHOUSE_WRITE_USER", "");
    vi.stubEnv("CLICKHOUSE_WRITE_PASSWORD", "");

    await expect(
      chInsertPersonCorrections([{ correction_id: "test" }]),
    ).rejects.toThrow("dedicated ClickHouse writer credentials");
    expect(clickhouse.createClient).not.toHaveBeenCalled();
  });

  it("uses dedicated credentials and the correction ledger only", async () => {
    vi.stubEnv("CLICKHOUSE_WRITE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_WRITE_PASSWORD", "writer-secret");
    clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert });
    clickhouse.insert.mockResolvedValue(undefined);

    const rows = [{ correction_id: "test" }];
    await chInsertPersonCorrections(rows);

    expect(clickhouse.createClient).toHaveBeenCalledWith(
      expect.objectContaining({
        username: "correction_writer",
        password: "writer-secret",
      }),
    );
    expect(clickhouse.insert).toHaveBeenCalledWith({
      table: "country_person_correction",
      values: rows,
      format: "JSONEachRow",
    });
  });
});
