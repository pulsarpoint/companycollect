import { afterEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({
  createClient: vi.fn(),
  insert: vi.fn(),
  command: vi.fn(),
}));

vi.mock("@clickhouse/client", () => ({
  createClient: clickhouse.createClient,
}));

import {
  chInsertSeCompanyDomainRules,
  chInsertSeBasicInfoPrecedence,
  chInsertSeCompanyAddressRules,
} from "~/lib/clickhouse.server";

describe("correction and domain ClickHouse writers", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    clickhouse.createClient.mockReset();
    clickhouse.insert.mockReset();
    clickhouse.command.mockReset();
  });

  it("fails closed without ClickHouse credentials", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "");

    await expect(
      chInsertSeBasicInfoPrecedence([{ company_id: "test" }]),
    ).rejects.toThrow("CLICKHOUSE_USER and CLICKHOUSE_PASSWORD");
    expect(clickhouse.createClient).not.toHaveBeenCalled();
  });

  it("writes the review rule before refreshing the country-based filter", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
    clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert, command: clickhouse.command });
    clickhouse.insert.mockResolvedValue(undefined);

    const rows = [{ company_id: "5560593575", root_domain: "assaabloy.com" }];
    await chInsertSeCompanyDomainRules(rows);

    // The write client is a module-level singleton (see clickhouse.server.ts)
    // reused across every write helper, so the credential/settings shape is
    // only asserted once, here, against whichever test's write is first to
    // trigger client creation.
    expect(clickhouse.createClient).toHaveBeenCalledWith(
      expect.objectContaining({
        username: "correction_writer",
        password: "writer-secret",
      }),
    );
    expect(clickhouse.createClient).toHaveBeenCalledWith(
      expect.objectContaining({
        clickhouse_settings: expect.objectContaining({
          async_insert: 1,
          wait_for_async_insert: 1,
        }),
      }),
    );
    expect(clickhouse.insert).toHaveBeenCalledWith({
      table: "se_company_domain_rule",
      values: rows,
      format: "JSONEachRow",
    });
    expect(clickhouse.insert.mock.invocationCallOrder[0]).toBeLessThan(clickhouse.command.mock.invocationCallOrder[0]);
    expect(clickhouse.command).toHaveBeenNthCalledWith(1, {
      query: "SYSTEM REFRESH VIEW corpscout.domains_company_filter",
    });
  });

  it("surfaces a failed filter refresh without writing the source index", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
    clickhouse.insert.mockResolvedValue(undefined);
    clickhouse.command.mockRejectedValueOnce(new Error("filter refresh unavailable"));
    await expect(chInsertSeCompanyDomainRules([
      { company_id: "5560593575", root_domain: "assaabloy.com" },
    ])).rejects.toThrow("filter refresh unavailable");
    expect(clickhouse.command).toHaveBeenCalledTimes(1);
    expect(clickhouse.command.mock.calls[0][0].query).toBe("SYSTEM REFRESH VIEW corpscout.domains_company_filter");
  });

  // An empty batch is a normal caller state (nothing was decided), and an
  // INSERT with no rows would still open a connection and a part.
  it("no-ops on an empty address batch", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
    clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert, command: clickhouse.command });

    await chInsertSeCompanyAddressRules([]);

    expect(clickhouse.insert).not.toHaveBeenCalled();
    expect(clickhouse.createClient).not.toHaveBeenCalled();
  });
});
