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
  chCommand,
  chInsertCompanyDomains,
  chInsertSeCompanyAddressCorrections,
  chInsertSeCompanyInfoFieldValues,
  chInsertSeCompanyPersonCorrections,
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
      chInsertSeCompanyPersonCorrections([{ correction_id: "test" }]),
    ).rejects.toThrow("CLICKHOUSE_USER and CLICKHOUSE_PASSWORD");
    await expect(chCommand("SELECT 1")).rejects.toThrow(
      "CLICKHOUSE_USER and CLICKHOUSE_PASSWORD",
    );
    expect(clickhouse.createClient).not.toHaveBeenCalled();
  });

  it("writes domain reviews only to the unified company domains table", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
    clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert, command: clickhouse.command });
    clickhouse.insert.mockResolvedValue(undefined);

    const rows = [{ company_id: "5560593575", root_domain: "assaabloy.com" }];
    await chInsertCompanyDomains(rows);

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
      table: "company_domains",
      values: rows,
      format: "JSONEachRow",
    });
  });

  it("writes Sweden company-person corrections with the writer client", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
    clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert, command: clickhouse.command });
    clickhouse.insert.mockResolvedValue(undefined);

    await chInsertSeCompanyPersonCorrections([{ correction_id: "test" }]);

    expect(clickhouse.insert).toHaveBeenCalledWith({
      table: "se_company_person_correction",
      values: [{ correction_id: "test" }],
      format: "JSONEachRow",
    });
  });

  it("writes Sweden company-info field values with the writer client", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
    clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert, command: clickhouse.command });
    clickhouse.insert.mockResolvedValue(undefined);

    await chInsertSeCompanyInfoFieldValues([{ value_id: "test" }]);

    expect(clickhouse.insert).toHaveBeenCalledWith({
      table: "se_company_info_field_value",
      values: [{ value_id: "test" }],
      format: "JSONEachRow",
    });
  });

  it("writes Sweden company-address corrections with the writer client", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
    clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert, command: clickhouse.command });
    clickhouse.insert.mockResolvedValue(undefined);

    await chInsertSeCompanyAddressCorrections([{ correction_id: "test" }]);

    expect(clickhouse.insert).toHaveBeenCalledWith({
      table: "se_company_address_correction",
      values: [{ correction_id: "test" }],
      format: "JSONEachRow",
    });
  });

  // An empty batch is a normal caller state (nothing was decided), and an
  // INSERT with no rows would still open a connection and a part.
  it("no-ops on an empty address batch", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
    clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert, command: clickhouse.command });

    await chInsertSeCompanyAddressCorrections([]);

    expect(clickhouse.insert).not.toHaveBeenCalled();
    expect(clickhouse.createClient).not.toHaveBeenCalled();
  });

  // The registry's generated statements are INSERT ... SELECT: no rows to
  // hand to insert(), so they go through command(), on the SAME writer client
  // (the read client sends readonly=2 and would refuse them). Values are
  // bound as named parameters, never interpolated.
  it("runs a parameterised statement through the writer client", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
    clickhouse.createClient.mockReturnValue({
      insert: clickhouse.insert,
      command: clickhouse.command,
    });
    clickhouse.command.mockResolvedValue({ query_id: "q1" });

    await chCommand(
      "INSERT INTO corpscout.se_company_field SELECT {field:String}",
      { field: "description", company_ids: ["5565200028"] },
    );

    expect(clickhouse.command).toHaveBeenCalledWith({
      query: "INSERT INTO corpscout.se_company_field SELECT {field:String}",
      query_params: { field: "description", company_ids: ["5565200028"] },
    });
  });
});
