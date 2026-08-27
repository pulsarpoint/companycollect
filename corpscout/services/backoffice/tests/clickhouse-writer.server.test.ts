import { afterEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({
  createClient: vi.fn(),
  insert: vi.fn(),
}));

vi.mock("@clickhouse/client", () => ({
  createClient: clickhouse.createClient,
}));

import {
  chInsertCompanyDomains,
  chInsertSeCompanyAddressCorrections,
  chInsertSeCompanyInfoCorrections,
  chInsertSeCompanyPersonCorrections,
} from "~/lib/clickhouse.server";

describe("correction and domain ClickHouse writers", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    clickhouse.createClient.mockReset();
    clickhouse.insert.mockReset();
  });

  it("fails closed without ClickHouse credentials", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "");

    await expect(
      chInsertSeCompanyPersonCorrections([{ correction_id: "test" }]),
    ).rejects.toThrow("CLICKHOUSE_USER and CLICKHOUSE_PASSWORD");
    expect(clickhouse.createClient).not.toHaveBeenCalled();
  });

  it("writes domain reviews only to the unified company domains table", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
    clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert });
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
    clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert });
    clickhouse.insert.mockResolvedValue(undefined);

    await chInsertSeCompanyPersonCorrections([{ correction_id: "test" }]);

    expect(clickhouse.insert).toHaveBeenCalledWith({
      table: "se_company_person_correction",
      values: [{ correction_id: "test" }],
      format: "JSONEachRow",
    });
  });

  it("writes Sweden company-info corrections with the writer client", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
    clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert });
    clickhouse.insert.mockResolvedValue(undefined);

    await chInsertSeCompanyInfoCorrections([{ correction_id: "test" }]);

    expect(clickhouse.insert).toHaveBeenCalledWith({
      table: "se_company_info_correction",
      values: [{ correction_id: "test" }],
      format: "JSONEachRow",
    });
  });

  it("writes Sweden company-address corrections with the writer client", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
    clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert });
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
    clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert });

    await chInsertSeCompanyAddressCorrections([]);

    expect(clickhouse.insert).not.toHaveBeenCalled();
    expect(clickhouse.createClient).not.toHaveBeenCalled();
  });
});
