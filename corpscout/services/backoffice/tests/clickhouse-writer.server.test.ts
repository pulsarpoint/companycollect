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
  chInsertPersonCorrections,
  chInsertSeCompanyInfoCorrections,
  chInsertSeCompanyPersonCorrections,
} from "~/lib/clickhouse.server";

describe("person correction ClickHouse writer", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    clickhouse.createClient.mockReset();
    clickhouse.insert.mockReset();
  });

  it("fails closed without ClickHouse credentials", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "");

    await expect(
      chInsertPersonCorrections([{ correction_id: "test" }]),
    ).rejects.toThrow("CLICKHOUSE_USER and CLICKHOUSE_PASSWORD");
    expect(clickhouse.createClient).not.toHaveBeenCalled();
  });

  it("uses the shared credentials and the correction ledger only", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
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
    expect(clickhouse.createClient).toHaveBeenCalledWith(
      expect.objectContaining({
        clickhouse_settings: expect.objectContaining({
          async_insert: 1,
          wait_for_async_insert: 1,
        }),
      }),
    );
    expect(clickhouse.insert).toHaveBeenCalledWith({
      table: "country_person_correction",
      values: rows,
      format: "JSONEachRow",
    });
  });

  it("writes domain reviews only to the unified company domains table", async () => {
    vi.stubEnv("CLICKHOUSE_USER", "correction_writer");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "writer-secret");
    clickhouse.createClient.mockReturnValue({ insert: clickhouse.insert });
    clickhouse.insert.mockResolvedValue(undefined);

    const rows = [{ company_id: "5560593575", root_domain: "assaabloy.com" }];
    await chInsertCompanyDomains(rows);

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
});
