import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));

function installQueryResults() {
  clickhouse.query.mockImplementation(
    async (sql: string, params?: Record<string, unknown>): Promise<unknown[]> => {
      if (sql.includes("FROM nace_categories")) {
        return [{ normalized_code: "6201" }];
      }
      if (sql.startsWith("SELECT toString(count())")) {
        return [{ total: "1" }];
      }
      if (
        sql.includes("ORDER BY root_domain") &&
        sql.includes("domain_matches AS")
      ) {
        return [{ root_domain: "example.com" }];
      }
      if (sql.includes("coverage_by_crawl")) {
        return [
          {
            root_domain: "example.com",
            latest_crawl_id: "CC-MAIN-2026-30",
            latest_page_count: "18",
            crawl_count: "3",
            observed_at: "2026-07-25 12:00:00.000",
          },
        ];
      }
      if (sql.includes("feature_type = 'name'")) {
        return [
          { root_domain: "example.com", organization_name: "Example Company" },
        ];
      }
      if (sql.includes("AS address") && params?.address === "Stockholm") {
        return [
          {
            root_domain: "example.com",
            address: "Example Street 1, 111 22 Stockholm, SE",
          },
        ];
      }
      if (sql.includes("FROM commoncrawl_industries")) {
        return [
          {
            root_domain: "example.com",
            industry_code: "62.01",
            industry_label: "Computer programming activities",
          },
        ];
      }
      return [];
    },
  );
}

vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

const { searchCommonCrawlDomains, resolveCommonCrawlIndustryCodes } =
  await import("~/lib/common-crawl.server");

describe("Common Crawl server search", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    installQueryResults();
  });

  it("combines domain, address, and resolved industry candidates with bound parameters", async () => {
    const result = await searchCommonCrawlDomains(
      {
        domain: "example",
        address: "Stockholm",
        industry: "computer programming",
      },
      1,
      50,
    );

    expect(result).toEqual({
      total: 1,
      rows: [
        {
          rootDomain: "example.com",
          organizationName: "Example Company",
          address: "Example Street 1, 111 22 Stockholm, SE",
          industryCode: "62.01",
          industryLabel: "Computer programming activities",
          latestCrawlId: "CC-MAIN-2026-30",
          latestPageCount: 18,
          crawlCount: 3,
          observedAt: "2026-07-25 12:00:00.000",
        },
      ],
    });
    const calls = clickhouse.query.mock.calls as Array<
      [string, Record<string, unknown> | undefined]
    >;
    const candidateCall = calls.find(([sql]) => sql.includes("domain_matches AS"));
    expect(candidateCall?.[0]).toContain("address_matches AS");
    expect(candidateCall?.[0]).toContain("industry_matches AS");
    expect(candidateCall?.[0]).not.toContain("Stockholm");
    expect(candidateCall?.[0]).not.toContain("computer programming");
    expect(candidateCall?.[1]).toMatchObject({
      domain: "example",
      address: "Stockholm",
      industryCodes: ["6201"],
    });
  });

  it("resolves a NACE code prefix without interpolating it into SQL", async () => {
    await resolveCommonCrawlIndustryCodes("62.01");

    const [sql, params] = clickhouse.query.mock.calls[0] as [
      string,
      Record<string, unknown>,
    ];
    expect(sql).toContain("startsWith(normalized_code, {industryCode:String})");
    expect(sql).not.toContain("62.01");
    expect(params).toEqual({ industryCode: "6201" });
  });
});
