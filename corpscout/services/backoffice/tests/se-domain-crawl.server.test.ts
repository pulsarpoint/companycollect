import { describe, expect, it, vi } from "vitest";

// Selection parsing never touches ClickHouse or Dagster; any regression must fail here.
vi.mock("~/lib/clickhouse.server", () => { throw new Error("Selection parsing belongs to Dagster"); });
vi.mock("~/lib/dagster.server", () => { throw new Error("Selection parsing launches nothing"); });
import { inputConfig, parseSeDomainSelection } from "~/lib/se-domain-crawl.server";
import { EMPTY_SE_DOMAINS_FILTERS } from "~/lib/se-domains-filters";

describe("SE domain selection parsing", () => {
  it("deduplicates explicit domains and maps them to the input asset's ids", () => {
    const selection = parseSeDomainSelection({ mode: "ids", domains: ["shared.se", "other.se", "shared.se"] });
    expect(selection).toEqual({ mode: "ids", domains: ["shared.se", "other.se"] });
    expect(inputConfig(selection)).toEqual({
      source_relation: "corpscout.se_company_domain", source_final: true,
      id_column: "root_domain", website_column: "root_domain", ids: ["shared.se", "other.se"],
    });
  });

  it("passes all applied filters and exclusions without expanding the selection", () => {
    const selection = parseSeDomainSelection({ mode: "query", query: {
      domain: "exa%' or 1=1 --", company: "5560049529", source: "brave", association: "connected",
      status: "inactive", minConfidence: "0.4", maxConfidence: "0.9", shared: "1",
    }, excludedDomains: ["skip.se", "skip.se"] });
    const config = inputConfig(selection);
    expect(config).toMatchObject({select_all: true, excluded_ids: ["skip.se"], se_domain_filters: {
      domain: "exa%' or 1=1 --", company: "5560049529", source: "brave", association: "connected",
      status: "inactive", min_confidence: .4, max_confidence: .9, shared: true,
    }});
    expect(config).not.toHaveProperty("ids");
    expect(config).not.toHaveProperty("max_domains");
  });

  it("supports an explicitly selected complete unfiltered inventory", () => {
    const config = inputConfig(parseSeDomainSelection({ mode: "query", query: EMPTY_SE_DOMAINS_FILTERS, excludedDomains: [] }));
    expect(config.select_all).toBe(true);
    expect(config.se_domain_filters).toMatchObject({shared: false});
  });

  it.each([
    null, { mode: "ids", domains: [] }, { mode: "ids", domains: ["https://example.se"] },
    { mode: "query", query: {}, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, source: "unknown" }, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, status: "typo" }, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, page: "2" }, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, minConfidence: "0.9", maxConfidence: "0.5" }, excludedDomains: [] },
    { mode: "query", query: EMPTY_SE_DOMAINS_FILTERS, excludedDomains: ["x'); DROP TABLE x"] },
  ])("rejects malformed selections without widening them: %j", (selection) => {
    expect(() => parseSeDomainSelection(selection)).toThrow();
  });

  it("no longer exports the retired Send-for-crawl launchers", async () => {
    const module = await import("~/lib/se-domain-crawl.server");
    expect(module).not.toHaveProperty("launchSeDomainCrawlWorkflow");
    expect(module).not.toHaveProperty("saveSeDomainCrawlInputs");
  });
});
