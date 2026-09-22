import { beforeEach, describe, expect, it, vi } from "vitest";

const dagster = vi.hoisted(() => ({ launchRun: vi.fn(), dagsterRunUrl: vi.fn(() => "http://dagster/runs/input") }));
vi.mock("~/lib/dagster.server", () => dagster);
// Any regression to direct writes must fail this test boundary.
vi.mock("~/lib/clickhouse.server", () => { throw new Error("Input selection belongs to Dagster"); });
import { saveSeDomainCrawlInputs } from "~/lib/se-domain-crawl.server";
import { EMPTY_SE_DOMAINS_FILTERS } from "~/lib/se-domains-filters";

describe("SE domain crawl input saving", () => {
  beforeEach(() => {
    dagster.launchRun.mockReset().mockResolvedValue({runId: "input", status: "QUEUED"});
  });

  it.each([["full", "website_full_crawl"], ["jobs", "website_jobs_crawl"], ["site_info", "website_site_info"]])("launches only the %s input asset", async (type, prefix) => {
    const result = await saveSeDomainCrawlInputs({ mode: "ids", domains: ["shared.se", "other.se", "shared.se"] }, type);
    expect(dagster.launchRun).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({
      job: `${prefix}_input_job`,
      runConfig: {ops: {[`${prefix}_requests`]: {config: {
        source_relation: "corpscout.se_company_domain", source_final: true,
        id_column: "root_domain", website_column: "root_domain", ids: ["shared.se", "other.se"],
      }}}},
    }), {timeoutMs: 15_000});
    expect(result).toMatchObject({ok: true, runId: "input", runUrl: "http://dagster/runs/input", table: `corpscout.${prefix}_requests`});
    expect(result).not.toHaveProperty("insertedDomains");
  });

  it("passes all applied filters and exclusions without expanding the selection in Backoffice", async () => {
    await saveSeDomainCrawlInputs({ mode: "query", query: {
      domain: "exa%' or 1=1 --", company: "5560049529", source: "brave", association: "connected",
      status: "inactive", minConfidence: "0.4", maxConfidence: "0.9", shared: "1",
    }, excludedDomains: ["skip.se", "skip.se"] }, "jobs");
    const config = dagster.launchRun.mock.calls[0][0].runConfig.ops.website_jobs_crawl_requests.config;
    expect(config).toMatchObject({select_all: true, excluded_ids: ["skip.se"], se_domain_filters: {
      domain: "exa%' or 1=1 --", company: "5560049529", source: "brave", association: "connected",
      status: "inactive", min_confidence: .4, max_confidence: .9, shared: true,
    }});
    expect(config).not.toHaveProperty("max_domains");
  });

  it("supports an explicitly selected complete unfiltered inventory", async () => {
    await saveSeDomainCrawlInputs({ mode: "query", query: EMPTY_SE_DOMAINS_FILTERS, excludedDomains: [] }, "full");
    expect(dagster.launchRun.mock.calls[0][0].runConfig.ops.website_full_crawl_requests.config.select_all).toBe(true);
  });

  it.each([
    null, { mode: "ids", domains: [] }, { mode: "ids", domains: ["https://example.se"] },
    { mode: "query", query: {}, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, source: "unknown" }, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, status: "typo" }, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, page: "2" }, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, minConfidence: "0.9", maxConfidence: "0.5" }, excludedDomains: [] },
    { mode: "query", query: EMPTY_SE_DOMAINS_FILTERS, excludedDomains: ["x'); DROP TABLE x"] },
  ])("rejects malformed selections without widening them: %j", async (selection) => {
    await expect(saveSeDomainCrawlInputs(selection, "full")).rejects.toThrow();
    expect(dagster.launchRun).not.toHaveBeenCalled();
  });

  it("rejects unknown crawl types before launching", async () => {
    await expect(saveSeDomainCrawlInputs({ mode: "ids", domains: ["valid.se"] }, "other")).rejects.toThrow();
    expect(dagster.launchRun).not.toHaveBeenCalled();
  });

  it("propagates a failed Dagster launch without claiming success", async () => {
    dagster.launchRun.mockRejectedValue(new Error("Dagster unavailable"));
    await expect(saveSeDomainCrawlInputs({ mode: "ids", domains: ["valid.se"] }, "full")).rejects.toThrow("Dagster unavailable");
  });
});
