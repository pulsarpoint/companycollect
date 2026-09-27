import { expect, it } from "vitest";
import { parseWorkspaceDomainSelection, workspaceDomainInputConfig } from "~/lib/workspace-domain-crawl.server";
import { parseWorkspaceDomainFilters, workspaceDomainsHref } from "~/lib/workspace-domains";
import { isDomainSelected, selectDomains, selectionForDomainFilters } from "~/lib/domain-selection";

const filters = parseWorkspaceDomainFilters(new URLSearchParams("suffix=.SE&companies=without&companyMatching=without&source=commoncrawl_graph"));
it("carries the entire filtered selection and exclusions without a page cursor or cap", () => {
  const selection = parseWorkspaceDomainSelection({ mode: "query", query: filters, excludedDomains: ["skip.se"] });
  const config = workspaceDomainInputConfig(selection);
  expect(config).toEqual({
    source_relation: "corpscout.domains_search", source_final: false, id_column: "root_domain", website_column: "root_domain",
    select_all: true, excluded_ids: ["skip.se"], workspace_domain_filters: {
      prefix: "", suffix: "se", sources: ["commoncrawl_graph"], source_match: "any",
      dns: "any", websites: "any", companies: "without", company_matching: "without",
    },
  });
  expect(workspaceDomainsHref(filters, "skip.se")).toContain("companyMatching=without&after=skip.se");
});
it.each([
  { ...filters, companyMatching: "bad" }, { ...filters, suffix: ".SE" }, { ...filters, companies: "bad" },
  { ...filters, sources: ["unknown"] }, { ...filters, sources: "commoncrawl" },
  { ...filters, page: "2" }, { suffix: "se" },
])("rejects invalid or incomplete bulk filters instead of broadening selection: %j", query => {
  expect(() => parseWorkspaceDomainSelection({ mode: "query", query, excludedDomains: [] })).toThrow();
});
it("validates explicit inventory domains and deduplicates them", () => {
  expect(workspaceDomainInputConfig(parseWorkspaceDomainSelection({ mode: "ids", domains: ["a.se", "a.se"] }))).toMatchObject({ ids: ["a.se"], source_relation: "corpscout.domains_search" });
  for (const domains of [[], ["a.se' OR 1=1"], ["https://a.se"], [""]]) {
    expect(() => parseWorkspaceDomainSelection({ mode: "ids", domains })).toThrow();
  }
});
it("retains all-matching exclusions across pages and resets when applied filters change", () => {
  const selection = parseWorkspaceDomainSelection({ mode: "query", query: filters, excludedDomains: [] });
  const next = selectDomains(selection, ["a.se", "b.se"], false);
  expect(isDomainSelected(next, "a.se")).toBe(false);
  expect(isDomainSelected(next, "on-another-page.se")).toBe(true);
  expect(selectionForDomainFilters(next, filters)).toBe(next);
  expect(selectionForDomainFilters(next, { ...filters, suffix: "no" })).toEqual({ mode: "ids", domains: [] });
});
