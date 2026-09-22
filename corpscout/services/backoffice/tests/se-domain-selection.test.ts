import { describe, expect, it } from "vitest";
import { EMPTY_SE_DOMAINS_FILTERS } from "~/lib/se-domains-filters";
import { NO_DOMAINS_SELECTED, isSeDomainSelected, selectSeDomains, selectionForSeDomainFilters, type SeDomainSelection } from "~/lib/se-domain-selection";

describe("domain selections", () => {
  it("deduplicates shared domains, retains other pages, and deselects every occurrence", () => {
    const first = selectSeDomains(NO_DOMAINS_SELECTED, ["shared.se", "shared.se", "first.se"], true);
    expect(first).toEqual({ mode: "ids", domains: ["shared.se", "first.se"] });
    const second = selectSeDomains(first, ["next.se"], true);
    expect(selectSeDomains(second, ["shared.se"], false)).toEqual({ mode: "ids", domains: ["first.se", "next.se"] });
    expect(isSeDomainSelected(second, "shared.se")).toBe(true);
  });
  it("keeps query exclusions across pages and clears them when reticked", () => {
    const selection: SeDomainSelection = { mode: "query", query: EMPTY_SE_DOMAINS_FILTERS, excludedDomains: ["old.se"] };
    const changed = selectSeDomains(selection, ["shared.se", "new.se"], false);
    expect(changed).toMatchObject({ excludedDomains: ["old.se", "shared.se", "new.se"] });
    expect(isSeDomainSelected(changed, "shared.se")).toBe(false);
    expect(selectSeDomains(changed, ["shared.se"], true)).toMatchObject({ excludedDomains: ["old.se", "new.se"] });
  });
  it("resets a changed query without discarding explicit picks", () => {
    const query: SeDomainSelection = { mode: "query", query: EMPTY_SE_DOMAINS_FILTERS, excludedDomains: [] };
    expect(selectionForSeDomainFilters(query, { ...EMPTY_SE_DOMAINS_FILTERS, source: "brave" })).toEqual(NO_DOMAINS_SELECTED);
    expect(selectionForSeDomainFilters(query, EMPTY_SE_DOMAINS_FILTERS)).toBe(query);
    const ids: SeDomainSelection = { mode: "ids", domains: ["keep.se"] };
    expect(selectionForSeDomainFilters(ids, EMPTY_SE_DOMAINS_FILTERS)).toBe(ids);
  });
});
