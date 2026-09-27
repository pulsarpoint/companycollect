import { describe, expect, it } from "vitest";
import { EMPTY_SE_DOMAINS_FILTERS } from "~/lib/se-domains-filters";
import { NO_DOMAINS_SELECTED, isDomainSelected, selectDomains, selectionForDomainFilters } from "~/lib/domain-selection";
import { type SeDomainSelection } from "~/lib/se-domain-selection";

describe("domain selections", () => {
  it("deduplicates shared domains, retains other pages, and deselects every occurrence", () => {
    const first = selectDomains(NO_DOMAINS_SELECTED, ["shared.se", "shared.se", "first.se"], true);
    expect(first).toEqual({ mode: "ids", domains: ["shared.se", "first.se"] });
    const second = selectDomains(first, ["next.se"], true);
    expect(selectDomains(second, ["shared.se"], false)).toEqual({ mode: "ids", domains: ["first.se", "next.se"] });
    expect(isDomainSelected(second, "shared.se")).toBe(true);
  });
  it("keeps query exclusions across pages and clears them when reticked", () => {
    const selection: SeDomainSelection = { mode: "query", query: EMPTY_SE_DOMAINS_FILTERS, excludedDomains: ["old.se"] };
    const changed = selectDomains(selection, ["shared.se", "new.se"], false);
    expect(changed).toMatchObject({ excludedDomains: ["old.se", "shared.se", "new.se"] });
    expect(isDomainSelected(changed, "shared.se")).toBe(false);
    expect(selectDomains(changed, ["shared.se"], true)).toMatchObject({ excludedDomains: ["old.se", "new.se"] });
  });
  it("resets a changed query without discarding explicit picks", () => {
    const query: SeDomainSelection = { mode: "query", query: EMPTY_SE_DOMAINS_FILTERS, excludedDomains: [] };
    expect(selectionForDomainFilters(query, { ...EMPTY_SE_DOMAINS_FILTERS, source: "brave" })).toEqual(NO_DOMAINS_SELECTED);
    expect(selectionForDomainFilters(query, EMPTY_SE_DOMAINS_FILTERS)).toBe(query);
    const ids: SeDomainSelection = { mode: "ids", domains: ["keep.se"] };
    expect(selectionForDomainFilters(ids, EMPTY_SE_DOMAINS_FILTERS)).toBe(ids);
  });
});
