import { describe, expect, it } from "vitest";
import { EMPTY_INFO_FILTERS, parseInfoFilters } from "~/lib/se-company-info-filters";
import {
  NO_COMPANIES_SELECTED,
  seCompanyRowSelection,
  selectionForSeCompanyFilters,
  updateSeCompanyRowSelection,
  type SeCompanySelection,
} from "~/lib/se-company-selection";

const A = "5565200028";
const B = "5560125220";
const C = "5567890123";
const filters = { ...EMPTY_INFO_FILTERS, status: "active", source: "esef" };
const all: SeCompanySelection = { mode: "query", query: filters, excludedCompanyIds: [] };

describe("SE company selection", () => {
  it("represents all matches with a query, independently of visited pages", () => {
    expect(seCompanyRowSelection(all, [A, B])).toEqual({ [A]: true, [B]: true });
    expect(seCompanyRowSelection(all, [C])).toEqual({ [C]: true });
    expect(JSON.parse(JSON.stringify(all))).toEqual({ mode: "query", query: filters, excludedCompanyIds: [] });
    expect(all).not.toHaveProperty("companyIds");
  });

  it("keeps explicit picks while selecting or clearing another page", () => {
    const selection: SeCompanySelection = { mode: "ids", companyIds: [C] };
    const picked = updateSeCompanyRowSelection(selection, [A, B], (rows) => ({ ...rows, [A]: true, [B]: true }));
    expect(picked).toEqual({ mode: "ids", companyIds: [C, A, B] });
    const cleared = updateSeCompanyRowSelection(picked, [A, B], (rows) => ({ ...rows, [A]: false, [B]: false }));
    expect(cleared).toEqual(selection);
  });

  it("tracks excluded rows across pages, then removes an exclusion on reselect", () => {
    const excludedA = updateSeCompanyRowSelection(all, [A, B], { [B]: true });
    expect(excludedA).toEqual({ ...all, excludedCompanyIds: [A] });
    expect(seCompanyRowSelection(excludedA, [A, B])).toEqual({ [B]: true });
    expect(seCompanyRowSelection(excludedA, [C])).toEqual({ [C]: true });
    const excludedAC = updateSeCompanyRowSelection(excludedA, [C], {});
    expect(excludedAC).toEqual({ ...all, excludedCompanyIds: [A, C] });
    const restoredA = updateSeCompanyRowSelection(excludedAC, [A, B], (rows) => ({ ...rows, [A]: true }));
    expect(restoredA).toEqual({ ...all, excludedCompanyIds: [C] });
    expect(all.excludedCompanyIds).toEqual([]);
  });

  it("unselects and reselects a whole page without dropping off-page exclusions", () => {
    const excludedC: SeCompanySelection = { ...all, excludedCompanyIds: [C] };
    const pageCleared = updateSeCompanyRowSelection(excludedC, [A, B], {});
    expect(pageCleared).toEqual({ ...all, excludedCompanyIds: [C, A, B] });
    expect(updateSeCompanyRowSelection(pageCleared, [A, B], { [A]: true, [B]: true })).toEqual(excludedC);
    expect(updateSeCompanyRowSelection(pageCleared, [], {})).toEqual(pageCleared);
  });

  it("preserves a query through paging, page-size and sort changes", () => {
    const initialFilters = parseInfoFilters(new URL("http://localhost/admin/se/companies?source=esef&status=active"));
    const selection: SeCompanySelection = { mode: "query", query: initialFilters, excludedCompanyIds: [A] };
    const nextFilters = parseInfoFilters(new URL("http://localhost/admin/se/companies?page=8&pageSize=100&sort=legal_name&dir=desc&status=active&source=esef"));
    expect(selectionForSeCompanyFilters(selection, nextFilters)).toBe(selection);
  });

  it("clears a query when any applied filter changes, but preserves explicit picks", () => {
    const changes = {
      companyId: A, name: "Alpha", status: "inactive", legalForm: "none",
      entity: "sole", description: "no", source: "wikidata", datatypes: {has_people: "has" as const},
    };
    for (const [key, value] of Object.entries(changes)) {
      expect(selectionForSeCompanyFilters(all, { ...filters, [key]: value })).toBe(NO_COMPANIES_SELECTED);
    }
    const explicit: SeCompanySelection = { mode: "ids", companyIds: [A, B] };
    expect(selectionForSeCompanyFilters(explicit, EMPTY_INFO_FILTERS)).toBe(explicit);
  });
});


it("clears all-matching selection when a field changes from Has to Missing", () => {
  const has = {...EMPTY_INFO_FILTERS, datatypes: {has_domains: "has" as const}};
  const selection: SeCompanySelection = {mode: "query", query: has, excludedCompanyIds: []};
  expect(selectionForSeCompanyFilters(selection, {...has, datatypes: {has_domains: "missing"}})).toBe(NO_COMPANIES_SELECTED);
});
