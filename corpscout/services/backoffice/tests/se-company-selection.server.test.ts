import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ stream: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chStreamQuery: clickhouse.stream }));

import { EMPTY_INFO_FILTERS, PROFILE_DATATYPES, parseInfoFilters } from "~/lib/se-company-info-filters";
import { buildInfoListFilter, SE_COMPANIES_SERVING_TABLE } from "~/lib/se-company-info-lists.server";
import { parseSeCompanySelection, resolveSeCompanySelection } from "~/lib/se-company-selection.server";

const A = "5565200028";
const B = "198012345678";
const all = { mode: "query", query: EMPTY_INFO_FILTERS, excludedCompanyIds: [] };

beforeEach(() => {
  clickhouse.stream.mockReset().mockImplementation(async function* () {});
});

describe("materializing a company selection", () => {
  it("returns distinct explicit IDs without a query, preserving leading zeroes", async () => {
    expect(await resolveSeCompanySelection({ mode: "ids", companyIds: [A, B, A, "0123456789"] })).toEqual([A, B, "0123456789"]);
    expect(await resolveSeCompanySelection({ mode: "ids", companyIds: [] })).toEqual([]);
    expect(clickhouse.stream).not.toHaveBeenCalled();
  });

  it("materializes more than a page of results, without LIMIT, OFFSET or a row cap", async () => {
    const ids = Array.from({ length: 1205 }, (_, index) => String(1000000000 + index));
    clickhouse.stream.mockImplementation(async function* () {
      for (const company_id of ids) yield { company_id };
    });
    expect(await resolveSeCompanySelection(all)).toEqual(ids);
    const [sql, params] = clickhouse.stream.mock.calls[0];
    expect(sql).toContain(`FROM ${SE_COMPANIES_SERVING_TABLE} AS i`);
    expect(sql).not.toMatch(/\b(LIMIT|OFFSET|WHERE)\b/);
    expect(params).toEqual({});
  });

  it("applies all current list filters and exclusions as SQL parameters", async () => {
    const query = {
      companyId: A, name: "Alpha' OR 1=1 --", status: "none", legalForm: "49",
      entity: "legal", description: "no", source: "esef", datatypes: PROFILE_DATATYPES.map((datatype) => datatype.key),
    };
    await resolveSeCompanySelection({ ...all, query, excludedCompanyIds: [B, B] });
    const [sql, params] = clickhouse.stream.mock.calls[0];
    const expected = buildInfoListFilter(query);
    for (const predicate of expected.where) expect(sql).toContain(predicate);
    expect(sql).toContain("i.company_id NOT IN {excludedCompanyIds:Array(String)}");
    expect(sql).not.toContain(query.name);
    expect(sql).not.toContain(B);
    expect(sql).not.toMatch(/\b(LIMIT|OFFSET)\b/);
    expect(params).toEqual({ ...expected.params, excludedCompanyIds: [B] });
  });

  it("accepts the loader's normalized filters and repeated datatype parameters", async () => {
    const query = parseInfoFilters(new URL("http://localhost/admin/se/companies?name=Alpha&status=any&legalForm=none&entity=sole&description=yes&source=wikidata&datatype=has_people&datatype=has_address&datatype=has_people&page=9&pageSize=50"));
    await resolveSeCompanySelection({ ...all, query });
    const [sql, params] = clickhouse.stream.mock.calls[0];
    expect(sql).toContain("length(i.company_id) = 12");
    expect(sql).toContain("i.has_people = 1");
    expect(sql).toContain("i.has_address = 1");
    expect(params).toEqual({ name: "%Alpha%", legalForm: "" });
  });

  it("returns an empty list for no matches and resolves against current data each time", async () => {
    expect(await resolveSeCompanySelection(all)).toEqual([]);
    clickhouse.stream.mockImplementation(async function* () { yield { company_id: A }; });
    expect(await resolveSeCompanySelection(all)).toEqual([A]);
    expect(clickhouse.stream).toHaveBeenCalledTimes(2);
  });

  it("propagates a streaming failure rather than returning a partial action target", async () => {
    clickhouse.stream.mockImplementation(async function* () {
      yield { company_id: A };
      throw new Error("ClickHouse unavailable");
    });
    await expect(resolveSeCompanySelection(all)).rejects.toThrow("ClickHouse unavailable");
  });
});

describe("bulk selection validation", () => {
  it.each([
    null, {}, { mode: "all" }, { mode: "ids" },
    { mode: "ids", companyIds: [123] }, { mode: "ids", companyIds: ["1"] },
    { mode: "ids", companyIds: [A], query: {} },
    { mode: "query", excludedCompanyIds: [] },
    { mode: "query", query: null, excludedCompanyIds: [] },
    { mode: "query", query: {}, excludedCompanyIds: [] },
    { ...all, excludedCompanyIds: ["bad"] }, { ...all, companyIds: [A] },
    { ...all, query: { ...EMPTY_INFO_FILTERS, page: 2 } },
    { ...all, query: { ...EMPTY_INFO_FILTERS, sql: "1=1" } },
    { ...all, query: { ...EMPTY_INFO_FILTERS, source: "unknown" } },
    { ...all, query: { ...EMPTY_INFO_FILTERS, entity: "unknown" } },
    { ...all, query: { ...EMPTY_INFO_FILTERS, description: "unknown" } },
    { ...all, query: { ...EMPTY_INFO_FILTERS, status: "any" } },
    { ...all, query: { ...EMPTY_INFO_FILTERS, name: "  Alpha  " } },
    { ...all, query: { ...EMPTY_INFO_FILTERS, datatypes: "has_people" } },
    { ...all, query: { ...EMPTY_INFO_FILTERS, datatypes: ["unknown"] } },
  ])("rejects malformed selections without querying: %j", async (selection) => {
    await expect(resolveSeCompanySelection(selection)).rejects.toThrow();
    expect(clickhouse.stream).not.toHaveBeenCalled();
  });

  it("requires every filter field so an incomplete request cannot broaden the action", () => {
    for (const key of Object.keys(EMPTY_INFO_FILTERS)) {
      const query: Record<string, unknown> = { ...EMPTY_INFO_FILTERS };
      delete query[key];
      expect(() => parseSeCompanySelection({ ...all, query })).toThrow();
    }
  });
});
