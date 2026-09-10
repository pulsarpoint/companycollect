import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import {
  buildSePeopleFilter,
  COMPANY_MATCH_LIMIT,
  listSePeoplePage,
  loadSePeopleCounts,
  resolveSePeopleCompanyIds,
  PEOPLE_COMPANY_NAMES_SQL,
  PEOPLE_COMPANY_SEARCH_SQL,
  PEOPLE_COUNTS_SQL,
  PEOPLE_LIST_SELECT_SQL,
} from "~/lib/se-people-list.server";

const EMPTY = { company: "", name: "", source: "", role: "", year: "", status: "" };
const ROW = {
  company_id: "5560125220", person_key: "a".repeat(64), display_name: "Anna Svensson",
  birth_year: "1975", wikidata_id: "", sources: ["bolagsverket"],
  current_roles: ["board_member"], role_years: [2025], first_year: "2024", last_year: "2025",
  active: 1, inactive_reason: "",
};

describe("se-people-list.server", () => {
  beforeEach(() => {
    clickhouse.query.mockReset().mockResolvedValue([]);
  });

  it("reads the main table through FINAL, sorted by company then name, paged by parameter", () => {
    expect(PEOPLE_LIST_SELECT_SQL).toContain("FROM corpscout.se_company_person_v2 AS p FINAL");
    expect(PEOPLE_LIST_SELECT_SQL).toContain("toString(p.person_key) AS person_key");
    expect(PEOPLE_LIST_SELECT_SQL).toContain("ifNull(toString(p.birth_year), '') AS birth_year");
    expect(PEOPLE_COUNTS_SQL).toContain("toString(countIf(p.active = 1)) AS active");
    expect(PEOPLE_COUNTS_SQL).toContain("toString(uniqExact(p.company_id)) AS companies");
    expect(PEOPLE_COMPANY_NAMES_SQL).toContain("FROM corpscout.se_companies_serving");
    expect(PEOPLE_COMPANY_NAMES_SQL).toContain("WHERE company_id IN {companyIds:Array(String)}");
    expect(PEOPLE_COMPANY_SEARCH_SQL).toContain("legal_name ILIKE {name:String}");
  });

  it("builds one predicate per filter, parameterized", async () => {
    expect(buildSePeopleFilter({ ...EMPTY, name: "svens" })).toEqual({
      where: ["p.display_name ILIKE {name:String}"], params: { name: "svens%" },
    });
    expect(buildSePeopleFilter({ ...EMPTY, source: "esef" })).toEqual({
      where: ["has(p.sources, {source:String})"], params: { source: "esef" },
    });
    expect(buildSePeopleFilter({ ...EMPTY, role: "board_member" })).toEqual({
      where: ["has(p.role_codes, {role:String})"], params: { role: "board_member" },
    });
    expect(buildSePeopleFilter({ ...EMPTY, year: "2025" })).toEqual({
      where: ["has(p.role_years, {year:UInt16})"], params: { year: 2025 },
    });
    expect(buildSePeopleFilter({ ...EMPTY, status: "active" })).toEqual({
      where: ["p.active = 1"], params: {},
    });
    expect(buildSePeopleFilter({ ...EMPTY, status: "hidden" })).toEqual({
      where: ["p.inactive_reason = 'hidden'"], params: {},
    });
    expect(buildSePeopleFilter({ ...EMPTY, status: "withdrawn" })).toEqual({
      where: ["p.inactive_reason = 'withdrawn'"], params: {},
    });
    // The company filter is ALWAYS an id set by the time it reaches the SQL: the
    // loader resolved it once, for the page and the counts alike.
    expect(buildSePeopleFilter(EMPTY, ["5560125220"])).toEqual({
      where: ["p.company_id IN {companyIds:Array(String)}"],
      params: { companyIds: ["5560125220"] },
    });
    expect(buildSePeopleFilter(EMPTY, null)).toEqual({ where: [], params: {} });
  });

  it("resolves a company id without a query and a company name through the view, capped", async () => {
    // All digits is the company itself: no lookup at all.
    expect(await resolveSePeopleCompanyIds("5560125220")).toEqual({
      companyIds: ["5560125220"], truncated: false,
    });
    expect(await resolveSePeopleCompanyIds("")).toEqual({ companyIds: null, truncated: false });
    expect(clickhouse.query).not.toHaveBeenCalled();

    clickhouse.query.mockResolvedValue([{ company_id: "5560125220" }]);
    expect(await resolveSePeopleCompanyIds("beijer")).toEqual({
      companyIds: ["5560125220"], truncated: false,
    });
    expect(clickhouse.query).toHaveBeenCalledWith(PEOPLE_COMPANY_SEARCH_SQL, {
      name: "beijer%", limit: COMPANY_MATCH_LIMIT,
    });

    clickhouse.query.mockResolvedValue(
      Array.from({ length: COMPANY_MATCH_LIMIT }, (_, index) => ({ company_id: String(index) })),
    );
    expect((await resolveSePeopleCompanyIds("a")).truncated).toBe(true);
  });

  it("pages the persons under the resolved ids and names every company of the page in one lookup", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (String(sql).includes("se_company_person_v2")) return [ROW];
      if (sql === PEOPLE_COMPANY_NAMES_SQL) return [{ company_id: "5560125220", legal_name: "Beijer" }];
      return [];
    });
    const page = await listSePeoplePage({ ...EMPTY, companyIds: ["5560125220"], page: 2, pageSize: 50 });
    expect(page.rows).toEqual([{ ...ROW, legal_name: "Beijer" }]);
    const listCall = clickhouse.query.mock.calls.find(([sql]) => String(sql).includes("se_company_person_v2"));
    expect(listCall?.[0]).toContain("WHERE p.company_id IN {companyIds:Array(String)}");
    expect(listCall?.[0]).toContain("ORDER BY p.company_id, p.display_name");
    expect(listCall?.[0]).toContain("LIMIT {limit:UInt32} OFFSET {offset:UInt32}");
    expect(listCall?.[1]).toMatchObject({ companyIds: ["5560125220"], limit: 50, offset: 50 });
    expect(clickhouse.query.mock.calls.find(([sql]) => sql === PEOPLE_COMPANY_NAMES_SQL)?.[1]).toEqual({
      companyIds: ["5560125220"],
    });
    // A company the serving view has no row for reads blank, never `undefined`.
    clickhouse.query.mockImplementation(async (sql: string) =>
      String(sql).includes("se_company_person_v2") ? [ROW] : [],
    );
    expect((await listSePeoplePage({ ...EMPTY, companyIds: null, page: 1, pageSize: 50 })).rows[0]?.legal_name).toBe("");
  });

  it("counts persons, active persons and companies under the SAME filter and ids as the page", async () => {
    clickhouse.query.mockResolvedValue([{ persons: "1126402", active: "1126402", companies: "578289" }]);
    expect(await loadSePeopleCounts({ ...EMPTY, source: "esef", companyIds: null })).toEqual({
      persons: 1126402, active: 1126402, companies: 578289,
    });
    const [sql, params] = clickhouse.query.mock.calls[0] ?? [];
    expect(String(sql)).toContain("WHERE has(p.sources, {source:String})");
    expect(params).toEqual({ source: "esef" });

    // The pager total is `persons`, so a company-name filter MUST reach the counts too
    // (pre-flight review 3.3): without the ids the strip would say 1,126,402 over a page
    // of one company and the pager would offer 22,528 empty pages.
    clickhouse.query.mockReset().mockResolvedValue([{ persons: "7", active: "6", companies: "1" }]);
    expect(await loadSePeopleCounts({ ...EMPTY, companyIds: ["5560125220"] })).toEqual({
      persons: 7, active: 6, companies: 1,
    });
    const [countsSql, countsParams] = clickhouse.query.mock.calls[0] ?? [];
    expect(String(countsSql)).toContain("WHERE p.company_id IN {companyIds:Array(String)}");
    expect(countsParams).toEqual({ companyIds: ["5560125220"] });
  });
});
