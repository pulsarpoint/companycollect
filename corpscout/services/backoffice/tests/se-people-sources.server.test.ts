import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

const tasks = vi.hoisted(() => ({ load: vi.fn() }));
vi.mock("~/lib/se-people-tasks.server", () => ({ loadSePeopleTasks: tasks.load }));

import { PAGE_LIMIT_OFFSET_SQL } from "~/lib/se-company-info-lists.server";
import {
  BOLAGSVERKET_SELECT_SQL,
  BOLAGSVERKET_TABLE,
  countSePeopleBolagsverketRows,
  countSePeopleEsefRows,
  countSePeopleFinalRows,
  countSePeopleWikidataRows,
  ESEF_SELECT_SQL,
  ESEF_TABLE,
  FINAL_PEOPLE_TABLE,
  FINAL_SELECT_SQL,
  listSePeopleBolagsverketPage,
  listSePeopleEsefPage,
  listSePeopleFinalPage,
  listSePeopleWikidataPage,
  loadSePeopleSourcePage,
  WIKIDATA_SELECT_SQL,
  WIKIDATA_TABLE,
} from "~/lib/se-people-sources.server";

const NO_FILTERS = { companyId: "", name: "" };

describe("the three source views: read directly, no FINAL, no JOIN", () => {
  it("selects each view's own columns, aliased to the row's own names", () => {
    expect(BOLAGSVERKET_SELECT_SQL).toContain(`FROM ${BOLAGSVERKET_TABLE}`);
    expect(BOLAGSVERKET_SELECT_SQL).toContain("company_id AS company_id");
    expect(BOLAGSVERKET_SELECT_SQL).toContain("full_name AS full_name");
    expect(BOLAGSVERKET_SELECT_SQL).toContain("role_original AS role_original");
    expect(BOLAGSVERKET_SELECT_SQL).toContain("signatory_kind AS signatory_kind");

    expect(ESEF_SELECT_SQL).toContain(`FROM ${ESEF_TABLE}`);
    expect(ESEF_SELECT_SQL).toContain("role_category AS role_category");
    expect(ESEF_SELECT_SQL).toContain("effective_from AS effective_from");
    expect(ESEF_SELECT_SQL).toContain("confidence AS confidence");

    expect(WIKIDATA_SELECT_SQL).toContain(`FROM ${WIKIDATA_TABLE}`);
    expect(WIKIDATA_SELECT_SQL).toContain("person_wikidata_id AS person_wikidata_id");
    expect(WIKIDATA_SELECT_SQL).toContain("birth_year AS birth_year");
  });

  it("never FINALs or JOINs the three source views -- each view already resolved that internally", () => {
    for (const sql of [BOLAGSVERKET_SELECT_SQL, ESEF_SELECT_SQL, WIKIDATA_SELECT_SQL]) {
      expect(sql).not.toContain("FINAL");
      expect(sql).not.toContain("JOIN");
    }
  });
});

describe("the resolved se_company_person table: read FINAL", () => {
  it("selects the display columns plus provenance, aliased to the row's own names", () => {
    expect(FINAL_SELECT_SQL).toContain(`FROM ${FINAL_PEOPLE_TABLE} FINAL`);
    expect(FINAL_SELECT_SQL).toContain("company_id AS company_id");
    expect(FINAL_SELECT_SQL).toContain("toString(person_id) AS person_id");
    expect(FINAL_SELECT_SQL).toContain("name AS name");
    expect(FINAL_SELECT_SQL).toContain("description AS description");
    expect(FINAL_SELECT_SQL).toContain("toString(model_provider) AS model_provider");
    expect(FINAL_SELECT_SQL).toContain("model_name AS model_name");
    expect(FINAL_SELECT_SQL).toContain("toString(updated_at) AS updated_at");
  });

  it("is a live ReplacingMergeTree, unlike the three read-only source views -- FINAL is required here", () => {
    expect(FINAL_SELECT_SQL).toContain("FINAL");
  });
});

describe.each([
  ["listSePeopleBolagsverketPage", listSePeopleBolagsverketPage, BOLAGSVERKET_SELECT_SQL, "full_name"],
  ["listSePeopleEsefPage", listSePeopleEsefPage, ESEF_SELECT_SQL, "full_name"],
  ["listSePeopleWikidataPage", listSePeopleWikidataPage, WIKIDATA_SELECT_SQL, "full_name"],
  ["listSePeopleFinalPage", listSePeopleFinalPage, FINAL_SELECT_SQL, "name"],
] as const)("%s", (_label, listPage, selectSql, nameColumn) => {
  beforeEach(() => clickhouse.query.mockReset());

  it("with no filters: no WHERE clause, orders by company_id, pages with named LIMIT/OFFSET params", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    const rows = await listPage(NO_FILTERS, 1, 50);

    expect(rows).toEqual([]);
    expect(clickhouse.query).toHaveBeenCalledTimes(1);
    const [sql, params] = clickhouse.query.mock.calls[0];
    expect(sql).toContain(selectSql);
    expect(sql).not.toContain("WHERE");
    expect(sql).toContain("ORDER BY company_id ASC");
    expect(sql).toContain(PAGE_LIMIT_OFFSET_SQL);
    expect(params).toEqual({ limit: 50, offset: 0 });
  });

  it("filters on an exact company_id and an ILIKE contains on the tab's own name column", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    await listPage({ companyId: "5560125220", name: "Ada" }, 1, 50);

    const [sql, params] = clickhouse.query.mock.calls[0];
    expect(sql).toContain("WHERE company_id = {companyId:String}");
    expect(sql).toContain(`AND ${nameColumn} ILIKE {name:String}`);
    expect(params).toMatchObject({ companyId: "5560125220", name: "%Ada%" });
  });

  it("clamps pageSize to [10, 200] and computes offset from page", async () => {
    clickhouse.query.mockResolvedValue([]);
    await listPage(NO_FILTERS, 3, 5);
    expect(clickhouse.query.mock.calls[0][1]).toEqual({ limit: 10, offset: 20 });

    clickhouse.query.mockClear();
    clickhouse.query.mockResolvedValue([]);
    await listPage(NO_FILTERS, 2, 500);
    expect(clickhouse.query.mock.calls[0][1]).toEqual({ limit: 200, offset: 200 });
  });
});

describe.each([
  ["countSePeopleBolagsverketRows", countSePeopleBolagsverketRows, BOLAGSVERKET_TABLE],
  ["countSePeopleEsefRows", countSePeopleEsefRows, ESEF_TABLE],
  ["countSePeopleWikidataRows", countSePeopleWikidataRows, WIKIDATA_TABLE],
  ["countSePeopleFinalRows", countSePeopleFinalRows, `${FINAL_PEOPLE_TABLE} FINAL`],
] as const)("%s", (_label, countRows, fromSql) => {
  beforeEach(() => clickhouse.query.mockReset());

  it("counts with the exact same table/FINAL the row page reads", async () => {
    clickhouse.query.mockResolvedValueOnce([{ total: "42" }]);
    const total = await countRows(NO_FILTERS);

    expect(total).toBe(42);
    const [sql] = clickhouse.query.mock.calls[0];
    expect(sql).toContain(`FROM ${fromSql}`);
    expect(sql).not.toContain("WHERE");
  });

  it("returns zero (never throws) on an empty result", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    expect(await countRows(NO_FILTERS)).toBe(0);
  });

  it("threads the same company_id filter the row page uses", async () => {
    clickhouse.query.mockResolvedValueOnce([{ total: "1" }]);
    await countRows({ companyId: "5560125220", name: "" });
    const [sql, params] = clickhouse.query.mock.calls[0];
    expect(sql).toContain("WHERE company_id = {companyId:String}");
    expect(params).toEqual({ companyId: "5560125220" });
  });
});

describe("loadSePeopleSourcePage", () => {
  beforeEach(() => clickhouse.query.mockReset());

  it("runs exactly two queries (page + count) against ONE table per tab -- never all four", async () => {
    for (const tab of ["bolagsverket", "esef", "wikidata", "final"] as const) {
      clickhouse.query.mockReset();
      clickhouse.query.mockResolvedValue([]);
      const result = await loadSePeopleSourcePage(tab, NO_FILTERS, 1, 50);
      expect(result).toEqual({ tab, rows: [], total: 0 });
      expect(clickhouse.query).toHaveBeenCalledTimes(2);
    }
  });

  it("tags the result with the tab it queried", async () => {
    clickhouse.query.mockResolvedValueOnce([{ company_id: "5560125220" }]);
    clickhouse.query.mockResolvedValueOnce([{ total: "1" }]);
    const result = await loadSePeopleSourcePage("esef", NO_FILTERS, 1, 50);
    expect(result.tab).toBe("esef");
    expect(result.total).toBe(1);
  });

  it("dispatches the tasks tab to loadSePeopleTasks -- no ClickHouse table read at all", async () => {
    tasks.load.mockResolvedValueOnce({
      rows: [{ key: "clean-copy" } as never],
      error: "",
    });
    const result = await loadSePeopleSourcePage("tasks", NO_FILTERS, 1, 50);
    expect(result).toEqual({
      tab: "tasks",
      rows: [{ key: "clean-copy" }],
      total: 1,
      error: "",
    });
    expect(clickhouse.query).not.toHaveBeenCalled();
  });
});
