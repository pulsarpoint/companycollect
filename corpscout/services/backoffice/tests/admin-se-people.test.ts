/**
 * The /admin/se/people route's loader: reads the tab/filters/paging out of
 * the URL and hands them to se-people-sources.server's dispatcher unchanged.
 * The dispatcher itself (which query runs, how WHERE/paging are built) is
 * covered by se-people-sources.server.test.ts; this only pins that the
 * loader wires the URL to that call correctly.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";

const loadSePeopleSourcePage = vi.fn(async () => ({
  tab: "bolagsverket" as const,
  rows: [],
  total: 0,
}));

vi.mock("~/lib/se-people-sources.server", () => ({ loadSePeopleSourcePage }));

const { loader } = await import("~/routes/admin-se-people");

function get(search: string) {
  return loader({
    request: new Request(`http://backoffice/admin/se/people${search}`),
  } as unknown as Parameters<typeof loader>[0]);
}

describe("admin-se-people loader", () => {
  beforeEach(() => loadSePeopleSourcePage.mockClear());

  it("defaults to the bolagsverket tab, page 1, the shared default page size, and no filters", async () => {
    await get("");
    expect(loadSePeopleSourcePage).toHaveBeenCalledWith(
      "bolagsverket",
      { companyId: "", name: "" },
      1,
      50,
    );
  });

  it("threads tab, filters, and paging straight from the URL", async () => {
    await get("?tab=esef&companyId=5560125220&name=Ada&page=2&pageSize=100");
    expect(loadSePeopleSourcePage).toHaveBeenCalledWith(
      "esef",
      { companyId: "5560125220", name: "Ada" },
      2,
      100,
    );
  });

  it("returns the dispatcher's result alongside the parsed filters/view", async () => {
    const result = await get("?tab=final");
    expect(result).toEqual({
      page: { tab: "bolagsverket", rows: [], total: 0 },
      filters: { companyId: "", name: "" },
      view: { page: 1, pageSize: 50 },
    });
  });
});
