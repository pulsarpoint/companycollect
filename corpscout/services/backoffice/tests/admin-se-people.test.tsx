import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

const server = vi.hoisted(() => ({
  listSePeoplePage: vi.fn(),
  loadSePeopleCounts: vi.fn(),
  resolveSePeopleCompanyIds: vi.fn(),
}));
vi.mock("~/lib/se-people-list.server", () => server);

import { loader } from "~/routes/admin-se-people";
import { SePeopleTable } from "~/components/admin/se-people-table";
import { parseSePeopleFilters, sePeopleHref, sePersonHref } from "~/lib/se-people-filters";

const KEY = "a".repeat(64);
const ROW = {
  company_id: "5560125220", legal_name: "Beijer Byggmaterial Aktiebolag", person_key: KEY,
  display_name: "Anna Svensson", birth_year: "1975", wikidata_id: "Q7",
  sources: ["bolagsverket", "esef"], current_roles: ["board_member"], role_years: [2024, 2025],
  first_year: "2024", last_year: "2025", active: 1, inactive_reason: "",
};
const COUNTS = { persons: 2, active: 1, companies: 1 };

function render(element: React.ReactElement, search = ""): string {
  const router = createMemoryRouter([{ path: "/admin/se/people", element }], {
    initialEntries: [`/admin/se/people${search}`],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("se people filters", () => {
  it("reads and rebuilds the six filters, dropping anything the catalogue does not know", () => {
    const params = new URLSearchParams("company=beijer&name=svens&source=esef&role=board_member&year=2025&status=hidden");
    expect(parseSePeopleFilters(params)).toEqual({
      company: "beijer", name: "svens", source: "esef", role: "board_member", year: "2025", status: "hidden",
    });
    expect(parseSePeopleFilters(new URLSearchParams("source=scb&status=gone&year=20xx"))).toEqual({
      company: "", name: "", source: "", role: "", year: "", status: "",
    });
    expect(sePeopleHref({ company: "", name: "svens", source: "", role: "", year: "", status: "" }, 2, 50))
      .toBe("/admin/se/people?name=svens&page=2");
    expect(sePersonHref("5560125220", KEY)).toBe(`/admin/se/company/5560125220/people?person=${KEY}`);
  });
});

describe("admin-se-people route", () => {
  beforeEach(() => {
    server.listSePeoplePage.mockReset().mockResolvedValue({ rows: [ROW] });
    server.loadSePeopleCounts.mockReset().mockResolvedValue(COUNTS);
    server.resolveSePeopleCompanyIds.mockReset().mockResolvedValue({ companyIds: null, truncated: false });
  });

  it("pages and counts under the same filters", async () => {
    const data = await loader({
      request: new Request("http://x/admin/se/people?source=esef&page=3&pageSize=50"),
    } as never);
    expect(data).toEqual({
      rows: [ROW], counts: COUNTS, truncatedCompanies: false, page: 3, pageSize: 50,
      filters: { company: "", name: "", source: "esef", role: "", year: "", status: "" },
    });
    expect(server.listSePeoplePage).toHaveBeenCalledWith(
      expect.objectContaining({ source: "esef", companyIds: null, page: 3, pageSize: 50 }),
    );
    expect(server.loadSePeopleCounts).toHaveBeenCalledWith(
      expect.objectContaining({ source: "esef", companyIds: null }),
    );
  });

  it("resolves a company-name filter once and hands the same ids to the page and the counts", async () => {
    server.resolveSePeopleCompanyIds.mockResolvedValue({ companyIds: ["5560125220"], truncated: true });
    const data = await loader({
      request: new Request("http://x/admin/se/people?company=beijer"),
    } as never);
    expect(server.resolveSePeopleCompanyIds).toHaveBeenCalledTimes(1);
    expect(server.resolveSePeopleCompanyIds).toHaveBeenCalledWith("beijer");
    expect(server.listSePeoplePage).toHaveBeenCalledWith(
      expect.objectContaining({ companyIds: ["5560125220"] }),
    );
    expect(server.loadSePeopleCounts).toHaveBeenCalledWith(
      expect.objectContaining({ companyIds: ["5560125220"] }),
    );
    expect(data.truncatedCompanies).toBe(true);
  });

  it("renders the counts strip, the row and its link into the company tab", () => {
    const html = render(
      <SePeopleTable
        rows={[ROW]}
        counts={COUNTS}
        truncatedCompanies={false}
        page={1}
        pageSize={50}
        filters={{ company: "", name: "", source: "", role: "", year: "", status: "" }}
      />,
    );
    expect(html).toContain("Anna Svensson");
    expect(html).toContain("Beijer Byggmaterial Aktiebolag");
    expect(html).toContain("Bolagsverket");
    expect(html).toContain("ESEF");
    expect(html).toContain(`/admin/se/company/5560125220/people?person=${KEY}`);
    // The strip's three numbers.
    expect(html).toContain("Persons");
    expect(html).toContain("Active");
    expect(html).toContain("Companies");
  });

  it("says when a company-name filter matched more companies than it could carry", () => {
    const html = render(
      <SePeopleTable
        rows={[ROW]}
        counts={COUNTS}
        truncatedCompanies
        page={1}
        pageSize={50}
        filters={{ company: "aktiebolag", name: "", source: "", role: "", year: "", status: "" }}
      />,
    );
    expect(html).toContain("first 200 matching companies");
  });
});
