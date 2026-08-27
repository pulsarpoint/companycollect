import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { SePeopleSourcesTable } from "~/components/admin/se-people-sources-table";
import type { SePeopleSourcePage } from "~/lib/se-people-sources.server";
import type { SePeopleSourceFilters, SePeopleSourceView } from "~/lib/se-people-sources";

const FILTERS: SePeopleSourceFilters = { companyId: "", name: "" };
const VIEW: SePeopleSourceView = { page: 1, pageSize: 50 };

function render(page: SePeopleSourcePage, filters: SePeopleSourceFilters = FILTERS) {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: <SePeopleSourcesTable page={page} filters={filters} view={VIEW} />,
      },
    ],
    { initialEntries: ["/admin/se/people"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("tab navigation", () => {
  it("links every tab, with the default tab's link carrying no ?tab= param", () => {
    const html = render({ tab: "bolagsverket", rows: [], total: 0 });

    expect(html).toContain(">Bolagsverket<");
    expect(html).toContain(">ESEF<");
    expect(html).toContain(">Wikidata<");
    expect(html).toContain(">People (final)<");
    expect(html).toContain('href="/admin/se/people?tab=esef"');
    expect(html).toContain('href="/admin/se/people?tab=wikidata"');
    expect(html).toContain('href="/admin/se/people?tab=final"');
    // Switching back to the default tab must not carry a stale ?tab=.
    expect(html).not.toContain("tab=bolagsverket");
  });
});

describe("bolagsverket tab", () => {
  it("renders each column and links company_id to the company page", () => {
    const html = render({
      tab: "bolagsverket",
      rows: [
        {
          company_id: "5560125220",
          full_name: "Ada Lovelace",
          first_name: "Ada",
          last_name: "Lovelace",
          role_original: "Styrelseledamot",
          role_kind: "board_member",
          signatory_kind: "ordinary",
          fiscal_year: 2025,
        },
      ],
      total: 1,
    });

    expect(html).toContain('href="/company/SE/5560125220"');
    expect(html).toContain("Ada Lovelace");
    expect(html).toContain("Styrelseledamot");
    expect(html).toContain("2025");
  });
});

describe("final (People) tab", () => {
  it("links the whole row to the person review page, and shows model provenance", () => {
    const html = render({
      tab: "final",
      rows: [
        {
          company_id: "5560125220",
          person_id: "43234b7d-0184-16b5-de47-dc086a2b0ed9",
          name: "Ada Lovelace",
          description: "Board member",
          model_provider: "anthropic",
          model_name: "claude",
          updated_at: "2026-08-22 11:00:00.000",
        },
      ],
      total: 1,
    });

    const personHref =
      "/admin/se/people/person/5560125220/43234b7d-0184-16b5-de47-dc086a2b0ed9";
    expect(html).toContain(`data-href="${personHref}"`);
    expect(html).toContain(`href="${personHref}"`);
    expect(html).toContain("anthropic / claude");
  });

  it("shows an explanatory empty state instead of an error when nothing is resolved yet", () => {
    const html = render({ tab: "final", rows: [], total: 0 });
    expect(html).toContain("clean-copy step");
  });
});

describe("filter form", () => {
  it("carries the active tab and page size as hidden fields, and the applied filters as field values", () => {
    const html = render(
      { tab: "esef", rows: [], total: 0 },
      { companyId: "5560125220", name: "Ada" },
    );

    expect(html).toContain('name="tab" value="esef"');
    expect(html).toContain('name="pageSize" value="50"');
    expect(html).toContain('value="5560125220"');
    expect(html).toContain('value="Ada"');
    expect(html).toContain(">Clear<");
  });

  it("hides Clear when no filter is applied", () => {
    const html = render({ tab: "esef", rows: [], total: 0 });
    expect(html).not.toContain(">Clear<");
  });
});
