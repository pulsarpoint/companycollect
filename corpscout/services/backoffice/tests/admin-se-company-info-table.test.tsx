import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { SeCompanyInfoTable } from "~/components/admin/se-company-info-table";
import type {
  SeCompanyInfoListCounts,
  SeCompanyInfoListRow,
} from "~/lib/se-company-info-lists.server";

const ROW: SeCompanyInfoListRow = {
  company_id: "5565200028",
  legal_name: "Alpha AB",
  status: "active",
  description_source: "llm",
  description_sources: ["llm", "scb"],
  description_snippet: "Alpha builds payment software.",
  has_suggestion: 1,
  corrections_count: 2,
  resolved_at: "2026-08-22 09:00:00.000",
};

const COUNTS: SeCompanyInfoListCounts = {
  bySource: [
    { source: "scb", count: 1200 },
    { source: "llm", count: 340 },
    { source: "", count: 55 },
  ],
  multiSourceCount: 87,
  pendingModelCount: 12,
};

const EMPTY_FILTERS = {
  companyId: "",
  name: "",
  source: "",
  multi: false,
  entity: "",
  corrected: false,
};

function render(props: Partial<Parameters<typeof SeCompanyInfoTable>[0]> = {}) {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <SeCompanyInfoTable
            rows={[ROW]}
            total={3500000}
            page={1}
            pageSize={50}
            counts={COUNTS}
            filters={EMPTY_FILTERS}
            {...props}
          />
        ),
      },
    ],
    { initialEntries: ["/admin/se/company-info"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("SeCompanyInfoTable", () => {
  it("links the company id to the company page and to the review page", () => {
    const html = render();
    expect(html).toContain('href="/company/se/5565200028"');
    expect(html).toContain('href="/admin/se/company/5565200028/info"');
  });

  it("shows the row's legal name, status, sources, snippet, suggestion and corrections count", () => {
    const html = render();
    expect(html).toContain("Alpha AB");
    expect(html).toContain("active");
    expect(html).toContain("llm");
    expect(html).toContain("llm, scb");
    expect(html).toContain("Alpha builds payment software.");
    expect(html).toContain("yes");
    expect(html).toContain("2");
    expect(html).toContain("2026-08-22 09:00:00.000");
  });

  it("shows the no-suggestion row as \"no\"", () => {
    const html = render({ rows: [{ ...ROW, has_suggestion: 0 }] });
    expect(html).toContain("no");
  });

  it("renders the counts strip from the SAME filtered counts, not recomputed", () => {
    const html = render();
    expect(html).toContain("scb");
    expect(html).toContain("1,200");
    expect(html).toContain("340");
    expect(html).toContain("87");
    expect(html).toContain("12");
  });

  it("shows the pager total and page", () => {
    const html = render();
    expect(html).toContain("3,500,000");
    expect(html).toContain("Page 1");
  });

  it("carries the applied filters as the form's default values", () => {
    const html = render({
      filters: {
        companyId: "5565200028",
        name: "Alpha",
        source: "llm",
        multi: true,
        entity: "legal",
        corrected: true,
      },
    });
    expect(html).toContain('name="companyId" value="5565200028"');
    expect(html).toContain('name="name" value="Alpha"');
    expect(html).toContain('name="source" value="llm"');
    expect(html).toContain('name="entity" value="legal"');
    expect(html).toContain('name="multi"');
    expect(html).toContain('name="corrected"');
    expect(html).toContain('checked=""');
  });

  it("renders an empty state when no rows match", () => {
    const html = render({ rows: [], total: 0 });
    expect(html).toContain("No companies match these filters.");
  });
});
