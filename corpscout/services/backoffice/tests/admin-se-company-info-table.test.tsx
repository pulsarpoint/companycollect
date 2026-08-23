import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { SeCompanyInfoTable } from "~/components/admin/se-company-info-table";
import { SeCompanyInfoFilterFields } from "~/components/admin/se-company-info-filter-sheet";
import type {
  SeCompanyInfoFilterOptions,
  SeCompanyInfoListCounts,
  SeCompanyInfoListRow,
} from "~/lib/se-company-info-lists.server";
import {
  EMPTY_INFO_FILTERS,
  infoFilterChips,
  infoListSearch,
  optionLabel,
  optionValue,
  type SeCompanyInfoTableFilters,
} from "~/lib/se-company-info-filters";

/** Every in-page link resolves against the route the table is rendered at. */
const PATH = "/admin/se/company-info";

const ROW: SeCompanyInfoListRow = {
  company_id: "5565200028",
  legal_name: "Alpha AB",
  status: "active",
  legal_form_code: "AB",
  description_source: "llm",
  description_sources: ["llm", "scb"],
  description_language: "en",
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

const OPTIONS: SeCompanyInfoFilterOptions = {
  statuses: ["active", "dissolved"],
  legalFormCodes: ["", "AB"],
  descriptionLanguages: ["en", "sv"],
};

const APPLIED_FILTERS: SeCompanyInfoTableFilters = {
  ...EMPTY_INFO_FILTERS,
  companyId: "5565200028",
  name: "Alpha",
  source: "llm",
  status: "active",
  legalForm: "AB",
  language: "en",
  suggestion: "yes",
  entity: "legal",
  multi: true,
  corrected: true,
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
            sort="company_id"
            dir="asc"
            counts={COUNTS}
            filters={EMPTY_INFO_FILTERS}
            options={OPTIONS}
            {...props}
          />
        ),
      },
    ],
    { initialEntries: ["/admin/se/company-info"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

/** The filter sheet's own fields, rendered without the sheet: a Base UI dialog
 * renders through a portal, which produces nothing at all under
 * renderToStaticMarkup. */
function renderFields(filters: SeCompanyInfoTableFilters = EMPTY_INFO_FILTERS) {
  return renderToStaticMarkup(
    <SeCompanyInfoFilterFields
      filters={filters}
      options={OPTIONS}
      view={{ sort: "company_id", dir: "asc", pageSize: 100 }}
    />,
  );
}

describe("SeCompanyInfoTable", () => {
  it("opens the company-info detail page from the company id AND from the whole row", () => {
    const html = render();
    expect(html).toContain('href="/admin/se/company/5565200028/info"');
    expect(html).toContain('data-href="/admin/se/company/5565200028/info"');
    expect(html).toContain('role="link"');
  });

  it("no longer carries a separate Review link or a company-page link (the detail page links there)", () => {
    const html = render();
    expect(html).not.toContain(">Review<");
    expect(html).not.toContain('href="/company/se/5565200028"');
  });

  it("shows the row's legal name, status, legal form, sources, language, snippet, suggestion and corrections count", () => {
    const html = render();
    expect(html).toContain("Alpha AB");
    expect(html).toContain("active");
    expect(html).toContain(">AB<");
    expect(html).toContain("llm");
    expect(html).toContain("llm, scb");
    expect(html).toContain("Alpha builds payment software.");
    // Scoped to the suggestion/corrections cells' own markup, not a bare
    // "yes"/"2" that could trivially match unrelated text elsewhere on the
    // page (a date, a class name, an "Any" option, ...).
    expect(html).toContain(">yes<");
    expect(html).toContain('<span class="tabular-nums">2</span>');
    expect(html).toContain("2026-08-22 09:00:00.000");
  });

  it("shows the no-suggestion row's badge as \"no\"", () => {
    const html = render({ rows: [{ ...ROW, has_suggestion: 0 }] });
    expect(html).toContain(">no<");
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

  it("renders an empty state when no rows match", () => {
    const html = render({ rows: [], total: 0 });
    expect(html).toContain("No companies match these filters.");
  });
});

describe("SeCompanyInfoTable sorting", () => {
  it("gives EVERY column a header that sorts by it, server-side, via ?sort=&dir=", () => {
    // Sorted by company_id ascending (the default), so that one header offers
    // the flip to descending and every other one offers its own first click.
    const html = render();
    for (const key of [
      "legal_name",
      "status",
      "legal_form_code",
      "description_source",
      "description_sources",
      "description_language",
      "description_snippet",
      "has_suggestion",
      "corrections_count",
      "resolved_at",
    ]) {
      expect(html).toContain(`href="${PATH}?sort=${key}&amp;dir=asc"`);
    }
    expect(html).toContain(`href="${PATH}?sort=company_id&amp;dir=desc"`);
  });

  it("marks the active column and flips its direction on the next click", () => {
    const html = render({ sort: "legal_name", dir: "asc" });
    expect(html).toContain('data-active="true"');
    expect(html).toContain(`href="${PATH}?sort=legal_name&amp;dir=desc"`);
    // Another column still offers its own first click, unaffected.
    expect(html).toContain(`href="${PATH}?sort=resolved_at&amp;dir=asc"`);
  });
});

describe("SeCompanyInfoTable filter sheet", () => {
  it("opens the filters from one button, badged with the number applied", () => {
    expect(render()).toContain("Filters");
    const html = render({ filters: APPLIED_FILTERS });
    // Ten filters are set on APPLIED_FILTERS, and the badge says so.
    expect(infoFilterChips(APPLIED_FILTERS)).toHaveLength(10);
    expect(html).toContain(">10<");
  });

  it("summarises each applied filter as a chip whose X re-navigates without that param", () => {
    const html = render({ filters: APPLIED_FILTERS });
    expect(html).toContain("Source llm");
    expect(html).toContain("Status active");
    expect(html).toContain("Legal form AB");
    expect(html).toContain("Language en");
    expect(html).toContain("Suggestion yes");
    expect(html).toContain("Multi-source");
    expect(html).toContain("Has corrections");
    expect(html).toContain('aria-label="Remove filter Source llm"');

    // The chip's link is the same URL minus that one param -- with the sort and
    // page size kept, and `page` deliberately dropped.
    const withoutSource = infoListSearch(
      APPLIED_FILTERS,
      { sort: "company_id", dir: "asc", pageSize: 50 },
      "source",
    );
    expect(withoutSource).not.toContain("source=");
    expect(withoutSource).toContain("companyId=5565200028");
    expect(withoutSource).toContain("sort=company_id");
    expect(withoutSource).toContain("pageSize=50");
    expect(withoutSource).not.toContain("page=1");
    expect(html).toContain(`href="${PATH}${withoutSource.replaceAll("&", "&amp;")}"`);
  });

  it("keeps sort and page size when every filter is cleared", () => {
    const cleared = infoListSearch(EMPTY_INFO_FILTERS, {
      sort: "legal_name",
      dir: "desc",
      pageSize: 100,
    });
    expect(cleared).toBe("?sort=legal_name&dir=desc&pageSize=100");
    const html = render({ filters: APPLIED_FILTERS, sort: "legal_name", dir: "desc", pageSize: 100 });
    expect(html).toContain(`href="${PATH}${cleared.replaceAll("&", "&amp;")}"`);
    expect(html).toContain("Clear all");
  });

  it("shows no chips and no count when nothing is filtered", () => {
    expect(infoFilterChips(EMPTY_INFO_FILTERS)).toEqual([]);
    expect(render()).not.toContain("Clear all");
  });
});

describe("SeCompanyInfoFilterFields", () => {
  it("offers a field for every filter, including one select per discrete column", () => {
    const html = renderFields();
    for (const name of [
      "companyId",
      "name",
      "source",
      "status",
      "legalForm",
      "language",
      "suggestion",
      "entity",
      "multi",
      "corrected",
    ]) {
      expect(html).toContain(`name="${name}"`);
    }
    for (const label of [
      "Company id",
      "Name",
      "Description source",
      "Status",
      "Legal form",
      "Description language",
      "Has suggestion",
      "Entity",
    ]) {
      expect(html).toContain(label);
    }
    // Not filters, but the form must carry them: applying a filter resets
    // `page` on purpose and must never reset the page size or the sort.
    expect(html).toContain('type="hidden" name="pageSize" value="100"');
    expect(html).toContain('type="hidden" name="sort" value="company_id"');
    expect(html).toContain('type="hidden" name="dir" value="asc"');
  });

  it("shows an empty data-driven value as the \"none\" option, which is what travels in the URL", () => {
    // A Base UI select renders its item list in a popup (a portal), so the
    // options themselves are not in the SSR markup -- what IS pinned here is
    // the mapping every data-driven option goes through, in both directions.
    expect(optionLabel("")).toBe("none");
    expect(optionValue("")).toBe("none");
    expect(optionLabel("AB")).toBe("AB");
    expect(optionValue("AB")).toBe("AB");
  });

  it("selects \"Any\" for an unset filter and the applied value otherwise", () => {
    expect(renderFields()).toContain('name="status" value="any"');
    const applied = renderFields(APPLIED_FILTERS);
    expect(applied).toContain('name="status" value="active"');
    expect(applied).toContain('name="legalForm" value="AB"');
    expect(applied).toContain('name="language" value="en"');
    expect(applied).toContain('name="suggestion" value="yes"');
    expect(applied).toContain('name="companyId" value="5565200028"');
    expect(applied).toContain('checked=""');
  });
});
