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
  parseInfoFilters,
  parseListView,
  type SeCompanyInfoTableFilters,
} from "~/lib/se-company-info-filters";
import { legalFormOptionLabel } from "~/lib/se-legal-form";

/** Every in-page link resolves against the route the table is rendered at. */
const PATH = "/admin/se/company-info";

const ROW: SeCompanyInfoListRow = {
  company_id: "5565200028",
  legal_name: "Alpha AB",
  status: "active",
  legal_form_code: "AB-ORGFO",
  legal_form_label_en: "Limited company (aktiebolag)",
  legal_form_label_sv: "Aktiebolag",
  entity_type: "legal",
  has_description: 1,
};

const COUNTS: SeCompanyInfoListCounts = {
  total: 1595,
  withDescription: 1540,
  withoutDescription: 55,
};

const OPTIONS: SeCompanyInfoFilterOptions = {
  statuses: ["active", "dissolved"],
  legalForms: [
    { code: "", label_sv: "", label_en: "" },
    { code: "AB-ORGFO", label_sv: "Aktiebolag", label_en: "Limited company (aktiebolag)" },
    // A code in use that the curated dictionary does not name: the option must
    // still be offered, by its bare code.
    { code: "ZZZ", label_sv: "", label_en: "" },
  ],
};

const APPLIED_FILTERS: SeCompanyInfoTableFilters = {
  ...EMPTY_INFO_FILTERS,
  companyId: "5565200028",
  name: "Alpha",
  status: "active",
  legalForm: "AB-ORGFO",
  entity: "legal",
  description: "yes",
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

  it("shows the row's legal name, status, legal form, entity and description yes/no", () => {
    const html = render();
    expect(html).toContain("Alpha AB");
    expect(html).toContain("active");
    // The legal form reads as its OFFICIAL Swedish name with the English gloss
    // muted beside it, the code as the cell's tooltip.
    expect(html).toContain('title="AB-ORGFO"');
    expect(html).toContain("Aktiebolag");
    expect(html).toContain(">Limited company (aktiebolag)<");
    expect(html).toContain(">Legal<");
    expect(html).toContain(">yes<");
  });

  it("shows a company with no description as \"no\"", () => {
    const html = render({ rows: [{ ...ROW, has_description: 0 }] });
    expect(html).toContain(">no<");
  });

  it("labels a 12-digit sole trader by its entity type", () => {
    const html = render({
      rows: [{ ...ROW, company_id: "196408233412", entity_type: "sole" }],
    });
    expect(html).toContain(">Sole trader<");
  });

  it("says nothing about the description's provenance -- that is the detail page's job", () => {
    // Task 17 (owner addendum): this list is a COMPANIES list. No source, no
    // language, no snippet, no suggestion/correction counts, no resolved stamp.
    const html = render();
    for (const gone of [
      ">Source<",
      ">Sources<",
      ">LLM<",
      ">Language<",
      ">Suggestion<",
      ">Corrections<",
      ">Resolved<",
    ]) {
      expect(html).not.toContain(gone);
    }
    expect(html).not.toContain("Alpha builds payment software.");
  });

  it("renders the counts strip from the SAME filtered counts, not recomputed", () => {
    const html = render();
    expect(html).toContain("Companies");
    expect(html).toContain("1,595");
    expect(html).toContain("With description");
    expect(html).toContain("1,540");
    expect(html).toContain("Without description");
    expect(html).toContain("55");
    // The model/review totals moved to the Pipeline page.
    expect(html).not.toContain("Multi-source");
    expect(html).not.toContain("Pending model");
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
      "entity_type",
      "has_description",
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
    expect(html).toContain(`href="${PATH}?sort=has_description&amp;dir=asc"`);
  });
});

describe("SeCompanyInfoTable filter sheet", () => {
  it("opens the filters from one button, badged with the number applied", () => {
    expect(render()).toContain("Filters");
    const html = render({ filters: APPLIED_FILTERS });
    // Six filters are set on APPLIED_FILTERS, and the badge says so.
    expect(infoFilterChips(APPLIED_FILTERS)).toHaveLength(6);
    expect(html).toContain(">6<");
  });

  it("summarises each applied filter as a chip whose X re-navigates without that param", () => {
    const html = render({ filters: APPLIED_FILTERS });
    expect(html).toContain("Status active");
    expect(html).toContain("Legal form AB-ORGFO");
    expect(html).toContain("Entity Legal (10-digit)");
    expect(html).toContain("Description yes");
    expect(html).toContain('aria-label="Remove filter Description yes"');

    // The chip's link is the same URL minus that one param -- with the sort and
    // page size kept, and `page` deliberately dropped.
    const withoutDescription = infoListSearch(
      APPLIED_FILTERS,
      { sort: "company_id", dir: "asc", pageSize: 50 },
      "description",
    );
    expect(withoutDescription).not.toContain("description=");
    expect(withoutDescription).toContain("companyId=5565200028");
    expect(withoutDescription).toContain("sort=company_id");
    expect(withoutDescription).toContain("pageSize=50");
    expect(withoutDescription).not.toContain("page=1");
    expect(html).toContain(`href="${PATH}${withoutDescription.replaceAll("&", "&amp;")}"`);
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
    for (const name of ["companyId", "name", "status", "legalForm", "entity", "description"]) {
      expect(html).toContain(`name="${name}"`);
    }
    for (const label of ["Company id", "Name", "Status", "Legal form", "Entity", "Description"]) {
      expect(html).toContain(label);
    }
    // Task 17: the description-provenance filters are gone from this page.
    for (const gone of ['name="source"', 'name="language"', 'name="suggestion"',
                        'name="multi"', 'name="corrected"']) {
      expect(html).not.toContain(gone);
    }
    // Not filters, but the form must carry them: applying a filter resets
    // `page` on purpose and must never reset the page size or the sort.
    expect(html).toContain('type="hidden" name="pageSize" value="100"');
    expect(html).toContain('type="hidden" name="sort" value="company_id"');
    expect(html).toContain('type="hidden" name="dir" value="asc"');
    // A Base UI select's trigger is a button whose only text is the current
    // value, so the visible <Label> above it names nothing to a screen reader.
    for (const label of ["Company id", "Name", "Status", "Legal form", "Entity", "Description"]) {
      expect(html).toContain(`aria-label="${label}"`);
    }
  });

  it("shows an empty data-driven value as the \"(none)\" option, which travels in the URL as \"none\"", () => {
    // A Base UI select renders its item list in a popup (a portal), so the
    // options themselves are not in the SSR markup -- what IS pinned here is
    // the mapping every data-driven option goes through, in both directions.
    // The label is parenthesised so an absent value never reads as a code the
    // register might actually use.
    expect(optionLabel("")).toBe("(none)");
    expect(optionValue("")).toBe("none");
    expect(optionLabel("AB")).toBe("AB");
    expect(optionValue("AB")).toBe("AB");
  });

  it("labels a legal-form option by both of its names, with the code last", () => {
    // A dropdown item has no tooltip, and two forms can read alike in one
    // language, so the code is what tells them apart -- always last, after
    // whichever names the curated dictionary has.
    expect(
      legalFormOptionLabel({
        code: "AB-ORGFO",
        label_sv: "Aktiebolag",
        label_en: "Limited company (aktiebolag)",
      }),
    ).toBe("Aktiebolag — Limited company (aktiebolag) (AB-ORGFO)");
    // A code the dictionary does not name is still selectable, by its code.
    expect(
      legalFormOptionLabel({ code: "ZZZ", label_sv: "", label_en: "" }),
    ).toBe("ZZZ");
    // ... and "no legal form code at all" keeps the shared "(none)" wording.
    expect(
      legalFormOptionLabel({ code: "", label_sv: "", label_en: "" }),
    ).toBe("(none)");
  });

  it("selects \"Any\" for an unset filter and the applied value otherwise", () => {
    expect(renderFields()).toContain('name="status" value="any"');
    const applied = renderFields(APPLIED_FILTERS);
    expect(applied).toContain('name="status" value="active"');
    expect(applied).toContain('name="legalForm" value="AB-ORGFO"');
    expect(applied).toContain('name="entity" value="legal"');
    expect(applied).toContain('name="description" value="yes"');
    expect(applied).toContain('name="companyId" value="5565200028"');
  });
});

describe("parseInfoFilters", () => {
  const at = (search: string) => new URL(`http://localhost/admin/se/company-info${search}`);

  it("reads every filter from the URL", () => {
    const filters = parseInfoFilters(
      at("?companyId=5565200028&name=Alpha&status=active&legalForm=AB-ORGFO&entity=legal&description=yes"),
    );
    expect(filters).toEqual(APPLIED_FILTERS);
    expect(infoFilterChips(filters)).toHaveLength(6);
  });

  it("drops a value the query builder would ignore, so no chip claims a filter the table does not have", () => {
    // Live before this fix: ?description=bogus showed a chip "Description
    // bogus" and a count of 1 over all 3.5M rows. A URL naming one of the
    // filters Task 17 removed is dropped the same way -- it is simply unknown.
    for (const search of [
      "?description=bogus",
      "?description=any",
      "?description=",
      "?entity=sideways",
      "?source=llm",
      "?language=en",
      "?suggestion=yes",
      "?multi=1",
      "?corrected=1",
    ]) {
      const filters = parseInfoFilters(at(search));
      expect(filters).toEqual(EMPTY_INFO_FILTERS);
      expect(infoFilterChips(filters)).toEqual([]);
    }
  });

  it("passes data-driven values through: their options come from the column, not from an enum", () => {
    expect(parseInfoFilters(at("?status=whatever")).status).toBe("whatever");
    expect(parseInfoFilters(at("?legalForm=none")).legalForm).toBe("none");
    // ...and the "none" sentinel reads as "(none)" in the chip.
    expect(infoFilterChips(parseInfoFilters(at("?legalForm=none")))).toEqual([
      { param: "legalForm", label: "Legal form (none)" },
    ]);
  });

  it("parses and clamps the view both list routes share, leaving sort/dir to the query whitelist", () => {
    expect(parseListView(at("?page=3&pageSize=500&sort=legal_name&dir=desc"))).toEqual({
      page: 3,
      pageSize: 200,
      sort: "legal_name",
      dir: "desc",
    });
    expect(parseListView(at(""))).toEqual({
      page: 1,
      pageSize: 50,
      sort: undefined,
      dir: undefined,
    });
    expect(parseListView(at("?page=0&pageSize=1")).page).toBe(1);
    expect(parseListView(at("?page=0&pageSize=1")).pageSize).toBe(10);
  });
});
