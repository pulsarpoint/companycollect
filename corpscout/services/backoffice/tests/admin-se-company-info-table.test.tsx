import type { ReactElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import {
  functionalUpdate,
  type CellContext,
  type ColumnDef,
  type HeaderContext,
  type Table,
} from "@tanstack/react-table";
import { DataTable } from "~/components/data-table/data-table";
import {
  SeCompanyInfoTable,
  SelectionIndicator,
  selectionColumn,
} from "~/components/admin/se-company-info-table";
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
  PROFILE_DATATYPES,
  PROFILE_SOURCES,
  PROFILE_SOURCES_LEGEND,
  profileSourceLabel,
  profileSourceParts,
  type SeCompanyInfoTableFilters,
} from "~/lib/se-company-info-filters";
import {
  NO_ROWS_SELECTED,
  selectedRowCount,
  selectedRowIds,
  type RowSelection,
} from "~/lib/row-selection";
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
  has_address: 1,
  has_financial: 1,
  has_people: 0,
  has_domains: 1,
  profile_sources: "BSEW",
};

/** Two more companies, so a page can be part-selected and a selection can
 * name a company that is NOT on the page being rendered. */
const ROW_B: SeCompanyInfoListRow = {
  ...ROW,
  company_id: "5560125220",
  legal_name: "Beta AB",
};

const ROW_C: SeCompanyInfoListRow = {
  ...ROW,
  company_id: "5567890123",
  legal_name: "Gamma AB",
};

/** The header checkbox names itself; the row ones name their company AND its
 * id -- legal names repeat across the register, ids do not. */
const SELECT_PAGE = "Select every company on this page";

function selectLabel(row: SeCompanyInfoListRow): string {
  return `Select ${row.legal_name} (${row.company_id})`;
}

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
  source: "esef",
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
            selection={NO_ROWS_SELECTED}
            onSelectionChange={() => {}}
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

  it("shows which sources built the profile, one monospace letter each, under a legend", () => {
    // Owner ruling: S is unconditional (SCB is the register base); B, E and W
    // are earned by ANY datatype. This fixture company has all four.
    const html = render();
    expect(html).toContain(PROFILE_SOURCES_LEGEND);
    expect(html).toContain(
      "Sources: B = Bolagsverket · S = SCB · E = ESEF · W = Wikidata",
    );
    for (const [letter, label] of [
      ["B", "Bolagsverket"],
      ["S", "SCB"],
      ["E", "ESEF"],
      ["W", "Wikidata"],
    ]) {
      expect(html).toContain(`title="${label}"`);
      expect(html).toContain(`>${letter}</span>`);
    }
    // Monospace, so 'BS' and 'BSEW' line up down the column.
    expect(html).toContain("font-mono");
  });

  it("shows only the letters a company has -- the BS majority stays two letters", () => {
    const html = render({ rows: [{ ...ROW, profile_sources: "BS" }] });
    expect(html).toContain('title="Bolagsverket"');
    expect(html).toContain('title="SCB"');
    expect(html).not.toContain('title="ESEF"');
    expect(html).not.toContain('title="Wikidata"');
    // The legend is the table's, not the row's: it stands whatever the rows say.
    expect(html).toContain(PROFILE_SOURCES_LEGEND);
  });

  it("ticks a datatype the company has and em-dashes one it does not, per column", () => {
    // Four one-glyph cells in a row: each carries its own column name, so a
    // check that moved to the wrong column fails here rather than reading as
    // "some datatype is present".
    const html = render();
    expect(html).toContain('data-presence="has_address" data-present="yes"');
    expect(html).toContain('data-presence="has_financial" data-present="yes"');
    expect(html).toContain('data-presence="has_people" data-present="no"');
    expect(html).toContain('data-presence="has_domains" data-present="yes"');
    expect(html).toContain('title="People: no"');
    expect(html).toContain('title="Address: yes"');
    // The absent one says so with the SHARED em dash, not a blank cell.
    expect(html).toContain("—");
    // ...and every datatype of the catalog has a header of its own (the sort
    // icon follows the label, so the "<" closes the sort chevron's <svg>).
    for (const datatype of PROFILE_DATATYPES) {
      expect(html).toContain(`>${datatype.label}<svg`);
    }
  });

  it("inverts every tick for a company that has nothing but its register row", () => {
    const html = render({
      rows: [
        {
          ...ROW,
          has_address: 0,
          has_financial: 0,
          has_people: 1,
          has_domains: 0,
          profile_sources: "S",
        },
      ],
    });
    expect(html).toContain('data-presence="has_address" data-present="no"');
    expect(html).toContain('data-presence="has_financial" data-present="no"');
    expect(html).toContain('data-presence="has_people" data-present="yes"');
    expect(html).toContain('data-presence="has_domains" data-present="no"');
  });

  it("says nothing about the description's provenance -- that is the detail page's job", () => {
    // Task 17 (owner addendum): this list is a COMPANIES list. The Sources
    // column says which REGISTERS built the profile; it says nothing about
    // which of them the published TEXT came from, whether a model wrote it, or
    // what language it is in -- that whole story stays on the detail page.
    const html = render();
    for (const gone of [
      ">LLM<",
      ">Language<",
      // Singular, and it is NOT the "Sources" column: a description-provenance
      // column called "Source" (which is what this page used to have) would
      // match this and nothing else does -- the header below renders as
      // ">Sources</a>", and the filter sheet's own "Source" field label is not
      // in this markup at all (Base UI renders the sheet through a portal).
      ">Source<",
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
    // The model/review totals moved to the Pipeline sheet.
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

type Row = SeCompanyInfoListRow;

/** The state of the checkbox that names itself `label`: "true", "false", or
 * the "mixed" a Base UI checkbox reports while it is indeterminate. A row's
 * label carries its company id in brackets, so the label is escaped rather
 * than spliced into the pattern raw. */
function checkboxState(html: string, label: string): string | undefined {
  const escaped = label.replaceAll(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return html.match(
    new RegExp(`<span[^>]*aria-checked="([^"]*)"[^>]*aria-label="${escaped}"`),
  )?.[1];
}

type CheckboxProps = {
  checked: boolean;
  indeterminate?: boolean;
  onCheckedChange: (checked: boolean) => void;
};

type SelectCellProps = {
  onClick: (event: { stopPropagation: () => void }) => void;
  onKeyDown: (event: { stopPropagation: () => void }) => void;
  children: ReactElement<CheckboxProps>;
};

/**
 * The page's selection wiring, rendered for real: the REAL select column
 * inside the REAL `DataTable`, plus a spy column that hands the TanStack
 * instance back.
 *
 * These tests render to a string -- there is no DOM here to click -- so the
 * checkboxes' handlers are invoked directly. They are invoked through the real
 * contexts of a real table, though, so what a tick does to the selection is
 * real: which keys survive a select-all is TanStack's answer, not a fake's,
 * and a `DataTable` that stopped passing `onRowSelectionChange` on would leave
 * `selectionNow()` unchanged and fail these tests.
 */
function mountSelectable(rows: Row[], initial: RowSelection) {
  let selection = initial;
  const captured: Table<Row>[] = [];
  const columns: ColumnDef<Row, unknown>[] = [
    selectionColumn(),
    {
      id: "capture",
      header: (context) => {
        captured.push(context.table);
        return null;
      },
      cell: () => null,
    },
  ];
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <DataTable
            columns={columns}
            data={rows}
            rowHref={(row) => `/admin/se/company/${row.company_id}/info`}
            selection={{
              state: selection,
              onChange: (updater) => {
                selection = functionalUpdate(updater, selection);
              },
              getRowId: (row) => row.company_id,
            }}
          />
        ),
      },
    ],
    { initialEntries: [PATH] },
  );
  const html = renderToStaticMarkup(<RouterProvider router={router} />);
  const table = captured[0];
  if (!table) throw new Error("the spy column never rendered: no table to drive");
  return { html, table, selectionNow: () => selection };
}

/** The header checkbox's own props, from the real header context. */
function headerCheckbox(table: Table<Row>): CheckboxProps {
  const header = table
    .getHeaderGroups()[0]
    .headers.find((candidate) => candidate.column.id === "select");
  if (!header) throw new Error("the select column has no header");
  const renderHeader = header.column.columnDef.header as (
    context: HeaderContext<Row, unknown>,
  ) => ReactElement<CheckboxProps>;
  return renderHeader(header.getContext()).props;
}

/** One row's select cell: the wrapper whose handlers keep the click off the
 * row, and the checkbox inside it. */
function selectCell(table: Table<Row>, companyId: string) {
  const cell = table
    .getRow(companyId)
    .getVisibleCells()
    .find((candidate) => candidate.column.id === "select");
  if (!cell) throw new Error(`no select cell for ${companyId}`);
  const renderCell = cell.column.columnDef.cell as (
    context: CellContext<Row, unknown>,
  ) => ReactElement<SelectCellProps>;
  const wrapper = renderCell(cell.getContext()).props;
  return { wrapper, checkbox: wrapper.children.props };
}

describe("SeCompanyInfoTable selection", () => {
  it("puts the checkbox column FIRST, one box per row and one in the header", () => {
    const html = render({ rows: [ROW, ROW_B] });
    expect(html).toContain('data-slot="row-select"');
    // Before the company id's own cell -- note the row's `data-href` carries
    // that URL too, so the id is compared by the text the link shows.
    expect(html.indexOf('data-slot="row-select"')).toBeLessThan(
      html.indexOf(">5565200028<"),
    );
    expect(html.indexOf(`aria-label="${SELECT_PAGE}"`)).toBeLessThan(
      html.indexOf(">Company<"),
    );
    // Each row's box names its company: a page of identical "Select row"
    // labels tells a screen reader (and a test) nothing.
    expect(html).toContain(`aria-label="${selectLabel(ROW)}"`);
    expect(html).toContain(`aria-label="${selectLabel(ROW_B)}"`);
    expect(html).toContain('aria-label="Select Alpha AB (5565200028)"');
  });

  it("tells two companies of the SAME legal name apart by the id in the label", () => {
    // Legal names repeat across the register; the ids the selection is keyed by
    // do not, so both boxes have to be nameable on their own.
    const twin = { ...ROW_B, legal_name: ROW.legal_name };
    const html = render({ rows: [ROW, twin], selection: { [twin.company_id]: true } });
    expect(html).toContain(`aria-label="${selectLabel(ROW)}"`);
    expect(html).toContain(`aria-label="${selectLabel(twin)}"`);
    expect(checkboxState(html, selectLabel(ROW))).toBe("false");
    expect(checkboxState(html, selectLabel(twin))).toBe("true");
  });

  it("ticks exactly the companies the selection names", () => {
    const html = render({ rows: [ROW, ROW_B], selection: { "5565200028": true } });
    expect(checkboxState(html, selectLabel(ROW))).toBe("true");
    expect(checkboxState(html, selectLabel(ROW_B))).toBe("false");
  });

  it("reads the page back in the header checkbox: none, some, all", () => {
    const rows = [ROW, ROW_B];
    expect(checkboxState(render({ rows }), SELECT_PAGE)).toBe("false");
    expect(
      checkboxState(render({ rows, selection: { "5565200028": true } }), SELECT_PAGE),
    ).toBe("mixed");
    expect(
      checkboxState(
        render({ rows, selection: { "5565200028": true, "5560125220": true } }),
        SELECT_PAGE,
      ),
    ).toBe("true");
  });

  it("keeps a selection across pages: keyed by company id, counted over the lot", () => {
    const selection: RowSelection = { "5565200028": true, "5567890123": true };
    // Page one shows Alpha and Beta; Gamma is picked and not on it.
    const page1 = render({ rows: [ROW, ROW_B], selection });
    expect(checkboxState(page1, selectLabel(ROW))).toBe("true");
    expect(checkboxState(page1, selectLabel(ROW_B))).toBe("false");
    expect(checkboxState(page1, SELECT_PAGE)).toBe("mixed");
    // The company that is off-page still counts -- the indicator is the
    // cross-page total, not what this page happens to show.
    expect(page1).toContain("2 selected");

    // The SAME selection with the next page's rows: what the route component
    // holds while a search-param navigation re-runs the loader.
    const page2 = render({ rows: [ROW_C], selection });
    expect(checkboxState(page2, selectLabel(ROW_C))).toBe("true");
    // This page is now wholly picked while the total is unchanged: the header
    // speaks for the page, the indicator for the whole selection.
    expect(checkboxState(page2, SELECT_PAGE)).toBe("true");
    expect(page2).toContain("2 selected");
  });

  it('shows "N selected · Clear" only once something is picked', () => {
    const empty = render({ rows: [ROW, ROW_B] });
    expect(empty).not.toContain('data-slot="selection-indicator"');
    expect(empty).not.toMatch(/\d+ selected/);
    expect(empty).not.toContain(">Clear<");

    const picked = render({ rows: [ROW, ROW_B], selection: { "5565200028": true } });
    expect(picked).toContain('data-slot="selection-indicator"');
    expect(picked).toContain("1 selected");
    expect(picked).toContain(">Clear<");
    // Not the filter sheet's "Clear all", which stays absent with no filters.
    expect(picked).not.toContain("Clear all");
  });

  it("clears the WHOLE selection, including the pages not on screen", () => {
    expect(
      SelectionIndicator({ selection: NO_ROWS_SELECTED, onSelectionChange: () => {} }),
    ).toBeNull();

    const selection: RowSelection = { "5565200028": true, "5567890123": true };
    const applied: RowSelection[] = [];
    const element = SelectionIndicator({
      selection,
      onSelectionChange: (updater) => {
        applied.push(functionalUpdate(updater, selection));
      },
    }) as ReactElement<{
      children: ReactElement<{ children?: unknown; onClick?: () => void }>[];
    }> | null;
    if (!element) throw new Error("the indicator hid a non-empty selection");
    const clear = element.props.children.find((child) => child.props.children === "Clear");
    expect(clear).toBeDefined();
    clear?.props.onClick?.();
    expect(applied).toEqual([{}]);
  });
});

describe("SeCompanyInfoTable selection handlers", () => {
  it("hands the selection to TanStack as CONTROLLED state, never as a seed", () => {
    // The state belongs to the route component: `state.rowSelection` IS the
    // object passed in, and nothing was handed to `initialState`. Seeding
    // `initialState` instead reads identically on this first render and then
    // ignores every later selection prop -- filtering or paging the list would
    // quietly restore the ticks it was mounted with.
    const initial: RowSelection = { "5565200028": true };
    const { table } = mountSelectable([ROW, ROW_B], initial);
    expect(table.options.state?.rowSelection).toBe(initial);
    expect(table.options.initialState?.rowSelection).toBeUndefined();
  });

  it("picks one company at a time, on top of what is already picked", () => {
    const { table, selectionNow } = mountSelectable([ROW, ROW_B], { "5567890123": true });
    expect(selectCell(table, "5560125220").checkbox.checked).toBe(false);
    selectCell(table, "5560125220").checkbox.onCheckedChange(true);
    expect(selectionNow()).toEqual({ "5567890123": true, "5560125220": true });
  });

  it("unticks that company and nothing else", () => {
    const { table, selectionNow } = mountSelectable([ROW, ROW_B], {
      "5565200028": true,
      "5567890123": true,
    });
    const cell = selectCell(table, "5565200028");
    expect(cell.checkbox.checked).toBe(true);
    cell.checkbox.onCheckedChange(false);
    expect(selectionNow()).toEqual({ "5567890123": true });
    expect(selectedRowIds(selectionNow())).toEqual(["5567890123"]);
  });

  it("selects every row of THIS page from the header, keeping the other pages' picks", () => {
    const { table, selectionNow } = mountSelectable([ROW, ROW_B], { "5567890123": true });
    expect(headerCheckbox(table).checked).toBe(false);
    headerCheckbox(table).onCheckedChange(true);
    expect(selectionNow()).toEqual({
      "5567890123": true,
      "5565200028": true,
      "5560125220": true,
    });
    expect(selectedRowCount(selectionNow())).toBe(3);
  });

  it("clears THIS page from the header, and only this page", () => {
    const { table, selectionNow } = mountSelectable([ROW, ROW_B], {
      "5565200028": true,
      "5560125220": true,
      "5567890123": true,
    });
    const header = headerCheckbox(table);
    expect(header.checked).toBe(true);
    expect(header.indeterminate).toBe(false);
    header.onCheckedChange(false);
    expect(selectionNow()).toEqual({ "5567890123": true });
  });

  it("goes indeterminate while only part of the page is picked", () => {
    const header = headerCheckbox(
      mountSelectable([ROW, ROW_B], { "5565200028": true }).table,
    );
    expect(header.checked).toBe(false);
    expect(header.indeterminate).toBe(true);
  });

  it("keeps the click off the row: ticking a box must never open a company", () => {
    const { html, table } = mountSelectable([ROW, ROW_B], NO_ROWS_SELECTED);
    // The row is still a link ...
    expect(html).toContain('data-href="/admin/se/company/5565200028/info"');
    // ... and the checkbox is NOT one of the inner controls DataTable exempts
    // from that navigation: Base UI renders a <span role="checkbox">, which is
    // no anchor, button or input, so the cell must stop the event itself.
    expect(html).toMatch(/<span[^>]*role="checkbox"/);
    const { wrapper } = selectCell(table, "5565200028");
    const stopped: string[] = [];
    wrapper.onClick({ stopPropagation: () => stopped.push("click") });
    wrapper.onKeyDown({ stopPropagation: () => stopped.push("keydown") });
    expect(stopped).toEqual(["click", "keydown"]);
  });
});

describe("row selection helpers", () => {
  it("lists and counts the ids that are actually ticked", () => {
    expect(selectedRowIds({ "5565200028": true, "5567890123": true })).toEqual([
      "5565200028",
      "5567890123",
    ]);
    expect(selectedRowCount({ "5565200028": true, "5567890123": true })).toBe(2);
    // An explicit `false` is not a selection: TanStack deletes a row's key
    // when it is unticked, but a state built anywhere else can carry one.
    expect(selectedRowIds({ "5565200028": true, "5560125220": false })).toEqual([
      "5565200028",
    ]);
    expect(selectedRowCount({ "5560125220": false })).toBe(0);
    expect(NO_ROWS_SELECTED).toEqual({});
    expect(selectedRowCount(NO_ROWS_SELECTED)).toBe(0);
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
      ...PROFILE_DATATYPES.map((datatype) => datatype.key),
      "profile_sources",
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
    expect(html).toContain(`href="${PATH}?sort=has_domains&amp;dir=asc"`);
    expect(html).toContain(`href="${PATH}?sort=profile_sources&amp;dir=asc"`);
  });
});

describe("SeCompanyInfoTable filter sheet", () => {
  it("opens the filters from one button, badged with the number applied", () => {
    expect(render()).toContain("Filters");
    const html = render({ filters: APPLIED_FILTERS });
    // Seven filters are set on APPLIED_FILTERS, and the badge says so.
    expect(infoFilterChips(APPLIED_FILTERS)).toHaveLength(7);
    expect(html).toContain(">7<");
  });

  it("summarises each applied filter as a chip whose X re-navigates without that param", () => {
    const html = render({ filters: APPLIED_FILTERS });
    expect(html).toContain("Status active");
    expect(html).toContain("Legal form AB-ORGFO");
    expect(html).toContain("Entity Legal (10-digit)");
    expect(html).toContain("Description yes");
    // The source chip reads as the source's NAME, not its URL value.
    expect(html).toContain("Source ESEF");
    expect(html).toContain('aria-label="Remove filter Source ESEF"');
    expect(html).toContain('aria-label="Remove filter Description yes"');

    // The chip's link is the same URL minus that one param -- with the sort and
    // page size kept, and `page` deliberately dropped.
    const withoutDescription = infoListSearch(
      APPLIED_FILTERS,
      { sort: "company_id", dir: "asc", pageSize: 50 },
      "description",
    );
    expect(withoutDescription).not.toContain("description=");
    expect(withoutDescription).toContain("source=esef");
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
    for (const name of ["companyId", "name", "status", "legalForm", "entity", "description",
                        "source"]) {
      expect(html).toContain(`name="${name}"`);
    }
    for (const label of ["Company id", "Name", "Status", "Legal form", "Entity", "Description",
                         "Source"]) {
      expect(html).toContain(label);
    }
    // Task 17: the description-PROVENANCE filters are still gone. `source` is
    // back with an entirely different meaning -- which registers built the
    // profile, in any datatype (bolagsverket/scb/esef/wikidata), not where the
    // published text came from.
    for (const gone of ['name="language"', 'name="suggestion"',
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
    for (const label of ["Company id", "Name", "Status", "Legal form", "Entity", "Description",
                         "Source"]) {
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
    expect(renderFields()).toContain('name="source" value="any"');
    const applied = renderFields(APPLIED_FILTERS);
    expect(applied).toContain('name="status" value="active"');
    expect(applied).toContain('name="legalForm" value="AB-ORGFO"');
    expect(applied).toContain('name="entity" value="legal"');
    expect(applied).toContain('name="description" value="yes"');
    expect(applied).toContain('name="source" value="esef"');
    expect(applied).toContain('name="companyId" value="5565200028"');
  });
});

describe("profileSourceLabel", () => {
  it("names a source value, and is what BOTH the chip and the sheet name it with", () => {
    // One lookup, so a renamed source cannot read one way in the filter
    // dropdown and another way in the chip that summarises it.
    for (const source of PROFILE_SOURCES) {
      expect(profileSourceLabel(source.value)).toBe(source.label);
      expect(infoFilterChips({ ...EMPTY_INFO_FILTERS, source: source.value })).toEqual([
        { param: "source", label: `Source ${source.label}` },
      ]);
    }
    expect(profileSourceLabel("bolagsverket")).toBe("Bolagsverket");
    // A value the catalog does not name is shown, not swallowed.
    expect(profileSourceLabel("llm")).toBe("llm");
  });
});

describe("profileSourceParts", () => {
  it("reads the derived string left to right, naming each letter", () => {
    expect(profileSourceParts("BSEW")).toEqual([
      { letter: "B", label: "Bolagsverket" },
      { letter: "S", label: "SCB" },
      { letter: "E", label: "ESEF" },
      { letter: "W", label: "Wikidata" },
    ]);
    expect(profileSourceParts("S")).toEqual([{ letter: "S", label: "SCB" }]);
    expect(profileSourceParts("BS")).toEqual([
      { letter: "B", label: "Bolagsverket" },
      { letter: "S", label: "SCB" },
    ]);
  });

  it("shows a letter it cannot name rather than dropping it, and nothing for no sources", () => {
    // A letter this build does not know (a source added server-side first) is
    // still data about the company: it is shown, named after itself.
    expect(profileSourceParts("SX")).toEqual([
      { letter: "S", label: "SCB" },
      { letter: "X", label: "X" },
    ]);
    expect(profileSourceParts("")).toEqual([]);
  });

  it("legends exactly the letters it can render, in the order they appear", () => {
    expect(PROFILE_SOURCES_LEGEND).toBe(
      "Sources: B = Bolagsverket · S = SCB · E = ESEF · W = Wikidata",
    );
    expect(PROFILE_SOURCES.map((source) => source.letter).join("")).toBe("BSEW");
  });
});

describe("parseInfoFilters", () => {
  const at = (search: string) => new URL(`http://localhost/admin/se/company-info${search}`);

  it("reads every filter from the URL", () => {
    const filters = parseInfoFilters(
      at(
        "?companyId=5565200028&name=Alpha&status=active&legalForm=AB-ORGFO&entity=legal&description=yes&source=esef",
      ),
    );
    expect(filters).toEqual(APPLIED_FILTERS);
    expect(infoFilterChips(filters)).toHaveLength(7);
    // Round trip: the parsed filters rebuild the very search string they came
    // from (order is the sheet's, `page` deliberately dropped).
    expect(infoListSearch(filters, { sort: "company_id", dir: "asc", pageSize: 50 })).toBe(
      "?companyId=5565200028&name=Alpha&status=active&legalForm=AB-ORGFO&entity=legal" +
        "&description=yes&source=esef&sort=company_id&dir=asc&pageSize=50",
    );
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
      // `source` is whitelisted against the profile-source catalog, so the
      // removed provenance filter's values -- and anything else -- are dropped.
      "?source=llm",
      "?source=bogus",
      "?source=any",
      "?source=",
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

  it("keeps every source of the catalog, and describes it by name in the chip", () => {
    for (const source of PROFILE_SOURCES) {
      const filters = parseInfoFilters(at(`?source=${source.value}`));
      expect(filters.source).toBe(source.value);
      expect(infoFilterChips(filters)).toEqual([
        { param: "source", label: `Source ${source.label}` },
      ]);
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
