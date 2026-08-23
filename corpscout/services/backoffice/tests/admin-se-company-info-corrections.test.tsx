import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { SeCompanyInfoCorrectionsTable } from "~/components/admin/se-company-info-corrections-table";
import { SeCompanyInfoCorrectionsFilterFields } from "~/components/admin/se-company-info-filter-sheet";
import type {
  SeCompanyInfoCorrectionFilterOptions,
  SeCompanyInfoCorrectionListRow,
} from "~/lib/se-company-info-lists.server";
import {
  correctionFilterChips,
  correctionsListSearch,
  EMPTY_CORRECTION_FILTERS,
  parseCorrectionFilters,
  type SeCompanyInfoCorrectionsTableFilters,
} from "~/lib/se-company-info-filters";

/** Every in-page link resolves against the route this table is rendered at. */
const PATH = "/admin/se/company-info/corrections";

const OVERRIDE_ROW: SeCompanyInfoCorrectionListRow = {
  correction_id: "22222222-2222-4222-8222-222222222222",
  company_id: "5565200028",
  created_at: "2026-08-22 11:00:00.000",
  correction_kind: "override_field",
  payload: '{"description":"Reviewer-written summary."}',
  reason: "SCB copy was templated boilerplate",
  decided_by: "backoffice",
  supersedes_correction_id: null,
  status: "applied",
};

const CLEAR_ROW: SeCompanyInfoCorrectionListRow = {
  ...OVERRIDE_ROW,
  correction_id: "33333333-3333-4333-8333-333333333333",
  payload: '{"description":null}',
  status: "pending",
};

const APPROVE_ROW: SeCompanyInfoCorrectionListRow = {
  correction_id: "44444444-4444-4444-8444-444444444444",
  company_id: "5565200028",
  created_at: "2026-08-22 12:00:00.000",
  correction_kind: "approve_suggestion",
  payload: '{"suggestion_id":"11111111-1111-4111-8111-111111111111"}',
  reason: "Matches SCB",
  decided_by: "backoffice",
  supersedes_correction_id: null,
  status: "stale",
};

const UNDO_ROW: SeCompanyInfoCorrectionListRow = {
  correction_id: "55555555-5555-4555-8555-555555555555",
  company_id: "5565200028",
  created_at: "2026-08-22 13:00:00.000",
  correction_kind: "undo",
  payload: "{}",
  reason: "Wrong call",
  decided_by: "backoffice",
  supersedes_correction_id: "22222222-2222-4222-8222-222222222222",
  status: "undone",
};

const OPTIONS: SeCompanyInfoCorrectionFilterOptions = {
  decidedBy: ["backoffice", "dagster"],
};

const APPLIED_FILTERS: SeCompanyInfoCorrectionsTableFilters = {
  companyId: "5565200028",
  kind: "undo",
  status: "applied",
  decidedBy: "backoffice",
};

function render(props: Partial<Parameters<typeof SeCompanyInfoCorrectionsTable>[0]> = {}) {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <SeCompanyInfoCorrectionsTable
            rows={[OVERRIDE_ROW]}
            total={12}
            page={1}
            pageSize={50}
            sort="created_at"
            dir="desc"
            filters={EMPTY_CORRECTION_FILTERS}
            options={OPTIONS}
            {...props}
          />
        ),
      },
    ],
    { initialEntries: ["/admin/se/company-info/corrections"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("SeCompanyInfoCorrectionsTable", () => {
  it("links the company id to the company page and opens the review page from the row", () => {
    const html = render();
    expect(html).toContain('href="/company/se/5565200028"');
    expect(html).toContain('data-href="/admin/se/company/5565200028/info"');
    expect(html).toContain('role="link"');
    expect(html).not.toContain(">Review<");
  });

  it("shows the 8-char correction id, matching the review page's prefix", () => {
    const html = render();
    expect(html).toContain(OVERRIDE_ROW.correction_id.slice(0, 8));
    expect(html).not.toContain(OVERRIDE_ROW.correction_id);
  });

  it("summarizes an override payload as its description text", () => {
    const html = render();
    expect(html).toContain("Reviewer-written summary.");
  });

  it("summarizes a null-description override as 'clear description'", () => {
    const html = render({ rows: [CLEAR_ROW] });
    expect(html).toContain("clear description");
  });

  it("summarizes approve/reject as 'suggestion <8-char id>'", () => {
    const html = render({ rows: [APPROVE_ROW] });
    expect(html).toContain("suggestion 11111111");
  });

  it("summarizes undo as 'undo <8-char id>' of the correction it supersedes", () => {
    const html = render({ rows: [UNDO_ROW] });
    expect(html).toContain("undo 22222222");
  });

  it("shows reason and decided_by", () => {
    const html = render();
    expect(html).toContain("SCB copy was templated boilerplate");
    expect(html).toContain("backoffice");
  });

  it("shows a status badge for each of the four statuses", () => {
    expect(render({ rows: [OVERRIDE_ROW] })).toContain("applied");
    expect(render({ rows: [CLEAR_ROW] })).toContain("pending");
    expect(render({ rows: [APPROVE_ROW] })).toContain("stale");
    expect(render({ rows: [UNDO_ROW] })).toContain("undone");
  });

  it("shows the pager total and page", () => {
    const html = render();
    expect(html).toContain("12");
    expect(html).toContain("Page 1");
  });

  it("renders an empty state when no rows match", () => {
    const html = render({ rows: [], total: 0 });
    expect(html).toContain("No corrections match these filters.");
  });
});

describe("SeCompanyInfoCorrectionsTable sorting", () => {
  it("gives EVERY column a header that sorts by it, server-side, via ?sort=&dir=", () => {
    // Sorted newest-first by created_at (the default), so that header offers
    // the flip to ascending and every other one offers its own first click.
    const html = render();
    for (const key of [
      "company_id",
      "correction_id",
      "correction_kind",
      "payload",
      "reason",
      "decided_by",
      "status",
    ]) {
      expect(html).toContain(`href="${PATH}?sort=${key}&amp;dir=asc"`);
    }
    expect(html).toContain(`href="${PATH}?sort=created_at&amp;dir=asc"`);
  });

  it("marks the active column and flips its direction on the next click", () => {
    const html = render({ sort: "decided_by", dir: "asc" });
    expect(html).toContain('data-active="true"');
    expect(html).toContain(`href="${PATH}?sort=decided_by&amp;dir=desc"`);
  });
});

describe("SeCompanyInfoCorrectionsTable filter sheet", () => {
  it("opens the filters from one button, badged with the number applied, one chip each", () => {
    expect(render()).toContain("Filters");
    const html = render({ filters: APPLIED_FILTERS });
    expect(correctionFilterChips(APPLIED_FILTERS)).toHaveLength(4);
    expect(html).toContain(">4<");
    expect(html).toContain("Kind undo");
    expect(html).toContain("Status applied");
    expect(html).toContain("Decided by backoffice");
    expect(html).toContain('aria-label="Remove filter Kind undo"');
  });

  it("gives each chip a link that drops just that param, keeping sort and page size", () => {
    const html = render({ filters: APPLIED_FILTERS });
    const withoutKind = correctionsListSearch(
      APPLIED_FILTERS,
      { sort: "created_at", dir: "desc", pageSize: 50 },
      "kind",
    );
    expect(withoutKind).not.toContain("kind=");
    expect(withoutKind).toContain("decidedBy=backoffice");
    expect(withoutKind).toContain("sort=created_at");
    expect(withoutKind).toContain("pageSize=50");
    expect(html).toContain(`href="${PATH}${withoutKind.replaceAll("&", "&amp;")}"`);
  });

  it("shows no chips when nothing is filtered", () => {
    expect(correctionFilterChips(EMPTY_CORRECTION_FILTERS)).toEqual([]);
    expect(render()).not.toContain("Clear all");
  });
});

describe("SeCompanyInfoCorrectionsFilterFields", () => {
  it("offers a field for every filter, including one select per discrete column", () => {
    const html = renderToStaticMarkup(
      <SeCompanyInfoCorrectionsFilterFields
        filters={EMPTY_CORRECTION_FILTERS}
        options={OPTIONS}
        view={{ sort: "created_at", dir: "desc", pageSize: 100 }}
      />,
    );
    for (const name of ["companyId", "kind", "status", "decidedBy"]) {
      expect(html).toContain(`name="${name}"`);
    }
    for (const label of ["Company id", "Kind", "Status", "Decided by"]) {
      expect(html).toContain(label);
    }
    // An unset filter shows the explicit "Any" item Base UI needs.
    expect(html).toContain('name="kind" value="any"');
    expect(html).toContain('name="decidedBy" value="any"');
    // A Base UI select's trigger is a button whose only text is the current
    // value, so the visible <Label> above it names nothing to a screen reader.
    for (const label of ["Company id", "Kind", "Status", "Decided by"]) {
      expect(html).toContain(`aria-label="${label}"`);
    }
    // Not filters, but they must survive one being applied.
    expect(html).toContain('type="hidden" name="pageSize" value="100"');
    expect(html).toContain('type="hidden" name="sort" value="created_at"');
    expect(html).toContain('type="hidden" name="dir" value="desc"');
  });

  it("carries the applied filters as the form's default values", () => {
    const html = renderToStaticMarkup(
      <SeCompanyInfoCorrectionsFilterFields
        filters={APPLIED_FILTERS}
        options={OPTIONS}
        view={{ sort: "created_at", dir: "desc", pageSize: 50 }}
      />,
    );
    expect(html).toContain('name="companyId" value="5565200028"');
    expect(html).toContain('name="kind" value="undo"');
    expect(html).toContain('name="status" value="applied"');
    expect(html).toContain('name="decidedBy" value="backoffice"');
  });
});

describe("parseCorrectionFilters", () => {
  const at = (search: string) =>
    new URL(`http://localhost/admin/se/company-info/corrections${search}`);

  it("reads every ledger filter from the URL", () => {
    expect(
      parseCorrectionFilters(
        at("?companyId=5565200028&kind=undo&status=applied&decidedBy=backoffice"),
      ),
    ).toEqual(APPLIED_FILTERS);
  });

  it("drops a kind or status the query builder would ignore, so no chip claims it", () => {
    for (const search of ["?kind=bogus", "?kind=any", "?status=bogus", "?status=any"]) {
      const filters = parseCorrectionFilters(at(search));
      expect(filters).toEqual(EMPTY_CORRECTION_FILTERS);
      expect(correctionFilterChips(filters)).toEqual([]);
    }
  });

  it("passes decided_by through: its options come from the ledger, not from an enum", () => {
    expect(parseCorrectionFilters(at("?decidedBy=dagster")).decidedBy).toBe("dagster");
  });
});
