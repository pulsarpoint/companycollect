import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { SeCompanyInfoCorrectionsTable } from "~/components/admin/se-company-info-corrections-table";
import type { SeCompanyInfoCorrectionListRow } from "~/lib/se-company-info-lists.server";

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

const EMPTY_FILTERS = { companyId: "", kind: "", status: "" };

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
            filters={EMPTY_FILTERS}
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

  it("carries the applied filters as the form's default values", () => {
    const html = render({
      filters: { companyId: "5565200028", kind: "undo", status: "applied" },
    });
    expect(html).toContain('name="companyId" value="5565200028"');
    expect(html).toContain('name="kind" value="undo"');
    expect(html).toContain('name="status" value="applied"');
  });

  it("carries the current pageSize as a hidden field, so submitting a filter doesn't silently reset it", () => {
    const html = render({ pageSize: 100 });
    expect(html).toContain('type="hidden" name="pageSize" value="100"');
  });

  it("offers an explicit \"Any\" option on the kind and status selects, selected when no filter is set", () => {
    const html = render();
    expect(html).toContain('name="kind" value="any"');
    expect(html).toContain('name="status" value="any"');
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
