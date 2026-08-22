import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { SeStaleCorrectionsTable } from "~/components/admin/se-stale-corrections-table";
import type { SeStaleCorrectionRow } from "~/lib/se-company-person.server";

const PERSON_ID = "43234b7d-0184-16b5-de47-dc086a2b0ed9";

function row(overrides: Partial<SeStaleCorrectionRow>): SeStaleCorrectionRow {
  return {
    company_id: "5565200028",
    correction_id: "88888888-8888-4888-8888-888888888888",
    correction_kind: "override_field",
    subject_person_id: PERSON_ID,
    reason: "Register spelling",
    decided_by: "backoffice",
    created_at: "2026-08-22 11:00:00.000",
    subject_missing: 0,
    evidence_moved: 0,
    drafts_missing: 0,
    ...overrides,
  };
}

function render(rows: SeStaleCorrectionRow[]): string {
  const router = createMemoryRouter(
    [{ path: "*", element: <SeStaleCorrectionsTable rows={rows} /> }],
    { initialEntries: ["/admin/se/people/stale-corrections"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("stale correction list", () => {
  it("links each row to the person review page it came from", () => {
    const html = render([row({ evidence_moved: 1 })]);

    expect(html).toContain(
      `href="/admin/se/people/person/5565200028/${PERSON_ID}"`,
    );
    expect(html).toContain("override_field");
    expect(html).toContain("Register spelling");
    expect(html).toContain("backoffice");
    expect(html).toContain("2026-08-22 11:00:00.000");
  });

  it("names the reason the pipeline refuses each row", () => {
    const html = render([
      row({ correction_id: "a", evidence_moved: 1 }),
      row({ correction_id: "b", subject_missing: 1 }),
      row({ correction_id: "c", drafts_missing: 1 }),
    ]);

    expect(html).toContain("the evidence changed after the decision");
    expect(html).toContain("the person is no longer published");
    expect(html).toContain("its observations moved to another person");
  });

  it("says so when nothing is stale", () => {
    const html = render([]);

    expect(html).toContain("Nothing to re-decide");
  });
});
