import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { SePersonReviewWorkspace } from "~/components/admin/se-person-review-workspace";

const detail = {
  person: {
    person_id: "43234b7d-0184-16b5-de47-dc086a2b0ed9",
    company_id: "5565200028",
    name: "David Mindus",
    description: null,
    draft_ids: ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
    draft_set_hash: "a".repeat(64),
    correction_ids: [],
    suggestion_id: null,
    merged_into_person_id: null,
    model_provider: "deterministic",
    model_name: "single-source:bolagsverket",
    prompt_version: "single-source-copy-v2",
    updated_at: "2026-08-22 09:00:00.000",
  },
  drafts: [
    {
      draft_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      source: "bolagsverket",
      name: "David Mindus",
      role_original: "Verkställande direktör",
      fiscal_year: 2024,
      source_observed_at: "2026-08-01 00:00:00.000",
      source_value_json: "{}",
    },
  ],
  roles: [],
  suggestions: [],
  corrections: [],
};

function render(result: Parameters<typeof SePersonReviewWorkspace>[0]["result"] = null) {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <SePersonReviewWorkspace
            detail={detail}
            activeRoleCodes={["board_member", "chief_executive_officer"]}
            result={result}
          />
        ),
        action: () => null,
      },
    ],
    { initialEntries: ["/admin/se/people/person/5565200028/43234b7d-0184-16b5-de47-dc086a2b0ed9"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("Sweden company-person review page", () => {
  it("shows evidence, provenance and every correction form with the evidence hash", () => {
    const html = render();

    expect(html).toContain("David Mindus");
    expect(html).toContain("Verkställande direktör");
    expect(html).toContain("deterministic");
    for (const kind of ["override_field", "merge_persons", "reassign_draft", "split_person", "set_role", "remove_role"]) {
      expect(html).toContain(`value="${kind}"`);
    }
    expect(html).toContain(`name="evidence_hash" value="${"a".repeat(64)}"`);
    expect(html).toContain("chief_executive_officer");
  });

  it("confirms a saved correction and says Dagster will re-run the company", () => {
    const html = render({ ok: true, correctionId: "55555555-5555-4555-8555-555555555555" });

    expect(html).toContain("Saved");
    expect(html).toContain("re-run company 5565200028");
  });

  it("shows a validation error", () => {
    const html = render({ ok: false, error: "The evidence changed while you were reviewing." });

    expect(html).toContain("The evidence changed while you were reviewing.");
  });
});
