import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { PeopleSourceTable } from "~/components/admin/people-source-table";
import type { PeopleSourceResult } from "~/lib/people-sources.server";
import { getPeopleSourceDefinition } from "~/lib/people-sources";

const common = {
  filter: { input: "5565200028", companyId: "5565200028", error: "" },
  rowLimit: 100,
};

function renderResult(result: PeopleSourceResult): string {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: <PeopleSourceTable result={result} />,
      },
    ],
    { initialEntries: [`/admin/se/people/sources/${result.source}`] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("PeopleSourceTable", () => {
  it("renders Bolagsverket source columns and source identifiers", () => {
    const html = renderResult({
      ...common,
      source: "bolagsverket",
      definition: getPeopleSourceDefinition("bolagsverket"),
      rows: [
        {
          company_id: "5565200028",
          fiscal_year: 2025,
          statement_key: "sagax-2025",
          source_record_uid: "a".repeat(64),
          signatory_kind: "certification",
          signatory_uid: "b".repeat(64),
          first_name: "David Gustaf",
          last_name: "Mindus",
          role_original: "Verkställande direktör",
          role_kind: "ceo",
          resolved_at: "2026-08-20 10:00:00.000",
        },
      ],
    });

    expect(html).toContain("Bolagsverket source rows");
    expect(html).toContain("David Gustaf Mindus");
    expect(html).toContain("Verkställande direktör");
    expect(html).toContain('href="/company/se/5565200028"');
  });

  it("renders ESEF model evidence without treating it as a merged person", () => {
    const html = renderResult({
      ...common,
      source: "esef",
      definition: getPeopleSourceDefinition("esef"),
      rows: [
        {
          candidate_uid: "c".repeat(64),
          source_record_uid: "d".repeat(64),
          source_document_id: "sagax-2024",
          company_id: "5565200028",
          fiscal_year: 2024,
          name: "Staffan Salén",
          role: "Styrelsens ordförande",
          role_category: "board_chair",
          organization: "AB Sagax",
          status: "current",
          confidence: 0.96,
          evidence_ids: ["evidence-1"],
          model_name: "deepseek-v3.2",
          prompt_version: "v5",
          extracted_at: "2026-08-20 10:00:00.000",
        },
      ],
    });

    expect(html).toContain("ESEF source rows");
    expect(html).toContain("Staffan Salén");
    expect(html).toContain("96%");
    expect(html).toContain("1 evidence IDs");
  });

  it("renders an explicit empty state for a source without company rows", () => {
    const html = renderResult({
      ...common,
      source: "wikidata",
      definition: getPeopleSourceDefinition("wikidata"),
      rows: [],
    });

    expect(html).toContain("No source rows found");
    expect(html).toContain("no person observations");
  });
});
