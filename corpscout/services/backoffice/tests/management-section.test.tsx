import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import { ManagementSection } from "~/components/detail/management-section";
import type { EvidenceRef, OfficerRow } from "~/lib/queries.server";

function annualReportEvidence(uid: string, year: number): EvidenceRef {
  return {
    sourceRecordUid: uid,
    recordKind: "annual_report_xhtml",
    contentSha256: uid,
    firstSeenAt: `${year}-01-01 00:00:00`,
    lastSeenAt: `${year}-01-01 00:00:00`,
    origins: [
      {
        sourceSlug: "sweden_financial",
        sourceRecordKey: `annual-report-${year}`,
        sourceUrl: `https://example.test/annual-report-${year}`,
        sourceObjectKey: `annual-report-${year}.xhtml`,
        payloadSha256: uid,
        retrievedAt: `${year}-01-01 00:00:00`,
        sourceRunId: `run-${year}`,
      },
    ],
    connectionKind: "annual_report_signature",
  };
}

function officer(
  overrides: Partial<OfficerRow> & Pick<OfficerRow, "fiscal_year" | "evidence">,
): OfficerRow {
  return {
    country_iso2: "SE",
    person_id: "988fe79f-22b6-1914-86d9-f9e47de29008",
    first_name: "Niklas",
    last_name: "Thorén",
    role_original: "Verkställande direktör",
    role_kind: "ceo",
    signatory_kind: "board_signature",
    ...overrides,
  };
}

describe("ManagementSection", () => {
  it("renders one person with evidence from every report observation", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter>
        <ManagementSection
          officers={[
            officer({
              fiscal_year: 2025,
              evidence: [annualReportEvidence("a".repeat(64), 2025)],
            }),
            officer({
              fiscal_year: 2025,
              signatory_kind: "certification",
              evidence: [annualReportEvidence("a".repeat(64), 2025)],
            }),
            officer({
              fiscal_year: 2024,
              evidence: [annualReportEvidence("b".repeat(64), 2024)],
            }),
          ]}
          peopleMatches={[]}
          audit={null}
        />
      </MemoryRouter>,
    );

    expect(html.match(/Niklas Thorén/g)).toHaveLength(1);
    expect(html).toContain(
      'href="/country/se/person/988fe79f-22b6-1914-86d9-f9e47de29008"',
    );
    expect(html.match(/>CEO</g)).toHaveLength(1);
    expect(html).toContain("Sources and connections (2)");
    expect(html).toContain("annual report signature");
    expect(html).toContain("annual-report-2025");
    expect(html).toContain("annual-report-2024");
  });

  it("combines source-specific roles into one company person", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter>
        <ManagementSection
          officers={[
            officer({
              fiscal_year: 2025,
              evidence: [annualReportEvidence("c".repeat(64), 2025)],
            }),
          ]}
          peopleMatches={[]}
          audit={null}
          wikidataPeople={[
            {
              person_wikidata_id: "Q123",
              name: "Niklas Thorén",
              description: "",
              birth_year: null,
              image_url: "",
              wikidata_url: "https://www.wikidata.org/wiki/Q123",
              role_label: "board member",
              is_current: 1,
              start_date: "",
              end_date: "",
              evidence: [
                {
                  ...annualReportEvidence("d".repeat(64), 2025),
                  recordKind: "wikidata_person_item",
                  origins: [
                    {
                      sourceSlug: "wikidata",
                      sourceRecordKey: "Q123",
                      sourceUrl: "https://www.wikidata.org/wiki/Q123",
                      sourceObjectKey: "",
                      payloadSha256: "d".repeat(64),
                      retrievedAt: "2025-01-01 00:00:00",
                      sourceRunId: "run-wikidata",
                    },
                  ],
                  connectionKind: "public_knowledge_graph_company_role",
                },
              ],
            },
          ]}
        />
      </MemoryRouter>,
    );

    expect(html.match(/Niklas Thorén/g)).toHaveLength(1);
    expect(html).toContain("CEO");
    expect(html).toContain("board member");
    expect(html).toContain("Sources and connections (2)");
    expect(html).toContain("public knowledge graph company role");
  });

  it("presents an auditor as an external professional, not a board role", () => {
    const personId = "3210b10c-fc3b-053a-d0e5-a8eec362a1a5";
    const html = renderToStaticMarkup(
      <MemoryRouter>
        <ManagementSection
          officers={[
            officer({
              person_id: personId,
              first_name: "Johan",
              last_name: "Lindström",
              role_original: "",
              role_kind: "unknown",
              signatory_kind: "board_signature",
              fiscal_year: 2025,
              evidence: [annualReportEvidence("e".repeat(64), 2025)],
            }),
            officer({
              person_id: personId,
              first_name: "Johan",
              last_name: "Lindström",
              role_original: "Auktoriserad revisor",
              role_kind: "auditor",
              signatory_kind: "auditor",
              fiscal_year: 2025,
              evidence: [annualReportEvidence("f".repeat(64), 2025)],
            }),
          ]}
          peopleMatches={[]}
          audit={null}
        />
      </MemoryRouter>,
    );

    expect(html.match(/Johan Lindström/g)).toHaveLength(1);
    expect(html).toContain("External auditor");
    expect(html).not.toContain("Report signatory");
    expect(html).not.toContain("Board member");
  });
});
