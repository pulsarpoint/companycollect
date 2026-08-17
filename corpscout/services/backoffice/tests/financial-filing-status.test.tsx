import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import {
  FINANCIAL_FILING_STATUS_PRESENTATION,
  FinancialFilingStatusBadge,
  FinancialFilingStatusSummary,
} from "~/components/financial-filing-status";

describe("Swedish annual-report filing status", () => {
  it("keeps every evidence state distinct and human-readable", () => {
    expect(
      FINANCIAL_FILING_STATUS_PRESENTATION.map(
        (definition) => definition.value,
      ),
    ).toEqual([
      "data_available",
      "filed_unstructured",
      "not_submitted",
      "unknown",
    ]);

    const html = FINANCIAL_FILING_STATUS_PRESENTATION.map((definition) =>
      renderToStaticMarkup(
        <FinancialFilingStatusBadge status={definition.value} />,
      ),
    ).join("");

    expect(html).toContain("Financial data available");
    expect(html).toContain("Filed in another format");
    expect(html).toContain("Annual report not submitted");
    expect(html).toContain("Filing status unknown");
  });

  it("shows source metadata for an authoritative non-XBRL filing", () => {
    const html = renderToStaticMarkup(
      <FinancialFilingStatusSummary
        filing={{
          status: "filed_unstructured",
          reportPeriodEnd: "2025-04-30",
          filingRegisteredOn: "2025-10-31",
          sourceFileFormat: "application/pdf",
          bolagsverketDocumentId: "123456789",
          sourceSlug: "bolagsverket_xml_d3",
          observedAt: "2026-08-17 20:00:00.000",
        }}
      />,
    );

    expect(html).toContain("Bolagsverket scanned-document register");
    expect(html).toContain("Format: application/pdf");
    expect(html).toContain("Document: 123456789");
    expect(html).toContain("registered 2025-10-31");
  });
});
