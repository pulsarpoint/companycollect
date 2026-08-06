import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import { getCountry } from "~/lib/countries";
import {
  getNorwayFinancialReport,
  getNorwayFinancialReports,
} from "~/lib/norway-financial-reports.server";
import { getCompanyFinancialDetail } from "~/lib/queries.server";

describe("Norway annual account PDFs", () => {
  it("lists every fact-backed document with its real filename and source URL", async () => {
    const [seed] = await chQuery<{ org_number: string }>(
      `SELECT org_number
       FROM no_financial_facts
       GROUP BY org_number
       ORDER BY org_number
       LIMIT 1`,
    );

    const reports = await getNorwayFinancialReports(seed.org_number);

    expect(reports.length).toBeGreaterThan(0);
    for (const report of reports) {
      expect(report.sourceFileName).toMatch(/\.pdf$/i);
      expect(report.sourceUrl).toMatch(
        /^https:\/\/data\.brreg\.no\/regnskapsregisteret\/regnskap\/aarsregnskap\/kopi\//,
      );
      expect(report.factCount).toBeGreaterThan(0);
    }
  });

  it("keeps standardized rows and source PDFs in one Brønnøysund source", async () => {
    const [seed] = await chQuery<{ org_number: string }>(
      `SELECT org_number
       FROM no_financial_facts
       GROUP BY org_number
       ORDER BY org_number
       LIMIT 1`,
    );

    const detail = await getCompanyFinancialDetail(
      getCountry("no")!,
      seed.org_number,
    );
    const source = detail.financialSources.find(
      (candidate) => candidate.kind === "registry",
    );

    expect(source?.id).toBe("brreg-annual-accounts");
    expect(source?.kind).toBe("registry");
    if (source?.kind !== "registry") {
      throw new Error("expected the Brønnøysund registry source");
    }
    expect(source.documents.length).toBeGreaterThan(0);
  });

  it("keeps fact-backed PDFs visible when the report metadata row is absent", async () => {
    const [seed] = await chQuery<{ org_number: string; document_id: string }>(
      `SELECT any(f.org_number) AS org_number, f.document_id AS document_id
       FROM no_financial_facts AS f
       LEFT JOIN no_financial_reports AS r ON r.document_id = f.document_id
       WHERE r.document_id = ''
       GROUP BY f.document_id
       ORDER BY f.document_id
       LIMIT 1`,
    );

    // Once the metadata backfill is complete there may be no such document;
    // that is the desired end state and there is no fallback case left to
    // exercise against the live database.
    if (!seed) return;

    const reports = await getNorwayFinancialReports(seed.org_number);
    const report = reports.find(
      (candidate) => candidate.documentId === seed.document_id,
    );

    expect(report).toBeDefined();
    expect(report?.parseStatus).toBe("facts_loaded");
    expect(report?.hasReportMetadata).toBe(false);
  });

  it("loads facts only when the document belongs to the requested company", async () => {
    const [seed] = await chQuery<{ org_number: string; document_id: string }>(
      `SELECT any(org_number) AS org_number, document_id
       FROM no_financial_facts
       GROUP BY document_id
       ORDER BY document_id
       LIMIT 1`,
    );

    const report = await getNorwayFinancialReport(
      seed.org_number,
      seed.document_id,
    );
    const wrongCompany = await getNorwayFinancialReport(
      "000000000",
      seed.document_id,
    );

    expect(report?.facts.length).toBeGreaterThan(0);
    expect(report?.summary.documentId).toBe(seed.document_id);
    expect(wrongCompany).toBeNull();
  });
});
