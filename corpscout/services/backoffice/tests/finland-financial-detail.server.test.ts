import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import { getCountry } from "~/lib/countries";
import {
  getCompanyFacts,
  getCompanyFinancials,
  getFactsDocument,
} from "~/lib/queries.server";

const finland = getCountry("fi")!;

describe("Finland source-preserving financial detail", () => {
  it(
    "loads standardized PRH metrics, raw facts, and the original XML locator",
    async () => {
      const [filing] = await chQuery<{ id: string; fiscal_year: number }>(
        `SELECT business_id AS id,
           toUInt16(coalesce(fiscal_year, toYear(period_end))) AS fiscal_year
         FROM fi_financial_metrics FINAL
         WHERE source_fact_count > 0
         ORDER BY fiscal_year DESC, registration_date DESC
         LIMIT 1`,
      );

      const [financials, facts, document] = await Promise.all([
        getCompanyFinancials(finland, filing.id),
        getCompanyFacts(finland, filing.id, filing.fiscal_year),
        getFactsDocument(finland, filing.id, filing.fiscal_year),
      ]);

      expect(financials.length).toBeGreaterThan(0);
      expect(financials[0].source_record_uid).toMatch(/^[a-f0-9]{64}$/);
      expect(facts.length).toBeGreaterThan(0);
      expect(facts[0].concept).toBeTruthy();
      expect(facts[0].concept_label_original_language).toBe("fi");
      expect(document).toMatchObject({
        content_type: "application/xml; charset=utf-8",
      });
      expect(document?.source_uri).toMatch(/^s3:\/\/source-finland-prh-xbrl\//);
    },
    30_000,
  );
});
