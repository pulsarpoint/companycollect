import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { CompanyRecordSection } from "~/components/detail/detail-sections";
import type { CompanyListRow } from "~/lib/queries.server";

function company(overrides: Partial<CompanyListRow> = {}): CompanyListRow {
  return {
    active: 1,
    company_id: "5562434182",
    name: "T.I.R. Byggnads Aktiebolaget Rajaharju",
    industry_code: null,
    industry_label: null,
    ...overrides,
  };
}

describe("CompanyRecordSection", () => {
  it("omits an empty shell industry while lazy-loaded industry data is unavailable", () => {
    const html = renderToStaticMarkup(
      <CompanyRecordSection
        company={company()}
        record={{ legal_name_registration_date: "1984-11-08" }}
        lang="en"
      />,
    );

    expect(html).toContain("Legal name registration date");
    expect(html).toContain("1984-11-08");
    expect(html).not.toContain(">Industry<");
  });

  it("keeps an industry supplied by the company shell", () => {
    const html = renderToStaticMarkup(
      <CompanyRecordSection
        company={company({ industry_code: "4100", industry_label: "Construction" })}
        record={{}}
        lang="en"
      />,
    );

    expect(html).toContain(">Industry<");
    expect(html).toContain("4100 Construction");
  });

  it("explains a registry status conflict and keeps its provenance out of the main grid", () => {
    const html = renderToStaticMarkup(
      <CompanyRecordSection
        company={company()}
        record={{
          status: "active",
          status_source: "bolagsverket",
          status_observed_at: "2026-08-17T15:27:36.717Z",
          status_conflict: 1,
        }}
        lang="en"
      />,
    );

    expect(html).toContain("Status sources disagree");
    expect(html).toContain("status follows Bolagsverket");
    expect(html).toContain(">Active<");
    expect(html).toContain("Source &amp; lineage");
  });
});
