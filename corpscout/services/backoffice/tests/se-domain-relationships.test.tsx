import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import { SeCompanyDomainsTab } from "~/components/admin/se-company-domains";
import { SeDomainRelationships } from "~/components/admin/se-domain-relationships";
import type { SeCompanyDomainRow, SeDomainRelationship } from "~/lib/se-company-domains.server";

const relationship: SeDomainRelationship = {
  source_kind: "esef", captured_at: "",
  source_document_id: "filing-2024", registrable_domain: "ericsson.com",
  period_end: "2024-12-31", source_url: "https://report.example/2024",
  statements: [{ related_entity_name: "Ericsson", description: "The 2024 report identifies Ericsson among the investments.",
    relationship_supported: true, evidence: [{ evidence_id: "e0", quote: "Our investments include Ericsson." }] }],
  evidence: [{ id: "e0", url: "https://ericsson.com", report_member: "report.xhtml",
    locations: [{ xpath: "/section/p", page_id: "pf8", source_line: 5 }],
    source_context: { text: "Our investments include Ericsson.", local_text: "Ericsson", headings: ["Investments"],
      table_headers: [], truncated: false, reading_order: "document_order_unverified" } }],
};

describe("domain relationship presentation", () => {
  it("distinguishes website capture time from a report period", () => {
    const website: SeDomainRelationship = { ...relationship, source_kind: "website",
      period_end: "", captured_at: "2026-09-18 10:00:00", source_url: "https://issuer.example/partners",
      evidence: [{ ...relationship.evidence[0], source_url: "https://issuer.example/partners",
        locations: [{ page_id: "p1", link_id: "p1:l4" }] }] };
    const html = renderToStaticMarkup(<SeDomainRelationships relationships={[website]} />);
    expect(html).toContain("Website captured 2026-09-18");
    expect(html).not.toContain("Report period ending");
  });
  it("shows a dated explanation without presenting it as a company website or verified fact", () => {
    const html = renderToStaticMarkup(<SeDomainRelationships relationships={[relationship]} />);
    expect(html).toContain("The 2024 report identifies Ericsson among the investments.");
    expect(html).toContain("2024-12-31");
    expect(html).toContain("have not been independently verified");
    expect(html).toContain("Show source evidence");
    expect(html).not.toContain("confidence");
  });

  it("preserves an unexplained mention and its free-text description", () => {
    const uncertain = { ...relationship, statements: [{ ...relationship.statements[0],
      relationship_supported: false, description: "Ericsson is mentioned, but the connection is unclear." }] };
    const html = renderToStaticMarkup(<SeDomainRelationships relationships={[uncertain]} />);
    expect(html).toContain("Connection unclear");
    expect(html).toContain("Ericsson is mentioned, but the connection is unclear.");
  });

  it("keeps rejected website candidates in review history while showing their relationship", () => {
    const domain: SeCompanyDomainRow = {
      root_domain: "ericsson.com", website_url: "https://ericsson.com", website_host: "ericsson.com",
      source_names: ["esef_filing"], supporting_sources: ["esef_filing"], source_confidences: [0.9],
      source_urls: [relationship.source_url], confidence_bases: ["repeated_filing_website"],
      suggested_confidence: 0.95, suggested_primary: 0, review_status: "unreviewed", review_note: "",
      reviewed_by: "", reviewed_at: "", is_active: 0, first_seen_at: "", last_seen_at: "", resolved_at: "",
      association: "not_connected", verification_status: "success", inactive_reason: "rejected",
    };
    const html = renderToStaticMarkup(<MemoryRouter><SeCompanyDomainsTab companyId="5560639147"
      domains={[domain]} relationships={[relationship]} /></MemoryRouter>);
    expect(html).toContain("No current company domains recorded.");
    expect(html).toContain("Website candidate review history (1)");
    expect(html).toContain(relationship.statements[0].description);
    expect(html).not.toContain("confidence 95%");
  });
});
