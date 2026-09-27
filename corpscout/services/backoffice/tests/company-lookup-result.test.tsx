import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import { CompanyLookupSummary, type CompanyLookupOutput } from "~/components/admin/company-lookup-result";

const result: CompanyLookupOutput = {status: "matched", found: true, company_id: "5564608726", confidence: 0.95,
  site_type: "company", reasons: ["Organisation number agrees"], company: {legal_name: "Example AB"}, searches: [{}], candidates: [{}]};
const render = (value: CompanyLookupOutput) => renderToStaticMarkup(<MemoryRouter><CompanyLookupSummary result={value} /></MemoryRouter>);

describe("company lookup output", () => {
  it("shows an existing connection as a skipped match rather than no match", () => {
    const html = render({...result, status: "already_mapped", found: false, company_id: null, confidence: null,
      existing_company_ids: ["5560123456"], reasons: ["Domain already has an active company association"]});
    expect(html).toContain("Company matching skipped · already connected");
    expect(html).toContain('href="/admin/se/company/5560123456"');
    expect(html).not.toContain("No company match found");
    expect(html).not.toContain("Company match proposed");
  });
  it("retains a successful site brief when matching fails and reports delivery", () => {
    const html = render({...result, status: "failed", found: false, company_id: null,
      publication: {state: "published"},
      site_info: {site_description: "Example manufactures sensors.", purpose: "Sensor manufacturing", operator_name: "Example AB", business_activities: ["Sensor design"], evidence_status: "source_matched"},
      site_info_result: {crawl: {status: "finished", stop_reason: "site_info_complete"}}});
    expect(html).toContain("Company lookup failed");
    expect(html).toContain("Basic site information");
    expect(html).toContain("Example manufactures sensors.");
    expect(html).toContain("Findings saved to ClickHouse");
    expect(html).toContain("links await acceptance");
  });
  it("shows industry evidence and uncertain code versions separately from identity", () => {
    const html = render({...result,
      identity: [{kind: 'business_activity', value: 'Property management', quote: 'We manage properties', source_url: 'https://example.se/about'}],
      industry_assessments: [{company_id: '5564608726', legal_name: 'Example AB', status: 'insufficient_evidence', reasons: ['Classification version is uncertain'],
        industries: [{classification_code: '6832', classification_version: 'NACE_REV_2', reported_label: 'Property management', reference_label: 'Management of real estate', reference_status: 'label_version_conflict', is_primary: 1}]}]});
    expect(html).toContain('NACE industry checks');
    expect(html).toContain('We manage properties');
    expect(html).toContain('href="https://example.se/about"');
    expect(html).toContain('NACE REV 2');
    expect(html).toContain('6832');
    expect(html).toContain('Not used: label version conflict');
    expect(html).toContain('insufficient evidence');
    expect(html).toContain('Company match proposed');
  });
  it("separates a rejected model proposal from the final evidence check", () => {
    const html = render({...result, found: false, company_id: null, confidence: null, status: "not_found",
      reasons: ["Unique company claimed by model", "Multiple registry companies share the legal name"],
      assessment: {reasons: ["Unique company claimed by model"]}});
    expect(html).toContain("Final evidence check:");
    expect(html).toContain("Multiple registry companies share the legal name");
    expect(html).toContain("<summary class=\"cursor-pointer text-sm\">Model assessment</summary>");
    expect(html).toContain("No company match found");
  });
  it("shows Jev alternatives and the model actually served even if the match is rejected", () => {
    const html = render({...result, found: false, company_id: null, confidence: null, status: 'not_found',
      models: {requested: 'configured-model', served: ['actual-model']}, no_match_probability: 0.3,
      candidate_assessments: [{company_id: '5564608726', legal_name: 'Example AB', confidence: 0.7, reasons: ['Ambiguous operator']}]});
    expect(html).toContain('Jev candidate ranking');
    expect(html).toContain('70%');
    expect(html).toContain('30%');
    expect(html).toContain('actual-model');
    expect(html).toContain('Ambiguous operator');
    expect(html).toContain('No company match found');
  });
  it("shows a proposal with its company link, confidence, evidence and read-only status", () => {
    const html = render(result);
    expect(html).toContain("Company match proposed");
    expect(html).toContain('href="/admin/se/company/5564608726"');
    expect(html).toContain("95%");
    expect(html).toContain("not a calibrated probability");
    expect(html).toContain("Historical test: no database results or associations saved");
    expect(html).toContain("Full lookup output");
  });
  it("distinguishes excluded websites from failures and escapes website-derived content", () => {
    const noMatch = {...result, status: "not_found" as const, found: false, company_id: null, confidence: null, company: null, site_type: "online_store", reasons: ["<script>untrusted</script>"]};
    const html = render(noMatch);
    expect(html).toContain("No company match found");
    expect(html).toContain("online store");
    expect(html).not.toContain("<script>");
    expect(html).not.toContain("Confidence:");
    expect(render({...noMatch, status: "failed"})).toContain("Company lookup failed");
  });
});
