import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { getCountry } from "~/lib/countries";

const sectionServer = readFileSync(
  new URL("../app/lib/company-sections.server.ts", import.meta.url),
  "utf8",
);
const companyDetailRoute = readFileSync(
  new URL("../app/routes/country-company-detail.tsx", import.meta.url),
  "utf8",
);

describe("Sweden company serving cutover", () => {
  it("keeps raw enrichment sources out of every section request", () => {
    for (const forbidden of [
      "gleif_lei_records",
      "gleif_lei_relationships",
      "wikidata_company_identifiers",
      "wikidata_company_people",
      "country_person_observation",
      "company_description_observations",
      "company_contract_facts",
      "commoncrawl_",
      "replaceRegexpAll",
    ]) {
      expect(sectionServer).not.toContain(forbidden);
    }
    // The reviewable company-domain projection is a ReplacingMergeTree. FINAL
    // is intentional here so a just-written human decision wins immediately.
    expect(sectionServer).toContain("company_domains FINAL");
  });

  it("resolves section evidence from company-scoped source-record keys", () => {
    expect(sectionServer).toContain(
      "source_record_uid IN {source_record_uids:Array(String)}",
    );
    expect(sectionServer).not.toContain(
      "INNER JOIN corpscout.company_source_records",
    );
  });

  it("composes company-address relationships with address-owned data", () => {
    expect(sectionServer).toContain(
      "if(length(address_types) > 0, address_types[1], 'address') AS address_type",
    );
    expect(sectionServer).toContain(
      "FROM corpscout.se_company_addresses_serving_current",
    );
    expect(sectionServer).toContain(
      "FROM corpscout.se_addresses_current",
    );
    expect(sectionServer).toContain(
      "FROM corpscout.se_address_geocodes_current",
    );
    expect(sectionServer).toContain(
      "PREWHERE address_id IN {address_ids:Array(String)}",
    );
    expect(sectionServer).toContain(
      "FROM corpscout.se_company_address_members_current",
    );
    expect(sectionServer).not.toContain("ANY LEFT JOIN");
    expect(sectionServer).not.toContain("se_company_address_geocode_results");
    expect(sectionServer).not.toContain(
      "se_company_addresses_canonical_current AS address",
    );
    expect(sectionServer).not.toContain(
      "se_company_address_geocodes AS geocode",
    );
    expect(sectionServer).toContain("match_status AS geocode_status");
    expect(sectionServer).toContain(
      "candidate_record_urls AS geocode_candidate_record_urls",
    );
    expect(sectionServer).toContain("ifNull(coordinate_locality, '')");
    expect(sectionServer).toContain(
      "coordinate_supporting_point_count\n           AS geocode_coordinate_supporting_point_count",
    );
  });

  it("keeps aggregate output aliases distinct from their input columns", () => {
    // ClickHouse expands aliases throughout a SELECT. Reusing last_seen_at or
    // retrieved_at for max(...) makes argMax(..., <column>) a nested aggregate.
    expect(sectionServer).toContain("AS earliest_seen_at");
    expect(sectionServer).toContain("AS latest_seen_at");
    expect(sectionServer).toContain("AS latest_retrieved_at");
    expect(sectionServer).not.toMatch(/max\(last_seen_at\) AS last_seen_at/);
    expect(sectionServer).not.toMatch(/max\(retrieved_at\) AS retrieved_at/);
  });

  it("loads only present sections and isolates them behind lazy resources", () => {
    expect(companyDetailRoute).toContain('loaderData.mode === "serving"');
    expect(companyDetailRoute).toContain("availableSections.has(section)");
    expect(companyDetailRoute).toContain("<LazyCompanySection");
    expect(companyDetailRoute).not.toContain(
      'getCompanyDetail(country, params.id, shell);\n  if (country.code === "se"',
    );
  });

  it("removes GLEIF identity scans from the Sweden shell", () => {
    const sweden = getCountry("se")!;
    expect(sweden.detail?.companyShellQuery).toContain(
      "FROM se_companies AS c",
    );
    expect(sweden.detail?.companyShellQuery).toContain(
      "PREWHERE c.company_id = {id:String}",
    );
    expect(sweden.detail?.companyShellQuery).not.toContain(
      "se_companies_translated",
    );
    expect(sweden.detail?.companyShellQuery).not.toContain("gleif_lei_records");
    expect(sweden.detail?.companyShellQuery).not.toContain("replaceRegexpAll");
  });
});
