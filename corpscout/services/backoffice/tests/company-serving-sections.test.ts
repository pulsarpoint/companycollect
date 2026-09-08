import { readFileSync } from "node:fs";
import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import { getCompanySection } from "~/lib/company-sections.server";
import { getCountry } from "~/lib/countries";

const sectionServer = readFileSync(
  new URL("../app/lib/company-sections.server.ts", import.meta.url),
  "utf8",
);
const companyDetailRoute = readFileSync(
  new URL("../app/routes/country-company-detail.tsx", import.meta.url),
  "utf8",
);

describe("Sweden company sections", () => {
  it("keeps raw enrichment sources out of every section request", () => {
    for (const forbidden of [
      "gleif_lei_records",
      "gleif_lei_relationships",
      "wikidata_company_identifiers",
      "wikidata_company_people",
      "country_person_observation",
      "company_source_records",
      "company_source_record_origins",
      "company_source_record_links",
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
      "FROM corpscout.company_section_item_source_links",
    );
    expect(sectionServer).toContain("record_kind, content_sha256");
  });

  it("composes the addresses section from the published address entity", () => {
    expect(sectionServer).toContain(
      'import { SE_COMPANY_ADDRESS_TABLE } from "~/lib/se-address-tables"',
    );
    expect(sectionServer).toContain("FROM ${SE_COMPANY_ADDRESS_TABLE} AS address FINAL");
    expect(sectionServer).not.toContain("se_company_address_v2");
    expect(sectionServer).toContain(
      "if(length(address.kinds) > 0, toString(address.kinds[1]), 'address') AS address_type",
    );
    // The members are the row's own parallel arrays, resolved to their raw
    // text in the suggestion store -- no members projection any more.
    expect(sectionServer).toContain("arrayZip(");
    expect(sectionServer).toContain(
      "FROM corpscout.se_company_address_suggestion AS raw FINAL",
    );
    // The geocode now travels on the published row itself.
    expect(sectionServer).toContain(
      "toString(address.geocode_status) AS geocode_status",
    );
    expect(sectionServer).toContain(
      "toString(address.geocode_method) AS geocode_match_method",
    );
    expect(sectionServer).toContain(
      "address.geocode_status = 'matched_area', 'centroid_fallback',",
    );
    expect(sectionServer).toContain(
      "ifNull(address.geocode_confidence, 0) AS geocode_match_confidence",
    );
    expect(sectionServer).toContain("ifNull(address.city, '')");
    // ACTIVE ROWS ONLY: the section renders every address it is handed the
    // same way, so a hidden or withdrawn row would sit among the live ones
    // with nothing to tell them apart.
    expect(sectionServer).toContain("WHERE address.active = 1");
    expect(sectionServer).not.toContain("ORDER BY address.active DESC");
    // A postcode-only line ("100 11 Stockholm") has no comma to strip at, so
    // the strip alone would show the whole line as the street.
    expect(sectionServer).toContain(
      "match(address.normalized_address, '^[0-9]{3} [0-9]{2}[^,]*$'),",
    );
    for (const retired of [
      "se_addresses_current",
      "se_address_geocodes_current",
      "se_company_address_links_current",
      "se_company_address_members_current",
      "se_company_address_geocode_results",
    ]) {
      expect(sectionServer).not.toContain(retired);
    }
    expect(sectionServer).not.toContain("ANY LEFT JOIN");
  });

  it("reads source lineage directly from the serving link", () => {
    expect(sectionServer).toContain("toString(first_seen_at) AS first_seen_at");
    expect(sectionServer).toContain("toString(last_seen_at) AS last_seen_at");
    expect(sectionServer).toContain("toString(retrieved_at) AS retrieved_at");
    expect(sectionServer).not.toContain("argMax(");
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
    expect(sweden.detail?.companyShellQuery).not.toContain("splitByChar");
  });
});

/** One published entity row and the three raw suggestions it was folded from,
 * exactly as the two section queries return them. */
const PUBLISHED_ADDRESS = {
  address_id: "a".repeat(64),
  canonical_address_key: "a".repeat(64),
  address_type: "postal",
  address_types: ["postal", "visiting_or_postal"],
  address_sources: ["bolagsverket", "scb"],
  address_member_count: 2,
  members: [
    ["bolagsverket", "", "b".repeat(64)],
    ["scb", "", "c".repeat(64)],
  ],
  full_address: "Gammelvägen 74D, 871 98 Ramvik",
  address_country_code: "SE",
  address_is_foreign: 0,
  geocode_street: "Gammelvägen 74D",
  street_name: "gammelvägen",
  house_number: "74D",
  address_unit: "",
  geocode_postal_code: "87198",
  latitude: 62.81,
  longitude: 17.85,
  geocode_status: "matched_exact",
  geocode_provider: "osm",
  geocode_precision: "building",
  geocode_match_method: "postal_code_street_house_exact_unique",
  geocode_match_confidence: 1,
  geocode_candidate_count: 0,
  geocode_candidate_record_urls: [],
  geocode_coordinate_locality: "ramvik",
  geocode_coordinate_supporting_point_count: 0,
  geocode_source_record_id: "",
  geocode_source_record_url: "",
  geocode_source_url: "",
  geocode_source_object_key: "",
  geocode_source_md5: "",
  geocode_source_snapshot_at: "",
  geocode_source_retrieved_at: "",
  geocode_source_run_id: "",
  geocode_matched_at: "2026-09-07 16:58:14.615",
};
const RAW_SUGGESTIONS = [
  {
    address_source: "bolagsverket",
    slot: "",
    address_type: "postal",
    raw_address: "Gammelvägen 74D$$RAMVIK$87198$SE-LAND",
    structured_address: "",
    registry_source_record_uid: "bolagsverket-record",
    registry_source_run_id: "bolagsverket-run",
    source_observed_at: "2026-09-03 18:16:21.117",
  },
  {
    address_source: "scb",
    slot: "",
    address_type: "visiting_or_postal",
    raw_address: "",
    structured_address: "GAMMELVÄGEN 74 D, 87198 RAMVIK",
    registry_source_record_uid: "scb-record",
    registry_source_run_id: "scb-run",
    source_observed_at: "2026-09-03 18:16:21.117",
  },
];

describe("Sweden addresses section", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql.includes("se_company_address_suggestion")
        ? RAW_SUGGESTIONS
        : [PUBLISHED_ADDRESS],
    );
  });

  it("zips each published row's sources onto their raw suggestion", async () => {
    const section = await getCompanySection("SE", "5595421834", "addresses");

    expect(section).toEqual({
      section: "addresses",
      addresses: [
        {
          ...Object.fromEntries(
            Object.entries(PUBLISHED_ADDRESS).filter(
              ([key]) => key !== "members",
            ),
          ),
          source_members: [
            {
              address_key: "b".repeat(64),
              address_type: "postal",
              address_source: "bolagsverket",
              // Bolagsverket delivers a raw line and no components, so it is
              // its own display value and the "source value" line stays away.
              raw_address: "Gammelvägen 74D$$RAMVIK$87198$SE-LAND",
              display_address: "Gammelvägen 74D$$RAMVIK$87198$SE-LAND",
              registry_source_record_uid: "bolagsverket-record",
              registry_source_run_id: "bolagsverket-run",
              source_observed_at: "2026-09-03 18:16:21.117",
            },
            {
              address_key: "c".repeat(64),
              address_type: "visiting_or_postal",
              address_source: "scb",
              raw_address: "",
              display_address: "GAMMELVÄGEN 74 D, 87198 RAMVIK",
              registry_source_record_uid: "scb-record",
              registry_source_run_id: "scb-run",
              source_observed_at: "2026-09-03 18:16:21.117",
            },
          ],
        },
      ],
    });
    expect(clickhouse.query.mock.calls.map(([, params]) => params)).toEqual([
      { id: "5595421834" },
      { id: "5595421834" },
    ]);
  });

  it("stands in a member's line when the published row has none", async () => {
    // 42,126 active foreign rows were published with an empty
    // normalized_address before the normalizer composed a display line for
    // them; the detail card hides a section whose addresses have no line, so
    // the reader falls back to what the first member delivered rather than
    // rendering a blank row for an address the register does have.
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql.includes("se_company_address_suggestion")
        ? RAW_SUGGESTIONS
        : [{ ...PUBLISHED_ADDRESS, full_address: "" }],
    );

    const section = await getCompanySection("SE", "5595421834", "addresses");
    const addresses =
      section.section === "addresses" ? section.addresses : undefined;

    expect(addresses?.[0]?.full_address).toBe(
      "Gammelvägen 74D$$RAMVIK$87198$SE-LAND",
    );
  });

  it("keeps the published line whenever the row carries one", async () => {
    const section = await getCompanySection("SE", "5595421834", "addresses");
    const addresses =
      section.section === "addresses" ? section.addresses : undefined;

    expect(addresses?.[0]?.full_address).toBe(
      "Gammelvägen 74D, 871 98 Ramvik",
    );
  });

  it("keeps a member the suggestion store no longer holds", async () => {
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql.includes("se_company_address_suggestion") ? [] : [PUBLISHED_ADDRESS],
    );

    const section = await getCompanySection("SE", "5595421834", "addresses");
    const addresses =
      section.section === "addresses" ? section.addresses : undefined;

    expect(addresses?.[0]?.source_members).toEqual([
      {
        address_key: "b".repeat(64),
        address_type: "postal",
        address_source: "bolagsverket",
        raw_address: "",
        display_address: "",
        registry_source_record_uid: "",
        registry_source_run_id: "",
        source_observed_at: "",
      },
      {
        address_key: "c".repeat(64),
        address_type: "postal",
        address_source: "scb",
        raw_address: "",
        display_address: "",
        registry_source_record_uid: "",
        registry_source_run_id: "",
        source_observed_at: "",
      },
    ]);
  });
});
