import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import {
  parseAddressQualityFilter,
  searchAddressQualityQueue,
} from "~/lib/address-quality.server";

/** The queue's four reads, as the entity answers them. */
const STATS_ROW = {
  reviewable: 9,
  ambiguous: 4,
  unmatched: 3,
  invalid: 0,
  street_fallback: 1,
  city_fallback: 1,
  low_confidence: 0,
};
const PAGE_ROW = {
  company_id: "5595421834",
  address_id: "0".repeat(64),
  display_address: "Gammelvägen 74D, 871 98 Ramvik",
  representative_source: "bolagsverket",
  street_address: "Gammelvägen 74D",
  postal_code: "87198",
  post_town: "ramvik",
  address_kind: "physical",
  company_count: 1,
  evidence_count: 3,
  match_status: "ambiguous",
  candidate_count: 0,
  candidate_record_urls: [] as string[],
  match_method: "street_requested_house_missing_postcode_conflict",
  match_confidence: 0.35,
  latitude: null,
  longitude: null,
  geocode_precision: "",
  coordinate_method: "street_requested_house_missing_postcode_conflict",
  coordinate_locality: "ramvik",
  coordinate_supporting_point_count: 0,
  source_url: "",
  source_snapshot_at: "",
  matched_at: "2026-09-07 16:58:14.615",
};

function answerFor(sql: string): unknown[] {
  if (sql.includes("countIf(")) return [STATS_ROW];
  if (sql.includes("count() AS total")) return [{ total: 2 }];
  if (sql.includes("FROM corpscout.se_companies")) {
    return [{ company_id: "5595421834", company_name: "Nordvind AB" }];
  }
  return [PAGE_ROW];
}

function callFor(fragment: string): [string, Record<string, unknown>] {
  const call = clickhouse.query.mock.calls.find(
    (args: unknown[]) => (args[0] as string).includes(fragment),
  );
  if (!call) throw new Error(`no query contains ${fragment}`);
  return call as [string, Record<string, unknown>];
}

function sqlOf(fragment: string): string {
  return callFor(fragment)[0];
}

beforeEach(() => {
  clickhouse.query.mockReset();
  clickhouse.query.mockImplementation(async (sql: string) => answerFor(sql));
});

describe("Sweden address quality queue", () => {
  it("defaults unknown filters to the actionable ambiguous queue", () => {
    expect(parseAddressQualityFilter(null)).toBe("ambiguous");
    expect(parseAddressQualityFilter("unknown")).toBe("ambiguous");
    expect(parseAddressQualityFilter("city_fallback")).toBe("city_fallback");
    expect(parseAddressQualityFilter("street_fallback")).toBe(
      "street_fallback",
    );
  });

  it("reads the published address entity and never the retired chain", async () => {
    await searchAddressQualityQueue({
      filter: "ambiguous",
      query: "",
      page: 1,
      pageSize: 25,
    });

    const queries: string[] = clickhouse.query.mock.calls.map(
      (args: unknown[]) => args[0] as string,
    );
    expect(queries).toHaveLength(3);
    for (const sql of queries) {
      for (const retired of [
        "se_addresses_current",
        "se_address_geocodes_current",
        "se_company_address_links_current",
        "se_company_address_members_current",
      ]) {
        expect(sql).not.toContain(retired);
      }
    }
    // Every address read is one FINAL scan of the published rows.
    for (const sql of queries.filter((q: string) => q.includes("address."))) {
      expect(sql).toContain("FROM corpscout.se_company_address AS address FINAL");
      expect(sql).toContain("WHERE address.active = 1");
      // Slice 4b renamed the table; the interim name must not survive anywhere.
      expect(sql).not.toContain("se_company_address_v2");
    }
  });

  it("counts and filters with the same predicates", async () => {
    await searchAddressQualityQueue({
      filter: "low_confidence",
      query: "",
      page: 1,
      pageSize: 25,
    });

    const stats = sqlOf("countIf(");
    expect(stats).toContain("countIf(address.geocode_status = 'ambiguous')");
    expect(stats).toContain("countIf(address.geocode_status = 'unmatched')");
    // Invalid and property are ONE queue: the entity writes 'invalid_address'
    // for a line it could not parse and 'property_identifier' for a cadastral
    // designation, and both need the same reviewer. Counting only the first
    // would leave every property row out of the tile AND out of the filter it
    // links to.
    expect(stats).toContain(
      "countIf(address.geocode_status IN ('invalid_address', 'property_identifier'))",
    );
    expect(stats).toContain("countIf(address.geocode_precision = 'street')");
    expect(stats).toContain("countIf(address.geocode_precision = 'city')");
    expect(stats).toContain("address.geocode_confidence < 0.8");
    // The page uses the very same low-confidence predicate as its tile.
    const page = sqlOf("AS display_address");
    expect(page).toContain(
      "address.geocode_status = 'matched_exact'\n    AND address.geocode_confidence < 0.8",
    );
  });

  it("puts a property identifier in the invalid queue and in All reviewable", async () => {
    await searchAddressQualityQueue({
      filter: "invalid",
      query: "",
      page: 1,
      pageSize: 25,
    });

    // The page the tile links to filters on the SAME widened predicate...
    const page = sqlOf("AS display_address");
    expect(page).toContain(
      "address.geocode_status IN ('invalid_address', 'property_identifier')",
    );
    // ...and the All-reviewable predicate, the union of the tiles, admits it
    // too -- otherwise a property row would count in one tile and vanish from
    // the queue that is supposed to contain every reviewable address.
    const stats = sqlOf("countIf(");
    expect(stats).toContain("'property_identifier'\n    )");
  });

  it("maps the entity's columns onto the queue's row shape", async () => {
    await searchAddressQualityQueue({
      filter: "ambiguous",
      query: "",
      page: 2,
      pageSize: 25,
    });

    const page = sqlOf("AS display_address");
    expect(page).toContain("address.company_id AS company_id");
    expect(page).toContain("toString(address.address_key) AS address_id");
    expect(page).toContain("address.normalized_address AS display_address");
    expect(page).toContain(
      "toString(address.text_source) AS representative_source",
    );
    // The street part of the line: the postcode and town are cut off the end,
    // and a line that is ONLY a postal part (normalizer v3's postcode-only
    // addresses, which have no comma to cut at) has no street part at all --
    // the strip alone would put the whole "100 11 Stockholm" in the column.
    expect(page).toContain(
      "if(match(address.normalized_address, '^[0-9]{3} [0-9]{2}[^,]*$'), ''," +
        " replaceRegexpOne(address.normalized_address, ',\\\\s*[0-9]{3} [0-9]{2}[^,]*$', ''))" +
        " AS street_address",
    );
    expect(page).toContain("ifNull(address.city, '') AS post_town");
    expect(page).toContain(
      "if(ifNull(address.box, '') != '', 'postal_box', 'physical') AS address_kind",
    );
    expect(page).toContain("toUInt64(1) AS company_count");
    expect(page).toContain(
      "toUInt64(length(address.sources)) AS evidence_count",
    );
    expect(page).toContain("toString(address.geocode_status) AS match_status");
    expect(page).toContain("toString(address.geocode_method) AS match_method");
    expect(page).toContain(
      "ifNull(address.geocode_confidence, 0) AS match_confidence",
    );
    expect(page).toContain("ifNull(address.city, '') AS coordinate_locality");
    expect(page).toContain(
      "ifNull(toString(address.geocoded_at), '') AS matched_at",
    );
    // Nothing the entity does not carry is invented.
    expect(page).toContain("toUInt64(0) AS candidate_count");
    expect(page).toContain("CAST([], 'Array(String)') AS candidate_record_urls");
    expect(page).toContain("'' AS source_url");
    // Ambiguous first, then the fallbacks, then the best-evidenced row.
    expect(page).toContain("address.geocode_status = 'ambiguous', 0,");
    expect(page).toContain("length(address.sources) DESC");
    expect(callFor("AS display_address")[1]).toEqual({
      query: "",
      limit: 25,
      offset: 25,
    });
  });

  it("returns one row per company with its own address and name", async () => {
    const result = await searchAddressQualityQueue({
      filter: "ambiguous",
      query: "",
      page: 1,
      pageSize: 25,
    });

    expect(result.rows).toEqual([
      {
        companyId: "5595421834",
        addressId: "0".repeat(64),
        displayAddress: "Gammelvägen 74D, 871 98 Ramvik",
        representativeSource: "bolagsverket",
        streetAddress: "Gammelvägen 74D",
        postalCode: "87198",
        postTown: "ramvik",
        addressKind: "physical",
        companyCount: 1,
        evidenceCount: 3,
        matchStatus: "ambiguous",
        candidateCount: 0,
        candidateRecordUrls: [],
        matchMethod: "street_requested_house_missing_postcode_conflict",
        matchConfidence: 0.35,
        latitude: null,
        longitude: null,
        geocodePrecision: "",
        coordinateMethod: "street_requested_house_missing_postcode_conflict",
        coordinateLocality: "ramvik",
        coordinateSupportingPointCount: 0,
        sourceUrl: "",
        sourceSnapshotAt: "",
        matchedAt: "2026-09-07 16:58:14.615",
        companies: [
          { companyId: "5595421834", companyName: "Nordvind AB" },
        ],
      },
    ]);
    expect(result.stats.ambiguous).toBe(4);
    // Unsearched, the total is the filter's own counter -- no second count.
    expect(result.total).toBe(4);
    expect(result.page).toBe(1);
    expect(result.pageSize).toBe(25);
  });

  it("searches the published line, postcode, town and key", async () => {
    const result = await searchAddressQualityQueue({
      filter: "ambiguous",
      query: "Ramvik",
      page: 1,
      pageSize: 25,
    });

    const search = sqlOf("count() AS total");
    expect(search).toContain(
      "positionCaseInsensitiveUTF8(\n    address.normalized_address,\n    {query:String}\n  ) > 0",
    );
    expect(search).toContain(
      "positionCaseInsensitiveUTF8(ifNull(address.postal_code, ''), {query:String}) > 0",
    );
    expect(search).toContain(
      "positionCaseInsensitiveUTF8(ifNull(address.city, ''), {query:String}) > 0",
    );
    expect(search).toContain("toString(address.address_key) = {query:String}");
    // A searched queue counts its own matches instead of the filter tile.
    expect(result.total).toBe(2);
  });
});
