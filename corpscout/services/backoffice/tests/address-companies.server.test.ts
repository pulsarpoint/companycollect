import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { getSwedenCompaniesAtSameBuilding } from "~/lib/address-companies.server";

const lookupServer = readFileSync(
  new URL("../app/lib/address-companies.server.ts", import.meta.url),
  "utf8",
);

describe("getSwedenCompaniesAtSameBuilding", () => {
  it("matches the parsed components of the published address entity", () => {
    expect(lookupServer).toContain(
      'const ADDRESS_TABLE = "corpscout.se_company_address_v2"',
    );
    for (const retired of [
      "se_addresses_current",
      "se_address_geocodes_current",
      "se_company_address_links_current",
    ]) {
      expect(lookupServer).not.toContain(retired);
    }
    // The target is the company's primary published address that names a
    // building: visiting before postal, an exact building before a fallback.
    expect(lookupServer).toContain("has(target.kinds, 'visiting_or_postal') DESC");
    expect(lookupServer).toContain("has(target.kinds, 'visiting') DESC");
    expect(lookupServer).toContain("target.geocode_precision = 'building' DESC");
    expect(lookupServer).toContain("AND ifNull(target.box, '') = ''");
    // Components, not a rebuilt street key -- and the floor is ignored, so the
    // regexes the old free-text line needed are gone.
    expect(lookupServer).toContain(
      "AND ifNull(street_name, '') = (SELECT street_name FROM target_address)",
    );
    expect(lookupServer).toContain(
      "AND ifNull(house_number, '') = (SELECT house_number FROM target_address)",
    );
    expect(lookupServer).not.toContain("ifNull(unit, '')");
    expect(lookupServer).not.toContain("replaceRegexpAll");
  });

  it("finds other registrations in the same building while ignoring floor", async () => {
    const result = await getSwedenCompaniesAtSameBuilding("8024123872");
    const ids = result.companies.map((company) => company.company_id);

    expect(ids).not.toContain("8024123872");
    expect(ids).toEqual(
      expect.arrayContaining(["8025035497", "9697518182"]),
    );
    expect(result.truncated).toBe(false);
  }, 30_000);
});
