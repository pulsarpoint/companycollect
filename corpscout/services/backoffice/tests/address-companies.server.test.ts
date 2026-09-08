import { describe, expect, it, vi } from "vitest";
import { getSwedenCompaniesAtSameBuilding } from "~/lib/address-companies.server";

/**
 * The lookup composes ONE query and maps its rows, so both halves are checked
 * through a mocked ClickHouse client -- the SQL the module actually sends and
 * what it makes of the answer -- rather than by reading the module's source
 * text, which passes just as happily when the query never runs.
 *
 * The module is re-imported against the mock per call so the live test below,
 * which runs the real query against ClickHouse, keeps its real client.
 */
async function lookupWithMockedClient(
  rows: unknown[],
  id = "5560000001",
): Promise<{
  companies: { company_id: string }[];
  truncated: boolean;
  calls: [string, Record<string, unknown>][];
}> {
  const query = vi.fn().mockResolvedValue(rows);
  vi.doMock("~/lib/clickhouse.server", () => ({ chQuery: query }));
  vi.resetModules();
  try {
    const module = await import("~/lib/address-companies.server");
    const result = await module.getSwedenCompaniesAtSameBuilding(id);
    return {
      companies: result.companies,
      truncated: result.truncated,
      calls: query.mock.calls as [string, Record<string, unknown>][],
    };
  } finally {
    vi.doUnmock("~/lib/clickhouse.server");
    vi.resetModules();
  }
}

function companyRow(index: number): { company_id: string; company_name: string; status: string } {
  return {
    company_id: `55600000${String(index).padStart(2, "0")}`,
    company_name: `Neighbour ${index} AB`,
    status: "active",
  };
}

describe("getSwedenCompaniesAtSameBuilding", () => {
  it("matches the parsed components of the published address entity", async () => {
    const { calls } = await lookupWithMockedClient([]);

    expect(calls).toHaveLength(1);
    const [sql, params] = calls[0];
    expect(params).toEqual({ id: "5560000001" });
    // The entity, named through the module's one constant: the target pick and
    // the neighbour scan are the only two reads, and slice 4b's rename is one
    // edit.
    expect(sql.match(/corpscout\.se_company_address AS/g)).toHaveLength(2);
    expect(sql).not.toContain("se_company_address_v2");
    for (const retired of [
      "se_addresses_current",
      "se_address_geocodes_current",
      "se_company_address_links_current",
    ]) {
      expect(sql).not.toContain(retired);
    }
    // The target is the company's primary published address that names a
    // building: visiting before postal, an exact building before a fallback,
    // and never a box (a PO box identifies no building).
    expect(sql).toContain("has(target.kinds, 'visiting_or_postal') DESC");
    expect(sql).toContain("has(target.kinds, 'visiting') DESC");
    expect(sql).toContain("target.geocode_precision = 'building' DESC");
    expect(sql).toContain("AND ifNull(target.box, '') = ''");
    expect(sql).toContain("AND ifNull(target.street_name, '') != ''");
    // The target CTE is EMPTY for every company that names no building (all
    // foreign, box only, postcode only, addressless). A scalar subquery over an
    // empty set must therefore have a Nullable result type: ClickHouse 26.5
    // raises INCORRECT_RESULT_OF_SCALAR_SUBQUERY for the LowCardinality
    // country_code otherwise, and the detail page 500s.
    expect(sql).toContain(
      "CAST(toString(target.country_code), 'Nullable(String)') AS country_code",
    );
    for (const column of ["street_name", "house_number", "postal_code"]) {
      expect(sql).toContain(
        `CAST(ifNull(target.${column}, ''), 'Nullable(String)') AS ${column}`,
      );
    }
    // Published rows only, on both sides of the lookup.
    expect(sql.match(/active = 1/g)).toHaveLength(2);
    // Components, not a rebuilt street key -- and the floor is ignored, so the
    // regexes the old free-text line needed are gone.
    expect(sql).toContain(
      "AND ifNull(street_name, '') = (SELECT street_name FROM target_address)",
    );
    expect(sql).toContain(
      "AND ifNull(house_number, '') = (SELECT house_number FROM target_address)",
    );
    expect(sql).toContain(
      "AND ifNull(postal_code, '') = (SELECT postal_code FROM target_address)",
    );
    expect(sql).not.toContain("ifNull(unit, '')");
    expect(sql).not.toContain("replaceRegexpAll");
    // The company asked about is never one of its own neighbours.
    expect(sql).toContain("AND registration_number != {id:String}");
  });

  it("maps the answer and flags a page it had to cut", async () => {
    const under = await lookupWithMockedClient(
      Array.from({ length: 50 }, (_, index) => companyRow(index)),
    );
    expect(under.companies).toHaveLength(50);
    expect(under.truncated).toBe(false);

    // The query asks for 51 so a full page can be told from an overflowing
    // one; the 51st row is the signal, not a company to show.
    const over = await lookupWithMockedClient(
      Array.from({ length: 51 }, (_, index) => companyRow(index)),
    );
    expect(over.companies).toHaveLength(50);
    expect(over.truncated).toBe(true);
    expect(over.calls[0][0]).toContain("LIMIT 51");
  });

  it("answers empty for a company whose addresses name no building", async () => {
    // 5565007076 is published with ONE address, a c/o box line: the target CTE
    // is empty, and before the Nullable cast ClickHouse 26.5 answered the whole
    // query with "Scalar subquery returned empty result of type
    // LowCardinality(String) which cannot be Nullable" -- a 500 on the company
    // detail page rather than an empty Same-building card.
    const result = await getSwedenCompaniesAtSameBuilding("5565007076");

    expect(result.companies).toEqual([]);
    expect(result.truncated).toBe(false);
  }, 30_000);

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
