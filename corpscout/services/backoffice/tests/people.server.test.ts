import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import { getCountry } from "~/lib/countries";
import { getCountryPerson, searchCountryPeople } from "~/lib/people.server";

const ERIK_WESTMAN = "erik johan westman";
const AAK = "5566692850";

describe("country-scoped people", () => {
  it("searches person profiles instead of grouping globally by name", async () => {
    const result = await searchCountryPeople(ERIK_WESTMAN);
    const person = result.rows.find(
      (row) =>
        row.country_iso2 === "SE" &&
        row.preferred_name_normalized === ERIK_WESTMAN,
    );

    expect(person).toBeTruthy();
    expect(person!.person_id).toMatch(/^[0-9a-f-]{36}$/);
    expect(person!.resolution_status).toBe("provisional");
    expect(person!.observation_count).toBe(3);
    expect(person!.company_count).toBe(1);
  });

  it("returns the raw observations behind a combined profile", async () => {
    const search = await searchCountryPeople(ERIK_WESTMAN);
    const person = search.rows.find((row) => row.country_iso2 === "SE")!;
    const detail = await getCountryPerson("se", person.person_id);

    expect(detail).not.toBeNull();
    expect(detail!.person.person_id).toBe(person.person_id);
    expect(detail!.observations).toHaveLength(3);
    expect(detail!.observations.every((row) => row.company_id === AAK)).toBe(
      true,
    );
    expect(
      detail!.observations.every(
        (row) =>
          row.source === "se_xbrl_signatures" &&
          row.source_statement_key !== "" &&
          row.source_person_key !== "",
      ),
    ).toBe(true);
    expect(detail!.identifiers).toEqual([]);
  });

  it("rejects malformed country person ids before querying", async () => {
    await expect(
      getCountryPerson("se", "erik-johan-westman"),
    ).resolves.toBeNull();
  });

  it("links Sweden company officers to country person ids", async () => {
    const sweden = getCountry("se")!;
    const rows = await chQuery<{ person_id: string; country_iso2: string }>(
      sweden.detail!.officersQuery!,
      { id: AAK },
    );

    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((row) => row.country_iso2 === "SE")).toBe(true);
    expect(rows.every((row) => /^[0-9a-f-]{36}$/.test(row.person_id))).toBe(
      true,
    );
  });

  it("executes same-name candidate lookup against the identity layer", async () => {
    const sweden = getCountry("se")!;
    const rows = await chQuery(sweden.detail!.peopleMatchesQuery!, {
      id: AAK,
      names: [ERIK_WESTMAN],
    });

    expect(Array.isArray(rows)).toBe(true);
  });
});
