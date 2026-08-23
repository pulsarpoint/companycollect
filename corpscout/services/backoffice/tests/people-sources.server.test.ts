import { describe, expect, it } from "vitest";
import {
  getSwedenPeopleSourceRows,
  parseSwedenCompanyIdFilter,
} from "~/lib/people-sources.server";

describe("Sweden people source rows", () => {
  it("normalizes standard Swedish company ID formatting", () => {
    expect(parseSwedenCompanyIdFilter(" 556520-0028 ")).toEqual({
      input: "556520-0028",
      companyId: "5565200028",
      error: "",
    });
    expect(parseSwedenCompanyIdFilter("SE5565200028").error).toContain(
      "10-digit",
    );
  });

  it("reads Sagax signatories directly from the Bolagsverket source table", async () => {
    const result = await getSwedenPeopleSourceRows(
      "bolagsverket",
      "556520-0028",
    );

    expect(result.source).toBe("bolagsverket");
    expect(result.rows.length).toBeGreaterThan(0);
    expect(result.rows.every((row) => row.company_id === "5565200028")).toBe(
      true,
    );
    expect(result.rows.some((row) => row.last_name === "Mindus")).toBe(true);
  });

  it("reads Sagax people directly from normalized ESEF observations", async () => {
    const result = await getSwedenPeopleSourceRows("esef", "5565200028");

    expect(result.source).toBe("esef");
    expect(result.rows.length).toBeGreaterThan(1);
    expect(result.rows.every((row) => row.company_id === "5565200028")).toBe(
      true,
    );
    expect(result.rows.some((row) => row.name === "David Mindus")).toBe(true);
  });

  it("bridges Wikidata QIDs back to a Swedish company ID", async () => {
    const result = await getSwedenPeopleSourceRows("wikidata", "5590701545");

    expect(result.source).toBe("wikidata");
    expect(result.rows.length).toBeGreaterThan(0);
    expect(result.rows.every((row) => row.company_id === "5590701545")).toBe(
      true,
    );
    expect(result.rows.some((row) => row.person_wikidata_id.startsWith("Q"))).toBe(
      true,
    );
  });
});
