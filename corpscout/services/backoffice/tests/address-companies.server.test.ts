import { describe, expect, it } from "vitest";
import { getSwedenCompaniesAtSameBuilding } from "~/lib/address-companies.server";

describe("getSwedenCompaniesAtSameBuilding", () => {
  it("finds other registrations in the same building while ignoring floor", async () => {
    const result = await getSwedenCompaniesAtSameBuilding("8024123872");
    const ids = result.companies.map((company) => company.company_id);

    expect(ids).not.toContain("8024123872");
    expect(ids).toEqual(
      expect.arrayContaining(["8025035497", "9698001493", "9697518182"]),
    );
    expect(result.truncated).toBe(false);
  }, 30_000);
});
