import { describe, expect, it } from "vitest";
import { getCompanyPersonRoleTypes } from "~/lib/company-roles.server";

describe("canonical company role types", () => {
  it("reads the controlled role vocabulary directly from ClickHouse", async () => {
    const roles = await getCompanyPersonRoleTypes();
    const roleCodes = roles.map((role) => role.role_code);

    expect(roles.length).toBeGreaterThan(0);
    expect(new Set(roleCodes).size).toBe(roleCodes.length);
    expect(roleCodes).toContain("board_member");
    expect(roleCodes).toContain("chief_executive_officer");
    expect(roleCodes).toContain("auditor");
    expect(roles.every((role) => role.display_name.length > 0)).toBe(true);
  });
});
