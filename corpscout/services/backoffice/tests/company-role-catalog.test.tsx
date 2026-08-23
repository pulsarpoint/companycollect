import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { CompanyRoleCatalog } from "~/components/admin/company-role-catalog";
import type { CompanyPersonRoleType } from "~/lib/company-roles.server";

const roles: CompanyPersonRoleType[] = [
  {
    role_code: "board_chair",
    display_name: "Board chair",
    role_group: "governance",
    description: "Chair of the company board.",
    is_active: 1,
    created_at: "2026-08-19 00:00:00.000",
    updated_at: "2026-08-19 00:00:00.000",
  },
  {
    role_code: "auditor",
    display_name: "Auditor",
    role_group: "audit",
    description: "Person serving as an auditor of the company.",
    is_active: 0,
    created_at: "2026-08-19 00:00:00.000",
    updated_at: "2026-08-20 12:30:00.000",
  },
];

describe("CompanyRoleCatalog", () => {
  it("renders the canonical role pool and its summary", () => {
    const html = renderToStaticMarkup(<CompanyRoleCatalog roles={roles} />);

    expect(html).toContain("Canonical company roles");
    expect(html).toContain("corpscout.company_person_role_type");
    expect(html).toContain("Board chair");
    expect(html).toContain("board_chair");
    expect(html).toContain("Governance");
    expect(html).toContain("Inactive");
    expect(html).not.toContain("Source mappings");
  });

  it("renders an explicit empty state when the role pool has no rows", () => {
    const html = renderToStaticMarkup(<CompanyRoleCatalog roles={[]} />);

    expect(html).toContain("No canonical roles found");
  });
});
