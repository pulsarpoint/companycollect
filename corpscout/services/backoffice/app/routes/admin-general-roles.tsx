import type { Route } from "./+types/admin-general-roles";
import { CompanyRoleCatalog } from "~/components/admin/company-role-catalog";
import { getCompanyPersonRoleTypes } from "~/lib/company-roles.server";

export async function loader() {
  return { roles: await getCompanyPersonRoleTypes() };
}

export function meta() {
  return [{ title: "Canonical company roles | CompanyCollect" }];
}

export default function AdminGeneralRoles({ loaderData }: Route.ComponentProps) {
  return <CompanyRoleCatalog roles={loaderData.roles} />;
}
