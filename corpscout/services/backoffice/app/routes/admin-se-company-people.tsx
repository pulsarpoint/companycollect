import type { Route } from "./+types/admin-se-company-people";
import { SeCompanyPeopleTab } from "~/components/admin/se-company-people";
import {
  loadSeCompanyPeople,
  loadSeCompanyPeopleEvidence,
} from "~/lib/se-company-people.server";

// Only `loader`, `meta` and the component live here -- see
// admin-se-company-layout.tsx for why.

export async function loader({ params }: Route.LoaderArgs) {
  const [people, evidence] = await Promise.all([
    loadSeCompanyPeople(params.companyId),
    loadSeCompanyPeopleEvidence(params.companyId),
  ]);
  return { people, evidence };
}

export default function AdminSwedenCompanyPeople({
  loaderData,
  params,
}: Route.ComponentProps) {
  return (
    <SeCompanyPeopleTab
      companyId={params.companyId}
      people={loaderData.people}
      evidence={loaderData.evidence}
    />
  );
}
