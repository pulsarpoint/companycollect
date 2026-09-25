import type { Route } from "./+types/admin-se-domain-technology";
import { TechnologyOverview } from "~/components/detail/technology-overview";
import { getDomainTechnologyDetail } from "~/lib/queries.server";

export async function loader({ params }: Route.LoaderArgs) {
  return getDomainTechnologyDetail(params.domain.trim().toLowerCase());
}

export default function DomainTechnology({
  loaderData,
  params,
}: Route.ComponentProps) {
  return <TechnologyOverview domain={params.domain.trim().toLowerCase()} data={loaderData} />;
}
