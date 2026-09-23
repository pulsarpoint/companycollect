import type { Route } from "./+types/admin-se-domain-infrastructure";
import { TechnologyInfrastructureSection } from "~/components/detail/technology-infrastructure-section";
import { getDomainTechnologyInfrastructure } from "~/lib/queries.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const search = new URL(request.url).searchParams;
  return getDomainTechnologyInfrastructure(params.domain.trim().toLowerCase(), {
    page: Number(search.get("page") ?? "1"),
    pageSize: Number(search.get("pageSize") ?? "50"),
  });
}

export default function DomainInfrastructure({
  loaderData,
}: Route.ComponentProps) {
  return (
    <TechnologyInfrastructureSection
      infrastructure={loaderData}
      ipAddressesPath="../ip-addresses"
    />
  );
}
