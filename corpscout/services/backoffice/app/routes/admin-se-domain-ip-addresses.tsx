import type { Route } from "./+types/admin-se-domain-ip-addresses";
import { TechnologyIpAddressesSection } from "~/components/detail/technology-ip-addresses-section";
import { getDomainTechnologyIpInventory } from "~/lib/queries.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const search = new URL(request.url).searchParams;
  return getDomainTechnologyIpInventory(params.domain.trim().toLowerCase(), {
    page: Number(search.get("page") ?? "1"),
    pageSize: Number(search.get("pageSize") ?? "25"),
  });
}

export default function DomainIpAddresses({
  loaderData,
}: Route.ComponentProps) {
  return <TechnologyIpAddressesSection inventory={loaderData} />;
}
