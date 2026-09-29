import type { Route } from "./+types/admin-ip-address-domains";
import { IpDomainsView } from "~/components/admin/ip-address-detail";
import { parseDomainScope, parsePage } from "~/lib/ip-address-detail";
import { getIpAddressDomains, resolveIpAddress } from "~/lib/ip-address-detail.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const address = await resolveIpAddress(params.address);
  if (!address) throw new Response("Not found", { status: 404 });
  const search = new URL(request.url).searchParams;
  return getIpAddressDomains(address, {
    page: parsePage(search.get("page")),
    scope: parseDomainScope(search.get("scope")),
  });
}

export default function IpAddressDomainsPage({ loaderData }: Route.ComponentProps) {
  return <IpDomainsView data={loaderData} />;
}
