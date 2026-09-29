import { isRouteErrorResponse } from "react-router";
import type { Route } from "./+types/admin-ip-address-domains";
import { IpDomainsView, IpTabError } from "~/components/admin/ip-address-detail";
import { parseAfter, parseDomainScope } from "~/lib/ip-address-detail";
import { getIpAddressDomains, resolveCanonicalIpAddress } from "~/lib/ip-address-detail.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const address = await resolveCanonicalIpAddress(params.address);
  if (!address) return null; // the parent loader redirects to the canonical address
  const search = new URL(request.url).searchParams;
  return getIpAddressDomains(address, {
    after: parseAfter(search.get("after")),
    scope: parseDomainScope(search.get("scope")),
  });
}

export default function IpAddressDomainsPage({ loaderData }: Route.ComponentProps) {
  return loaderData ? <IpDomainsView data={loaderData} /> : null;
}

export function ErrorBoundary({ error }: Route.ErrorBoundaryProps) {
  const status = isRouteErrorResponse(error) ? error.status : null;
  return <IpTabError status={status} message="This tab could not be loaded." />;
}
