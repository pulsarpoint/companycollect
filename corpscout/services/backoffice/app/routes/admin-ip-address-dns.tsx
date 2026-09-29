import { isRouteErrorResponse } from "react-router";
import type { Route } from "./+types/admin-ip-address-dns";
import { IpDnsRecordsView, IpTabError } from "~/components/admin/ip-address-detail";
import { parseAfter } from "~/lib/ip-address-detail";
import { getIpAddressDnsRecords, resolveCanonicalIpAddress } from "~/lib/ip-address-detail.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const address = await resolveCanonicalIpAddress(params.address);
  if (!address) return null; // the parent loader redirects to the canonical address
  return getIpAddressDnsRecords(address, {
    after: parseAfter(new URL(request.url).searchParams.get("after")),
  });
}

export default function IpAddressDnsPage({ loaderData }: Route.ComponentProps) {
  return loaderData ? <IpDnsRecordsView data={loaderData} /> : null;
}

export function ErrorBoundary({ error }: Route.ErrorBoundaryProps) {
  const status = isRouteErrorResponse(error) ? error.status : null;
  return <IpTabError status={status} message="This tab could not be loaded." />;
}
