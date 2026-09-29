import { isRouteErrorResponse } from "react-router";
import type { Route } from "./+types/admin-ip-address-overview";
import { IpOverviewView, IpTabError } from "~/components/admin/ip-address-detail";
import { getIpAddressOverview, resolveCanonicalIpAddress } from "~/lib/ip-address-detail.server";

export async function loader({ params }: Route.LoaderArgs) {
  const address = await resolveCanonicalIpAddress(params.address);
  if (!address) return null; // the parent loader redirects to the canonical address
  return getIpAddressOverview(address);
}

export default function IpAddressOverviewPage({ loaderData }: Route.ComponentProps) {
  return loaderData ? <IpOverviewView data={loaderData} /> : null;
}

export function ErrorBoundary({ error }: Route.ErrorBoundaryProps) {
  const status = isRouteErrorResponse(error) ? error.status : null;
  return <IpTabError status={status} message="This tab could not be loaded." />;
}
