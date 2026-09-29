import { isRouteErrorResponse } from "react-router";
import type { Route } from "./+types/admin-ip-address-registration";
import { IpRegistrationView, IpTabError } from "~/components/admin/ip-address-detail";
import { getIpAddressRegistration, resolveCanonicalIpAddress } from "~/lib/ip-address-detail.server";

export async function loader({ params }: Route.LoaderArgs) {
  const address = await resolveCanonicalIpAddress(params.address);
  if (!address) return null; // the parent loader redirects to the canonical address
  return getIpAddressRegistration(address);
}

export default function IpAddressRegistrationPage({ loaderData }: Route.ComponentProps) {
  return loaderData ? <IpRegistrationView data={loaderData} /> : null;
}

export function ErrorBoundary({ error }: Route.ErrorBoundaryProps) {
  const status = isRouteErrorResponse(error) ? error.status : null;
  return <IpTabError status={status} message="This tab could not be loaded." />;
}
