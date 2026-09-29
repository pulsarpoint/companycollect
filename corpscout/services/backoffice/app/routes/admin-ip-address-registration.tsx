import type { Route } from "./+types/admin-ip-address-registration";
import { IpRegistrationView } from "~/components/admin/ip-address-detail";
import { getIpAddressRegistration, resolveIpAddress } from "~/lib/ip-address-detail.server";

export async function loader({ params }: Route.LoaderArgs) {
  const address = await resolveIpAddress(params.address);
  if (!address) throw new Response("Not found", { status: 404 });
  return getIpAddressRegistration(address);
}

export default function IpAddressRegistrationPage({ loaderData }: Route.ComponentProps) {
  return <IpRegistrationView data={loaderData} />;
}
