import type { Route } from "./+types/admin-ip-address-overview";
import { IpOverviewView } from "~/components/admin/ip-address-detail";
import { getIpAddressOverview, resolveIpAddress } from "~/lib/ip-address-detail.server";

export async function loader({ params }: Route.LoaderArgs) {
  const address = await resolveIpAddress(params.address);
  if (!address) throw new Response("Not found", { status: 404 });
  return getIpAddressOverview(address);
}

export default function IpAddressOverviewPage({ loaderData }: Route.ComponentProps) {
  return <IpOverviewView data={loaderData} />;
}
