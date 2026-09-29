import { redirect } from "react-router";
import type { Route } from "./+types/admin-ip-address-index";
import { ipAddressDetailHref } from "~/lib/ip-address-detail";
import { resolveIpAddress } from "~/lib/ip-address-detail.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const address = await resolveIpAddress(params.address);
  if (!address) throw new Response("Not found", { status: 404 });
  return redirect(`${ipAddressDetailHref(address.ip, "overview")}${new URL(request.url).search}`);
}
