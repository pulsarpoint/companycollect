import { data } from "react-router";
import type { Route } from "./+types/admin-ip-address-dns-history";
import { DNS_HISTORY_MAX_HOSTNAMES } from "~/lib/ip-address-detail";
import { getIpDnsRecordHistory, resolveCanonicalIpAddress } from "~/lib/ip-address-detail.server";

const ROOT_DOMAIN = /^[a-z0-9._-]{1,253}$/;

// Resource route (JSON, no UI): record-level history for one root domain of the DNS tab,
// loaded with useFetcher when its row is expanded. Read failures come back as
// { records: null } so the tab keeps rendering.
export async function loader({ params, request }: Route.LoaderArgs) {
  const address = await resolveCanonicalIpAddress(params.address);
  const rootDomain = params.rootDomain.trim().toLowerCase();
  if (!address || !ROOT_DOMAIN.test(rootDomain)) {
    throw new Response("Not found", { status: 404 });
  }
  const hostnames = new URL(request.url).searchParams
    .getAll("h")
    .slice(0, DNS_HISTORY_MAX_HOSTNAMES + 1);
  try {
    return { records: await getIpDnsRecordHistory(address, rootDomain, hostnames) };
  } catch (error) {
    console.error("IP DNS record history failed", rootDomain, error);
    return data({ records: null }, { status: 200 });
  }
}
