import {
  isRouteErrorResponse,
  Link,
  Outlet,
  redirect,
  useLocation,
  useNavigation,
  useParams,
} from "react-router";
import type { Route } from "./+types/admin-ip-address";
import { formatObservedAt } from "~/components/detail/technology-infrastructure-section";
import { IpDetailTabs, IpTabError, LookupStatusBadge } from "~/components/admin/ip-address-detail";
import { Badge } from "~/components/ui/badge";
import { buttonVariants } from "~/components/ui/button";
import { ipAddressDetailHref, ipDetailTabFromPath } from "~/lib/ip-address-detail";
import { getIpAddressHeader, resolveIpAddress } from "~/lib/ip-address-detail.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const address = await resolveIpAddress(params.address);
  if (!address) throw new Response("Not found", { status: 404 });
  if (address.ip !== params.address) {
    // Serve one URL per address: redirect to the canonical form, keeping the tab and query.
    const url = new URL(request.url);
    const rest = url.pathname.split("/").slice(4).join("/");
    throw redirect(`${ipAddressDetailHref(address.ip)}${rest ? `/${rest}` : ""}${url.search}`);
  }
  return getIpAddressHeader(address);
}

export function meta({ loaderData }: Route.MetaArgs) {
  return [{ title: `${loaderData?.address.ip ?? "IP address"} · IP addresses | CompanyCollect` }];
}

export default function AdminIpAddress({ loaderData }: Route.ComponentProps) {
  const { address, firstSeen, lastSeen, statuses } = loaderData;
  const location = useLocation();
  const navigation = useNavigation();
  const segmentLabel = address.version === 4 ? "/24" : "/48";
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6" aria-busy={navigation.state !== "idle"}>
      <header className="flex flex-wrap items-start justify-between gap-3">
        <div className="flex flex-col gap-2">
          <div className="flex flex-wrap items-center gap-2">
            <h1 className="break-all font-mono text-2xl font-semibold">{address.ip}</h1>
            <Badge variant="outline">IPv{address.version}</Badge>
          </div>
          <p className="text-muted-foreground text-sm">
            {segmentLabel} segment <span className="text-foreground font-mono">{address.networkSegment}</span>
            {" · "}
            {firstSeen
              ? `seen ${formatObservedAt(firstSeen)} to ${formatObservedAt(lastSeen)} (UTC)`
              : "not in the DNS address inventory"}
          </p>
          <div className="flex flex-wrap gap-2">
            {statuses ? (
              <>
                <LookupStatusBadge label="City" status={statuses.city} />
                <LookupStatusBadge label="ASN" status={statuses.asn} />
                <LookupStatusBadge label="RDAP" status={statuses.rdap} />
              </>
            ) : (
              <Badge variant="outline">Not enriched yet</Badge>
            )}
          </div>
        </div>
        <Link className={buttonVariants({ variant: "ghost", size: "sm" })} to="/admin/ip-addresses">
          All IP addresses
        </Link>
      </header>
      <IpDetailTabs ip={address.ip} tab={ipDetailTabFromPath(location.pathname)} />
      <Outlet />
    </div>
  );
}

export function ErrorBoundary({ error }: Route.ErrorBoundaryProps) {
  const params = useParams();
  const status = isRouteErrorResponse(error) ? error.status : null;
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-wrap items-start justify-between gap-3">
        <h1 className="break-all font-mono text-2xl font-semibold">{params.address ?? "IP address"}</h1>
        <Link className={buttonVariants({ variant: "ghost", size: "sm" })} to="/admin/ip-addresses">
          All IP addresses
        </Link>
      </header>
      <IpTabError
        status={status}
        message={status === 404 ? "This is not a valid IP address." : "The page could not be loaded."}
      />
    </div>
  );
}
