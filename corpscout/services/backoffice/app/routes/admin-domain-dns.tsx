import type { Route } from "./+types/admin-domain-dns";
import { DomainDnsRecords } from "~/components/admin/domain-dns-records";
import { getDomainDns } from "~/lib/domain-dns.server";
import { parseDnsFilters } from "~/lib/domain-dns";

export function loader({ params, request }: Route.LoaderArgs) {
  return getDomainDns(params.domain.trim().toLowerCase(), parseDnsFilters(new URL(request.url).searchParams));
}

export default function DomainDnsPage({ loaderData }: Route.ComponentProps) {
  return <DomainDnsRecords data={loaderData} />;
}
