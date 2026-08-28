import {
  NavLink,
  Outlet,
  useLocation,
  useNavigate,
  useSearchParams,
} from "react-router";
import type { Route } from "./+types/admin-se-company-technology-layout";
import { Badge } from "~/components/ui/badge";
import { Field, FieldLabel } from "~/components/ui/field";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import { technologySectionFromPath } from "~/lib/company-tabs";
import { getCountry } from "~/lib/countries";
import { getCompanyDomains } from "~/lib/queries.server";

// Only `loader` and the component live here -- see
// admin-se-company-layout.tsx for why.

// The admin twin of company-technology-layout.tsx: the same domain selector
// and section tabs over the same loader query, but on the admin base path and
// without the public layout's 404s -- a reviewer needs the tab shell to
// render even when no source has resolved a domain yet. The public file
// stays byte-identical; only the shared helpers are reused.

export async function loader({ params, request }: Route.LoaderArgs) {
  const domains = await getCompanyDomains(getCountry("se")!, params.companyId);
  const requestedDomain = new URL(request.url).searchParams
    .get("domain")
    ?.trim()
    .toLowerCase();
  const selectedDomain =
    domains.find((domain) => domain.domain === requestedDomain) ??
    domains.find((domain) => domain.is_primary === 1) ??
    domains[0];
  return { domains, selectedDomain: selectedDomain?.domain ?? "" };
}

function reviewLabel(status?: string): string {
  if (status === "confirmed_primary") return "Confirmed primary";
  if (status === "confirmed_related") return "Confirmed related";
  if (status === "rejected") return "Rejected";
  return "Unreviewed";
}

export default function AdminSwedenCompanyTechnologyLayout({
  loaderData,
  params,
}: Route.ComponentProps) {
  const location = useLocation();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const section = technologySectionFromPath(location.pathname);
  const basePath = `/admin/se/company/${params.companyId}/technology`;
  const selected = loaderData.domains.find(
    (domain) => domain.domain === loaderData.selectedDomain,
  );
  const domainSearch = loaderData.selectedDomain
    ? `?domain=${encodeURIComponent(loaderData.selectedDomain)}`
    : "";

  function selectDomain(value: string | null) {
    if (!value) return;
    const next = new URLSearchParams(searchParams);
    next.set("domain", value);
    next.delete("page");
    next.delete("exactPage");
    next.delete("segmentPage");
    const destination = location.pathname.startsWith(
      `${basePath}/ip-addresses/`,
    )
      ? `${basePath}/ip-addresses`
      : location.pathname;
    navigate(`${destination}?${next.toString()}`, {
      preventScrollReset: true,
    });
  }

  return (
    <div className="flex w-full flex-col gap-5">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <p className="text-muted-foreground max-w-3xl text-sm">
          Technology, infrastructure, DNS, and IP information belongs to the
          selected domain. Company association status is shown separately.
        </p>
        {loaderData.domains.length > 0 ? (
          <Field className="w-full max-w-sm">
            <FieldLabel>Associated domain</FieldLabel>
            <div className="flex items-center gap-2">
              <Select
                value={loaderData.selectedDomain}
                onValueChange={selectDomain}
              >
                <SelectTrigger className="min-w-64">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    <SelectLabel>Company domains</SelectLabel>
                    {loaderData.domains.map((domain) => (
                      <SelectItem key={domain.domain} value={domain.domain}>
                        {domain.domain}
                      </SelectItem>
                    ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
              <Badge
                variant={
                  selected?.review_status === "rejected"
                    ? "destructive"
                    : "outline"
                }
              >
                {reviewLabel(selected?.review_status)}
              </Badge>
            </div>
          </Field>
        ) : null}
      </div>

      <Tabs value={section}>
        <TabsList>
          <TabsTrigger
            value="overview"
            render={<NavLink to={`${basePath}${domainSearch}`} end />}
            nativeButton={false}
          >
            Overview
          </TabsTrigger>
          <TabsTrigger
            value="web-intelligence"
            render={
              <NavLink to={`${basePath}/web-intelligence${domainSearch}`} />
            }
            nativeButton={false}
          >
            Web intelligence
          </TabsTrigger>
          <TabsTrigger
            value="infrastructure"
            render={
              <NavLink to={`${basePath}/infrastructure${domainSearch}`} />
            }
            nativeButton={false}
          >
            Infrastructure
          </TabsTrigger>
          <TabsTrigger
            value="ip-addresses"
            render={<NavLink to={`${basePath}/ip-addresses${domainSearch}`} />}
            nativeButton={false}
          >
            IP addresses
          </TabsTrigger>
        </TabsList>
      </Tabs>

      <Outlet />
    </div>
  );
}
