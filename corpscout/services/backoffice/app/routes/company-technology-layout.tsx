import {
  Outlet,
  useLocation,
  useNavigate,
  useSearchParams,
} from "react-router";
import type { Route } from "./+types/company-technology-layout";
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
import { TechnologySectionTabs } from "~/components/detail/technology-section-tabs";
import {
  technologySectionFromPath,
  technologyTabSupported,
} from "~/lib/company-tabs";
import { getCountry } from "~/lib/countries";
import { getCompanyDomains } from "~/lib/queries.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country || !technologyTabSupported(country.code)) {
    throw new Response("Not found", { status: 404 });
  }
  const domains = await getCompanyDomains(country, params.id);
  if (domains.length === 0) {
    throw new Response("Technology information not found", { status: 404 });
  }
  const requestedDomain = new URL(request.url).searchParams
    .get("domain")
    ?.trim()
    .toLowerCase();
  const selectedDomain =
    domains.find((domain) => domain.domain === requestedDomain) ??
    domains.find((domain) => domain.is_primary === 1) ??
    domains[0];
  return { domains, selectedDomain: selectedDomain.domain };
}

function reviewLabel(status?: string): string {
  if (status === "confirmed_primary") return "Confirmed primary";
  if (status === "confirmed_related") return "Confirmed related";
  if (status === "rejected") return "Rejected";
  return "Unreviewed";
}

export default function CompanyTechnologyLayout({
  loaderData,
  params,
}: Route.ComponentProps) {
  const location = useLocation();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const section = technologySectionFromPath(location.pathname);
  const basePath = `/company/${params.country}/${params.id}/technology`;
  const selected = loaderData.domains.find(
    (domain) => domain.domain === loaderData.selectedDomain,
  );
  const domainSearch = `?domain=${encodeURIComponent(loaderData.selectedDomain)}`;

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
        <div>
          <h2 className="text-xl font-semibold tracking-tight">Technology</h2>
          <p className="text-muted-foreground mt-1 max-w-3xl text-sm">
            Technology, infrastructure, DNS, and IP information belongs to the
            selected domain. Company association status is shown separately.
          </p>
        </div>
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
      </div>

      <TechnologySectionTabs
        basePath={basePath}
        section={section}
        search={domainSearch}
      />

      <Outlet />
    </div>
  );
}
