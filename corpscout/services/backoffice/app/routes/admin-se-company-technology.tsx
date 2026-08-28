import { CpuIcon } from "lucide-react";
import { Link, useNavigate, useSearchParams } from "react-router";
import type { Route } from "./+types/admin-se-company-technology";
import { TechnologyDomainsSection } from "~/components/detail/technology-domains-section";
import { WebTechnologyHistorySection } from "~/components/detail/web-technology-history-section";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
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
import { getCountry } from "~/lib/countries";
import { getCompanyTechnologyDetail } from "~/lib/queries.server";

// Only `loader` and the component live here -- see
// admin-se-company-layout.tsx for why.

// The tab is the public technology experience, not an admin re-reading of the
// raw tables: it loads through the same query the public
// /company/se/:id/technology page uses and renders the same shared sections,
// deep-linking into the public web-intelligence/infrastructure/IP readers.
// Unlike the public page it never 404s on an empty result -- an admin
// reviewing a company needs the tab to say "nothing yet", not vanish.

export async function loader({ params, request }: Route.LoaderArgs) {
  return getCompanyTechnologyDetail(
    getCountry("se")!,
    params.companyId,
    new URL(request.url).searchParams.get("domain") ?? undefined,
  );
}

export default function AdminSwedenCompanyTechnology({
  loaderData,
  params,
}: Route.ComponentProps) {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { domains, selectedDomain, webTechnologyHistory } = loaderData;

  if (domains.length === 0) {
    return (
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <CpuIcon />
          </EmptyMedia>
          <EmptyTitle>No technology data for this company yet</EmptyTitle>
          <EmptyDescription>
            Technology detection is scoped to a company&apos;s domains, and no
            source has suggested a domain for this company.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }

  const publicBase = `/company/se/${params.companyId}/technology`;
  const domainSearch = selectedDomain
    ? `?domain=${encodeURIComponent(selectedDomain)}`
    : "";

  function selectDomain(value: string | null) {
    if (!value) return;
    const next = new URLSearchParams(searchParams);
    next.set("domain", value);
    navigate(`?${next.toString()}`, { preventScrollReset: true });
  }

  return (
    <div className="flex flex-col gap-5">
      {domains.length > 1 ? (
        <Field className="w-full max-w-sm">
          <FieldLabel>Associated domain</FieldLabel>
          <Select value={selectedDomain} onValueChange={selectDomain}>
            <SelectTrigger className="min-w-64">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                <SelectLabel>Company domains</SelectLabel>
                {domains.map((domain) => (
                  <SelectItem key={domain.domain} value={domain.domain}>
                    {domain.domain}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
        </Field>
      ) : null}
      <TechnologyDomainsSection
        domains={domains}
        selectedDomain={selectedDomain}
      />
      {webTechnologyHistory?.technologies.length ? (
        <WebTechnologyHistorySection history={webTechnologyHistory} />
      ) : null}
      <div className="text-muted-foreground flex flex-wrap items-center gap-x-4 gap-y-1 text-sm">
        <span>Public technology readers:</span>
        <Link
          className="underline underline-offset-2"
          to={`${publicBase}/web-intelligence${domainSearch}`}
        >
          Web intelligence
        </Link>
        <Link
          className="underline underline-offset-2"
          to={`${publicBase}/infrastructure${domainSearch}`}
        >
          Infrastructure
        </Link>
        <Link
          className="underline underline-offset-2"
          to={`${publicBase}/ip-addresses${domainSearch}`}
        >
          IP addresses
        </Link>
      </div>
    </div>
  );
}
