import { Link } from "react-router";
import { ArrowLeft } from "lucide-react";
import type { Route } from "./+types/country-company-detail";
import { getCountry } from "~/lib/countries";
import { getCompanyDetail } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  ContactsSection,
  DomainsSection,
  OverviewSection,
} from "~/components/detail/detail-sections";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });
  const detail = await getCompanyDetail(country, params.id);
  if (!detail) throw new Response("Company not found", { status: 404 });
  return { detail };
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  const name = loaderData?.detail.company.name;
  return [{ title: name ? `${name} – CompanyCollect Backoffice` : `Company ${params.id}` }];
}

export default function CompanyDetail({ loaderData, params }: Route.ComponentProps) {
  const { detail } = loaderData;
  const country = getCountry(params.country)!;
  const { company } = detail;
  const status = country.columns.find((c) => c.kind === "status");

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4">
      <div>
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2"
          nativeButton={false}
          render={<Link to={`/${country.code}/companies`} />}
        >
          <ArrowLeft className="size-4" />
          Companies
        </Button>
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <h2 className="text-2xl font-semibold">{String(company.name ?? "")}</h2>
        {status ? (
          <Badge variant={company.active ? "default" : "outline"}>
            {String(company[status.key] ?? (company.active ? "active" : "inactive"))}
          </Badge>
        ) : null}
        <span className="text-muted-foreground font-mono text-sm">
          {String(company.id)}
        </span>
      </div>

      <OverviewSection country={country} company={company} />
      <ContactsSection contacts={detail.contacts} />
      <DomainsSection domains={detail.domains} />
    </div>
  );
}
