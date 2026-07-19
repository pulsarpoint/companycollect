import type { ComponentType } from "react";
import { Link } from "react-router";
import { ArrowLeft } from "lucide-react";
import type { Route } from "./+types/country-company-detail";
import { getCountry } from "~/lib/countries";
import { getCompanyDetail } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  CompanyRecordSection,
  DomainsSection,
} from "~/components/detail/detail-sections";
import { ContactLocationCard } from "~/components/detail/contact-location-card";
import { FinancialsSection } from "~/components/detail/financials-section";
import { IndustriesSection } from "~/components/detail/industries-section";
import { NoFinancialsSection, StatementsFallback } from "~/components/detail/countries/no-financials";
import { decorateFiRecord, FiRegistryBadges } from "~/components/detail/countries/fi-registry";
import { LangToggle } from "~/components/detail/lang-toggle";
import { resolveRecordFields, type Lang } from "~/components/detail/language";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";

const COUNTRY_FINANCIALS: Record<
  string,
  ComponentType<{ statements: Record<string, unknown>[] }>
> = {
  no: NoFinancialsSection,
};

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
  const searchParams = useEffectiveSearchParams();
  const lang: Lang = searchParams.get("lang") === "original" ? "original" : "en";
  const record = country.code === "fi" ? decorateFiRecord(detail.record) : detail.record;
  const { pairCount } = resolveRecordFields(record, lang);

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4">
      <div>
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2"
          nativeButton={false}
          render={<Link to="/companies" />}
        >
          <ArrowLeft className="size-4" />
          Companies
        </Button>
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <h2 className="text-2xl font-semibold">{String(company.name ?? "")}</h2>
        {status ? (
          <Badge variant={company.active ? "default" : "outline"}>
            {(() => {
              const v = company[status.key];
              const s = v == null ? "" : String(v);
              return s !== "" ? s : company.active ? "active" : "inactive";
            })()}
          </Badge>
        ) : null}
        <span className="text-muted-foreground font-mono text-sm">
          {String(company.id)}
        </span>
        {country.code === "fi" ? <FiRegistryBadges record={detail.record} /> : null}
        <LangToggle lang={lang} pairCount={pairCount} />
      </div>

      <CompanyRecordSection company={company} record={record} lang={lang} />
      <IndustriesSection industries={detail.industries} />
      {(() => {
        const Specific = COUNTRY_FINANCIALS[country.code];
        if (Specific) return <Specific statements={detail.statements} />;
        if (detail.statements.length > 0) return <StatementsFallback statements={detail.statements} />;
        return (
          <FinancialsSection
            financials={detail.financials}
            factsHref={
              country.detail?.factsQuery
                ? (year) => `/company/${country.code}/${params.id}/facts/${year}`
                : undefined
            }
          />
        );
      })()}
      <ContactLocationCard
        country={country}
        contacts={detail.contacts}
        addresses={detail.addresses}
        record={detail.record}
      />
      <DomainsSection domains={detail.domains} />
    </div>
  );
}
