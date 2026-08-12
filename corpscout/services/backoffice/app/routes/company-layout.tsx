import { Link, NavLink, Outlet, useLocation } from "react-router";
import { ArrowLeft } from "lucide-react";
import type { Route } from "./+types/company-layout";
import { decorateBrRecord } from "~/components/detail/countries/br-company";
import {
  decorateFiRecord,
  FiRegistryBadges,
} from "~/components/detail/countries/fi-registry";
import { LangToggle } from "~/components/detail/lang-toggle";
import { resolveRecordFields, type Lang } from "~/components/detail/language";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Skeleton } from "~/components/ui/skeleton";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import { getCountry } from "~/lib/countries";
import {
  companyTabFromPath,
  domainSuggestionsTabAvailable,
  domainSuggestionsTabSupported,
  technologyTabAvailable,
  technologyTabSupported,
} from "~/lib/company-tabs";
import { getEntityType, legalFormCodeOf } from "~/lib/entity-type.server";
import { getUnifiedCompanyDomains } from "~/lib/company-domains.server";
import { companyHasFlag, getCompanyShell } from "~/lib/queries.server";
import {
  companySectionPresencePromiseContext,
  companyShellPromiseContext,
} from "~/lib/company-shell-context.server";
import { getCompanySectionPresence } from "~/lib/company-sections.server";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";

export const middleware: Route.MiddlewareFunction[] = [
  async ({ context, params }, next) => {
    const country = getCountry(params.country);
    if (country) {
      context.set(
        companyShellPromiseContext,
        getCompanyShell(country, params.id),
      );
      if (country.code === "se") {
        context.set(
          companySectionPresencePromiseContext,
          getCompanySectionPresence("SE", params.id),
        );
      }
    }
    return next();
  },
];

export async function loader({ context, params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });
  const suggestionsSupported = domainSuggestionsTabSupported(country.code);
  const [shell, hasLegacyDomain, unifiedDomains] = await Promise.all([
    context.get(companyShellPromiseContext),
    country.code !== "se" && technologyTabSupported(country.code)
      ? companyHasFlag(country, params.id, "domain")
      : Promise.resolve(false),
    suggestionsSupported
      ? getUnifiedCompanyDomains(country.code, params.id)
      : Promise.resolve([]),
  ]);
  if (!shell) throw new Response("Company not found", { status: 404 });
  const entityType = await getEntityType(
    country.code,
    legalFormCodeOf(country.code, shell.record),
  );
  const hasDomain =
    hasLegacyDomain ||
    unifiedDomains.some(
      (domain) => domain.active && domain.reviewStatus !== "rejected",
    );
  return {
    shell: { company: shell.company, record: shell.record },
    entityType,
    technologyAvailable: technologyTabAvailable(country.code, hasDomain),
    domainSuggestionsAvailable: domainSuggestionsTabAvailable(
      country.code,
      unifiedDomains.length > 0,
    ),
  };
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  const name = loaderData?.shell.company.name;
  return [
    {
      title: name
        ? `${name} – CompanyCollect Backoffice`
        : `Company ${params.id}`,
    },
  ];
}

export function HydrateFallback() {
  return (
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-5">
      <Skeleton className="h-8 w-28" />
      <Skeleton className="h-16 w-full" />
      <Skeleton className="h-8 w-72" />
      <Skeleton className="h-96 w-full" />
    </div>
  );
}

export default function CompanyLayout({
  loaderData,
  params,
}: Route.ComponentProps) {
  const { shell, entityType, technologyAvailable, domainSuggestionsAvailable } =
    loaderData;
  const country = getCountry(params.country)!;
  const { company } = shell;
  const tab = companyTabFromPath(useLocation().pathname);
  const technologySupported = technologyTabSupported(country.code);
  const suggestionsSupported = domainSuggestionsTabSupported(country.code);
  const statusColumn = country.columns.find(
    (column) => column.kind === "status",
  );
  const searchParams = useEffectiveSearchParams();
  const lang: Lang =
    searchParams.get("lang") === "original" ? "original" : "en";
  const decorators: Record<
    string,
    (record: Record<string, unknown>) => Record<string, unknown>
  > = {
    fi: decorateFiRecord,
    br: decorateBrRecord,
  };
  const decoratedRecord = (decorators[country.code] ?? ((record) => record))(
    shell.record,
  );
  const { pairCount } = resolveRecordFields(decoratedRecord, lang);

  const statusValue = statusColumn ? company[statusColumn.key] : null;
  const statusLabel =
    statusValue == null || String(statusValue) === ""
      ? company.active
        ? "active"
        : "inactive"
      : String(statusValue);

  return (
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-5">
      <div>
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2"
          nativeButton={false}
          render={<Link to={`/countries/${country.code}/companies`} />}
        >
          <ArrowLeft data-icon="inline-start" />
          {country.name} companies
        </Button>
      </div>

      <header className="flex flex-wrap items-start justify-between gap-4">
        <div className="flex min-w-0 flex-col gap-2">
          <div className="flex flex-wrap items-center gap-2">
            <h1 className="truncate text-3xl font-semibold tracking-tight">
              {String(company.name ?? "")}
            </h1>
            {statusColumn ? (
              <Badge variant={company.active ? "default" : "outline"}>
                {statusLabel}
              </Badge>
            ) : null}
          </div>
          <div className="flex flex-wrap items-center gap-2 text-sm">
            <span className="text-muted-foreground font-mono">
              {String(company.id)}
            </span>
            {entityType ? (
              <span className="inline-flex items-center gap-1.5">
                <Badge
                  variant={
                    entityType.is_public_sector ? "secondary" : "outline"
                  }
                >
                  {entityType.entity_type_label}
                </Badge>
                {entityType.source_label ? (
                  <span className="text-muted-foreground text-xs">
                    {entityType.source_label}
                  </span>
                ) : null}
              </span>
            ) : null}
            {country.code === "fi" ? (
              <FiRegistryBadges record={shell.record} />
            ) : null}
            {suggestionsSupported ? (
              <Badge
                variant={domainSuggestionsAvailable ? "secondary" : "outline"}
              >
                {domainSuggestionsAvailable
                  ? "Company domains available"
                  : "No company domains"}
              </Badge>
            ) : null}
          </div>
        </div>
        {tab === "overview" ? (
          <LangToggle lang={lang} pairCount={pairCount} />
        ) : null}
      </header>

      <Tabs value={tab}>
        <TabsList variant="line">
          <TabsTrigger
            value="overview"
            render={
              <NavLink to={`/company/${country.code}/${params.id}`} end />
            }
            nativeButton={false}
          >
            Overview
          </TabsTrigger>
          {country.detail?.financialSources?.length ? (
            <TabsTrigger
              value="financials"
              render={
                <NavLink
                  to={`/company/${country.code}/${params.id}/financials`}
                />
              }
              nativeButton={false}
            >
              Financials
            </TabsTrigger>
          ) : null}
          {suggestionsSupported ? (
            <TabsTrigger
              value="suggestions"
              disabled={!domainSuggestionsAvailable}
              render={
                domainSuggestionsAvailable ? (
                  <NavLink
                    to={`/company/${country.code}/${params.id}/suggestions`}
                  />
                ) : (
                  <span title="No company domains available" />
                )
              }
              nativeButton={false}
            >
              Domains
            </TabsTrigger>
          ) : null}
          {technologySupported ? (
            <TabsTrigger
              value="technology"
              disabled={!technologyAvailable}
              render={
                technologyAvailable ? (
                  <NavLink
                    to={`/company/${country.code}/${params.id}/technology`}
                  />
                ) : (
                  <span title="No technology information available" />
                )
              }
              nativeButton={false}
            >
              Technology
            </TabsTrigger>
          ) : null}
        </TabsList>
      </Tabs>

      <div className="pt-1">
        <Outlet />
      </div>
    </div>
  );
}
