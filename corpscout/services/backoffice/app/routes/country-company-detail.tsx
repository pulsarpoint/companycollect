import type { Route } from "./+types/country-company-detail";
import { Link } from "react-router";
import { buttonVariants } from "~/components/ui/button";
import { getCountry } from "~/lib/countries";
import { getCompanyDetail } from "~/lib/queries.server";
import {
  companySectionPresencePromiseContext,
  companyShellPromiseContext,
} from "~/lib/company-shell-context.server";
import { CompanyRecordSection } from "~/components/detail/detail-sections";
import { ContactLocationCard } from "~/components/detail/contact-location-card";
import { GleifGroupSection } from "~/components/detail/gleif-group-section";
import { WikidataSection } from "~/components/detail/wikidata-section";
import { SecondaryNamesSection } from "~/components/detail/secondary-names-section";
import { ManagementSection } from "~/components/detail/management-section";
import { FinancialsSection } from "~/components/detail/financials-section";
import { FinancialSnapshot } from "~/components/detail/financial-snapshot";
import { EsefSection } from "~/components/detail/esef-section";
import { FrFinancialsSection } from "~/components/detail/countries/fr-financials";
import { IndustriesSection } from "~/components/detail/industries-section";
import { StatementsFallback } from "~/components/detail/countries/no-financials";
import { decorateFiRecord } from "~/components/detail/countries/fi-registry";
import { decorateBrRecord } from "~/components/detail/countries/br-company";
import { FiTaxRecordsSection } from "~/components/detail/countries/fi-tax-records";
import { PublicContractsSection } from "~/components/detail/public-contracts-section";
import type { Lang } from "~/components/detail/language";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";
import {
  ContactsDomainsSection,
  DescriptionsSection,
  ProductsMarketsSection,
  SourcesSection,
} from "~/components/detail/source-information-sections";
import { LazyCompanySection } from "~/components/detail/lazy-company-section";

export async function loader({ context, params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });
  const shell = await context.get(companyShellPromiseContext);
  if (country.code === "se") {
    if (!shell) throw new Response("Company not found", { status: 404 });
    return {
      mode: "serving" as const,
      shell,
      presence: await context.get(companySectionPresencePromiseContext),
    };
  }
  const detail = await getCompanyDetail(country, params.id, shell);
  if (!detail) throw new Response("Company not found", { status: 404 });
  return { mode: "legacy" as const, detail };
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  const name = loaderData
    ? loaderData.mode === "serving"
      ? loaderData.shell.company.name
      : loaderData.detail.company.name
    : undefined;
  return [
    {
      title: name
        ? `${name} – CompanyCollect Backoffice`
        : `Company ${params.id}`,
    },
  ];
}

export default function CompanyDetail({
  loaderData,
  params,
}: Route.ComponentProps) {
  const country = getCountry(params.country)!;
  const searchParams = useEffectiveSearchParams();
  const lang: Lang =
    searchParams.get("lang") === "original" ? "original" : "en";
  if (loaderData.mode === "serving") {
    const { shell, presence } = loaderData;
    const availableSections = new Set(presence.map((row) => row.section));
    const sectionOrder = [
      "descriptions",
      "wikidata",
      "financials",
      "management",
      "domains",
      "industries",
      "gleif",
      "contracts",
      "addresses",
      "sources",
    ] as const;
    return (
      <div className="flex w-full max-w-5xl flex-col gap-4">
        {/* company.company_id is undefined for SE shells; params.id is the
            company id the loader used to look this record up. */}
        <div className="flex justify-end">
          <Link
            className={buttonVariants({ variant: "outline", size: "sm" })}
            to={`/admin/se/company/${params.id}/info`}
          >
            Review company info
          </Link>
        </div>
        <CompanyRecordSection
          company={shell.company}
          record={shell.record}
          lang={lang}
          hiddenFieldKeys={
            availableSections.has("descriptions")
              ? new Set(["activity_description", "company_description"])
              : undefined
          }
        />
        {sectionOrder
          .filter((section) => availableSections.has(section))
          .map((section) => (
            <LazyCompanySection
              key={section}
              section={section}
              country={country}
              companyId={params.id}
              record={shell.record}
            />
          ))}
      </div>
    );
  }
  const { detail } = loaderData;
  const { company } = detail;
  // One per-country seam: each register decides how its own record reads.
  const DECORATORS: Record<
    string,
    (r: Record<string, unknown>) => Record<string, unknown>
  > = {
    fi: decorateFiRecord,
    br: decorateBrRecord,
  };
  const record = (DECORATORS[country.code] ?? ((r) => r))(detail.record);

  return (
    <div className="flex w-full max-w-5xl flex-col gap-4">
      <CompanyRecordSection
        company={company}
        record={record}
        lang={lang}
        hiddenFieldKeys={
          detail.descriptions.length > 0
            ? new Set(["activity_description", "company_description"])
            : undefined
        }
      />
      <DescriptionsSection descriptions={detail.descriptions} />
      <WikidataSection wikidata={detail.wikidata} />
      <SecondaryNamesSection names={detail.secondaryNames} />
      {(() => {
        if (country.detail?.financialSources?.length) {
          return (
            <FinancialSnapshot
              sources={detail.financialSources}
              href={`/company/${country.code}/${params.id}/financials`}
            />
          );
        }
        if (detail.statements.length > 0)
          return <StatementsFallback statements={detail.statements} />;
        return (
          <FinancialsSection
            financials={detail.financials}
            factsHref={
              country.detail?.factsQuery
                ? (year) =>
                    `/company/${country.code}/${params.id}/facts/${year}`
                : undefined
            }
          />
        );
      })()}
      <FrFinancialsSection financials={detail.frFinancials} />
      {country.detail?.financialSources?.length ? null : (
        <EsefSection filings={detail.esefFilings} />
      )}
      <ManagementSection
        officers={[]}
        peopleMatches={[]}
        audit={detail.audit}
        wikidataPeople={detail.wikidataPeople}
        esefPeople={detail.esefPeople}
      />
      <ContactsDomainsSection
        contacts={detail.contacts}
        domains={detail.domains}
        sourceContacts={detail.sourceContacts}
        wikidata={detail.wikidata}
      />
      <IndustriesSection
        industries={detail.industries}
        wikidata={detail.wikidata}
      />
      <GleifGroupSection
        relationships={detail.gleifRelationships}
        entity={detail.gleifEntity}
        countryCode={country.code}
        sourceRelationships={detail.sourceRelationships}
      />
      <ProductsMarketsSection items={detail.businessItems} />
      <FiTaxRecordsSection taxRecords={detail.taxRecords} />
      <PublicContractsSection
        contracts={detail.publicContracts}
        summary={detail.contractSummary}
      />
      <ContactLocationCard
        country={country}
        companyId={String(company.id)}
        contacts={[]}
        addresses={detail.addresses}
        record={detail.record}
      />
      <SourcesSection records={detail.sourceRecords} />
    </div>
  );
}
