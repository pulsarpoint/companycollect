import type { Route } from "./+types/country-company-detail";
import { getCountry } from "~/lib/countries";
import { getCompanyDetail } from "~/lib/queries.server";
import {
  CompanyRecordSection,
  DomainsSection,
} from "~/components/detail/detail-sections";
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
  const searchParams = useEffectiveSearchParams();
  const lang: Lang = searchParams.get("lang") === "original" ? "original" : "en";
  // One per-country seam: each register decides how its own record reads.
  const DECORATORS: Record<string, (r: Record<string, unknown>) => Record<string, unknown>> = {
    fi: decorateFiRecord,
    br: decorateBrRecord,
  };
  const record = (DECORATORS[country.code] ?? ((r) => r))(detail.record);

  return (
    <div className="flex w-full max-w-5xl flex-col gap-4">
      <CompanyRecordSection company={company} record={record} lang={lang} />
      <GleifGroupSection
        relationships={detail.gleifRelationships}
        entity={detail.gleifEntity}
        countryCode={country.code}
      />
      <WikidataSection wikidata={detail.wikidata} people={detail.wikidataPeople} />
      <SecondaryNamesSection names={detail.secondaryNames} />
      <ManagementSection officers={detail.officers} peopleMatches={detail.peopleMatches} audit={detail.audit} />
      <IndustriesSection industries={detail.industries} />
      {(() => {
        if (country.detail?.financialReports) {
          return (
            <FinancialSnapshot
              financials={detail.financials}
              href={`/company/${country.code}/${params.id}/financials`}
            />
          );
        }
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
      <FrFinancialsSection financials={detail.frFinancials} />
      <EsefSection filings={detail.esefFilings} />
      <FiTaxRecordsSection taxRecords={detail.taxRecords} />
      <PublicContractsSection contracts={detail.publicContracts} />
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
