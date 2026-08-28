import type { Route } from "./+types/company-financials";
import { FinancialReportDocuments } from "~/components/detail/financial-report-documents";
import { FinancialsSection } from "~/components/detail/financials-section";
import {
  FinancialSourcesEmpty,
  SeFinancialsView,
} from "~/components/financials/se-financials-view";
import { FinancialFilingStatusSummary } from "~/components/financial-filing-status";
import { getCountry } from "~/lib/countries";
import { getCompanyFinancialDetail } from "~/lib/queries.server";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country?.detail?.financialSources?.length) {
    throw new Response("Not found", { status: 404 });
  }
  return getCompanyFinancialDetail(country, params.id);
}

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `Financials · ${params.id} – CompanyCollect Backoffice` }];
}

export default function CompanyFinancials({
  loaderData,
  params,
}: Route.ComponentProps) {
  const { financialSources, filingStatus } = loaderData;
  const country = getCountry(params.country)!;
  const basePath = `/company/${params.country}/${params.id}/financials`;

  if (country.code === "se") {
    return (
      <SeFinancialsView
        financialSources={financialSources}
        filingStatus={filingStatus}
        basePath={basePath}
        factsBase={
          country.detail?.factsQuery
            ? `/company/${country.code}/${params.id}/facts`
            : undefined
        }
      />
    );
  }

  return (
    <div className="flex w-full flex-col gap-5">
      <div>
        <h2 className="text-xl font-semibold tracking-tight">Financials</h2>
        <p className="text-muted-foreground mt-1 max-w-3xl text-sm">
          Financial observations remain grouped by source and accounting scope.
          Open a report for its complete tagged facts and evidence.
        </p>
      </div>

      {filingStatus ? (
        <FinancialFilingStatusSummary filing={filingStatus} />
      ) : null}

      {financialSources.length > 0 ? null : (
        <FinancialSourcesEmpty filingStatus={filingStatus} />
      )}

      {financialSources.map((source) => {
        const factsHref =
          source.kind === "esef"
            ? (year: string) => {
                const row = source.financials.find(
                  (financial) => financial.fiscal_year === year,
                );
                return row?.source_document_id
                  ? `${basePath}/esef/${encodeURIComponent(row.source_document_id)}`
                  : basePath;
              }
            : source.yearFacts && country.detail?.factsQuery
              ? (year: string) =>
                  `/company/${country.code}/${params.id}/facts/${year}`
              : undefined;
        return (
          <FinancialsSection
            key={source.id}
            financials={source.financials}
            title={source.title}
            description={source.description}
            factsHref={factsHref}
          >
            {source.kind === "registry" ? (
              <FinancialReportDocuments
                reports={source.documents}
                detailsHref={(report) =>
                  `${basePath}/${encodeURIComponent(report.documentId)}`
                }
              />
            ) : null}
          </FinancialsSection>
        );
      })}
    </div>
  );
}
