import { FileText } from "lucide-react";
import type { Route } from "./+types/company-financials";
import { FinancialReportDocuments } from "~/components/detail/financial-report-documents";
import { FinancialsSection } from "~/components/detail/financials-section";
import { EsefSection } from "~/components/detail/esef-section";
import { SwedenFinancialOverview } from "~/components/financials/sweden-financial-overview";
import { FinancialFilingStatusSummary } from "~/components/financial-filing-status";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
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
    const registrySource = financialSources.find(
      (source) => source.kind === "registry",
    );
    const factsHref = country.detail?.factsQuery
      ? (year: string) => `/company/${country.code}/${params.id}/facts/${year}`
      : undefined;

    return (
      <div className="flex w-full flex-col gap-10">
        <SwedenFinancialOverview
          financials={
            registrySource?.kind === "registry" ? registrySource.financials : []
          }
          filingStatus={filingStatus}
          factsHref={factsHref}
        >
          {registrySource?.kind === "registry" ? (
            <FinancialReportDocuments
              reports={registrySource.documents}
              detailsHref={(report) =>
                `${basePath}/${encodeURIComponent(report.documentId)}`
              }
            />
          ) : null}
        </SwedenFinancialOverview>

        {financialSources
          .filter((source) => source.kind === "esef")
          .map((source) =>
            source.kind === "esef" ? (
              <EsefSection
                key={source.id}
                filings={source.filings}
                title={source.title}
                description={source.description}
                detailsHref={(filing) =>
                  `${basePath}/esef/${encodeURIComponent(filing.primary_fxo_id)}`
                }
              />
            ) : null,
          )}
      </div>
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
        <Empty className="border">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <FileText />
            </EmptyMedia>
            <EmptyTitle>
              {filingStatus?.status === "filed_unstructured"
                ? "Annual report filed in another format."
                : filingStatus?.status === "not_submitted"
                  ? "Annual report not submitted."
                  : "No digitally filed annual report found in our sources."}
            </EmptyTitle>
            <EmptyDescription>
              {filingStatus?.status === "filed_unstructured"
                ? "An official filing exists, but structured financial figures are not available."
                : filingStatus?.status === "not_submitted"
                  ? "An official source reports that the expected annual report is missing."
                  : "No registry financial statement or ESEF report is connected to this company yet."}
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      )}

      {financialSources.map((source) => {
        if (source.kind === "esef") {
          return (
            <EsefSection
              key={source.id}
              filings={source.filings}
              title={source.title}
              description={source.description}
              detailsHref={(filing) =>
                `${basePath}/esef/${encodeURIComponent(filing.primary_fxo_id)}`
              }
            />
          );
        }
        const factsHref =
          source.yearFacts && country.detail?.factsQuery
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
            <FinancialReportDocuments
              reports={source.documents}
              detailsHref={(report) =>
                `${basePath}/${encodeURIComponent(report.documentId)}`
              }
            />
          </FinancialsSection>
        );
      })}
    </div>
  );
}
