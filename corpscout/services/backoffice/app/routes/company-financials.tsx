import { useState } from "react";
import { FileText } from "lucide-react";
import type { Route } from "./+types/company-financials";
import { FinancialReportDocuments } from "~/components/detail/financial-report-documents";
import { FinancialsSection } from "~/components/detail/financials-section";
import type { FinancialLocale } from "~/components/financials/copy";
import { FinancialSourceOverview } from "~/components/financials/financial-source-overview";
import { FinancialSourceSwitcher } from "~/components/financials/financial-source-switcher";
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
  const [locale, setLocale] = useState<FinancialLocale>("en");
  const [selectedSourceId, setSelectedSourceId] = useState(
    financialSources[0]?.id ?? "",
  );

  if (country.code === "se") {
    const selectedSource =
      financialSources.find((source) => source.id === selectedSourceId) ??
      financialSources[0];
    const factsHref = selectedSource
      ? selectedSource.kind === "registry" && country.detail?.factsQuery
        ? (year: string) =>
            `/company/${country.code}/${params.id}/facts/${year}`
        : selectedSource.kind === "esef"
          ? (year: string) => {
              const row = selectedSource.financials.find(
                (financial) => financial.fiscal_year === year,
              );
              return row?.source_document_id
                ? `${basePath}/esef/${encodeURIComponent(row.source_document_id)}`
                : basePath;
            }
          : undefined
      : undefined;

    return (
      <div className="flex w-full flex-col gap-8">
        {financialSources.length > 0 ? (
          <FinancialSourceSwitcher
            sources={financialSources}
            selectedSourceId={selectedSource?.id ?? ""}
            onSourceChange={setSelectedSourceId}
            locale={locale}
            onLocaleChange={setLocale}
          />
        ) : null}

        {selectedSource ? (
          <FinancialSourceOverview
            key={selectedSource.id}
            source={selectedSource}
            filingStatus={filingStatus}
            factsHref={factsHref}
            locale={locale}
          >
            {selectedSource.kind === "registry" ? (
              <FinancialReportDocuments
                reports={selectedSource.documents}
                detailsHref={(report) =>
                  `${basePath}/${encodeURIComponent(report.documentId)}`
                }
              />
            ) : null}
          </FinancialSourceOverview>
        ) : (
          <FinancialSourcesEmpty filingStatus={filingStatus} />
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

function FinancialSourcesEmpty({
  filingStatus,
}: {
  filingStatus: Awaited<
    ReturnType<typeof getCompanyFinancialDetail>
  >["filingStatus"];
}) {
  return (
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
  );
}
