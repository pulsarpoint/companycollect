import { FileText } from "lucide-react";
import type { Route } from "./+types/company-financials";
import { FinancialReportDocuments } from "~/components/detail/financial-report-documents";
import { FinancialsSection } from "~/components/detail/financials-section";
import { EsefSection } from "~/components/detail/esef-section";
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
  const { financialSources } = loaderData;
  const country = getCountry(params.country)!;
  const basePath = `/company/${params.country}/${params.id}/financials`;

  return (
    <div className="flex w-full flex-col gap-5">
      <div>
        <h2 className="text-xl font-semibold tracking-tight">Financials</h2>
        <p className="text-muted-foreground mt-1 max-w-3xl text-sm">
          Financial observations remain grouped by source and accounting scope.
          Open a report for its complete tagged facts and evidence.
        </p>
      </div>

      {financialSources.length > 0 ? null : (
        <Empty className="border">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <FileText />
            </EmptyMedia>
            <EmptyTitle>
              No digitally filed annual report found in our sources.
            </EmptyTitle>
            <EmptyDescription>
              No registry financial statement or ESEF report is connected to
              this company yet.
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
