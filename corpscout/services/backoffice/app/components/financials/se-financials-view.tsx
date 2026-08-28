import { useState } from "react";
import { FileText } from "lucide-react";
import { FinancialReportDocuments } from "~/components/detail/financial-report-documents";
import type { FinancialLocale } from "~/components/financials/copy";
import { FinancialSourceOverview } from "~/components/financials/financial-source-overview";
import { FinancialSourceSwitcher } from "~/components/financials/financial-source-switcher";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import type { CompanyFinancialFilingStatus } from "~/lib/financial-filing-status";
import type { CompanyFinancialSource } from "~/lib/queries.server";

/**
 * Sweden's financial experience: the source switcher (Bolagsverket standalone
 * accounts vs ESEF consolidated IFRS) with its sv/en language toggle, one
 * source's full overview at a time, and the registry source's document table.
 *
 * Shared between the public company financials page and the admin company
 * area's Financial tab, so both always show the same thing. Facts and
 * report/ESEF deep links always point at the PUBLIC readers — the admin tab
 * links into them rather than duplicating them — which is why the link bases
 * are props instead of being derived from the current URL.
 */
export function SeFinancialsView({
  financialSources,
  filingStatus,
  basePath,
  factsBase,
}: {
  financialSources: CompanyFinancialSource[];
  filingStatus: CompanyFinancialFilingStatus | null;
  /** Public financials base path (`/company/se/:id/financials`); report and
   * ESEF document deep links hang off it. */
  basePath: string;
  /** Public facts base path (`/company/se/:id/facts`), or undefined when the
   * country config exposes no facts query. */
  factsBase?: string;
}) {
  const [locale, setLocale] = useState<FinancialLocale>("en");
  const [selectedSourceId, setSelectedSourceId] = useState(
    financialSources[0]?.id ?? "",
  );

  const selectedSource =
    financialSources.find((source) => source.id === selectedSourceId) ??
    financialSources[0];
  const factsHref = selectedSource
    ? selectedSource.kind === "registry" && factsBase
      ? (year: string) => `${factsBase}/${year}`
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

export function FinancialSourcesEmpty({
  filingStatus,
}: {
  filingStatus: CompanyFinancialFilingStatus | null;
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
