import type { Route } from "./+types/company-esef-financial-report";
import { EsefReportView } from "~/components/detail/esef-report-view";
import { getEsefFinancialReport } from "~/lib/esef-financial-reports.server";

export async function loader({ params }: Route.LoaderArgs) {
  const report = await getEsefFinancialReport(
    params.country,
    params.id,
    params.documentId,
  );
  if (!report) throw new Response("ESEF report not found", { status: 404 });
  return report;
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  return [
    {
      title: `${loaderData?.summary.entityName ?? params.documentId} · ESEF facts – CompanyCollect Backoffice`,
    },
  ];
}

export default function CompanyEsefFinancialReport({
  loaderData,
  params,
}: Route.ComponentProps) {
  return (
    <EsefReportView
      report={loaderData}
      backHref={`/company/${params.country}/${params.id}/financials`}
      backLabel="All financial sources"
      notesHref="notes"
    />
  );
}
