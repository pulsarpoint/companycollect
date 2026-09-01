import { Link } from "react-router";
import { Bot } from "lucide-react";
import type { Route } from "./+types/admin-se-company-esef-document";
import { Button } from "~/components/ui/button";
import { EsefReportView } from "~/components/detail/esef-report-view";
import { getEsefFinancialReport } from "~/lib/esef-financial-reports.server";

// Only `loader`, `meta` and the component live here -- see
// admin-se-company-layout.tsx for why.

// The same facts reader the public financials page uses, hosted on the admin
// ESEF area, plus the admin-only LLM subpage entry point.
export async function loader({ params }: Route.LoaderArgs) {
  const report = await getEsefFinancialReport(
    "se",
    params.companyId,
    params.documentId,
  );
  if (!report) throw new Response("ESEF report not found", { status: 404 });
  return report;
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  return [
    {
      title: `${loaderData?.summary.entityName ?? params.documentId} · ESEF document – CompanyCollect Backoffice`,
    },
  ];
}

export default function AdminSwedenCompanyEsefDocument({
  loaderData,
  params,
}: Route.ComponentProps) {
  const base = `/admin/se/company/${params.companyId}/esef`;
  return (
    <EsefReportView
      report={loaderData}
      backHref={base}
      backLabel="ESEF overview"
      notesHref={`${base}/${params.documentId}/notes`}
      extraActions={
        <Button
          variant="outline"
          nativeButton={false}
          render={<Link to={`${base}/${params.documentId}/llm`} />}
        >
          <Bot data-icon="inline-start" />
          LLM extraction
        </Button>
      }
    />
  );
}
