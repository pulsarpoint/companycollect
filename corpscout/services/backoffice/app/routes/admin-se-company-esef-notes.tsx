import type { Route } from "./+types/admin-se-company-esef-notes";
import { EsefReportNotesView } from "~/components/detail/esef-report-notes-view";
import { getEsefReportNotes } from "~/lib/esef-report-notes.server";

// Only `loader`, `meta` and the component live here -- see
// admin-se-company-layout.tsx for why.

export async function loader({ params }: Route.LoaderArgs) {
  const reportNotes = await getEsefReportNotes(
    "se",
    params.companyId,
    params.documentId,
  );
  if (!reportNotes) {
    throw new Response("ESEF report not found", { status: 404 });
  }
  return reportNotes;
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  return [
    {
      title: `${loaderData?.summary.entityName ?? params.documentId} · Report notes – CompanyCollect Backoffice`,
    },
  ];
}

export default function AdminSwedenCompanyEsefNotes({
  loaderData,
  params,
}: Route.ComponentProps) {
  return (
    <EsefReportNotesView
      reportNotes={loaderData}
      backHref={`/admin/se/company/${params.companyId}/esef/${params.documentId}`}
    />
  );
}
