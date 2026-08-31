import { Link } from "react-router";
import { ArrowLeft } from "lucide-react";
import type { Route } from "./+types/company-esef-report-notes";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { EsefDisclosureReader } from "~/components/detail/esef-disclosure-reader";
import { getEsefReportNotes } from "~/lib/esef-report-notes.server";

export async function loader({ params }: Route.LoaderArgs) {
  const reportNotes = await getEsefReportNotes(
    params.country,
    params.id,
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

export default function CompanyEsefReportNotes({
  loaderData,
  params,
}: Route.ComponentProps) {
  const { summary, notes } = loaderData;
  const backHref = `/company/${params.country}/${params.id}/financials/esef/${params.documentId}`;

  return (
    <div className="flex w-full flex-col gap-5">
      <div>
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2"
          nativeButton={false}
          render={<Link to={backHref} />}
        >
          <ArrowLeft data-icon="inline-start" />
          Back to facts
        </Button>
      </div>

      <div className="flex min-w-0 flex-col gap-2">
        <h2 className="break-all text-xl font-semibold tracking-tight">
          Report notes · {summary.fiscalYear}
        </h2>
        <p className="text-muted-foreground text-sm">
          {summary.entityName} · period ending {summary.periodEnd}
        </p>
        <p className="text-muted-foreground break-all font-mono text-xs">
          {summary.fxoId}
        </p>
      </div>

      {notes.length === 0 ? (
        <p className="text-muted-foreground text-sm">
          No narrative disclosures were reported for this filing.
        </p>
      ) : (
        <div className="flex flex-col gap-4">
          {notes.map((note) => (
            <section
              key={note.disclosureId}
              className="min-w-0 rounded-xl border p-5"
            >
              <div className="mb-3 flex flex-wrap items-center gap-2">
                <h3 className="text-base font-semibold tracking-tight">
                  {note.conceptLocalName}
                </h3>
                <Badge variant="outline">{note.language}</Badge>
                {note.printedPageNumber ? (
                  <Badge variant="secondary">
                    p. {note.printedPageNumber}
                  </Badge>
                ) : null}
                {note.tableCount > 0 ? (
                  <Badge variant="secondary">{note.tableCount} tables</Badge>
                ) : null}
              </div>
              <EsefDisclosureReader disclosure={note.disclosure} />
            </section>
          ))}
        </div>
      )}
    </div>
  );
}
