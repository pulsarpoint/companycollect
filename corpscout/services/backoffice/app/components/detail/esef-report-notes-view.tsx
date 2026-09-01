import { Link } from "react-router";
import { ArrowLeft } from "lucide-react";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { EsefDisclosureReader } from "~/components/detail/esef-disclosure-reader";
import type { getEsefReportNotes } from "~/lib/esef-report-notes.server";

// The one ESEF report-notes reader, shared by the public financials page and
// the admin per-document subpage; only the back target differs.
export type EsefReportNotes = NonNullable<
  Awaited<ReturnType<typeof getEsefReportNotes>>
>;

export function EsefReportNotesView({
  reportNotes,
  backHref,
}: {
  reportNotes: EsefReportNotes;
  backHref: string;
}) {
  const { summary, notes } = reportNotes;

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
                  <Badge variant="secondary">p. {note.printedPageNumber}</Badge>
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
