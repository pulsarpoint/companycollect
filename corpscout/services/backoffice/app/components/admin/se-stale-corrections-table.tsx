import { Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
} from "~/components/ui/empty";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import type { SeStaleCorrectionRow } from "~/lib/se-company-person.server";

/** Why the pipeline refuses this row, in the reviewer's words (spec §4.3). */
function whyStale(row: SeStaleCorrectionRow): string {
  if (row.subject_missing) return "the person is no longer published";
  if (row.drafts_missing) return "its observations moved to another person";
  if (row.evidence_moved) return "the evidence changed after the decision";
  return "the pipeline cannot apply it";
}

export function SeStaleCorrectionsTable({
  rows,
}: {
  rows: SeStaleCorrectionRow[];
}) {
  if (rows.length === 0) {
    return (
      <Empty className="border">
        <EmptyHeader>
          <EmptyTitle>Nothing to re-decide</EmptyTitle>
          <EmptyDescription>
            Every live correction still matches the evidence it was decided on.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Company</TableHead>
          <TableHead>Person</TableHead>
          <TableHead>Kind</TableHead>
          <TableHead>Why stale</TableHead>
          <TableHead>Reason given</TableHead>
          <TableHead>Decided by</TableHead>
          <TableHead>Decided</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map((row) => (
          <TableRow key={row.correction_id}>
            <TableCell className="font-mono text-xs">
              {row.company_id}
            </TableCell>
            <TableCell>
              <Link
                className="font-mono text-xs underline"
                to={`/admin/se/people/person/${encodeURIComponent(
                  row.company_id,
                )}/${encodeURIComponent(row.subject_person_id)}`}
              >
                {row.subject_person_id}
              </Link>
            </TableCell>
            <TableCell>
              <Badge variant="outline">{row.correction_kind}</Badge>
            </TableCell>
            <TableCell>
              <Badge variant="destructive">{whyStale(row)}</Badge>
            </TableCell>
            <TableCell>{row.reason}</TableCell>
            <TableCell className="text-muted-foreground">
              {row.decided_by}
            </TableCell>
            <TableCell className="text-muted-foreground">
              {row.created_at}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
