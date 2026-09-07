import { Form, Link } from "react-router";
import type { Route } from "./+types/admin-technology-proposals";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Empty,
  EmptyHeader,
  EmptyTitle,
  EmptyDescription,
} from "~/components/ui/empty";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { loadTechnologyProposals } from "~/lib/technology-proposals.server";

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const page = Math.min(
    10000,
    Math.max(1, Number.parseInt(url.searchParams.get("page") ?? "1", 10) || 1),
  );
  const status = url.searchParams.get("status") === "all" ? "all" : "pending";
  return { rows: await loadTechnologyProposals(page, status), page, status };
}

export function meta() {
  return [{ title: "Technology proposals | CompanyCollect" }];
}

export default function TechnologyProposals({
  loaderData,
}: Route.ComponentProps) {
  const { rows, page, status } = loaderData;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <Link
          to="/admin/technologies"
          className="text-sm text-muted-foreground"
        >
          ← Technology catalog
        </Link>
        <h1 className="text-2xl font-semibold tracking-tight">
          Technology proposals
        </h1>
        <p className="text-muted-foreground">
          Review technologies found by crawlers. Approve a new entry, map it to
          an existing technology, or reject the suggestion.
        </p>
      </header>
      <Form method="get" className="flex gap-2">
        <Button
          type="submit"
          name="status"
          value="pending"
          variant={status === "pending" ? "default" : "outline"}
        >
          Pending review
        </Button>
        <Button
          type="submit"
          name="status"
          value="all"
          variant={status === "all" ? "default" : "outline"}
        >
          All proposals
        </Button>
      </Form>
      {rows.length ? (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Proposed technology</TableHead>
              <TableHead>Company / role</TableHead>
              <TableHead>Current catalog</TableHead>
              <TableHead>Received</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => (
              <TableRow key={row.proposal_id}>
                <TableCell>
                  <Link
                    className="font-medium underline underline-offset-4"
                    to={`/admin/technology-proposals/${row.proposal_id}`}
                  >
                    {row.proposed_name}
                  </Link>
                  <p className="text-sm text-muted-foreground">
                    Observed as {row.observed_name}
                  </p>
                </TableCell>
                <TableCell>
                  {row.company || "Company not established"}
                  <p className="text-sm text-muted-foreground">
                    {row.job_title || row.context}
                  </p>
                </TableCell>
                <TableCell>
                  {row.existing_technology ? (
                    <Badge variant="secondary">{row.existing_technology}</Badge>
                  ) : (
                    "No exact match at submission"
                  )}
                </TableCell>
                <TableCell>{row.received_at}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      ) : (
        <Empty>
          <EmptyHeader>
            <EmptyTitle>
              No {status === "pending" ? "pending " : ""}proposals
            </EmptyTitle>
            <EmptyDescription>
              New technologies submitted by crawlers will appear here.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      )}
      <nav className="flex gap-4" aria-label="Proposal pages">
        {page > 1 && (
          <Link to={`?status=${status}&page=${page - 1}`}>Previous</Link>
        )}
        <span>Page {page}</span>
        {rows.length === 25 && (
          <Link to={`?status=${status}&page=${page + 1}`}>Next</Link>
        )}
      </nav>
    </div>
  );
}
