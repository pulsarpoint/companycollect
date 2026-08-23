import { Form, Link } from "react-router";
import {
  ArrowLeftIcon,
  DatabaseIcon,
  ExternalLinkIcon,
  SearchIcon,
  XIcon,
} from "lucide-react";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import {
  Field,
  FieldDescription,
  FieldError,
  FieldGroup,
  FieldLabel,
} from "~/components/ui/field";
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupInput,
} from "~/components/ui/input-group";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import type {
  BolagsverketPersonSourceRow,
  EsefPersonSourceRow,
  PeopleSourceResult,
  WikidataPersonSourceRow,
} from "~/lib/people-sources.server";

function CompanyLink({ companyId }: { companyId: string }) {
  return (
    <Link
      to={`/company/se/${encodeURIComponent(companyId)}`}
      className="font-mono text-xs underline-offset-2 hover:underline"
    >
      {companyId}
    </Link>
  );
}

function SourceIdentifier({ value }: { value: string }) {
  return (
    <span
      className="block max-w-52 truncate font-mono text-xs text-muted-foreground"
      title={value}
    >
      {value}
    </span>
  );
}

function observedAt(value: string): string {
  return value.replace("T", " ").slice(0, 19);
}

function BolagsverketRows({
  rows,
}: {
  rows: BolagsverketPersonSourceRow[];
}) {
  return (
    <Table className="min-w-[76rem]">
      <TableHeader>
        <TableRow>
          <TableHead>Company</TableHead>
          <TableHead>Fiscal year</TableHead>
          <TableHead>Observed person</TableHead>
          <TableHead>Original role</TableHead>
          <TableHead>Mapped role</TableHead>
          <TableHead>Signatory kind</TableHead>
          <TableHead>Source record</TableHead>
          <TableHead>Resolved</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map((row) => (
          <TableRow key={row.signatory_uid}>
            <TableCell>
              <CompanyLink companyId={row.company_id} />
            </TableCell>
            <TableCell className="tabular-nums">{row.fiscal_year}</TableCell>
            <TableCell className="font-medium">
              {[row.first_name, row.last_name].filter(Boolean).join(" ")}
            </TableCell>
            <TableCell className="max-w-64 whitespace-normal">
              {row.role_original || "—"}
            </TableCell>
            <TableCell>
              <Badge variant="outline">{row.role_kind || "unknown"}</Badge>
            </TableCell>
            <TableCell>{row.signatory_kind}</TableCell>
            <TableCell>
              <SourceIdentifier value={row.source_record_uid} />
              <SourceIdentifier value={row.statement_key} />
            </TableCell>
            <TableCell className="text-xs text-muted-foreground">
              {observedAt(row.resolved_at)}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

function EsefRows({ rows }: { rows: EsefPersonSourceRow[] }) {
  return (
    <Table className="min-w-[82rem]">
      <TableHeader>
        <TableRow>
          <TableHead>Company</TableHead>
          <TableHead>Fiscal year</TableHead>
          <TableHead>Observed person</TableHead>
          <TableHead>Role</TableHead>
          <TableHead>Category</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>Confidence</TableHead>
          <TableHead>Document</TableHead>
          <TableHead>Model</TableHead>
          <TableHead>Extracted</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map((row) => (
          <TableRow key={row.candidate_uid}>
            <TableCell>
              <CompanyLink companyId={row.company_id} />
            </TableCell>
            <TableCell className="tabular-nums">{row.fiscal_year}</TableCell>
            <TableCell className="font-medium">{row.name}</TableCell>
            <TableCell className="max-w-72 whitespace-normal">
              {row.role || "—"}
            </TableCell>
            <TableCell>
              <Badge variant="outline">{row.role_category}</Badge>
            </TableCell>
            <TableCell>
              <Badge variant="secondary">{row.status}</Badge>
            </TableCell>
            <TableCell className="tabular-nums">
              {Math.round(row.confidence * 100)}%
            </TableCell>
            <TableCell>
              <SourceIdentifier value={row.source_document_id} />
              <span className="text-xs text-muted-foreground">
                {row.evidence_ids.length} evidence IDs
              </span>
            </TableCell>
            <TableCell>
              <span className="block text-xs">{row.model_name}</span>
              <span className="text-xs text-muted-foreground">
                {row.prompt_version}
              </span>
            </TableCell>
            <TableCell className="text-xs text-muted-foreground">
              {observedAt(row.extracted_at)}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

function WikidataRows({ rows }: { rows: WikidataPersonSourceRow[] }) {
  return (
    <Table className="min-w-[80rem]">
      <TableHeader>
        <TableRow>
          <TableHead>Company</TableHead>
          <TableHead>Person</TableHead>
          <TableHead>Description</TableHead>
          <TableHead>Role claim</TableHead>
          <TableHead>Current</TableHead>
          <TableHead>Period</TableHead>
          <TableHead>Wikidata IDs</TableHead>
          <TableHead>Source record</TableHead>
          <TableHead>Retrieved</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map((row) => (
          <TableRow
            key={`${row.company_id}:${row.source_record_id}:${row.person_wikidata_id}`}
          >
            <TableCell>
              <CompanyLink companyId={row.company_id} />
            </TableCell>
            <TableCell className="font-medium">
              {row.wikidata_url ? (
                <a
                  href={row.wikidata_url}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex items-center gap-1 underline-offset-2 hover:underline"
                >
                  {row.name}
                  <ExternalLinkIcon />
                </a>
              ) : (
                row.name
              )}
            </TableCell>
            <TableCell className="max-w-72 whitespace-normal text-muted-foreground">
              {row.description || "—"}
            </TableCell>
            <TableCell>
              <span className="block">{row.role_label}</span>
              <span className="font-mono text-xs text-muted-foreground">
                {row.role_property}
              </span>
            </TableCell>
            <TableCell>
              <Badge variant={row.is_current ? "secondary" : "outline"}>
                {row.is_current ? "Current" : "Historical"}
              </Badge>
            </TableCell>
            <TableCell className="text-xs tabular-nums">
              {[row.start_date, row.end_date].filter(Boolean).join(" – ") || "—"}
            </TableCell>
            <TableCell>
              <SourceIdentifier value={row.company_wikidata_id} />
              <SourceIdentifier value={row.person_wikidata_id} />
            </TableCell>
            <TableCell>
              <SourceIdentifier value={row.source_record_id} />
              <SourceIdentifier value={row.source_record_uid} />
            </TableCell>
            <TableCell className="text-xs text-muted-foreground">
              {observedAt(row.retrieved_at)}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

export function PeopleSourceTable({ result }: { result: PeopleSourceResult }) {
  const sourcePath = `/admin/se/people/sources/${result.source}`;
  const rowCount = result.rows.length;

  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-3">
        <Button
          variant="ghost"
          size="sm"
          className="w-fit"
          nativeButton={false}
          render={<Link to="/admin/se/people/sources" />}
        >
          <ArrowLeftIcon data-icon="inline-start" />
          All sources
        </Button>
        <div className="flex flex-col justify-between gap-3 md:flex-row md:items-start">
          <div className="flex flex-col gap-1">
            <div className="flex items-center gap-2">
              <h1 className="text-2xl font-semibold tracking-tight">
                {result.definition.label} source rows
              </h1>
              <Badge variant="outline">ClickHouse</Badge>
            </div>
            <p className="max-w-3xl text-sm text-muted-foreground">
              {result.definition.description} These are source-owned rows, before
              draft matching or person normalization.
            </p>
          </div>
          <div className="flex flex-wrap gap-1.5">
            {result.definition.tables.map((table) => (
              <Badge key={table} variant="secondary">
                {table}
              </Badge>
            ))}
          </div>
        </div>
      </header>

      <Form method="get" action={sourcePath} replace>
        <FieldGroup className="max-w-xl">
          <Field data-invalid={Boolean(result.filter.error)}>
            <FieldLabel htmlFor="people-source-company-id">
              Company ID
            </FieldLabel>
            <InputGroup>
              <InputGroupAddon>
                <SearchIcon />
              </InputGroupAddon>
              <InputGroupInput
                key={result.filter.input}
                id="people-source-company-id"
                name="company_id"
                defaultValue={result.filter.input}
                placeholder="5565200028"
                aria-invalid={Boolean(result.filter.error)}
              />
              <InputGroupAddon align="inline-end">
                {result.filter.input ? (
                  <InputGroupButton
                    size="icon-xs"
                    variant="ghost"
                    aria-label="Clear company filter"
                    title="Clear company filter"
                    nativeButton={false}
                    render={<Link to={sourcePath} />}
                  >
                    <XIcon />
                  </InputGroupButton>
                ) : null}
                <InputGroupButton type="submit" size="sm" variant="secondary">
                  Filter
                </InputGroupButton>
              </InputGroupAddon>
            </InputGroup>
            <FieldDescription>
              Swedish organization number, with or without a hyphen.
            </FieldDescription>
            <FieldError>{result.filter.error}</FieldError>
          </Field>
        </FieldGroup>
      </Form>

      <Card>
        <CardHeader className="border-b">
          <CardTitle>Source entries</CardTitle>
          <CardDescription>
            {result.filter.companyId
              ? `${rowCount} rows for company ${result.filter.companyId}`
              : `${rowCount} rows across Swedish companies`}
            {rowCount === result.rowLimit
              ? ` — limited to the first ${result.rowLimit}`
              : ""}
            .
          </CardDescription>
        </CardHeader>
        <CardContent className="px-0">
          {rowCount === 0 ? (
            <Empty className="min-h-64">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <DatabaseIcon />
                </EmptyMedia>
                <EmptyTitle>No source rows found</EmptyTitle>
                <EmptyDescription>
                  {result.filter.error
                    ? "Correct the company ID and run the filter again."
                    : "This source has no person observations for the selected company."}
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : result.source === "bolagsverket" ? (
            <BolagsverketRows rows={result.rows} />
          ) : result.source === "esef" ? (
            <EsefRows rows={result.rows} />
          ) : (
            <WikidataRows rows={result.rows} />
          )}
        </CardContent>
      </Card>
    </div>
  );
}
