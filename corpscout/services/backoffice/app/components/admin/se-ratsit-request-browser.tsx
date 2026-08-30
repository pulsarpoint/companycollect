import type { ColumnDef } from "@tanstack/react-table";
import {
  ArrowLeftIcon,
  ExternalLinkIcon,
  FileWarningIcon,
  InfoIcon,
} from "lucide-react";
import { useEffect } from "react";
import ReactMarkdown from "react-markdown";
import { Link, useLocation } from "react-router";
import remarkGfm from "remark-gfm";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { Separator } from "~/components/ui/separator";
import { formatFileSize } from "~/lib/norway-financial-reports";
import {
  seRatsitRequestListPath,
  seRatsitRequestPath,
} from "~/lib/se-ratsit-results";
import type {
  SeRatsitRequestDetail,
  SeRatsitRequestPage,
  SeRatsitRequestRow,
} from "~/lib/se-ratsit-results.server";
import { cn } from "~/lib/utils";

const dateTime = new Intl.DateTimeFormat("en-GB", {
  dateStyle: "medium",
  timeStyle: "medium",
  timeZone: "UTC",
});
const number = new Intl.NumberFormat("en-US");

function utcDate(value: string): Date | null {
  if (value === "") return null;
  const normalized = value.includes("T")
    ? value
    : `${value.replace(" ", "T")}Z`;
  const parsed = new Date(normalized);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function formatTimestamp(value: string): string {
  const parsed = utcDate(value);
  return parsed ? `${dateTime.format(parsed)} UTC` : value || "—";
}

function formatDuration(milliseconds: number): string {
  if (milliseconds < 1_000) return `${number.format(milliseconds)} ms`;
  return `${(milliseconds / 1_000).toFixed(2)} s`;
}

function outcomeVariant(
  outcome: string,
): React.ComponentProps<typeof Badge>["variant"] {
  if (outcome === "success") return "default";
  if (outcome === "retry_exhausted" || outcome === "blocked") {
    return "destructive";
  }
  if (outcome === "not_found") return "secondary";
  return "outline";
}

function OutcomeBadge({ outcome }: { outcome: string }) {
  return <Badge variant={outcomeVariant(outcome)}>{outcome}</Badge>;
}

function requestColumns(search: string): ColumnDef<SeRatsitRequestRow, unknown>[] {
  return [
    {
      id: "company",
      header: "Company",
      cell: ({ row }) => (
        <div className="flex max-w-[20rem] flex-col gap-1">
          <span className="truncate font-medium" title={row.original.legal_name}>
            {row.original.legal_name || "Unpublished company"}
          </span>
          <Link
            className="w-fit font-mono text-xs underline underline-offset-2"
            to={seRatsitRequestPath(
              {
                companyId: row.original.company_id,
                batchId: row.original.batch_id,
              },
              search,
            )}
          >
            {row.original.company_id}
          </Link>
        </div>
      ),
    },
    {
      id: "outcome",
      header: "Outcome",
      cell: ({ row }) => <OutcomeBadge outcome={row.original.outcome} />,
    },
    {
      id: "completed_at",
      header: "Completed",
      cell: ({ row }) => (
        <span className="whitespace-nowrap text-sm">
          {formatTimestamp(row.original.completed_at)}
        </span>
      ),
    },
    {
      id: "http_status",
      header: "HTTP",
      cell: ({ row }) => (
        <span className="font-mono text-xs">
          {row.original.http_status ?? "—"}
        </span>
      ),
    },
    {
      id: "content_size_bytes",
      header: "Response",
      cell: ({ row }) => (
        <span className="whitespace-nowrap text-sm tabular-nums">
          {formatFileSize(row.original.content_size_bytes) ?? "—"}
        </span>
      ),
    },
    {
      id: "duration_ms",
      header: "Duration",
      cell: ({ row }) => (
        <span className="whitespace-nowrap text-sm tabular-nums">
          {formatDuration(row.original.duration_ms)}
        </span>
      ),
    },
    {
      id: "attempt_count",
      header: "Attempts",
      cell: ({ row }) => (
        <span className="block text-right text-sm tabular-nums">
          {number.format(row.original.attempt_count)}
        </span>
      ),
    },
  ];
}

export function SeRatsitRequestList({ page }: { page: SeRatsitRequestPage }) {
  const { search } = useLocation();
  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div className="flex flex-col gap-1">
          <h2 className="text-lg font-semibold">Ratsit requests</h2>
          <p className="text-muted-foreground text-sm">
            Terminal crawl results recorded in ClickHouse. Open a row to inspect
            the company, request metadata, and its stored response.
          </p>
        </div>
        <Badge variant="outline">
          Requests
          <span className="text-muted-foreground tabular-nums">
            {number.format(page.total)}
          </span>
        </Badge>
      </div>
      <DataTable
        columns={requestColumns(search)}
        data={page.rows}
        emptyText="No Ratsit requests have been recorded yet."
        minWidthClassName="min-w-[66rem]"
        rowHref={(row) =>
          seRatsitRequestPath(
            { companyId: row.company_id, batchId: row.batch_id },
            search,
          )
        }
      />
      <DataTablePagination
        total={page.total}
        page={page.page}
        pageSize={page.pageSize}
        itemsLabel="requests"
      />
    </div>
  );
}

function MetadataItem({
  label,
  children,
  wide = false,
}: {
  label: string;
  children: React.ReactNode;
  wide?: boolean;
}) {
  return (
    <div className={cn("flex min-w-0 flex-col gap-1", wide && "sm:col-span-2")}>
      <dt className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
        {label}
      </dt>
      <dd className="min-w-0 break-words text-sm">{children}</dd>
    </div>
  );
}

function CompanySummary({ detail }: { detail: SeRatsitRequestDetail }) {
  const { company, request } = detail;
  return (
    <aside className="flex min-w-0 flex-col gap-5 border-b pb-6 lg:border-r lg:border-b-0 lg:pr-6 lg:pb-0">
      <div className="flex flex-col gap-2">
        <span className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
          Company
        </span>
        <h2 className="text-xl font-semibold tracking-tight">
          {company?.legal_name || request.legal_name || request.company_id}
        </h2>
        <span className="font-mono text-sm">{request.company_id}</span>
        <div className="flex flex-wrap gap-2">
          {company?.status ? (
            <Badge variant="secondary">{company.status}</Badge>
          ) : null}
          <Badge variant="outline">
            {request.company_id.length === 12
              ? "Individual owner"
              : "Legal entity"}
          </Badge>
          {company?.entity_type_label ? (
            <Badge variant="outline">{company.entity_type_label}</Badge>
          ) : null}
        </div>
      </div>

      {company ? (
        <dl className="flex flex-col gap-4">
          <MetadataItem label="Legal form">
            {company.legal_form_label_sv || company.legal_form_code || "—"}
            {company.legal_form_label_en ? (
              <span className="text-muted-foreground block">
                {company.legal_form_label_en}
              </span>
            ) : null}
          </MetadataItem>
          <MetadataItem label="Incorporated">
            {company.incorporation_date || "—"}
          </MetadataItem>
          <MetadataItem label="Published profile">
            {company.published ? "Yes" : "No"}
          </MetadataItem>
        </dl>
      ) : (
        <Alert>
          <InfoIcon />
          <AlertTitle>Company profile unavailable</AlertTitle>
          <AlertDescription>
            This request exists, but the company is not currently available in
            the Swedish company tables.
          </AlertDescription>
        </Alert>
      )}

      <div className="flex flex-wrap gap-2">
        <Button
          variant="outline"
          size="sm"
          render={
            <Link
              to={`/admin/se/company/${encodeURIComponent(request.company_id)}/info`}
            />
          }
          nativeButton={false}
        >
          Admin profile
        </Button>
        <Button
          variant="ghost"
          size="sm"
          render={
            <Link to={`/company/se/${encodeURIComponent(request.company_id)}`} />
          }
          nativeButton={false}
        >
          Public page
          <ExternalLinkIcon data-icon="inline-end" />
        </Button>
      </div>
    </aside>
  );
}

function RequestMetadata({ detail }: { detail: SeRatsitRequestDetail }) {
  const { request, payload } = detail;
  return (
    <section className="flex flex-col gap-4" aria-labelledby="ratsit-request-metadata">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h2 id="ratsit-request-metadata" className="text-lg font-semibold">
          Request metadata
        </h2>
        <OutcomeBadge outcome={request.outcome} />
      </div>
      <dl className="grid gap-x-6 gap-y-4 sm:grid-cols-2 xl:grid-cols-4">
        <MetadataItem label="HTTP status">
          {request.http_status ?? "—"}
        </MetadataItem>
        <MetadataItem label="Attempts">{request.attempt_count}</MetadataItem>
        <MetadataItem label="Duration">
          {formatDuration(request.duration_ms)}
        </MetadataItem>
        <MetadataItem label="Content size">
          {formatFileSize(request.content_size_bytes) ?? "—"}
        </MetadataItem>
        <MetadataItem label="Selected">
          {formatTimestamp(request.selected_at)}
        </MetadataItem>
        <MetadataItem label="Attempted">
          {formatTimestamp(request.attempted_at)}
        </MetadataItem>
        <MetadataItem label="Completed">
          {formatTimestamp(request.completed_at)}
        </MetadataItem>
        <MetadataItem label="Recorded">
          {formatTimestamp(request.recorded_at)}
        </MetadataItem>
        <MetadataItem label="Browser">
          {payload?.browserId || "—"}
        </MetadataItem>
        <MetadataItem label="Stored type">
          {payload?.contentType || "—"}
        </MetadataItem>
        <MetadataItem label="Batch ID" wide>
          <code className="font-mono text-xs">{request.batch_id}</code>
        </MetadataItem>
        <MetadataItem label="Temporal workflow" wide>
          <code className="font-mono text-xs">
            {request.temporal_workflow_id}
          </code>
        </MetadataItem>
        <MetadataItem label="Temporal run" wide>
          <code className="font-mono text-xs">{request.temporal_run_id}</code>
        </MetadataItem>
        <MetadataItem label="Requested URL" wide>
          <a
            className="underline underline-offset-2"
            href={request.source_url}
            target="_blank"
            rel="noreferrer"
          >
            {request.source_url}
          </a>
        </MetadataItem>
        {payload?.finalUrl && payload.finalUrl !== request.source_url ? (
          <MetadataItem label="Final URL" wide>
            <a
              className="underline underline-offset-2"
              href={payload.finalUrl}
              target="_blank"
              rel="noreferrer"
            >
              {payload.finalUrl}
            </a>
          </MetadataItem>
        ) : null}
        <MetadataItem label="S3 object" wide>
          {request.source_object_key ? (
            <code className="font-mono text-xs">
              s3://{request.source_bucket}/{request.source_object_key}
            </code>
          ) : (
            "Not stored"
          )}
        </MetadataItem>
        {request.error_type || request.error_message ? (
          <MetadataItem label="Error" wide>
            <span className="font-medium">{request.error_type || "Error"}</span>
            {request.error_message ? (
              <span className="text-muted-foreground block">
                {request.error_message}
              </span>
            ) : null}
          </MetadataItem>
        ) : null}
      </dl>
    </section>
  );
}

function MarkdownViewer({ markdown }: { markdown: string }) {
  return (
    <div className="max-h-[70vh] overflow-auto rounded-md border bg-background p-5">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          h1: ({ node: _node, ...props }) => (
            <h1 className="mb-4 text-2xl font-semibold tracking-tight" {...props} />
          ),
          h2: ({ node: _node, ...props }) => (
            <h2 className="mt-7 mb-3 text-xl font-semibold" {...props} />
          ),
          h3: ({ node: _node, ...props }) => (
            <h3 className="mt-6 mb-2 text-lg font-semibold" {...props} />
          ),
          h4: ({ node: _node, ...props }) => (
            <h4 className="mt-5 mb-2 font-semibold" {...props} />
          ),
          p: ({ node: _node, ...props }) => (
            <p className="mb-3 leading-7 last:mb-0" {...props} />
          ),
          ul: ({ node: _node, ...props }) => (
            <ul className="mb-4 list-disc pl-6" {...props} />
          ),
          ol: ({ node: _node, ...props }) => (
            <ol className="mb-4 list-decimal pl-6" {...props} />
          ),
          li: ({ node: _node, ...props }) => (
            <li className="my-1 pl-1" {...props} />
          ),
          a: ({ node: _node, ...props }) => (
            <a
              className="font-medium underline underline-offset-2"
              target="_blank"
              rel="noreferrer"
              {...props}
            />
          ),
          blockquote: ({ node: _node, ...props }) => (
            <blockquote
              className="text-muted-foreground my-4 border-l-2 pl-4 italic"
              {...props}
            />
          ),
          hr: () => <Separator className="my-6" />,
          table: ({ node: _node, ...props }) => (
            <div className="my-5 overflow-x-auto rounded-md border">
              <table className="w-full border-collapse text-sm" {...props} />
            </div>
          ),
          th: ({ node: _node, ...props }) => (
            <th
              className="bg-muted border-b px-3 py-2 text-left font-medium"
              {...props}
            />
          ),
          td: ({ node: _node, ...props }) => (
            <td className="border-b px-3 py-2 align-top" {...props} />
          ),
          pre: ({ node: _node, ...props }) => (
            <pre
              className="bg-muted my-4 overflow-x-auto rounded-md p-4 text-xs"
              {...props}
            />
          ),
          code: ({ node: _node, ...props }) => (
            <code className="bg-muted rounded px-1 py-0.5 font-mono text-xs" {...props} />
          ),
        }}
      >
        {markdown}
      </ReactMarkdown>
    </div>
  );
}

function StoredResponse({ detail }: { detail: SeRatsitRequestDetail }) {
  if (detail.payloadError) {
    return (
      <Alert variant="destructive">
        <FileWarningIcon />
        <AlertTitle>Stored response unavailable</AlertTitle>
        <AlertDescription>{detail.payloadError}</AlertDescription>
      </Alert>
    );
  }
  if (!detail.payload) {
    return (
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <FileWarningIcon />
          </EmptyMedia>
          <EmptyTitle>No response content stored</EmptyTitle>
          <EmptyDescription>
            Only successful Ratsit responses are written to S3. This request's
            outcome and error remain available in ClickHouse above.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  return (
    <section className="flex flex-col gap-3" aria-labelledby="ratsit-response-content">
      <div className="flex flex-wrap items-end justify-between gap-2">
        <div className="flex flex-col gap-1">
          <h2 id="ratsit-response-content" className="text-lg font-semibold">
            Stored Ratsit page
          </h2>
          <p className="text-muted-foreground text-sm">
            The captured HTML is converted to Markdown when this detail is
            opened; the object in S3 remains unchanged.
          </p>
        </div>
        {detail.payload.browserId ? (
          <Badge variant="outline">{detail.payload.browserId}</Badge>
        ) : null}
      </div>
      {detail.payload.markdown ? (
        <MarkdownViewer markdown={detail.payload.markdown} />
      ) : (
        <Empty className="border">
          <EmptyHeader>
            <EmptyTitle>Empty response</EmptyTitle>
            <EmptyDescription>
              The S3 envelope is valid, but its HTML produced no readable
              Markdown.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      )}
    </section>
  );
}

export function SeRatsitRequestInspector({
  detail,
}: {
  detail: SeRatsitRequestDetail | null;
}) {
  const { search } = useLocation();
  const listPath = seRatsitRequestListPath(search);
  useEffect(() => {
    window.scrollTo(0, 0);
  }, [detail?.request.company_id, detail?.request.batch_id]);
  if (!detail) {
    return (
      <div className="flex flex-col gap-4">
        <Button
          variant="ghost"
          size="sm"
          className="w-fit"
          render={<Link to={listPath} />}
          nativeButton={false}
        >
          <ArrowLeftIcon data-icon="inline-start" />
          Back to requests
        </Button>
        <Alert variant="destructive">
          <FileWarningIcon />
          <AlertTitle>Ratsit request not found</AlertTitle>
          <AlertDescription>
            ClickHouse does not contain this company and batch combination.
          </AlertDescription>
        </Alert>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-5">
      <Button
        variant="ghost"
        size="sm"
        className="w-fit"
        render={<Link to={listPath} />}
        nativeButton={false}
      >
        <ArrowLeftIcon data-icon="inline-start" />
        Back to requests
      </Button>
      <div className="grid min-w-0 gap-6 lg:grid-cols-[minmax(16rem,0.6fr)_minmax(0,1.7fr)]">
        <CompanySummary detail={detail} />
        <section
          className="flex min-w-0 flex-col gap-6"
          aria-label="Ratsit request evidence"
        >
          <RequestMetadata detail={detail} />
          <Separator />
          <StoredResponse detail={detail} />
        </section>
      </div>
    </div>
  );
}
