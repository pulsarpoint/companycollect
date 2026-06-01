import { useEffect, useMemo, useState } from "react";
import { AlertCircle } from "lucide-react";
import { api, errorMessage } from "~/lib/api";
import type { BrregRawRecordDetail } from "~/types/api";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Separator } from "~/components/ui/separator";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "~/components/ui/sheet";
import { Skeleton } from "~/components/ui/skeleton";
import type { BrregDomainSearchEvidence } from "~/types/api";

function statusClass(status?: string) {
  if (status === "succeeded" || status === "built" || status === "published") {
    return "border-green-200 bg-green-100 text-green-800";
  }
  if (status?.includes("failed") || status === "publish_failed") {
    return "border-red-200 bg-red-100 text-red-800";
  }
  if (status === "running") return "border-blue-200 bg-blue-100 text-blue-800";
  if (!status || status === "not_started") return "border-gray-200 bg-gray-50 text-gray-700";
  return "border-amber-200 bg-amber-50 text-amber-800";
}

function formatStatus(status?: string) {
  return status ? status.replace(/_/g, " ") : "not started";
}

function formatDate(value?: string) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString();
}

function isEmptyObject(value: unknown) {
  return Boolean(value && typeof value === "object" && !Array.isArray(value) && Object.keys(value).length === 0);
}

function StatusBadge({ status }: { status?: string }) {
  return (
    <Badge variant="outline" className={`whitespace-nowrap text-xs ${statusClass(status)}`}>
      {formatStatus(status)}
    </Badge>
  );
}

function DetailRow({ label, value }: { label: string; value?: string | null }) {
  if (!value) return null;
  return (
    <div className="grid grid-cols-[120px_1fr] gap-3 py-1.5 text-sm">
      <span className="text-muted-foreground">{label}</span>
      <span className="break-all">{value}</span>
    </div>
  );
}

function JsonBlock({
  title,
  value,
  emptyText,
}: {
  title: string;
  value: unknown;
  emptyText: string;
}) {
  const hasValue = value !== null && value !== undefined && !isEmptyObject(value);

  return (
    <section className="space-y-2">
      <h3 className="text-sm font-medium">{title}</h3>
      {hasValue ? (
        <pre className="max-h-80 overflow-auto rounded-md border bg-muted/60 p-3 text-xs whitespace-pre-wrap break-all">
          {JSON.stringify(value, null, 2)}
        </pre>
      ) : (
        <div className="rounded-md border px-3 py-5 text-sm text-muted-foreground">{emptyText}</div>
      )}
    </section>
  );
}

function stringArray(value: unknown) {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === "string" && item.length > 0);
}

function DomainSearchEvidenceSummary({
  evidence,
}: {
  evidence: BrregDomainSearchEvidence[];
}) {
  if (!Array.isArray(evidence) || evidence.length === 0) {
    return (
      <section className="space-y-2">
        <h3 className="text-sm font-medium">Domain search evidence</h3>
        <div className="rounded-md border px-3 py-5 text-sm text-muted-foreground">
          No search-page evidence recorded.
        </div>
      </section>
    );
  }

  return (
    <section className="space-y-3">
      <h3 className="text-sm font-medium">Domain search evidence</h3>
      {evidence.map((item) => {
        const links = stringArray(item.links);
        return (
          <div key={item.action_attempt_id} className="space-y-3 rounded-md border p-3">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <p className="text-sm font-medium">{item.search_engine || "search"}</p>
                <p className="text-xs text-muted-foreground">{formatDate(item.artifact_created_at || item.started_at)}</p>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <StatusBadge status={item.action_status} />
                <StatusBadge status={item.crawl_status} />
              </div>
            </div>

            <div className="space-y-1">
              <DetailRow label="Search term" value={item.search_term} />
              <DetailRow label="Search URL" value={item.search_url} />
              <DetailRow label="Final URL" value={item.final_url} />
              <DetailRow label="Markdown hash" value={item.markdown_hash} />
            </div>

            {links.length > 0 && (
              <div className="space-y-1">
                <p className="text-xs text-muted-foreground">Links</p>
                <div className="max-h-32 overflow-auto rounded-md border bg-muted/40 p-2">
                  {links.slice(0, 30).map((link) => (
                    <a
                      key={link}
                      className="block truncate text-xs text-primary underline-offset-4 hover:underline"
                      href={link}
                      rel="noreferrer"
                      target="_blank"
                    >
                      {link}
                    </a>
                  ))}
                </div>
              </div>
            )}

            {item.markdown ? (
              <div className="space-y-1">
                <p className="text-xs text-muted-foreground">Search result markdown</p>
                <pre className="max-h-72 overflow-auto rounded-md border bg-muted/60 p-3 text-xs whitespace-pre-wrap break-words">
                  {item.markdown}
                </pre>
              </div>
            ) : null}
          </div>
        );
      })}
    </section>
  );
}

function TaskSummary({ detail }: { detail: BrregRawRecordDetail }) {
  const taskStatuses = useMemo(() => Object.entries(detail.task_statuses ?? {}), [detail.task_statuses]);
  const tasks = detail.tasks ?? [];

  return (
    <section className="space-y-3">
      <h3 className="text-sm font-medium">Task state</h3>
      {taskStatuses.length > 0 ? (
        <div className="grid gap-2 sm:grid-cols-2">
          {taskStatuses.map(([task, status]) => (
            <div key={task} className="rounded-md border p-3">
              <div className="flex items-center justify-between gap-3">
                <span className="text-sm font-medium">{task.replace(/_/g, " ")}</span>
                <StatusBadge status={status} />
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="rounded-md border px-3 py-5 text-sm text-muted-foreground">No task state recorded.</div>
      )}

      {tasks.length > 0 && (
        <div className="space-y-2">
          {tasks.map((task, index) => (
            <div key={`${String(task.task_type ?? "task")}-${index}`} className="rounded-md border p-3 text-sm">
              <div className="flex flex-wrap items-center gap-2">
                <span className="font-medium">{String(task.task_type ?? "task").replace(/_/g, " ")}</span>
                <StatusBadge status={typeof task.status === "string" ? task.status : undefined} />
                {typeof task.attempt_count === "number" && (
                  <span className="text-xs text-muted-foreground">attempts {task.attempt_count}</span>
                )}
              </div>
              {typeof task.last_error === "string" && task.last_error && (
                <p className="mt-2 text-xs text-red-700">{task.last_error}</p>
              )}
              {typeof task.error_category === "string" && task.error_category && (
                <p className="mt-1 text-xs text-muted-foreground">
                  {task.error_category}
                  {typeof task.error_code === "string" && task.error_code ? ` - ${task.error_code}` : ""}
                </p>
              )}
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

export function BrregRawRecordDetailSheet({
  id,
  open,
  onClose,
}: {
  id: string | null;
  open: boolean;
  onClose: () => void;
}) {
  const [detail, setDetail] = useState<BrregRawRecordDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();

  useEffect(() => {
    if (!open || !id) {
      setDetail(null);
      setError(undefined);
      setLoading(false);
      return;
    }

    let cancelled = false;
    setDetail(null);
    setError(undefined);
    setLoading(true);
    api.getBrregRawRecord(id)
      .then((result) => {
        if (!cancelled) setDetail(result);
      })
      .catch((err) => {
        if (!cancelled) setError(errorMessage(err, "Failed to load BRREG raw record."));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [id, open]);

  return (
    <Sheet open={open} onOpenChange={(value) => !value && onClose()}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-4xl">
        {loading && (
          <div className="space-y-3 p-4 pt-6">
            <Skeleton className="h-6 w-2/3" />
            <Skeleton className="h-4 w-1/2" />
            <Skeleton className="h-24 w-full" />
            <Skeleton className="h-40 w-full" />
          </div>
        )}

        {error && !loading && (
          <div className="p-4 pt-6">
            <SheetHeader className="px-0 pb-4">
              <SheetTitle>BRREG raw record</SheetTitle>
            </SheetHeader>
            <Alert variant="destructive">
              <AlertCircle className="size-4" />
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          </div>
        )}

        {detail && (
          <div className="space-y-5 p-4 pt-6">
            <SheetHeader className="px-0 pb-1">
              <SheetTitle className="text-lg leading-snug">
                {detail.organization_name || detail.organization_number}
              </SheetTitle>
              <div className="flex flex-wrap items-center gap-2 pt-1">
                <Badge variant="outline" className="font-mono text-xs">{detail.organization_number}</Badge>
                <StatusBadge status={detail.lifecycle_state} />
                <span className="text-xs text-muted-foreground">last seen {formatDate(detail.last_seen_at)}</span>
              </div>
            </SheetHeader>

            <section>
              <DetailRow label="Website" value={detail.website || detail.best_domain} />
              <DetailRow label="Status" value={detail.registration_status} />
              <DetailRow label="Country" value={detail.country_iso2} />
              <DetailRow label="Payload hash" value={detail.payload_hash} />
            </section>

            <div className="grid gap-2 sm:grid-cols-4">
              <div className="rounded-md border p-3">
                <p className="mb-2 text-xs text-muted-foreground">Translation</p>
                <StatusBadge status={detail.translation_status} />
              </div>
              <div className="rounded-md border p-3">
                <p className="mb-2 text-xs text-muted-foreground">Domain</p>
                <StatusBadge status={detail.domain_status} />
              </div>
              <div className="rounded-md border p-3">
                <p className="mb-2 text-xs text-muted-foreground">Financial</p>
                <StatusBadge status={detail.financial_status} />
              </div>
              <div className="rounded-md border p-3">
                <p className="mb-2 text-xs text-muted-foreground">Enhanced</p>
                <StatusBadge status={detail.enhanced_status} />
              </div>
            </div>

            <Separator />
            <TaskSummary detail={detail} />
            <Separator />
            <DomainSearchEvidenceSummary evidence={detail.domain_search_evidence ?? []} />
            <Separator />
            <JsonBlock title="Raw payload" value={detail.raw_payload} emptyText="No raw payload available." />
            <JsonBlock title="Translation result" value={detail.translation_result} emptyText="No translation result available." />
            <JsonBlock title="Domain result" value={detail.domain_result} emptyText="No domain result available." />
            <JsonBlock title="Financial result" value={detail.financial_result} emptyText="No financial result available." />
            <JsonBlock title="Enhanced result" value={detail.enhanced_result} emptyText="No enhanced result available." />
          </div>
        )}
      </SheetContent>
    </Sheet>
  );
}
