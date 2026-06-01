import { useEffect, useState } from "react";
import { api } from "~/lib/api";
import type { RawInputDetail } from "~/types/api";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Separator } from "~/components/ui/separator";
import { Skeleton } from "~/components/ui/skeleton";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";

const SOURCE_LABELS: Record<string, string> = {
  companies_house: "Companies House",
};

function timeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime();
  const minutes = Math.floor(diff / 60_000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  const months = Math.floor(days / 30);
  if (months < 12) return `${months}mo ago`;
  return `${Math.floor(months / 12)}y ago`;
}

function statusBadgeVariant(status: string): "default" | "secondary" | "destructive" | "outline" {
  switch (status) {
    case "pending": return "default";
    case "processing": return "secondary";
    case "processed": return "outline";
    case "failed": return "destructive";
    default: return "outline";
  }
}

function DetailRow({ label, value }: { label: string; value: React.ReactNode }) {
  if (!value && value !== 0) return null;
  return (
    <div className="grid grid-cols-[140px_1fr] gap-2 py-1.5">
      <span className="text-sm text-muted-foreground">{label}</span>
      <span className="text-sm break-all">{value}</span>
    </div>
  );
}

export function RawInputDetailSheet({
  open,
  onClose,
  source,
  id,
}: {
  open: boolean;
  onClose: () => void;
  source: string;
  id: string;
}) {
  const [detail, setDetail] = useState<RawInputDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [jsonExpanded, setJsonExpanded] = useState(false);
  const [error, setError] = useState<string>();

  useEffect(() => {
    if (!open || !id) {
      setDetail(null);
      setError(undefined);
      return;
    }
    let cancelled = false;
    setDetail(null);
    setError(undefined);
    setJsonExpanded(false);
    setLoading(true);
    api.getRawInput(source, id)
      .then((result) => {
        if (!cancelled) setDetail(result);
      })
      .catch(() => {
        if (!cancelled) setError("Failed to load raw input details.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [open, source, id]);

  const typeLabel = detail?.source === "companies_house"
    ? detail.company_type
    : detail?.registration_status;

  return (
    <Sheet open={open} onOpenChange={(v) => !v && onClose()}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-3xl">
        {loading && (
          <div className="space-y-3 p-4 pt-6">
            <Skeleton className="h-6 w-3/4" />
            <Skeleton className="h-4 w-1/2" />
            <Skeleton className="h-4 w-full" />
            <Skeleton className="h-4 w-full" />
          </div>
        )}
        {error && !loading && (
          <div className="p-4 pt-6">
            <SheetHeader className="pb-4">
              <SheetTitle>Raw input details</SheetTitle>
            </SheetHeader>
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          </div>
        )}
        {detail && (
          <>
            <SheetHeader className="pb-4">
              <SheetTitle className="text-lg leading-snug">{detail.name}</SheetTitle>
              <div className="flex items-center gap-2 mt-1">
                <Badge variant="outline" className="text-xs">
                  {SOURCE_LABELS[detail.source] ?? detail.source}
                </Badge>
                <Badge variant={statusBadgeVariant(detail.status)} className="text-xs">
                  {detail.status}
                </Badge>
                {detail.country_iso2 && (
                  <span className="text-xs text-muted-foreground">{detail.country_iso2}</span>
                )}
              </div>
            </SheetHeader>

            <div className="px-4 pb-4">
              <Separator className="mb-4" />

            <section className="space-y-0.5 mb-4">
              <DetailRow label="Native ID" value={<span className="font-mono text-xs">{detail.native_id}</span>} />
              <DetailRow label="Lifecycle state" value={detail.state} />
              <DetailRow label="Processing status" value={detail.status} />
              {typeLabel && <DetailRow label="Type" value={typeLabel} />}
              {detail.website && (
                <DetailRow label="Website" value={
                  <a href={detail.website} target="_blank" rel="noreferrer"
                     className="text-primary underline-offset-4 hover:underline">
                    {detail.website}
                  </a>
                } />
              )}
              {detail.run_id && <DetailRow label="Run ID" value={<span className="font-mono text-xs">{detail.run_id}</span>} />}
            </section>

            <Separator className="mb-4" />

            <section className="space-y-0.5 mb-4">
              <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">Timestamps</p>
              <DetailRow label="Created" value={`${new Date(detail.created_at).toLocaleString()} (${timeAgo(detail.created_at)})`} />
              <DetailRow label="First seen" value={`${new Date(detail.first_seen_at).toLocaleString()} (${timeAgo(detail.first_seen_at)})`} />
              <DetailRow label="Last seen" value={`${new Date(detail.last_seen_at).toLocaleString()} (${timeAgo(detail.last_seen_at)})`} />
              {detail.processed_at && (
                <DetailRow label="Processed" value={`${new Date(detail.processed_at).toLocaleString()} (${timeAgo(detail.processed_at)})`} />
              )}
              <DetailRow label="Updated" value={`${new Date(detail.updated_at).toLocaleString()} (${timeAgo(detail.updated_at)})`} />
            </section>

            <Separator className="mb-4" />

            <section className="space-y-0.5 mb-4">
              <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">Processing</p>
              <DetailRow label="Attempts" value={detail.processing_attempts} />
              <DetailRow label="Hash" value={<span className="font-mono text-xs">{detail.payload_hash.slice(0, 16)}…</span>} />
              {detail.processing_error && (
                <div className="mt-2 rounded-md bg-destructive/10 px-3 py-2">
                  <p className="text-xs font-medium text-destructive mb-1">Error</p>
                  <p className="text-xs text-destructive break-all">{detail.processing_error}</p>
                </div>
              )}
            </section>

            <Separator className="mb-4" />

            <section>
              <button
                className="flex w-full items-center justify-between text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2"
                onClick={() => setJsonExpanded((v) => !v)}
              >
                Raw payload
                <span className="normal-case font-normal">{jsonExpanded ? "hide" : "show"}</span>
              </button>
              {jsonExpanded && (
                <pre className="rounded-md bg-muted p-3 text-xs overflow-auto max-h-96 whitespace-pre-wrap break-all">
                  {JSON.stringify(detail.raw_payload, null, 2)}
                </pre>
              )}
            </section>
            </div>
          </>
        )}
      </SheetContent>
    </Sheet>
  );
}
