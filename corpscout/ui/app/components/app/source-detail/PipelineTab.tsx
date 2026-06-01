import type { DataSource } from "~/types/api";
import { Badge } from "~/components/ui/badge";

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  if (!value && value !== 0) return null;
  return (
    <div className="grid grid-cols-[180px_1fr] gap-2 py-1.5">
      <span className="text-sm text-muted-foreground">{label}</span>
      <span className="text-sm">{value}</span>
    </div>
  );
}

function fmt(dateStr?: string) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  const diff = Date.now() - d.getTime();
  const days = Math.floor(diff / 86_400_000);
  const ago = days === 0 ? "today" : days === 1 ? "yesterday" : `${days}d ago`;
  return `${d.toLocaleDateString()} ${d.toLocaleTimeString()} (${ago})`;
}

const MODE_LABELS: Record<string, string> = {
  bulk: "Bulk (next trigger will download full export)",
  incremental: "Incremental (fetching new records since bulk)",
  none: "Not started",
};

export function PipelineTab({ source }: { source: DataSource }) {
  const cp = source.sync_checkpoint;

  if (!cp) {
    return (
      <div className="rounded-md border p-6 text-sm text-muted-foreground">
        No pipeline sync data yet. Trigger the source to start the initial bulk download.
      </div>
    );
  }

  const modeBadgeVariant =
    cp.mode === "incremental" ? "outline" : cp.mode === "bulk" ? "secondary" : "outline";

  return (
    <div className="rounded-md border divide-y">
      <div className="p-4 space-y-0.5">
        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-3">
          Pipeline Mode
        </p>
        <div className="flex items-center gap-3">
          <Badge variant={modeBadgeVariant} className="text-xs capitalize">
            {cp.mode === "none" ? "Not started" : cp.mode}
          </Badge>
          <span className="text-sm text-muted-foreground">
            {MODE_LABELS[cp.mode] ?? cp.mode}
          </span>
        </div>
      </div>

      <div className="p-4 space-y-0.5">
        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
          Bulk Download
        </p>
        {cp.bulk_date ? (
          <>
            <Row label="Bulk date" value={cp.bulk_date} />
            <Row label="Completed" value={fmt(cp.last_completed_at)} />
          </>
        ) : (
          <p className="text-sm text-muted-foreground">
            No bulk download completed yet.
          </p>
        )}
      </div>

      <div className="p-4 space-y-0.5">
        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
          Sync Cursor
        </p>
        <Row label="Cursor" value={<code className="font-mono text-xs bg-muted px-1 py-0.5 rounded">{cp.cursor || "—"}</code>} />
        <Row label="Last updated" value={fmt(cp.updated_at)} />
      </div>
    </div>
  );
}
