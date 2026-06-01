import { Link } from "react-router";
import type { DataSource } from "~/types/api";
import { formatDate, timeAgo } from "~/lib/utils";
import { Badge } from "~/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

interface SourcesTableProps {
  sources: DataSource[];
}

function autoUpdateStatus(source: DataSource): { label: string; detail: string; className: string } {
  const hasAutoSchedule = (source.schedule_kind === "interval" || source.schedule_kind === "cron") && source.schedule_expression;
  if (!hasAutoSchedule) {
    return {
      label: "Manual",
      detail: "Manual only",
      className: "text-muted-foreground",
    };
  }

  if (!source.enabled || !source.schedule_enabled) {
    return {
      label: "Paused",
      detail: "Auto update off",
      className: "border-amber-200 bg-amber-100 text-amber-800",
    };
  }

  return {
    label: "On",
    detail: `${source.schedule_kind} schedule`,
    className: "border-green-200 bg-green-100 text-green-800",
  };
}

function TimestampCell({ iso, empty }: { iso: string | null; empty: string }) {
  if (!iso) {
    return <span className="text-sm text-muted-foreground">{empty}</span>;
  }

  return (
    <div className="space-y-0.5 text-sm">
      <div className="font-medium">{timeAgo(iso)}</div>
      <div className="text-xs text-muted-foreground">{formatDate(iso)}</div>
    </div>
  );
}

function nextScheduledText(source: DataSource): string {
  if (source.next_scheduled_at) return "";
  if (
    source.enabled &&
    source.schedule_enabled &&
    source.schedule_kind === "interval" &&
    source.schedule_expression &&
    !source.last_started_at
  ) {
    return "Next scheduler check";
  }
  return "Not scheduled";
}

function sourceCategoryLabel(value: string): string {
  const labels: Record<string, string> = {
    ai_research: "AI Research",
    security_identifier: "Security Identifier",
  };
  if (labels[value]) return labels[value];

  return value
    .split("_")
    .filter(Boolean)
    .map((part) => part[0]?.toUpperCase() + part.slice(1))
    .join(" ");
}

export function SourcesTable({ sources }: SourcesTableProps) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Source</TableHead>
          <TableHead>Category</TableHead>
          <TableHead>Auto update</TableHead>
          <TableHead>Schedule</TableHead>
          <TableHead>Last triggered</TableHead>
          <TableHead>Next scheduled</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {sources.map((s) => {
          const status = autoUpdateStatus(s);
          const nextFallback = nextScheduledText(s);

          return (
            <TableRow key={s.name}>
              <TableCell className="min-w-[18rem] font-medium">
                <Link
                  to={`/sources/${s.name}`}
                  className="text-foreground hover:underline"
                >
                  {s.display_name || s.name}
                </Link>
                {s.description && (
                  <p className="mt-1 max-w-xl text-xs leading-5 text-muted-foreground line-clamp-2">
                    {s.description}
                  </p>
                )}
              </TableCell>
              <TableCell>
                <Badge className="text-muted-foreground" variant="outline">
                  {sourceCategoryLabel(s.source_group)}
                </Badge>
              </TableCell>
              <TableCell>
                <div className="space-y-1">
                  <Badge className={status.className} variant="outline">
                    {status.label}
                  </Badge>
                  <div className="text-xs text-muted-foreground">{status.detail}</div>
                </div>
              </TableCell>
              <TableCell>
                {s.schedule_expression ? (
                  <div className="space-y-1">
                    <div className="font-mono text-sm">{s.schedule_expression}</div>
                    <div className="text-xs text-muted-foreground">{s.schedule_kind}</div>
                  </div>
                ) : (
                  <span className="text-sm text-muted-foreground">-</span>
                )}
              </TableCell>
              <TableCell>
                <TimestampCell iso={s.last_started_at} empty="Never" />
              </TableCell>
              <TableCell>
                {s.next_scheduled_at ? (
                  <TimestampCell iso={s.next_scheduled_at} empty="Not scheduled" />
                ) : (
                  <span className="text-sm text-muted-foreground">{nextFallback}</span>
                )}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}
