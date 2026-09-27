import { Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { flagFeed, rangeStatus, type FeedRun, type IndicatorRules, type IPRange } from "~/lib/provider-recon";

const STATUS_VARIANT: Record<string, "default" | "secondary" | "destructive" | "outline"> = {
  ok: "secondary",
  active: "secondary",
  stale: "outline",
  missing: "outline",
  failed: "destructive",
  removed: "destructive",
};

export function StatusBadge({ status }: { status: string }) {
  return <Badge variant={STATUS_VARIANT[status] ?? "outline"}>{status}</Badge>;
}

/** One row per feed; rows the warning rules flag are highlighted with their reasons. */
export function FeedTable({ feeds, rules }: { feeds: FeedRun[]; rules: IndicatorRules }) {
  if (feeds.length === 0) return <p className="text-sm text-muted-foreground">No feeds in this run.</p>;
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Provider</TableHead>
          <TableHead>Feed</TableHead>
          <TableHead>Status</TableHead>
          <TableHead className="text-right">Ranges</TableHead>
          <TableHead className="text-right">Added</TableHead>
          <TableHead className="text-right">Missing</TableHead>
          <TableHead className="text-right">Reappeared</TableHead>
          <TableHead className="text-right">Removed</TableHead>
          <TableHead className="text-right">Purged</TableHead>
          <TableHead>Warnings</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {feeds.map((feed) => {
          const flags = flagFeed(feed, rules);
          const flagged = flags.length > 0;
          return (
            <TableRow key={`${feed.slug}/${feed.collector}`} data-flagged={flagged ? "true" : "false"} className={flagged ? "bg-destructive/5" : undefined}>
              <TableCell>
                <Link className="font-medium underline-offset-4 hover:underline" to={`/admin/provider-feeds/providers/${feed.slug}`}>
                  {feed.slug}
                </Link>
              </TableCell>
              <TableCell className="font-mono text-xs">{feed.collector}</TableCell>
              <TableCell><StatusBadge status={feed.status} /></TableCell>
              <TableCell className="text-right tabular-nums">{feed.items}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.added}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.missing}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.reappeared}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.removed}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.purged}</TableCell>
              <TableCell>
                {flagged ? (
                  <ul className="flex flex-col gap-1 text-sm text-destructive">
                    {flags.map((f) => <li key={f.kind}>{f.message}</li>)}
                  </ul>
                ) : (
                  <span className="text-sm text-muted-foreground">—</span>
                )}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

/** Missing and removed ranges with their lifecycle dates. */
export function RangeTable({ rows }: { rows: { serviceKey: string; range: IPRange }[] }) {
  if (rows.length === 0) return <p className="text-sm text-muted-foreground">Every range is active.</p>;
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Range</TableHead>
          <TableHead>Service</TableHead>
          <TableHead>Feed</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>First seen</TableHead>
          <TableHead>Last seen</TableHead>
          <TableHead>Missing since</TableHead>
          <TableHead>Removed</TableHead>
          <TableHead>Action</TableHead>
          <TableHead>Restored</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map(({ serviceKey, range }) => (
          <TableRow key={`${serviceKey}/${range.cidr}/${range.first_seen ?? ""}`}>
            <TableCell className="font-mono text-xs">{range.cidr}</TableCell>
            <TableCell className="text-sm">{serviceKey}</TableCell>
            <TableCell className="font-mono text-xs">{range.collector ?? "curated"}</TableCell>
            <TableCell><StatusBadge status={rangeStatus(range)} /></TableCell>
            <TableCell className="tabular-nums">{range.first_seen || "—"}</TableCell>
            <TableCell className="tabular-nums">{range.last_seen || "—"}</TableCell>
            <TableCell className="tabular-nums">{range.missing_since ?? "—"}</TableCell>
            <TableCell className="tabular-nums">{range.removed_at ?? "—"}</TableCell>
            <TableCell className="text-sm">{range.removal_action ?? "—"}</TableCell>
            <TableCell className="tabular-nums">{range.restored_at ?? "—"}</TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
