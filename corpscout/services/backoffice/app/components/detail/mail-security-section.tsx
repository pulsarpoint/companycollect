import { MailWarning } from "lucide-react";
import { Badge } from "~/components/ui/badge";
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
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import type {
  MailControlStatus,
  MailSecurityReport,
} from "~/lib/mail-security";
import { cn } from "~/lib/utils";

// The "Mail security" technology sub-tab body: a score card plus the
// per-control verdict table. The score is computed live from crawled DNS
// records (see ~/lib/mail-security); the card says so explicitly because a
// crawl-derived score makes weaker claims than runner3's live probe.

const dateFormat = new Intl.DateTimeFormat("en-GB", {
  dateStyle: "medium",
  timeStyle: "short",
  timeZone: "UTC",
});

function formatStamp(stamp: string): string {
  if (stamp === "") return "unknown date";
  const parsed = new Date(stamp.replace(" ", "T") + "Z");
  if (Number.isNaN(parsed.getTime())) return stamp;
  return `${dateFormat.format(parsed)} UTC`;
}

const statusStyles: Record<MailControlStatus, string> = {
  pass: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-400",
  warn: "bg-amber-500/15 text-amber-700 dark:text-amber-400",
  fail: "bg-destructive/10 text-destructive dark:bg-destructive/20",
  unknown: "bg-muted text-muted-foreground",
};

export function MailSecurityStatusBadge({
  status,
}: {
  status: MailControlStatus;
}) {
  return <Badge className={cn(statusStyles[status])}>{status}</Badge>;
}

function scoreTone(score: number): string {
  if (score >= 70) return "text-emerald-600 dark:text-emerald-400";
  if (score >= 40) return "text-amber-600 dark:text-amber-400";
  return "text-destructive";
}

export function MailSecuritySection({
  report,
}: {
  report: MailSecurityReport;
}) {
  return (
    <div className="flex flex-col gap-5">
      <Card>
        <CardHeader>
          <CardTitle>Mail security score</CardTitle>
          <CardDescription>
            Computed live from crawled DNS records for {report.domain} — not a
            live probe. SMTP, MTA-STS policy fetch, and DKIM selector
            enumeration are out of scope.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap items-end gap-x-6 gap-y-3">
            <div className="flex items-baseline gap-1">
              <span
                className={cn(
                  "text-5xl font-semibold tabular-nums",
                  scoreTone(report.score),
                )}
              >
                {report.score}
              </span>
              <span className="text-muted-foreground text-lg">/100</span>
            </div>
            <div className="flex flex-col gap-1.5 pb-1">
              <Badge variant={report.mail_ready ? "secondary" : "destructive"}>
                {report.mail_ready ? "Mail ready" : "Not mail ready"}
              </Badge>
              <span className="text-muted-foreground text-xs">
                as of {formatStamp(report.last_seen)}
              </span>
            </div>
            <div className="text-muted-foreground flex flex-wrap gap-x-4 gap-y-1 pb-1 text-xs">
              <span>MX hosts: {report.summary.mx_host_count}</span>
              <span>
                DNSSEC: {report.summary.dnssec_available ? "signed" : "not observed"}
              </span>
              <span>Detections: {report.summary.total_detections}</span>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Controls</CardTitle>
          <CardDescription>
            Per-control verdicts following runner3&apos;s mail function: warn
            −10, fail −20, unknown −15 from a starting score of 100.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Control</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Reasons</TableHead>
                  <TableHead>Evidence</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {report.controls.map((control) => (
                  <TableRow key={control.key}>
                    <TableCell className="font-medium whitespace-nowrap">
                      {control.label}
                    </TableCell>
                    <TableCell>
                      <MailSecurityStatusBadge status={control.status} />
                    </TableCell>
                    <TableCell>
                      <ul className="max-w-xl list-none space-y-1 text-sm">
                        {control.reasons.map((reason) => (
                          <li key={reason}>{reason}</li>
                        ))}
                      </ul>
                    </TableCell>
                    <TableCell>
                      {control.evidence.length > 0 ? (
                        <details>
                          <summary className="text-muted-foreground cursor-pointer text-xs select-none">
                            {control.evidence.length} record
                            {control.evidence.length === 1 ? "" : "s"}
                          </summary>
                          <ul className="mt-1 max-w-xl space-y-1">
                            {control.evidence.map((value) => (
                              <li
                                key={value}
                                className="bg-muted rounded px-1.5 py-0.5 font-mono text-xs break-all"
                              >
                                {value}
                              </li>
                            ))}
                          </ul>
                        </details>
                      ) : (
                        <span className="text-muted-foreground text-xs">—</span>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

/** Rendered when the crawl holds no DNS rows at all for the selected
 * domain: scoring pure absence would judge the crawl, not the domain. */
export function MailSecurityNoRecords({ domain }: { domain: string }) {
  return (
    <Empty className="border">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <MailWarning />
        </EmptyMedia>
        <EmptyTitle>No DNS records held for this domain</EmptyTitle>
        <EmptyDescription>
          The crawl has no mail-relevant DNS records for {domain}, so no mail
          security score can be computed.
        </EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}
