import { BriefcaseIcon } from "lucide-react";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
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
import type { SeCompanyJobRow } from "~/lib/se-company-jobs.server";

/** The date part of a ClickHouse DateTime64 string, or '' as it came. */
function day(value: string): string {
  return value.slice(0, 10);
}

/**
 * The company's job ads from corpscout.company_job_history, newest interval
 * first. Currently-open ads (still in company_job_current, or with no
 * recorded end) wear an `open` badge; an estimated end is marked rather than
 * passed off as a fact the source stated.
 */
export function SeCompanyJobsTab({ jobs }: { jobs: SeCompanyJobRow[] }) {
  if (jobs.length === 0) {
    return (
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <BriefcaseIcon />
          </EmptyMedia>
          <EmptyTitle>No job-ad data collected for this company</EmptyTitle>
          <EmptyDescription>
            The Platsbanken pipeline has not landed data yet, so
            company_job_history holds no ads -- for this company or any other.
            Once the pipeline runs, this tab fills in on its own.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  const openCount = jobs.filter((job) => job.is_open === 1).length;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Job ads</CardTitle>
        <p className="text-muted-foreground text-sm">
          {jobs.length} ad interval{jobs.length === 1 ? "" : "s"}
          {openCount > 0 ? ` · ${openCount} currently open` : ""}
        </p>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="overflow-x-auto">
          <Table className="min-w-[44rem]">
            <TableHeader>
              <TableRow>
                <TableHead>Headline</TableHead>
                <TableHead>Active</TableHead>
                <TableHead>Deadline</TableHead>
                <TableHead>Source</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {jobs.map((job) => (
                <TableRow
                  key={`${job.source_system}:${job.source_job_ad_id}:${job.interval_number}`}
                >
                  <TableCell className="align-top">
                    <div className="flex flex-wrap items-center gap-2">
                      <span>
                        {job.headline_original === ""
                          ? job.source_job_ad_id
                          : job.headline_original}
                      </span>
                      {job.is_open === 1 ? <Badge>open</Badge> : null}
                    </div>
                  </TableCell>
                  <TableCell className="tabular-nums align-top whitespace-nowrap">
                    {day(job.active_from)}
                    {" – "}
                    {job.active_to === "" ? (
                      <span className="text-muted-foreground">open-ended</span>
                    ) : (
                      <>
                        {day(job.active_to)}
                        {job.is_end_estimated === 1 ? (
                          <span
                            className="text-muted-foreground"
                            title={
                              job.active_to_basis === ""
                                ? "estimated end"
                                : `estimated end (${job.active_to_basis})`
                            }
                          >
                            {" "}
                            (est.)
                          </span>
                        ) : null}
                      </>
                    )}
                  </TableCell>
                  <TableCell className="tabular-nums align-top whitespace-nowrap">
                    {job.application_deadline === "" ? (
                      <span className="text-muted-foreground">—</span>
                    ) : (
                      day(job.application_deadline)
                    )}
                  </TableCell>
                  <TableCell className="align-top">
                    <Badge variant="secondary">{job.source_system}</Badge>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
        <p className="text-muted-foreground text-xs">
          One row per active interval of an ad: an ad taken down and
          republished appears once per interval. Ends marked (est.) are
          estimated by the pipeline, not stated by the source.
        </p>
      </CardContent>
    </Card>
  );
}
