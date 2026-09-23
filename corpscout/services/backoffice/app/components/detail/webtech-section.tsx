import { TechnologyLabel } from "~/components/detail/technology-label";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
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
import type { getDomainWebtech } from "~/lib/webtech.server";

export function WebtechSection({
  data,
  linkTechnologies = false,
  site,
}: {
  data: Awaited<ReturnType<typeof getDomainWebtech>>;
  linkTechnologies?: boolean;
  site?: string;
}) {
  const { domain, scan, detections, catalog } = data;
  if (!scan) {
    return (
      <Empty className="min-h-48 border">
        <EmptyHeader>
          <EmptyTitle>No Webtech scan available</EmptyTitle>
          <EmptyDescription>
            No Webtech scan results have been recorded for {site || domain} yet.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  const incomplete =
    scan.outcome !== "success" ||
    detections.some((row) => !row.analysis_complete);
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <CardTitle>Web technologies</CardTitle>
          <Badge variant="secondary">{detections.length} technologies</Badge>
        </div>
        <CardDescription>
          Technologies detected by the latest Webtech scan of {site || domain}.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-5">
        <dl className="grid gap-x-6 gap-y-3 text-sm sm:grid-cols-2 xl:grid-cols-4">
          <div>
            <dt className="text-muted-foreground">Scanned at (UTC)</dt>
            <dd>{scan.scanned_at}</dd>
          </div>
          <div>
            <dt className="text-muted-foreground">Scan outcome</dt>
            <dd>{scan.outcome.replaceAll("_", " ")}</dd>
          </div>
          <div>
            <dt className="text-muted-foreground">Detector</dt>
            <dd>{scan.detector_version}</dd>
          </div>
          <div>
            <dt className="text-muted-foreground">Crawl</dt>
            <dd>{scan.crawl_id}</dd>
          </div>
          <div className="sm:col-span-2 xl:col-span-4">
            <dt className="text-muted-foreground">Scanned page</dt>
            <dd className="break-all">
              {scan.final_url || scan.requested_url}
            </dd>
          </div>
        </dl>
        {incomplete ? (
          <Alert>
            <AlertTitle>Scan did not complete fully</AlertTitle>
            <AlertDescription>
              These are the detections collected before the scan ended. Other
              technologies may be present.
            </AlertDescription>
          </Alert>
        ) : null}
        {detections.length !== scan.technology_count ? (
          <Alert>
            <AlertTitle>Detection details are incomplete</AlertTitle>
            <AlertDescription>
              The scan reports {scan.technology_count} technologies;{" "}
              {detections.length} detection details are currently available.
            </AlertDescription>
          </Alert>
        ) : null}
        {detections.length === 0 ? (
          <Empty className="min-h-40 border">
            <EmptyHeader>
              <EmptyTitle>
                {scan.technology_count === 0
                  ? "No technologies detected in this scan"
                  : "Detection details are not available yet"}
              </EmptyTitle>
              <EmptyDescription>
                {incomplete
                  ? "The scan was incomplete, so this does not establish that the domain uses no web technologies."
                  : "No technology details are available from the latest scan."}
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Technology</TableHead>
                <TableHead>Version</TableHead>
                <TableHead>Categories</TableHead>
                <TableHead>Confidence</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {detections.map((row) => {
                const entry =
                  row.technology_id === null
                    ? undefined
                    : catalog[row.technology];
                return (
                  <TableRow key={row.detected_name}>
                    <TableCell className="max-w-md whitespace-normal">
                      <div className="flex flex-col gap-1">
                        <TechnologyLabel
                          name={row.technology || row.detected_name}
                          entry={entry}
                          linkToCatalog={linkTechnologies}
                        />
                        {entry?.description ? (
                          <p className="text-muted-foreground text-xs">
                            {entry.description}
                          </p>
                        ) : null}
                        {row.technology &&
                        row.technology !== row.detected_name ? (
                          <p className="text-muted-foreground text-xs">
                            Detected as {row.detected_name}
                          </p>
                        ) : null}
                        {row.technology_id === null ? (
                          <Badge variant="outline">
                            {row.catalog_match === "ambiguous"
                              ? "Ambiguous catalog match"
                              : "Not in catalog"}
                          </Badge>
                        ) : null}
                      </div>
                    </TableCell>
                    <TableCell>{row.version || "—"}</TableCell>
                    <TableCell className="max-w-sm whitespace-normal">
                      <div className="flex flex-wrap gap-1">
                        {row.categories.map((category) => (
                          <Badge key={category} variant="outline">
                            {category}
                          </Badge>
                        ))}
                      </div>
                    </TableCell>
                    <TableCell className="tabular-nums">
                      {row.confidence}%
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  );
}
