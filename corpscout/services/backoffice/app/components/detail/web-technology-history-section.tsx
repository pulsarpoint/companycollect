import { History, Layers3 } from "lucide-react";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "~/components/ui/accordion";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import {
  TechnologyIcon,
  TechnologyLabel,
} from "~/components/detail/technology-label";
import type { TechnologyCatalogEntry } from "~/lib/technology-catalog.server";
import type {
  CompanyWebTechnologyHistory,
  WebTechnologyCrawlSnapshot,
} from "~/lib/web-technology-history";

/** Detector name -> catalog entry (icon, description, website). */
export type TechnologyCatalog = Record<string, TechnologyCatalogEntry>;

function websiteHostname(website: string): string {
  try {
    return new URL(website).hostname.replace(/^www\./, "");
  } catch {
    return website;
  }
}

const numberFormat = new Intl.NumberFormat("en-US");
const dateFormat = new Intl.DateTimeFormat("en-GB", {
  dateStyle: "medium",
  timeStyle: "short",
  timeZone: "UTC",
});

function formatProcessedAt(value: string): string {
  const iso = value.includes("T") ? value : value.replace(" ", "T");
  const date = new Date(/[zZ]|[+-]\d\d:\d\d$/.test(iso) ? iso : `${iso}Z`);
  return Number.isNaN(date.getTime()) ? value : dateFormat.format(date);
}

function TechnologyStateBadge({
  state,
}: {
  state: "detected_latest" | "not_detected_latest";
}) {
  return state === "detected_latest" ? (
    <Badge variant="secondary">Detected in latest crawl</Badge>
  ) : (
    <Badge variant="outline">Not detected in latest crawl</Badge>
  );
}

function TechnologySummaryTable({
  history,
  catalog,
}: {
  history: CompanyWebTechnologyHistory;
  catalog: TechnologyCatalog;
}) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Technology</TableHead>
          <TableHead>Category</TableHead>
          <TableHead>Latest state</TableHead>
          <TableHead>Detection history</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {history.technologies.map((technology) => {
          const entry = catalog[technology.name];
          return (
          <TableRow key={technology.name}>
            <TableCell className="max-w-md whitespace-normal">
              <div className="flex flex-col gap-1">
                <span className="flex items-center gap-1.5 font-medium">
                  <TechnologyIcon name={technology.name} entry={entry} />
                  {technology.name}
                  {entry?.website ? (
                    <a
                      href={entry.website}
                      target="_blank"
                      rel="noreferrer"
                      className="text-muted-foreground text-xs font-normal underline underline-offset-2"
                    >
                      {websiteHostname(entry.website)}
                    </a>
                  ) : null}
                </span>
                {entry?.description ? (
                  <span className="text-muted-foreground line-clamp-2 text-xs">
                    {entry.description}
                  </span>
                ) : null}
                {technology.versions.length ? (
                  <span className="text-muted-foreground text-xs">
                    Version{technology.versions.length === 1 ? "" : "s"}{" "}
                    {technology.versions.join(", ")}
                  </span>
                ) : null}
              </div>
            </TableCell>
            <TableCell className="max-w-64 whitespace-normal">
              <div className="flex flex-wrap gap-1">
                {technology.categories.map((category) => (
                  <Badge key={category} variant="outline">
                    {category}
                  </Badge>
                ))}
              </div>
            </TableCell>
            <TableCell>
              <TechnologyStateBadge state={technology.state} />
            </TableCell>
            <TableCell className="text-muted-foreground min-w-56 text-xs tabular-nums whitespace-normal">
              <span className="font-mono">
                {technology.firstDetectedCrawl} → {technology.lastDetectedCrawl}
              </span>
              <br />
              {technology.detectedCrawlCount}/{history.snapshots.length} crawls
              · {numberFormat.format(technology.lastDetectedPages)} pages in
              last detection
            </TableCell>
          </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

function CrawlChanges({
  snapshot,
  catalog,
}: {
  snapshot: WebTechnologyCrawlSnapshot;
  catalog: TechnologyCatalog;
}) {
  const hasChanges =
    snapshot.newlyDetected.length > 0 || snapshot.noLongerDetected.length > 0;

  if (!hasChanges) {
    return (
      <div className="flex items-center gap-2">
        <Badge variant="secondary">Baseline or unchanged snapshot</Badge>
      </div>
    );
  }

  return (
    <div className="grid gap-3 md:grid-cols-2">
      <div>
        <p className="text-muted-foreground mb-2 text-xs font-medium">
          Newly detected
        </p>
        <div className="flex flex-wrap gap-1.5">
          {snapshot.newlyDetected.length ? (
            snapshot.newlyDetected.map((technology) => (
              <Badge
                key={technology}
                variant="secondary"
                title={catalog[technology]?.description || undefined}
              >
                <TechnologyIcon name={technology} entry={catalog[technology]} />
                {technology}
              </Badge>
            ))
          ) : (
            <span className="text-muted-foreground text-xs">None</span>
          )}
        </div>
      </div>
      <div>
        <p className="text-muted-foreground mb-2 text-xs font-medium">
          No longer detected in this crawl
        </p>
        <div className="flex flex-wrap gap-1.5">
          {snapshot.noLongerDetected.length ? (
            snapshot.noLongerDetected.map((technology) => (
              <Badge
                key={technology}
                variant="outline"
                title={catalog[technology]?.description || undefined}
              >
                <TechnologyIcon name={technology} entry={catalog[technology]} />
                {technology}
              </Badge>
            ))
          ) : (
            <span className="text-muted-foreground text-xs">None</span>
          )}
        </div>
      </div>
    </div>
  );
}

function SnapshotDetectionTable({
  snapshot,
  catalog,
}: {
  snapshot: WebTechnologyCrawlSnapshot;
  catalog: TechnologyCatalog;
}) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Technology</TableHead>
          <TableHead>Category / version</TableHead>
          <TableHead>Confidence</TableHead>
          <TableHead>Detected pages</TableHead>
          <TableHead>Example pages</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {snapshot.detections.map((detection) => (
          <TableRow key={detection.name}>
            <TableCell className="font-medium">
              <TechnologyLabel
                name={detection.name}
                entry={catalog[detection.name]}
              />
            </TableCell>
            <TableCell className="max-w-64 whitespace-normal">
              <div className="flex flex-wrap gap-1">
                {detection.categories.map((category) => (
                  <Badge key={category} variant="outline">
                    {category}
                  </Badge>
                ))}
                {detection.versions.map((version) => (
                  <Badge key={version} variant="secondary">
                    v{version}
                  </Badge>
                ))}
              </div>
            </TableCell>
            <TableCell className="tabular-nums">
              {detection.confidence}%
            </TableCell>
            <TableCell className="tabular-nums">
              {numberFormat.format(detection.detectedPages)} of{" "}
              {numberFormat.format(snapshot.observedPages)}
            </TableCell>
            <TableCell className="max-w-sm whitespace-normal">
              <div className="flex flex-col gap-1">
                {detection.sampleUrls.slice(0, 3).map((url) => (
                  <a
                    key={url}
                    href={url}
                    target="_blank"
                    rel="noreferrer"
                    className="text-muted-foreground truncate text-xs underline underline-offset-2"
                  >
                    {url}
                  </a>
                ))}
              </div>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

function SnapshotSummary({
  snapshot,
}: {
  snapshot: WebTechnologyCrawlSnapshot;
}) {
  const changeCount =
    snapshot.newlyDetected.length + snapshot.noLongerDetected.length;
  return (
    <div className="grid min-w-0 flex-1 grid-cols-1 items-center gap-2 pr-3 sm:grid-cols-[minmax(12rem,1fr)_auto_auto]">
      <div>
        <p className="font-mono font-medium">{snapshot.crawlId}</p>
        <p className="text-muted-foreground mt-1 text-xs tabular-nums">
          Processed {formatProcessedAt(snapshot.processedAt)}
        </p>
      </div>
      <span className="text-muted-foreground text-xs tabular-nums">
        {numberFormat.format(snapshot.observedPages)} pages ·{" "}
        {numberFormat.format(snapshot.detections.length)} technologies
      </span>
      <Badge variant={changeCount ? "secondary" : "outline"}>
        {changeCount
          ? `${numberFormat.format(changeCount)} detection change${changeCount === 1 ? "" : "s"}`
          : "No detection changes"}
      </Badge>
    </div>
  );
}

export function WebTechnologyHistorySection({
  history,
  catalog = {},
}: {
  history: CompanyWebTechnologyHistory;
  catalog?: TechnologyCatalog;
}) {
  const detectedLatest = history.technologies.filter(
    (technology) => technology.state === "detected_latest",
  ).length;
  const previouslyDetected = history.technologies.length - detectedLatest;
  const latestSnapshot = history.snapshots[0];

  return (
    <div className="flex flex-col gap-5">
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <CardTitle>Web application technologies</CardTitle>
              <CardDescription className="mt-1">
                Technologies detected on archived pages for the selected domain{" "}
                <span className="font-mono">{history.domain}</span>.
              </CardDescription>
            </div>
            {history.latestCrawlId ? (
              <Badge variant="secondary">Latest {history.latestCrawlId}</Badge>
            ) : null}
          </div>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <div className="flex flex-wrap gap-x-6 gap-y-3 text-sm">
            <div className="flex items-center gap-2">
              <Layers3 className="text-muted-foreground size-4" />
              <span className="font-medium tabular-nums">
                {numberFormat.format(detectedLatest)} detected in latest crawl
              </span>
            </div>
            <span className="text-muted-foreground tabular-nums">
              {numberFormat.format(previouslyDetected)} previously detected
            </span>
            <span className="text-muted-foreground tabular-nums">
              {numberFormat.format(history.snapshots.length)} crawl snapshots
            </span>
            {latestSnapshot ? (
              <span className="text-muted-foreground tabular-nums">
                {numberFormat.format(latestSnapshot.observedPages)} pages in
                latest snapshot
              </span>
            ) : null}
          </div>
          <TechnologySummaryTable history={history} catalog={catalog} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex items-start gap-3">
            <History className="text-muted-foreground mt-0.5 size-4" />
            <div>
              <CardTitle>Detection history</CardTitle>
              <CardDescription className="mt-1 max-w-4xl">
                Each snapshot compares positive detections with the preceding
                Common Crawl. Page coverage varies, so a missing detection does
                not prove installation or removal.
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <Accordion
            multiple
            defaultValue={history.latestCrawlId ? [history.latestCrawlId] : []}
            className="overflow-hidden rounded-xl border bg-background"
            aria-label="Web technology detection history"
          >
            {history.snapshots.map((snapshot) => (
              <AccordionItem key={snapshot.crawlId} value={snapshot.crawlId}>
                <AccordionTrigger className="rounded-none px-4 py-4 hover:bg-muted/40 hover:no-underline aria-expanded:bg-muted/40 sm:px-5">
                  <SnapshotSummary snapshot={snapshot} />
                </AccordionTrigger>
                <AccordionContent className="pb-0">
                  <div className="flex flex-col gap-4 border-t bg-muted/10 px-4 py-4 sm:px-5">
                    <CrawlChanges snapshot={snapshot} catalog={catalog} />
                    <SnapshotDetectionTable
                      snapshot={snapshot}
                      catalog={catalog}
                    />
                  </div>
                </AccordionContent>
              </AccordionItem>
            ))}
          </Accordion>
        </CardContent>
      </Card>
    </div>
  );
}
