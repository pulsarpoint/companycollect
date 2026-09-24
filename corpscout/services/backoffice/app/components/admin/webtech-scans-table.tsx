import { Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import { WEBTECH_PAGE_SIZE, webtechDomainPath, webtechListPath, webtechScanPath, type WebtechView } from "~/lib/webtech";
import type { listWebtechScans } from "~/lib/webtech.server";

export function WebtechViewTabs({ basePath, view, prefix = "", showInputs = false }: { basePath: string; view: WebtechView | "input"; prefix?: string; showInputs?: boolean }) {
  return (
    <Tabs value={view}>
      <TabsList aria-label="Webtech results">
        <TabsTrigger value="latest" nativeButton={false} render={<Link to={webtechListPath(basePath, "latest", 1, prefix)} />}>
          Latest results
        </TabsTrigger>
        <TabsTrigger value="history" nativeButton={false} render={<Link to={webtechListPath(basePath, "history", 1, prefix)} />}>
          Scan history
        </TabsTrigger>
        {showInputs ? <TabsTrigger value="input" nativeButton={false} render={<Link to={`/admin/webtech/input${prefix ? `?${new URLSearchParams({ prefix })}` : ""}`} />}>
          Inputs
        </TabsTrigger> : null}
      </TabsList>
    </Tabs>
  );
}

export function WebtechScansTable({ result, basePath, view, page, prefix = "" }: {
  result: Awaited<ReturnType<typeof listWebtechScans>>;
  basePath: string;
  view: WebtechView;
  page: number;
  prefix?: string;
}) {
  const pages = Math.max(1, Math.ceil(result.total / WEBTECH_PAGE_SIZE));
  return (
    <div className="flex flex-col gap-4">
      <p className="text-sm text-muted-foreground" role="status">
        {result.total.toLocaleString()} {view === "latest" ? "latest page results" : "scans"} ·{" "}
        {result.domains.toLocaleString()} domains · {result.pages.toLocaleString()} requested pages
        {prefix ? " matching this filter" : ""}
      </p>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Domain / requested page</TableHead>
            <TableHead>Scanned at (UTC)</TableHead>
            <TableHead>Outcome</TableHead>
            <TableHead>Technologies</TableHead>
            <TableHead>Detector</TableHead>
            <TableHead><span className="sr-only">Details</span></TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {result.rows.map(scan => (
            <TableRow key={JSON.stringify([scan.root_domain, scan.website_origin, scan.page_url, scan.crawl_id, scan.detector_version, scan.scan_id, scan.report_sha256])}>
              <TableCell className="max-w-lg whitespace-normal">
                <Link className="font-medium underline underline-offset-2" to={webtechDomainPath(scan.root_domain)}>{scan.root_domain}</Link>
                <p className="break-all text-xs text-muted-foreground">{scan.page_url}</p>
              </TableCell>
              <TableCell className="tabular-nums">{scan.scanned_at}</TableCell>
              <TableCell><Badge variant={scan.outcome === "success" ? "secondary" : "outline"}>{scan.outcome.replaceAll("_", " ")}</Badge></TableCell>
              <TableCell className="tabular-nums">{scan.technology_count}</TableCell>
              <TableCell>{scan.detector_version}</TableCell>
              <TableCell><Link className="underline underline-offset-2" to={webtechScanPath(scan.root_domain, scan)}>View scan</Link></TableCell>
            </TableRow>
          ))}
          {!result.rows.length ? <TableRow><TableCell colSpan={6} className="h-32 text-center text-muted-foreground">No Webtech scans found.</TableCell></TableRow> : null}
        </TableBody>
      </Table>
      <div className="flex items-center justify-between gap-3">
        <span className="text-sm text-muted-foreground">Page {page.toLocaleString()} of {pages.toLocaleString()}</span>
        <div className="flex gap-2">
          {page > 1 ? <Button variant="outline" nativeButton={false} render={<Link to={webtechListPath(basePath, view, page - 1, prefix)} />}>Previous</Button> : null}
          {page < pages ? <Button variant="outline" nativeButton={false} render={<Link to={webtechListPath(basePath, view, page + 1, prefix)} />}>Next</Button> : null}
        </div>
      </div>
    </div>
  );
}
