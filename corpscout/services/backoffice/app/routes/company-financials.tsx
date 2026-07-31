import { Link } from "react-router";
import { ChevronRight, ExternalLink, FileText } from "lucide-react";
import type { Route } from "./+types/company-financials";
import { FinancialsSection } from "~/components/detail/financials-section";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
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
import { getCountry } from "~/lib/countries";
import {
  formatFileSize,
  parseWarnings,
  type NorwayFinancialReportSummary,
} from "~/lib/norway-financial-reports";
import { getNorwayFinancialReports } from "~/lib/norway-financial-reports.server";
import { getCompanyFinancials } from "~/lib/queries.server";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country?.detail?.financialReports || country.code !== "no") {
    throw new Response("Not found", { status: 404 });
  }
  const [financials, reports] = await Promise.all([
    getCompanyFinancials(country, params.id),
    getNorwayFinancialReports(params.id),
  ]);
  return { financials, reports };
}

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `Financials · ${params.id} – CompanyCollect Backoffice` }];
}

function processingLabel(report: NorwayFinancialReportSummary): string {
  if (!report.hasReportMetadata) return "Facts loaded";
  if (report.ocrPageCount > 0 && report.nativeTextPageCount > 0) return "Text + OCR";
  if (report.ocrPageCount > 0) return "OCR processed";
  if (report.nativeTextPageCount > 0) return "Text extracted";
  return report.parseStatus === "loaded" ? "Processed" : report.parseStatus;
}

export default function CompanyFinancials({ loaderData, params }: Route.ComponentProps) {
  const { financials, reports } = loaderData;
  const basePath = `/company/${params.country}/${params.id}/financials`;

  return (
    <div className="flex w-full flex-col gap-5">
      <div>
        <h2 className="text-xl font-semibold tracking-tight">Financials</h2>
        <p className="text-muted-foreground mt-1 max-w-3xl text-sm">
          Registry figures and the official annual-account PDFs used to extract detailed report
          facts.
        </p>
      </div>

      <FinancialsSection financials={financials} title="Financial history" />

      <Card>
        <CardHeader>
          <CardTitle>Annual reports</CardTitle>
          <CardDescription>
            Official PDFs from the Brønnøysund Register Centre, with extraction coverage and direct
            source access.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {reports.length === 0 ? (
            <Empty className="border">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <FileText />
                </EmptyMedia>
                <EmptyTitle>No annual reports loaded</EmptyTitle>
                <EmptyDescription>
                  This company does not have a parsed Brønnøysund annual-account PDF yet.
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <Table className="min-w-[54rem]">
              <TableHeader>
                <TableRow>
                  <TableHead>Filing</TableHead>
                  <TableHead>Source document</TableHead>
                  <TableHead>Extraction</TableHead>
                  <TableHead className="text-right">Pages</TableHead>
                  <TableHead className="text-right">Facts</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {reports.map((report) => {
                  const warnings = parseWarnings(report.parseWarnings);
                  const detailsHref = `${basePath}/${encodeURIComponent(report.documentId)}`;
                  return (
                    <TableRow key={report.documentId}>
                      <TableCell className="align-top">
                        <div className="flex flex-col gap-1">
                          <span className="font-medium tabular-nums">{report.filingYear}</span>
                          <span className="text-muted-foreground text-xs">Annual accounts</span>
                        </div>
                      </TableCell>
                      <TableCell className="max-w-md align-top whitespace-normal">
                        <Link
                          to={detailsHref}
                          className="group inline-flex items-start gap-2 font-medium underline-offset-4 hover:underline"
                        >
                          <FileText className="mt-0.5 size-4 shrink-0 text-muted-foreground" />
                          <span className="break-all">{report.sourceFileName}</span>
                        </Link>
                        {formatFileSize(report.pdfSizeBytes) ? (
                          <div className="text-muted-foreground mt-1 text-xs">
                            {formatFileSize(report.pdfSizeBytes)}
                          </div>
                        ) : null}
                      </TableCell>
                      <TableCell className="align-top">
                        <div className="flex flex-wrap items-center gap-1.5">
                          <Badge variant={report.hasReportMetadata ? "secondary" : "outline"}>
                            {processingLabel(report)}
                          </Badge>
                          {warnings.length > 0 ? (
                            <Badge variant="outline">
                              {warnings.length} {warnings.length === 1 ? "warning" : "warnings"}
                            </Badge>
                          ) : null}
                        </div>
                      </TableCell>
                      <TableCell className="text-right align-top tabular-nums">
                        {report.pageCount || "—"}
                      </TableCell>
                      <TableCell className="text-right align-top tabular-nums">
                        {report.factCount.toLocaleString("en-US")}
                      </TableCell>
                      <TableCell className="align-top">
                        <div className="flex justify-end gap-1">
                          <Button
                            variant="ghost"
                            size="sm"
                            nativeButton={false}
                            render={
                              <a href={report.sourceUrl} target="_blank" rel="noreferrer" />
                            }
                          >
                            PDF
                            <ExternalLink data-icon="inline-end" />
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            nativeButton={false}
                            render={<Link to={detailsHref} />}
                          >
                            Details
                            <ChevronRight data-icon="inline-end" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
