import { Link } from "react-router";
import { ChevronRight, ExternalLink, FileText } from "lucide-react";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Separator } from "~/components/ui/separator";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import {
  formatFileSize,
  parseWarnings,
  type FinancialReportDocumentSummary,
} from "~/lib/norway-financial-reports";

function processingLabel(report: FinancialReportDocumentSummary): string {
  if (!report.hasReportMetadata) return "Facts loaded";
  if (report.ocrPageCount > 0 && report.nativeTextPageCount > 0)
    return "Text + OCR";
  if (report.ocrPageCount > 0) return "OCR processed";
  if (report.nativeTextPageCount > 0) return "Text extracted";
  return report.parseStatus === "loaded" ? "Processed" : report.parseStatus;
}

export function FinancialReportDocuments({
  reports,
  detailsHref,
}: {
  reports: FinancialReportDocumentSummary[];
  detailsHref: (report: FinancialReportDocumentSummary) => string;
}) {
  if (reports.length === 0) return null;

  return (
    <section className="flex flex-col gap-3">
      <Separator />
      <div>
        <h3 className="font-medium">Source documents</h3>
        <p className="text-muted-foreground mt-1 text-sm">
          Official annual-account PDFs with extraction coverage and direct
          source access.
        </p>
      </div>
      <div className="overflow-x-auto">
        <Table className="min-w-[54rem]">
          <TableHeader>
            <TableRow>
              <TableHead>Filing</TableHead>
              <TableHead>Document</TableHead>
              <TableHead>Extraction</TableHead>
              <TableHead className="text-right">Pages</TableHead>
              <TableHead className="text-right">Facts</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {reports.map((report) => {
              const warnings = parseWarnings(report.parseWarnings);
              const reportHref = detailsHref(report);
              return (
                <TableRow key={report.documentId}>
                  <TableCell className="align-top">
                    <div className="flex flex-col gap-1">
                      <span className="font-medium tabular-nums">
                        {report.filingYear}
                      </span>
                      <span className="text-muted-foreground text-xs">
                        Annual accounts
                      </span>
                    </div>
                  </TableCell>
                  <TableCell className="max-w-md align-top whitespace-normal">
                    <Link
                      to={reportHref}
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
                      <Badge
                        variant={
                          report.hasReportMetadata ? "secondary" : "outline"
                        }
                      >
                        {processingLabel(report)}
                      </Badge>
                      {warnings.length > 0 ? (
                        <Badge variant="outline">
                          {warnings.length}{" "}
                          {warnings.length === 1 ? "warning" : "warnings"}
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
                          <a
                            href={report.sourceUrl}
                            target="_blank"
                            rel="noreferrer"
                          />
                        }
                      >
                        PDF
                        <ExternalLink data-icon="inline-end" />
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        nativeButton={false}
                        render={<Link to={reportHref} />}
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
      </div>
    </section>
  );
}
