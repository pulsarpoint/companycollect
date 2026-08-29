import { CalendarClock, ExternalLink, Globe2 } from "lucide-react";
import { Link } from "react-router";
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
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { SE_COMPANIES_USING_TECHNOLOGY_LIMIT } from "~/lib/technologies";
// Type-only: erased at build, so the ClickHouse module stays server-side.
import type {
  SeCompanyUsingTechnology,
  TechnologyAdoption,
  TechnologyDetail,
} from "~/lib/technologies.server";

const nf = new Intl.NumberFormat("en-US");

function websiteHostname(website: string): string {
  try {
    return new URL(website).hostname.replace(/^www\./, "");
  } catch {
    return website;
  }
}

/** The detail header's icon: the proxy image at a readable size, or the same
 * monogram fallback the shared TechnologyIcon uses at list size. */
function DetailIcon({ technology }: { technology: TechnologyDetail }) {
  if (technology.icon) {
    return (
      <img
        src={`/icons/tech/${technology.slug}`}
        alt=""
        width={40}
        height={40}
        className="size-10 shrink-0 rounded-md border object-contain p-1"
      />
    );
  }
  return (
    <span
      aria-hidden
      data-slot="technology-monogram"
      className="bg-muted text-muted-foreground flex size-10 shrink-0 items-center justify-center rounded-md text-base font-semibold"
    >
      {(technology.technology.trim()[0] ?? "?").toUpperCase()}
    </span>
  );
}

function MetadataItem({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col gap-1">
      <span className="text-muted-foreground text-xs font-medium">{label}</span>
      <span className="text-sm">{children}</span>
    </div>
  );
}

export function TechnologyDetailView({
  technology,
  adoption,
  companies,
  companiesError = "",
}: {
  technology: TechnologyDetail;
  adoption: TechnologyAdoption | null;
  companies: SeCompanyUsingTechnology[];
  /** Non-empty when the live companies lookup failed (guarded read) --
   * shown as a section-level notice, the rest of the page stays useful. */
  companiesError?: string;
}) {
  return (
    <div className="flex flex-col gap-5">
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="flex items-start gap-3">
              <DetailIcon technology={technology} />
              <div>
                <CardTitle className="text-xl">
                  {technology.technology}
                </CardTitle>
                {technology.website ? (
                  <a
                    href={technology.website}
                    target="_blank"
                    rel="noreferrer"
                    className="text-muted-foreground mt-1 inline-flex items-center gap-1 text-sm underline underline-offset-2"
                  >
                    {websiteHostname(technology.website)}
                    <ExternalLink className="size-3.5" />
                  </a>
                ) : null}
              </div>
            </div>
            <div className="flex gap-1">
              {technology.saas ? <Badge variant="secondary">SaaS</Badge> : null}
              {technology.oss ? <Badge variant="outline">OSS</Badge> : null}
            </div>
          </div>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {technology.description ? (
            <p className="text-muted-foreground max-w-4xl text-sm">
              {technology.description}
            </p>
          ) : null}
          {technology.categories.length > 0 ? (
            <div className="flex flex-wrap gap-1.5">
              {technology.categories.map((category) => (
                <Badge key={category} variant="outline">
                  {category}
                </Badge>
              ))}
            </div>
          ) : null}
          <div className="flex flex-wrap gap-x-8 gap-y-3">
            {technology.pricing.length > 0 ? (
              <MetadataItem label="Pricing">
                <span className="flex flex-wrap gap-1">
                  {technology.pricing.map((tier) => (
                    <Badge key={tier} variant="secondary">
                      {tier}
                    </Badge>
                  ))}
                </span>
              </MetadataItem>
            ) : null}
            <MetadataItem label="Catalog source">
              {technology.source}
              {technology.source_version
                ? ` (${technology.source_version})`
                : ""}
            </MetadataItem>
            <MetadataItem label="Catalog updated">
              <span className="tabular-nums">{technology.updated_at}</span>
            </MetadataItem>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex items-start gap-3">
            <CalendarClock className="text-muted-foreground mt-0.5 size-4" />
            <div>
              <CardTitle>Global adoption</CardTitle>
              <CardDescription className="mt-1">
                Distinct root domains with a positive detection, from the
                weekly adoption rollup.
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {adoption ? (
            <p className="text-sm">
              <span className="text-2xl font-semibold tabular-nums">
                {nf.format(adoption.domainCount)}
              </span>{" "}
              <span className="text-muted-foreground">
                domains · computed {adoption.computedAt}
              </span>
            </p>
          ) : (
            <p className="text-muted-foreground text-sm">
              Adoption not computed yet — the weekly rollup has no row for
              this technology.
            </p>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex items-start gap-3">
            <Globe2 className="text-muted-foreground mt-0.5 size-4" />
            <div>
              <CardTitle>Swedish companies using it</CardTitle>
              <CardDescription className="mt-1">
                Live detection lookup over the Swedish company↔domain
                register, capped at {SE_COMPANIES_USING_TECHNOLOGY_LIMIT}{" "}
                companies.
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {companiesError !== "" ? (
            <Alert variant="destructive">
              <AlertTitle>Live lookup unavailable</AlertTitle>
              <AlertDescription>
                The detection query did not complete — ClickHouse may be busy
                (a rollup materialization, say). Reload to retry.
              </AlertDescription>
            </Alert>
          ) : companies.length === 0 ? (
            <p className="text-muted-foreground text-sm">
              No Swedish company domain has a detection of this technology.
            </p>
          ) : (
            <div className="overflow-x-auto">
              <Table className="min-w-[40rem]">
                <TableHeader>
                  <TableRow>
                    <TableHead>Company</TableHead>
                    <TableHead>Org number</TableHead>
                    <TableHead>Domain</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {companies.map((company) => (
                    <TableRow key={`${company.company_id}:${company.root_domain}`}>
                      <TableCell>
                        <Link
                          to={`/admin/se/company/${encodeURIComponent(company.company_id)}/technology`}
                          className="font-medium underline underline-offset-2"
                        >
                          {company.legal_name || company.company_id}
                        </Link>
                      </TableCell>
                      <TableCell className="font-mono text-xs">
                        {company.company_id}
                      </TableCell>
                      <TableCell className="font-mono text-xs">
                        {company.root_domain}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
