import {
  Activity,
  Building2,
  ContactRound,
  FileCode2,
  Fingerprint,
  Globe2,
  Info,
  MapPin,
  SearchCheck,
  ShieldCheck,
} from "lucide-react";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "~/components/ui/accordion";
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
  CompanyWebIntelligence,
  WebAuthoritySnapshot,
  WebIndustrySnapshot,
  WebOrganizationClaim,
  WebPageMetadataSnapshot,
  WebSecuritySnapshot,
} from "~/lib/web-intelligence";

const numberFormat = new Intl.NumberFormat("en-US");
const compactNumberFormat = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});
const dateFormat = new Intl.DateTimeFormat("en-GB", {
  dateStyle: "medium",
  timeStyle: "short",
  timeZone: "UTC",
});

const securityHeaderNames = [
  "strict-transport-security",
  "content-security-policy",
  "x-frame-options",
  "x-content-type-options",
  "referrer-policy",
  "permissions-policy",
];

const identifierLabels: Record<string, string> = {
  adsense: "Google AdSense",
  clarity: "Microsoft Clarity",
  duns: "D-U-N-S",
  fb_pixel: "Meta Pixel",
  ga: "Google Analytics",
  gtm: "Google Tag Manager",
  hotjar: "Hotjar",
  lei: "LEI",
  linkedin_insight: "LinkedIn Insight",
  mixpanel: "Mixpanel",
  naics: "NAICS",
  pinterest_tag: "Pinterest Tag",
  segment: "Segment",
  snap_pixel: "Snap Pixel",
  tax: "Tax identifier",
  tiktok_pixel: "TikTok Pixel",
  ua: "Universal Analytics",
  vat: "VAT identifier",
  yandex_metrika: "Yandex Metrica",
};

function formatObservedAt(value: string): string {
  if (!value) return "Time unavailable";
  const normalized = value.includes("T") ? value : value.replace(" ", "T");
  const date = new Date(
    /[zZ]|[+-]\d\d:\d\d$/.test(normalized) ? normalized : `${normalized}Z`,
  );
  return Number.isNaN(date.getTime()) ? value : dateFormat.format(date);
}

function formatScore(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "Unavailable";
  return value >= 0 && value <= 1
    ? `${Math.round(value * 100)}%`
    : value.toLocaleString("en-US", { maximumFractionDigits: 3 });
}

function safeWebUrl(value: string): string | null {
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:"
      ? url.toString()
      : null;
  } catch {
    return null;
  }
}

function SourceLink({
  url,
  label = "Source page",
}: {
  url: string;
  label?: string;
}) {
  const href = safeWebUrl(url);
  if (!href)
    return <span className="text-muted-foreground">Source unavailable</span>;
  return (
    <a
      href={href}
      target="_blank"
      rel="noreferrer"
      className="break-all underline underline-offset-2"
    >
      {label}
    </a>
  );
}

function EmptyEvidence({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <Empty className="border">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <SearchCheck />
        </EmptyMedia>
        <EmptyTitle>{title}</EmptyTitle>
        <EmptyDescription>{description}</EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}

function SummaryCard({
  intelligence,
}: {
  intelligence: CompanyWebIntelligence;
}) {
  const latestCoverage = intelligence.crawlCoverage[0];
  const facts = [
    {
      label: "Crawl snapshots",
      value: intelligence.crawlCoverage.length,
    },
    {
      label: "Pages in latest crawl",
      value: latestCoverage?.observedPages ?? 0,
    },
    {
      label: "Organization claims",
      value: intelligence.organizationClaims.length,
    },
    { label: "Observed addresses", value: intelligence.addresses.length },
    { label: "Observed contacts", value: intelligence.contacts.length },
    { label: "Identifiers", value: intelligence.identifiers.length },
  ];

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <CardTitle>Website-derived intelligence</CardTitle>
            <CardDescription className="mt-1">
              Extracted from archived pages for the selected domain{" "}
              <span className="font-mono">{intelligence.domain}</span>.
            </CardDescription>
          </div>
          {latestCoverage ? (
            <Badge variant="secondary">Latest {latestCoverage.crawlId}</Badge>
          ) : (
            <Badge variant="outline">No crawl coverage</Badge>
          )}
        </div>
      </CardHeader>
      <CardContent>
        <dl className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
          {facts.map((fact) => (
            <div key={fact.label} className="rounded-lg border bg-muted/20 p-3">
              <dt className="text-muted-foreground text-xs">{fact.label}</dt>
              <dd className="mt-1 text-lg font-semibold tabular-nums">
                {numberFormat.format(fact.value)}
              </dd>
            </div>
          ))}
        </dl>
        {latestCoverage ? (
          <p className="text-muted-foreground mt-3 text-xs tabular-nums">
            Latest domain evidence processed{" "}
            {formatObservedAt(latestCoverage.observedAt)}.
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}

function ClaimDetails({ claim }: { claim: WebOrganizationClaim }) {
  const name = claim.legalName || claim.name || "Unnamed organization claim";
  const details = [
    ["Display name", claim.name && claim.name !== name ? claim.name : ""],
    ["Country", claim.country],
    ["Founded", claim.foundingYear?.toString() ?? ""],
    [
      "Employees",
      claim.employeeCount ? numberFormat.format(claim.employeeCount) : "",
    ],
    ["Email", claim.email],
    ["Telephone", claim.telephone],
  ].filter((detail) => detail[1]);

  return (
    <article className="rounded-lg border bg-background p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h4 className="font-medium">{name}</h4>
          {claim.entityTypes.length ? (
            <div className="mt-2 flex flex-wrap gap-1">
              {claim.entityTypes.map((type) => (
                <Badge key={type} variant="outline">
                  {type}
                </Badge>
              ))}
            </div>
          ) : null}
        </div>
        <Badge variant="outline">Website claim</Badge>
      </div>
      {claim.description ? (
        <p className="text-muted-foreground mt-3 max-w-5xl text-sm leading-relaxed">
          {claim.description}
        </p>
      ) : null}
      {details.length ? (
        <dl className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {details.map(([label, value]) => (
            <div key={label}>
              <dt className="text-muted-foreground text-xs">{label}</dt>
              <dd className="mt-1 break-words text-sm">{value}</dd>
            </div>
          ))}
        </dl>
      ) : null}
      <div className="text-muted-foreground mt-4 flex flex-wrap gap-x-4 gap-y-2 text-xs">
        <SourceLink url={claim.pageUrl} />
        {safeWebUrl(claim.entityUrl) ? (
          <SourceLink url={claim.entityUrl} label="Claimed entity URL" />
        ) : null}
        <span className="tabular-nums">
          Observed {formatObservedAt(claim.observedAt)}
        </span>
      </div>
      {claim.sameAs.length ? (
        <div className="mt-3 flex flex-wrap gap-1.5">
          {claim.sameAs.slice(0, 8).map((url) => {
            const href = safeWebUrl(url);
            return href ? (
              <Badge
                key={url}
                variant="outline"
                render={<a href={href} target="_blank" rel="noreferrer" />}
              >
                Related profile
              </Badge>
            ) : null;
          })}
        </div>
      ) : null}
    </article>
  );
}

function OrganizationClaims({
  intelligence,
}: {
  intelligence: CompanyWebIntelligence;
}) {
  const grouped = new Map<string, WebOrganizationClaim[]>();
  for (const claim of intelligence.organizationClaims) {
    grouped.set(claim.crawlId, [...(grouped.get(claim.crawlId) ?? []), claim]);
  }
  const crawlGroups = [...grouped.entries()].sort(([left], [right]) =>
    right.localeCompare(left),
  );

  return (
    <Card>
      <CardHeader>
        <div className="flex items-start gap-3">
          <Building2 className="text-muted-foreground mt-0.5 size-4" />
          <div>
            <CardTitle>Organization claims</CardTitle>
            <CardDescription className="mt-1">
              Organization-shaped JSON-LD found on pages, preserved by crawl and
              source.
            </CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {crawlGroups.length ? (
          <Accordion
            multiple
            defaultValue={[crawlGroups[0][0]]}
            className="overflow-hidden rounded-xl border bg-background"
          >
            {crawlGroups.map(([crawlId, claims]) => (
              <AccordionItem key={crawlId} value={crawlId}>
                <AccordionTrigger className="rounded-none px-4 py-4 hover:bg-muted/40 hover:no-underline aria-expanded:bg-muted/40 sm:px-5">
                  <div className="flex min-w-0 flex-1 flex-wrap items-center justify-between gap-2 pr-3">
                    <span className="font-mono">{crawlId}</span>
                    <Badge variant="outline">
                      {numberFormat.format(claims.length)} claim
                      {claims.length === 1 ? "" : "s"}
                    </Badge>
                  </div>
                </AccordionTrigger>
                <AccordionContent className="pb-0">
                  <div className="flex flex-col gap-3 border-t bg-muted/10 p-4 sm:p-5">
                    {claims.map((claim, index) => (
                      <ClaimDetails
                        key={`${claim.pageUrl}:${index}`}
                        claim={claim}
                      />
                    ))}
                  </div>
                </AccordionContent>
              </AccordionItem>
            ))}
          </Accordion>
        ) : (
          <EmptyEvidence
            title="No organization claims extracted"
            description="No organization-shaped JSON-LD was found in the available crawls. Other website evidence may still be available below."
          />
        )}
        {intelligence.truncated.organizationClaims ? (
          <p className="text-muted-foreground mt-3 text-xs">
            Showing the first{" "}
            {numberFormat.format(intelligence.organizationClaims.length)}{" "}
            bounded claims.
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}

function ContactsCard({
  intelligence,
}: {
  intelligence: CompanyWebIntelligence;
}) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-start gap-3">
          <ContactRound className="text-muted-foreground mt-0.5 size-4" />
          <div>
            <CardTitle>Observed contacts</CardTitle>
            <CardDescription className="mt-1">
              Contact values extracted from pages. “Last observed” does not mean
              currently active.
            </CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {intelligence.contacts.length ? (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Type</TableHead>
                <TableHead>Value</TableHead>
                <TableHead>Extraction source</TableHead>
                <TableHead>Last observed</TableHead>
                <TableHead>Evidence</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {intelligence.contacts.map((contact) => (
                <TableRow key={`${contact.type}:${contact.value}`}>
                  <TableCell>
                    <Badge variant="outline">{contact.type}</Badge>
                  </TableCell>
                  <TableCell className="max-w-sm break-all whitespace-normal font-medium">
                    {contact.value}
                  </TableCell>
                  <TableCell>{contact.source || "Unspecified"}</TableCell>
                  <TableCell className="text-muted-foreground text-xs tabular-nums whitespace-normal">
                    <span className="font-mono">
                      {contact.lastObservedCrawl}
                    </span>
                    <br />
                    {formatObservedAt(contact.observedAt)}
                  </TableCell>
                  <TableCell className="text-xs">
                    <SourceLink url={contact.sourceUrl} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : (
          <EmptyEvidence
            title="No contacts extracted"
            description="No website contact observations are available for this domain."
          />
        )}
      </CardContent>
    </Card>
  );
}

function AddressesCard({
  intelligence,
}: {
  intelligence: CompanyWebIntelligence;
}) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-start gap-3">
          <MapPin className="mt-0.5 size-4 text-muted-foreground" />
          <div>
            <CardTitle>Observed addresses</CardTitle>
            <CardDescription className="mt-1">
              Postal addresses extracted from organization JSON-LD, retained
              with their crawl window and source page.
            </CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {intelligence.addresses.length ? (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Extracted address</TableHead>
                <TableHead>Observation window</TableHead>
                <TableHead>Crawls</TableHead>
                <TableHead>Evidence</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {intelligence.addresses.map((address) => (
                <TableRow key={address.value}>
                  <TableCell className="max-w-xl whitespace-normal font-medium">
                    {address.value}
                  </TableCell>
                  <TableCell className="text-xs tabular-nums text-muted-foreground whitespace-normal">
                    <span className="font-mono">
                      {address.firstObservedCrawl}
                    </span>{" "}
                    →<br />
                    <span className="font-mono">
                      {address.lastObservedCrawl}
                    </span>
                  </TableCell>
                  <TableCell className="tabular-nums">
                    {numberFormat.format(address.observedCrawls)}
                  </TableCell>
                  <TableCell className="text-xs">
                    <SourceLink url={address.sourceUrl} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : (
          <EmptyEvidence
            title="No addresses extracted"
            description="No organization postal addresses were found in the indexed JSON-LD evidence."
          />
        )}
        {intelligence.truncated.addresses ? (
          <p className="mt-3 text-xs text-muted-foreground">
            Showing the first {numberFormat.format(intelligence.addresses.length)}{" "}
            bounded address observations.
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}

function IdentifiersCard({
  intelligence,
}: {
  intelligence: CompanyWebIntelligence;
}) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-start gap-3">
          <Fingerprint className="text-muted-foreground mt-0.5 size-4" />
          <div>
            <CardTitle>Website identifiers</CardTitle>
            <CardDescription className="mt-1">
              Analytics, advertising, and structured-data identifiers linked by
              observed pages.
            </CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {intelligence.identifiers.length ? (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Identifier</TableHead>
                <TableHead>Value</TableHead>
                <TableHead>Observation window</TableHead>
                <TableHead>Coverage</TableHead>
                <TableHead>Evidence</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {intelligence.identifiers.map((identifier) => (
                <TableRow key={`${identifier.type}:${identifier.value}`}>
                  <TableCell>
                    <div className="flex flex-col gap-1">
                      <span className="font-medium">
                        {identifierLabels[identifier.type] ?? identifier.type}
                      </span>
                      <span className="text-muted-foreground text-xs">
                        {identifier.sources.join(", ")}
                      </span>
                    </div>
                  </TableCell>
                  <TableCell className="max-w-xs break-all font-mono text-xs whitespace-normal">
                    {identifier.value}
                  </TableCell>
                  <TableCell className="text-muted-foreground text-xs tabular-nums whitespace-normal">
                    <span className="font-mono">
                      {identifier.firstObservedCrawl}
                    </span>{" "}
                    →<br />
                    <span className="font-mono">
                      {identifier.lastObservedCrawl}
                    </span>
                  </TableCell>
                  <TableCell className="text-xs tabular-nums">
                    {numberFormat.format(identifier.observedCrawls)} crawls ·{" "}
                    {numberFormat.format(identifier.observedPages)} pages
                  </TableCell>
                  <TableCell className="max-w-sm whitespace-normal">
                    <div className="flex flex-col gap-1 text-xs">
                      {identifier.sampleUrls.map((url) => (
                        <SourceLink key={url} url={url} />
                      ))}
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : (
          <EmptyEvidence
            title="No identifiers extracted"
            description="No analytics, advertising, or registry identifiers were observed on the available pages."
          />
        )}
      </CardContent>
    </Card>
  );
}

function IndustrySnapshotDetails({
  snapshot,
}: {
  snapshot: WebIndustrySnapshot;
}) {
  return (
    <div className="flex flex-col gap-4 border-t bg-muted/10 p-4 sm:p-5">
      <div className="flex flex-wrap gap-2">
        {snapshot.pageType ? (
          <Badge variant="outline">Page type: {snapshot.pageType}</Badge>
        ) : null}
        {snapshot.pageTypeScore !== null ? (
          <Badge variant="outline">
            Page score {formatScore(snapshot.pageTypeScore)}
          </Badge>
        ) : null}
        {snapshot.classificationConfident !== null ? (
          <Badge
            variant={snapshot.classificationConfident ? "secondary" : "outline"}
          >
            {snapshot.classificationConfident
              ? "Classifier confident"
              : "Low-confidence classification"}
          </Badge>
        ) : null}
      </div>
      {snapshot.industries.length ? (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Rank</TableHead>
              <TableHead>NACE</TableHead>
              <TableHead>Website-derived label</TableHead>
              <TableHead>Score</TableHead>
              <TableHead>Method</TableHead>
              <TableHead>Evidence</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {snapshot.industries.map((industry) => (
              <TableRow key={industry.naceCode}>
                <TableCell className="tabular-nums">{industry.rank}</TableCell>
                <TableCell>
                  <Badge variant={industry.isPrimary ? "secondary" : "outline"}>
                    {industry.naceCode}
                  </Badge>
                </TableCell>
                <TableCell className="max-w-md whitespace-normal">
                  {industry.naceLabel}
                </TableCell>
                <TableCell className="tabular-nums">
                  {formatScore(industry.score)}
                </TableCell>
                <TableCell>{industry.method}</TableCell>
                <TableCell className="text-xs">
                  <SourceLink url={industry.sourceUrl} />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      ) : (
        <p className="text-muted-foreground text-sm">
          No NACE candidates were retained for this crawl.
        </p>
      )}
    </div>
  );
}

function IndustriesCard({ snapshots }: { snapshots: WebIndustrySnapshot[] }) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-start gap-3">
          <Activity className="text-muted-foreground mt-0.5 size-4" />
          <div>
            <CardTitle>Inferred industries</CardTitle>
            <CardDescription className="mt-1">
              Model-derived NACE candidates from website content. These are
              separate from registry industries.
            </CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {snapshots.length ? (
          <Accordion
            multiple
            defaultValue={[snapshots[0].crawlId]}
            className="overflow-hidden rounded-xl border bg-background"
          >
            {snapshots.map((snapshot) => (
              <AccordionItem key={snapshot.crawlId} value={snapshot.crawlId}>
                <AccordionTrigger className="rounded-none px-4 py-4 hover:bg-muted/40 hover:no-underline aria-expanded:bg-muted/40 sm:px-5">
                  <div className="flex min-w-0 flex-1 flex-wrap items-center justify-between gap-2 pr-3">
                    <span className="font-mono">{snapshot.crawlId}</span>
                    <span className="text-muted-foreground text-xs tabular-nums">
                      {snapshot.industries.length} candidates ·{" "}
                      {formatObservedAt(snapshot.observedAt)}
                    </span>
                  </div>
                </AccordionTrigger>
                <AccordionContent className="pb-0">
                  <IndustrySnapshotDetails snapshot={snapshot} />
                </AccordionContent>
              </AccordionItem>
            ))}
          </Accordion>
        ) : (
          <EmptyEvidence
            title="No industry inference"
            description="No website-derived NACE classification is available for this domain."
          />
        )}
      </CardContent>
    </Card>
  );
}

function MetadataSnapshotDetails({
  snapshot,
}: {
  snapshot: WebPageMetadataSnapshot;
}) {
  const metaEntries = Object.entries(snapshot.meta).sort(([left], [right]) =>
    left.localeCompare(right),
  );
  return (
    <div className="flex flex-col gap-4 border-t bg-muted/10 p-4 sm:p-5">
      <dl className="grid gap-3 sm:grid-cols-2">
        <div>
          <dt className="text-muted-foreground text-xs">Page title</dt>
          <dd className="mt-1">{snapshot.title || "Not extracted"}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground text-xs">Canonical URL</dt>
          <dd className="mt-1 break-all">
            {snapshot.canonical || "Not declared"}
          </dd>
        </div>
        <div>
          <dt className="text-muted-foreground text-xs">Character set</dt>
          <dd className="mt-1">{snapshot.charset || "Not declared"}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground text-xs">Source</dt>
          <dd className="mt-1 text-xs">
            <SourceLink url={snapshot.sourceUrl} />
          </dd>
        </div>
      </dl>
      {snapshot.jsonLdTypes.length ? (
        <div>
          <p className="text-muted-foreground mb-2 text-xs">JSON-LD types</p>
          <div className="flex flex-wrap gap-1">
            {snapshot.jsonLdTypes.map((type) => (
              <Badge key={type} variant="outline">
                {type}
              </Badge>
            ))}
          </div>
        </div>
      ) : null}
      {snapshot.hreflang.length ? (
        <div>
          <p className="text-muted-foreground mb-2 text-xs">
            Language alternatives
          </p>
          <div className="flex flex-wrap gap-1">
            {snapshot.hreflang.map((value) => (
              <Badge key={value} variant="outline">
                {value}
              </Badge>
            ))}
          </div>
        </div>
      ) : null}
      {metaEntries.length ? (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Metadata key</TableHead>
              <TableHead>Extracted value</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {metaEntries.map(([key, value]) => (
              <TableRow key={key}>
                <TableCell className="font-mono text-xs">{key}</TableCell>
                <TableCell className="max-w-4xl break-words whitespace-normal">
                  {value}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      ) : null}
    </div>
  );
}

function PageMetadataCard({
  snapshots,
}: {
  snapshots: WebPageMetadataSnapshot[];
}) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-start gap-3">
          <FileCode2 className="text-muted-foreground mt-0.5 size-4" />
          <div>
            <CardTitle>Page metadata</CardTitle>
            <CardDescription className="mt-1">
              Titles, canonical URLs, structured-data types, and metadata by
              crawl.
            </CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {snapshots.length ? (
          <Accordion
            multiple
            defaultValue={[snapshots[0].crawlId]}
            className="overflow-hidden rounded-xl border bg-background"
          >
            {snapshots.map((snapshot) => (
              <AccordionItem key={snapshot.crawlId} value={snapshot.crawlId}>
                <AccordionTrigger className="rounded-none px-4 py-4 hover:bg-muted/40 hover:no-underline aria-expanded:bg-muted/40 sm:px-5">
                  <div className="flex min-w-0 flex-1 flex-wrap items-center justify-between gap-2 pr-3">
                    <span className="font-mono">{snapshot.crawlId}</span>
                    <span className="text-muted-foreground max-w-xl truncate text-xs">
                      {snapshot.title || snapshot.sourceUrl}
                    </span>
                  </div>
                </AccordionTrigger>
                <AccordionContent className="pb-0">
                  <MetadataSnapshotDetails snapshot={snapshot} />
                </AccordionContent>
              </AccordionItem>
            ))}
          </Accordion>
        ) : (
          <EmptyEvidence
            title="No page metadata extracted"
            description="No domain-level page metadata snapshots are available."
          />
        )}
      </CardContent>
    </Card>
  );
}

function SecuritySnapshotDetails({
  snapshot,
}: {
  snapshot: WebSecuritySnapshot;
}) {
  const normalizedHeaders = new Map(
    Object.entries(snapshot.headers).map(([name, value]) => [
      name.toLowerCase(),
      value,
    ]),
  );
  const headerEntries = [...normalizedHeaders.entries()].sort(
    ([left], [right]) => left.localeCompare(right),
  );
  return (
    <div className="flex flex-col gap-4 border-t bg-muted/10 p-4 sm:p-5">
      <div className="flex flex-wrap gap-1.5">
        {securityHeaderNames.map((name) => (
          <Badge
            key={name}
            variant={normalizedHeaders.has(name) ? "secondary" : "outline"}
          >
            {normalizedHeaders.has(name) ? "Present" : "Not observed"}: {name}
          </Badge>
        ))}
      </div>
      {headerEntries.length ? (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>HTTP header</TableHead>
              <TableHead>Observed value</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {headerEntries.map(([name, value]) => (
              <TableRow key={name}>
                <TableCell className="font-mono text-xs">{name}</TableCell>
                <TableCell className="max-w-4xl break-all font-mono text-xs whitespace-normal">
                  {value}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      ) : (
        <p className="text-muted-foreground text-sm">
          No response headers were retained for this snapshot.
        </p>
      )}
      <p className="text-muted-foreground text-xs">
        <SourceLink url={snapshot.sourceUrl} /> · Observed{" "}
        {formatObservedAt(snapshot.observedAt)}
      </p>
    </div>
  );
}

function SecurityCard({ snapshots }: { snapshots: WebSecuritySnapshot[] }) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-start gap-3">
          <ShieldCheck className="text-muted-foreground mt-0.5 size-4" />
          <div>
            <CardTitle>HTTP and security headers</CardTitle>
            <CardDescription className="mt-1">
              Headers observed in archived responses. Absence means “not
              observed,” not necessarily “not configured today.”
            </CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {snapshots.length ? (
          <Accordion
            multiple
            defaultValue={[snapshots[0].crawlId]}
            className="overflow-hidden rounded-xl border bg-background"
          >
            {snapshots.map((snapshot) => (
              <AccordionItem key={snapshot.crawlId} value={snapshot.crawlId}>
                <AccordionTrigger className="rounded-none px-4 py-4 hover:bg-muted/40 hover:no-underline aria-expanded:bg-muted/40 sm:px-5">
                  <div className="flex min-w-0 flex-1 flex-wrap items-center justify-between gap-2 pr-3">
                    <span className="font-mono">{snapshot.crawlId}</span>
                    <span className="text-muted-foreground text-xs tabular-nums">
                      {Object.keys(snapshot.headers).length} headers ·{" "}
                      {formatObservedAt(snapshot.observedAt)}
                    </span>
                  </div>
                </AccordionTrigger>
                <AccordionContent className="pb-0">
                  <SecuritySnapshotDetails snapshot={snapshot} />
                </AccordionContent>
              </AccordionItem>
            ))}
          </Accordion>
        ) : (
          <EmptyEvidence
            title="No security headers extracted"
            description="No domain-level HTTP header snapshots are available."
          />
        )}
      </CardContent>
    </Card>
  );
}

function AuthorityCard({ snapshots }: { snapshots: WebAuthoritySnapshot[] }) {
  const latest = snapshots[0];
  return (
    <Card>
      <CardHeader>
        <div className="flex items-start gap-3">
          <Globe2 className="text-muted-foreground mt-0.5 size-4" />
          <div>
            <CardTitle>Web graph authority</CardTitle>
            <CardDescription className="mt-1">
              Common Crawl link-graph measurements and their history.
            </CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {latest ? (
          <div className="flex flex-col gap-4">
            <dl className="grid gap-3 sm:grid-cols-3">
              <div className="rounded-lg border bg-muted/20 p-3">
                <dt className="text-muted-foreground text-xs">Harmonic rank</dt>
                <dd className="mt-1 text-lg font-semibold tabular-nums">
                  {numberFormat.format(latest.harmonicRank)}
                </dd>
              </div>
              <div className="rounded-lg border bg-muted/20 p-3">
                <dt className="text-muted-foreground text-xs">PageRank rank</dt>
                <dd className="mt-1 text-lg font-semibold tabular-nums">
                  {numberFormat.format(latest.pageRankRank)}
                </dd>
              </div>
              <div className="rounded-lg border bg-muted/20 p-3">
                <dt className="text-muted-foreground text-xs">
                  Observed hosts
                </dt>
                <dd className="mt-1 text-lg font-semibold tabular-nums">
                  {compactNumberFormat.format(latest.observedHosts)}
                </dd>
              </div>
            </dl>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Crawl</TableHead>
                  <TableHead>Harmonic centrality</TableHead>
                  <TableHead>Harmonic rank</TableHead>
                  <TableHead>PageRank</TableHead>
                  <TableHead>PageRank rank</TableHead>
                  <TableHead>Hosts</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {snapshots.map((snapshot) => (
                  <TableRow key={snapshot.crawlId}>
                    <TableCell className="font-mono text-xs">
                      {snapshot.crawlId}
                    </TableCell>
                    <TableCell className="tabular-nums">
                      {snapshot.harmonicCentrality.toLocaleString("en-US", {
                        maximumFractionDigits: 4,
                      })}
                    </TableCell>
                    <TableCell className="tabular-nums">
                      {numberFormat.format(snapshot.harmonicRank)}
                    </TableCell>
                    <TableCell className="tabular-nums">
                      {snapshot.pageRank.toExponential(3)}
                    </TableCell>
                    <TableCell className="tabular-nums">
                      {numberFormat.format(snapshot.pageRankRank)}
                    </TableCell>
                    <TableCell className="tabular-nums">
                      {numberFormat.format(snapshot.observedHosts)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        ) : (
          <EmptyEvidence
            title="No authority measurements"
            description="No Common Crawl web-graph signals are available for this domain."
          />
        )}
      </CardContent>
    </Card>
  );
}

export function WebIntelligenceSection({
  intelligence,
}: {
  intelligence: CompanyWebIntelligence;
}) {
  return (
    <div className="flex flex-col gap-5">
      <SummaryCard intelligence={intelligence} />
      <Alert>
        <Info />
        <AlertTitle>Website evidence, not registry-verified facts</AlertTitle>
        <AlertDescription>
          Extracted pages can describe subsidiaries, customers, partners,
          article subjects, or stale information. Every claim remains attached
          to its source page and crawl; compare it with official company records
          before treating it as a company fact.
        </AlertDescription>
      </Alert>
      <OrganizationClaims intelligence={intelligence} />
      <AddressesCard intelligence={intelligence} />
      <ContactsCard intelligence={intelligence} />
      <IdentifiersCard intelligence={intelligence} />
      <IndustriesCard snapshots={intelligence.industrySnapshots} />
      <PageMetadataCard snapshots={intelligence.pageMetadataSnapshots} />
      <SecurityCard snapshots={intelligence.securitySnapshots} />
      <AuthorityCard snapshots={intelligence.authoritySnapshots} />
    </div>
  );
}
