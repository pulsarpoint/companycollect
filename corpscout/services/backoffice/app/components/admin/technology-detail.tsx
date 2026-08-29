import type { ColumnDef } from "@tanstack/react-table";
import { CalendarClock, ExternalLink, Globe2 } from "lucide-react";
import { Link, useNavigate } from "react-router";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { Label } from "~/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import {
  TECHNOLOGY_DETAIL_TABS,
  technologyCompanyPath,
  technologyDetailSearch,
  type TechnologyListView,
} from "~/lib/technologies";
// Type-only: erased at build, so the ClickHouse module stays server-side.
import type {
  TechnologyAdoption,
  TechnologyCompanyRow,
  TechnologyDetail,
  TechnologyDomainRow,
} from "~/lib/technologies.server";

const nf = new Intl.NumberFormat("en-US");
/** Harmonic centrality is a Float64 score, not a count -- keep a couple of
 * decimals so nearby domains stay distinguishable. */
const centralityFormat = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 2,
});

/**
 * The adoption section's per-tab loader data -- only the ACTIVE tab's rollup
 * is ever fetched (mirrors SePeopleSourcePage). `computedAt` is the rollup's
 * newest computed_at for this technology, null while the weekly rollup has
 * not landed yet -- the tab then shows an honest "not computed yet", never an
 * empty table pretending the answer is "none".
 */
export type TechnologyAdoptionTabData =
  | {
      tab: "domains";
      rows: TechnologyDomainRow[];
      total: number;
      computedAt: string | null;
    }
  | {
      tab: "companies";
      rows: TechnologyCompanyRow[];
      total: number;
      countries: string[];
      computedAt: string | null;
    };

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

/* -------------------------------------------------------------------- */
/* The adoption section: two URL-driven tabs                             */
/* -------------------------------------------------------------------- */

function AdoptionTabsNav({ tab }: { tab: TechnologyAdoptionTabData["tab"] }) {
  const searchParams = useEffectiveSearchParams();
  return (
    <Tabs value={tab}>
      <TabsList variant="line">
        {TECHNOLOGY_DETAIL_TABS.map((entry) => (
          <TabsTrigger
            key={entry.value}
            value={entry.value}
            render={
              <Link
                to={technologyDetailSearch(searchParams, { tab: entry.value })}
                preventScrollReset
              />
            }
            nativeButton={false}
          >
            {entry.label}
          </TabsTrigger>
        ))}
      </TabsList>
    </Tabs>
  );
}

/** The weekly stamp under the tab bar, or the tab's honest empty state when
 * the rollup has not landed yet (first population can be in flight). */
function ComputedStamp({ computedAt }: { computedAt: string }) {
  return (
    <p className="text-muted-foreground text-xs">
      Computed weekly · latest rollup {computedAt}
    </p>
  );
}

function NotComputedYet({ what }: { what: string }) {
  return (
    <p className="text-muted-foreground text-sm">
      Not computed yet — the weekly {what} rollup has no rows for this
      technology.
    </p>
  );
}

/* ---------------------- Domains tab ---------------------------------- */

function domainColumns(): ColumnDef<TechnologyDomainRow, unknown>[] {
  return [
    {
      id: "harmonic_rank",
      header: "#",
      cell: ({ row }) => (
        <span className="text-muted-foreground tabular-nums">
          {row.original.harmonic_rank}
        </span>
      ),
    },
    {
      id: "root_domain",
      header: "Domain",
      cell: ({ row }) => (
        <a
          href={`https://${row.original.root_domain}`}
          target="_blank"
          rel="noreferrer"
          className="inline-flex items-center gap-1 font-medium underline underline-offset-2"
        >
          {row.original.root_domain}
          <ExternalLink className="size-3.5" />
        </a>
      ),
    },
    {
      id: "harmonic_centrality",
      header: "Harmonic centrality",
      cell: ({ row }) => (
        <span className="tabular-nums">
          {centralityFormat.format(row.original.harmonic_centrality)}
        </span>
      ),
    },
  ];
}

function DomainsTab({
  data,
  view,
}: {
  data: Extract<TechnologyAdoptionTabData, { tab: "domains" }>;
  view: TechnologyListView;
}) {
  if (data.computedAt === null) {
    return <NotComputedYet what="top-domains" />;
  }
  return (
    <div className="flex flex-col gap-3">
      <ComputedStamp computedAt={data.computedAt} />
      <DataTable
        columns={domainColumns()}
        data={data.rows}
        emptyText="No crawled domain carries a detection of this technology."
        minWidthClassName="min-w-[36rem]"
      />
      <DataTablePagination
        total={data.total}
        page={view.page}
        pageSize={view.pageSize}
        itemsLabel="domains"
      />
    </div>
  );
}

/* ---------------------- Companies tab --------------------------------- */

/** Cap the NACE badges per row; the rest collapses into a "+N" badge whose
 * title lists what it hides. Primary classifications come first (the server
 * orders is_primary DESC). */
const INDUSTRY_BADGE_CAP = 4;

function IndustriesCell({
  industries,
}: {
  industries: TechnologyCompanyRow["industries"];
}) {
  if (industries.length === 0) {
    return <span className="text-muted-foreground">—</span>;
  }
  const shown = industries.slice(0, INDUSTRY_BADGE_CAP);
  const hidden = industries.slice(INDUSTRY_BADGE_CAP);
  return (
    <div className="flex max-w-[24rem] flex-wrap gap-1">
      {/* The code alone is not unique across classification systems/levels,
          so the key carries the position too. */}
      {shown.map((industry, index) => (
        <Badge
          key={`${industry.code}:${index}`}
          variant={industry.is_primary ? "secondary" : "outline"}
          title={`${industry.code} ${industry.label}`}
        >
          <span className="max-w-[14rem] truncate">
            {industry.code} {industry.label}
          </span>
        </Badge>
      ))}
      {hidden.length > 0 ? (
        <Badge
          variant="outline"
          title={hidden
            .map((industry) => `${industry.code} ${industry.label}`)
            .join(", ")}
        >
          +{hidden.length}
        </Badge>
      ) : null}
    </div>
  );
}

function companyColumns(): ColumnDef<TechnologyCompanyRow, unknown>[] {
  return [
    {
      id: "company",
      header: "Company",
      cell: ({ row }) => (
        <Link
          to={technologyCompanyPath(
            row.original.country_code,
            row.original.company_id,
          )}
          className="font-medium underline underline-offset-2"
        >
          {/* Non-SE registers have no name lookup yet (see
              loadTechnologyCompaniesPage's extension point): the id stands in
              for the name, the link is already correct. */}
          {row.original.legal_name || row.original.company_id}
        </Link>
      ),
    },
    {
      id: "country_code",
      header: "Country",
      cell: ({ row }) => (
        <Badge variant="outline">{row.original.country_code}</Badge>
      ),
    },
    {
      id: "root_domain",
      header: "Domain",
      cell: ({ row }) => (
        <span className="font-mono text-xs">{row.original.root_domain}</span>
      ),
    },
    {
      id: "industries",
      header: "Industries",
      cell: ({ row }) => <IndustriesCell industries={row.original.industries} />,
    },
  ];
}

const ANY_COUNTRY = "__any__";

/** Country as the tab's main filter: options are whatever country codes the
 * rollup holds for this technology (today only SE -- more countries appear
 * here on their own). Navigates on change, like the catalog's category
 * Select -- a loader navigation, never component state. */
function CompaniesCountryFilter({
  country,
  countries,
}: {
  country: string;
  countries: string[];
}) {
  const searchParams = useEffectiveSearchParams();
  const navigate = useNavigate();
  // Base UI renders the selected VALUE unless given labels; the sentinel
  // must read "All countries", not "__any__".
  const countryItems: Record<string, string> = {
    [ANY_COUNTRY]: "All countries",
    ...Object.fromEntries(countries.map((code) => [code, code])),
  };
  return (
    <div className="flex flex-col gap-1">
      <Label htmlFor="technology-companies-country" className="text-xs font-medium">
        Country
      </Label>
      <Select
        items={countryItems}
        value={country === "" ? ANY_COUNTRY : country}
        onValueChange={(value: string | null) => {
          if (value === null) return;
          navigate(
            technologyDetailSearch(searchParams, {
              country: value === ANY_COUNTRY ? "" : value,
            }),
            { preventScrollReset: true },
          );
        }}
      >
        <SelectTrigger id="technology-companies-country" className="w-48">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ANY_COUNTRY}>All countries</SelectItem>
          {countries.map((code) => (
            <SelectItem key={code} value={code}>
              {code}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}

function CompaniesTab({
  data,
  country,
  view,
}: {
  data: Extract<TechnologyAdoptionTabData, { tab: "companies" }>;
  country: string;
  view: TechnologyListView;
}) {
  if (data.computedAt === null) {
    return <NotComputedYet what="companies" />;
  }
  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <CompaniesCountryFilter country={country} countries={data.countries} />
        <ComputedStamp computedAt={data.computedAt} />
      </div>
      <DataTable
        columns={companyColumns()}
        data={data.rows}
        emptyText={
          country === ""
            ? "No registered company domain carries a detection of this technology."
            : `No ${country} company domain carries a detection of this technology.`
        }
        minWidthClassName="min-w-[56rem]"
      />
      <DataTablePagination
        total={data.total}
        page={view.page}
        pageSize={view.pageSize}
        itemsLabel="companies"
      />
    </div>
  );
}

/* -------------------------------------------------------------------- */
/* The page                                                              */
/* -------------------------------------------------------------------- */

export function TechnologyDetailView({
  technology,
  adoption,
  tab,
  country,
  view,
}: {
  technology: TechnologyDetail;
  adoption: TechnologyAdoption | null;
  tab: TechnologyAdoptionTabData;
  /** Companies tab's applied country filter ('' = all). */
  country: string;
  view: TechnologyListView;
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
              <CardTitle>Adoption</CardTitle>
              <CardDescription className="mt-1">
                Who carries this technology: the top crawled domains by
                CommonCrawl harmonic centrality, and the registered companies
                whose domains have a detection. Both from weekly rollups.
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <AdoptionTabsNav tab={tab.tab} />
          {tab.tab === "domains" ? (
            <DomainsTab data={tab} view={view} />
          ) : (
            <CompaniesTab data={tab} country={country} view={view} />
          )}
        </CardContent>
      </Card>
    </div>
  );
}
