import type { ColumnDef } from "@tanstack/react-table";
import {
  ArrowLeft,
  ExternalLink,
  MapPin,
  Search,
  SearchX,
} from "lucide-react";
import { Form, Link, useNavigate } from "react-router";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";
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
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { ToggleGroup, ToggleGroupItem } from "~/components/ui/toggle-group";
import type {
  AddressQualityFilter,
  AddressQualityQueueResult,
  AddressQualityRow,
} from "~/lib/address-quality.server";

const integer = new Intl.NumberFormat("en-US");
const percent = new Intl.NumberFormat("en-US", {
  style: "percent",
  maximumFractionDigits: 0,
});

const qualityOptions: Array<{
  value: AddressQualityFilter;
  label: string;
}> = [
  { value: "all", label: "All reviewable" },
  { value: "ambiguous", label: "Ambiguous" },
  { value: "unmatched", label: "Unmatched" },
  { value: "invalid", label: "Invalid" },
  { value: "city_fallback", label: "City fallback" },
  { value: "low_confidence", label: "Low confidence" },
];

function qualityLabel(row: AddressQualityRow): string {
  if (row.geocodePrecision === "city") return "City fallback";
  if (row.matchStatus === "invalid_address") return "Invalid address";
  if (row.matchStatus === "matched_exact" && row.matchConfidence < 0.8) {
    return "Low confidence";
  }
  return row.matchStatus.replaceAll("_", " ");
}

function qualityVariant(
  row: AddressQualityRow,
): "destructive" | "outline" | "secondary" {
  if (row.matchStatus === "invalid_address") return "destructive";
  if (row.matchStatus === "unmatched") return "outline";
  return "secondary";
}

function columns(): ColumnDef<AddressQualityRow, unknown>[] {
  return [
    {
      id: "address",
      header: "Address identity",
      cell: ({ row }) => (
        <div className="flex max-w-[28rem] flex-col gap-1">
          <span className="font-medium">{row.original.displayAddress}</span>
          <span
            className="text-muted-foreground max-w-72 truncate font-mono text-xs"
            title={row.original.addressId}
          >
            {row.original.addressId}
          </span>
          <span className="text-muted-foreground text-xs">
            {row.original.representativeSource.replaceAll("_", " ")} ·{" "}
            {row.original.addressKind.replaceAll("_", " ")}
          </span>
        </div>
      ),
    },
    {
      id: "quality",
      header: "Quality issue",
      cell: ({ row }) => (
        <div className="flex min-w-40 flex-col items-start gap-1.5">
          <Badge variant={qualityVariant(row.original)}>
            {qualityLabel(row.original)}
          </Badge>
          {row.original.matchMethod ? (
            <span className="text-muted-foreground text-xs">
              {row.original.matchMethod.replaceAll("_", " ")}
            </span>
          ) : null}
          {row.original.matchConfidence > 0 ? (
            <span className="text-muted-foreground text-xs tabular-nums">
              {percent.format(row.original.matchConfidence)} confidence
            </span>
          ) : null}
        </div>
      ),
    },
    {
      id: "location",
      header: "Resolved location",
      cell: ({ row }) => (
        <div className="flex min-w-44 flex-col gap-1">
          <span>{row.original.coordinateLocality || row.original.postTown}</span>
          <span className="text-muted-foreground text-xs">
            {[row.original.postalCode, row.original.geocodePrecision]
              .filter(Boolean)
              .join(" · ") || "No coordinate"}
          </span>
          {row.original.coordinateMethod ? (
            <span className="text-muted-foreground text-xs">
              {row.original.coordinateMethod.replaceAll("_", " ")}
              {row.original.coordinateSupportingPointCount > 0
                ? ` · ${integer.format(row.original.coordinateSupportingPointCount)} supporting points`
                : ""}
            </span>
          ) : null}
          {row.original.latitude !== null &&
          row.original.longitude !== null ? (
            <a
              href={`https://www.openstreetmap.org/?mlat=${row.original.latitude}&mlon=${row.original.longitude}#map=17/${row.original.latitude}/${row.original.longitude}`}
              target="_blank"
              rel="noreferrer"
              className="text-primary flex items-center gap-1 text-xs underline-offset-2 hover:underline"
            >
              <MapPin className="size-3" />
              {row.original.latitude.toFixed(5)}, {row.original.longitude.toFixed(5)}
            </a>
          ) : null}
        </div>
      ),
    },
    {
      id: "candidates",
      header: "OSM evidence",
      cell: ({ row }) => (
        <div className="flex min-w-40 flex-col items-start gap-1.5">
          <span className="text-sm tabular-nums">
            {integer.format(row.original.candidateCount)} candidates
          </span>
          <div className="flex flex-wrap gap-1">
            {row.original.candidateRecordUrls.slice(0, 2).map((url, index) => (
              <Badge
                key={url}
                variant="outline"
                render={<a href={url} target="_blank" rel="noreferrer" />}
              >
                OSM {index + 1}
                <ExternalLink data-icon="inline-end" />
              </Badge>
            ))}
          </div>
          {row.original.sourceUrl ? (
            <a
              href={row.original.sourceUrl}
              target="_blank"
              rel="noreferrer"
              className="text-muted-foreground text-xs underline-offset-2 hover:underline"
            >
              OSM snapshot
              {row.original.sourceSnapshotAt
                ? ` · ${row.original.sourceSnapshotAt.slice(0, 10)}`
                : ""}
            </a>
          ) : null}
        </div>
      ),
    },
    {
      id: "companies",
      header: "Associated companies",
      cell: ({ row }) => (
        <div className="flex min-w-52 flex-col gap-1">
          {row.original.companies.map((company) => (
            <Link
              key={company.companyId}
              to={`/company/se/${encodeURIComponent(company.companyId)}`}
              className="max-w-64 truncate text-sm underline-offset-2 hover:underline"
              title={company.companyName}
            >
              {company.companyName}
            </Link>
          ))}
          {row.original.companyCount > row.original.companies.length ? (
            <span className="text-muted-foreground text-xs tabular-nums">
              +{integer.format(
                row.original.companyCount - row.original.companies.length,
              )}{" "}
              more
            </span>
          ) : null}
          <span className="text-muted-foreground text-xs tabular-nums">
            {integer.format(row.original.evidenceCount)} source observations
          </span>
        </div>
      ),
    },
  ];
}

function QualityMetric({
  title,
  value,
  description,
}: {
  title: string;
  value: number;
  description: string;
}) {
  return (
    <Card size="sm">
      <CardHeader>
        <CardDescription>{title}</CardDescription>
        <CardTitle className="text-2xl tabular-nums">
          {integer.format(value)}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-muted-foreground text-xs">{description}</p>
      </CardContent>
    </Card>
  );
}

export function AddressQualityReview({
  query,
  quality,
  result,
}: {
  query: string;
  quality: AddressQualityFilter;
  result: AddressQualityQueueResult;
}) {
  const navigate = useNavigate();
  const searchParams = useEffectiveSearchParams();

  function selectQuality(nextQuality: AddressQualityFilter) {
    const next = new URLSearchParams(searchParams);
    next.delete("page");
    if (nextQuality === "ambiguous") next.delete("quality");
    else next.set("quality", nextQuality);
    navigate(`?${next.toString()}`, { preventScrollReset: true });
  }

  return (
    <div className="mx-auto flex w-full max-w-[100rem] flex-col gap-5">
      <header className="flex flex-col gap-2">
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2 self-start"
          render={<Link to="/countries/se/companies" />}
          nativeButton={false}
        >
          <ArrowLeft data-icon="inline-start" />
          Sweden companies
        </Button>
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">
            Address quality review
          </h1>
          <p className="text-muted-foreground mt-1 max-w-4xl text-sm">
            Inspect normalized Swedish address identities that could not be
            resolved precisely. Compare registry observations with OSM
            candidates and open associated companies for source-level context.
          </p>
        </div>
      </header>

      <section
        aria-label="Address quality summary"
        className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5"
      >
        <QualityMetric
          title="Ambiguous"
          value={result.stats.ambiguous}
          description="Multiple exact OSM candidates"
        />
        <QualityMetric
          title="Unmatched"
          value={result.stats.unmatched}
          description="No exact OSM address candidate"
        />
        <QualityMetric
          title="Invalid"
          value={result.stats.invalid}
          description="Insufficient normalized address"
        />
        <QualityMetric
          title="City fallback"
          value={result.stats.cityFallback}
          description="Locality coordinate, not a building"
        />
        <QualityMetric
          title="Low confidence"
          value={result.stats.lowConfidence}
          description="Exact result below the confidence floor"
        />
      </section>

      <div className="flex flex-wrap items-end justify-between gap-4">
        <Form method="get" className="w-full max-w-xl">
          <input type="hidden" name="quality" value={quality} />
          <FieldGroup>
            <Field orientation="horizontal">
              <FieldLabel htmlFor="address-quality-search" className="sr-only">
                Search addresses
              </FieldLabel>
              <Input
                id="address-quality-search"
                type="search"
                name="q"
                defaultValue={query}
                placeholder="Address, postal code, town, or address ID…"
              />
              <Button type="submit" variant="secondary">
                <Search data-icon="inline-start" />
                Search
              </Button>
            </Field>
          </FieldGroup>
        </Form>

        <Field orientation="horizontal" className="w-auto max-w-full">
          <FieldLabel>Issue</FieldLabel>
          <div className="max-w-full overflow-x-auto pb-1">
            <ToggleGroup
              value={[quality]}
              onValueChange={(values) => {
                const next = values.at(-1) as AddressQualityFilter | undefined;
                if (next) selectQuality(next);
              }}
              variant="outline"
              size="sm"
              spacing={0}
              aria-label="Address quality filter"
            >
              {qualityOptions.map((option) => (
                <ToggleGroupItem key={option.value} value={option.value}>
                  {option.label}
                </ToggleGroupItem>
              ))}
            </ToggleGroup>
          </div>
        </Field>
      </div>

      {result.rows.length ? (
        <>
          <DataTable
            columns={columns()}
            data={result.rows}
            minWidthClassName="min-w-[92rem]"
          />
          <DataTablePagination
            total={result.total}
            page={result.page}
            pageSize={result.pageSize}
            itemsLabel="address identities"
          />
        </>
      ) : (
        <Empty>
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <SearchX />
            </EmptyMedia>
            <EmptyTitle>No addresses found</EmptyTitle>
            <EmptyDescription>
              Change the issue or search filter to inspect another part of the
              quality queue.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      )}
    </div>
  );
}
