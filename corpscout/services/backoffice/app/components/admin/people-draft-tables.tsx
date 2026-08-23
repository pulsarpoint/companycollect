import { type FormEvent, useEffect, useMemo, useState } from "react";
import { Form, Link, useLocation, useNavigate } from "react-router";
import {
  CheckCheckIcon,
  DatabaseIcon,
  EyeIcon,
  ListFilterIcon,
  SearchIcon,
  SparklesIcon,
  XIcon,
} from "lucide-react";
import { PeopleProfileBulkEnhancer } from "~/components/admin/people-profile-bulk-enhancer";
import { Badge } from "~/components/ui/badge";
import { Button, buttonVariants } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { Checkbox } from "~/components/ui/checkbox";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import {
  Field,
  FieldDescription,
  FieldError,
  FieldGroup,
  FieldLabel,
  FieldLegend,
  FieldSet,
} from "~/components/ui/field";
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupInput,
} from "~/components/ui/input-group";
import {
  Pagination,
  PaginationContent,
  PaginationEllipsis,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
} from "~/components/ui/pagination";
import {
  Sheet,
  SheetClose,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "~/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "~/components/ui/tabs";
import {
  ToggleGroup,
  ToggleGroupItem,
} from "~/components/ui/toggle-group";
import type {
  SwedenPeopleDraftOneRow,
  SwedenPeopleDraftRowsPage,
  SwedenPeopleDraftSourceObservation,
  SwedenPeopleDraftTwoRow,
} from "~/lib/sweden-people-draft-two.server";
import type { LlmRequestAvailability } from "~/lib/llm-availability.server";
import type { SwedenPeopleProfileBulkJob } from "~/lib/sweden-person-profile-bulk.server";

export interface PeopleDraftFilter {
  input: string;
  companyId: string;
  error: string;
  draftOneView: "all" | "unmapped";
  draftTwoView: "all" | "multiple-sources";
  draftTwoSources: DraftTwoSourceFilter[];
  draftTwoHasLlmSuggestion: boolean;
  draftOnePage: number;
  draftTwoPage: number;
  currentStep: PeopleDraftStep;
}

export type PeopleDraftStep = "draft-1" | "draft-2" | "final";
export type DraftTwoSourceFilter = "bolagsverket" | "esef" | "wikidata";

const numberFormat = new Intl.NumberFormat("en-US");
const sourceOrder: DraftTwoSourceFilter[] = [
  "bolagsverket",
  "esef",
  "wikidata",
];
const sourceLabels: Record<string, string> = {
  bolagsverket: "Bolagsverket",
  esef: "ESEF",
  wikidata: "Wikidata",
};
const unavailableLlm: LlmRequestAvailability = {
  ready: false,
  warning: "Configure and activate an LLM before starting bulk enhancement.",
  profile: null,
};

function sourceSortIndex(source: string): number {
  const index = sourceOrder.indexOf(source as DraftTwoSourceFilter);
  return index === -1 ? sourceOrder.length : index;
}

export function peopleDraftUrl({
  companyId = "",
  draftOneView = "all",
  draftTwoView = "all",
  draftTwoSources = [],
  draftTwoHasLlmSuggestion = false,
  draftOnePage = 1,
  draftTwoPage = 1,
  currentStep = "draft-1",
}: {
  companyId?: string;
  draftOneView?: PeopleDraftFilter["draftOneView"];
  draftTwoView?: PeopleDraftFilter["draftTwoView"];
  draftTwoSources?: PeopleDraftFilter["draftTwoSources"];
  draftTwoHasLlmSuggestion?: boolean;
  draftOnePage?: number;
  draftTwoPage?: number;
  currentStep?: PeopleDraftStep;
}): string {
  const searchParams = new URLSearchParams();
  searchParams.set("step", currentStep);
  if (companyId) searchParams.set("company_id", companyId);
  if (draftOneView === "unmapped") searchParams.set("draft_1", "unmapped");
  if (draftTwoView === "multiple-sources") {
    searchParams.set("draft_2", "multiple-sources");
  }
  for (const source of sourceOrder) {
    if (draftTwoSources.includes(source)) {
      searchParams.append("draft_2_source", source);
    }
  }
  if (draftTwoHasLlmSuggestion) {
    searchParams.set("draft_2_llm", "suggestion");
  }
  if (draftOnePage > 1) {
    searchParams.set("draft_1_page", String(draftOnePage));
  }
  if (draftTwoPage > 1) {
    searchParams.set("draft_2_page", String(draftTwoPage));
  }
  const query = searchParams.toString();
  return `/admin/se/people${query ? `?${query}` : ""}`;
}

function CompanyLink({ companyId }: { companyId: string }) {
  return (
    <Link
      to={`/company/se/${encodeURIComponent(companyId)}`}
      className="font-mono text-xs underline-offset-2 hover:underline"
      onClick={(event) => event.stopPropagation()}
    >
      {companyId}
    </Link>
  );
}

function ShortIdentifier({ value }: { value: string }) {
  return (
    <span
      className="block max-w-44 truncate font-mono text-xs text-muted-foreground"
      title={value}
    >
      {value}
    </span>
  );
}

export function PeopleDraftCompanyFilter({
  filter,
}: {
  filter: PeopleDraftFilter;
}) {
  return (
    <Form method="get" action="/admin/se/people">
      <input type="hidden" name="step" value={filter.currentStep} />
      {filter.draftOneView === "unmapped" ? (
        <input type="hidden" name="draft_1" value="unmapped" />
      ) : null}
      {filter.draftTwoView === "multiple-sources" ? (
        <input type="hidden" name="draft_2" value="multiple-sources" />
      ) : null}
      {filter.draftTwoSources.map((source) => (
        <input
          key={source}
          type="hidden"
          name="draft_2_source"
          value={source}
        />
      ))}
      {filter.draftTwoHasLlmSuggestion ? (
        <input type="hidden" name="draft_2_llm" value="suggestion" />
      ) : null}
      <FieldGroup className="max-w-xl">
        <Field data-invalid={Boolean(filter.error)}>
          <FieldLabel htmlFor="people-draft-company-id">Company ID</FieldLabel>
          <InputGroup>
            <InputGroupAddon>
              <SearchIcon />
            </InputGroupAddon>
            <InputGroupInput
              key={filter.input}
              id="people-draft-company-id"
              name="company_id"
              defaultValue={filter.input}
              placeholder="5565200028"
              aria-invalid={Boolean(filter.error)}
            />
            <InputGroupAddon align="inline-end">
              {filter.input ? (
                <InputGroupButton
                  size="icon-xs"
                  variant="ghost"
                  aria-label="Clear company filter"
                  title="Clear company filter"
                  nativeButton={false}
                  render={
                    <Link
                      to={peopleDraftUrl({
                        draftOneView: filter.draftOneView,
                        draftTwoView: filter.draftTwoView,
                        draftTwoSources: filter.draftTwoSources,
                        draftTwoHasLlmSuggestion:
                          filter.draftTwoHasLlmSuggestion,
                        currentStep: filter.currentStep,
                      })}
                    />
                  }
                >
                  <XIcon />
                </InputGroupButton>
              ) : null}
              <InputGroupButton type="submit" size="sm" variant="secondary">
                Filter
              </InputGroupButton>
            </InputGroupAddon>
          </InputGroup>
          <FieldDescription>
            Filters both Draft 1 and Draft 2 by Swedish organization number.
          </FieldDescription>
          <FieldError>{filter.error}</FieldError>
        </Field>
      </FieldGroup>
    </Form>
  );
}

function useFilteredRowSelection({
  rowIds,
  filterKey,
}: {
  rowIds: string[];
  filterKey: string;
}) {
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [allFiltered, setAllFiltered] = useState(false);

  useEffect(() => {
    setSelectedIds(new Set());
    setAllFiltered(false);
  }, [filterKey]);

  const pageSelected =
    rowIds.length > 0 &&
    (allFiltered || rowIds.every((rowId) => selectedIds.has(rowId)));

  function selectPage(checked: boolean) {
    setAllFiltered(false);
    setSelectedIds((current) => {
      const next = new Set(current);
      for (const rowId of rowIds) {
        if (checked) next.add(rowId);
        else next.delete(rowId);
      }
      return next;
    });
  }

  function selectRow(rowId: string, checked: boolean) {
    if (allFiltered && !checked) {
      setAllFiltered(false);
      setSelectedIds(new Set(rowIds.filter((candidate) => candidate !== rowId)));
      return;
    }
    setSelectedIds((current) => {
      const next = new Set(current);
      if (checked) next.add(rowId);
      else next.delete(rowId);
      return next;
    });
  }

  function clearSelection() {
    setAllFiltered(false);
    setSelectedIds(new Set());
  }

  return {
    selectedIds,
    allFiltered,
    pageSelected,
    selectPage,
    selectRow,
    selectAllFiltered: () => setAllFiltered(true),
    clearSelection,
  };
}

function SelectionToolbar({
  pageRowCount,
  totalRows,
  selectedCount,
  pageSelected,
  allFiltered,
  onSelectAllFiltered,
  onClear,
}: {
  pageRowCount: number;
  totalRows: number;
  selectedCount: number;
  pageSelected: boolean;
  allFiltered: boolean;
  onSelectAllFiltered: () => void;
  onClear: () => void;
}) {
  const canSelectEveryFilteredRow =
    pageSelected && !allFiltered && selectedCount < totalRows;
  const everyFilteredRowSelected =
    totalRows > 0 && (allFiltered || selectedCount >= totalRows);

  return (
    <div className="flex flex-wrap items-center justify-between gap-3 border-b px-4 py-3">
      <div className="flex flex-wrap items-center gap-2 text-sm">
        <Badge variant="outline">5-row exercise</Badge>
        <span className="text-muted-foreground">
          {everyFilteredRowSelected
            ? `All ${numberFormat.format(totalRows)} filtered entries selected`
            : selectedCount > 0
              ? `${numberFormat.format(selectedCount)} selected`
              : `Select any of the ${pageRowCount} entries on this page`}
        </span>
      </div>
      <div className="flex items-center gap-2">
        {canSelectEveryFilteredRow ? (
          <Button variant="outline" size="sm" onClick={onSelectAllFiltered}>
            <CheckCheckIcon data-icon="inline-start" />
            Select all {numberFormat.format(totalRows)} filtered
          </Button>
        ) : null}
        {selectedCount > 0 || allFiltered ? (
          <Button variant="ghost" size="sm" onClick={onClear}>
            Clear selection
          </Button>
        ) : null}
      </div>
    </div>
  );
}

type PaginationToken = number | "leading-ellipsis" | "trailing-ellipsis";

function paginationTokens(page: number, totalPages: number): PaginationToken[] {
  if (totalPages <= 7) {
    return Array.from({ length: totalPages }, (_, index) => index + 1);
  }

  const pages = new Set([1, totalPages, page - 1, page, page + 1]);
  const ordered = [...pages]
    .filter((candidate) => candidate >= 1 && candidate <= totalPages)
    .sort((left, right) => left - right);
  const tokens: PaginationToken[] = [];
  for (const [index, current] of ordered.entries()) {
    const previous = ordered[index - 1];
    if (previous !== undefined && current - previous > 1) {
      tokens.push(
        previous === 1 ? "leading-ellipsis" : "trailing-ellipsis",
      );
    }
    tokens.push(current);
  }
  return tokens;
}

function DraftRowsPagination({
  page,
  pageKind,
  filter,
}: {
  page: SwedenPeopleDraftRowsPage<unknown>;
  pageKind: "draft-1" | "draft-2";
  filter: PeopleDraftFilter;
}) {
  if (page.totalRows === 0) return null;

  const firstRow = (page.page - 1) * page.pageSize + 1;
  const lastRow = Math.min(page.totalRows, firstRow + page.rows.length - 1);
  const urlForPage = (targetPage: number) =>
    peopleDraftUrl({
      companyId: filter.companyId,
      draftOneView: filter.draftOneView,
      draftTwoView: filter.draftTwoView,
      draftTwoSources: filter.draftTwoSources,
      draftTwoHasLlmSuggestion: filter.draftTwoHasLlmSuggestion,
      draftOnePage:
        pageKind === "draft-1" ? targetPage : filter.draftOnePage,
      draftTwoPage:
        pageKind === "draft-2" ? targetPage : filter.draftTwoPage,
      currentStep: filter.currentStep,
    });

  return (
    <div className="flex w-full flex-col items-center justify-between gap-3 px-4 py-3 sm:flex-row">
      <p className="text-sm text-muted-foreground">
        Showing {numberFormat.format(firstRow)}–{numberFormat.format(lastRow)} of{" "}
        {numberFormat.format(page.totalRows)} filtered entries
      </p>
      <Pagination className="mx-0 w-auto">
        <PaginationContent>
          <PaginationItem>
            <PaginationPrevious
              href={page.page > 1 ? urlForPage(page.page - 1) : undefined}
              aria-disabled={page.page <= 1}
              tabIndex={page.page <= 1 ? -1 : undefined}
            />
          </PaginationItem>
          {paginationTokens(page.page, page.totalPages).map((token) =>
            typeof token === "number" ? (
              <PaginationItem key={token}>
                <PaginationLink
                  href={urlForPage(token)}
                  isActive={token === page.page}
                  aria-label={`Go to page ${token}`}
                >
                  {token}
                </PaginationLink>
              </PaginationItem>
            ) : (
              <PaginationItem key={token}>
                <PaginationEllipsis />
              </PaginationItem>
            ),
          )}
          <PaginationItem>
            <PaginationNext
              href={
                page.page < page.totalPages
                  ? urlForPage(page.page + 1)
                  : undefined
              }
              aria-disabled={page.page >= page.totalPages}
              tabIndex={page.page >= page.totalPages ? -1 : undefined}
            />
          </PaginationItem>
        </PaginationContent>
      </Pagination>
    </div>
  );
}

function readablePayload(payload: string | null): string {
  if (!payload) return "No raw payload stored.";
  try {
    return JSON.stringify(JSON.parse(payload), null, 2);
  } catch {
    return payload;
  }
}

function SourceObservationCard({
  observation,
}: {
  observation: SwedenPeopleDraftSourceObservation;
}) {
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline">
            {observation.fiscal_year ?? "No year"}
          </Badge>
          <Badge variant="secondary">
            {observation.role_original || "No source role"}
          </Badge>
        </div>
        <CardTitle>{observation.name}</CardTitle>
        <CardDescription>
          {observation.description || "This source has no person description."}
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <dl className="grid gap-3 sm:grid-cols-2">
          <div className="flex flex-col gap-1">
            <dt className="text-xs text-muted-foreground">Observation ID</dt>
            <dd className="break-all font-mono text-xs">
              {observation.observation_id}
            </dd>
          </div>
          <div className="flex flex-col gap-1">
            <dt className="text-xs text-muted-foreground">Source entity ID</dt>
            <dd className="break-all font-mono text-xs">
              {observation.source_entity_id}
            </dd>
          </div>
          <div className="flex flex-col gap-1">
            <dt className="text-xs text-muted-foreground">Source record UID</dt>
            <dd className="break-all font-mono text-xs">
              {observation.source_record_uid}
            </dd>
          </div>
          <div className="flex flex-col gap-1">
            <dt className="text-xs text-muted-foreground">Observed at</dt>
            <dd className="text-xs">
              {observation.source_observed_at || "Not supplied"}
            </dd>
          </div>
        </dl>
        <details>
          <summary className="cursor-pointer text-sm font-medium">
            Raw source payload
          </summary>
          <pre className="mt-3 max-h-80 overflow-auto whitespace-pre-wrap break-words rounded-md border bg-muted p-3 text-xs">
            {readablePayload(observation.source_payload_json)}
          </pre>
        </details>
      </CardContent>
    </Card>
  );
}

interface EvidenceSheetPerson {
  id: string;
  draftTwoId: string | null;
  sePersonId: string | null;
  companyId: string;
  name: string;
  position: string;
  period: string;
  observations: SwedenPeopleDraftSourceObservation[];
}

function PersonEvidenceSheet({
  person,
  onOpenChange,
}: {
  person: EvidenceSheetPerson | null;
  onOpenChange: (open: boolean) => void;
}) {
  const location = useLocation();
  const returnTo = `${location.pathname}${location.search}`;
  const observationsBySource = useMemo(() => {
    const grouped = new Map<string, SwedenPeopleDraftSourceObservation[]>();
    for (const observation of person?.observations ?? []) {
      const sourceRows = grouped.get(observation.source) ?? [];
      sourceRows.push(observation);
      grouped.set(observation.source, sourceRows);
    }
    return [...grouped.entries()].sort(
      ([left], [right]) =>
        sourceSortIndex(left) - sourceSortIndex(right),
    );
  }, [person]);
  return (
    <Sheet open={person !== null} onOpenChange={onOpenChange}>
      <SheetContent className="data-[side=right]:w-[calc(100vw-2rem)] data-[side=right]:sm:max-w-6xl">
        <SheetHeader className="border-b pr-12">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="outline">{person?.companyId ?? "Company"}</Badge>
            {person?.position ? (
              <Badge variant="secondary">{person.position}</Badge>
            ) : null}
          </div>
          <SheetTitle>{person?.name ?? "Person evidence"}</SheetTitle>
          <SheetDescription>
            {person
              ? `${person.period} · Compare the original values retained from every contributing source.`
              : "Compare retained source evidence."}
          </SheetDescription>
          <div className="flex flex-wrap items-center gap-2">
            {person?.draftTwoId ? (
              <Link
                className={buttonVariants({ variant: "outline" })}
                to={`/admin/se/people/llm-input/${encodeURIComponent(person.draftTwoId)}?${new URLSearchParams({ return_to: returnTo })}`}
              >
                View prepared LLM request
              </Link>
            ) : null}
            {person?.sePersonId ? (
              <Link
                className={buttonVariants({ variant: "ghost" })}
                to={`/admin/se/people/person/${encodeURIComponent(person.companyId)}/${encodeURIComponent(person.sePersonId)}`}
              >
                Review in ClickHouse
              </Link>
            ) : null}
          </div>
        </SheetHeader>
        <div className="min-h-0 flex-1 overflow-y-auto px-4 pb-6">
          {person && observationsBySource.length > 0 ? (
            <div className="flex flex-col gap-4">
              <Tabs
                key={person.id}
                defaultValue={observationsBySource[0][0]}
                className="gap-4"
              >
                <TabsList variant="line" className="w-full justify-start">
                  {observationsBySource.map(([source, observations]) => (
                    <TabsTrigger key={source} value={source}>
                      {sourceLabels[source] ?? source} ({observations.length})
                    </TabsTrigger>
                  ))}
                </TabsList>
                {observationsBySource.map(([source, observations]) => (
                  <TabsContent key={source} value={source}>
                    <div className="flex flex-col gap-3">
                      {observations.map((observation) => (
                        <SourceObservationCard
                          key={observation.observation_id}
                          observation={observation}
                        />
                      ))}
                    </div>
                  </TabsContent>
                ))}
              </Tabs>
            </div>
          ) : (
            <Empty className="min-h-64">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <DatabaseIcon />
                </EmptyMedia>
                <EmptyTitle>No source observations found</EmptyTitle>
                <EmptyDescription>
                  This Draft 2 row has source identifiers but the corresponding
                  Draft 1 rows are unavailable.
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}

export function DraftOneRowsTable({
  rows,
  page,
  filter,
}: {
  rows: SwedenPeopleDraftOneRow[];
  page: SwedenPeopleDraftRowsPage<SwedenPeopleDraftOneRow>;
  filter: PeopleDraftFilter;
}) {
  const navigate = useNavigate();
  const [detailRow, setDetailRow] = useState<SwedenPeopleDraftOneRow | null>(
    null,
  );
  const selection = useFilteredRowSelection({
    rowIds: rows.map((row) => row.observation_id),
    filterKey: `${filter.companyId}:${filter.draftOneView}`,
  });
  const selectedCount = selection.allFiltered
    ? page.totalRows
    : selection.selectedIds.size;

  function changeDraftOneView(values: unknown) {
    if (!Array.isArray(values)) return;
    const selected = values[0];
    if (selected !== "all" && selected !== "unmapped") return;
    navigate(
      peopleDraftUrl({
        companyId: filter.companyId,
        draftOneView: selected,
        draftTwoView: filter.draftTwoView,
        draftTwoSources: filter.draftTwoSources,
        draftTwoHasLlmSuggestion: filter.draftTwoHasLlmSuggestion,
        draftTwoPage: filter.draftTwoPage,
        currentStep: filter.currentStep,
      }),
    );
  }

  const detailPerson: EvidenceSheetPerson | null = detailRow
    ? {
        id: detailRow.observation_id,
        draftTwoId: null,
        sePersonId: null,
        companyId: detailRow.company_id,
        name: detailRow.name,
        position: detailRow.role_original || "Original source observation",
        period: detailRow.fiscal_year
          ? String(detailRow.fiscal_year)
          : "No fiscal year",
        observations: [detailRow],
      }
    : null;

  return (
    <>
      <Card>
        <CardHeader className="border-b">
          <div className="flex flex-col justify-between gap-3 sm:flex-row sm:items-start">
            <div className="flex flex-col gap-1.5">
              <CardTitle>
                {filter.draftOneView === "unmapped"
                  ? "Unmapped Draft 1 observations"
                  : "Draft 1 observations"}
              </CardTitle>
              <CardDescription>
                {numberFormat.format(page.totalRows)} filtered immutable source
                observations. Open a row to inspect its retained source values.
              </CardDescription>
            </div>
            <ToggleGroup
              value={[filter.draftOneView]}
              onValueChange={changeDraftOneView}
              variant="outline"
              spacing={0}
              size="sm"
              aria-label="Draft 1 observation view"
            >
              <ToggleGroupItem value="all">All</ToggleGroupItem>
              <ToggleGroupItem value="unmapped">Unmapped</ToggleGroupItem>
            </ToggleGroup>
          </div>
        </CardHeader>
        <CardContent className="px-0">
          {rows.length === 0 ? (
            <Empty className="min-h-48">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <DatabaseIcon />
                </EmptyMedia>
                <EmptyTitle>No Draft 1 rows found</EmptyTitle>
                <EmptyDescription>
                  Initialize Draft 1 or change the company filter.
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <>
              <SelectionToolbar
                pageRowCount={rows.length}
                totalRows={page.totalRows}
                selectedCount={selectedCount}
                pageSelected={selection.pageSelected}
                allFiltered={selection.allFiltered}
                onSelectAllFiltered={selection.selectAllFiltered}
                onClear={selection.clearSelection}
              />
              <Table className="min-w-[76rem]">
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-12">
                      <Checkbox
                        checked={selection.pageSelected}
                        onCheckedChange={selection.selectPage}
                        aria-label="Select all Draft 1 rows on this page"
                      />
                    </TableHead>
                    <TableHead>Company</TableHead>
                    <TableHead>Person</TableHead>
                    <TableHead>Source</TableHead>
                    <TableHead>Position</TableHead>
                    <TableHead>Year</TableHead>
                    <TableHead>Description</TableHead>
                    <TableHead>Source evidence</TableHead>
                    <TableHead className="text-right">Details</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.map((row) => (
                    <TableRow
                      key={row.observation_id}
                      className="cursor-pointer"
                      onClick={() => setDetailRow(row)}
                    >
                      <TableCell onClick={(event) => event.stopPropagation()}>
                        <Checkbox
                          checked={
                            selection.allFiltered ||
                            selection.selectedIds.has(row.observation_id)
                          }
                          onCheckedChange={(checked) =>
                            selection.selectRow(row.observation_id, checked)
                          }
                          aria-label={`Select ${row.name}`}
                        />
                      </TableCell>
                      <TableCell>
                        <CompanyLink companyId={row.company_id} />
                      </TableCell>
                      <TableCell className="font-medium">{row.name}</TableCell>
                      <TableCell>
                        <Badge variant="outline">{row.source}</Badge>
                      </TableCell>
                      <TableCell className="max-w-64 whitespace-normal">
                        {row.role_original || "—"}
                      </TableCell>
                      <TableCell className="tabular-nums">
                        {row.fiscal_year ?? "—"}
                      </TableCell>
                      <TableCell className="max-w-72 whitespace-normal text-muted-foreground">
                        {row.description || "—"}
                      </TableCell>
                      <TableCell>
                        <ShortIdentifier value={row.observation_id} />
                        <ShortIdentifier value={row.source_entity_id} />
                      </TableCell>
                      <TableCell className="text-right">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={(event) => {
                            event.stopPropagation();
                            setDetailRow(row);
                          }}
                        >
                          <EyeIcon data-icon="inline-start" />
                          Open
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </>
          )}
        </CardContent>
        <CardFooter className="p-0">
          <DraftRowsPagination
            page={page}
            pageKind="draft-1"
            filter={filter}
          />
        </CardFooter>
      </Card>
      <PersonEvidenceSheet
        person={detailPerson}
        onOpenChange={(open) => {
          if (!open) setDetailRow(null);
        }}
      />
    </>
  );
}

function EvidenceCell({
  label,
  ids,
  descriptions,
}: {
  label: string;
  ids: string[];
  descriptions: string[];
}) {
  if (ids.length === 0) {
    return <TableCell className="text-muted-foreground">—</TableCell>;
  }
  return (
    <TableCell className="max-w-72 whitespace-normal">
      <Badge variant="outline">
        {label} · {ids.length}
      </Badge>
      {descriptions.map((description) => (
        <p key={description} className="mt-2 text-xs text-muted-foreground">
          {description}
        </p>
      ))}
      {ids.slice(0, 2).map((id) => (
        <ShortIdentifier key={id} value={id} />
      ))}
    </TableCell>
  );
}

function DraftTwoFiltersSheet({ filter }: { filter: PeopleDraftFilter }) {
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const [onlyMultipleSources, setOnlyMultipleSources] = useState(
    filter.draftTwoView === "multiple-sources",
  );
  const [selectedSources, setSelectedSources] = useState<
    DraftTwoSourceFilter[]
  >(filter.draftTwoSources);
  const [hasLlmSuggestion, setHasLlmSuggestion] = useState(
    filter.draftTwoHasLlmSuggestion,
  );
  const activeFilterCount =
    (filter.draftTwoView === "multiple-sources" ? 1 : 0) +
    filter.draftTwoSources.length +
    (filter.draftTwoHasLlmSuggestion ? 1 : 0);

  function changeOpen(nextOpen: boolean) {
    if (nextOpen) {
      setOnlyMultipleSources(filter.draftTwoView === "multiple-sources");
      setSelectedSources(filter.draftTwoSources);
      setHasLlmSuggestion(filter.draftTwoHasLlmSuggestion);
    }
    setOpen(nextOpen);
  }

  function changeSource(source: DraftTwoSourceFilter, checked: boolean) {
    setSelectedSources((current) =>
      checked
        ? sourceOrder.filter(
            (candidate) => candidate === source || current.includes(candidate),
          )
        : current.filter((candidate) => candidate !== source),
    );
  }

  function applyFilters(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    navigate(
      peopleDraftUrl({
        companyId: filter.companyId,
        draftOneView: filter.draftOneView,
        draftTwoView: onlyMultipleSources ? "multiple-sources" : "all",
        draftTwoSources: selectedSources,
        draftTwoHasLlmSuggestion: hasLlmSuggestion,
        draftOnePage: filter.draftOnePage,
        currentStep: filter.currentStep,
      }),
    );
    setOpen(false);
  }

  return (
    <Sheet open={open} onOpenChange={changeOpen}>
      <SheetTrigger render={<Button variant="outline" size="sm" />}>
        <ListFilterIcon data-icon="inline-start" />
        Filters
        {activeFilterCount > 0 ? (
          <Badge variant="secondary">{activeFilterCount}</Badge>
        ) : null}
      </SheetTrigger>
      <SheetContent>
        <SheetHeader>
          <SheetTitle>Filter Draft 2 people</SheetTitle>
          <SheetDescription>
            Limit rows by source count, required evidence, and saved LLM
            suggestions.
          </SheetDescription>
        </SheetHeader>
        <form
          onSubmit={applyFilters}
          className="flex min-h-0 flex-1 flex-col"
        >
          <div className="min-h-0 flex-1 overflow-y-auto px-4 py-2">
            <FieldGroup>
              <FieldSet>
                <FieldLegend>Row scope</FieldLegend>
                <FieldDescription>
                  Limit the table to candidates supported by more than one
                  source.
                </FieldDescription>
                <FieldGroup data-slot="checkbox-group">
                  <Field orientation="horizontal">
                    <Checkbox
                      id="draft-two-multiple-sources"
                      checked={onlyMultipleSources}
                      onCheckedChange={setOnlyMultipleSources}
                    />
                    <FieldLabel htmlFor="draft-two-multiple-sources">
                      Multiple-source rows only
                    </FieldLabel>
                  </Field>
                </FieldGroup>
              </FieldSet>

              <FieldSet>
                <FieldLegend>Required sources</FieldLegend>
                <FieldDescription>
                  A result must contain evidence from every selected source.
                </FieldDescription>
                <FieldGroup data-slot="checkbox-group">
                  {sourceOrder.map((source) => {
                    const id = `draft-two-source-${source}`;
                    return (
                      <Field key={source} orientation="horizontal">
                        <Checkbox
                          id={id}
                          checked={selectedSources.includes(source)}
                          onCheckedChange={(checked) =>
                            changeSource(source, checked)
                          }
                        />
                        <FieldLabel htmlFor={id}>
                          {sourceLabels[source]}
                        </FieldLabel>
                      </Field>
                    );
                  })}
                </FieldGroup>
              </FieldSet>

              <FieldSet>
                <FieldLegend>LLM suggestion</FieldLegend>
                <FieldDescription>
                  Suggestions include all stored attempts, including responses
                  that may need retrying after Draft 2 changes.
                </FieldDescription>
                <FieldGroup data-slot="checkbox-group">
                  <Field orientation="horizontal">
                    <Checkbox
                      id="draft-two-has-llm-suggestion"
                      checked={hasLlmSuggestion}
                      onCheckedChange={setHasLlmSuggestion}
                    />
                    <FieldLabel htmlFor="draft-two-has-llm-suggestion">
                      Has a saved LLM suggestion
                    </FieldLabel>
                  </Field>
                </FieldGroup>
              </FieldSet>
            </FieldGroup>
          </div>
          <SheetFooter className="sm:flex-row sm:justify-end">
            <Button
              type="button"
              variant="ghost"
              onClick={() => {
                setOnlyMultipleSources(false);
                setSelectedSources([]);
                setHasLlmSuggestion(false);
              }}
            >
              Clear filters
            </Button>
            <SheetClose render={<Button type="button" variant="outline" />}>
              Cancel
            </SheetClose>
            <Button type="submit">Apply filters</Button>
          </SheetFooter>
        </form>
      </SheetContent>
    </Sheet>
  );
}

export function DraftTwoRowsTable({
  rows,
  page,
  filter,
  bulkJob = null,
  llmAvailability = unavailableLlm,
}: {
  rows: SwedenPeopleDraftTwoRow[];
  page: SwedenPeopleDraftRowsPage<SwedenPeopleDraftTwoRow>;
  filter: PeopleDraftFilter;
  bulkJob?: SwedenPeopleProfileBulkJob | null;
  llmAvailability?: LlmRequestAvailability;
}) {
  const [detailRow, setDetailRow] = useState<SwedenPeopleDraftTwoRow | null>(
    null,
  );
  const selection = useFilteredRowSelection({
    rowIds: rows.map((row) => row.draft_2_id),
    filterKey: `${filter.companyId}:${filter.draftTwoView}:${filter.draftTwoSources.join(",")}:${filter.draftTwoHasLlmSuggestion}`,
  });
  const selectedCount = selection.allFiltered
    ? page.totalRows
    : selection.selectedIds.size;

  const detailPerson: EvidenceSheetPerson | null = detailRow
    ? {
        id: detailRow.draft_2_id,
        draftTwoId: detailRow.draft_2_id,
        sePersonId: detailRow.se_person_id ?? null,
        companyId: detailRow.company_id,
        name: detailRow.name,
        position: detailRow.position,
        period: `${detailRow.start_year ?? "?"}–${detailRow.end_year ?? "current"}`,
        observations: detailRow.source_observations ?? [],
      }
    : null;

  return (
    <>
      <Card>
        <CardHeader className="border-b">
          <div className="flex flex-col justify-between gap-3 sm:flex-row sm:items-start">
            <div className="flex flex-col gap-1.5">
              <CardTitle>
                {filter.draftTwoView === "multiple-sources"
                  ? "Multiple-source Draft 2 rows"
                  : "Draft 2 merged rows"}
              </CardTitle>
              <CardDescription>
                {numberFormat.format(page.totalRows)} filtered person-position
                candidates. Open a row to compare the original sources in
                separate tabs.
              </CardDescription>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <DraftTwoFiltersSheet filter={filter} />
            </div>
          </div>
        </CardHeader>
        <CardContent className="px-0">
          {rows.length === 0 ? (
            <Empty className="min-h-48">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <DatabaseIcon />
                </EmptyMedia>
                <EmptyTitle>No Draft 2 rows found</EmptyTitle>
                <EmptyDescription>
                  {filter.draftTwoSources.length > 0 ||
                  filter.draftTwoHasLlmSuggestion
                    ? "No rows match the current source and LLM suggestion filters."
                    : filter.draftTwoView === "multiple-sources"
                      ? "No rows merged from multiple sources match the current company filter."
                      : "Create Draft 2 or change the company filter."}
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <>
              <SelectionToolbar
                pageRowCount={rows.length}
                totalRows={page.totalRows}
                selectedCount={selectedCount}
                pageSelected={selection.pageSelected}
                allFiltered={selection.allFiltered}
                onSelectAllFiltered={selection.selectAllFiltered}
                onClear={selection.clearSelection}
              />
              <PeopleProfileBulkEnhancer
                filter={filter}
                selectedIds={selection.selectedIds}
                allFiltered={selection.allFiltered}
                selectedCount={selectedCount}
                initialJob={bulkJob}
                llmAvailability={llmAvailability}
                onStarted={selection.clearSelection}
              />
              <Table className="min-w-[106rem]">
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-12">
                      <Checkbox
                        checked={selection.pageSelected}
                        onCheckedChange={selection.selectPage}
                        aria-label="Select all Draft 2 rows on this page"
                      />
                    </TableHead>
                    <TableHead>Company</TableHead>
                    <TableHead>Person</TableHead>
                    <TableHead>Canonical position</TableHead>
                    <TableHead>Period</TableHead>
                    <TableHead>Sources</TableHead>
                    <TableHead>Bolagsverket evidence</TableHead>
                    <TableHead>ESEF evidence</TableHead>
                    <TableHead>Wikidata evidence</TableHead>
                    <TableHead>LLM suggestion</TableHead>
                    <TableHead className="text-right">Details</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.map((row) => (
                    <TableRow
                      key={row.draft_2_id}
                      className="cursor-pointer"
                      onClick={() => setDetailRow(row)}
                    >
                      <TableCell onClick={(event) => event.stopPropagation()}>
                        <Checkbox
                          checked={
                            selection.allFiltered ||
                            selection.selectedIds.has(row.draft_2_id)
                          }
                          onCheckedChange={(checked) =>
                            selection.selectRow(row.draft_2_id, checked)
                          }
                          aria-label={`Select ${row.name}`}
                        />
                      </TableCell>
                      <TableCell>
                        <CompanyLink companyId={row.company_id} />
                      </TableCell>
                      <TableCell className="font-medium">{row.name}</TableCell>
                      <TableCell>
                        <Badge variant="secondary">{row.position}</Badge>
                      </TableCell>
                      <TableCell className="tabular-nums">
                        {row.start_year ?? "?"} – {row.end_year ?? "current"}
                      </TableCell>
                      <TableCell>
                        {row.source_count}{" "}
                        {row.source_count === 1 ? "source" : "sources"}
                        {" · "}
                        {row.observation_count}{" "}
                        {row.observation_count === 1 ? "row" : "rows"}
                      </TableCell>
                      <EvidenceCell
                        label="Bolagsverket"
                        ids={row.bolagsverket_source_ids}
                        descriptions={row.bolagsverket_descriptions}
                      />
                      <EvidenceCell
                        label="ESEF"
                        ids={row.esef_source_ids}
                        descriptions={row.esef_descriptions}
                      />
                      <EvidenceCell
                        label="Wikidata"
                        ids={row.wikidata_source_ids}
                        descriptions={row.wikidata_descriptions}
                      />
                      <TableCell>
                        {row.has_llm_suggestion ? (
                          <Badge variant="secondary">
                            <SparklesIcon data-icon="inline-start" />
                            Saved
                          </Badge>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </TableCell>
                      <TableCell className="text-right">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={(event) => {
                            event.stopPropagation();
                            setDetailRow(row);
                          }}
                        >
                          <EyeIcon data-icon="inline-start" />
                          Compare
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </>
          )}
        </CardContent>
        <CardFooter className="p-0">
          <DraftRowsPagination
            page={page}
            pageKind="draft-2"
            filter={filter}
          />
        </CardFooter>
      </Card>
      <PersonEvidenceSheet
        person={detailPerson}
        onOpenChange={(open) => {
          if (!open) setDetailRow(null);
        }}
      />
    </>
  );
}
