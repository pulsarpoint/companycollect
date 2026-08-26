import { useEffect, useState } from "react";
import { Link, useFetcher, useRevalidator } from "react-router";
import {
  ActivityIcon,
  AlertTriangleIcon,
  ArrowUpRightIcon,
  BotIcon,
  CheckCircle2Icon,
  Clock3Icon,
  DatabaseIcon,
  PlayIcon,
  RefreshCwIcon,
} from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
import {
  Combobox,
  ComboboxChip,
  ComboboxChips,
  ComboboxChipsInput,
  ComboboxContent,
  ComboboxEmpty,
  ComboboxItem,
  ComboboxList,
  ComboboxValue,
} from "~/components/ui/combobox";
import {
  Field,
  FieldContent,
  FieldDescription,
  FieldGroup,
  FieldLabel,
  FieldLegend,
  FieldSet,
} from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import { Separator } from "~/components/ui/separator";
import {
  Sheet,
  SheetContent,
  SheetDescription,
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
import { Textarea } from "~/components/ui/textarea";
import type { AssetMaterialization, DagsterRun } from "~/lib/dagster.server";
import type {
  EsefInventoryAsset,
  EsefOperationsStatus,
  EsefOverview,
} from "~/lib/esef-operations.server";

export interface EsefProfileOption {
  profileId: string;
  name: string;
  provider: string;
  model: string;
  baseUrl: string;
  isActive: boolean;
}

export interface EsefRuntimeDefaults {
  temperature: number;
  concurrency: number;
  maxDocuments: number;
  maxEvidenceChars: number;
  timeoutSeconds: number;
}

export interface EsefLaunchActionResult {
  ok: boolean;
  error: string;
  launched: null | {
    runId: string;
    runUrl: string | null;
    status: string;
    requestId: string;
    model: string;
    selection: string;
  };
}

interface EsefOperationsWorkspaceProps {
  overview: EsefOverview | null;
  error: string;
  profiles: EsefProfileOption[];
  countries: string[];
  countryError: string;
  runtimeDefaults: EsefRuntimeDefaults;
  runUrls: Record<string, string>;
}

const REFRESH_BEHAVIORS = [
  { label: "Reuse existing results", value: "reuse_existing" },
  { label: "Refresh existing results", value: "refresh_existing" },
  {
    label: "Reprocess without calling the model",
    value: "reprocess_existing_without_model",
  },
];

function formatMaterializationTime(milliseconds: number | null): string {
  if (milliseconds === null) return "Never";
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "Europe/Belgrade",
  }).format(new Date(milliseconds));
}

function formatRunTime(seconds: number | null): string {
  return seconds === null
    ? "Not started"
    : formatMaterializationTime(seconds * 1_000);
}

function runVariant(
  status: string,
): "default" | "secondary" | "destructive" | "outline" {
  if (status === "SUCCESS") return "default";
  if (status === "FAILURE" || status === "CANCELED") return "destructive";
  if (
    status === "QUEUED" ||
    status === "NOT_STARTED" ||
    status === "STARTING" ||
    status === "STARTED" ||
    status === "CANCELING"
  ) {
    return "secondary";
  }
  return "outline";
}

function runLabel(status: string): string {
  if (status === "SUCCESS") return "Succeeded";
  if (status === "FAILURE") return "Failed";
  if (status === "CANCELED") return "Canceled";
  if (status === "STARTED" || status === "STARTING") return "Running";
  if (status === "CANCELING") return "Canceling";
  return "Queued";
}

function activeRun(asset: EsefInventoryAsset): DagsterRun | null {
  return asset.activeRuns[0] ?? null;
}

function assetState(asset: EsefInventoryAsset): {
  label: string;
  variant: "default" | "secondary" | "destructive" | "outline";
  priority: number;
} {
  const run = activeRun(asset);
  if (run) {
    return { label: runLabel(run.status), variant: "secondary", priority: 0 };
  }
  if (asset.staleStatus === "MISSING" || asset.materialization === null) {
    return { label: "Missing", variant: "destructive", priority: 1 };
  }
  if (asset.staleStatus === "STALE") {
    return { label: "Stale", variant: "outline", priority: 2 };
  }
  if (asset.staleStatus === "FRESH") {
    return { label: "Fresh", variant: "default", priority: 3 };
  }
  return { label: "Unknown", variant: "outline", priority: 4 };
}

function sortedAssets(assets: EsefInventoryAsset[]): EsefInventoryAsset[] {
  return [...assets].sort((left, right) => {
    const statusOrder = assetState(left).priority - assetState(right).priority;
    return statusOrder || left.asset.localeCompare(right.asset);
  });
}

function runLink(runId: string, runUrls: Record<string, string>) {
  const url = runUrls[runId];
  if (!url)
    return <span className="font-mono text-xs">{runId.slice(0, 8)}</span>;
  return (
    <a
      className="inline-flex items-center gap-1 font-mono text-xs underline-offset-4 hover:underline"
      href={url}
      rel="noreferrer"
      target="_blank"
    >
      {runId.slice(0, 8)}
      <ArrowUpRightIcon className="size-3" />
    </a>
  );
}

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function strings(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === "string")
    : [];
}

const REGION_NAMES = new Intl.DisplayNames(["en"], { type: "region" });

function countryLabel(countryIso2: string): string {
  const name = REGION_NAMES.of(countryIso2);
  return name && name !== countryIso2
    ? `${countryIso2} — ${name}`
    : countryIso2;
}

function enrichmentRunDetails(run: DagsterRun): {
  model: string;
  selection: string;
} {
  const ops = record(run.runConfig.ops);
  const operation = record(ops.esef_document_company_information_clickhouse);
  const config = record(operation.config);
  const model =
    run.tags["corpscout/llm_model"] ??
    (typeof config.model === "string" ? config.model : "Not recorded");
  const documents = strings(config.source_document_ids);
  const companies = strings(config.company_ids);
  const countries = strings(config.country_iso2s);
  const limit =
    typeof config.max_documents === "number" ? config.max_documents : null;
  if (documents.length === 1) return { model, selection: documents[0] };
  if (documents.length > 1) {
    return { model, selection: `${documents.length} ESEF filings` };
  }
  if (companies.length > 0) {
    return { model, selection: `${companies.length} companies` };
  }
  if (countries.length === 1) {
    return { model, selection: `Country ${countries[0]}` };
  }
  if (countries.length > 1) {
    return { model, selection: `${countries.length} countries` };
  }
  return {
    model,
    selection:
      limit === null ? "All eligible documents" : `Up to ${limit} documents`,
  };
}

function ProfileSelect({ profiles }: { profiles: EsefProfileOption[] }) {
  const selected = profiles.find((profile) => profile.isActive) ?? profiles[0];
  const items = profiles.map((profile) => ({
    label: `${profile.name} — ${profile.model}`,
    value: profile.profileId,
  }));
  return (
    <Field>
      <FieldLabel htmlFor="esef-profile">LLM profile</FieldLabel>
      <Select
        items={items}
        name="profile_id"
        defaultValue={selected?.profileId ?? ""}
      >
        <SelectTrigger id="esef-profile" className="w-full">
          <SelectValue placeholder="No profile configured" />
        </SelectTrigger>
        <SelectContent>
          <SelectGroup>
            {profiles.map((profile) => (
              <SelectItem key={profile.profileId} value={profile.profileId}>
                {profile.name} — {profile.model}
              </SelectItem>
            ))}
          </SelectGroup>
        </SelectContent>
      </Select>
      <FieldDescription>
        Provider, model, and base URL come from central LLM settings. API keys
        remain on the Dagster host.
      </FieldDescription>
    </Field>
  );
}

function EnrichmentBatchStatistics({
  batch,
  headingId,
}: {
  batch: AssetMaterialization | null;
  headingId: string;
}) {
  const numbers = batch?.numbers ?? {};
  const hasStatistics =
    numbers.attempted_document_count !== undefined ||
    numbers.processed_document_count !== undefined ||
    numbers.failed_document_count !== undefined;
  if (!batch || !hasStatistics) return null;
  return (
    <section className="flex flex-col gap-3" aria-labelledby={headingId}>
      <div>
        <h3 id={headingId} className="font-medium">
          Latest completed enrichment batch
        </h3>
        <p className="text-sm text-muted-foreground">
          Document outcomes reported by the latest Dagster materialization.
        </p>
      </div>
      <dl className="grid grid-cols-2 gap-4 rounded-lg border p-4 sm:grid-cols-3 lg:grid-cols-6">
        {[
          ["Attempted", numbers.attempted_document_count ?? 0],
          [
            "Processed",
            numbers.processed_document_count ??
              numbers.selected_document_count ??
              0,
          ],
          ["New results", numbers.enriched_document_count ?? 0],
          ["Reused", numbers.reused_enrichment_count ?? 0],
          ["Failed", numbers.failed_document_count ?? 0],
          ["Rate limited", numbers.rate_limited_document_count ?? 0],
        ].map(([label, value]) => (
          <div key={label} className="flex flex-col gap-1">
            <dt className="text-xs text-muted-foreground">{label}</dt>
            <dd className="text-xl font-semibold tabular-nums">{value}</dd>
          </div>
        ))}
      </dl>
      {(numbers.failed_document_count ?? 0) > 0 ? (
        <Alert variant="destructive">
          <AlertTriangleIcon />
          <AlertTitle>Some documents need another attempt</AlertTitle>
          <AlertDescription>
            Successful artifacts were preserved. A later run will reuse them and
            retry documents that did not produce a valid response.
          </AlertDescription>
        </Alert>
      ) : null}
    </section>
  );
}

function EnrichmentActionSheet({
  status,
  profiles,
  countries,
  countryError,
  runtimeDefaults,
  runUrls,
}: {
  status: EsefOperationsStatus | null;
  profiles: EsefProfileOption[];
  countries: string[];
  countryError: string;
  runtimeDefaults: EsefRuntimeDefaults;
  runUrls: Record<string, string>;
}) {
  const fetcher = useFetcher<EsefLaunchActionResult>();
  const [documentLimitEnabled, setDocumentLimitEnabled] = useState(false);
  const [selectedCountryIso2s, setSelectedCountryIso2s] = useState<string[]>(
    [],
  );
  const busy = fetcher.state !== "idle";
  const latestRun = status?.latestEnrichmentRun ?? null;
  const latestBatch = status?.latestBatch ?? null;

  return (
    <Sheet>
      <SheetTrigger render={<Button />}>
        <PlayIcon data-icon="inline-start" />
        Enhance company information
      </SheetTrigger>
      <SheetContent
        side="right"
        className="w-full data-[side=right]:sm:max-w-3xl sm:max-w-3xl"
      >
        <SheetHeader>
          <SheetTitle>Enhance ESEF company information</SheetTitle>
          <SheetDescription>
            Launches the fixed esef_document_company_information_job with a
            server-validated scope and LLM profile.
          </SheetDescription>
        </SheetHeader>
        <div className="min-h-0 flex-1 overflow-y-auto px-4 pb-6">
          <div className="flex flex-col gap-6">
            {status && !status.canLaunch ? (
              <Alert>
                <Clock3Icon />
                <AlertTitle>
                  Action unavailable while Dagster is busy
                </AlertTitle>
                <AlertDescription>
                  <ul className="flex list-disc flex-col gap-1 pl-4">
                    {status.blockingReasons.map((reason) => (
                      <li key={reason}>{reason}</li>
                    ))}
                  </ul>
                </AlertDescription>
              </Alert>
            ) : null}

            {fetcher.data ? (
              <Alert variant={fetcher.data.ok ? "default" : "destructive"}>
                {fetcher.data.ok ? <CheckCircle2Icon /> : <AlertTriangleIcon />}
                <AlertTitle>
                  {fetcher.data.ok ? "Dagster run queued" : "Launch refused"}
                </AlertTitle>
                <AlertDescription>
                  {fetcher.data.ok && fetcher.data.launched ? (
                    <span>
                      {fetcher.data.launched.selection} with model{" "}
                      {fetcher.data.launched.model}.{" "}
                      {fetcher.data.launched.runUrl ? (
                        <a
                          href={fetcher.data.launched.runUrl}
                          rel="noreferrer"
                          target="_blank"
                        >
                          Open run {fetcher.data.launched.runId.slice(0, 8)}
                        </a>
                      ) : null}
                    </span>
                  ) : (
                    fetcher.data.error
                  )}
                </AlertDescription>
              </Alert>
            ) : null}

            {profiles.length === 0 ? (
              <Alert variant="destructive">
                <BotIcon />
                <AlertTitle>No LLM profile configured</AlertTitle>
                <AlertDescription>
                  Add a profile in{" "}
                  <Link to="/admin/settings/llms">LLM settings</Link> before
                  launching this action.
                </AlertDescription>
              </Alert>
            ) : null}

            <fetcher.Form method="post" action="/admin/esef">
              <input
                type="hidden"
                name="intent"
                value="launch-company-information"
              />
              <FieldGroup>
                <FieldSet>
                  <FieldLegend>Document scope</FieldLegend>
                  <FieldGroup className="grid gap-4 md:grid-cols-2">
                    <Field>
                      <FieldLabel htmlFor="esef-country">Countries</FieldLabel>
                      {selectedCountryIso2s.map((countryIso2) => (
                        <input
                          key={countryIso2}
                          type="hidden"
                          name="country_iso2s"
                          value={countryIso2}
                        />
                      ))}
                      <Combobox
                        items={countries}
                        multiple
                        value={selectedCountryIso2s}
                        onValueChange={setSelectedCountryIso2s}
                        itemToStringValue={countryLabel}
                        disabled={countries.length === 0}
                      >
                        <ComboboxChips>
                          <ComboboxValue>
                            {selectedCountryIso2s.map((countryIso2) => (
                              <ComboboxChip key={countryIso2}>
                                {countryLabel(countryIso2)}
                              </ComboboxChip>
                            ))}
                          </ComboboxValue>
                          <ComboboxChipsInput
                            id="esef-country"
                            placeholder="Search countries"
                          />
                        </ComboboxChips>
                        <ComboboxContent>
                          <ComboboxEmpty>No countries found.</ComboboxEmpty>
                          <ComboboxList>
                            {(countryIso2) => (
                              <ComboboxItem
                                key={countryIso2}
                                value={countryIso2}
                              >
                                {countryLabel(countryIso2)}
                              </ComboboxItem>
                            )}
                          </ComboboxList>
                        </ComboboxContent>
                      </Combobox>
                      <FieldDescription>
                        {countryError ||
                          "Select one or more countries found in ESEF filing documents. Leave empty for every country."}
                      </FieldDescription>
                    </Field>
                    <FieldGroup>
                      <Field orientation="horizontal">
                        <Checkbox
                          id="esef-limit-enabled"
                          name="limit_documents"
                          value="1"
                          checked={documentLimitEnabled}
                          onCheckedChange={setDocumentLimitEnabled}
                        />
                        <FieldContent>
                          <FieldLabel htmlFor="esef-limit-enabled">
                            Enable document limit
                          </FieldLabel>
                          <FieldDescription>
                            Apply a safety cap after document and company
                            filters.
                          </FieldDescription>
                        </FieldContent>
                      </Field>
                      {documentLimitEnabled ? (
                        <Field>
                          <FieldLabel htmlFor="esef-limit">
                            Maximum documents
                          </FieldLabel>
                          <Input
                            id="esef-limit"
                            name="max_documents"
                            type="number"
                            min={1}
                            max={100_000}
                            defaultValue={runtimeDefaults.maxDocuments}
                          />
                        </Field>
                      ) : null}
                    </FieldGroup>
                  </FieldGroup>
                  <FieldGroup className="grid gap-4 md:grid-cols-2">
                    <Field>
                      <FieldLabel htmlFor="esef-documents">
                        ESEF filing IDs (fxo_id)
                      </FieldLabel>
                      <Textarea
                        id="esef-documents"
                        name="source_document_ids"
                        placeholder="549300CSLHPO6Y1AZN37-2021-12-31-ESEF-SE-1"
                      />
                      <FieldDescription>
                        Exact filing-version IDs from ESEF filings. Leave blank
                        to use the company and country filters.
                      </FieldDescription>
                    </Field>
                    <Field>
                      <FieldLabel htmlFor="esef-companies">
                        Company IDs
                      </FieldLabel>
                      <Textarea
                        id="esef-companies"
                        name="company_ids"
                        placeholder="One ID per line"
                        disabled={selectedCountryIso2s.length !== 1}
                      />
                      <FieldDescription>
                        Company IDs are country-scoped, so this field is
                        available when exactly one country is selected.
                      </FieldDescription>
                    </Field>
                  </FieldGroup>
                </FieldSet>

                <Separator />

                <FieldSet>
                  <FieldLegend>Model and execution</FieldLegend>
                  <FieldGroup className="grid gap-4 md:grid-cols-2">
                    <ProfileSelect profiles={profiles} />
                    <Field>
                      <FieldLabel htmlFor="esef-refresh">
                        Existing results
                      </FieldLabel>
                      <Select
                        items={REFRESH_BEHAVIORS}
                        name="refresh_behavior"
                        defaultValue="reuse_existing"
                      >
                        <SelectTrigger id="esef-refresh" className="w-full">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectGroup>
                            {REFRESH_BEHAVIORS.map((behavior) => (
                              <SelectItem
                                key={behavior.value}
                                value={behavior.value}
                              >
                                {behavior.label}
                              </SelectItem>
                            ))}
                          </SelectGroup>
                        </SelectContent>
                      </Select>
                    </Field>
                  </FieldGroup>
                  <FieldGroup className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
                    <Field>
                      <FieldLabel htmlFor="esef-concurrency">
                        Concurrency
                      </FieldLabel>
                      <Input
                        id="esef-concurrency"
                        name="concurrency"
                        type="number"
                        min={1}
                        max={8}
                        defaultValue={runtimeDefaults.concurrency}
                      />
                    </Field>
                    <Field>
                      <FieldLabel htmlFor="esef-temperature">
                        Temperature
                      </FieldLabel>
                      <Input
                        id="esef-temperature"
                        name="temperature"
                        type="number"
                        min={0}
                        max={2}
                        step={0.1}
                        defaultValue={runtimeDefaults.temperature}
                      />
                    </Field>
                    <Field>
                      <FieldLabel htmlFor="esef-evidence">
                        Evidence characters
                      </FieldLabel>
                      <Input
                        id="esef-evidence"
                        name="max_evidence_chars"
                        type="number"
                        min={500}
                        max={250_000}
                        defaultValue={runtimeDefaults.maxEvidenceChars}
                      />
                    </Field>
                    <Field>
                      <FieldLabel htmlFor="esef-timeout">
                        Timeout seconds
                      </FieldLabel>
                      <Input
                        id="esef-timeout"
                        name="timeout_seconds"
                        type="number"
                        min={1}
                        max={600}
                        defaultValue={runtimeDefaults.timeoutSeconds}
                      />
                    </Field>
                  </FieldGroup>
                </FieldSet>

                <Button
                  type="submit"
                  size="lg"
                  disabled={
                    busy ||
                    profiles.length === 0 ||
                    status === null ||
                    !status.canLaunch
                  }
                >
                  <PlayIcon data-icon="inline-start" />
                  {busy ? "Checking Dagster…" : "Launch enhancement"}
                </Button>
              </FieldGroup>
            </fetcher.Form>

            <Separator />

            <EnrichmentBatchStatistics
              batch={latestBatch}
              headingId="esef-sheet-batch-statistics-heading"
            />

            {latestBatch ? <Separator /> : null}

            <section
              className="flex flex-col gap-3"
              aria-labelledby="esef-runs-heading"
            >
              <div>
                <h3 id="esef-runs-heading" className="font-medium">
                  Recent executions
                </h3>
                <p className="text-sm text-muted-foreground">
                  Read directly from Dagster; no Backoffice run history is
                  stored.
                </p>
              </div>
              {latestRun ? (
                <div className="rounded-lg border">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Run</TableHead>
                        <TableHead>Status</TableHead>
                        <TableHead>Selection</TableHead>
                        <TableHead>Model</TableHead>
                        <TableHead>Started</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {status?.recentEnrichmentRuns.map((run) => {
                        const details = enrichmentRunDetails(run);
                        return (
                          <TableRow key={run.runId}>
                            <TableCell>{runLink(run.runId, runUrls)}</TableCell>
                            <TableCell>
                              <Badge variant={runVariant(run.status)}>
                                {runLabel(run.status)}
                              </Badge>
                            </TableCell>
                            <TableCell>{details.selection}</TableCell>
                            <TableCell>{details.model}</TableCell>
                            <TableCell>
                              {formatRunTime(run.startTime)}
                            </TableCell>
                          </TableRow>
                        );
                      })}
                    </TableBody>
                  </Table>
                </div>
              ) : (
                <p className="text-sm text-muted-foreground">
                  No company-information executions recorded.
                </p>
              )}
            </section>
          </div>
        </div>
      </SheetContent>
    </Sheet>
  );
}

function Summary({ assets }: { assets: EsefInventoryAsset[] }) {
  const fresh = assets.filter(
    (asset) => assetState(asset).label === "Fresh",
  ).length;
  const stale = assets.filter(
    (asset) => assetState(asset).label === "Stale",
  ).length;
  const missing = assets.filter(
    (asset) => assetState(asset).label === "Missing",
  ).length;
  const active = assets.filter((asset) => activeRun(asset) !== null).length;
  return (
    <dl className="grid grid-cols-2 gap-5 border-y py-5 sm:grid-cols-4">
      {[
        ["Assets", assets.length],
        ["Fresh", fresh],
        ["Needs attention", stale + missing],
        ["Assets running", active],
      ].map(([label, value]) => (
        <div key={label} className="flex flex-col gap-1">
          <dt className="text-sm text-muted-foreground">{label}</dt>
          <dd className="text-2xl font-semibold tabular-nums">{value}</dd>
        </div>
      ))}
    </dl>
  );
}

export function EsefOperationsWorkspace({
  overview,
  error,
  profiles,
  countries,
  countryError,
  runtimeDefaults,
  runUrls,
}: EsefOperationsWorkspaceProps) {
  const revalidator = useRevalidator();
  useEffect(() => {
    const interval = window.setInterval(() => {
      if (document.visibilityState === "visible") revalidator.revalidate();
    }, 10_000);
    return () => window.clearInterval(interval);
  }, [revalidator]);

  const assets = sortedAssets(overview?.inventory.assets ?? []);
  return (
    <main className="flex flex-1 flex-col gap-8 p-6 lg:p-8">
      <div className="mx-auto flex w-full max-w-7xl flex-col gap-8">
        <header className="flex flex-col justify-between gap-4 lg:flex-row lg:items-end">
          <div className="flex flex-col gap-1.5">
            <p className="text-sm font-medium text-muted-foreground">
              Data operations
            </p>
            <h1 className="text-2xl font-semibold tracking-tight">
              ESEF filings
            </h1>
            <p className="max-w-2xl text-sm text-muted-foreground">
              Live Dagster status for every asset in the ESEF group. Freshness,
              materializations, and active executions are read from Dagster.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button
              nativeButton={false}
              render={<Link to="/admin/esef" />}
              variant="outline"
            >
              <RefreshCwIcon data-icon="inline-start" />
              Refresh
            </Button>
            <EnrichmentActionSheet
              status={overview?.enrichment ?? null}
              profiles={profiles}
              countries={countries}
              countryError={countryError}
              runtimeDefaults={runtimeDefaults}
              runUrls={runUrls}
            />
          </div>
        </header>

        {error ? (
          <Alert variant="destructive">
            <AlertTriangleIcon />
            <AlertTitle>Dagster status is unavailable</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        ) : null}

        {overview ? (
          <>
            <Summary assets={assets} />

            <EnrichmentBatchStatistics
              batch={overview.enrichment.latestBatch}
              headingId="esef-batch-statistics-heading"
            />

            {overview.inventory.activeRuns.length > 0 ? (
              <Alert>
                <ActivityIcon />
                <AlertTitle>
                  {overview.inventory.activeRuns.length} ESEF execution
                  {overview.inventory.activeRuns.length === 1
                    ? " is"
                    : "s are"}{" "}
                  active
                </AlertTitle>
                <AlertDescription>
                  Asset states refresh every ten seconds. Actions repeat the
                  live check server-side before Dagster accepts another run.
                </AlertDescription>
              </Alert>
            ) : null}

            <section
              className="flex flex-col gap-4"
              aria-labelledby="esef-assets-heading"
            >
              <div>
                <h2 id="esef-assets-heading" className="font-semibold">
                  Assets
                </h2>
                <p className="text-sm text-muted-foreground">
                  Problems and active work are shown first; healthy assets
                  follow.
                </p>
              </div>
              <div className="overflow-x-auto rounded-lg border">
                <Table className="min-w-[72rem]">
                  <TableHeader>
                    <TableRow>
                      <TableHead>Asset</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Kind</TableHead>
                      <TableHead>Latest materialization</TableHead>
                      <TableHead>Run</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {assets.map((asset) => {
                      const state = assetState(asset);
                      const run = activeRun(asset);
                      const runId =
                        run?.runId ?? asset.materialization?.runId ?? null;
                      return (
                        <TableRow key={asset.asset}>
                          <TableCell className="max-w-xl whitespace-normal">
                            <div className="flex items-start gap-2">
                              <DatabaseIcon className="mt-0.5 size-4 shrink-0 text-muted-foreground" />
                              <div className="flex min-w-0 flex-col gap-1">
                                <span className="font-mono text-xs font-medium">
                                  {asset.asset}
                                </span>
                                {asset.description ? (
                                  <span className="line-clamp-2 text-xs text-muted-foreground">
                                    {asset.description}
                                  </span>
                                ) : null}
                              </div>
                            </div>
                          </TableCell>
                          <TableCell>
                            <Badge variant={state.variant}>{state.label}</Badge>
                          </TableCell>
                          <TableCell className="text-xs text-muted-foreground">
                            {[...asset.kinds].sort().slice(0, 4).join(", ")}
                            {asset.partitioned ? " · partitioned" : ""}
                          </TableCell>
                          <TableCell>
                            {formatMaterializationTime(
                              asset.materialization?.timestamp ?? null,
                            )}
                          </TableCell>
                          <TableCell>
                            {runId ? runLink(runId, runUrls) : "—"}
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </div>
            </section>
          </>
        ) : null}
      </div>
    </main>
  );
}
