import { useEffect, useRef, useState } from "react";
import { data, Form, Link, useFetcher, useNavigation } from "react-router";
import type { Route } from "./+types/admin-ip-addresses";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Checkbox } from "~/components/ui/checkbox";
import {
  addIpsToEnrichmentQueue,
  IpEnrichmentSelectionError,
} from "~/lib/ip-enrichment.server";
import {
  QueueImportStatus,
  useQueueSubmission,
} from "~/components/admin/queue-import-status";
import {
  isIpSelected,
  selectIpAddresses,
  type WorkspaceIpSelection,
} from "~/lib/workspace-ip-selection";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
} from "~/components/ui/empty";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import {
  NativeSelect,
  NativeSelectOption,
} from "~/components/ui/native-select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import {
  parseWorkspaceIpFilters,
  workspaceIpAddressesHref,
} from "~/lib/workspace-ip-addresses";
import {
  listWorkspaceIpAddresses,
  type WorkspaceIpStatistics,
} from "~/lib/workspace-ip-addresses.server";

function IpStatistics() {
  const { load, data, state } = useFetcher<{
    statistics: WorkspaceIpStatistics | null;
  }>();
  useEffect(() => {
    void load("/admin/ip-addresses/statistics");
  }, [load]);
  const statistics = data?.statistics;
  return (
    <section
      aria-label="IP inventory statistics"
      aria-live="polite"
      className="flex flex-col gap-2"
    >
      {statistics ? (
        <>
          <dl className="flex flex-wrap gap-x-12 gap-y-4">
            {[
              ["Total unique IP addresses", statistics.total],
              ["IPv4 addresses", statistics.ipv4],
              ["IPv6 addresses", statistics.ipv6],
            ].map(([label, value]) => (
              <div key={label} className="flex flex-col gap-1">
                <dt className="text-muted-foreground text-sm">{label}</dt>
                <dd className="text-2xl font-semibold tabular-nums">
                  {Number(value).toLocaleString("en-US")}
                </dd>
              </div>
            ))}
          </dl>
          <p className="text-muted-foreground text-xs">
            Entire DNS inventory, independent of filters · Counted{" "}
            {statistics.countedAt.replace("T", " ").slice(0, 19)} UTC · Cached
            for up to 5 minutes.
          </p>
        </>
      ) : data && state === "idle" ? (
        <div className="flex items-center gap-3">
          <p className="text-muted-foreground text-sm">
            Inventory counts are temporarily unavailable.
          </p>
          <Button
            variant="outline"
            size="sm"
            onClick={() => void load("/admin/ip-addresses/statistics")}
          >
            Retry counts
          </Button>
        </div>
      ) : (
        <p className="text-muted-foreground text-sm" role="status">
          Counting unique IP addresses… The table is ready to browse.
        </p>
      )}
    </section>
  );
}

export async function loader({ request }: Route.LoaderArgs) {
  const params = new URL(request.url).searchParams;
  const filters = parseWorkspaceIpFilters(params);
  return {
    ...(await listWorkspaceIpAddresses(filters, params.get("after") ?? "")),
    filters,
  };
}

export async function action({ request }: Route.ActionArgs) {
  let body;
  try {
    body = await request.json();
  } catch {
    return data(
      { ok: false as const, error: "Invalid enrichment request." },
      { status: 400 },
    );
  }
  if (body?.action !== "enrich") {
    return data(
      { ok: false as const, error: "Choose a supported IP address action." },
      { status: 400 },
    );
  }
  try {
    return data(
      await addIpsToEnrichmentQueue(
        body.selection,
        String(body.submissionId ?? ""),
        process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice",
      ),
    );
  } catch (error) {
    if (error instanceof IpEnrichmentSelectionError) {
      return data(
        { ok: false as const, error: error.message },
        { status: 400 },
      );
    }
    return data(
      {
        ok: false as const,
        error:
          "Could not submit the queue import. Check Dagster for a submitted run before retrying.",
      },
      { status: 502 },
    );
  }
}

export function meta() {
  return [{ title: "IP addresses | CompanyCollect" }];
}

export default function WorkspaceIpAddresses({
  loaderData,
}: Route.ComponentProps) {
  const { rows, filters, after, next, hasMore } = loaderData;
  type SelectionState = {
    filterKey: string;
    selection: WorkspaceIpSelection;
  };
  const navigationBusy = useNavigation().state !== "idle";
  const fetcher = useFetcher<typeof action>();
  const receipt = fetcher.data?.ok ? fetcher.data : null;
  const { state: importState, error: importError } = useQueueSubmission(
    receipt,
    "ip-enrichment",
  );
  const submitting =
    fetcher.state !== "idle" || Boolean(receipt && !importState?.finished);
  const busy = navigationBusy || submitting;
  const filterKey = JSON.stringify(filters);
  const [selectionState, setSelectionState] = useState<SelectionState>({
    filterKey,
    selection: { mode: "ips", ips: [] },
  });
  const currentState =
    selectionState.filterKey === filterKey
      ? selectionState
      : { filterKey, selection: { mode: "ips" as const, ips: [] } };
  if (currentState !== selectionState) setSelectionState(currentState);
  const selection = currentState.selection;
  const setSelection = (selection: WorkspaceIpSelection) =>
    setSelectionState({ filterKey, selection });
  const request = useRef<{ id: string; state: SelectionState } | null>(null);
  const [retry, setRetry] = useState(false);
  useEffect(() => {
    if (importState?.status === "SUCCESS") {
      // Only a saved import clears the selection; an accepted launch alone does not.
      setSelectionState((current) =>
        current === request.current?.state
          ? { ...current, selection: { mode: "ips", ips: [] } }
          : current,
      );
      setRetry(false);
    } else if (
      importState?.status === "FAILURE" ||
      importState?.status === "CANCELED" ||
      fetcher.data?.ok === false
    ) {
      setRetry(true);
    }
  }, [importState?.status, fetcher.data]);
  const pageSelected = rows.filter((row) =>
    isIpSelected(selection, row.ip),
  ).length;
  const hasSelection = selection.mode === "all" || selection.ips.length > 0;
  const submitEnrichment = (again = false) => {
    if (busy || (!again && !hasSelection)) return;
    const id = again && request.current ? request.current.id : crypto.randomUUID();
    const state = again && request.current ? request.current.state : currentState;
    request.current = { id, state };
    setRetry(false);
    void fetcher.submit(
      JSON.stringify({
        action: "enrich",
        selection: state.selection,
        submissionId: id,
      }),
      {
        method: "post",
        encType: "application/json",
        action: "/admin/ip-addresses",
      },
    );
  };
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6" aria-busy={busy}>
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold">IP addresses</h1>
        <p className="text-muted-foreground max-w-4xl text-sm">
          All IPv4 and IPv6 addresses observed by our DNS and zone-transfer
          scanners. Each address appears once, including historical records and
          addresses without enrichment.
        </p>
      </header>
      <IpStatistics />
      <Form method="get" key={JSON.stringify(filters)}>
        <FieldGroup className="sm:flex-row sm:items-end">
          <Field className="sm:max-w-xs">
            <FieldLabel htmlFor="ip-search">IP address or prefix</FieldLabel>
            <Input
              id="ip-search"
              name="search"
              placeholder="116.203. or 2001:db8:"
              defaultValue={filters.search}
              maxLength={128}
            />
          </Field>
          <Field className="sm:max-w-48">
            <FieldLabel htmlFor="ip-version">Address type</FieldLabel>
            <NativeSelect
              id="ip-version"
              name="version"
              defaultValue={filters.version}
            >
              <NativeSelectOption value="any">All addresses</NativeSelectOption>
              <NativeSelectOption value="4">IPv4 · A</NativeSelectOption>
              <NativeSelectOption value="6">IPv6 · AAAA</NativeSelectOption>
            </NativeSelect>
          </Field>
          <Button type="submit" disabled={busy}>
            {busy ? "Loading…" : "Apply filters"}
          </Button>
          <Button
            variant="ghost"
            nativeButton={false}
            render={<Link to="/admin/ip-addresses" />}
          >
            Reset
          </Button>
        </FieldGroup>
      </Form>
      {receipt ? (
        <QueueImportStatus
          receipt={receipt}
          state={importState}
          queue="ip-enrichment"
        />
      ) : null}
      {fetcher.data?.ok === false || importError ? (
        <Alert variant="destructive">
          <AlertDescription>
            {fetcher.data?.ok === false ? fetcher.data.error : importError}
          </AlertDescription>
        </Alert>
      ) : null}
      {retry && request.current ? (
        <Button
          variant="outline"
          size="sm"
          disabled={busy}
          onClick={() => submitEnrichment(true)}
        >
          Retry queue import
        </Button>
      ) : null}
      <div className="flex flex-col gap-2">
        <div className="flex flex-wrap items-center gap-3">
          <p className="text-muted-foreground text-sm" role="status">
            {selection.mode === "all"
              ? `All matching addresses selected across every page${selection.excludedIps.length ? ` · ${selection.excludedIps.length} excluded` : ""}`
              : `${selection.ips.length.toLocaleString("en-US")} ${selection.ips.length === 1 ? "address" : "addresses"} selected`}
          </p>
          {selection.mode !== "all" && rows.length > 0 ? (
            <Button
              size="sm"
              variant="outline"
              disabled={busy}
              onClick={() =>
                setSelection({ mode: "all", filters, excludedIps: [] })
              }
            >
              Select all matching addresses
            </Button>
          ) : null}
          {hasSelection ? (
            <Button
              size="sm"
              variant="ghost"
              disabled={busy}
              onClick={() => setSelection({ mode: "ips", ips: [] })}
            >
              Clear selection
            </Button>
          ) : null}
          <Button
            size="sm"
            disabled={busy || !hasSelection}
            onClick={() => submitEnrichment()}
          >
            {submitting ? "Submitting…" : "Add to enrichment queue"}
          </Button>
        </div>
        <p className="text-muted-foreground text-xs">
          Adds the selection to the open IP enrichment draft. Start processing
          from{" "}
          <Link to="/admin/queues/ip-enrichment">Queues → IP enrichment</Link>
          ; GeoIP, ASN and RDAP are saved per address and fresh RDAP coverage
          is reused.
        </p>
      </div>
      <div className="rounded-lg border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-10">
                <Checkbox
                  aria-label="Select all addresses on this page"
                  checked={rows.length > 0 && pageSelected === rows.length}
                  indeterminate={pageSelected > 0 && pageSelected < rows.length}
                  disabled={busy || !rows.length}
                  onCheckedChange={(checked) =>
                    setSelection(
                      selectIpAddresses(
                        selection,
                        rows.map((row) => row.ip),
                        checked,
                      ),
                    )
                  }
                />
              </TableHead>
              <TableHead>IP address</TableHead>
              <TableHead>DNS type</TableHead>
              <TableHead>Location</TableHead>
              <TableHead>ASN / organization</TableHead>
              <TableHead>RDAP network</TableHead>
              <TableHead>First seen (UTC)</TableHead>
              <TableHead>Last seen (UTC)</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => (
              <TableRow
                key={row.ip}
                data-state={
                  isIpSelected(selection, row.ip) ? "selected" : undefined
                }
              >
                <TableCell>
                  <Checkbox
                    aria-label={`Select ${row.ip}`}
                    checked={isIpSelected(selection, row.ip)}
                    disabled={busy}
                    onCheckedChange={(checked) =>
                      setSelection(
                        selectIpAddresses(selection, [row.ip], checked),
                      )
                    }
                  />
                </TableCell>
                <TableCell>
                  <span className="font-mono">{row.ip}</span>
                </TableCell>
                <TableCell>
                  <Badge variant="secondary">
                    {row.ip_version === 4 ? "A · IPv4" : "AAAA · IPv6"}
                  </Badge>
                </TableCell>
                <TableCell>
                  {[row.city_name, row.country_iso_code]
                    .filter(Boolean)
                    .join(", ") || "—"}
                </TableCell>
                <TableCell>
                  <div className="flex flex-col gap-1">
                    <span>{row.asn === null ? "—" : `AS${row.asn}`}</span>
                    {row.asn_organization ? (
                      <span
                        className="text-muted-foreground max-w-60 truncate"
                        title={row.asn_organization}
                      >
                        {row.asn_organization}
                      </span>
                    ) : null}
                  </div>
                </TableCell>
                <TableCell>
                  <div className="flex flex-col gap-1">
                    <span className="font-mono">
                      {row.rdap_matched_cidr ?? "—"}
                    </span>
                    {row.rdap_name ? (
                      <span
                        className="text-muted-foreground max-w-52 truncate"
                        title={row.rdap_name}
                      >
                        {row.rdap_name}
                      </span>
                    ) : null}
                  </div>
                </TableCell>
                <TableCell>
                  <time dateTime={`${row.first_seen.replace(" ", "T")}Z`}>
                    {row.first_seen.slice(0, 19)}
                  </time>
                </TableCell>
                <TableCell>
                  <time dateTime={`${row.last_seen.replace(" ", "T")}Z`}>
                    {row.last_seen.slice(0, 19)}
                  </time>
                </TableCell>
              </TableRow>
            ))}
            {!rows.length ? (
              <TableRow>
                <TableCell colSpan={8}>
                  <Empty>
                    <EmptyHeader>
                      <EmptyTitle>No matching IP addresses</EmptyTitle>
                      <EmptyDescription>
                        Try a different address or prefix, or reset the filters.
                      </EmptyDescription>
                    </EmptyHeader>
                  </Empty>
                </TableCell>
              </TableRow>
            ) : null}
          </TableBody>
        </Table>
      </div>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="text-muted-foreground text-sm">
          {rows.length} {rows.length === 1 ? "address" : "addresses"} shown ·
          Dates reflect recorded DNS observations.
        </p>
        <div className="flex gap-2">
          {after ? (
            <Button
              variant="outline"
              nativeButton={false}
              render={<Link to={workspaceIpAddressesHref(filters)} />}
            >
              First page
            </Button>
          ) : null}
          {hasMore ? (
            <Button
              variant="outline"
              nativeButton={false}
              render={<Link to={workspaceIpAddressesHref(filters, next)} />}
            >
              Next page
            </Button>
          ) : null}
        </div>
      </div>
      <p className="text-muted-foreground text-xs">
        Address inventory: <code>corpscout.commoncrawl_ip_addresses</code>.{" "}
        Location, ASN and RDAP come from saved IP enrichment results. A dash
        means no data is available.
      </p>
    </div>
  );
}

export function ErrorBoundary() {
  return (
    <div className="p-4 md:p-6">
      <Alert variant="destructive">
        <AlertTitle>Unable to load IP addresses</AlertTitle>
        <AlertDescription>
          Try again, or <Link to="/admin/ip-addresses">reset the filters</Link>.
        </AlertDescription>
      </Alert>
    </div>
  );
}
