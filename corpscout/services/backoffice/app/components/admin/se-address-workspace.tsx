import { CheckCircle2Icon, MapPinIcon, TriangleAlertIcon } from "lucide-react";
import { useEffect, useState } from "react";
import { Form, Link, useNavigate, useNavigation } from "react-router";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "~/components/ui/accordion";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
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
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "~/components/ui/dialog";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { Input } from "~/components/ui/input";
import { DefinitionList, EMPTY_VALUE, text } from "~/components/admin/definition-list";
import { FoldRunPoller } from "~/components/admin/fold-run-poller";
import {
  EMPTY_ADDRESS_INITIAL,
  SeAddressEditSheet,
  type SeAddressEditInitial,
  type SeAddressEditMode,
} from "~/components/admin/se-address-edit-sheet";
import { AddressMap, type AddressMapPoint } from "~/components/detail/address-map";
import {
  addressKindLabel,
  addressSearchString,
  addressSourceLabel,
  geocodeStatusLabel,
  MAX_NOTE_LENGTH,
  MAX_WORKPLACE_QUERY_LENGTH,
} from "~/lib/se-address-fields";
import type {
  SeAddressComponents,
  SeAddressDetail,
  SeAddressDraft,
  SeAddressListEntry,
  SeAddressMember,
  SeAddressPublishedDetail,
  SeAddressRawRow,
  SeAddressRow,
  SeAddressWorkplacePage,
} from "~/lib/se-company-address-entity.server";
import { cn } from "~/lib/utils";

/**
 * The Address tab (spec 2026-09-06 section 8), built like the Info tab: the
 * company's published addresses and their history on the left, the selected
 * address with its contributing sources, its geocode and its rule on the
 * right, and every write a plain `<Form method="post">` behind a confirmation
 * dialog -- nothing posts until the reviewer confirms.
 *
 * The component never publishes anything itself: Remove, Reset to default,
 * Activate and Discard all write a suggestion or a rule version, and the next
 * fold (Fold now, or the scheduled one) is what republishes the addresses.
 */

/** What the route's action returns. `runId`/`url` come back from Fold now,
 * `slot` from a saved draft; a refusal carries the reviewer-readable reason and
 * the intent it refused, so the sheet and the page alert can each show only
 * their own (an intent-less refusal is shown by both rather than by neither). */
export type SeAddressResult =
  | { ok: true; intent: string; runId?: string; url?: string | null; slot?: string }
  | { ok: false; intent?: string; error: string }
  | null;

/** The street line a reviewer would type for a parsed address: the box when
 * there is one, else the street with its number and unit. */
export function addressStreetLine(row: SeAddressComponents): string {
  if (row.box !== "") return `Box ${row.box}`;
  return [row.street_name, row.house_number, row.unit].filter((part) => part !== "").join(" ");
}

/** The parsed components on one line, for the panel's source entries. */
function componentsLine(row: SeAddressComponents): string {
  return [
    row.care_of,
    addressStreetLine(row),
    [row.postal_code, row.city].filter((part) => part !== "").join(" "),
    row.country_code,
  ]
    .filter((part) => part !== "")
    .join(" · ");
}

/** What a source (or the reviewer) delivered, before normalization. */
function rawLine(raw: SeAddressRawRow): string {
  const line = [
    raw.care_of,
    raw.street_address,
    [raw.postal_code, raw.post_town].filter((part) => part !== "").join(" "),
  ]
    .filter((part) => part !== "")
    .join(", ");
  return line === "" ? raw.raw_address : line;
}

/** A 64-hex key or reference is unreadable in full and never needed in full:
 * the first characters identify it, the title attribute carries the rest. */
function shortHash(value: string): string {
  return value.length > 12 ? `${value.slice(0, 12)}…` : value;
}

/** One badge per distinct member source -- `sources` is index-parallel with
 * `slots`, so the same source appears once per member it contributed. */
function distinctSources(sources: readonly string[]): string[] {
  return [...new Set(sources)];
}

function activityLabel(row: SeAddressRow): string {
  if (row.active === 1) return "active";
  return row.inactive_reason === "" ? "inactive" : row.inactive_reason;
}

/** A geocode nobody should read as a doorstep: the street line or the area
 * centroid, or a position the postcode or the city alone produced. */
const APPROXIMATE_STATUSES = new Set(["matched_area", "matched_street"]);
const APPROXIMATE_PRECISIONS = new Set(["postcode", "city"]);

/** Where one published address goes on the map, or null when there is nothing
 * to put there: no coordinates, a row no geocode run has touched (`''`), or a
 * foreign address the Swedish reference set never places. Exported for the
 * test -- the map itself never renders server-side. */
export function mapPoint(row: SeAddressRow): AddressMapPoint | null {
  if (row.latitude === null || row.longitude === null) return null;
  if (row.geocode_status === "" || row.geocode_status === "foreign") return null;
  return {
    key: row.address_key,
    lat: row.latitude,
    lon: row.longitude,
    label: [row.normalized_address, geocodeStatusLabel(row.geocode_status)]
      .filter((part) => part !== "")
      .join(" · "),
    approximate:
      APPROXIMATE_PRECISIONS.has(row.geocode_precision) ||
      APPROXIMATE_STATUSES.has(row.geocode_status),
  };
}

/** Every point the list's one map carries: the company's ACTIVE addresses and
 * the workplace page's rows (spec 8, amended -- "the map shows the company
 * addresses and the current workplace page"). A withdrawn or hidden row is not
 * where this company is, and a row with no usable geocode has nothing to put on
 * a map, so the map may hold fewer points than the cards hold rows. Exported
 * for the test: the map itself never renders server-side. */
export function listMapPoints(
  published: readonly SeAddressListEntry[],
  workplaces: readonly SeAddressListEntry[],
): AddressMapPoint[] {
  return [...published.filter((entry) => entry.row.active === 1), ...workplaces]
    .map((entry) => mapPoint(entry.row))
    .filter((point): point is AddressMapPoint => point !== null);
}

/** OpenStreetMap's marker permalink: close in on an exact position, further
 * out on an approximate one, where the reviewer can judge the surroundings. */
function openStreetMapUrl(point: AddressMapPoint): string {
  const zoom = point.approximate ? 13 : 18;
  return `https://www.openstreetmap.org/?mlat=${point.lat}&mlon=${point.lon}#map=${zoom}/${point.lat}/${point.lon}`;
}

function initialFromRow(row: SeAddressRow): SeAddressEditInitial {
  return {
    careOf: row.care_of,
    streetLine: addressStreetLine(row),
    postalCode: row.postal_code,
    city: row.city,
    kind: row.kinds[0] ?? "postal",
    note: "",
  };
}

function initialFromDraft(draft: SeAddressDraft): SeAddressEditInitial {
  const { raw } = draft;
  return {
    careOf: raw.care_of,
    streetLine: raw.street_address,
    postalCode: raw.postal_code,
    city: raw.post_town,
    kind: raw.kind,
    note: raw.note,
  };
}

/** What a Remove / Reset to default / Activate / Discard click proposes, shown
 * for confirmation before it posts. */
export interface PendingAddressDecision {
  intent: "remove" | "reset" | "activate" | "discard";
  /** The published key `remove` and `reset` act on; `''` for a draft action. */
  addressKey: string;
  /** The draft slot `activate` and `discard` act on; `''` otherwise. */
  slot: string;
  /** The address line being acted on, shown in the dialog. */
  line: string;
  /** Remove only (Ruling 1, amended): a row every member of which is the
   * reviewer's own is withdrawn; a row with any source member is hidden whole. */
  reviewerOnly: boolean;
}

const DECISION_HEADINGS: Record<PendingAddressDecision["intent"], string> = {
  remove: "Remove this address",
  reset: "Reset this address to default",
  activate: "Activate this draft",
  discard: "Discard this draft",
};

const REMOVE_DESCRIPTIONS = {
  whole:
    "This hides the whole address, all sources included. Use Correct to replace the text under a new key.",
  reviewer: "This withdraws the reviewer's address.",
} as const;

/** What Remove will do to one address, in the reviewer's words (Ruling 1,
 * amended): there is no way to hide one source's contribution to a merged
 * address, so Remove takes the whole address out; only a row the reviewer alone
 * contributed is withdrawn instead. Exported because the dialog it appears in
 * cannot be rendered on its own -- `DialogTitle` needs the Dialog root, and an
 * open dialog renders nothing at all server-side. */
export function removeDescription(reviewerOnly: boolean): string {
  return REMOVE_DESCRIPTIONS[reviewerOnly ? "reviewer" : "whole"];
}

const DECISION_DESCRIPTIONS: Record<
  Exclude<PendingAddressDecision["intent"], "remove">,
  string
> = {
  reset:
    "The hide rule is released and the next fold publishes this address again, as its sources deliver it.",
  activate:
    "The draft becomes this company's reviewer address; a correction also hides the address it replaces. Fold now publishes it.",
  discard: "The draft is cleared. Nothing published changes.",
};

const DECISION_CONFIRM: Record<PendingAddressDecision["intent"], string> = {
  remove: "Remove",
  reset: "Reset to default",
  activate: "Activate",
  discard: "Discard",
};

/**
 * The confirmation's form: what is about to be written, against which address
 * or draft, plus the optional note. `DialogTitle` labels the dialog, so this
 * body belongs inside a `Dialog` root -- `AddressDecisionDialog` is what
 * renders it.
 */
export function AddressDecisionDialogBody({
  pending,
  busy,
  onClose,
}: {
  pending: PendingAddressDecision;
  busy: boolean;
  onClose: () => void;
}) {
  const { intent } = pending;
  // Discard is the one decision that carries no note at all (the parser reads
  // `{ intent: "discard"; slot }` and nothing else).
  const showNote = intent !== "discard";
  const description =
    intent === "remove"
      ? removeDescription(pending.reviewerOnly)
      : DECISION_DESCRIPTIONS[intent];
  return (
    <Form method="post" onSubmit={onClose} className="flex flex-col gap-4">
      {/* DialogTitle, so the dialog is labelled for a screen reader -- which
          means this body only renders inside a Dialog root, never on its own. */}
      <DialogHeader>
        <DialogTitle>{DECISION_HEADINGS[intent]}</DialogTitle>
        <p className="text-muted-foreground text-sm">{description}</p>
      </DialogHeader>
      <div className="rounded-md border p-3 text-sm" data-testid="address-decision-value">
        {pending.line === "" ? EMPTY_VALUE : pending.line}
      </div>
      <input type="hidden" name="intent" value={intent} />
      {pending.addressKey === "" ? null : (
        <input type="hidden" name="address_key" value={pending.addressKey} />
      )}
      {pending.slot === "" ? null : <input type="hidden" name="slot" value={pending.slot} />}
      {showNote ? (
        <label className="flex flex-col gap-1 text-sm" htmlFor="address-note">
          <span className="text-muted-foreground text-xs">Why (optional)</span>
          <Input
            id="address-note"
            name="note"
            maxLength={MAX_NOTE_LENGTH}
            placeholder="Note saved with the decision"
          />
        </label>
      ) : null}
      <DialogFooter>
        <Button type="button" variant="outline" onClick={onClose} disabled={busy}>
          Cancel
        </Button>
        <Button type="submit" disabled={busy}>
          {DECISION_CONFIRM[intent]}
        </Button>
      </DialogFooter>
    </Form>
  );
}

function AddressDecisionDialog({
  pending,
  busy,
  onClose,
}: {
  pending: PendingAddressDecision | null;
  busy: boolean;
  onClose: () => void;
}) {
  return (
    <Dialog open={pending !== null} onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogContent>
        {pending ? (
          <AddressDecisionDialogBody pending={pending} busy={busy} onClose={onClose} />
        ) : null}
      </DialogContent>
    </Dialog>
  );
}

/** One published address: the line links to its panel, Correct sits outside
 * the link (an anchor may not wrap a button). The same row renders in the
 * Addresses card and in the Workplaces card. */
function AddressLine({
  entry,
  selectedKey,
  addressHref,
  busy,
  onCorrect,
}: {
  entry: SeAddressListEntry;
  selectedKey: string | null;
  /** The search string that selects this row, with the workplace page and its
   * filter kept. */
  addressHref: (addressKey: string) => string;
  busy: boolean;
  onCorrect: (entry: SeAddressListEntry) => void;
}) {
  const { row } = entry;
  const selected = row.address_key === selectedKey;
  return (
    <li className="flex items-center gap-1" data-address-key={row.address_key}>
      <Link
        to={{ search: addressHref(row.address_key) }}
        preventScrollReset
        aria-current={selected ? "true" : undefined}
        className={cn(
          "grid flex-1 grid-cols-1 gap-x-6 rounded-md px-2 py-2 hover:bg-muted/60 sm:grid-cols-[1fr_auto]",
          selected && "bg-muted",
        )}
      >
        <span>{row.normalized_address === "" ? EMPTY_VALUE : row.normalized_address}</span>
        <span className="flex flex-wrap gap-1 sm:justify-end">
          {row.kinds.map((kind) => (
            <Badge key={kind} variant="outline">
              {addressKindLabel(kind)}
            </Badge>
          ))}
          {distinctSources(row.sources).map((source) => (
            <Badge key={source} variant="secondary">
              {addressSourceLabel(source)}
            </Badge>
          ))}
          <Badge variant="outline">
            {geocodeStatusLabel(row.geocode_status)}
            {row.geocode_precision === "" ? "" : ` · ${row.geocode_precision}`}
          </Badge>
          {entry.refoldPending ? <Badge variant="outline">re-fold pending</Badge> : null}
          {row.inactive_reason === "hidden" ? <Badge variant="destructive">hidden</Badge> : null}
        </span>
      </Link>
      <Button
        type="button"
        size="sm"
        variant="ghost"
        disabled={busy}
        onClick={() => onCorrect(entry)}
      >
        Correct
      </Button>
    </li>
  );
}

function AddressesCard({
  detail,
  selectedKey,
  mapPoints,
  addressHref,
  busy,
  onCorrect,
}: {
  detail: SeAddressDetail;
  selectedKey: string | null;
  /** The company's addresses AND the current workplace page. */
  mapPoints: readonly AddressMapPoint[];
  addressHref: (addressKey: string) => string;
  busy: boolean;
  onCorrect: (entry: SeAddressListEntry) => void;
}) {
  const navigate = useNavigate();
  const active = detail.published.filter((entry) => entry.row.active === 1);
  const inactive = detail.published.filter((entry) => entry.row.active !== 1);
  // A withdrawn or hidden address the panel is describing must be visible in
  // the list too: linking to one opens the group it hides in.
  const selectedIsInactive = inactive.some((entry) => entry.row.address_key === selectedKey);
  return (
    <Card>
      <CardHeader>
        <CardTitle>Addresses</CardTitle>
        <CardDescription>
          What the last fold published, active first. Click a row to see the sources
          behind it; Correct types a replacement.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {detail.published.length === 0 ? (
          <Empty className="border">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <MapPinIcon />
              </EmptyMedia>
              <EmptyTitle>No addresses published yet</EmptyTitle>
              <EmptyDescription>
                No fold has published an address for this company. Add address types one;
                Fold now publishes it.
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : null}
        {/* Nothing active and nothing on the map -- not even the empty frame,
            which would only repeat what the empty state above already says. */}
        {active.length === 0 && mapPoints.length === 0 ? null : (
          <div className="mb-3">
            <AddressMap
              points={mapPoints}
              selectedKey={selectedKey}
              onSelect={(key) => navigate({ search: addressHref(key) }, { preventScrollReset: true })}
            />
          </div>
        )}
        {/* A list of links, not a <dl>: an anchor may not wrap dt/dd pairs. */}
        <ul className="grid grid-cols-1 gap-y-1 text-sm">
          {active.map((entry) => (
            <AddressLine
              key={entry.row.address_key}
              entry={entry}
              selectedKey={selectedKey}
              addressHref={addressHref}
              busy={busy}
              onCorrect={onCorrect}
            />
          ))}
        </ul>
        {inactive.length === 0 ? null : (
          <Accordion className="mt-2" defaultValue={selectedIsInactive ? ["inactive"] : []}>
            <AccordionItem value="inactive" className="border-0">
              <AccordionTrigger>Withdrawn and hidden ({inactive.length})</AccordionTrigger>
              <AccordionContent>
                <ul className="grid grid-cols-1 gap-y-1 text-sm">
                  {inactive.map((entry) => (
                    <AddressLine
                      key={entry.row.address_key}
                      entry={entry}
                      selectedKey={selectedKey}
                      addressHref={addressHref}
                      busy={busy}
                      onCorrect={onCorrect}
                    />
                  ))}
                </ul>
              </AccordionContent>
            </AccordionItem>
          </Accordion>
        )}
      </CardContent>
    </Card>
  );
}

/**
 * The company's workplace addresses (spec 8, amended 2026-09-13, rule 2): rows
 * whose kinds are exactly `['workplace']`. Ratsit's establishments put hundreds
 * of them on some companies -- a kommun has 1,502 -- so they are counted, cut
 * and filtered in ClickHouse, fifty a page, and every link here keeps both the
 * page and the filter. The card is absent when the company has none and no
 * filter is in force; with a filter and no hit it stays, and says so.
 */
function WorkplacesCard({
  workplaces,
  selectedKey,
  addressHref,
  busy,
  onCorrect,
}: {
  workplaces: SeAddressWorkplacePage;
  selectedKey: string | null;
  addressHref: (addressKey: string) => string;
  busy: boolean;
  onCorrect: (entry: SeAddressListEntry) => void;
}) {
  const { rows, total, page, pageSize, query } = workplaces;
  if (total === 0 && query === "") return null;
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  const from = rows.length === 0 ? 0 : (page - 1) * pageSize + 1;
  const to = (page - 1) * pageSize + rows.length;
  const pageSearch = (nextPage: number) =>
    addressSearchString({ address: selectedKey, workplacePage: nextPage, workplaceQuery: query });
  return (
    <Card>
      <CardHeader>
        <CardTitle>{`Workplaces (${total})`}</CardTitle>
        <CardDescription>
          Establishments this company runs, from Ratsit. They are the entity's addresses but
          not the address it publishes as itself, so they are listed apart -- and the serving
          view leaves them out.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {/* A GET form REPLACES the search, which is exactly how a new filter
            resets to page 1; the selected address rides along in a hidden field
            so filtering does not empty the panel. */}
        <Form method="get" className="mb-3 flex flex-wrap items-center gap-2">
          {selectedKey === null ? null : <input type="hidden" name="address" value={selectedKey} />}
          <Input
            name="workplace_q"
            defaultValue={query}
            maxLength={MAX_WORKPLACE_QUERY_LENGTH}
            placeholder="Filter by address"
            aria-label="Filter workplaces"
            className="max-w-xs"
          />
          <Button type="submit" size="sm" variant="outline" disabled={busy}>
            Filter
          </Button>
          {query === "" ? null : (
            <Link
              className="text-muted-foreground text-xs underline"
              to={{
                search: addressSearchString({
                  address: selectedKey,
                  workplacePage: 1,
                  workplaceQuery: "",
                }),
              }}
              preventScrollReset
            >
              Clear
            </Link>
          )}
        </Form>
        {rows.length === 0 ? (
          <p className="text-muted-foreground text-sm">No workplace matches.</p>
        ) : (
          <ul className="grid grid-cols-1 gap-y-1 text-sm">
            {rows.map((entry) => (
              <AddressLine
                key={entry.row.address_key}
                entry={entry}
                selectedKey={selectedKey}
                addressHref={addressHref}
                busy={busy}
                onCorrect={onCorrect}
              />
            ))}
          </ul>
        )}
        <div className="text-muted-foreground mt-3 flex items-center justify-between text-xs">
          <span>{`${from}–${to} of ${total}`}</span>
          <span className="flex gap-3">
            {page > 1 ? (
              <Link className="underline" to={{ search: pageSearch(page - 1) }} preventScrollReset>
                Previous
              </Link>
            ) : null}
            {page < lastPage ? (
              <Link className="underline" to={{ search: pageSearch(page + 1) }} preventScrollReset>
                Next
              </Link>
            ) : null}
          </span>
        </div>
      </CardContent>
    </Card>
  );
}

function DraftsCard({
  detail,
  busy,
  onEdit,
  onDecide,
}: {
  detail: SeAddressDetail;
  busy: boolean;
  onEdit: (draft: SeAddressDraft) => void;
  onDecide: (pending: PendingAddressDecision) => void;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Drafts</CardTitle>
        <CardDescription>
          Typed here, published by nobody yet. Activate writes the reviewer address;
          Discard drops the draft.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ul className="flex flex-col gap-2">
          {detail.drafts.map((draft) => {
            const line = rawLine(draft.raw);
            const replaced =
              draft.replacesKey === ""
                ? null
                : ([...detail.published, ...detail.workplaces.rows].find(
                    (entry) => entry.row.address_key === draft.replacesKey,
                  ) ?? null);
            return (
              <li
                key={draft.slot}
                data-slot-id={draft.slot}
                className="rounded-md border p-3 text-sm"
              >
                <div className="flex flex-wrap items-center gap-2">
                  <span className="font-medium">{line === "" ? EMPTY_VALUE : line}</span>
                  <Badge variant="outline">draft</Badge>
                  <Badge variant="secondary">{addressKindLabel(draft.raw.kind)}</Badge>
                </div>
                {draft.normalized === null ? (
                  <p className="text-muted-foreground mt-1 text-xs">parses on Fold now</p>
                ) : (
                  <p className="text-muted-foreground mt-1 text-xs">
                    {draft.normalized.normalized_address === ""
                      ? componentsLine(draft.normalized)
                      : draft.normalized.normalized_address}{" "}
                    · {draft.normalized.parse_status}
                  </p>
                )}
                {replaced === null ? null : (
                  <p className="text-muted-foreground mt-1 text-xs">
                    Replaces {replaced.row.normalized_address}
                  </p>
                )}
                {draft.raw.note === "" ? null : (
                  <p className="text-muted-foreground mt-1 text-xs">{draft.raw.note}</p>
                )}
                <div className="mt-2 flex gap-2">
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    disabled={busy}
                    onClick={() => onEdit(draft)}
                  >
                    Edit
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    disabled={busy}
                    onClick={() =>
                      onDecide({
                        intent: "activate",
                        addressKey: "",
                        slot: draft.slot,
                        line,
                        reviewerOnly: false,
                      })
                    }
                  >
                    Activate
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    disabled={busy}
                    onClick={() =>
                      onDecide({
                        intent: "discard",
                        addressKey: "",
                        slot: draft.slot,
                        line,
                        reviewerOnly: false,
                      })
                    }
                  >
                    Discard
                  </Button>
                </div>
              </li>
            );
          })}
        </ul>
      </CardContent>
    </Card>
  );
}

function HistoryCard({ detail }: { detail: SeAddressDetail }) {
  return (
    <Card>
      <Accordion>
        <AccordionItem value="history" className="border-0">
          <CardHeader>
            <AccordionTrigger className="py-0">
              <div className="text-left">
                <CardTitle>History</CardTitle>
                <CardDescription>
                  Every published version, newest first ({detail.history.length}).
                </CardDescription>
              </div>
            </AccordionTrigger>
          </CardHeader>
          <AccordionContent>
            <CardContent>
              {detail.history.length === 0 ? (
                <p className="text-muted-foreground text-sm">
                  No fold has published an address for this company yet.
                </p>
              ) : (
                <ul className="flex flex-col gap-2 text-sm">
                  {detail.history.map((row, index) => (
                    <li
                      key={`${row.folded_at}-${row.address_key}-${index}`}
                      className="grid gap-x-4 sm:grid-cols-[auto_1fr_auto]"
                    >
                      <span className="font-mono text-xs">{row.folded_at}</span>
                      <span>
                        {row.normalized_address === "" ? EMPTY_VALUE : row.normalized_address}
                      </span>
                      <span className="text-muted-foreground text-xs">
                        {activityLabel(row)} ·{" "}
                        {distinctSources(row.sources).map(addressSourceLabel).join(", ")}
                      </span>
                    </li>
                  ))}
                </ul>
              )}
            </CardContent>
          </AccordionContent>
        </AccordionItem>
      </Accordion>
    </Card>
  );
}

function FoldCard({
  companyId,
  detail,
  result,
  busy,
}: {
  companyId: string;
  detail: SeAddressDetail;
  result: SeAddressResult;
  busy: boolean;
}) {
  const launched = result !== null && result.ok && result.intent === "fold-now";
  const runId = launched ? (result.runId ?? "") : "";
  const runUrl = result?.ok ? (result.url ?? null) : null;
  return (
    <Card>
      <CardHeader>
        <CardTitle>Fold</CardTitle>
        <CardDescription>
          The targeted fold normalizes this company's raw rows -- a draft saved a moment
          ago parses -- and republishes its addresses.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        {detail.foldPending ? (
          <Alert>
            <TriangleAlertIcon />
            <AlertTitle>Fold pending</AlertTitle>
            <AlertDescription>
              A suggestion or a rule is newer than the last fold. Fold now publishes it.
            </AlertDescription>
          </Alert>
        ) : null}
        <Form method="post">
          <input type="hidden" name="intent" value="fold-now" />
          <Button type="submit" size="sm" disabled={busy}>
            Fold now
          </Button>
        </Form>
        {launched && runId === "" ? (
          // The run started but its id did not come back, so there is nothing
          // to poll: say so instead of leaving the click looking ignored.
          <Alert>
            <CheckCircle2Icon />
            <AlertTitle>Fold launched</AlertTitle>
            <AlertDescription>
              The run id did not come back, so this page cannot follow it. Check Dagster,
              then reload.
              {runUrl === null ? null : (
                <>
                  {" "}
                  <a className="underline" href={runUrl} target="_blank" rel="noreferrer">
                    Open in Dagster
                  </a>
                </>
              )}
            </AlertDescription>
          </Alert>
        ) : null}
        {runId === "" ? null : (
          // Keyed by run: a relaunch after the ten-minute cap must start a fresh
          // poller (new start instant, timed-out flag cleared), not reuse the old one.
          <FoldRunPoller
            key={runId}
            companyId={companyId}
            runId={runId}
            url={runUrl}
            foldPending={detail.foldPending}
          />
        )}
      </CardContent>
    </Card>
  );
}

const SUCCESS_COPY: Record<string, string> = {
  "save-draft": "Draft saved. Activate it to make it this company's reviewer address.",
  activate: "Reviewer address written. Fold now publishes it.",
  discard: "Draft discarded.",
  reset: "Rule released. Fold now publishes the address again.",
};

const REMOVE_SUCCESS = {
  whole: "Address hidden; fold to apply.",
  reviewer: "Reviewer address withdrawn; fold to apply.",
} as const;

/** What Remove just did, in the same two cases the dialog described (Ruling 1,
 * amended): a row any source also delivers is hidden whole, a row the reviewer
 * alone contributed is withdrawn. Exported for the same reason
 * `removeDescription` is: the alert it appears in needs a result to render. */
export function removeSuccessCopy(reviewerOnly: boolean): string {
  return REMOVE_SUCCESS[reviewerOnly ? "reviewer" : "whole"];
}

function ResultAlert({
  result,
  removedReviewerOnly,
}: {
  result: SeAddressResult;
  /** The confirmed Remove was of a reviewer-only row. */
  removedReviewerOnly: boolean;
}) {
  if (result === null) return null;
  if (!result.ok) {
    // The sheet shows its own refusal, in the form the reviewer is still
    // typing in; an intent-less one is shown here as well rather than nowhere.
    if (result.intent === "save-draft") return null;
    return (
      <Alert variant="destructive">
        <TriangleAlertIcon />
        <AlertTitle>Not saved</AlertTitle>
        <AlertDescription>{result.error}</AlertDescription>
      </Alert>
    );
  }
  // Fold now speaks through the poller instead.
  const copy =
    result.intent === "remove"
      ? removeSuccessCopy(removedReviewerOnly)
      : SUCCESS_COPY[result.intent];
  if (copy === undefined) return null;
  return (
    <Alert>
      <CheckCircle2Icon />
      <AlertTitle>Decision saved</AlertTitle>
      <AlertDescription>{copy}</AlertDescription>
    </Alert>
  );
}

/** One contributing source of the selected address: what it delivered, what
 * that parsed to, and whether the row was folded from an older version. */
function MemberEntry({ member }: { member: SeAddressMember }) {
  const raw = member.raw;
  const delivered = raw === null ? "" : rawLine(raw);
  const current = member.current;
  return (
    <li className="rounded-md border p-3 text-sm" data-source={member.source}>
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-medium">{addressSourceLabel(member.source)}</span>
        <span className="text-muted-foreground font-mono text-xs">{member.slot}</span>
        {raw === null || raw.kind === "" ? null : (
          <Badge variant="outline">{addressKindLabel(raw.kind)}</Badge>
        )}
        {member.refoldPending ? <Badge variant="outline">re-fold pending</Badge> : null}
      </div>
      <p className="mt-1">{delivered === "" ? EMPTY_VALUE : delivered}</p>
      {current === null ? (
        <p className="text-muted-foreground mt-1 text-xs">
          No current normalized version for this slot.
        </p>
      ) : (
        <>
          <p className="text-muted-foreground mt-1 text-xs">
            {componentsLine(current)} · {current.parse_status} · {current.normalizer_version}
          </p>
          {current.parse_notes === "" ? null : (
            <p className="text-muted-foreground mt-1 text-xs">{current.parse_notes}</p>
          )}
        </>
      )}
    </li>
  );
}

function AddressPanel({
  entry,
  busy,
  onAdd,
  onDecide,
}: {
  entry: SeAddressPublishedDetail | null;
  busy: boolean;
  onAdd: () => void;
  onDecide: (pending: PendingAddressDecision) => void;
}) {
  const hidden = entry !== null && entry.row.inactive_reason === "hidden";
  // Ruling 1 (amended): only an address the reviewer alone contributed is
  // withdrawn; anything a source also delivers is hidden whole.
  const reviewerOnly =
    entry !== null &&
    entry.members.length > 0 &&
    entry.members.every((member) => member.source === "reviewer");
  const selectedPoint = entry === null ? null : mapPoint(entry.row);
  return (
    <Card>
      <CardHeader>
        <CardTitle>{entry === null ? "No address selected" : "Address"}</CardTitle>
        <CardDescription>
          {entry === null
            ? "Nothing published for this company yet."
            : "Where the published line came from, and what the reviewer decided about it."}
        </CardDescription>
        <Button type="button" size="sm" variant="outline" className="mt-2 w-fit" onClick={onAdd}>
          Add address
        </Button>
      </CardHeader>
      {entry === null ? null : (
        <CardContent className="flex flex-col gap-4">
          <div>
            <p className="text-sm font-medium">
              {entry.row.normalized_address === "" ? EMPTY_VALUE : entry.row.normalized_address}
            </p>
            <p
              className="text-muted-foreground font-mono text-xs"
              title={entry.row.address_key}
            >
              {shortHash(entry.row.address_key)} · {activityLabel(entry.row)}
            </p>
          </div>

          <section>
            <h3 className="text-muted-foreground text-xs uppercase tracking-wide">Sources</h3>
            <ul className="mt-2 flex flex-col gap-2">
              {entry.members.map((member) => (
                <MemberEntry key={`${member.source}-${member.slot}`} member={member} />
              ))}
            </ul>
          </section>

          <section>
            <h3 className="text-muted-foreground text-xs uppercase tracking-wide">
              Published text
            </h3>
            <p className="mt-1 text-sm">
              {addressSourceLabel(entry.row.text_source)} ({entry.textSourceReason})
            </p>
          </section>

          <section>
            <h3 className="text-muted-foreground text-xs uppercase tracking-wide">Geocode</h3>
            <DefinitionList
              className="mt-2"
              entries={[
                ["Status", geocodeStatusLabel(entry.row.geocode_status)],
                ["Method", text(entry.row.geocode_method)],
                ["Precision", text(entry.row.geocode_precision)],
                [
                  "Confidence",
                  entry.row.geocode_confidence === null
                    ? EMPTY_VALUE
                    : String(entry.row.geocode_confidence),
                ],
                [
                  "Position",
                  entry.row.latitude === null || entry.row.longitude === null
                    ? EMPTY_VALUE
                    : `${entry.row.latitude}, ${entry.row.longitude}`,
                ],
                ["Policy", text(entry.row.geocode_policy)],
                [
                  "Reference",
                  entry.row.geocode_reference === "" ? (
                    EMPTY_VALUE
                  ) : (
                    <span className="font-mono" title={entry.row.geocode_reference}>
                      {shortHash(entry.row.geocode_reference)}
                    </span>
                  ),
                ],
                ["Geocoded", text(entry.row.geocoded_at)],
                // Spec 8's "the geocode block with its versions": which
                // normalizer parsed the components and which fold published them.
                ["Normalizer", text(entry.row.normalizer_version)],
                ["Fold", text(entry.row.fold_version)],
              ]}
            />
            {/* One point, so an ungeocoded address reads "No location" here
                rather than showing the whole company's map again. Clicking it
                selects what is already selected, so selection stays put. */}
            <div className="mt-2">
              <AddressMap
                points={selectedPoint === null ? [] : [selectedPoint]}
                selectedKey={selectedPoint?.key ?? null}
                onSelect={() => {}}
              />
            </div>
            {selectedPoint === null ? null : (
              <a
                className="text-muted-foreground mt-1 inline-block text-xs underline"
                href={openStreetMapUrl(selectedPoint)}
                target="_blank"
                rel="noreferrer"
              >
                Open in OpenStreetMap
              </a>
            )}
          </section>

          <section>
            <h3 className="text-muted-foreground text-xs uppercase tracking-wide">Rule</h3>
            {entry.hideRule === null ? (
              <p className="mt-1 text-sm">none</p>
            ) : (
              <p className="mt-1 text-sm">
                Hidden by the reviewer
                {entry.hideRule.note === "" ? "" : `: ${entry.hideRule.note}`}
                <span className="text-muted-foreground block text-xs">
                  {entry.hideRule.decided_at}
                </span>
              </p>
            )}
          </section>

          <div className="flex flex-wrap gap-2">
            <Button
              type="button"
              size="sm"
              variant="outline"
              disabled={busy || hidden}
              onClick={() =>
                onDecide({
                  intent: "remove",
                  addressKey: entry.row.address_key,
                  slot: "",
                  line: entry.row.normalized_address,
                  reviewerOnly,
                })
              }
            >
              Remove
            </Button>
            {entry.hideRule === null ? null : (
              <Button
                type="button"
                size="sm"
                variant="outline"
                disabled={busy}
                onClick={() =>
                  onDecide({
                    intent: "reset",
                    addressKey: entry.row.address_key,
                    slot: "",
                    line: entry.row.normalized_address,
                    reviewerOnly,
                  })
                }
              >
                Reset to default
              </Button>
            )}
          </div>
        </CardContent>
      )}
    </Card>
  );
}

/** What the edit sheet is open on: the mode plus everything the form posts. */
interface AddressSheetState {
  mode: SeAddressEditMode;
  initial: SeAddressEditInitial;
  slot: string | null;
  replacesKey: string | null;
}

export function SeAddressWorkspace({
  companyId,
  detail,
  result,
}: {
  companyId: string;
  detail: SeAddressDetail;
  result: SeAddressResult;
}) {
  const navigation = useNavigation();
  // React Router's navigation.formMethod is lower- or upper-cased depending on
  // version, so compare case-insensitively; a GET revalidation must not read
  // as busy.
  const busy =
    navigation.state !== "idle" && (navigation.formMethod ?? "").toUpperCase() === "POST";
  const [pending, setPending] = useState<PendingAddressDecision | null>(null);
  const [sheet, setSheet] = useState<AddressSheetState | null>(null);
  // The action's result carries the intent and nothing else, but Remove reads
  // differently for a reviewer-only row -- so the workspace keeps what the
  // dialog was opened on. Remove can only be posted from that dialog, so by the
  // time its result arrives this is the row it acted on; on a fresh load with a
  // persisting result it is the first-load default, the whole-address wording.
  const [removedReviewerOnly, setRemovedReviewerOnly] = useState(false);
  const decide = (next: PendingAddressDecision) => {
    if (next.intent === "remove") setRemovedReviewerOnly(next.reviewerOnly);
    setPending(next);
  };
  useEffect(() => {
    // Any result closes the confirmation: it either wrote what it proposed or
    // said why it did not, and the alert above carries that.
    if (result) setPending(null);
  }, [result]);
  useEffect(() => {
    // The saved draft closes the sheet -- here, not in the sheet itself: the
    // route's `actionData` outlives the save, so an effect inside the sheet
    // would see that stale success the moment the next Add / Correct / Edit
    // mounted it and shut it again immediately. Keyed on the result's
    // identity, this runs once per action round trip.
    if (result?.ok && result.intent === "save-draft") setSheet(null);
  }, [result]);
  // The loader made the selection: the `?address=` row when the key names one
  // of this company's, else the first active row -- the same default the
  // deleted `selectAddress` applied -- and only that row carries members.
  const selected = detail.selected;
  const selectedAddressKey = selected?.row.address_key ?? null;
  // One builder for every link on the page: selecting an address keeps the
  // workplace page and its filter, paging keeps the selected address.
  const addressHref = (addressKey: string) =>
    addressSearchString({
      address: addressKey,
      workplacePage: detail.workplaces.page,
      workplaceQuery: detail.workplaces.query,
    });
  const correct = (entry: SeAddressListEntry) =>
    setSheet({
      mode: "correct",
      initial: initialFromRow(entry.row),
      slot: null,
      replacesKey: entry.row.address_key,
    });
  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,2fr)_minmax(20rem,1fr)]">
      <div className="flex flex-col gap-6">
        <ResultAlert result={result} removedReviewerOnly={removedReviewerOnly} />
        <AddressesCard
          detail={detail}
          // The effective selection, not the raw query string: the loader's
          // resolved row, default included, so the list marks what the panel
          // describes.
          selectedKey={selectedAddressKey}
          mapPoints={listMapPoints(detail.published, detail.workplaces.rows)}
          addressHref={addressHref}
          busy={busy}
          onCorrect={correct}
        />
        <WorkplacesCard
          workplaces={detail.workplaces}
          selectedKey={selectedAddressKey}
          addressHref={addressHref}
          busy={busy}
          onCorrect={correct}
        />
        {detail.drafts.length === 0 ? null : (
          <DraftsCard
            detail={detail}
            busy={busy}
            onEdit={(draft) =>
              setSheet({
                mode: "edit-draft",
                initial: initialFromDraft(draft),
                slot: draft.slot,
                replacesKey: draft.replacesKey === "" ? null : draft.replacesKey,
              })
            }
            onDecide={decide}
          />
        )}
        <HistoryCard detail={detail} />
        <FoldCard companyId={companyId} detail={detail} result={result} busy={busy} />
      </div>
      <aside className="lg:sticky lg:top-4 lg:self-start">
        <AddressPanel
          entry={selected}
          busy={busy}
          onAdd={() =>
            setSheet({
              mode: "add",
              initial: EMPTY_ADDRESS_INITIAL,
              slot: null,
              replacesKey: null,
            })
          }
          onDecide={decide}
        />
      </aside>
      <AddressDecisionDialog pending={pending} busy={busy} onClose={() => setPending(null)} />
      {sheet === null ? null : (
        <SeAddressEditSheet
          open
          onOpenChange={(open) => {
            if (!open) setSheet(null);
          }}
          mode={sheet.mode}
          initial={sheet.initial}
          slot={sheet.slot}
          replacesKey={sheet.replacesKey}
          result={result}
        />
      )}
    </div>
  );
}
