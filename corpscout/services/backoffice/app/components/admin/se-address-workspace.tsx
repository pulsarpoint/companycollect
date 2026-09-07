import { CheckCircle2Icon, MapPinIcon, TriangleAlertIcon } from "lucide-react";
import { useEffect, useState } from "react";
import { Form, Link, useNavigation } from "react-router";
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
import { Dialog, DialogContent, DialogFooter, DialogHeader } from "~/components/ui/dialog";
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
import {
  addressKindLabel,
  addressSourceLabel,
  geocodeStatusLabel,
  MAX_NOTE_LENGTH,
} from "~/lib/se-address-fields";
import type {
  SeAddressComponents,
  SeAddressDetail,
  SeAddressDraft,
  SeAddressMember,
  SeAddressPublished,
  SeAddressRawRow,
  SeAddressRow,
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
 * `slot` from a saved draft; a refusal carries the reviewer-readable reason. */
export type SeAddressResult =
  | { ok: true; intent: string; runId?: string; url?: string | null; slot?: string }
  | { ok: false; error: string }
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

/** The address the panel describes: the one the query string names, else the
 * first active row, else the first row at all. */
function selectAddress(
  detail: SeAddressDetail,
  selectedKey: string | null,
): SeAddressPublished | null {
  if (selectedKey !== null) {
    const named = detail.published.find((entry) => entry.row.address_key === selectedKey);
    if (named) return named;
  }
  return detail.published.find((entry) => entry.row.active === 1) ?? detail.published[0] ?? null;
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
}

const DECISION_HEADINGS: Record<PendingAddressDecision["intent"], string> = {
  remove: "Remove this address",
  reset: "Reset this address to default",
  activate: "Activate this draft",
  discard: "Discard this draft",
};

const DECISION_DESCRIPTIONS: Record<PendingAddressDecision["intent"], string> = {
  remove:
    "A hide rule keeps this address out of the next fold; the sources keep delivering it, nothing is deleted. Reset to default brings it back.",
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
 * or draft, plus the optional note. Portal-free so a test can render it
 * statically; `AddressDecisionDialog` wraps it in the dialog chrome.
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
  return (
    <Form method="post" onSubmit={onClose} className="flex flex-col gap-4">
      {/* Plain heading and paragraph, not DialogTitle/DialogDescription: those
          need the dialog root's context, and this body also renders on its own
          in tests. */}
      <DialogHeader>
        <h2 className="text-base font-semibold leading-none">{DECISION_HEADINGS[intent]}</h2>
        <p className="text-muted-foreground text-sm">{DECISION_DESCRIPTIONS[intent]}</p>
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
 * the link (an anchor may not wrap a button). */
function AddressLine({
  entry,
  selectedKey,
  busy,
  onCorrect,
}: {
  entry: SeAddressPublished;
  selectedKey: string | null;
  busy: boolean;
  onCorrect: (entry: SeAddressPublished) => void;
}) {
  const { row } = entry;
  const selected = row.address_key === selectedKey;
  return (
    <li className="flex items-center gap-1" data-address-key={row.address_key}>
      <Link
        to={{ search: `?address=${row.address_key}` }}
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
  busy,
  onCorrect,
}: {
  detail: SeAddressDetail;
  selectedKey: string | null;
  busy: boolean;
  onCorrect: (entry: SeAddressPublished) => void;
}) {
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
              <EmptyTitle>No published address</EmptyTitle>
              <EmptyDescription>
                No fold has published an address for this company. Add address types one;
                Fold now publishes it.
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : null}
        {/* A list of links, not a <dl>: an anchor may not wrap dt/dd pairs. */}
        <ul className="grid grid-cols-1 gap-y-1 text-sm">
          {active.map((entry) => (
            <AddressLine
              key={entry.row.address_key}
              entry={entry}
              selectedKey={selectedKey}
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
                : (detail.published.find(
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
                      onDecide({ intent: "activate", addressKey: "", slot: draft.slot, line })
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
                      onDecide({ intent: "discard", addressKey: "", slot: draft.slot, line })
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
  const runId = result?.ok && result.intent === "fold-now" ? (result.runId ?? "") : "";
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
  remove: "Hide rule written. Fold now applies it.",
  reset: "Rule released. Fold now publishes the address again.",
};

function ResultAlert({ result }: { result: SeAddressResult }) {
  if (result === null) return null;
  if (!result.ok) {
    return (
      <Alert variant="destructive">
        <TriangleAlertIcon />
        <AlertTitle>Not saved</AlertTitle>
        <AlertDescription>{result.error}</AlertDescription>
      </Alert>
    );
  }
  // Fold now speaks through the poller instead.
  const copy = SUCCESS_COPY[result.intent];
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
            {componentsLine(current)} · {current.parse_status}
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
  entry: SeAddressPublished | null;
  busy: boolean;
  onAdd: () => void;
  onDecide: (pending: PendingAddressDecision) => void;
}) {
  const hidden = entry !== null && entry.row.inactive_reason === "hidden";
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
              ]}
            />
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
  selectedKey,
  result,
}: {
  companyId: string;
  detail: SeAddressDetail;
  /** The `?address=` key, when it is a well-formed one. */
  selectedKey: string | null;
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
  useEffect(() => {
    // Any result closes the confirmation: it either wrote what it proposed or
    // said why it did not, and the alert above carries that.
    if (result) setPending(null);
  }, [result]);
  const selected = selectAddress(detail, selectedKey);
  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,2fr)_minmax(20rem,1fr)]">
      <div className="flex flex-col gap-6">
        <ResultAlert result={result} />
        <AddressesCard
          detail={detail}
          selectedKey={selectedKey}
          busy={busy}
          onCorrect={(entry) =>
            setSheet({
              mode: "correct",
              initial: initialFromRow(entry.row),
              slot: null,
              replacesKey: entry.row.address_key,
            })
          }
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
            onDecide={setPending}
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
          onDecide={setPending}
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
