import { CheckCircle2Icon, TriangleAlertIcon, UsersIcon } from "lucide-react";
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
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "~/components/ui/dialog";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { Input } from "~/components/ui/input";
import { DefinitionList, EMPTY_VALUE, text } from "~/components/admin/definition-list";
import { FoldRunPoller } from "~/components/admin/fold-run-poller";
import {
  EMPTY_PERSON_INITIAL,
  SePersonEditSheet,
  type SePersonEditInitial,
  type SePersonEditMode,
  type SePersonEditRole,
} from "~/components/admin/se-person-edit-sheet";
import {
  MAX_NOTE_LENGTH,
  personSourceLabel,
  RESERVED_DATA_KEYS,
  REVIEWER_SOURCE,
  roleLabel,
  type SePersonRoleOption,
} from "~/lib/se-person-fields";
import {
  formatConfidence,
  matchNote,
  parseLlmMatch,
  stripLlmMatch,
  type SePersonPossibleMatch,
} from "~/lib/se-person-match";
import type {
  SePersonDetail,
  SePersonDraft,
  SePersonHistoryRow,
  SePersonMember,
  SePersonPrecedenceRow,
  SePersonPublished,
  SePersonRawRow,
  SePersonRoleYear,
  SePersonRow,
} from "~/lib/se-company-person-entity.server";
import { cn } from "~/lib/utils";

/**
 * The People tab (spec 2026-09-09 section 7), built like the Address tab: the company's
 * published persons and their history on the left, the selected person with the
 * observations behind it on the right, and every write a plain `<Form method="post">`
 * behind a confirmation dialog -- nothing posts until the reviewer confirms.
 *
 * The component never publishes anything itself: Remove, Reset, Merge, Split, Activate
 * and Discard all write a suggestion row or a rule version, and the next fold (Fold now,
 * the one Activate launches, or the scheduled one) is what republishes the persons.
 */

/** What the route's action returns. `runId`/`url` come back from Fold now and from
 * Activate (Ruling 6), `slot` from a saved draft; a refusal carries the
 * reviewer-readable reason and the intent it refused, so the sheet and the page alert
 * can each show only their own (an intent-less refusal is shown by both rather than by
 * neither). */
export type SePersonResult =
  | {
      ok: true;
      intent: string;
      runId?: string;
      url?: string | null;
      slot?: string;
      /** Set only when the decision itself landed but a follow-on step (the fold
       * Activate launches) failed -- so the write is not a lost one, but the reviewer
       * still has to act (Minor 1: Fold now). */
      message?: string;
    }
  | { ok: false; intent?: string; error: string }
  | null;

/** Ruling 9: a split pins slots, and Bolagsverket's slots are per filing. The sentence
 * is exported as a plain string because the dialog it appears in cannot be rendered on
 * its own -- `DialogTitle` needs the Dialog root, and an open dialog renders nothing at
 * all server-side (the same reason `se-address-workspace.tsx` exports
 * `removeDescription`). */
export const SPLIT_CAVEAT =
  "A split pins the observations you tick. Bolagsverket mints a new slot per annual report, so this split holds for today's observations and may need writing again after next year's report.";

/** A 64-hex key is unreadable in full and never needed in full: the first characters
 * identify it, the title attribute carries the rest. */
function shortHash(value: string): string {
  return value.length > 12 ? `${value.slice(0, 12)}…` : value;
}

/** One badge per distinct member source -- `sources` is the DISTINCT list the fold
 * published, but a hand-built row is cheap to defend against. */
function distinctSources(sources: readonly string[]): string[] {
  return [...new Set(sources)];
}

function activityLabel(row: SePersonRow): string {
  if (row.active === 1) return "active";
  return row.inactive_reason === "" ? "inactive" : row.inactive_reason;
}

/** The years this person was seen in, as one label: a single year when the fold saw
 * only one, the span otherwise. */
function yearsLabel(row: SePersonRow): string {
  if (row.first_year === "" && row.last_year === "") return "";
  if (row.first_year === row.last_year) return row.first_year;
  return `${row.first_year === "" ? "…" : row.first_year}–${row.last_year === "" ? "…" : row.last_year}`;
}

function wikidataUrl(qid: string): string {
  return `https://www.wikidata.org/wiki/${qid}`;
}

/** A `data` string as an object; a hand-written row that is not one reads as empty
 * rather than throwing the whole tab. */
function parseObject(data: string): Record<string, unknown> {
  try {
    const parsed: unknown = JSON.parse(data === "" ? "{}" : data);
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return parsed as Record<string, unknown>;
  } catch {
    return {};
  }
}

/** A stored `data` object, indented for reading; anything unparseable is shown as it
 * is stored rather than hidden. */
function prettyJson(data: string): string {
  try {
    return JSON.stringify(JSON.parse(data), null, 2);
  } catch {
    return data;
  }
}

function hasData(data: string): boolean {
  return data !== "" && data !== "{}";
}

/** A stored row's `data` without the three keys the backoffice owns (Ruling 4): the
 * sheet may not post them back, so a draft reopens on the reviewer's own object alone. */
function reviewerData(data: string): string {
  const parsed = parseObject(data);
  for (const key of RESERVED_DATA_KEYS) delete parsed[key];
  const own = JSON.stringify(parsed);
  return own === "{}" ? "" : own;
}

/** A `reviewer_draft` row still naming somebody; a row with all three name columns
 * empty is the tombstone an edit, an Activate or a Discard left behind. */
function holdsName(row: SePersonRawRow): boolean {
  return row.full_name !== "" || row.first_name !== "" || row.last_name !== "";
}

/** Ruling 5, read backwards: one fiscal year is a single year, a span is two dates. */
function rawYears(row: SePersonRawRow): string {
  if (row.fiscal_year !== "") return row.fiscal_year;
  const from = row.role_from.slice(0, 4);
  const to = row.role_to.slice(0, 4);
  if (from === "" && to === "") return "";
  return `${from === "" ? "…" : from}–${to === "" ? "…" : to}`;
}

/** One draft row as the sheet holds it: the catalog code it carries and its years. */
function draftRole(row: SePersonRawRow): SePersonEditRole | null {
  const code = row.role_key === "" ? row.role_original : row.role_key;
  const single = row.fiscal_year !== "";
  const fromYear = single ? row.fiscal_year : row.role_from.slice(0, 4);
  const toYear = single ? row.fiscal_year : row.role_to.slice(0, 4);
  if (code === "" && fromYear === "" && toYear === "") return null;
  return { code, fromYear, toYear };
}

/** The published role block, year by year, newest first (spec 5.4's parallel triple,
 * already zipped by the loader). */
function rolesByYear(roles: readonly SePersonRoleYear[]): [year: number, entries: SePersonRoleYear[]][] {
  const years = new Map<number, SePersonRoleYear[]>();
  for (const role of roles) {
    const entries = years.get(role.year);
    if (entries === undefined) years.set(role.year, [role]);
    else entries.push(role);
  }
  return [...years].sort((a, b) => b[0] - a[0]);
}

/** The roles view (spec section 11) has nothing FOR an inactive person to begin with --
 * the view's own WHERE is `p.active = 1` -- so an empty read there is not staleness, it
 * is the view working exactly as designed (F3). Only an ACTIVE person's empty-or-stale
 * read means "the view has not rebuilt since this fold" (F2). */
function personRoleFallbackNote(entry: SePersonPublished): string {
  return entry.row.active === 1
    ? "the roles view has not rebuilt since this fold"
    : "hidden and withdrawn persons are not in the roles view";
}

/** F2: the view's rows lag a fold by up to an hour, so a set of rows can be genuinely
 * present yet still describe the person as they were BEFORE the fold that is showing on
 * screen right now -- the empty case and the stale-rows case are the same fallback for
 * the same reason. Both `folded_at` columns share one format
 * (`YYYY-MM-DD HH:MM:SS.mmm` UTC), so a plain string compare orders them. */
function personRoleRowsAreStale(entry: SePersonPublished): boolean {
  return (
    entry.roleRows.length === 0 ||
    entry.roleRows.some((role) => role.folded_at < entry.row.folded_at)
  );
}

/** Whether the panel shows the view's own stored rows (spec section 11) rather than the
 * fold's array summary: only for an ACTIVE person (F3 -- the view holds no row for any
 * other kind) whose rows are not stale (F2). */
function showsPersonRoleRows(entry: SePersonPublished): boolean {
  return entry.row.active === 1 && !personRoleRowsAreStale(entry);
}

/** `precedence.py::precedence_for` as the panel shows it: the company's own row for a
 * source when one is in force, else the global number, highest first. */
function namePrecedence(
  rows: readonly SePersonPrecedenceRow[],
): { source: string; precedence: number; override: boolean }[] {
  const own = new Map<string, number>();
  const global = new Map<string, number>();
  for (const row of rows) {
    if (row.field !== "name" || row.removed !== 0) continue;
    (row.company_id === "" ? global : own).set(row.source, row.precedence);
  }
  return [...new Set([...global.keys(), ...own.keys()])]
    .map((source) => ({
      source,
      precedence: own.get(source) ?? global.get(source) ?? 0,
      override: own.has(source),
    }))
    .sort((a, b) => b.precedence - a.precedence || a.source.localeCompare(b.source));
}

/** The person the panel describes: the one the query string names, else the first
 * active row, else the first row at all. */
function selectPerson(detail: SePersonDetail, selectedKey: string | null): SePersonPublished | null {
  if (selectedKey !== null) {
    const named = detail.published.find((entry) => entry.row.person_key === selectedKey);
    if (named) return named;
  }
  return detail.published.find((entry) => entry.row.active === 1) ?? detail.published[0] ?? null;
}

/** What a Correct opens on: the published person's own columns and the roles it holds.
 * `data` stays empty on purpose -- the published object is the fold's merge of every
 * source's extras, and retyping it would re-assert all of it as the reviewer's own. A
 * role code the live catalogue does not have (spec 4.3's unmapped label, published as
 * itself) is dropped rather than prefilled -- its `<select>` has no matching `<option>`,
 * which silently falls back to "No role" while the years stay filled -- and named in
 * `unmappedRoleCodes` so the sheet can say so. */
function initialFromRow(
  entry: SePersonPublished,
  roleOptions: readonly SePersonRoleOption[],
): SePersonEditInitial {
  const { row } = entry;
  const known = new Set(roleOptions.map((option) => option.code));
  const spans = new Map<string, { from: number; to: number }>();
  const unmapped = new Set<string>();
  for (const role of entry.roles) {
    if (role.year === 0) continue;
    if (!known.has(role.code)) {
      unmapped.add(role.code);
      continue;
    }
    const span = spans.get(role.code);
    if (span === undefined) spans.set(role.code, { from: role.year, to: role.year });
    else {
      span.from = Math.min(span.from, role.year);
      span.to = Math.max(span.to, role.year);
    }
  }
  const roles: SePersonEditRole[] = [...spans].map(([code, span]) => ({
    code,
    fromYear: String(span.from),
    toYear: String(span.to),
  }));
  for (const code of row.current_roles) {
    if (!known.has(code)) {
      unmapped.add(code);
      continue;
    }
    if (!spans.has(code)) roles.push({ code, fromYear: "", toYear: "" });
  }
  return {
    firstName: row.first_name,
    lastName: row.last_name,
    birthYear: row.birth_year,
    wikidataId: row.wikidata_id,
    roles,
    unmappedRoleCodes: [...unmapped],
    data: "",
    note: "",
  };
}

/** What an Edit opens on: the draft's own rows, one role entry each (Ruling 2). */
function initialFromDraft(draft: SePersonDraft): SePersonEditInitial {
  const named = draft.rows.filter(holdsName);
  const first = named[0];
  const roles = named
    .map(draftRole)
    .filter((role): role is SePersonEditRole => role !== null);
  return {
    firstName: first?.first_name ?? "",
    lastName: first?.last_name ?? "",
    birthYear: first?.birth_year ?? "",
    wikidataId: first?.wikidata_id ?? "",
    roles,
    // A draft's rows are validated against the catalog before they are ever saved
    // (`validateSePersonInput`), so a draft never carries a code outside it.
    unmappedRoleCodes: [],
    data: reviewerData(first?.data ?? ""),
    note: draft.note,
  };
}

/** What a Remove / Reset / Activate / Discard / Split click proposes, shown for
 * confirmation before it posts. */
export interface PendingPersonDecision {
  intent: "remove" | "reset" | "activate" | "discard" | "split";
  /** The published key `remove` and `reset` act on; `''` otherwise. */
  personKey: string;
  /** The draft group slot `activate` and `discard` act on; `''` otherwise. */
  slot: string;
  /** The person or draft being acted on, shown in the dialog. */
  line: string;
  /** Split only: the observations to pick from. */
  members: SePersonMember[];
  /** Remove only: a person every member of which is the reviewer's own is withdrawn by
   * tombstoning its slots; a person with any source member is hidden whole. */
  reviewerOnly: boolean;
}

const DECISION_HEADINGS: Record<PendingPersonDecision["intent"], string> = {
  remove: "Remove this person",
  reset: "Reset this person to default",
  activate: "Activate this draft",
  discard: "Discard this draft",
  split: "Split these observations off",
};

const REMOVE_DESCRIPTIONS = {
  whole:
    "This hides the whole person, all sources included. Use Correct to publish a different spelling.",
  reviewer: "This withdraws the reviewer's person.",
} as const;

/** What Remove will do to one person, in the reviewer's words: the person key is
 * computed over the whole member set, so retiring one member of a mixed person would
 * only re-key it -- a person any source delivers is hidden whole, and only a
 * reviewer-only person is withdrawn. */
function removeDescription(reviewerOnly: boolean): string {
  return REMOVE_DESCRIPTIONS[reviewerOnly ? "reviewer" : "whole"];
}

const DECISION_DESCRIPTIONS: Record<
  Exclude<PendingPersonDecision["intent"], "remove" | "split">,
  string
> = {
  reset:
    "Every rule on this person is released and the next fold publishes it again, as its sources deliver it.",
  activate:
    "The draft becomes this company's reviewer person; a correction also hides the person it replaces. The fold runs at once.",
  discard: "The draft is cleared. Nothing published changes.",
};

const DECISION_CONFIRM: Record<PendingPersonDecision["intent"], string> = {
  remove: "Remove",
  reset: "Reset to default",
  activate: "Activate",
  discard: "Discard",
  split: "Split",
};

/**
 * The confirmation's form: what is about to be written, against which person, draft or
 * observations, plus the optional note. `DialogTitle` labels the dialog, so this body
 * belongs inside a `Dialog` root -- `PersonDecisionDialog` is what renders it.
 */
export function PersonDecisionDialogBody({
  pending,
  busy,
  onClose,
}: {
  pending: PendingPersonDecision;
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
      : intent === "split"
        ? SPLIT_CAVEAT
        : DECISION_DESCRIPTIONS[intent];
  return (
    <Form method="post" onSubmit={onClose} className="flex flex-col gap-4">
      {/* DialogTitle, so the dialog is labelled for a screen reader -- which means this
          body only renders inside a Dialog root, never on its own. */}
      <DialogHeader>
        <DialogTitle>{DECISION_HEADINGS[intent]}</DialogTitle>
        <p className="text-muted-foreground text-sm">{description}</p>
      </DialogHeader>
      <div className="rounded-md border p-3 text-sm" data-testid="person-decision-value">
        {pending.line === "" ? EMPTY_VALUE : pending.line}
      </div>
      <input type="hidden" name="intent" value={intent} />
      {pending.personKey === "" ? null : (
        <input type="hidden" name="person_key" value={pending.personKey} />
      )}
      {/* A split names slots through its checkboxes, never through this. */}
      {pending.slot === "" || intent === "split" ? null : (
        <input type="hidden" name="slot" value={pending.slot} />
      )}
      {intent === "split" ? (
        // A native checkbox: a Base UI one posts no form value.
        <ul className="flex max-h-64 flex-col gap-1 overflow-y-auto text-sm">
          {pending.members.map((member) => (
            <li key={member.slot}>
              <label className="flex items-start gap-2">
                <input type="checkbox" name="slot" value={member.slot} className="mt-1" />
                <span>
                  <span className="font-medium">{personSourceLabel(member.source)}</span>{" "}
                  <span className="text-muted-foreground font-mono text-xs">{member.slot}</span>
                  <span className="block">{member.name === "" ? EMPTY_VALUE : member.name}</span>
                </span>
              </label>
            </li>
          ))}
        </ul>
      ) : null}
      {showNote ? (
        <label className="flex flex-col gap-1 text-sm" htmlFor="person-note">
          <span className="text-muted-foreground text-xs">Why (optional)</span>
          <Input
            id="person-note"
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

function PersonDecisionDialog({
  pending,
  busy,
  onClose,
}: {
  pending: PendingPersonDecision | null;
  busy: boolean;
  onClose: () => void;
}) {
  return (
    <Dialog open={pending !== null} onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogContent>
        {pending ? (
          <PersonDecisionDialogBody pending={pending} busy={busy} onClose={onClose} />
        ) : null}
      </DialogContent>
    </Dialog>
  );
}

/** One published person: the name links to its panel, the merge checkbox, the Wikidata
 * link and Correct sit outside the link (an anchor may not wrap either). */
function PersonLine({
  entry,
  roleOptions,
  selectedKey,
  picked,
  busy,
  onPick,
  onCorrect,
}: {
  entry: SePersonPublished;
  roleOptions: readonly SePersonRoleOption[];
  selectedKey: string | null;
  picked: boolean;
  busy: boolean;
  onPick: (key: string, on: boolean) => void;
  onCorrect: (entry: SePersonPublished) => void;
}) {
  const { row } = entry;
  const selected = row.person_key === selectedKey;
  const years = yearsLabel(row);
  return (
    <li className="flex items-center gap-1" data-person-key={row.person_key}>
      {/* Native, so the Merge form actually posts it. */}
      <input
        type="checkbox"
        name="person_key"
        value={row.person_key}
        checked={picked}
        onChange={(event) => onPick(row.person_key, event.target.checked)}
        aria-label={`Pick ${row.display_name} for a merge`}
      />
      <Link
        to={{ search: `?person=${row.person_key}` }}
        preventScrollReset
        aria-current={selected ? "true" : undefined}
        className={cn(
          "grid flex-1 grid-cols-1 gap-x-6 rounded-md px-2 py-2 hover:bg-muted/60 sm:grid-cols-[1fr_auto]",
          selected && "bg-muted",
        )}
      >
        <span>
          {row.display_name === "" ? EMPTY_VALUE : row.display_name}
          {years === "" ? null : (
            <span className="text-muted-foreground ml-2 text-xs">{years}</span>
          )}
        </span>
        <span className="flex flex-wrap gap-1 sm:justify-end">
          {row.birth_year === "" ? null : <Badge variant="outline">b. {row.birth_year}</Badge>}
          {row.current_roles.map((code) => (
            <Badge key={code} variant="outline">
              {roleLabel(code, roleOptions)}
            </Badge>
          ))}
          {distinctSources(row.sources).map((source) => (
            <Badge key={source} variant="secondary">
              {personSourceLabel(source)}
            </Badge>
          ))}
          {row.inactive_reason === "" ? null : (
            <Badge variant="destructive">{row.inactive_reason}</Badge>
          )}
        </span>
      </Link>
      {row.wikidata_id === "" ? null : (
        <a
          className="text-muted-foreground font-mono text-xs underline"
          href={wikidataUrl(row.wikidata_id)}
          target="_blank"
          rel="noreferrer"
        >
          {row.wikidata_id}
        </a>
      )}
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

function PersonsCard({
  detail,
  roleOptions,
  selectedKey,
  busy,
  onAdd,
  onCorrect,
}: {
  detail: SePersonDetail;
  roleOptions: readonly SePersonRoleOption[];
  selectedKey: string | null;
  busy: boolean;
  onAdd: () => void;
  onCorrect: (entry: SePersonPublished) => void;
}) {
  // Merge takes two or more persons, so the ticks are the card's own state until the
  // reviewer submits them.
  const [picked, setPicked] = useState<string[]>([]);
  const pick = (key: string, on: boolean) =>
    setPicked((keys) => (on ? [...new Set([...keys, key])] : keys.filter((one) => one !== key)));
  const active = detail.published.filter((entry) => entry.row.active === 1);
  const inactive = detail.published.filter((entry) => entry.row.active !== 1);
  // A hidden or withdrawn person the panel is describing must be visible in the list
  // too: linking to one opens the group it hides in.
  const selectedIsInactive = inactive.some((entry) => entry.row.person_key === selectedKey);
  const line = (entry: SePersonPublished) => (
    <PersonLine
      key={entry.row.person_key}
      entry={entry}
      roleOptions={roleOptions}
      selectedKey={selectedKey}
      picked={picked.includes(entry.row.person_key)}
      busy={busy}
      onPick={pick}
      onCorrect={onCorrect}
    />
  );
  return (
    <Card>
      <CardHeader>
        <CardTitle>Persons</CardTitle>
        <CardDescription>
          What the last fold published, active first. Click a row to see the observations
          behind it; Correct types a replacement; tick two or more and Merge folds them
          into one person.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {detail.published.length === 0 && detail.drafts.length === 0 ? (
          // A company no source names a person for is a normal pipeline state, so the
          // empty card keeps Add person in reach rather than dead-ending.
          <Empty className="border">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <UsersIcon />
              </EmptyMedia>
              <EmptyTitle>No people published yet</EmptyTitle>
              <EmptyDescription>
                No fold has published a person for this company. Add person types one; Fold
                now publishes it.
              </EmptyDescription>
            </EmptyHeader>
            <EmptyContent>
              <Button type="button" size="sm" variant="outline" onClick={onAdd}>
                Add person
              </Button>
            </EmptyContent>
          </Empty>
        ) : null}
        <Form method="post">
          <input type="hidden" name="intent" value="merge" />
          {/* A list of links, not a <dl>: an anchor may not wrap dt/dd pairs. */}
          <ul className="grid grid-cols-1 gap-y-1 text-sm">{active.map(line)}</ul>
          {inactive.length === 0 ? null : (
            <Accordion className="mt-2" defaultValue={selectedIsInactive ? ["inactive"] : []}>
              <AccordionItem value="inactive" className="border-0">
                <AccordionTrigger>Hidden and withdrawn ({inactive.length})</AccordionTrigger>
                <AccordionContent>
                  <ul className="grid grid-cols-1 gap-y-1 text-sm">{inactive.map(line)}</ul>
                </AccordionContent>
              </AccordionItem>
            </Accordion>
          )}
          {detail.published.length === 0 ? null : (
            <div className="mt-3 flex flex-wrap items-end gap-2">
              <label className="flex flex-1 flex-col gap-1 text-sm" htmlFor="person-merge-note">
                <span className="text-muted-foreground text-xs">Why (optional)</span>
                <Input
                  id="person-merge-note"
                  name="note"
                  maxLength={MAX_NOTE_LENGTH}
                  placeholder="Note saved with the merge rule"
                />
              </label>
              <Button type="submit" size="sm" disabled={busy || picked.length < 2}>
                Merge
              </Button>
            </div>
          )}
        </Form>
      </CardContent>
    </Card>
  );
}

/**
 * Spec 2026-09-11 section 5: the pairs the model scored between the floor and the
 * merge threshold whose two sides sit in two different published persons. Nothing was
 * merged by them -- the reviewer decides, and the Merge writes the SAME merge rule the
 * Persons card writes (no new intent, no new rule kind), which outranks the threshold
 * on the next fold. One `<Form>` per row: a single form with several submit buttons
 * would have to carry every row's keys at once.
 */
function PossibleMatchesCard({
  matches,
  busy,
}: {
  matches: readonly SePersonPossibleMatch[];
  busy: boolean;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Possible matches</CardTitle>
        <CardDescription>
          Pairs the model scored below the merge threshold, each between two published
          persons. Merge writes a merge rule; Fold now folds them into one person.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ul className="flex flex-col gap-2 text-sm">
          {matches.map((match, index) => (
            <li
              key={`${match.personKeyA}-${match.personKeyB}-${index}`}
              className="flex flex-wrap items-center gap-2"
            >
              <span className="flex-1">
                {match.nameA === "" ? EMPTY_VALUE : match.nameA} ↔{" "}
                {match.nameB === "" ? EMPTY_VALUE : match.nameB}
                <span className="text-muted-foreground ml-2 text-xs">
                  {formatConfidence(match.confidence)}
                  {match.reason === "" ? "" : ` · ${match.reason}`}
                </span>
              </span>
              <Form method="post">
                <input type="hidden" name="intent" value="merge" />
                <input type="hidden" name="person_key" value={match.personKeyA} />
                <input type="hidden" name="person_key" value={match.personKeyB} />
                <input
                  type="hidden"
                  name="note"
                  value={matchNote(match.confidence, match.reason)}
                />
                <Button
                  type="submit"
                  size="sm"
                  variant="outline"
                  disabled={busy}
                  aria-label={`Merge ${match.nameA} and ${match.nameB}`}
                >
                  Merge
                </Button>
              </Form>
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
}

function DraftsCard({
  detail,
  roleOptions,
  busy,
  onEdit,
  onDecide,
}: {
  detail: SePersonDetail;
  roleOptions: readonly SePersonRoleOption[];
  busy: boolean;
  onEdit: (draft: SePersonDraft) => void;
  onDecide: (pending: PendingPersonDecision) => void;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Drafts</CardTitle>
        <CardDescription>
          Typed here, published by nobody yet. Activate writes the reviewer person and folds
          the company; Discard drops the draft.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ul className="flex flex-col gap-2">
          {detail.drafts.map((draft) => {
            const rows = draft.rows.filter(holdsName);
            const first = rows[0];
            const normalizedBySlot = new Map(draft.normalized.map((row) => [row.slot, row]));
            const replaced =
              draft.replacesKey === ""
                ? null
                : (detail.published.find((entry) => entry.row.person_key === draft.replacesKey) ??
                  null);
            return (
              <li
                key={draft.slot}
                data-slot-id={draft.slot}
                className="rounded-md border p-3 text-sm"
              >
                <div className="flex flex-wrap items-center gap-2">
                  <span className="font-medium">
                    {draft.name === "" ? EMPTY_VALUE : draft.name}
                  </span>
                  <Badge variant="outline">draft</Badge>
                  {first === undefined || first.birth_year === "" ? null : (
                    <Badge variant="outline">b. {first.birth_year}</Badge>
                  )}
                </div>
                <ul className="mt-1 flex flex-col gap-1">
                  {rows.map((row) => {
                    const normalized = normalizedBySlot.get(row.slot) ?? null;
                    const code = row.role_key === "" ? row.role_original : row.role_key;
                    const years = rawYears(row);
                    return (
                      <li key={row.slot} className="text-muted-foreground text-xs">
                        {code === "" ? "no role" : roleLabel(code, roleOptions)}
                        {years === "" ? "" : ` · ${years}`} ·{" "}
                        {normalized === null
                          ? // Nothing has parsed this row yet, so there is no identity to
                            // show: the targeted fold normalizes before it folds.
                            "parses on Fold now"
                          : `${normalized.display_name} · ${normalized.parse_status}`}
                      </li>
                    );
                  })}
                </ul>
                {replaced === null ? null : (
                  <p className="text-muted-foreground mt-1 text-xs">
                    Replaces {replaced.row.display_name}
                  </p>
                )}
                {draft.note === "" ? null : (
                  <p className="text-muted-foreground mt-1 text-xs">{draft.note}</p>
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
                        personKey: "",
                        slot: draft.slot,
                        line: draft.name,
                        members: [],
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
                        personKey: "",
                        slot: draft.slot,
                        line: draft.name,
                        members: [],
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

function HistoryEntry({
  row,
  roleOptions,
}: {
  row: SePersonHistoryRow;
  roleOptions: readonly SePersonRoleOption[];
}) {
  return (
    <li className="grid gap-x-4 sm:grid-cols-[auto_1fr_auto]">
      <span className="font-mono text-xs">{row.changed_at}</span>
      <span>
        {row.display_name === "" ? EMPTY_VALUE : row.display_name}
        <span className="text-muted-foreground ml-2 text-xs">{row.change_kind}</span>
      </span>
      <span className="text-muted-foreground text-xs">
        {activityLabel(row)} · {distinctSources(row.sources).map(personSourceLabel).join(", ")}
        {row.current_roles.length === 0
          ? ""
          : ` · ${row.current_roles.map((code) => roleLabel(code, roleOptions)).join(", ")}`}
      </span>
    </li>
  );
}

function HistoryCard({
  detail,
  roleOptions,
}: {
  detail: SePersonDetail;
  roleOptions: readonly SePersonRoleOption[];
}) {
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
                  No fold has published a person for this company yet.
                </p>
              ) : (
                <ul className="flex flex-col gap-2 text-sm">
                  {detail.history.map((row, index) => (
                    <HistoryEntry
                      key={`${row.changed_at}-${row.person_key}-${index}`}
                      row={row}
                      roleOptions={roleOptions}
                    />
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
  detail: SePersonDetail;
  result: SePersonResult;
  busy: boolean;
}) {
  // Ruling 6: Activate launches the targeted fold itself, so its result carries a run
  // to follow exactly as Fold now's does -- unless the launch itself is what failed
  // (Minor 1), in which case there is no run to poll and `ResultAlert` says so instead.
  const launched =
    result !== null &&
    result.ok &&
    result.message === undefined &&
    (result.intent === "fold-now" || result.intent === "activate");
  const runId = launched ? (result.runId ?? "") : "";
  const runUrl = result?.ok ? (result.url ?? null) : null;
  return (
    <Card>
      <CardHeader>
        <CardTitle>Fold</CardTitle>
        <CardDescription>
          The targeted fold normalizes this company's raw rows -- a draft saved a moment ago
          parses -- and republishes its persons.
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
          // The run started but its id did not come back, so there is nothing to poll:
          // say so instead of leaving the click looking ignored.
          <Alert>
            <CheckCircle2Icon />
            <AlertTitle>Fold launched</AlertTitle>
            <AlertDescription>
              The run id did not come back, so this page cannot follow it. Check Dagster, then
              reload.
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
          // Keyed by run: a relaunch after the ten-minute cap must start a fresh poller
          // (new start instant, timed-out flag cleared), not reuse the old one.
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
  "save-draft": "Draft saved. Activate it to make it this company's reviewer person.",
  activate: "Reviewer person written. The fold below is publishing it.",
  discard: "Draft discarded.",
  merge: "Merge rule written. Fold now folds them into one person.",
  split: "Split rule written. Fold now separates the observations you picked.",
  reset: "Rules released. Fold now publishes this person as its sources deliver it.",
};

const REMOVE_SUCCESS = {
  whole: "Person hidden; fold to apply.",
  reviewer: "Reviewer person withdrawn; fold to apply.",
} as const;

function ResultAlert({
  result,
  removedReviewerOnly,
}: {
  result: SePersonResult;
  /** The confirmed Remove was of a reviewer-only person. */
  removedReviewerOnly: boolean;
}) {
  if (result === null) return null;
  if (!result.ok) {
    // The sheet shows its own refusal, in the form the reviewer is still typing in; an
    // intent-less one is shown here as well rather than nowhere.
    if (result.intent === "save-draft") return null;
    return (
      <Alert variant="destructive">
        <TriangleAlertIcon />
        <AlertTitle>Not saved</AlertTitle>
        <AlertDescription>{result.error}</AlertDescription>
      </Alert>
    );
  }
  // Minor 1: the rows landed but the fold launch that was meant to follow them did not
  // -- not a lost write, but not what Ruling 6 promised either.
  if (result.message !== undefined) {
    return (
      <Alert>
        <TriangleAlertIcon />
        <AlertTitle>Fold launch failed</AlertTitle>
        <AlertDescription>{result.message}</AlertDescription>
      </Alert>
    );
  }
  // Fold now speaks through the poller instead.
  const copy =
    result.intent === "remove"
      ? REMOVE_SUCCESS[removedReviewerOnly ? "reviewer" : "whole"]
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

/** One contributing observation of the selected person: what that source spelled, what
 * it parsed to, the evidence row behind it, and where its spelling ranks. */
function MemberEntry({ member }: { member: SePersonMember }) {
  const { current, raw } = member;
  return (
    <li className="rounded-md border p-3 text-sm" data-source={member.source}>
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-medium">{personSourceLabel(member.source)}</span>
        <span className="text-muted-foreground font-mono text-xs">{member.slot}</span>
        <Badge variant="outline">precedence {member.precedence}</Badge>
        {member.match === null ? null : (
          // The fold merged this observation into the person because the model scored
          // it against another one; the reason is the model's own sentence.
          <Badge variant="outline" title={member.match.reason}>
            matched by LLM · {formatConfidence(member.match.confidence)}
          </Badge>
        )}
        {member.refoldPending ? <Badge variant="outline">re-fold pending</Badge> : null}
      </div>
      <p className="mt-1">{member.name === "" ? EMPTY_VALUE : member.name}</p>
      <p className="text-muted-foreground mt-1 text-xs">
        {member.birthYear === "" ? "no birth year" : `b. ${member.birthYear}`}
        {member.wikidataId === "" ? "" : ` · ${member.wikidataId}`}
      </p>
      {current === null ? (
        <p className="text-muted-foreground mt-1 text-xs">
          No current normalized version for this observation.
        </p>
      ) : (
        <>
          <p className="text-muted-foreground mt-1 text-xs">
            {current.parse_status} · {current.normalizer_version}
          </p>
          {current.parse_notes.length === 0 ? null : (
            <p className="text-muted-foreground mt-1 text-xs">{current.parse_notes.join(", ")}</p>
          )}
        </>
      )}
      {hasData(member.data) ? (
        <pre className="bg-muted mt-2 overflow-x-auto rounded-md p-2 text-xs">
          {prettyJson(member.data)}
        </pre>
      ) : null}
      {raw === null ? (
        <p className="text-muted-foreground mt-1 text-xs">
          No evidence row for this observation.
        </p>
      ) : (
        <DefinitionList
          className="mt-2"
          entries={[
            ["Role as delivered", text(raw.role_original)],
            ["Role key", text(raw.role_key)],
            ["Years", text(rawYears(raw))],
            ["Document", text(raw.document_ref)],
            ["Suggested", text(raw.suggested_at)],
          ]}
        />
      )}
    </li>
  );
}

function PersonPanel({
  entry,
  precedence,
  roleOptions,
  busy,
  onAdd,
  onCorrect,
  onDecide,
}: {
  entry: SePersonPublished | null;
  precedence: readonly SePersonPrecedenceRow[];
  roleOptions: readonly SePersonRoleOption[];
  busy: boolean;
  onAdd: () => void;
  onCorrect: (entry: SePersonPublished) => void;
  onDecide: (pending: PendingPersonDecision) => void;
}) {
  const hidden = entry !== null && entry.row.inactive_reason === "hidden";
  const reviewerOnly =
    entry !== null &&
    entry.members.length > 0 &&
    entry.members.every((member) => member.source === REVIEWER_SOURCE);
  const order = namePrecedence(precedence);
  // The fold's own record of what it merged (`fold.py::_with_llm_match`), read from the
  // published row rather than from the live pair table: this is what was true when the
  // person was published. The JSON block below shows `data` WITHOUT it -- one fact is
  // not rendered twice, and a reviewer reading JSON is reading the sources' extras.
  const llmMatch = entry === null ? null : parseLlmMatch(entry.row.data);
  const dataWithoutMatch = entry === null ? "" : stripLlmMatch(entry.row.data);
  const decision = (
    intent: PendingPersonDecision["intent"],
    person: SePersonPublished,
  ): PendingPersonDecision => ({
    intent,
    // A split acts on the observations its checkboxes name, never on the key.
    personKey: intent === "split" ? "" : person.row.person_key,
    slot: "",
    line: person.row.display_name,
    members: intent === "split" ? person.members : [],
    reviewerOnly,
  });
  return (
    <Card>
      <CardHeader>
        <CardTitle>{entry === null ? "No person selected" : "Person"}</CardTitle>
        <CardDescription>
          {entry === null
            ? "Nothing published for this company yet."
            : "Where the published person came from, and what the reviewer decided about it."}
        </CardDescription>
        <Button type="button" size="sm" variant="outline" className="mt-2 w-fit" onClick={onAdd}>
          Add person
        </Button>
      </CardHeader>
      {entry === null ? null : (
        <CardContent className="flex flex-col gap-4">
          <div>
            <p className="text-sm font-medium">
              {entry.row.display_name === "" ? EMPTY_VALUE : entry.row.display_name}
            </p>
            <p className="text-muted-foreground font-mono text-xs" title={entry.row.person_key}>
              {shortHash(entry.row.person_key)} · {activityLabel(entry.row)}
            </p>
          </div>

          <DefinitionList
            entries={[
              ["Birth year", text(entry.row.birth_year)],
              [
                "Wikidata",
                entry.row.wikidata_id === "" ? (
                  EMPTY_VALUE
                ) : (
                  <a
                    className="underline"
                    href={wikidataUrl(entry.row.wikidata_id)}
                    target="_blank"
                    rel="noreferrer"
                  >
                    {entry.row.wikidata_id}
                  </a>
                ),
              ],
              [
                "Published spelling",
                `${personSourceLabel(entry.row.text_source)} (${entry.spellingReason})`,
              ],
              ["Seen", text(yearsLabel(entry.row))],
            ]}
          />

          <section>
            <h3 className="text-muted-foreground text-xs uppercase tracking-wide">Members</h3>
            <ul className="mt-2 flex flex-col gap-2">
              {entry.members.map((member) => (
                <MemberEntry key={`${member.source}-${member.slot}`} member={member} />
              ))}
            </ul>
          </section>

          {llmMatch === null ? null : (
            <section>
              <h3 className="text-muted-foreground text-xs uppercase tracking-wide">
                LLM match
              </h3>
              <ul className="mt-2 flex flex-col gap-1 text-sm">
                {llmMatch.pairs.map((pair, index) => (
                  <li key={`${pair.a}-${pair.b}-${index}`}>
                    <span>
                      {pair.a} ↔ {pair.b}
                    </span>
                    <span className="text-muted-foreground ml-2 text-xs">
                      {formatConfidence(pair.confidence)}
                      {pair.reason === "" ? "" : ` · ${pair.reason}`}
                    </span>
                  </li>
                ))}
              </ul>
              {entry.matchedBy.length === 0 ? (
                // The fold recorded this pair, but no pair in the CURRENT band still
                // names this person's observations -- the candidate list changed, or the
                // model stopped scoring it.
                <p className="text-muted-foreground mt-1 text-xs">
                  Recorded by the last fold; the current pairs no longer name these
                  observations.
                </p>
              ) : null}
              <p className="text-muted-foreground mt-1 text-xs">
                {llmMatch.model}
                {llmMatch.promptVersion === "" ? "" : ` · ${llmMatch.promptVersion}`}
              </p>
            </section>
          )}

          <section>
            <h3 className="text-muted-foreground text-xs uppercase tracking-wide">Roles</h3>
            {showsPersonRoleRows(entry) ? (
              <ul className="mt-2 flex flex-col gap-1 text-sm">
                {entry.roleRows.map((role) => (
                  <li
                    key={`${role.source}|${role.slot}|${role.role_code}|${role.role_year}`}
                    className="grid gap-x-3 sm:grid-cols-[4rem_1fr]"
                  >
                    <span className="text-muted-foreground font-mono text-xs">
                      {role.role_year === 0 ? EMPTY_VALUE : role.role_year}
                    </span>
                    <span className="flex flex-wrap items-center gap-2">
                      <Badge
                        variant={role.is_current === 1 ? "default" : "outline"}
                        title={
                          role.is_current === 1
                            ? "On the person's latest observed year"
                            : undefined
                        }
                      >
                        {roleLabel(role.role_code, roleOptions)}
                      </Badge>
                      {role.role_from === "" && role.role_to === "" ? null : (
                        <span className="text-muted-foreground font-mono text-xs">
                          {role.role_from === "" ? "?" : role.role_from}
                          {" – "}
                          {role.role_to === "" ? "" : role.role_to}
                        </span>
                      )}
                      <span className="text-muted-foreground text-xs">
                        {personSourceLabel(role.source)}
                      </span>
                    </span>
                  </li>
                ))}
              </ul>
            ) : entry.roles.length === 0 ? (
              <p className="mt-1 text-sm">none</p>
            ) : (
              <>
                <ul className="mt-2 flex flex-col gap-1 text-sm">
                  {rolesByYear(entry.roles).map(([year, entries]) => (
                    <li key={year} className="grid gap-x-3 sm:grid-cols-[4rem_1fr]">
                      <span className="text-muted-foreground font-mono text-xs">
                        {year === 0 ? EMPTY_VALUE : year}
                      </span>
                      <span className="flex flex-wrap items-center gap-2">
                        {entries.map((role) => (
                          <span key={role.code} className="flex items-center gap-1">
                            <Badge variant="outline">{roleLabel(role.code, roleOptions)}</Badge>
                            <span className="text-muted-foreground text-xs">
                              {role.sources.map(personSourceLabel).join(", ")}
                            </span>
                          </span>
                        ))}
                      </span>
                    </li>
                  ))}
                </ul>
                <p className="text-muted-foreground mt-1 text-xs">
                  From the person row's own arrays — {personRoleFallbackNote(entry)}.
                </p>
              </>
            )}
          </section>

          <section>
            <h3 className="text-muted-foreground text-xs uppercase tracking-wide">Data</h3>
            {hasData(dataWithoutMatch) ? (
              <pre className="bg-muted mt-2 overflow-x-auto rounded-md p-2 text-xs">
                {prettyJson(dataWithoutMatch)}
              </pre>
            ) : (
              <p className="mt-1 text-sm">none</p>
            )}
          </section>

          <section>
            <h3 className="text-muted-foreground text-xs uppercase tracking-wide">Rules</h3>
            {entry.rules.length === 0 ? (
              <p className="mt-1 text-sm">none</p>
            ) : (
              <ul className="mt-2 flex flex-col gap-1 text-sm">
                {entry.rules.map((rule) => (
                  <li key={rule.rule_id}>
                    <Badge variant="outline">{rule.kind}</Badge>
                    {rule.note === "" ? null : <span className="ml-2">{rule.note}</span>}
                    <span className="text-muted-foreground block text-xs">{rule.created_at}</span>
                  </li>
                ))}
              </ul>
            )}
          </section>

          <section>
            <h3 className="text-muted-foreground text-xs uppercase tracking-wide">Precedence</h3>
            {order.length === 0 ? (
              <p className="mt-1 text-sm">none</p>
            ) : (
              <ul className="mt-2 flex flex-col gap-1 text-sm">
                {order.map((row) => (
                  <li key={row.source} className="flex flex-wrap items-center gap-2">
                    <span>{personSourceLabel(row.source)}</span>
                    <span className="text-muted-foreground font-mono text-xs">
                      {row.precedence}
                    </span>
                    {row.override ? <Badge variant="outline">company override</Badge> : null}
                  </li>
                ))}
              </ul>
            )}
          </section>

          <div className="flex flex-wrap gap-2">
            <Button
              type="button"
              size="sm"
              variant="outline"
              disabled={busy}
              onClick={() => onCorrect(entry)}
            >
              Correct
            </Button>
            <Button
              type="button"
              size="sm"
              variant="outline"
              disabled={busy || hidden}
              onClick={() => onDecide(decision("remove", entry))}
            >
              Remove
            </Button>
            <Button
              type="button"
              size="sm"
              variant="outline"
              disabled={busy || entry.members.length < 2}
              onClick={() => onDecide(decision("split", entry))}
            >
              Split
            </Button>
            {entry.rules.length === 0 ? null : (
              <Button
                type="button"
                size="sm"
                variant="outline"
                disabled={busy}
                onClick={() => onDecide(decision("reset", entry))}
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
interface PersonSheetState {
  mode: SePersonEditMode;
  initial: SePersonEditInitial;
  slot: string | null;
  replacesKey: string | null;
}

export function SePersonWorkspace({
  companyId,
  detail,
  roleOptions,
  selectedKey,
  result,
}: {
  companyId: string;
  detail: SePersonDetail;
  /** The live role catalog (Ruling 3), as the loader read it. */
  roleOptions: readonly SePersonRoleOption[];
  /** The `?person=` key, when it is a well-formed one. */
  selectedKey: string | null;
  result: SePersonResult;
}) {
  const navigation = useNavigation();
  // React Router's navigation.formMethod is lower- or upper-cased depending on version,
  // so compare case-insensitively; a GET revalidation must not read as busy.
  const busy =
    navigation.state !== "idle" && (navigation.formMethod ?? "").toUpperCase() === "POST";
  const [pending, setPending] = useState<PendingPersonDecision | null>(null);
  const [sheet, setSheet] = useState<PersonSheetState | null>(null);
  // The action's result carries the intent and nothing else, but Remove reads
  // differently for a reviewer-only person -- so the workspace keeps what the dialog was
  // opened on. Remove can only be posted from that dialog, so by the time its result
  // arrives this is the person it acted on; on a fresh load with a persisting result it
  // is the first-load default, the whole-person wording.
  const [removedReviewerOnly, setRemovedReviewerOnly] = useState(false);
  const decide = (next: PendingPersonDecision) => {
    if (next.intent === "remove") setRemovedReviewerOnly(next.reviewerOnly);
    setPending(next);
  };
  useEffect(() => {
    // Any result closes the confirmation: it either wrote what it proposed or said why
    // it did not, and the alert above carries that.
    if (result) setPending(null);
  }, [result]);
  useEffect(() => {
    // The saved draft closes the sheet -- here, not in the sheet itself: the route's
    // `actionData` outlives the save, so an effect inside the sheet would see that stale
    // success the moment the next Add / Correct / Edit mounted it and shut it again
    // immediately. Keyed on the result's identity, this runs once per action round trip.
    if (result?.ok && result.intent === "save-draft") setSheet(null);
  }, [result]);
  const selected = selectPerson(detail, selectedKey);
  const openAdd = () =>
    setSheet({ mode: "add", initial: EMPTY_PERSON_INITIAL, slot: null, replacesKey: null });
  const openCorrect = (entry: SePersonPublished) =>
    setSheet({
      mode: "correct",
      initial: initialFromRow(entry, roleOptions),
      slot: null,
      replacesKey: entry.row.person_key,
    });
  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,2fr)_minmax(20rem,1fr)]">
      <div className="flex flex-col gap-6">
        <ResultAlert result={result} removedReviewerOnly={removedReviewerOnly} />
        <PersonsCard
          detail={detail}
          roleOptions={roleOptions}
          // The effective selection, not the raw query string: with no `?person=` the
          // panel describes the first active row, and that is the row the list must mark.
          selectedKey={selected?.row.person_key ?? null}
          busy={busy}
          onAdd={openAdd}
          onCorrect={openCorrect}
        />
        {detail.possibleMatches.length === 0 ? null : (
          <PossibleMatchesCard matches={detail.possibleMatches} busy={busy} />
        )}
        {detail.drafts.length === 0 ? null : (
          <DraftsCard
            detail={detail}
            roleOptions={roleOptions}
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
        <HistoryCard detail={detail} roleOptions={roleOptions} />
        <FoldCard companyId={companyId} detail={detail} result={result} busy={busy} />
      </div>
      <aside className="lg:sticky lg:top-4 lg:self-start">
        <PersonPanel
          entry={selected}
          precedence={detail.precedence}
          roleOptions={roleOptions}
          busy={busy}
          onAdd={openAdd}
          onCorrect={openCorrect}
          onDecide={decide}
        />
      </aside>
      <PersonDecisionDialog pending={pending} busy={busy} onClose={() => setPending(null)} />
      {sheet === null ? null : (
        <SePersonEditSheet
          open
          onOpenChange={(open) => {
            if (!open) setSheet(null);
          }}
          mode={sheet.mode}
          initial={sheet.initial}
          roleOptions={roleOptions}
          slot={sheet.slot}
          replacesKey={sheet.replacesKey}
          result={result}
        />
      )}
    </div>
  );
}
