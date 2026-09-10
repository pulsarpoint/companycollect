import type React from "react";
import { useState } from "react";
import { Form, useNavigation } from "react-router";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import { Textarea } from "~/components/ui/textarea";
import {
  MAX_DATA_LENGTH,
  MAX_NAME_LENGTH,
  MAX_NOTE_LENGTH,
  MAX_ROLE_ENTRIES,
  type SePersonRoleOption,
} from "~/lib/se-person-fields";
// Type-only, so no module cycle survives compilation: the workspace owns the
// action-result shape and imports this sheet as a value.
import type { SePersonResult } from "~/components/admin/se-person-workspace";

/**
 * The People tab's typing sheet (spec 2026-09-09 section 7): one reviewer draft, in
 * three modes -- `add` (a new group slot), `correct` (a new group slot carrying the
 * published key it replaces) and `edit-draft` (the group already drafted).
 *
 * The form only ever sends what `parseSePersonDecision` reads for
 * `intent=save-draft`: `first_name`, `last_name`, `birth_year`, `wikidata_id`, one
 * `role_code`/`role_from`/`role_to` triple per rendered role row, `data`, `note`, plus
 * the hidden `slot` and `replaces_key`. Every rule (a name the normalizer scores `ok`,
 * a plausible year and QID, a catalog role code, a `data` object without the reserved
 * keys) lives in `validateSePersonInput`, not here -- the sheet is the typing surface,
 * the parser is the gate.
 *
 * `SePersonEditForm` is the exported, portal-free body (like the Address tab's
 * `SeAddressEditForm`): a Base UI `Sheet.Title` needs the Sheet root's context to
 * render at all, so the header lives in `SePersonEditSheet` alone and the form itself
 * never uses it -- which is what lets a test render the form directly, outside any
 * `Sheet`. The form carries the mode's title as its own `aria-label` instead, so the
 * region a reviewer is typing in is named either way.
 *
 * Nothing here decides when the sheet closes: the sheet is a controlled component and
 * the workspace closes it, once, on the result of the save it just posted. An effect in
 * here would fire on mount instead -- the route's `actionData` outlives the save, so
 * every later Add / Correct / Edit would open onto a stale success and shut immediately.
 */

export type SePersonEditMode = "add" | "correct" | "edit-draft";

/** One role row as the sheet holds it: a catalog code and the two years around it. */
export interface SePersonEditRole {
  code: string;
  fromYear: string;
  toYear: string;
}

/** What the sheet opens with: a published person's columns for a Correct, the draft's
 * own raw rows for an edit, nothing at all for an Add. */
export interface SePersonEditInitial {
  firstName: string;
  lastName: string;
  birthYear: string;
  wikidataId: string;
  roles: SePersonEditRole[];
  /** Role codes the published person carries that the live catalogue does not (spec
   * 4.3: an unmapped source label publishes as itself) -- dropped from `roles` above
   * rather than prefilled into a row whose `<select>` has no matching `<option>`, which
   * silently falls back to "No role" while the years stay filled. */
  unmappedRoleCodes: string[];
  /** The reviewer's own JSON object, without the three keys the backoffice owns. */
  data: string;
  note: string;
}

/** An Add starts blank, on one empty role row. */
export const EMPTY_PERSON_INITIAL: SePersonEditInitial = {
  firstName: "",
  lastName: "",
  birthYear: "",
  wikidataId: "",
  roles: [],
  unmappedRoleCodes: [],
  data: "",
  note: "",
};

const NATIVE_SELECT_CLASSNAME =
  "h-8 w-full rounded-lg border border-input bg-transparent px-2.5 text-sm";

const TITLES: Record<SePersonEditMode, string> = {
  add: "Add person",
  correct: "Correct person",
  "edit-draft": "Edit draft person",
};

const DESCRIPTIONS: Record<SePersonEditMode, string> = {
  add: "Save draft writes a reviewer draft. Activate publishes it and folds the company.",
  correct:
    "Save draft writes a reviewer draft against this person. Activating it hides the person it replaces, unless the spelling folds back into it.",
  "edit-draft": "Save draft rewrites this draft. Nothing published changes until you activate it.",
};

function Field({
  label,
  hint,
  children,
}: {
  label: string;
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <label className="flex flex-col gap-1 text-sm">
      <span className="text-muted-foreground text-xs uppercase tracking-wide">{label}</span>
      {children}
      {hint === undefined ? null : (
        <span className="text-muted-foreground text-xs">{hint}</span>
      )}
    </label>
  );
}

/** The catalog's options in their own groups, in the order the catalog delivered them
 * (Ruling 3: the list is the live table's, never a hard-coded one). */
function groupedOptions(
  options: readonly SePersonRoleOption[],
): [group: string, options: SePersonRoleOption[]][] {
  const groups = new Map<string, SePersonRoleOption[]>();
  for (const option of options) {
    const existing = groups.get(option.group);
    if (existing === undefined) groups.set(option.group, [option]);
    else existing.push(option);
  }
  return [...groups];
}

/** One role row: the catalog code and its two years. The three inputs are always all
 * three, on every rendered row -- `roleRows` in the decision parser zips
 * `role_code`, `role_from` and `role_to` by index, and a row that posted only some of
 * them would shift every row after it. A row with nothing in it is dropped by the
 * validation, which is what makes the trailing blank row free. */
function RoleRow({
  role,
  grouped,
  index,
}: {
  role: SePersonEditRole | undefined;
  grouped: [group: string, options: SePersonRoleOption[]][];
  index: number;
}) {
  const position = index + 1;
  return (
    <div className="grid gap-2 sm:grid-cols-[1fr_7rem_7rem]">
      <select
        name="role_code"
        defaultValue={role?.code ?? ""}
        aria-label={`Role ${position}`}
        className={NATIVE_SELECT_CLASSNAME}
      >
        <option value="">No role</option>
        {grouped.map(([group, options]) => (
          <optgroup key={group === "" ? "other" : group} label={group === "" ? "Other" : group}>
            {options.map((option) => (
              <option key={option.code} value={option.code}>
                {option.label}
              </option>
            ))}
          </optgroup>
        ))}
      </select>
      <Input
        name="role_from"
        type="number"
        defaultValue={role?.fromYear ?? ""}
        aria-label={`Role ${position} from year`}
        placeholder="From"
      />
      <Input
        name="role_to"
        type="number"
        defaultValue={role?.toYear ?? ""}
        aria-label={`Role ${position} to year`}
        placeholder="To"
      />
    </div>
  );
}

/**
 * The sheet's form body, portal-free so a test can render it directly. `key` on the
 * `<Form>` is what makes reopening the sheet for another person re-run every
 * `defaultValue` -- without it React keeps reusing the same uncontrolled inputs and
 * never re-prefills them.
 */
export function SePersonEditForm({
  mode,
  initial,
  roleOptions,
  slot,
  replacesKey,
  result,
  onCancel,
}: {
  mode: SePersonEditMode;
  initial: SePersonEditInitial;
  /** The live role catalog (Ruling 3), as the loader read it. */
  roleOptions: readonly SePersonRoleOption[];
  /** The draft group slot being edited; `null` for a new draft (the store stamps one). */
  slot: string | null;
  /** The published key a Correct replaces; `null` otherwise. */
  replacesKey: string | null;
  result: SePersonResult;
  onCancel: () => void;
}) {
  const navigation = useNavigation();
  const busy =
    navigation.state !== "idle" && (navigation.formMethod ?? "").toUpperCase() === "POST";
  // The roles the person already has, plus one blank row to type the next one in; Add
  // role opens further rows up to the cap the validation enforces. State, not a prop:
  // the workspace unmounts the sheet between opens, so this starts over every time.
  const [rowCount, setRowCount] = useState(
    Math.min(Math.max(initial.roles.length + 1, 1), MAX_ROLE_ENTRIES),
  );
  const grouped = groupedOptions(roleOptions);
  // Only this form's own refusal belongs inside the sheet. A Remove, Reset, Merge,
  // Split, Activate or Discard refusal is the workspace's "Not saved" alert to show;
  // an intent-less refusal (an older route, a failure before parsing) is shown in both
  // places rather than nowhere.
  const error =
    result && !result.ok && (result.intent === undefined || result.intent === "save-draft")
      ? result.error
      : undefined;
  return (
    <Form
      method="post"
      key={`${mode}:${slot ?? ""}:${replacesKey ?? ""}`}
      aria-label={TITLES[mode]}
      className="flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto px-4 pb-4"
    >
      <input type="hidden" name="intent" value="save-draft" />
      <input type="hidden" name="slot" value={slot ?? ""} />
      <input type="hidden" name="replaces_key" value={replacesKey ?? ""} />
      <div className="grid gap-4 sm:grid-cols-2">
        <Field label="First name">
          <Input
            name="first_name"
            defaultValue={initial.firstName}
            maxLength={MAX_NAME_LENGTH}
            aria-label="First name"
            placeholder="Anna"
            required
          />
        </Field>
        <Field label="Last name" hint="A particle belongs to the last name: von Essen">
          <Input
            name="last_name"
            defaultValue={initial.lastName}
            maxLength={MAX_NAME_LENGTH}
            aria-label="Last name"
            placeholder="Svensson"
            required
          />
        </Field>
      </div>
      <div className="grid gap-4 sm:grid-cols-[10rem_1fr]">
        <Field label="Birth year">
          <Input
            name="birth_year"
            type="number"
            defaultValue={initial.birthYear}
            aria-label="Birth year"
            placeholder="1975"
          />
        </Field>
        <Field label="Wikidata id" hint="Q42, when this person has one.">
          <Input
            name="wikidata_id"
            defaultValue={initial.wikidataId}
            aria-label="Wikidata id"
            placeholder="Q42"
          />
        </Field>
      </div>
      <fieldset className="flex flex-col gap-2">
        <legend className="text-muted-foreground text-xs uppercase tracking-wide">Roles</legend>
        {initial.unmappedRoleCodes.length === 0 ? null : (
          <p className="text-muted-foreground text-xs">
            {initial.unmappedRoleCodes.length} role(s) with codes outside the catalogue were not
            prefilled: {initial.unmappedRoleCodes.join(", ")}
          </p>
        )}
        {Array.from({ length: rowCount }, (_, index) => (
          <RoleRow
            key={`role-${index}`}
            role={initial.roles[index]}
            grouped={grouped}
            index={index}
          />
        ))}
        <p className="text-muted-foreground text-xs">
          The same year in both fields is a fiscal year; two are a span; neither means the
          role is held now.
        </p>
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="w-fit"
          disabled={busy || rowCount >= MAX_ROLE_ENTRIES}
          onClick={() => setRowCount((count) => Math.min(count + 1, MAX_ROLE_ENTRIES))}
        >
          Add role
        </Button>
      </fieldset>
      <Field label="Data" hint="A JSON object of extras. Leave it empty for none.">
        <Textarea
          name="data"
          rows={4}
          defaultValue={initial.data}
          maxLength={MAX_DATA_LENGTH}
          aria-label="Data"
          placeholder='{"title": "Chair"}'
        />
      </Field>
      <Field label="Note">
        <Textarea
          name="note"
          rows={3}
          defaultValue={initial.note}
          maxLength={MAX_NOTE_LENGTH}
          aria-label="Note"
          placeholder="Why this person (optional)"
        />
      </Field>
      <SheetFooter className="p-0">
        {error === undefined ? null : (
          <p role="alert" className="text-destructive text-sm">
            {error}
          </p>
        )}
        <div className="flex gap-2">
          <Button type="button" variant="outline" onClick={onCancel} disabled={busy}>
            Cancel
          </Button>
          <Button type="submit" disabled={busy}>
            Save draft
          </Button>
        </div>
      </SheetFooter>
    </Form>
  );
}

/**
 * The sheet chrome around `SePersonEditForm`, as wide as the Address tab's. Fully
 * controlled: the workspace opens it, and closes it on the result of the save posted
 * from it. A refusal keeps it open with the typed values and the error, so the reviewer
 * can fix and resubmit.
 */
export function SePersonEditSheet({
  open,
  onOpenChange,
  mode,
  initial,
  roleOptions,
  slot,
  replacesKey,
  result,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  mode: SePersonEditMode;
  initial: SePersonEditInitial;
  roleOptions: readonly SePersonRoleOption[];
  slot: string | null;
  replacesKey: string | null;
  result: SePersonResult;
}) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="flex w-full flex-col data-[side=right]:sm:max-w-3xl">
        <SheetHeader>
          <SheetTitle>{TITLES[mode]}</SheetTitle>
          <SheetDescription>{DESCRIPTIONS[mode]}</SheetDescription>
        </SheetHeader>
        <SePersonEditForm
          mode={mode}
          initial={initial}
          roleOptions={roleOptions}
          slot={slot}
          replacesKey={replacesKey}
          result={result}
          onCancel={() => onOpenChange(false)}
        />
      </SheetContent>
    </Sheet>
  );
}
