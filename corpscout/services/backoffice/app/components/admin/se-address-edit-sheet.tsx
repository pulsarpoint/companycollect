import type React from "react";
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
import { addressKindLabel, REVIEWER_KINDS } from "~/lib/se-address-fields";
// Type-only, so no module cycle survives compilation: the workspace owns the
// action-result shape and imports this sheet as a value.
import type { SeAddressResult } from "~/components/admin/se-address-workspace";

/**
 * The Address tab's typing sheet (spec 2026-09-06 section 8): one reviewer
 * draft, in three modes -- `add` (a new slot), `correct` (a new slot carrying
 * the published key it replaces) and `edit-draft` (the slot already drafted).
 *
 * The form only ever sends what `parseSeAddressDecision` reads for
 * `intent=save-draft`: `care_of`, `street_line`, `postal_code`, `city`,
 * `country`, `kind`, `note`, plus the hidden `slot` and `replaces_key`. Every
 * rule (length, five-digit postcode, plain text, Sweden only, a catalogue
 * kind) lives in `validateSeAddressInput`, not here -- the sheet is the typing
 * surface, the parser is the gate.
 *
 * `SeAddressEditForm` is the exported, portal-free body (like the Info tab's
 * `SeBasicInfoEditForm`): a Base UI `Sheet.Title` needs the Sheet root's
 * context to render at all, so the header lives in `SeAddressEditSheet` alone
 * and the form itself never uses it -- which is what lets a test render the
 * form directly, outside any `Sheet`.
 *
 * Nothing here decides when the sheet closes: the sheet is a controlled
 * component and the workspace closes it, once, on the result of the save it
 * just posted. An effect in here would fire on mount instead -- the route's
 * `actionData` outlives the save, so every later Add / Correct / Edit would
 * open onto a stale success and shut immediately.
 */

export type SeAddressEditMode = "add" | "correct" | "edit-draft";

/** What the sheet opens with: a published row's components for a Correct, the
 * draft's own raw columns for an edit, nothing at all for an Add. */
export interface SeAddressEditInitial {
  careOf: string;
  streetLine: string;
  postalCode: string;
  city: string;
  kind: string;
  note: string;
}

/** An Add starts blank, on the commonest reviewer kind. */
export const EMPTY_ADDRESS_INITIAL: SeAddressEditInitial = {
  careOf: "",
  streetLine: "",
  postalCode: "",
  city: "",
  kind: "postal",
  note: "",
};

const NATIVE_SELECT_CLASSNAME =
  "h-8 w-full rounded-lg border border-input bg-transparent px-2.5 text-sm";

/** A source may deliver `workplace` or `unknown`; a reviewer may not type
 * either, so a Correct prefilled from such a row falls back to Postal. */
function reviewerKind(kind: string): string {
  return (REVIEWER_KINDS as readonly string[]).includes(kind) ? kind : "postal";
}

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

const TITLES: Record<SeAddressEditMode, string> = {
  add: "Add address",
  correct: "Correct address",
  "edit-draft": "Edit draft address",
};

const DESCRIPTIONS: Record<SeAddressEditMode, string> = {
  add: "Save draft writes a reviewer draft. Activate publishes it; Fold now parses it.",
  correct:
    "Save draft writes a reviewer draft against this address. Activating it hides the address it replaces.",
  "edit-draft": "Save draft rewrites this draft. Nothing published changes until you activate it.",
};

/**
 * The sheet's form body, portal-free so a test can render it directly. `key`
 * on the `<Form>` is what makes reopening the sheet for another address re-run
 * every `defaultValue` -- without it React keeps reusing the same uncontrolled
 * inputs and never re-prefills them.
 */
export function SeAddressEditForm({
  mode,
  initial,
  slot,
  replacesKey,
  result,
  onCancel,
}: {
  mode: SeAddressEditMode;
  initial: SeAddressEditInitial;
  /** The draft slot being edited; `null` for a new draft (the store stamps one). */
  slot: string | null;
  /** The published key a Correct replaces; `null` otherwise. */
  replacesKey: string | null;
  result: SeAddressResult;
  onCancel: () => void;
}) {
  const navigation = useNavigation();
  const busy =
    navigation.state !== "idle" && (navigation.formMethod ?? "").toUpperCase() === "POST";
  // Only this form's own refusal belongs inside the sheet. A Remove, Reset,
  // Activate or Discard refusal is the workspace's "Not saved" alert to show;
  // an intent-less refusal (an older route, a failure before parsing) is shown
  // in both places rather than nowhere.
  const error =
    result && !result.ok && (result.intent === undefined || result.intent === "save-draft")
      ? result.error
      : undefined;
  return (
    <Form
      method="post"
      key={`${mode}:${slot ?? ""}:${replacesKey ?? ""}`}
      className="flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto px-4 pb-4"
    >
      <input type="hidden" name="intent" value="save-draft" />
      <input type="hidden" name="slot" value={slot ?? ""} />
      <input type="hidden" name="replaces_key" value={replacesKey ?? ""} />
      <Field label="Care of">
        <Input
          name="care_of"
          defaultValue={initial.careOf}
          maxLength={200}
          aria-label="Care of"
          placeholder="c/o Anna Andersson"
        />
      </Field>
      <Field label="Street or box" hint="A street line with its number, or a box.">
        <Input
          name="street_line"
          defaultValue={initial.streetLine}
          maxLength={200}
          aria-label="Street or box"
          placeholder="Storgatan 5 or Box 123"
          required
        />
      </Field>
      <div className="grid gap-4 sm:grid-cols-[10rem_1fr]">
        <Field label="Postcode">
          <Input
            name="postal_code"
            defaultValue={initial.postalCode}
            aria-label="Postcode"
            placeholder="111 22"
            required
          />
        </Field>
        <Field label="City">
          <Input
            name="city"
            defaultValue={initial.city}
            maxLength={100}
            aria-label="City"
            placeholder="Stockholm"
            required
          />
        </Field>
      </div>
      <div className="grid gap-4 sm:grid-cols-[10rem_1fr]">
        <Field label="Country" hint="Swedish addresses only.">
          <Input name="country" value="SE" readOnly aria-label="Country" />
        </Field>
        <Field label="Kind">
          <select
            name="kind"
            defaultValue={reviewerKind(initial.kind)}
            aria-label="Kind"
            required
            className={NATIVE_SELECT_CLASSNAME}
          >
            {REVIEWER_KINDS.map((kind) => (
              <option key={kind} value={kind}>
                {addressKindLabel(kind)}
              </option>
            ))}
          </select>
        </Field>
      </div>
      <Field label="Note">
        <Textarea
          name="note"
          rows={3}
          defaultValue={initial.note}
          maxLength={500}
          aria-label="Note"
          placeholder="Why this address (optional)"
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
 * The sheet chrome around `SeAddressEditForm`, as wide as the Info tab's.
 * Fully controlled: the workspace opens it, and closes it on the result of the
 * save posted from it. A refusal keeps it open with the typed values and the
 * error, so the reviewer can fix and resubmit.
 */
export function SeAddressEditSheet({
  open,
  onOpenChange,
  mode,
  initial,
  slot,
  replacesKey,
  result,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  mode: SeAddressEditMode;
  initial: SeAddressEditInitial;
  slot: string | null;
  replacesKey: string | null;
  result: SeAddressResult;
}) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="flex w-full flex-col data-[side=right]:sm:max-w-3xl">
        <SheetHeader>
          <SheetTitle>{TITLES[mode]}</SheetTitle>
          <SheetDescription>{DESCRIPTIONS[mode]}</SheetDescription>
        </SheetHeader>
        <SeAddressEditForm
          mode={mode}
          initial={initial}
          slot={slot}
          replacesKey={replacesKey}
          result={result}
          onCancel={() => onOpenChange(false)}
        />
      </SheetContent>
    </Sheet>
  );
}
