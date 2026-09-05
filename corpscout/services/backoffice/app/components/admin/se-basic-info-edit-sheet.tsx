import { Form } from "react-router";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Textarea } from "~/components/ui/textarea";
import {
  Sheet,
  SheetContent,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import {
  BASIC_INFO_LANGUAGES,
  BASIC_INFO_STATUSES,
  basicInfoFieldLabel,
  basicInfoSourceLabel,
  type SeBasicInfoField,
} from "~/lib/se-basic-info-fields";
import { legalFormOptionLabel } from "~/lib/se-legal-form";
import type {
  SeBasicInfoDetail,
  SeBasicInfoRow,
  SeBasicInfoSuggestionRow,
} from "~/lib/se-basic-info.server";

/**
 * Slice 3c's edit sheet: types a reviewer draft for one field. The form only
 * ever sends the fields `parseSeBasicInfoDecision` reads for `intent=edit` --
 * `field`, `value`, `language` (description only), `note` -- plus the hidden
 * `legal_form_codes` the client-safe parser validates `legal_form_code`
 * against; every other rule (length, pattern, calendar range) lives in that
 * parser, not here.
 *
 * `SeBasicInfoEditForm` is the exported, portal-free body (like
 * `DecisionDialogBody`): a Base UI `Sheet.Title` needs the Sheet root's
 * context to render at all, so the sheet's header lives in
 * `SeBasicInfoEditSheet` alone and the form itself never uses it -- this is
 * what lets a test render the form directly, outside any `Sheet`.
 */

const NATIVE_SELECT_CLASSNAME =
  "h-8 w-full rounded-lg border border-input bg-transparent px-2.5 text-sm";

function valueOf(
  row: SeBasicInfoRow | SeBasicInfoSuggestionRow | null,
  field: SeBasicInfoField,
): string {
  return row ? row[field] : "";
}

function sourceOf(info: SeBasicInfoRow | null, field: SeBasicInfoField): string {
  return info ? info[`${field}_source`] : "";
}

/** The draft's own `description_language` when it has a description value,
 * else the published language when it is a legal one, else `en` (spec's
 * default for a company the fold has never touched). */
function defaultLanguage(detail: SeBasicInfoDetail, draft: SeBasicInfoSuggestionRow | null): string {
  if (draft && draft.description !== "") return draft.description_language;
  const published = detail.info?.description_language ?? "";
  return published === "en" || published === "sv" ? published : "en";
}

function ValueControl({
  field,
  detail,
  defaultValue,
}: {
  field: SeBasicInfoField;
  detail: SeBasicInfoDetail;
  defaultValue: string;
}) {
  const label = basicInfoFieldLabel(field);
  switch (field) {
    case "legal_name":
    case "lei":
    case "wikidata_id":
      return <Input name="value" defaultValue={defaultValue} aria-label={label} required />;
    case "legal_form_code":
      return (
        <select
          name="value"
          defaultValue={defaultValue}
          aria-label={label}
          required
          className={NATIVE_SELECT_CLASSNAME}
        >
          {detail.legalFormOptions.map((option) => (
            <option key={option.code} value={option.code}>
              {legalFormOptionLabel(option)}
            </option>
          ))}
        </select>
      );
    case "status":
      return (
        <select
          name="value"
          defaultValue={defaultValue}
          aria-label={label}
          required
          className={NATIVE_SELECT_CLASSNAME}
        >
          {BASIC_INFO_STATUSES.map((status) => (
            <option key={status} value={status}>
              {status === "active" ? "Active" : "Inactive"}
            </option>
          ))}
        </select>
      );
    case "incorporation_date":
      return (
        <Input type="date" name="value" defaultValue={defaultValue} aria-label={label} required />
      );
    case "description":
    case "description_sv":
      return (
        <Textarea name="value" rows={8} defaultValue={defaultValue} aria-label={label} required />
      );
  }
}

function LanguageControl({ defaultValue }: { defaultValue: string }) {
  return (
    <select
      name="language"
      defaultValue={defaultValue}
      aria-label="Language"
      required
      className={NATIVE_SELECT_CLASSNAME}
    >
      {BASIC_INFO_LANGUAGES.map((language) => (
        <option key={language} value={language}>
          {language === "en" ? "English" : "Swedish"}
        </option>
      ))}
    </select>
  );
}

/**
 * The sheet's form body, portal-free so a test can render it directly: the
 * published value and (when there is one) the draft value, the field's own
 * control, the optional note, and the footer. `key={field}` on the `<Form>`
 * is what makes switching the edited field re-run every `defaultValue` --
 * without it React would keep reusing the same uncontrolled inputs and never
 * re-prefill them.
 */
export function SeBasicInfoEditForm({
  companyId,
  field,
  detail,
  busy,
  onCancel,
  error,
}: {
  companyId: string;
  field: SeBasicInfoField;
  detail: SeBasicInfoDetail;
  busy: boolean;
  onCancel: () => void;
  error?: string;
}) {
  const draft = detail.suggestions.find((row) => row.source === "reviewer_draft") ?? null;
  const draftValue = valueOf(draft, field);
  const publishedValue = valueOf(detail.info, field);
  const publishedSource = sourceOf(detail.info, field);
  const prefill = draftValue !== "" ? draftValue : publishedValue;
  const language = defaultLanguage(detail, draft);
  const legalFormCodes = detail.legalFormOptions.map((option) => option.code).join(",");
  return (
    <Form
      method="post"
      key={field}
      data-company-id={companyId}
      className="flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto px-4 pb-4"
    >
      <p className="text-sm">
        Published now:{" "}
        {publishedValue === "" && publishedSource === ""
          ? "(none)"
          : `${publishedValue} (${basicInfoSourceLabel(publishedSource)})`}
      </p>
      {draftValue === "" ? null : <p className="text-sm">Draft: {draftValue}</p>}
      <input type="hidden" name="intent" value="edit" />
      <input type="hidden" name="field" value={field} />
      <input type="hidden" name="legal_form_codes" value={legalFormCodes} />
      <ValueControl field={field} detail={detail} defaultValue={prefill} />
      {field === "description" ? <LanguageControl defaultValue={language} /> : null}
      <Input name="note" placeholder="Note (optional)" maxLength={500} />
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
 * The sheet chrome around `SeBasicInfoEditForm`. Controlled by the workspace:
 * `field` is the field being edited (or `null` when the sheet has nothing to
 * show, in which case this renders nothing at all).
 */
export function SeBasicInfoEditSheet({
  companyId,
  field,
  detail,
  open,
  onOpenChange,
  busy,
  error,
}: {
  companyId: string;
  field: SeBasicInfoField | null;
  detail: SeBasicInfoDetail;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  busy: boolean;
  error?: string;
}) {
  if (field === null) return null;
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="flex w-full flex-col sm:max-w-sm">
        <SheetHeader>
          <SheetTitle>Edit {basicInfoFieldLabel(field)}</SheetTitle>
        </SheetHeader>
        <SeBasicInfoEditForm
          companyId={companyId}
          field={field}
          detail={detail}
          busy={busy}
          onCancel={() => onOpenChange(false)}
          error={error}
        />
      </SheetContent>
    </Sheet>
  );
}
