import { EMPTY_VALUE } from "~/components/admin/definition-list";
import { legalFormPrimary, type LegalFormLabels } from "~/lib/se-legal-form";

/**
 * A legal form, inline: the official Swedish name with the English gloss muted
 * beside it and the code as the tooltip, so the code stays reachable without
 * eating a table column's width. Renders the company area's shared em dash when
 * the register recorded no legal form code at all.
 *
 * See `~/lib/se-legal-form` for why both languages are shown and what an
 * unlabelled code falls back to.
 */
export function LegalForm({
  form,
  className,
}: {
  form: LegalFormLabels;
  className?: string;
}) {
  if (form.code === "") return EMPTY_VALUE;
  return (
    <span className={className} title={form.code}>
      {legalFormPrimary(form)}
      {form.label_en === "" ? null : (
        <span className="text-muted-foreground ml-1">{form.label_en}</span>
      )}
    </span>
  );
}
