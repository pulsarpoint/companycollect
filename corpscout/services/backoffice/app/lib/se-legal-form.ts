/**
 * How a Swedish legal form reads across the admin company area.
 *
 * `legal_form_code` mixes two registers' code systems -- Bolagsverket
 * organisationsform text codes (`AB-ORGFO`) and SCB juridisk-form numbers
 * (`10`, `71`) -- so the code alone tells a reviewer nothing. The curated
 * corpscout.se_code_labels dictionary names every code in use in the official
 * Swedish and in English, Dagster copies both onto the published row
 * (migration 000306), and both are shown: the Swedish name is the term the
 * register itself uses and the English one is the gloss beside it.
 *
 * Client-safe (no `.server` import): the list table, the company header, the
 * review workspace and the filter sheet all render from these.
 */

/** A legal-form code and what the dictionary calls it. */
export interface LegalFormLabels {
  code: string;
  label_sv: string;
  label_en: string;
}

/**
 * The Swedish name, or the code itself when the dictionary does not name it.
 *
 * An unlabelled code is not an error: a register value the curation has not
 * caught up with still has to be readable, and the raw code is the honest
 * thing to show for it.
 */
export function legalFormPrimary(form: LegalFormLabels): string {
  return form.label_sv === "" ? form.code : form.label_sv;
}

/**
 * One filter option's text: `Aktiebolag — Limited company (aktiebolag)
 * (AB-ORGFO)`.
 *
 * Every part that exists is shown, and the code is always last: a dropdown
 * item has no `title` to fall back on, and two forms can read alike in one
 * language (`SB-ORGFO` and SCB's `93` are both "Sparbank"), so the code is
 * what tells them apart. `''` is the "no legal form code at all" option, which
 * has neither a code nor labels to show.
 */
export function legalFormOptionLabel(option: LegalFormLabels): string {
  if (option.code === "") return "(none)";
  const parts = [option.label_sv, option.label_en].filter((part) => part !== "");
  return parts.length === 0
    ? option.code
    : `${parts.join(" — ")} (${option.code})`;
}
