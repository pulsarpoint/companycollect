/**
 * The shared filter vocabulary of the SE correction ledgers, client-safe.
 *
 * The company-INFO ledger itself is gone: reviewer decisions on a company's
 * description are values now, not corrections (see se-info-field-values.ts).
 * What survives here is the vocabulary the ADDRESS ledger's list page and
 * filter sheet still filter by -- kinds and statuses -- which the info list
 * page named first and both pages import from one place rather than keeping
 * two copies.
 */

export const SE_INFO_CORRECTION_KINDS = [
  "override_field",
  "approve_suggestion",
  "reject_suggestion",
  "undo",
] as const;

export type SeInfoCorrectionKind = (typeof SE_INFO_CORRECTION_KINDS)[number];

/**
 * A correction ledger row's status relative to the published row, as the
 * corrections list computes it in SQL (`CORRECTION_STATUS_EXPR` in
 * se-company-address-lists.server.ts / se-company-info-lists.server.ts).
 * Declared here (client-safe) rather than in those `.server` modules so the
 * list's `<Select>` filter can import the value set directly instead of
 * keeping a second copy.
 */
export const SE_INFO_CORRECTION_STATUSES = [
  "pending",
  "applied",
  "stale",
  "undone",
] as const;

export type SeInfoCorrectionStatus = (typeof SE_INFO_CORRECTION_STATUSES)[number];
