/**
 * A monetary claim, as an eForms register publishes it.
 *
 * Doffin and TED are the same standard and store money identically — a
 * `<key>_amount_original`, a `<key>_currency`, and a `<key>_amount_usd` filled
 * by the separate FX step — so one helper serves both registers rather than
 * each growing its own copy.
 *
 * Returns null when the register publishes nothing for that business term,
 * which is the common case rather than the exception: TED carries a realized
 * award on 22.6% of Swedish notices and 36% of Finnish ones. A caller drops the
 * row entirely instead of printing a dash, because "this notice does not state
 * BT-720" and "BT-720 is zero" are different facts.
 */

const nf = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });

export type NoticeMoney = { original: string; usd: string | null };

/** A stored value as display text, treating blank and missing alike. */
export function noticeText(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text === "" ? null : text;
}

export function eformsMoney(
  fields: Record<string, unknown>,
  key: string,
): NoticeMoney | null {
  const original = noticeText(fields[`${key}_amount_original`]);
  if (original === null) return null;

  const currency = noticeText(fields[`${key}_currency`]) ?? "";
  const usd = noticeText(fields[`${key}_amount_usd`]);
  const amount = Number(original);

  return {
    // An unparseable figure is shown exactly as stored. Formatting it would
    // print NaN and hide the fact that the register published something odd.
    original: `${Number.isFinite(amount) ? nf.format(amount) : original}${
      currency ? ` ${currency}` : ""
    }`,
    usd: usd !== null && Number.isFinite(Number(usd)) ? `$${nf.format(Number(usd))}` : null,
  };
}
