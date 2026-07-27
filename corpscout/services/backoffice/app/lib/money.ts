/** Money detection and formatting for the generic register pages.
 *
 * The procurement source and record pages render whatever columns a register
 * publishes, so monetary fields are recognized by name rather than declared.
 * Across every loaded register the monetary columns are `*_amount_original`,
 * `*_amount_usd`, `*value*` (Hilma, Doffin, TED) or `valor_*` (PNCP), always
 * Decimal(38, 2). Companion columns that share those stems but are not money
 * (`*_currency`, `fx_rate_to_usd`, `notice_value_source_field`) are excluded
 * by the veto pattern, and both gates require the value itself to be numeric.
 */

const MONEY_KEY = /amount|value|price|cost|budget|valor/i;
const NOT_MONEY_KEY = /currency|rate|field|source|code|id|type|count|year/i;
const NUMERIC_TEXT = /^-?\d+(\.\d+)?$/;

const moneyFormat = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });

/** Formats a register field as money, or returns null when the field is not
 * monetary. ClickHouse serializes Decimal columns as strings, so numeric
 * strings are accepted alongside numbers. Values beyond Number's safe integer
 * range are left untouched rather than formatted with silent precision loss. */
export function formatMoneyField(key: string, value: unknown): string | null {
  if (!MONEY_KEY.test(key) || NOT_MONEY_KEY.test(key)) return null;

  let numeric: number;
  if (typeof value === "number") {
    numeric = value;
  } else if (typeof value === "string" && NUMERIC_TEXT.test(value)) {
    numeric = Number(value);
  } else {
    return null;
  }

  if (!Number.isFinite(numeric) || Math.abs(numeric) > Number.MAX_SAFE_INTEGER) {
    return null;
  }
  return moneyFormat.format(numeric);
}
