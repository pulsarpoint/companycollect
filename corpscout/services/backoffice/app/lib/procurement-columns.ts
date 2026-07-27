/** Which register columns the records TABLE hides. The record detail page
 * deliberately shows everything a register publishes; the table is for
 * scanning, so load plumbing, FX bookkeeping, and the estimated/framework
 * value variants move to the detail page only. One shared rule set — every
 * register (TED, Doffin, Hilma, PNCP, UHM) inherits it. */

const HIDDEN_EXACT = new Set([
  "source_slug",
  "source_run_id",
  "partition_key",
  "resolved_at",
]);

const HIDDEN_PATTERN = /^fx_|^framework_|estimated_value/;

export function isHiddenTableColumn(name: string): boolean {
  return HIDDEN_EXACT.has(name) || HIDDEN_PATTERN.test(name);
}

export function visibleColumns(columns: string[]): string[] {
  return columns.filter((name) => !isHiddenTableColumn(name));
}

/** Tokens that are not words and must not be sentence-cased into "Usd" or
 * "Cpv". Keyed lowercase; the value is exactly how the token should read. */
const ACRONYMS: Record<string, string> = {
  id: "ID",
  ids: "IDs",
  iso2: "ISO-2",
  url: "URL",
  usd: "USD",
  eur: "EUR",
  nok: "NOK",
  sek: "SEK",
  brl: "BRL",
  fx: "FX",
  cpv: "CPV",
  cnpj: "CNPJ",
  nuts: "NUTS",
  vat: "VAT",
  sme: "SME",
  pncp: "PNCP",
  ted: "TED",
  uhm: "UHM",
  cvr: "CVR",
  doffin: "Doffin",
  hilma: "Hilma",
};

/** Suffixes that read better as a parenthetical than as trailing words.
 *
 * `total_value_amount_usd` is "Total value (USD)", not "Total value amount
 * usd". The `_amount` is dropped with them because it carries nothing once the
 * currency is in brackets — but only in this pairing, so `fx_rate_to_usd`
 * still reads "FX rate to USD" rather than the nonsense "FX rate to (USD)". */
const SUFFIXES: [pattern: RegExp, label: string][] = [
  [/_amount_original$/, "original"],
  [/_amount_usd$/, "USD"],
  [/_raw$/, "raw"],
];

function humanizeToken(token: string): string {
  return ACRONYMS[token] ?? token;
}

/**
 * A column name as a person should read it: `buyer_national_id_raw` becomes
 * "Buyer national ID (raw)".
 *
 * Derived rather than a hand-written map on purpose. These pages read whatever
 * columns a register happens to publish, discovered from `system.columns`, so a
 * map would silently fall back to the raw name the moment a source added a
 * field — which is exactly the state this fixes. Non-English column names
 * (PNCP's `numero_controle_pncp`) pass through with their casing tidied rather
 * than being translated, because inventing an English name for a Brazilian
 * field would make it unmatchable against the register.
 */
export function columnLabel(name: string): string {
  let stem = name;
  let suffix = "";
  for (const [pattern, label] of SUFFIXES) {
    if (pattern.test(stem)) {
      stem = stem.replace(pattern, "");
      suffix = ` (${label})`;
      break;
    }
  }

  const tokens = stem.split("_").filter((token) => token !== "");
  if (tokens.length === 0) return name;

  const words = tokens.map(humanizeToken);
  // Sentence case: the first word is capitalised unless it is an acronym that
  // already carries its own casing.
  const [first, ...rest] = words;
  const head =
    first === tokens[0] ? first.charAt(0).toUpperCase() + first.slice(1) : first;

  return [head, ...rest].join(" ") + suffix;
}
