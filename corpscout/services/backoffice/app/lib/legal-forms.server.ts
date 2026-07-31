import { chQuery } from "~/lib/clickhouse.server";
import type { CountryConfig } from "~/lib/countries";

/**
 * Legal-form codes decoded to the register's own wording.
 *
 * Sweden's company list showed `51`, `61`, `E-ORGFO` in its Legal form column,
 * because se_companies stores only the code — unlike Norway and Finland, whose
 * registers carry a description column the list can read directly.
 *
 * company_entity_types already holds the decoding, keyed on (country, code),
 * and covers 3,407,806 of Sweden's 3,407,809 companies. Its `source_label` is
 * the register's own term — Bankaktiebolag, Fysisk person / enskild
 * näringsidkare — which is what "legal form" means. `entity_type_label` is the
 * normalised category (Company, Sole trader) and answers a different question,
 * the one the entity badge on a company page already answers.
 *
 * A few dozen rows per country, so it is fetched once per process rather than
 * joined into the list query: joining a dimension this small into the shared
 * company search would put a JOIN in every country's hot path to save nothing.
 */

export type LegalFormLabel = {
  /** English, once the translator has been round. Empty until then. */
  en: string;
  /** The register's own term, always present. */
  original: string;
};

/**
 * How long a country's decoding is held before it is read again.
 *
 * Not a performance knob — a correctness one. This is a dimension a Dagster
 * asset rewrites, and caching it for the life of the process meant Sweden went
 * on rendering "Aktiebolag" after the curated English had already landed in
 * ClickHouse: the only cure was restarting a dev server that had been up for
 * three weeks. A few dozen rows per country makes the re-read free, so the
 * window is short enough that a translation load shows up on its own.
 */
const TTL_MS = 5 * 60 * 1000;

type Entry = {
  fetchedAt: number;
  labels: Promise<Map<string, LegalFormLabel>>;
};

const cache = new Map<string, Entry>();

/** Test seam: drop everything held, so a case can start from a cold process. */
export function __resetLegalFormCache(): void {
  cache.clear();
}

/**
 * English primary, original paired — the same rule every other field here
 * follows. Until company_entity_types_translation_load has run, `en` is empty
 * and the caller falls back to the register's wording, which is what the
 * column showed before and is never wrong, only untranslated.
 */
export function getLegalFormLabels(
  country: CountryConfig,
): Promise<Map<string, LegalFormLabel>> {
  const code = country.code.toUpperCase();
  const cached = cache.get(code);
  if (cached && Date.now() - cached.fetchedAt < TTL_MS) return cached.labels;

  // Most countries share company_entity_types, which is country-scoped. A
  // country with its own dimension table (France's INSEE nomenclature) has no
  // country_code column, so it must not be filtered by one.
  const lookup = country.legalFormLookup;
  const [sql, params] = lookup
    ? [
        // Qualified with `d.` throughout. The dimension views name their
        // columns `label` and `label_en`, which are also the aliases assigned
        // here — unqualified, ClickHouse reads the WHERE as referring to
        // `any(label) AS label` and rejects the query outright.
        `SELECT d.${lookup.codeColumn} AS legal_form_code,
                any(d.${lookup.labelColumn}) AS label,
                any(d.${lookup.enColumn}) AS label_en
         FROM ${lookup.table} AS d
         WHERE d.${lookup.labelColumn} != ''
         GROUP BY d.${lookup.codeColumn}`,
        {},
      ]
    : [
        `SELECT legal_form_code,
                any(source_label) AS label,
                any(source_label_en) AS label_en
         FROM company_entity_types_translated
         WHERE country_code = {country:String} AND source_label != ''
         GROUP BY legal_form_code`,
        { country: code },
      ];

  const pending = chQuery<{ legal_form_code: string; label: string; label_en: string }>(
    sql,
    params,
  ).then(
    (rows) =>
      new Map(
        rows.map((r) => [r.legal_form_code, { en: r.label_en, original: r.label }]),
      ),
  );

  const entry: Entry = { fetchedAt: Date.now(), labels: pending };
  cache.set(code, entry);
  // A failure must not be held for the TTL, and must not evict a newer entry
  // that replaced it in the meantime.
  pending.catch(() => {
    if (cache.get(code) === entry) cache.delete(code);
  });
  return pending;
}

/**
 * The label for a code, with the one fallback INSEE's numbering requires.
 *
 * Sirene records some units at INSEE's level II, written as four digits with
 * trailing zeros: 28,520 French companies carry '2200', which is level II's
 * '22'. Those would otherwise render as a bare number despite the nomenclature
 * naming them perfectly well.
 *
 * The trailing zeros are what license the fallback, and nothing else does.
 * '5498' is a level-III code that simply is not in the nomenclature — cutting
 * it to '54' would state that the company is a plain SARL on no evidence at
 * all, which is the same class of mistake as machine-translating a legal form.
 */
export function lookupLegalForm(
  labels: Map<string, LegalFormLabel>,
  code: string,
  paddedParentFallback = false,
): LegalFormLabel | undefined {
  const exact = labels.get(code);
  if (exact) return exact;
  if (paddedParentFallback && code.length === 4 && code.endsWith("00")) {
    return labels.get(code.slice(0, 2));
  }
  return undefined;
}
