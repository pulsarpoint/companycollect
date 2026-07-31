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

  const pending = chQuery<{ legal_form_code: string; label: string; label_en: string }>(
    `SELECT legal_form_code,
            any(source_label) AS label,
            any(source_label_en) AS label_en
     FROM company_entity_types_translated
     WHERE country_code = {country:String} AND source_label != ''
     GROUP BY legal_form_code`,
    { country: code },
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
