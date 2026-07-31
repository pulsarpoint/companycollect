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

const cache = new Map<string, Promise<Map<string, string>>>();

export function getLegalFormLabels(
  country: CountryConfig,
): Promise<Map<string, string>> {
  const code = country.code.toUpperCase();
  const cached = cache.get(code);
  if (cached) return cached;

  const pending = chQuery<{ legal_form_code: string; label: string }>(
    `SELECT legal_form_code, any(source_label) AS label
     FROM company_entity_types
     WHERE country_code = {country:String} AND source_label != ''
     GROUP BY legal_form_code`,
    { country: code },
  ).then((rows) => new Map(rows.map((r) => [r.legal_form_code, r.label])));

  cache.set(code, pending);
  pending.catch(() => cache.delete(code));
  return pending;
}
