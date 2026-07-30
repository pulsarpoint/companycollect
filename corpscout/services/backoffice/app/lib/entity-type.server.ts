/**
 * What kind of entity a register row is: a business, or an arm of the state.
 *
 * Brreg and Bolagsverket register legal *entities*, so a municipality sits in
 * the same table as a hairdresser. The classification lives in ClickHouse
 * (`company_entity_types`, keyed on the register's own legal-form code) rather
 * than here, so it is derived once and read everywhere instead of each surface
 * inventing its own rule.
 *
 * The flag answers public **form**, not public **ownership**. `AB Bostaden i
 * Umeå` is a municipally-owned aktiebolag and classifies as a company, which is
 * correct: it is one. Nearly half of Sweden's procurement buyers are like that.
 */
import { chQuery } from "./clickhouse.server";

export interface EntityType {
  entity_type: string;
  entity_type_label: string;
  /** The register's own wording, so a classification can be checked against the
   * source rather than trusted. Swedish codes carry no description upstream, so
   * this is the term the entities carrying the code evidence. */
  source_label: string;
  is_public_sector: number;
}

/** The column each register keeps its legal form code in. Registers that
 * publish no legal form at all are absent, so they resolve to null rather than
 * to a wrong lookup. */
const LEGAL_FORM_COLUMN: Record<string, string> = {
  se: "legal_form_code",
  no: "legal_form_code",
  fi: "legal_form_code",
  fr: "legal_form_code",
  lv: "legal_form_code",
  sk: "legal_form_code",
  // RFB calls it a legal NATURE, and publishes a closed 90-value CONCLA
  // domain. Without this entry legalFormCodeOf returned "" and no Brazilian
  // company ever showed its classification.
  br: "legal_nature_code",
};

/** Reads the code off the detail record, so no extra query is needed to find
 * it.
 *
 * Matched on the unqualified column name rather than the exact key, because a
 * country with a custom `recordQuery` aliases its columns: Sweden joins its
 * translations and so returns `c.legal_form_code`, not `legal_form_code`. An
 * exact lookup silently found nothing there and every Swedish entity came back
 * unclassified — including the state agencies this exists to label. */
export function legalFormCodeOf(
  countryCode: string,
  record: Record<string, unknown>,
): string {
  const column = LEGAL_FORM_COLUMN[countryCode.toLowerCase()];
  if (!column) return "";
  const key =
    column in record
      ? column
      : Object.keys(record).find((name) => name.split(".").pop() === column);
  const value = key === undefined ? null : record[key];
  return typeof value === "string" ? value : value == null ? "" : String(value);
}

export async function getEntityType(
  countryCode: string,
  legalFormCode: string,
): Promise<EntityType | null> {
  if (legalFormCode === "") return null;
  const rows = await chQuery<EntityType>(
    `SELECT entity_type, entity_type_label, source_label, is_public_sector
     FROM company_entity_types FINAL
     WHERE country_code = upper({country:String})
       AND legal_form_code = {code:String}
     LIMIT 1`,
    { country: countryCode, code: legalFormCode },
  );
  return rows[0] ?? null;
}
