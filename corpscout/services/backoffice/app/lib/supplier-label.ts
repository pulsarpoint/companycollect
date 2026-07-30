/**
 * How an awarded supplier is presented when it did not resolve to a company.
 *
 * `{country}_government_contracts` is company-keyed and ends
 * `WHERE company_match_status = 'exact' AND company_id != ''`, which made ~15,400
 * real awards unreachable from the UI. The `*_government_contract_awards` views
 * carry them, and these helpers turn the two extra columns
 * (`winner_match_status`, `winner_registered_id`) into something a reader can act
 * on.
 *
 * Client-safe: no `.server` imports, so route components may use it directly.
 */

/**
 * Four distinct situations, not one.
 *
 * "External" would be wrong for all of them: a Brazilian natural person is not
 * external, and `unmatched_company` is OUR matcher failing rather than anything
 * about the supplier. Distinguishing a foreign winner (3,955 rows, the case this
 * platform most exists to surface) from a bad id (~5,250) is the entire value of
 * the column.
 *
 * An unrecognised status renders raw. A new matcher verdict must show its own
 * word rather than be silently folded into an existing bucket.
 */
const SUPPLIER_STATUS_LABELS: Record<string, string> = {
  foreign_winner: "Foreign",
  natural_person: "Individual",
  unmatched_company: "Unmatched",
  invalid_supplier_id: "Unverified id",
  missing_supplier_id: "Unverified id",
  unknown_person_type: "Unverified id",
  invalid_identifier: "Unverified id",
};

export function supplierStatusLabel(status: string | null | undefined): string | null {
  if (!status || status === "exact") return null;
  return SUPPLIER_STATUS_LABELS[status] ?? status;
}

/**
 * Brazilian CPFs are masked for display; everything is still stored verbatim.
 *
 * PNCP publishes 2,733 unmasked 11-digit CPFs. RFB, the other Brazilian source
 * here, masks its own as `***XXXXXX**` and this project committed to that posture
 * for Socios — purpose-linked retention and an explicit no-de-anonymisation
 * non-goal. Mirroring a public register is not a reason to become a second
 * publisher of individuals' tax numbers, so the same mask is applied here and the
 * format is copied so the two sources read alike.
 *
 * Masked by SHAPE rather than by match status: 2,696 CPFs arrive as
 * `natural_person` but another 37 are filed as `invalid_supplier_id`, because
 * PNCP misclassified the person. A status-driven rule would publish those in
 * full.
 *
 * Scoped to Brazil because 11 digits is what makes an id a CPF. Norwegian org
 * numbers are 9 digits, Estonian 8, Swedish 10 — none are personal, and a blanket
 * length rule would mask company ids in other countries.
 */
export function maskPersonalSupplierId(id: string, countryCode: string): string {
  if (countryCode.toLowerCase() !== "br") return id;
  if (!/^\d{11}$/.test(id)) return id;
  return `***${id.slice(3, 9)}**`;
}

/**
 * "1/3" rather than "+2".
 *
 * The list shows one supplier per contract; this says which of how many it is.
 * The previous "+2" reported how many were hidden, which tells a reader there is
 * more without telling them where what they are looking at sits.
 *
 * Safe beside the amount column because that figure is `sum(value_amount_original)`
 * across all of a contract's suppliers — the contract total, not the named
 * supplier's share — so name and number do not contradict each other.
 */
export function supplierPosition(supplierCount: number): string | null {
  return supplierCount > 1 ? `1/${supplierCount}` : null;
}
