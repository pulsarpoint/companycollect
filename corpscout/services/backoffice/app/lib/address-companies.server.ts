import { chQuery } from "~/lib/clickhouse.server";

export interface AddressCompanyMatch {
  company_id: string;
  company_name: string;
  status: string;
}

export interface SameAddressCompaniesResult {
  companies: AddressCompanyMatch[];
  truncated: boolean;
}

/**
 * The published SE address entity (spec 2026-09-06, section 3.3). Slice 4b
 * renames the table, so the lookup names it exactly once.
 */
const ADDRESS_TABLE = "corpscout.se_company_address_v2";

/**
 * Companies at the same building. The entity stores parsed components, so the
 * street key the old chain had to rebuild with two regexes is now the
 * `street_name` and `house_number` pair; `unit` is deliberately left out, since
 * a different floor is the same building. The target is the company's primary
 * published address (visiting before postal, a matched building before a
 * fallback) among the ones that name a street: a PO box and a postcode-only
 * address identify no building, so neither can anchor the lookup.
 */
const SWEDEN_SAME_BUILDING_QUERY = `WITH
  target_address AS (
    SELECT
      ifNull(target.street_name, '') AS street_name,
      ifNull(target.house_number, '') AS house_number,
      ifNull(target.postal_code, '') AS postal_code,
      toString(target.country_code) AS country_code
    FROM ${ADDRESS_TABLE} AS target FINAL
    PREWHERE target.company_id = {id:String}
    WHERE target.active = 1
      AND ifNull(target.box, '') = ''
      AND ifNull(target.street_name, '') != ''
    ORDER BY
      has(target.kinds, 'visiting_or_postal') DESC,
      has(target.kinds, 'visiting') DESC,
      target.geocode_precision = 'building' DESC,
      target.address_key
    LIMIT 1
  ),
  matching_company_ids AS (
    SELECT DISTINCT company_id
    FROM ${ADDRESS_TABLE} FINAL
    WHERE active = 1
      AND ifNull(street_name, '') = (SELECT street_name FROM target_address)
      AND ifNull(house_number, '') = (SELECT house_number FROM target_address)
      AND ifNull(postal_code, '') = (SELECT postal_code FROM target_address)
      AND toString(country_code) = (SELECT country_code FROM target_address)
  )
SELECT
  toString(registration_number) AS company_id,
  coalesce(legal_name, '') AS company_name,
  status AS status
FROM corpscout.se_companies
WHERE company_id IN matching_company_ids
  AND registration_number != {id:String}
ORDER BY lowerUTF8(company_name), registration_number
LIMIT 51`;

export async function getSwedenCompaniesAtSameBuilding(
  registrationNumber: string,
): Promise<SameAddressCompaniesResult> {
  const rows = await chQuery<AddressCompanyMatch>(SWEDEN_SAME_BUILDING_QUERY, {
    id: registrationNumber,
  });
  return {
    companies: rows.slice(0, 50),
    truncated: rows.length > 50,
  };
}
