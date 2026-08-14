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

const SWEDEN_SAME_BUILDING_QUERY = `WITH
  target_address AS (
    SELECT
      replaceRegexpAll(
        replaceRegexpAll(
          lowerUTF8(address.street_address),
          '[, ]+[0-9]+ +(tr|trappor?)$',
          ''
        ),
        '[^\\p{L}\\p{N}]+',
        ''
      ) AS building_street_key,
      replaceRegexpAll(address.postal_code, '[^0-9]', '') AS postal_key,
      CAST(address.country_code, 'Nullable(String)') AS country_code
    FROM se_company_addresses_serving_current AS link
    INNER JOIN se_addresses_current AS address USING (address_id)
    INNER JOIN se_address_geocodes_current AS geocode USING (address_id)
    PREWHERE link.company_id = {id:String}
    WHERE address.address_kind = 'physical'
    ORDER BY
      has(link.address_types, 'visiting_or_postal') DESC,
      has(link.address_types, 'visiting') DESC,
      geocode.geocode_precision = 'building' DESC,
      link.address_id
    LIMIT 1
  ),
  matching_address_ids AS (
    SELECT address_id
    FROM se_addresses_current
    WHERE replaceRegexpAll(
      replaceRegexpAll(
        lowerUTF8(street_address),
        '[, ]+[0-9]+ +(tr|trappor?)$',
        ''
      ),
      '[^\\p{L}\\p{N}]+',
      ''
    ) = (SELECT building_street_key FROM target_address)
      AND replaceRegexpAll(postal_code, '[^0-9]', '') =
        (SELECT postal_key FROM target_address)
      AND country_code = (SELECT country_code FROM target_address)
  ),
  matching_company_ids AS (
    SELECT company_id
    FROM se_company_address_links_current
    PREWHERE address_id IN (SELECT address_id FROM matching_address_ids)
  )
SELECT
  toString(registration_number) AS company_id,
  coalesce(legal_name, '') AS company_name,
  status AS status
FROM se_companies
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
