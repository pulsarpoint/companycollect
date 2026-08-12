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
        upperUTF8(trim(replaceRegexpAll(
          coalesce(nullIf(street_address, ''), raw_address, ''),
          '\\s+',
          ' '
        ))),
        ' [0-9]+ TR$',
        ''
      ) AS street_key,
      replaceRegexpAll(coalesce(postal_code, ''), '[^0-9]', '') AS postal_key,
      upperUTF8(if(
        coalesce(country_code, '') = '' AND lowerUTF8(trim(coalesce(post_town, ''))) != 'utlandet',
        'SE',
        coalesce(country_code, '')
      )) AS country_key
    FROM se_company_addresses_current
    WHERE company_id IN (
      SELECT company_id
      FROM se_companies
      WHERE registration_number = {id:String}
    )
      AND coalesce(nullIf(street_address, ''), raw_address, '') != ''
      AND coalesce(postal_code, '') != ''
      AND has_address = 1
    ORDER BY
      address_type = 'visiting_or_postal' DESC,
      address_type = 'postal' DESC
    LIMIT 1
  ),
  matching_company_ids AS (
    SELECT DISTINCT address.company_id
    FROM se_company_addresses_current AS address
    CROSS JOIN target_address AS target
    WHERE replaceRegexpAll(
      upperUTF8(trim(replaceRegexpAll(
        coalesce(nullIf(address.street_address, ''), address.raw_address, ''),
        '\\s+',
        ' '
      ))),
      ' [0-9]+ TR$',
      ''
    ) = target.street_key
      AND replaceRegexpAll(coalesce(address.postal_code, ''), '[^0-9]', '') = target.postal_key
      AND upperUTF8(if(
        coalesce(address.country_code, '') = ''
          AND lowerUTF8(trim(coalesce(address.post_town, ''))) != 'utlandet',
        'SE',
        coalesce(address.country_code, '')
      )) = target.country_key
      AND address.has_address = 1
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
