import { chQuery } from "~/lib/clickhouse.server";

/** One current LEI linked to the company in corpscout.company_identifier. */
export interface SeCompanyLeiRow {
  lei: string;
  entity_status: string;
  registration_status: string;
}

/** One ESEF annual-report filing published under one of the company's LEIs. */
export interface SeCompanyEsefFilingRow {
  period_end: string;
  fxo_id: string;
  lei: string;
  entity_name: string;
  country: string;
  date_added: string;
  viewer_url: string;
}

/**
 * company_identifier is a plain MergeTree snapshot, but succession can leave
 * several versions of one link, so the statuses are argMax'd by resolved_at
 * per LEI instead of trusting the physical rows to be unique.
 */
export const COMPANY_LEI_SQL = `SELECT
  i.issuer_id AS lei,
  argMax(toString(i.entity_status), i.resolved_at) AS entity_status,
  argMax(toString(i.registration_status), i.resolved_at) AS registration_status
FROM corpscout.company_identifier AS i
WHERE i.issuer_scheme = 'lei'
  AND i.country_code = 'SE'
  AND i.is_current = 1
  AND i.company_id = {companyId:String}
GROUP BY lei
ORDER BY lei
LIMIT 50`;

/**
 * esef_filings is a ReplacingMergeTree on resolved_at, so FINAL: a re-crawled
 * filing must show once, in its newest state. The LEI normalization
 * (upperUTF8/trimBoth) mirrors ESEF_FILINGS_QUERY in queries.server --
 * company_identifier stores uppercase LEIs while filings.xbrl.org's casing
 * is whatever the filer sent.
 */
export const COMPANY_ESEF_FILINGS_SQL = `SELECT
  toString(f.period_end) AS period_end,
  f.fxo_id AS fxo_id,
  upperUTF8(trimBoth(f.lei)) AS lei,
  f.entity_name AS entity_name,
  toString(f.country) AS country,
  toString(f.date_added) AS date_added,
  f.viewer_url AS viewer_url
FROM corpscout.esef_filings AS f FINAL
WHERE upperUTF8(trimBoth(f.lei)) IN (
  SELECT issuer_id
  FROM corpscout.company_identifier
  WHERE issuer_scheme = 'lei'
    AND country_code = 'SE'
    AND is_current = 1
    AND company_id = {companyId:String}
)
ORDER BY f.period_end DESC, f.fxo_id DESC
LIMIT 200`;

export interface SeCompanyListed {
  leis: SeCompanyLeiRow[];
  filings: SeCompanyEsefFilingRow[];
}

/**
 * The company's public-market identity: its current LEI(s) and every ESEF
 * annual financial report filed under them, newest period first. A company
 * with an LEI but no filings is a real state (LEIs are issued for much more
 * than listing), so both slices are returned rather than one implying the
 * other.
 */
export async function loadSeCompanyListed(
  companyId: string,
): Promise<SeCompanyListed> {
  const [leis, filings] = await Promise.all([
    chQuery<SeCompanyLeiRow>(COMPANY_LEI_SQL, { companyId }),
    chQuery<SeCompanyEsefFilingRow>(COMPANY_ESEF_FILINGS_SQL, { companyId }),
  ]);
  return { leis, filings };
}
