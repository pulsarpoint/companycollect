import { chQuery } from "~/lib/clickhouse.server";

/**
 * One active interval of one job ad, as corpscout.company_job_history holds
 * it. `active_to` is '' while the interval is open-ended; `is_open` is
 * computed at load time (see loadSeCompanyJobs) rather than stored.
 */
export interface SeCompanyJobRow {
  source_system: string;
  source_job_ad_id: string;
  interval_number: number;
  active_from: string;
  active_to: string;
  active_to_basis: string;
  is_end_estimated: number;
  publication_at: string;
  application_deadline: string;
  employer_name: string;
  headline_original: string;
  /** 1 when the ad is in company_job_current or its interval has no end. */
  is_open: number;
}

/**
 * company_job_history is a plain MergeTree snapshot rebuilt per pipeline run,
 * so no FINAL. 'SE' is a literal because this is the Sweden admin area; the
 * company id, which a request supplies, stays a named parameter.
 */
export const COMPANY_JOBS_SQL = `SELECT
  toString(h.source_system) AS source_system,
  h.source_job_ad_id AS source_job_ad_id,
  toUInt16(h.interval_number) AS interval_number,
  toString(h.active_from) AS active_from,
  ifNull(toString(h.active_to), '') AS active_to,
  toString(h.active_to_basis) AS active_to_basis,
  toUInt8(h.is_end_estimated) AS is_end_estimated,
  ifNull(toString(h.publication_at), '') AS publication_at,
  ifNull(toString(h.application_deadline), '') AS application_deadline,
  h.employer_name AS employer_name,
  h.headline_original AS headline_original
FROM corpscout.company_job_history AS h
WHERE h.country_code = 'SE' AND h.company_id = {companyId:String}
ORDER BY h.active_from DESC, h.source_job_ad_id, h.interval_number
LIMIT 200`;

/** The currently-open ads, keyed like the history so the two zip together. */
export const COMPANY_JOBS_CURRENT_SQL = `SELECT
  toString(c.source_system) AS source_system,
  c.source_job_ad_id AS source_job_ad_id
FROM corpscout.company_job_current AS c
WHERE c.country_code = 'SE' AND c.company_id = {companyId:String}
LIMIT 500`;

interface CurrentJobKeyRow {
  source_system: string;
  source_job_ad_id: string;
}

/**
 * One Swedish company's job-ad history, newest interval first, with each row
 * marked open when company_job_current still lists the ad or the interval has
 * no recorded end.
 */
export async function loadSeCompanyJobs(
  companyId: string,
): Promise<SeCompanyJobRow[]> {
  const [history, current] = await Promise.all([
    chQuery<Omit<SeCompanyJobRow, "is_open">>(COMPANY_JOBS_SQL, { companyId }),
    chQuery<CurrentJobKeyRow>(COMPANY_JOBS_CURRENT_SQL, { companyId }),
  ]);
  const open = new Set(
    current.map((row) => `${row.source_system}:${row.source_job_ad_id}`),
  );
  return history.map((row) => ({
    ...row,
    is_open:
      open.has(`${row.source_system}:${row.source_job_ad_id}`) ||
      row.active_to === ""
        ? 1
        : 0,
  }));
}
