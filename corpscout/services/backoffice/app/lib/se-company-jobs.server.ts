import { chQuery } from "~/lib/clickhouse.server";

/**
 * One active interval of one job ad, as corpscout.company_job_history holds
 * it. `active_to` is '' while the interval is open-ended; `is_open` is
 * computed at load time (see loadSeCompanyJobs) rather than stored.
 *
 * The list row deliberately excludes description_text_original: the full ad
 * text is large (ZSTD(6) in storage for a reason) and belongs to the per-ad
 * detail read only.
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
  occupation_label: string;
  municipality_name: string;
  region_name: string;
  employment_type_label: string;
  working_hours_label: string;
  number_of_vacancies: number;
  webpage_url: string;
  /** 1 when the ad is in company_job_current or its interval has no end. */
  is_open: number;
}

/**
 * company_job_history is a plain MergeTree snapshot rebuilt per pipeline run,
 * so no FINAL. 'SE' is a literal because this is the Sweden admin area; the
 * company id, which a request supplies, stays a named parameter.
 *
 * occupation_label falls back to the occupation GROUP label because archive
 * eras differ in which taxonomy level they filled in.
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
  h.headline_original AS headline_original,
  if(h.occupation_label_original != '',
     toString(h.occupation_label_original),
     toString(h.occupation_group_label_original)) AS occupation_label,
  toString(h.municipality_name_original) AS municipality_name,
  toString(h.region_name_original) AS region_name,
  toString(h.employment_type_label_original) AS employment_type_label,
  toString(h.working_hours_label_original) AS working_hours_label,
  toUInt32(ifNull(h.number_of_vacancies, 0)) AS number_of_vacancies,
  h.webpage_url AS webpage_url
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

/** The latest-version salary/scope/address/application facts of one ad, from
 * the raw Platsbanken version table. Null when the ad has no version row
 * (the history pipeline can outrun a raw backfill). */
export interface SeCompanyJobAdExtras {
  salary_type_label: string;
  salary_description: string;
  scope_min: number | null;
  scope_max: number | null;
  /** Nullable(UInt8): 1 / 0 when Platsbanken stated it, null when unknown. */
  experience_required: number | null;
  driving_license_required: number | null;
  access_to_own_car: number | null;
  employer_workplace: string;
  street_address: string;
  postcode: string;
  city: string;
  application_email: string;
  application_url: string;
  application_information: string;
}

/** One structured requirement of the ad's latest version. requirement_level
 * is 'must_have' | 'nice_to_have'; requirement_type is one of skill,
 * language, work_experience, education, education_level, driving_license
 * (verified against live data 2026-08-29). */
export interface SeCompanyJobAdRequirement {
  requirement_level: string;
  requirement_type: string;
  label_original: string;
  weight: number | null;
}

/** One recruiting contact of the ad's latest version. contact_type is free
 * text from the employer (usually a role title like 'VD'), not a vocabulary. */
export interface SeCompanyJobAdContact {
  contact_index: number;
  name: string;
  description: string;
  email: string;
  telephone: string;
  contact_type: string;
}

export interface SeCompanyJobAdDetail {
  source_job_ad_id: string;
  headline_original: string;
  description_text_original: string;
  detected_language: string;
  webpage_url: string;
  extras: SeCompanyJobAdExtras | null;
  requirements: SeCompanyJobAdRequirement[];
  contacts: SeCompanyJobAdContact[];
}

/**
 * The ownership gate AND the description read in one statement: keyed by
 * (country, company, ad), so an `?ad=` pointing at another company's ad
 * simply matches nothing and the detail stays null. Latest interval wins
 * when a republished ad has several. Plain MergeTree snapshot -- no FINAL.
 */
export const COMPANY_JOB_AD_SQL = `SELECT
  h.source_job_ad_id AS source_job_ad_id,
  h.headline_original AS headline_original,
  h.description_text_original AS description_text_original,
  toString(h.detected_language) AS detected_language,
  h.webpage_url AS webpage_url
FROM corpscout.company_job_history AS h
WHERE h.country_code = 'SE'
  AND h.company_id = {companyId:String}
  AND h.source_job_ad_id = {adId:String}
ORDER BY h.interval_number DESC
LIMIT 1`;

/**
 * The ad's latest content version. se_platsbanken_job_ad_versions is a
 * ReplacingMergeTree(ingested_at) whose sorting key INCLUDES version_at, so
 * FINAL only collapses re-ingests of the same version -- "latest version"
 * still has to be ORDER BY version_at DESC. The ingested_at tiebreak makes
 * the read deterministic without FINAL's dedup pass.
 */
export const COMPANY_JOB_AD_VERSION_SQL = `SELECT
  toString(v.version_uid) AS version_uid,
  toString(v.salary_type_label_original) AS salary_type_label,
  v.salary_description_original AS salary_description,
  v.scope_min AS scope_min,
  v.scope_max AS scope_max,
  v.experience_required AS experience_required,
  v.driving_license_required AS driving_license_required,
  v.access_to_own_car AS access_to_own_car,
  v.employer_workplace AS employer_workplace,
  v.street_address AS street_address,
  v.postcode AS postcode,
  v.city AS city,
  v.application_email AS application_email,
  v.application_url AS application_url,
  v.application_information AS application_information
FROM corpscout.se_platsbanken_job_ad_versions AS v
WHERE v.source_job_ad_id = {adId:String}
ORDER BY v.version_at DESC, v.ingested_at DESC
LIMIT 1`;

/**
 * Requirements of ONE version. The Replacing sorting key ends in
 * requirement_uid, so FINAL is what folds a re-ingested requirement into one
 * row -- unlike the version read above, this returns many rows and needs it.
 */
export const COMPANY_JOB_AD_REQUIREMENTS_SQL = `SELECT
  toString(r.requirement_level) AS requirement_level,
  toString(r.requirement_type) AS requirement_type,
  toString(r.label_original) AS label_original,
  r.weight AS weight
FROM corpscout.se_platsbanken_job_ad_requirement_versions AS r FINAL
WHERE r.source_job_ad_id = {adId:String}
  AND r.version_uid = {versionUid:String}
ORDER BY r.requirement_level, r.requirement_type, r.label_original
LIMIT 200`;

/** Contacts of ONE version; same Replacing/FINAL reasoning as requirements. */
export const COMPANY_JOB_AD_CONTACTS_SQL = `SELECT
  toUInt16(c.contact_index) AS contact_index,
  c.name AS name,
  c.description AS description,
  c.email AS email,
  c.telephone AS telephone,
  toString(c.contact_type) AS contact_type
FROM corpscout.se_platsbanken_job_ad_contact_versions AS c FINAL
WHERE c.source_job_ad_id = {adId:String}
  AND c.version_uid = {versionUid:String}
ORDER BY c.contact_index
LIMIT 50`;

interface JobAdHistoryRow {
  source_job_ad_id: string;
  headline_original: string;
  description_text_original: string;
  detected_language: string;
  webpage_url: string;
}

interface JobAdVersionRow extends SeCompanyJobAdExtras {
  version_uid: string;
}

/**
 * The full detail of one ad the company owns: description from the history
 * snapshot, plus the latest raw version's extras, requirements and contacts.
 * Null when the ad id is not among this company's rows -- the raw tables are
 * only consulted AFTER the keyed history read has vouched for the id.
 */
export async function loadSeCompanyJobAdDetail(
  companyId: string,
  adId: string,
): Promise<SeCompanyJobAdDetail | null> {
  const [ad] = await chQuery<JobAdHistoryRow>(COMPANY_JOB_AD_SQL, {
    companyId,
    adId,
  });
  if (!ad) return null;

  const [version] = await chQuery<JobAdVersionRow>(COMPANY_JOB_AD_VERSION_SQL, {
    adId,
  });
  if (!version) {
    return { ...ad, extras: null, requirements: [], contacts: [] };
  }

  const { version_uid, ...extras } = version;
  const [requirements, contacts] = await Promise.all([
    chQuery<SeCompanyJobAdRequirement>(COMPANY_JOB_AD_REQUIREMENTS_SQL, {
      adId,
      versionUid: version_uid,
    }),
    chQuery<SeCompanyJobAdContact>(COMPANY_JOB_AD_CONTACTS_SQL, {
      adId,
      versionUid: version_uid,
    }),
  ]);
  return { ...ad, extras, requirements, contacts };
}
