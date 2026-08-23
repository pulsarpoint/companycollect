import { chQuery, chStreamQuery } from "~/lib/clickhouse.server";

export const SWEDEN_PEOPLE_DRAFT_SOURCES = [
  "bolagsverket",
  "esef",
  "wikidata",
] as const;

export type SwedenPeopleDraftSource =
  (typeof SWEDEN_PEOPLE_DRAFT_SOURCES)[number];

export interface SwedenPeopleDraftSourceObservation {
  company_id: string;
  name: string;
  source: SwedenPeopleDraftSource;
  source_entity_id: string;
  source_record_uid: string;
  role_original: string | null;
  fiscal_year: number | null;
  description: string | null;
  source_profile_hash: string;
  source_role_hash: string;
  source_payload_json: string;
  source_observed_at: string | null;
}

interface SourceCountRow {
  row_count: string;
}

const BOLAGSVERKET_COUNT_QUERY = `SELECT toString(count()) AS row_count
FROM corpscout.se_financial_report_signatories AS signatories
WHERE trim(concat(signatories.first_name, ' ', signatories.last_name)) != ''`;

const BOLAGSVERKET_OBSERVATIONS_QUERY = `SELECT
  signatories.company_id AS company_id,
  trim(concat(signatories.first_name, ' ', signatories.last_name)) AS name,
  'bolagsverket' AS source,
  toString(signatories.signatory_uid) AS source_entity_id,
  toString(signatories.source_record_uid) AS source_record_uid,
  nullIf(trim(signatories.role_original), '') AS role_original,
  if(
    signatories.fiscal_year > 0,
    toNullable(signatories.fiscal_year),
    NULL
  ) AS fiscal_year,
  CAST(NULL, 'Nullable(String)') AS description,
  toString(signatories.person_profile_hash) AS source_profile_hash,
  toString(signatories.person_role_hash) AS source_role_hash,
  toJSONString(CAST(tuple(
    signatories.company_id,
    signatories.fiscal_year,
    signatories.statement_key,
    toString(signatories.source_record_uid),
    toString(signatories.signatory_kind),
    signatories.person_seq,
    toString(signatories.signatory_uid),
    signatories.first_name,
    signatories.last_name,
    signatories.role_original,
    toString(signatories.role_kind),
    signatories.resolved_at
  ) AS Tuple(
    company_id String,
    fiscal_year Int32,
    statement_key String,
    source_record_uid String,
    signatory_kind String,
    person_seq UInt16,
    signatory_uid String,
    first_name String,
    last_name String,
    role_original String,
    role_kind String,
    resolved_at DateTime64(3, 'UTC')
  ))) AS source_payload_json,
  toString(signatories.resolved_at) AS source_observed_at
FROM corpscout.se_financial_report_signatories AS signatories
WHERE trim(concat(signatories.first_name, ' ', signatories.last_name)) != ''`;

const ESEF_COUNT_QUERY = `SELECT toString(count()) AS row_count
FROM corpscout.esef_document_people AS people FINAL
WHERE people.country_code = 'SE'
  AND people.company_id != ''
  AND trim(people.name) != ''`;

const ESEF_OBSERVATIONS_QUERY = `SELECT
  people.company_id AS company_id,
  trim(people.name) AS name,
  'esef' AS source,
  toString(people.candidate_uid) AS source_entity_id,
  toString(people.source_record_uid) AS source_record_uid,
  nullIf(trim(people.role), '') AS role_original,
  toNullable(people.fiscal_year) AS fiscal_year,
  CAST(NULL, 'Nullable(String)') AS description,
  toString(people.person_profile_hash) AS source_profile_hash,
  toString(people.person_role_hash) AS source_role_hash,
  toJSONString(CAST(tuple(
    toString(people.candidate_uid),
    toString(people.source_record_uid),
    people.source_document_id,
    toString(people.country_code),
    people.company_id,
    people.fiscal_year,
    people.name,
    people.role,
    toString(people.role_category),
    people.organization,
    toString(people.status),
    people.effective_from,
    people.effective_to,
    people.confidence,
    people.evidence_ids,
    toString(people.model_provider),
    people.model_name,
    people.prompt_version,
    people.source_run_id,
    people.extracted_at
  ) AS Tuple(
    candidate_uid String,
    source_record_uid String,
    source_document_id String,
    country_code String,
    company_id String,
    fiscal_year UInt16,
    name String,
    role String,
    role_category String,
    organization String,
    status String,
    effective_from Nullable(Date32),
    effective_to Nullable(Date32),
    confidence Float32,
    evidence_ids Array(String),
    model_provider String,
    model_name String,
    prompt_version String,
    source_run_id String,
    extracted_at DateTime64(3, 'UTC')
  ))) AS source_payload_json,
  toString(people.extracted_at) AS source_observed_at
FROM corpscout.esef_document_people AS people FINAL
WHERE people.country_code = 'SE'
  AND people.company_id != ''
  AND trim(people.name) != ''`;

const WIKIDATA_COMPANY_IDS_CTE = `swedish_companies AS (
  SELECT registration_number AS company_id
  FROM corpscout.se_companies FINAL
),
company_leis AS (
  SELECT
    identifiers.company_id,
    upperUTF8(identifiers.issuer_id) AS lei
  FROM corpscout.company_identifier AS identifiers
  INNER JOIN swedish_companies AS companies
    ON companies.company_id = identifiers.company_id
  WHERE identifiers.country_code = 'SE'
    AND identifiers.issuer_scheme = 'lei'
    AND identifiers.is_current = 1
  GROUP BY identifiers.company_id, lei
),
company_wikidata_ids AS (
  SELECT company_id, wikidata_id
  FROM (
    SELECT
      companies.company_id,
      identifiers.wikidata_id
    FROM corpscout.wikidata_company_identifiers AS identifiers FINAL
    INNER JOIN swedish_companies AS companies
      ON companies.company_id = replaceRegexpAll(
        identifiers.identifier_value,
        '[^0-9]',
        ''
      )
    WHERE identifiers.identifier_type = 'se_orgnr'

    UNION ALL

    SELECT
      leis.company_id,
      identifiers.wikidata_id
    FROM corpscout.wikidata_company_identifiers AS identifiers FINAL
    INNER JOIN company_leis AS leis
      ON leis.lei = upperUTF8(identifiers.identifier_value)
    WHERE identifiers.identifier_type = 'lei'
  )
  GROUP BY company_id, wikidata_id
)`;

const WIKIDATA_COUNT_QUERY = `WITH ${WIKIDATA_COMPANY_IDS_CTE}
SELECT toString(uniqExact(tuple(
  company_ids.company_id,
  links.source_record_id,
  persons.person_profile_hash,
  links.person_role_hash
))) AS row_count
FROM company_wikidata_ids AS company_ids
INNER JOIN corpscout.wikidata_company_people AS links FINAL
  ON links.company_wikidata_id = company_ids.wikidata_id
INNER JOIN corpscout.wikidata_persons AS persons FINAL
  ON persons.person_wikidata_id = links.person_wikidata_id
WHERE trim(persons.name) != ''`;

const WIKIDATA_OBSERVATIONS_QUERY = `WITH ${WIKIDATA_COMPANY_IDS_CTE}
SELECT
  company_ids.company_id,
  trim(persons.name) AS name,
  'wikidata' AS source,
  links.source_record_id AS source_entity_id,
  toString(persons.source_record_uid) AS source_record_uid,
  nullIf(trim(toString(links.role_label)), '') AS role_original,
  CAST(NULL, 'Nullable(UInt16)') AS fiscal_year,
  persons.description AS description,
  toString(persons.person_profile_hash) AS source_profile_hash,
  toString(links.person_role_hash) AS source_role_hash,
  toJSONString(CAST(tuple(
    links.company_wikidata_id,
    links.person_wikidata_id,
    toString(links.role_property),
    toString(links.role_label),
    links.start_date,
    links.end_date,
    links.is_current,
    toString(links.source_system),
    links.source_run_id,
    links.source_record_id,
    toString(links.source_payload_hash),
    links.retrieved_at,
    links.resolved_at,
    toString(persons.source_record_uid),
    persons.name,
    persons.description,
    persons.birth_year,
    persons.image_url,
    persons.wikidata_url,
    toString(persons.source_system),
    persons.source_run_id,
    persons.source_record_id,
    toString(persons.source_payload_hash),
    persons.retrieved_at,
    persons.resolved_at
  ) AS Tuple(
    company_wikidata_id String,
    person_wikidata_id String,
    role_property String,
    role_label String,
    start_date Nullable(Date),
    end_date Nullable(Date),
    is_current UInt8,
    link_source_system String,
    link_source_run_id String,
    link_source_record_id String,
    link_source_payload_hash String,
    link_retrieved_at DateTime64(3, 'UTC'),
    link_resolved_at DateTime64(3, 'UTC'),
    person_source_record_uid String,
    name String,
    description Nullable(String),
    birth_year Nullable(UInt16),
    image_url Nullable(String),
    wikidata_url Nullable(String),
    person_source_system String,
    person_source_run_id String,
    person_source_record_id String,
    person_source_payload_hash String,
    person_retrieved_at DateTime64(3, 'UTC'),
    person_resolved_at DateTime64(3, 'UTC')
  ))) AS source_payload_json,
  toString(greatest(links.resolved_at, persons.resolved_at)) AS source_observed_at
FROM company_wikidata_ids AS company_ids
INNER JOIN corpscout.wikidata_company_people AS links FINAL
  ON links.company_wikidata_id = company_ids.wikidata_id
INNER JOIN corpscout.wikidata_persons AS persons FINAL
  ON persons.person_wikidata_id = links.person_wikidata_id
WHERE trim(persons.name) != ''
ORDER BY
  company_ids.company_id,
  links.source_record_id,
  persons.person_profile_hash,
  links.person_role_hash,
  greatest(links.resolved_at, persons.resolved_at) DESC
LIMIT 1 BY
  company_ids.company_id,
  links.source_record_id,
  persons.person_profile_hash,
  links.person_role_hash`;

const COUNT_QUERY_BY_SOURCE: Record<SwedenPeopleDraftSource, string> = {
  bolagsverket: BOLAGSVERKET_COUNT_QUERY,
  esef: ESEF_COUNT_QUERY,
  wikidata: WIKIDATA_COUNT_QUERY,
};

const OBSERVATIONS_QUERY_BY_SOURCE: Record<
  SwedenPeopleDraftSource,
  string
> = {
  bolagsverket: BOLAGSVERKET_OBSERVATIONS_QUERY,
  esef: ESEF_OBSERVATIONS_QUERY,
  wikidata: WIKIDATA_OBSERVATIONS_QUERY,
};

export async function getSwedenPeopleDraftSourceCount(
  source: SwedenPeopleDraftSource,
): Promise<number> {
  const [row] = await chQuery<SourceCountRow>(COUNT_QUERY_BY_SOURCE[source]);
  return Number(row?.row_count ?? 0);
}

export function streamSwedenPeopleDraftSource(
  source: SwedenPeopleDraftSource,
): AsyncGenerator<SwedenPeopleDraftSourceObservation> {
  return chStreamQuery<SwedenPeopleDraftSourceObservation>(
    OBSERVATIONS_QUERY_BY_SOURCE[source],
  );
}
