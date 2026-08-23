import { chQuery } from "~/lib/clickhouse.server";
import {
  getPeopleSourceDefinition,
  type PeopleSourceDefinition,
  type PeopleSourceName,
} from "~/lib/people-sources";

export const PEOPLE_SOURCE_ROW_LIMIT = 100;

export interface CompanyIdFilter {
  input: string;
  companyId: string;
  error: string;
}

export interface BolagsverketPersonSourceRow {
  company_id: string;
  fiscal_year: number;
  statement_key: string;
  source_record_uid: string;
  signatory_kind: string;
  signatory_uid: string;
  first_name: string;
  last_name: string;
  role_original: string;
  role_kind: string;
  resolved_at: string;
}

export interface EsefPersonSourceRow {
  candidate_uid: string;
  source_record_uid: string;
  source_document_id: string;
  company_id: string;
  fiscal_year: number;
  name: string;
  role: string;
  role_category: string;
  organization: string;
  status: string;
  confidence: number;
  evidence_ids: string[];
  model_name: string;
  prompt_version: string;
  extracted_at: string;
}

export interface WikidataPersonSourceRow {
  company_id: string;
  company_wikidata_id: string;
  person_wikidata_id: string;
  source_record_uid: string;
  name: string;
  description: string;
  wikidata_url: string;
  role_property: string;
  role_label: string;
  start_date: string;
  end_date: string;
  is_current: number;
  source_record_id: string;
  retrieved_at: string;
}

export const BOLAGSVERKET_PEOPLE_SOURCE_QUERY = `SELECT
  company_id,
  fiscal_year,
  statement_key,
  source_record_uid,
  toString(signatory_kind) AS signatory_kind,
  toString(signatory_uid) AS signatory_uid,
  first_name,
  last_name,
  role_original,
  toString(role_kind) AS role_kind,
  toString(resolved_at) AS resolved_at
FROM corpscout.se_financial_report_signatories
WHERE {companyId:String} = '' OR company_id = {companyId:String}
ORDER BY company_id, fiscal_year DESC, statement_key, person_seq
LIMIT {limit:UInt64}`;

export const ESEF_PEOPLE_SOURCE_QUERY = `SELECT
  toString(candidate_uid) AS candidate_uid,
  toString(source_record_uid) AS source_record_uid,
  source_document_id,
  company_id,
  fiscal_year,
  name,
  role,
  toString(role_category) AS role_category,
  organization,
  toString(status) AS status,
  toFloat64(confidence) AS confidence,
  evidence_ids,
  model_name,
  prompt_version,
  toString(extracted_at) AS extracted_at
FROM corpscout.esef_document_people FINAL
WHERE country_code = 'SE'
  AND ({companyId:String} = '' OR company_id = {companyId:String})
ORDER BY company_id, fiscal_year DESC, source_document_id, name
LIMIT {limit:UInt64}`;

export const WIKIDATA_PEOPLE_SOURCE_QUERY = `WITH company_wikidata_ids AS (
  SELECT company_id, wikidata_id
  FROM (
    SELECT
      replaceRegexpAll(identifier_value, '[^0-9]', '') AS company_id,
      wikidata_id
    FROM corpscout.wikidata_company_identifiers FINAL
    WHERE identifier_type = 'se_orgnr'
      AND match(company_id, '^[0-9]{10}$')
      AND ({companyId:String} = '' OR company_id = {companyId:String})

    UNION ALL

    SELECT
      company_identifiers.company_id,
      wikidata_identifiers.wikidata_id
    FROM corpscout.company_identifier AS company_identifiers
    INNER JOIN corpscout.wikidata_company_identifiers AS wikidata_identifiers FINAL
      ON upperUTF8(wikidata_identifiers.identifier_value) =
         upperUTF8(company_identifiers.issuer_id)
    WHERE company_identifiers.country_code = 'SE'
      AND company_identifiers.issuer_scheme = 'lei'
      AND company_identifiers.is_current = 1
      AND wikidata_identifiers.identifier_type = 'lei'
      AND (
        {companyId:String} = ''
        OR company_identifiers.company_id = {companyId:String}
      )
  )
  GROUP BY company_id, wikidata_id
)
SELECT
  company_ids.company_id,
  links.company_wikidata_id AS company_wikidata_id,
  persons.person_wikidata_id AS person_wikidata_id,
  persons.source_record_uid AS source_record_uid,
  persons.name,
  ifNull(persons.description, '') AS description,
  ifNull(persons.wikidata_url, '') AS wikidata_url,
  toString(links.role_property) AS role_property,
  toString(links.role_label) AS role_label,
  ifNull(toString(links.start_date), '') AS start_date,
  ifNull(toString(links.end_date), '') AS end_date,
  toUInt8(links.is_current) AS is_current,
  links.source_record_id AS source_record_id,
  toString(greatest(links.retrieved_at, persons.retrieved_at)) AS retrieved_at
FROM company_wikidata_ids AS company_ids
INNER JOIN corpscout.wikidata_company_people AS links FINAL
  ON links.company_wikidata_id = company_ids.wikidata_id
INNER JOIN corpscout.wikidata_persons AS persons FINAL
  ON persons.person_wikidata_id = links.person_wikidata_id
ORDER BY company_ids.company_id, persons.name, links.role_label
LIMIT {limit:UInt64}`;

export type PeopleSourceResult =
  | {
      source: "bolagsverket";
      definition: PeopleSourceDefinition;
      filter: CompanyIdFilter;
      rows: BolagsverketPersonSourceRow[];
      rowLimit: number;
    }
  | {
      source: "esef";
      definition: PeopleSourceDefinition;
      filter: CompanyIdFilter;
      rows: EsefPersonSourceRow[];
      rowLimit: number;
    }
  | {
      source: "wikidata";
      definition: PeopleSourceDefinition;
      filter: CompanyIdFilter;
      rows: WikidataPersonSourceRow[];
      rowLimit: number;
    };

export function parseSwedenCompanyIdFilter(value: string): CompanyIdFilter {
  const input = value.trim().slice(0, 32);
  if (!input) return { input: "", companyId: "", error: "" };

  const companyId = input.replace(/[\s-]/g, "");
  if (!/^\d{10}$/.test(companyId)) {
    return {
      input,
      companyId: "",
      error: "Enter a 10-digit Swedish organization number.",
    };
  }

  return { input, companyId, error: "" };
}

export function getSwedenPeopleSourceRows(
  source: "bolagsverket",
  companyIdInput: string,
): Promise<Extract<PeopleSourceResult, { source: "bolagsverket" }>>;
export function getSwedenPeopleSourceRows(
  source: "esef",
  companyIdInput: string,
): Promise<Extract<PeopleSourceResult, { source: "esef" }>>;
export function getSwedenPeopleSourceRows(
  source: "wikidata",
  companyIdInput: string,
): Promise<Extract<PeopleSourceResult, { source: "wikidata" }>>;
export function getSwedenPeopleSourceRows(
  source: PeopleSourceName,
  companyIdInput: string,
): Promise<PeopleSourceResult>;
export async function getSwedenPeopleSourceRows(
  source: PeopleSourceName,
  companyIdInput: string,
): Promise<PeopleSourceResult> {
  const filter = parseSwedenCompanyIdFilter(companyIdInput);
  const definition = getPeopleSourceDefinition(source);
  const params = {
    companyId: filter.companyId,
    limit: PEOPLE_SOURCE_ROW_LIMIT,
  };

  if (filter.error) {
    if (source === "bolagsverket") {
      return {
        source,
        definition,
        filter,
        rows: [],
        rowLimit: PEOPLE_SOURCE_ROW_LIMIT,
      };
    }
    if (source === "esef") {
      return {
        source,
        definition,
        filter,
        rows: [],
        rowLimit: PEOPLE_SOURCE_ROW_LIMIT,
      };
    }
    return {
      source,
      definition,
      filter,
      rows: [],
      rowLimit: PEOPLE_SOURCE_ROW_LIMIT,
    };
  }

  if (source === "bolagsverket") {
    const rows = await chQuery<BolagsverketPersonSourceRow>(
      BOLAGSVERKET_PEOPLE_SOURCE_QUERY,
      params,
    );
    return { source, definition, filter, rows, rowLimit: PEOPLE_SOURCE_ROW_LIMIT };
  }

  if (source === "esef") {
    const rows = await chQuery<EsefPersonSourceRow>(
      ESEF_PEOPLE_SOURCE_QUERY,
      params,
    );
    return { source, definition, filter, rows, rowLimit: PEOPLE_SOURCE_ROW_LIMIT };
  }

  const rows = await chQuery<WikidataPersonSourceRow>(
    WIKIDATA_PEOPLE_SOURCE_QUERY,
    params,
  );
  return { source, definition, filter, rows, rowLimit: PEOPLE_SOURCE_ROW_LIMIT };
}
