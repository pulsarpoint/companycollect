import { chQuery } from "~/lib/clickhouse.server";

// Filings for the company via the LEI identifier bridge, annotated with how
// many facts and narrative notes have been parsed for each filing. A filing
// with fact_count 0 is cataloged but not yet parsed (backfill in flight).
export const ESEF_TAB_FILINGS_SQL = `
SELECT
  f.fxo_id AS fxo_id,
  f.entity_name AS entity_name,
  toString(f.period_end) AS period_end,
  toYear(f.period_end) AS fiscal_year,
  coalesce(fc.fact_count, 0) AS fact_count,
  coalesce(fc.note_count, 0) AS note_count,
  f.error_count AS error_count,
  f.warning_count AS warning_count,
  f.viewer_url AS viewer_url,
  f.source_url AS source_url,
  f.package_url AS package_url
FROM corpscout.esef_filings AS f FINAL
LEFT JOIN (
  SELECT
    fxo_id,
    uniqExact(fact_id) AS fact_count,
    uniqExactIf(fact_id, value_kind = 'text') AS note_count
  FROM corpscout.esef_facts
  WHERE upperUTF8(trimBoth(lei)) IN (
    SELECT issuer_id FROM corpscout.company_identifier
    WHERE issuer_scheme = 'lei' AND country_code = 'SE'
      AND company_id = {companyId:String}
  )
  GROUP BY fxo_id
) AS fc ON fc.fxo_id = f.fxo_id
WHERE upperUTF8(trimBoth(f.lei)) IN (
  SELECT issuer_id FROM corpscout.company_identifier
  WHERE issuer_scheme = 'lei' AND country_code = 'SE'
    AND company_id = {companyId:String}
)
ORDER BY f.period_end DESC`;

export const ESEF_TAB_INFORMATION_SQL = `
SELECT
  fiscal_year,
  extraction_status,
  company_description,
  toString(description_language) AS description_language,
  toFloat64(description_confidence) AS description_confidence,
  products_and_services_json,
  customer_markets_json,
  operating_geographies_json,
  business_segments_json,
  material_group_relationships_json
FROM corpscout.esef_document_company_information
WHERE country_iso2 = 'SE' AND company_id = {companyId:String}
ORDER BY fiscal_year DESC, extracted_at DESC
LIMIT 1 BY source_document_id`;

export const ESEF_TAB_PEOPLE_SQL = `
SELECT
  fiscal_year, name, role, role_category, organization, status,
  toFloat64(confidence) AS confidence
FROM corpscout.esef_document_people FINAL
WHERE country_code = 'SE' AND company_id = {companyId:String}
ORDER BY fiscal_year DESC, name, role`;

export const ESEF_TAB_BUSINESS_ITEMS_SQL = `
SELECT
  fiscal_year, item_kind, name, geography_type,
  toFloat64(confidence) AS confidence
FROM corpscout.esef_document_business_items FINAL
WHERE country_code = 'SE' AND company_id = {companyId:String}
ORDER BY fiscal_year DESC, item_kind, name`;

export const ESEF_TAB_CONTACTS_SQL = `
SELECT
  fiscal_year, candidate_kind, normalized_value, registrable_domain
FROM corpscout.esef_document_contact_candidates
WHERE country_iso2 = 'SE' AND company_id = {companyId:String}
ORDER BY fiscal_year DESC, candidate_kind, normalized_value`;

export const ESEF_TAB_RELATIONSHIPS_SQL = `
SELECT
  fiscal_year, related_company_name, relationship_type,
  toString(ownership_percentage) AS ownership_percentage, jurisdiction,
  toFloat64(confidence) AS confidence
FROM corpscout.esef_document_group_relationships FINAL
WHERE country_code = 'SE' AND company_id = {companyId:String}
ORDER BY fiscal_year DESC, related_company_name`;

// Query row types (snake_case, matching SQL aliases)
interface EsefTabFilingQueryRow {
  fxo_id: string;
  entity_name: string;
  period_end: string;
  fiscal_year: number;
  fact_count: number;
  note_count: number;
  error_count: number;
  warning_count: number;
  viewer_url: string;
  source_url: string;
  package_url: string;
}

interface EsefTabInformationQueryRow {
  fiscal_year: number;
  extraction_status: string;
  company_description: string;
  description_language: string;
  description_confidence: number;
  products_and_services_json: string;
  customer_markets_json: string;
  operating_geographies_json: string;
  business_segments_json: string;
  material_group_relationships_json: string;
}

interface EsefTabPersonQueryRow {
  fiscal_year: number;
  name: string;
  role: string;
  role_category: string;
  organization: string;
  status: string;
  confidence: number;
}

interface EsefTabBusinessItemQueryRow {
  fiscal_year: number;
  item_kind: string;
  name: string;
  geography_type: string;
  confidence: number;
}

interface EsefTabContactQueryRow {
  fiscal_year: number;
  candidate_kind: string;
  normalized_value: string;
  registrable_domain: string;
}

interface EsefTabRelationshipQueryRow {
  fiscal_year: number;
  related_company_name: string;
  relationship_type: string;
  ownership_percentage: string;
  jurisdiction: string;
  confidence: number;
}

// Public API interfaces (camelCase)
export interface EsefTabFiling {
  fxoId: string;
  entityName: string;
  periodEnd: string;
  fiscalYear: number;
  factCount: number;
  noteCount: number;
  errorCount: number;
  warningCount: number;
  viewerUrl: string;
  sourceUrl: string;
  packageUrl: string;
}

export interface EsefTabInformation {
  fiscalYear: number;
  extractionStatus: string;
  companyDescription: string;
  descriptionLanguage: string;
  descriptionConfidence: number;
  productsAndServicesJson: string;
  customerMarketsJson: string;
  operatingGeographiesJson: string;
  businessSegmentsJson: string;
  materialGroupRelationshipsJson: string;
}

export interface EsefTabPerson {
  fiscalYear: number;
  name: string;
  role: string;
  roleCategory: string;
  organization: string;
  status: string;
  confidence: number;
}

export interface EsefTabBusinessItem {
  fiscalYear: number;
  itemKind: string;
  name: string;
  geographyType: string;
  confidence: number;
}

export interface EsefTabContact {
  fiscalYear: number;
  candidateKind: string;
  normalizedValue: string;
  registrableDomain: string;
}

export interface EsefTabRelationship {
  fiscalYear: number;
  relatedCompanyName: string;
  relationshipType: string;
  ownershipPercentage: string;
  jurisdiction: string;
  confidence: number;
}

export interface SeCompanyEsefDetail {
  filings: EsefTabFiling[];
  information: EsefTabInformation[];
  people: EsefTabPerson[];
  businessItems: EsefTabBusinessItem[];
  contacts: EsefTabContact[];
  relationships: EsefTabRelationship[];
}

function mapFilingRow(r: EsefTabFilingQueryRow): EsefTabFiling {
  return {
    fxoId: r.fxo_id,
    entityName: r.entity_name,
    periodEnd: r.period_end,
    fiscalYear: Number(r.fiscal_year),
    factCount: Number(r.fact_count),
    noteCount: Number(r.note_count),
    errorCount: Number(r.error_count),
    warningCount: Number(r.warning_count),
    viewerUrl: r.viewer_url,
    sourceUrl: r.source_url,
    packageUrl: r.package_url,
  };
}

// Filings only -- the ESEF sub-tab bar needs the document list on every
// subpage without paying for the full extraction detail.
export async function loadSeCompanyEsefFilings(
  companyId: string,
): Promise<EsefTabFiling[]> {
  const rows = await chQuery<EsefTabFilingQueryRow>(ESEF_TAB_FILINGS_SQL, {
    companyId,
  });
  return rows.map(mapFilingRow);
}

export async function loadSeCompanyEsef(
  companyId: string,
): Promise<SeCompanyEsefDetail | null> {
  const params = { companyId };
  const [filings, information, people, items, contacts, relationships] =
    await Promise.all([
      chQuery<EsefTabFilingQueryRow>(ESEF_TAB_FILINGS_SQL, params),
      chQuery<EsefTabInformationQueryRow>(ESEF_TAB_INFORMATION_SQL, params),
      chQuery<EsefTabPersonQueryRow>(ESEF_TAB_PEOPLE_SQL, params),
      chQuery<EsefTabBusinessItemQueryRow>(ESEF_TAB_BUSINESS_ITEMS_SQL, params),
      chQuery<EsefTabContactQueryRow>(ESEF_TAB_CONTACTS_SQL, params),
      chQuery<EsefTabRelationshipQueryRow>(ESEF_TAB_RELATIONSHIPS_SQL, params),
    ]);

  if (filings.length === 0 && information.length === 0) return null;

  return {
    filings: filings.map(mapFilingRow),
    information: information.map((r) => ({
      fiscalYear: Number(r.fiscal_year),
      extractionStatus: r.extraction_status,
      companyDescription: r.company_description,
      descriptionLanguage: r.description_language,
      descriptionConfidence: Number(r.description_confidence),
      productsAndServicesJson: r.products_and_services_json,
      customerMarketsJson: r.customer_markets_json,
      operatingGeographiesJson: r.operating_geographies_json,
      businessSegmentsJson: r.business_segments_json,
      materialGroupRelationshipsJson: r.material_group_relationships_json,
    })),
    people: people.map((r) => ({
      fiscalYear: Number(r.fiscal_year),
      name: r.name,
      role: r.role,
      roleCategory: r.role_category,
      organization: r.organization,
      status: r.status,
      confidence: Number(r.confidence),
    })),
    businessItems: items.map((r) => ({
      fiscalYear: Number(r.fiscal_year),
      itemKind: r.item_kind,
      name: r.name,
      geographyType: r.geography_type,
      confidence: Number(r.confidence),
    })),
    contacts: contacts.map((r) => ({
      fiscalYear: Number(r.fiscal_year),
      candidateKind: r.candidate_kind,
      normalizedValue: r.normalized_value,
      registrableDomain: r.registrable_domain,
    })),
    relationships: relationships.map((r) => ({
      fiscalYear: Number(r.fiscal_year),
      relatedCompanyName: r.related_company_name,
      relationshipType: r.relationship_type,
      ownershipPercentage: r.ownership_percentage,
      jurisdiction: r.jurisdiction,
      confidence: Number(r.confidence),
    })),
  };
}
