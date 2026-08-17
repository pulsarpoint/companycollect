import { chQuery } from "~/lib/clickhouse.server";
import {
  normalizeCountryPersonName,
  resolveCountryPersonProfilesForCompany,
} from "~/lib/people.server";
import { COMPANY_SOURCE_RECORD_ORIGINS_QUERY } from "~/lib/queries.server";
import type {
  EvidenceOrigin,
  AddressRow,
  CompanyDescriptionObservation,
  CompanySourceRecord,
  ContractSummaryRow,
  DomainRow,
  EsefPersonObservation,
  EvidenceRef,
  GleifEntityRow,
  GleifRelationshipRow,
  IndustryDetailRow,
  OfficerRow,
  PublicContractRow,
  SourceContactObservation,
  WikidataCompanyRow,
  WikidataPersonRow,
} from "~/lib/queries.server";

export const COMPANY_SECTION_NAMES = [
  "gleif",
  "wikidata",
  "management",
  "descriptions",
  "domains",
  "contracts",
  "financials",
  "industries",
  "addresses",
  "sources",
  "technology",
] as const;

export type CompanySectionName = (typeof COMPANY_SECTION_NAMES)[number];

export interface CompanySectionPresence {
  section: CompanySectionName;
  itemCount: number;
  latestObservedAt: string;
}

export function isCompanySectionName(
  value: string,
): value is CompanySectionName {
  return COMPANY_SECTION_NAMES.includes(value as CompanySectionName);
}

export async function getCompanySectionPresence(
  countryCode: string,
  companyId: string,
): Promise<CompanySectionPresence[]> {
  if (countryCode.toUpperCase() !== "SE") return [];
  const rows = await chQuery<{
    section: CompanySectionName;
    item_count: number | string;
    latest_observed_at: string;
  }>(
    `SELECT section, item_count, toString(latest_observed_at) AS latest_observed_at
     FROM corpscout.company_section_presence_current
     PREWHERE country_code = {country:String} AND company_id = {id:String}
     ORDER BY section`,
    { country: "SE", id: companyId },
  );
  return rows.map((row) => ({
    section: row.section,
    itemCount: Number(row.item_count),
    latestObservedAt: row.latest_observed_at,
  }));
}

interface SectionEvidenceLinkRow {
  item_key: string;
  source_record_uid: string;
  relationship_kind: string;
  match_method: string;
  match_confidence: number | string;
}

interface SectionEvidenceRow {
  source_record_uid: string;
  record_kind: string;
  content_sha256: string;
  earliest_seen_at: string;
  latest_seen_at: string;
}

interface SectionEvidenceOriginRow {
  source_record_uid: string;
  source_slug: string;
  source_record_key: string;
  source_url: string;
  source_object_key: string;
  payload_sha256: string;
  retrieved_at: string;
  source_run_id: string;
}

async function getSectionEvidence(
  countryCode: string,
  companyId: string,
  section: CompanySectionName,
): Promise<Map<string, EvidenceRef[]>> {
  const links = await chQuery<SectionEvidenceLinkRow>(
    `SELECT item_key, toString(source_record_uid) AS source_record_uid,
       relationship_kind, match_method, toFloat64(match_confidence) AS match_confidence
     FROM corpscout.company_section_item_source_links
     PREWHERE country_code = {country:String} AND company_id = {id:String}
     WHERE section = {section:String}`,
    { country: countryCode, id: companyId, section },
  );
  if (links.length === 0) return new Map();

  // Resolve the small company-scoped UID set explicitly. A JOIN makes
  // ClickHouse build the right-hand side from all company_source_records
  // (millions of rows) even though the links CTE contains only a handful of
  // UIDs. The array predicate follows the table's source_record_uid sorting
  // key and therefore reads only the relevant key ranges.
  const sourceRecordUids = [
    ...new Set(links.map((link) => link.source_record_uid)),
  ];
  const [rows, originRows] = await Promise.all([
    chQuery<SectionEvidenceRow>(
      `SELECT toString(source_record_uid) AS source_record_uid,
         argMax(record_kind, last_seen_at) AS record_kind,
         argMax(content_sha256, last_seen_at) AS content_sha256,
         toString(min(first_seen_at)) AS earliest_seen_at,
         toString(max(last_seen_at)) AS latest_seen_at
       FROM corpscout.company_source_records
       PREWHERE source_record_uid IN {source_record_uids:Array(String)}
       GROUP BY source_record_uid`,
      { source_record_uids: sourceRecordUids },
    ),
    chQuery<SectionEvidenceOriginRow>(COMPANY_SOURCE_RECORD_ORIGINS_QUERY, {
      sourceRecordUids,
    }),
  ]);
  const recordsByUid = new Map(rows.map((row) => [row.source_record_uid, row]));
  const originsByUid = new Map<string, EvidenceOrigin[]>();
  for (const origin of originRows) {
    const origins = originsByUid.get(origin.source_record_uid) ?? [];
    origins.push({
      sourceSlug: origin.source_slug,
      sourceRecordKey: origin.source_record_key,
      sourceUrl: origin.source_url,
      sourceObjectKey: origin.source_object_key,
      payloadSha256: origin.payload_sha256,
      retrievedAt: origin.retrieved_at,
      sourceRunId: origin.source_run_id,
    });
    originsByUid.set(origin.source_record_uid, origins);
  }
  const byItem = new Map<string, EvidenceRef[]>();
  for (const link of links) {
    const row = recordsByUid.get(link.source_record_uid);
    if (!row) continue;
    const evidence: EvidenceRef = {
      sourceRecordUid: row.source_record_uid,
      recordKind: row.record_kind,
      contentSha256: row.content_sha256,
      firstSeenAt: row.earliest_seen_at,
      lastSeenAt: row.latest_seen_at,
      origins: originsByUid.get(row.source_record_uid) ?? [],
      connectionKind: link.relationship_kind,
      extractionMethod: link.match_method,
      confidence: Number(link.match_confidence),
    };
    byItem.set(link.item_key, [...(byItem.get(link.item_key) ?? []), evidence]);
  }
  return byItem;
}

export type CompanySectionData =
  | {
      section: "gleif";
      entity: GleifEntityRow | null;
      relationships: GleifRelationshipRow[];
    }
  | { section: "wikidata"; wikidata: WikidataCompanyRow | null }
  | {
      section: "management";
      officers: OfficerRow[];
      wikidataPeople: WikidataPersonRow[];
      esefPeople: EsefPersonObservation[];
    }
  | { section: "descriptions"; descriptions: CompanyDescriptionObservation[] }
  | {
      section: "domains";
      domains: DomainRow[];
      sourceContacts: SourceContactObservation[];
    }
  | {
      section: "contracts";
      contracts: PublicContractRow[];
      summary: ContractSummaryRow | null;
    }
  | { section: "financials"; available: boolean }
  | { section: "industries"; industries: IndustryDetailRow[] }
  | { section: "addresses"; addresses: AddressRow[] }
  | { section: "sources"; records: CompanySourceRecord[] }
  | { section: "technology"; available: boolean };

export async function getCompanySection(
  countryCode: string,
  companyId: string,
  section: CompanySectionName,
): Promise<CompanySectionData> {
  if (countryCode.toUpperCase() !== "SE") {
    throw new Error(
      "Company serving sections are currently available for Sweden only",
    );
  }
  const country = "SE";
  switch (section) {
    case "gleif":
      return getGleifSection(country, companyId);
    case "wikidata":
      return getWikidataSection(country, companyId);
    case "management":
      return getManagementSection(country, companyId);
    case "descriptions":
      return getDescriptionsSection(country, companyId);
    case "domains":
      return getDomainsSection(country, companyId);
    case "contracts":
      return getContractsSection(country, companyId);
    case "financials":
      return getFinancialsSection(companyId);
    case "industries":
      return getIndustriesSection(country, companyId);
    case "addresses":
      return getAddressesSection(companyId);
    case "sources":
      return getSourcesSection(country, companyId);
    case "technology":
      return { section, available: true };
  }
}

async function getGleifSection(
  country: string,
  id: string,
): Promise<Extract<CompanySectionData, { section: "gleif" }>> {
  const [entities, relationships] = await Promise.all([
    chQuery<GleifEntityRow>(
      `SELECT lei, registration_status AS lei_status, category,
         headquarters_country AS hq_country, headquarters_abroad AS hq_abroad,
         arrayStringConcat(ownership_exception_reasons, ',') AS ownership_exceptions
       FROM corpscout.company_gleif_current
       PREWHERE country_code = {country:String} AND company_id = {id:String}
       ORDER BY is_primary DESC, lei
       LIMIT 1`,
      { country, id },
    ),
    chQuery<GleifRelationshipRow>(
      `SELECT
         if(direction = 'outgoing', 'parent', 'subsidiary') AS direction,
         relationship_type, other_lei, other_name AS name,
         ifNull(other_country_code, '') AS jurisdiction,
         ifNull(other_company_id, '') AS local_id
       FROM corpscout.company_gleif_relationship_current
       PREWHERE country_code = {country:String} AND company_id = {id:String}
       ORDER BY direction, relationship_type, other_lei`,
      { country, id },
    ),
  ]);
  return { section: "gleif", entity: entities[0] ?? null, relationships };
}

async function getWikidataSection(
  country: string,
  id: string,
): Promise<Extract<CompanySectionData, { section: "wikidata" }>> {
  const [rows, evidence] = await Promise.all([
    chQuery<WikidataCompanyRow>(
      `SELECT wikidata_id, wikidata_url, description, official_name,
         ifNull(toString(inception_date), '') AS inception_date, employee_count,
         ifNull(toString(employee_count_as_of), '') AS employee_count_as_of,
         industry_label, legal_form_label, headquarters, headquarters_country,
         logo_url, has_current_listing, arrayStringConcat(listings, ' | ') AS listings,
         arrayStringConcat(websites, ' ') AS websites, linkedin_id
       FROM corpscout.company_wikidata_current
       PREWHERE country_code = {country:String} AND company_id = {id:String}
       ORDER BY is_primary DESC, wikidata_id
       LIMIT 1`,
      { country, id },
    ),
    getSectionEvidence(country, id, "wikidata"),
  ]);
  const wikidata = rows[0] ?? null;
  if (wikidata) wikidata.evidence = evidence.get(wikidata.wikidata_id) ?? [];
  return { section: "wikidata", wikidata };
}

interface ManagementServingRow {
  management_id: string;
  person_id: string;
  person_profile_available: number;
  external_person_scheme: string;
  external_person_value: string;
  display_name: string;
  first_name: string;
  last_name: string;
  person_description: string;
  birth_year: number | null;
  image_url: string;
  external_url: string;
  role_kind: string;
  role_label: string;
  signatory_kind: string;
  start_date: string;
  end_date: string;
  latest_fiscal_year: number | null;
  is_current: number;
  source_systems: string[];
}

async function getManagementSection(
  country: string,
  id: string,
): Promise<Extract<CompanySectionData, { section: "management" }>> {
  const [rows, evidence] = await Promise.all([
    chQuery<ManagementServingRow>(
      `SELECT current.management_id,
         ifNull(toString(current.person_id), lower(hex(SHA256(concat(
           'legacy-management-person|', current.country_code, '|', current.company_id, '|',
           lowerUTF8(trim(current.display_name))
         ))))) AS person_id,
         toUInt8(isNotNull(current.person_id)) AS person_profile_available,
         external_person_scheme, external_person_value, display_name, first_name,
         last_name, person_description, birth_year, image_url, external_url,
         role_kind, role_label, signatory_kind,
         ifNull(toString(start_date), '') AS start_date,
         ifNull(toString(end_date), '') AS end_date,
         latest_fiscal_year, is_current, source_systems
       FROM corpscout.company_management_current AS current
       PREWHERE current.country_code = {country:String} AND current.company_id = {id:String}
       ORDER BY is_current DESC, role_kind, display_name`,
      { country, id },
    ),
    getSectionEvidence(country, id, "management"),
  ]);
  const legacyProfileIds = await resolveCountryPersonProfilesForCompany(
    country,
    id,
    rows
      .filter(
        (row) =>
          row.person_profile_available === 0 &&
          row.source_systems.includes("se_xbrl_signatures"),
      )
      .map((row) => row.display_name),
  );
  const officers: OfficerRow[] = [];
  const wikidataPeople: WikidataPersonRow[] = [];
  const esefPeople: EsefPersonObservation[] = [];
  for (const row of rows) {
    const rowEvidence = evidence.get(row.management_id) ?? [];
    if (row.source_systems.includes("se_xbrl_signatures")) {
      const resolvedPersonId =
        legacyProfileIds.get(normalizeCountryPersonName(row.display_name)) ??
        null;
      officers.push({
        country_iso2: country,
        person_id: resolvedPersonId ?? row.person_id,
        person_profile_available:
          Boolean(row.person_profile_available) || resolvedPersonId !== null,
        first_name: row.first_name,
        last_name: row.last_name,
        role_original: row.role_label,
        role_kind: row.role_kind,
        signatory_kind: row.signatory_kind,
        fiscal_year: row.latest_fiscal_year ?? 0,
        evidence: rowEvidence,
      });
    } else if (row.source_systems.includes("wikidata")) {
      wikidataPeople.push({
        person_wikidata_id: row.external_person_value,
        name: row.display_name,
        description: row.person_description,
        birth_year: row.birth_year,
        image_url: row.image_url,
        wikidata_url: row.external_url,
        role_label: row.role_label,
        is_current: row.is_current,
        start_date: row.start_date,
        end_date: row.end_date,
        evidence: rowEvidence,
      });
    } else {
      esefPeople.push({
        candidateUid: row.management_id,
        sourceRecordUid: rowEvidence[0]?.sourceRecordUid ?? "",
        sourceDocumentId: "",
        fiscalYear: row.latest_fiscal_year ?? 0,
        name: row.display_name,
        role: row.role_label,
        roleCategory: row.role_kind,
        organization: row.person_description,
        status: row.is_current ? "current" : "former",
        effectiveFrom: row.start_date,
        effectiveTo: row.end_date,
        evidence: rowEvidence,
      });
    }
  }
  return { section: "management", officers, wikidataPeople, esefPeople };
}

async function getDescriptionsSection(
  country: string,
  id: string,
): Promise<Extract<CompanySectionData, { section: "descriptions" }>> {
  const [rows, evidence] = await Promise.all([
    chQuery<{
      description_id: string;
      description_kind: string;
      text_original: string;
      language_original: string;
      text_en: string | null;
      extracted_at: string;
    }>(
      `SELECT description_id, description_kind, text_original, language_original,
         text_en, toString(extracted_at) AS extracted_at
       FROM corpscout.company_description_current
       PREWHERE country_code = {country:String} AND company_id = {id:String}
       ORDER BY extracted_at DESC, description_kind, description_id`,
      { country, id },
    ),
    getSectionEvidence(country, id, "descriptions"),
  ]);
  return {
    section: "descriptions",
    descriptions: rows.map((row) => ({
      observationUid: row.description_id,
      sourceRecordUid:
        evidence.get(row.description_id)?.[0]?.sourceRecordUid ?? "",
      descriptionKind: row.description_kind,
      textOriginal: row.text_original,
      languageOriginal: row.language_original,
      textEn: row.text_en,
      extractedAt: row.extracted_at,
      evidence: evidence.get(row.description_id) ?? [],
    })),
  };
}

async function getDomainsSection(
  country: string,
  id: string,
): Promise<Extract<CompanySectionData, { section: "domains" }>> {
  const [domains, contacts, evidence] = await Promise.all([
    chQuery<DomainRow & { root_domain: string }>(
      `SELECT root_domain, root_domain AS domain, website_url,
         arrayStringConcat(source_names, ' + ') AS domain_source,
         suggested_confidence AS confidence,
         toUInt8(review_status = 'confirmed_primary' OR suggested_primary = 1) AS is_primary
       FROM corpscout.company_domains FINAL
       PREWHERE country_code = {country:String} AND company_id = {id:String}
       WHERE is_active = 1 AND review_status != 'rejected'
       ORDER BY is_primary DESC, suggested_confidence DESC, root_domain`,
      { country, id },
    ),
    chQuery<{
      contact_id: string;
      contact_type: string;
      contact_value: string;
      registrable_domain: string;
      fiscal_year: number | null;
    }>(
      `SELECT contact_id, contact_type, contact_value, registrable_domain, fiscal_year
       FROM corpscout.company_contact_current
       PREWHERE country_code = {country:String} AND company_id = {id:String}
       ORDER BY contact_type, contact_value`,
      { country, id },
    ),
    getSectionEvidence(country, id, "domains"),
  ]);
  for (const domain of domains) {
    domain.evidence = evidence.get(domain.root_domain) ?? [];
  }
  return {
    section: "domains",
    domains,
    sourceContacts: contacts.map((contact) => ({
      candidateId: contact.contact_id,
      sourceRecordUid:
        evidence.get(contact.contact_id)?.[0]?.sourceRecordUid ?? "",
      fiscalYear: contact.fiscal_year ?? 0,
      candidateKind: contact.contact_type,
      normalizedValue: contact.contact_value,
      registrableDomain: contact.registrable_domain,
      evidence: evidence.get(contact.contact_id) ?? [],
    })),
  };
}

async function getContractsSection(
  country: string,
  id: string,
): Promise<Extract<CompanySectionData, { section: "contracts" }>> {
  const [contracts, summaries] = await Promise.all([
    chQuery<PublicContractRow>(
      `SELECT source, notice_ref, ifNull(toString(contract_date), '') AS contract_date,
         buyer_name, title, amount_original, amount_usd, currency,
         notice_amount_original, notice_amount_usd, notice_currency, source_url
       FROM corpscout.company_contract_current
       PREWHERE country_code = {country:String} AND company_id = {id:String}
       ORDER BY contract_date DESC, contract_ref
       LIMIT 100`,
      { country, id },
    ),
    chQuery<ContractSummaryRow>(
      `SELECT contract_count AS award_count, valued_contract_count AS valued_count,
         total_attributable_value_usd AS total_value_usd,
         ifNull(toString(last_contract_date), '') AS last_award_date,
         arrayStringConcat(source_systems, ', ') AS sources
       FROM corpscout.company_contract_summary_current
       PREWHERE country_code = {country:String} AND company_id = {id:String}
       LIMIT 1`,
      { country, id },
    ),
  ]);
  return { section: "contracts", contracts, summary: summaries[0] ?? null };
}

async function getFinancialsSection(
  id: string,
): Promise<Extract<CompanySectionData, { section: "financials" }>> {
  const rows = await chQuery<{ found: number }>(
    `SELECT 1 AS found FROM corpscout.se_company_financials_latest
     PREWHERE company_id = {id:String} LIMIT 1`,
    { id },
  );
  return { section: "financials", available: rows.length > 0 };
}

async function getIndustriesSection(
  country: string,
  id: string,
): Promise<Extract<CompanySectionData, { section: "industries" }>> {
  const [rows, evidence] = await Promise.all([
    chQuery<IndustryDetailRow & { classification_code: string }>(
      `SELECT classification_code, classification_code AS industry_code,
         label_sv AS description_original, label_en AS industry_label, is_primary
       FROM corpscout.se_company_industry_display_current
       PREWHERE company_id = {id:String}
       ORDER BY is_primary DESC, classification_system, classification_code`,
      { id },
    ),
    getSectionEvidence(country, id, "industries"),
  ]);
  for (const row of rows)
    row.evidence = evidence.get(row.classification_code) ?? [];
  return { section: "industries", industries: rows };
}

async function getAddressesSection(
  id: string,
): Promise<Extract<CompanySectionData, { section: "addresses" }>> {
  type AddressLinkRow = Pick<AddressRow, "address_type"> &
    Partial<AddressRow> & {
      address_id: string;
      canonical_address_key: string;
    };
  type AddressOwnedRow = Pick<AddressRow, "full_address"> &
    Partial<AddressRow> & { address_id: string };
  type AddressGeocodeRow = Partial<AddressRow> & { address_id: string };
  type AddressMemberRow = NonNullable<AddressRow["source_members"]>[number] & {
    canonical_address_key: string;
  };
  const [links, members] = await Promise.all([
    chQuery<AddressLinkRow>(
      `SELECT
       toString(address_id) AS address_id,
       toString(canonical_address_key) AS canonical_address_key,
       if(length(address_types) > 0, address_types[1], 'address') AS address_type,
       address_types,
       address_sources,
       evidence_count AS address_member_count
     FROM corpscout.se_company_address_links_current
     PREWHERE company_id = {id:String}
     ORDER BY address_type, address_id`,
      { id },
    ),
    chQuery<AddressMemberRow>(
      `SELECT
         canonical_address_key,
         address_key,
         address_type,
         address_source,
         raw_address,
         display_address,
         street_name,
         house_number,
         unit AS address_unit,
         registry_source_record_uid,
         registry_source_run_id,
         toString(source_observed_at) AS source_observed_at
       FROM corpscout.se_company_address_members_current
       PREWHERE company_id = {id:String}
       ORDER BY canonical_address_key, address_source, address_type, address_key`,
      { id },
    ),
  ]);
  if (links.length === 0) return { section: "addresses", addresses: [] };

  const addressIds = links.map((link) => link.address_id);
  const [addressRows, geocodeRows] = await Promise.all([
    chQuery<AddressOwnedRow>(
      `SELECT
         toString(address_id) AS address_id,
         canonical_display_address AS full_address,
         country_code AS address_country_code,
         toUInt8(address_kind = 'foreign') AS address_is_foreign,
         canonical_display_address AS geocode_address,
         street_address AS geocode_street,
         street_name,
         house_number,
         unit AS address_unit,
         postal_code AS geocode_postal_code
       FROM corpscout.se_addresses_current
       PREWHERE address_id IN {address_ids:Array(String)}`,
      { address_ids: addressIds },
    ),
    chQuery<AddressGeocodeRow>(
      `SELECT
         toString(address_id) AS address_id,
         latitude,
         longitude,
         match_status AS geocode_status,
         geocode_provider,
         geocode_precision,
         match_method AS geocode_match_method,
         match_confidence AS geocode_match_confidence,
         candidate_count AS geocode_candidate_count,
         candidate_record_urls AS geocode_candidate_record_urls,
         ifNull(coordinate_locality, '') AS geocode_coordinate_locality,
         coordinate_supporting_point_count
           AS geocode_coordinate_supporting_point_count,
         coordinate_spread_meters AS geocode_coordinate_spread_meters,
         ifNull(source_record_id, '') AS geocode_source_record_id,
         ifNull(source_record_url, '') AS geocode_source_record_url,
         ifNull(source_url, '') AS geocode_source_url,
         ifNull(source_object_key, '') AS geocode_source_object_key,
         ifNull(source_md5, '') AS geocode_source_md5,
         ifNull(toString(source_snapshot_at), '') AS geocode_source_snapshot_at,
         ifNull(toString(source_retrieved_at), '') AS geocode_source_retrieved_at,
         geocode_run_id AS geocode_source_run_id,
         toString(matched_at) AS geocode_matched_at
       FROM corpscout.se_address_geocodes_current
       PREWHERE address_id IN {address_ids:Array(String)}`,
      { address_ids: addressIds },
    ),
  ]);
  const addressById = new Map(addressRows.map((row) => [row.address_id, row]));
  const geocodeById = new Map(geocodeRows.map((row) => [row.address_id, row]));
  const membersByCanonicalAddress = new Map<string, AddressMemberRow[]>();
  for (const member of members) {
    const group = membersByCanonicalAddress.get(member.canonical_address_key);
    if (group) group.push(member);
    else membersByCanonicalAddress.set(member.canonical_address_key, [member]);
  }
  const addresses = links.map((link): AddressRow => {
    const address = addressById.get(link.address_id);
    const geocode = geocodeById.get(link.address_id);
    if (!address || !geocode)
      throw new Error(`Incomplete address-owned data for ${link.address_id}`);
    return {
      ...link,
      ...address,
      ...geocode,
      source_members: (
        membersByCanonicalAddress.get(link.canonical_address_key) ?? []
      ).map(({ canonical_address_key: _, ...member }) => member),
    };
  });
  return { section: "addresses", addresses };
}

interface SourceRecordRow {
  source_record_uid: string;
  record_kind: string;
  content_sha256: string;
  first_seen_at: string;
  last_seen_at: string;
  source_slug: string;
  source_record_key: string;
  source_url: string;
  source_object_key: string;
  payload_sha256: string;
  retrieved_at: string;
  source_run_id: string;
}

async function getSourcesSection(
  country: string,
  id: string,
): Promise<Extract<CompanySectionData, { section: "sources" }>> {
  const rows = await chQuery<SourceRecordRow>(
    `WITH linked AS (
       SELECT DISTINCT toString(source_record_uid) AS source_record_uid
       FROM corpscout.company_source_record_links
       PREWHERE country_code = {country:String} AND company_id = {id:String}
     ), records AS (
       SELECT source_record_uid, argMax(record_kind, last_seen_at) AS record_kind,
         argMax(content_sha256, last_seen_at) AS content_sha256,
         min(first_seen_at) AS earliest_seen_at,
         max(last_seen_at) AS latest_seen_at
       FROM corpscout.company_source_records
       PREWHERE source_record_uid IN (SELECT source_record_uid FROM linked)
       GROUP BY source_record_uid
     ), origins AS (
       SELECT source_record_uid, source_slug, source_record_key, source_url,
         source_object_key, argMax(payload_sha256, retrieved_at) AS payload_sha256,
         max(retrieved_at) AS latest_retrieved_at,
         argMax(source_run_id, retrieved_at) AS source_run_id
       FROM corpscout.company_source_record_origins
       PREWHERE source_record_uid IN (SELECT source_record_uid FROM linked)
       GROUP BY source_record_uid, source_slug, source_record_key, source_url,
         source_object_key
     )
     SELECT toString(records.source_record_uid) AS source_record_uid,
       records.record_kind, records.content_sha256,
       toString(records.earliest_seen_at) AS first_seen_at,
       toString(records.latest_seen_at) AS last_seen_at,
       origins.source_slug, origins.source_record_key, origins.source_url,
       origins.source_object_key, origins.payload_sha256,
       toString(origins.latest_retrieved_at) AS retrieved_at, origins.source_run_id
     FROM records
     LEFT JOIN origins
       ON origins.source_record_uid = records.source_record_uid
     ORDER BY records.latest_seen_at DESC, origins.source_slug, origins.source_record_key`,
    { country, id },
  );
  const records = new Map<string, CompanySourceRecord>();
  for (const row of rows) {
    const origin = {
      sourceSlug: row.source_slug,
      sourceRecordKey: row.source_record_key,
      sourceUrl: row.source_url,
      sourceObjectKey: row.source_object_key,
      payloadSha256: row.payload_sha256,
      retrievedAt: row.retrieved_at,
      sourceRunId: row.source_run_id,
    };
    const existing = records.get(row.source_record_uid);
    if (existing) {
      existing.evidence[0]?.origins.push(origin);
      continue;
    }
    const evidence: EvidenceRef = {
      sourceRecordUid: row.source_record_uid,
      recordKind: row.record_kind,
      contentSha256: row.content_sha256,
      firstSeenAt: row.first_seen_at,
      lastSeenAt: row.last_seen_at,
      origins: row.source_slug ? [origin] : [],
    };
    records.set(row.source_record_uid, {
      sourceRecordUid: row.source_record_uid,
      recordKind: row.record_kind,
      firstSeenAt: row.first_seen_at,
      lastSeenAt: row.last_seen_at,
      evidence: [evidence],
    });
  }
  return { section: "sources", records: [...records.values()] };
}
