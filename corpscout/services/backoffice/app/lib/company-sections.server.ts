import { chQuery } from "~/lib/clickhouse.server";
import type {
  EvidenceOrigin,
  AddressRow,
  CompanyDescriptionObservation,
  CompanySourceRecord,
  ContractSummaryRow,
  DomainRow,
  EvidenceRef,
  GleifEntityRow,
  GleifRelationshipRow,
  IndustryDetailRow,
  PublicContractRow,
  SourceContactObservation,
  WikidataCompanyRow,
} from "~/lib/queries.server";
import { SE_COMPANY_ADDRESS_TABLE } from "~/lib/se-address-tables";

export const COMPANY_SECTION_NAMES = [
  "gleif",
  "wikidata",
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

async function getSectionEvidence(
  countryCode: string,
  companyId: string,
  section: CompanySectionName,
): Promise<Map<string, EvidenceRef[]>> {
  const links = await chQuery<SectionEvidenceLinkRow>(
    `SELECT item_key, toString(source_record_uid) AS source_record_uid,
       relationship_kind, match_method, toFloat64(match_confidence) AS match_confidence,
       record_kind, content_sha256, toString(first_seen_at) AS first_seen_at,
       toString(last_seen_at) AS last_seen_at, source_slug, source_record_key,
       source_url, source_object_key, payload_sha256,
       toString(retrieved_at) AS retrieved_at, source_run_id
     FROM corpscout.company_section_item_source_links
     PREWHERE country_code = {country:String} AND company_id = {id:String}
     WHERE section = {section:String}`,
    { country: countryCode, id: companyId, section },
  );
  if (links.length === 0) return new Map();
  const byItem = new Map<string, EvidenceRef[]>();
  for (const link of links) {
    const origin: EvidenceOrigin = {
      sourceSlug: link.source_slug,
      sourceRecordKey: link.source_record_key,
      sourceUrl: link.source_url,
      sourceObjectKey: link.source_object_key,
      payloadSha256: link.payload_sha256,
      retrievedAt: link.retrieved_at,
      sourceRunId: link.source_run_id,
    };
    const evidence: EvidenceRef = {
      sourceRecordUid: link.source_record_uid,
      recordKind: link.record_kind,
      contentSha256: link.content_sha256,
      firstSeenAt: link.first_seen_at,
      lastSeenAt: link.last_seen_at,
      origins: [origin],
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

/**
 * The company's published addresses, read straight from the SE address entity
 * (spec 2026-09-06, section 3.3): one row per company and address, geocode
 * included, with each contributing source zipped out of the row's parallel
 * arrays and its raw text read from the suggestion store. Slice 4b renames the
 * table, so the section names it exactly once.
 *
 * ACTIVE rows only: the section renders every address it is handed the same
 * way, so a hidden or withdrawn row would show up beside the live ones with
 * nothing to tell them apart. They come back when the page can badge them.
 *
 * The entity carries neither an OSM candidate list nor extract provenance, so
 * the geocode columns the retired serving view supplied are answered with
 * `''` / `0` / `[]` and the section's public type stays as it is.
 */
async function getAddressesSection(
  id: string,
): Promise<Extract<CompanySectionData, { section: "addresses" }>> {
  /** One published address; `members` is `[source, slot, normalized_id]`. */
  type AddressPublishedRow = Omit<AddressRow, "source_members"> & {
    address_id: string;
    members: [string, string, string][];
  };
  /** One raw suggestion, keyed by the published row's (source, slot) pair. */
  interface AddressRawRow {
    address_source: string;
    slot: string;
    address_type: string;
    raw_address: string;
    structured_address: string;
    registry_source_record_uid: string;
    registry_source_run_id: string;
    source_observed_at: string;
  }
  const [published, rawSuggestions] = await Promise.all([
    chQuery<AddressPublishedRow>(
      `SELECT
         toString(address.address_key) AS address_id,
         toString(address.address_key) AS canonical_address_key,
         if(length(address.kinds) > 0, toString(address.kinds[1]), 'address') AS address_type,
         arrayMap(x -> toString(x), address.kinds) AS address_types,
         arrayMap(x -> toString(x), address.sources) AS address_sources,
         toUInt64(length(address.sources)) AS address_member_count,
         arrayZip(
           arrayMap(x -> toString(x), address.sources),
           address.slots,
           arrayMap(x -> toString(x), address.normalized_ids)
         ) AS members,
         address.normalized_address AS full_address,
         toString(address.country_code) AS address_country_code,
         toUInt8(address.geocode_status = 'foreign') AS address_is_foreign,
         if(
           match(address.normalized_address, '^[0-9]{3} [0-9]{2}[^,]*$'),
           '',
           replaceRegexpOne(address.normalized_address, ',\\\\s*[0-9]{3} [0-9]{2}[^,]*$', '')
         ) AS geocode_street,
         ifNull(address.street_name, '') AS street_name,
         ifNull(address.house_number, '') AS house_number,
         ifNull(address.unit, '') AS address_unit,
         ifNull(address.postal_code, '') AS geocode_postal_code,
         address.latitude AS latitude,
         address.longitude AS longitude,
         toString(address.geocode_status) AS geocode_status,
         multiIf(
           address.geocode_status = 'matched_area', 'centroid_fallback',
           address.latitude IS NULL, '',
           'osm'
         ) AS geocode_provider,
         toString(address.geocode_precision) AS geocode_precision,
         toString(address.geocode_method) AS geocode_match_method,
         ifNull(address.geocode_confidence, 0) AS geocode_match_confidence,
         toUInt64(0) AS geocode_candidate_count,
         CAST([], 'Array(String)') AS geocode_candidate_record_urls,
         ifNull(address.city, '') AS geocode_coordinate_locality,
         toUInt64(0) AS geocode_coordinate_supporting_point_count,
         '' AS geocode_source_record_id,
         '' AS geocode_source_record_url,
         '' AS geocode_source_url,
         '' AS geocode_source_object_key,
         '' AS geocode_source_md5,
         '' AS geocode_source_snapshot_at,
         '' AS geocode_source_retrieved_at,
         '' AS geocode_source_run_id,
         ifNull(toString(address.geocoded_at), '') AS geocode_matched_at
       FROM ${SE_COMPANY_ADDRESS_TABLE} AS address FINAL
       PREWHERE address.company_id = {id:String}
       WHERE address.active = 1
       ORDER BY address.normalized_address`,
      { id },
    ),
    chQuery<AddressRawRow>(
      `SELECT
         toString(raw.source) AS address_source,
         raw.slot AS slot,
         toString(raw.kind) AS address_type,
         ifNull(raw.raw_address, '') AS raw_address,
         arrayStringConcat(
           arrayFilter(part -> part != '', [
             if(ifNull(raw.care_of, '') != '', concat('c/o ', raw.care_of), ''),
             ifNull(raw.street_address, ''),
             trimBoth(concat(ifNull(raw.postal_code, ''), ' ', ifNull(raw.post_town, '')))
           ]),
           ', '
         ) AS structured_address,
         raw.source_record_uid AS registry_source_record_uid,
         raw.source_run_id AS registry_source_run_id,
         toString(raw.observed_at) AS source_observed_at
       FROM corpscout.se_company_address_suggestion AS raw FINAL
       PREWHERE raw.company_id = {id:String}
       ORDER BY raw.source, raw.slot`,
      { id },
    ),
  ]);
  const rawByMember = new Map(
    rawSuggestions.map((row) => [`${row.address_source}|${row.slot}`, row]),
  );
  const addresses = published.map(({ members, ...row }): AddressRow => {
    // A source delivers either a raw line (Bolagsverket) or the structured
    // fields (SCB, Ratsit), never both, so the readable line is composed and
    // the raw one shown next to it only when it says something different.
    const source_members = members.map(([source, slot, normalizedId]) => {
      const raw = rawByMember.get(`${source}|${slot}`);
      return {
        address_key: normalizedId,
        address_type: raw?.address_type || row.address_type,
        address_source: source,
        raw_address: raw?.raw_address ?? "",
        display_address: raw?.structured_address || raw?.raw_address || "",
        registry_source_record_uid: raw?.registry_source_record_uid ?? "",
        registry_source_run_id: raw?.registry_source_run_id ?? "",
        source_observed_at: raw?.source_observed_at ?? "",
      };
    });
    return {
      ...row,
      // Belt and braces for the foreign rows published before the normalizer
      // composed a display line for them (2026-09-08): an empty line renders
      // as a blank row here and makes the detail card hide the whole Contact &
      // location section, so what the first member delivered stands in until
      // the next fold rewrites the row.
      full_address:
        row.full_address ||
        source_members.find((member) => member.display_address)
          ?.display_address ||
        "",
      source_members,
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
    `SELECT DISTINCT toString(source_record_uid) AS source_record_uid,
       record_kind, content_sha256, toString(first_seen_at) AS first_seen_at,
       toString(last_seen_at) AS last_seen_at, source_slug, source_record_key,
       source_url, source_object_key, payload_sha256,
       toString(retrieved_at) AS retrieved_at, source_run_id
     FROM corpscout.company_section_item_source_links
     PREWHERE country_code = {country:String} AND company_id = {id:String}
     ORDER BY last_seen_at DESC, source_slug, source_record_key`,
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
