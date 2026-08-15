import { randomUUID } from "node:crypto";
import { chInsertPersonCorrections, chQuery } from "~/lib/clickhouse.server";

export interface CountryPersonSummary {
  country_iso2: string;
  person_id: string;
  preferred_name: string;
  preferred_name_normalized: string;
  resolution_status:
    "verified" | "reviewed" | "provisional" | "unresolved" | "merged";
  resolution_method: string;
  merged_into_person_id: string | null;
  first_observed_year: number;
  last_observed_year: number;
  observation_count: number;
  company_count: number;
  resolved_at: string;
}

export interface CountryPersonObservation {
  observation_id: string;
  source: string;
  source_record_id: string;
  source_person_key: string;
  company_id: string;
  company_name: string;
  observed_first_name: string;
  observed_last_name: string;
  observed_full_name: string;
  role_original: string;
  role_kind: string;
  signatory_kind: string;
  fiscal_year: number;
  source_statement_key: string;
  match_method: string;
  match_status: string;
  confidence: number;
}

export type CountryPersonRelationshipKind =
  "leadership" | "governance" | "external_audit" | "report_signature" | "other";

export interface CountryPersonIdentifier {
  identifier_id: string;
  source: string;
  identifier_kind: string;
  identifier_value: string;
  observation_id: string;
  is_public: number;
}

export type CountryPersonContactKind = "email" | "phone" | "website" | "social";

export interface CountryPersonContact {
  contact_kind: CountryPersonContactKind;
  contact_value: string;
  source: string;
  observation_id: string;
}

export interface CountryPersonCompanyConnection {
  company_id: string;
  company_name: string;
  role_kind: string;
  role_original: string;
  relationship_kind: CountryPersonRelationshipKind;
  first_year: number;
  last_year: number;
  observation_count: number;
}

export interface CountryPersonSuggestion {
  person: CountryPersonSummary;
  connections: CountryPersonCompanyConnection[];
  reason: "compatible_relationship_and_name";
}

export interface CountryPersonDetail {
  person: CountryPersonSummary;
  observations: CountryPersonObservation[];
  identifiers: CountryPersonIdentifier[];
  contacts: CountryPersonContact[];
  correction_reviews: CountryPersonCorrectionReview[];
}

export type CountryPersonCorrectionKind =
  "reassign" | "split" | "merge" | "undo";

export interface CountryPersonCorrection {
  correction_id: string;
  review_id: string;
  observation_id: string | null;
  from_person_id: string;
  to_person_id: string;
  correction_kind: CountryPersonCorrectionKind;
  reason: string;
  decided_by: string;
  supersedes_correction_id: string | null;
  created_at: string;
  is_current: number;
}

export interface CountryPersonCorrectionReview {
  review_id: string;
  correction_kind: CountryPersonCorrectionKind;
  reason: string;
  decided_by: string;
  created_at: string;
  corrections: CountryPersonCorrection[];
  is_current: boolean;
  is_applied: boolean;
}

export type CountryPersonCorrectionCommand =
  | {
      kind: "reassign";
      countryIso2: string;
      sourcePersonId: string;
      targetPersonId: string;
      observationIds: string[];
      reason: string;
    }
  | {
      kind: "split";
      countryIso2: string;
      sourcePersonId: string;
      observationIds: string[];
      reason: string;
    }
  | {
      kind: "merge";
      countryIso2: string;
      sourcePersonId: string;
      targetPersonId: string;
      reason: string;
    }
  | {
      kind: "undo";
      countryIso2: string;
      sourcePersonId: string;
      reviewId: string;
      reason: string;
    };

export interface CountryPersonCorrectionResult {
  reviewId: string;
  targetPersonId: string;
  correctionCount: number;
}

export interface CountryPeopleSearchResult {
  rows: CountryPersonSummary[];
  total: number;
}

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const CORRECTION_ACTOR = "backoffice";
const CONTACT_KINDS: Readonly<Record<string, CountryPersonContactKind>> = {
  email: "email",
  e_mail: "email",
  phone: "phone",
  telephone: "phone",
  mobile: "phone",
  website: "website",
  url: "website",
  linkedin: "social",
  social: "social",
};

const COUNTRY_PERSON_SUMMARY_SELECT = `SELECT
  p.country_iso2 AS country_iso2,
  toString(p.person_id) AS person_id,
  p.preferred_name AS preferred_name,
  p.preferred_name_normalized AS preferred_name_normalized,
  p.resolution_status AS resolution_status,
  p.resolution_method AS resolution_method,
  toString(p.merged_into_person_id) AS merged_into_person_id,
  p.first_observed_year AS first_observed_year,
  p.last_observed_year AS last_observed_year,
  p.observation_count AS observation_count,
  p.company_count AS company_count,
  toString(p.resolved_at) AS resolved_at
FROM country_person AS p`;

export function normalizeCountryPersonName(name: string): string {
  return name.normalize("NFKC").trim().replace(/\s+/g, " ").toLowerCase();
}

export function countryPersonRelationshipKind(
  roleKind: string,
  signatoryKind: string,
): CountryPersonRelationshipKind {
  if (roleKind === "auditor" || signatoryKind === "auditor") {
    return "external_audit";
  }
  if (roleKind === "ceo") return "leadership";
  if (
    ["chairman", "board_member", "deputy_board_member", "liquidator"].includes(
      roleKind,
    )
  ) {
    return "governance";
  }
  if (roleKind === "unknown") return "report_signature";
  return "other";
}

function matchableRelationshipKinds(
  observations: CountryPersonObservation[],
): CountryPersonRelationshipKind[] {
  const kinds = new Set(
    observations.map((observation) =>
      countryPersonRelationshipKind(
        observation.role_kind,
        observation.signatory_kind,
      ),
    ),
  );
  if (kinds.size > 1) kinds.delete("report_signature");
  return [...kinds];
}

function separatePublicContacts(identifiers: CountryPersonIdentifier[]): {
  identifiers: CountryPersonIdentifier[];
  contacts: CountryPersonContact[];
} {
  const publicIdentifiers: CountryPersonIdentifier[] = [];
  const contacts: CountryPersonContact[] = [];
  for (const identifier of identifiers) {
    if (identifier.is_public !== 1) continue;
    const contactKind = CONTACT_KINDS[identifier.identifier_kind.toLowerCase()];
    if (contactKind) {
      contacts.push({
        contact_kind: contactKind,
        contact_value: identifier.identifier_value,
        source: identifier.source,
        observation_id: identifier.observation_id,
      });
    } else {
      publicIdentifiers.push(identifier);
    }
  }
  return { identifiers: publicIdentifiers, contacts };
}

/**
 * Bridges company-management serving rows published before person IDs were
 * added. Ambiguous names deliberately remain unlinked until the serving table
 * is republished with an observation-level identity.
 */
export async function resolveCountryPersonProfilesForCompany(
  countryIso2: string,
  companyId: string,
  names: string[],
): Promise<Map<string, string>> {
  const country = countryIso2.trim().toUpperCase();
  const company = companyId.trim();
  const normalizedNames = [
    ...new Set(names.map(normalizeCountryPersonName).filter(Boolean)),
  ];
  if (
    !/^[A-Z]{2}$/.test(country) ||
    company === "" ||
    normalizedNames.length === 0
  ) {
    return new Map();
  }

  const rows = await chQuery<{
    observed_name_normalized: string;
    person_id: string;
  }>(
    `SELECT
       o.observed_name_normalized,
       toString(any(m.person_id)) AS person_id
     FROM country_person_observation AS o
     INNER JOIN country_person_match AS m
       ON m.country_iso2 = o.country_iso2
      AND m.observation_id = o.observation_id
     WHERE o.country_iso2 = {country:String}
       AND o.company_id = {companyId:String}
       AND o.observed_name_normalized IN {names:Array(String)}
     GROUP BY o.observed_name_normalized
     HAVING uniqExact(m.person_id) = 1`,
    { country, companyId: company, names: normalizedNames },
  );
  return new Map(
    rows.map((row) => [
      normalizeCountryPersonName(row.observed_name_normalized),
      row.person_id,
    ]),
  );
}

export async function searchCountryPeople(
  query: string,
): Promise<CountryPeopleSearchResult> {
  const normalized = query.trim().toLowerCase();
  if (normalized.length < 2) return { rows: [], total: 0 };

  const params = { pattern: `%${normalized}%` };
  const [rows, totals] = await Promise.all([
    chQuery<CountryPersonSummary>(
      `${COUNTRY_PERSON_SUMMARY_SELECT}
       WHERE p.resolution_status != 'merged'
         AND p.preferred_name_normalized LIKE {pattern:String}
       ORDER BY p.company_count DESC, p.observation_count DESC, p.country_iso2, p.preferred_name_normalized
       LIMIT 50`,
      params,
    ),
    chQuery<{ total: string }>(
      `SELECT count() AS total
       FROM country_person
       WHERE resolution_status != 'merged'
         AND preferred_name_normalized LIKE {pattern:String}`,
      params,
    ),
  ]);
  return { rows, total: Number(totals[0]?.total ?? 0) };
}

export async function searchCountryPersonTargets(
  countryIso2: string,
  query: string,
  sourcePersonId: string,
): Promise<CountryPersonSummary[]> {
  const country = countryIso2.trim().toUpperCase();
  const normalized = query.trim().toLowerCase();
  if (
    !/^[A-Z]{2}$/.test(country) ||
    !UUID_PATTERN.test(sourcePersonId) ||
    normalized.length < 2
  ) {
    return [];
  }

  const sharedWhere = `p.country_iso2 = {country:String}
       AND p.person_id != {sourcePersonId:UUID}
       AND p.resolution_status != 'merged'`;
  if (UUID_PATTERN.test(normalized)) {
    return chQuery<CountryPersonSummary>(
      `${COUNTRY_PERSON_SUMMARY_SELECT}
       WHERE ${sharedWhere}
         AND p.person_id = {targetPersonId:UUID}
       LIMIT 1`,
      { country, sourcePersonId, targetPersonId: normalized },
    );
  }

  return chQuery<CountryPersonSummary>(
    `${COUNTRY_PERSON_SUMMARY_SELECT}
     WHERE ${sharedWhere}
       AND p.preferred_name_normalized LIKE {pattern:String}
     ORDER BY
       p.preferred_name_normalized = {normalized:String} DESC,
       startsWith(p.preferred_name_normalized, {normalized:String}) DESC,
       p.company_count DESC,
       p.observation_count DESC,
       p.preferred_name_normalized,
       p.person_id
     LIMIT 12`,
    {
      country,
      sourcePersonId,
      normalized,
      pattern: `%${normalized}%`,
    },
  );
}

export async function getCountryPerson(
  countryIso2: string,
  personId: string,
): Promise<CountryPersonDetail | null> {
  const country = countryIso2.trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(country) || !UUID_PATTERN.test(personId)) return null;

  const params = { country, personId };
  const people = await chQuery<CountryPersonSummary>(
    `${COUNTRY_PERSON_SUMMARY_SELECT}
     WHERE p.country_iso2 = {country:String} AND p.person_id = {personId:UUID}
     LIMIT 1`,
    params,
  );
  if (people.length === 0) return null;

  const [observations, identifiers, corrections] = await Promise.all([
    chQuery<CountryPersonObservation>(
      `SELECT
         toString(o.observation_id) AS observation_id,
         o.source,
         o.source_record_id,
         o.source_person_key,
         o.company_id,
         o.company_name,
         o.observed_first_name,
         o.observed_last_name,
         o.observed_full_name,
         o.role_original,
         o.role_kind,
         o.signatory_kind,
         o.fiscal_year,
         o.source_statement_key,
         m.match_method,
         m.match_status,
         m.confidence
       FROM country_person_observation AS o
       INNER JOIN country_person_match AS m
         ON m.country_iso2 = o.country_iso2
        AND m.observation_id = o.observation_id
       WHERE o.country_iso2 = {country:String} AND m.person_id = {personId:UUID}
       ORDER BY o.fiscal_year DESC, o.company_name, o.source_statement_key, o.source_person_key
       LIMIT 1000`,
      params,
    ),
    chQuery<CountryPersonIdentifier>(
      `SELECT
         toString(identifier_id) AS identifier_id,
         source,
         identifier_kind,
         identifier_value,
         toString(observation_id) AS observation_id,
         is_public
       FROM country_person_identifier
       WHERE country_iso2 = {country:String} AND person_id = {personId:UUID}
       ORDER BY source, identifier_kind, identifier_value`,
      params,
    ),
    chQuery<CountryPersonCorrection>(
      `WITH current_observation_corrections AS (
         SELECT c.country_iso2, c.observation_id,
           argMax(c.correction_id, (c.created_at, c.correction_id)) AS current_correction_id
         FROM country_person_correction AS c
         WHERE c.observation_id IS NOT NULL
         GROUP BY c.country_iso2, c.observation_id
       ), current_person_corrections AS (
         SELECT c.country_iso2, c.from_person_id,
           argMax(c.correction_id, (c.created_at, c.correction_id)) AS current_correction_id
         FROM country_person_correction AS c
         WHERE c.observation_id IS NULL
         GROUP BY c.country_iso2, c.from_person_id
       )
       SELECT
         toString(c.correction_id) AS correction_id,
         toString(c.review_id) AS review_id,
         toString(c.observation_id) AS observation_id,
         toString(c.from_person_id) AS from_person_id,
         toString(c.to_person_id) AS to_person_id,
         c.correction_kind,
         c.reason,
         c.decided_by,
         toString(c.supersedes_correction_id) AS supersedes_correction_id,
         toString(c.created_at) AS created_at,
         toUInt8(if(
           c.observation_id IS NULL,
           c.correction_id = pc.current_correction_id,
           c.correction_id = oc.current_correction_id
         )) AS is_current
       FROM country_person_correction AS c
       LEFT JOIN current_observation_corrections AS oc
         ON oc.country_iso2 = c.country_iso2
        AND oc.observation_id = c.observation_id
       LEFT JOIN current_person_corrections AS pc
         ON pc.country_iso2 = c.country_iso2
        AND pc.from_person_id = c.from_person_id
       WHERE c.country_iso2 = {country:String}
         AND (c.from_person_id = {personId:UUID} OR c.to_person_id = {personId:UUID})
       ORDER BY c.created_at DESC, c.correction_id DESC
       LIMIT 1000`,
      params,
    ),
  ]);
  const publicProfileData = separatePublicContacts(identifiers);
  return {
    person: people[0],
    observations,
    identifiers: publicProfileData.identifiers,
    contacts: publicProfileData.contacts,
    correction_reviews: groupCorrectionReviews(
      corrections,
      people[0].resolved_at,
    ),
  };
}

interface CountryPersonSuggestionRow extends CountryPersonSummary {
  company_id: string;
  company_name: string;
  role_kind: string;
  role_original: string;
  relationship_kind: CountryPersonRelationshipKind;
  first_year: number;
  last_year: number;
  company_observation_count: number;
}

export async function findPossibleCountryPersonMatches(
  person: CountryPersonSummary,
  observations: CountryPersonObservation[],
): Promise<CountryPersonSuggestion[]> {
  const relationshipKinds = matchableRelationshipKinds(observations);
  if (
    !/^[A-Z]{2}$/.test(person.country_iso2) ||
    !UUID_PATTERN.test(person.person_id) ||
    person.preferred_name_normalized.trim() === "" ||
    relationshipKinds.length === 0
  ) {
    return [];
  }

  const rows = await chQuery<CountryPersonSuggestionRow>(
    `WITH candidate_observations AS (
       SELECT
         o.*,
         multiIf(
           o.role_kind = 'auditor' OR o.signatory_kind = 'auditor', 'external_audit',
           o.role_kind = 'ceo', 'leadership',
           o.role_kind IN ('chairman', 'board_member', 'deputy_board_member', 'liquidator'), 'governance',
           o.role_kind = 'unknown', 'report_signature',
           'other'
         ) AS relationship_kind
       FROM country_person_observation AS o
       WHERE o.country_iso2 = {country:String}
     )
     SELECT
       p.country_iso2 AS country_iso2,
       toString(p.person_id) AS person_id,
       p.preferred_name AS preferred_name,
       p.preferred_name_normalized AS preferred_name_normalized,
       p.resolution_status AS resolution_status,
       p.resolution_method AS resolution_method,
       toString(p.merged_into_person_id) AS merged_into_person_id,
       p.first_observed_year AS first_observed_year,
       p.last_observed_year AS last_observed_year,
       p.observation_count AS observation_count,
       p.company_count AS company_count,
       toString(p.resolved_at) AS resolved_at,
       o.company_id,
       argMax(o.company_name, (o.fiscal_year, o.source_statement_key)) AS company_name,
       argMax(o.role_kind, (o.fiscal_year, o.source_statement_key)) AS role_kind,
       argMax(o.role_original, (o.fiscal_year, o.source_statement_key)) AS role_original,
       o.relationship_kind,
       toInt32(minIf(o.fiscal_year, o.fiscal_year > 0)) AS first_year,
       toInt32(max(o.fiscal_year)) AS last_year,
       toUInt32(count()) AS company_observation_count
     FROM country_person AS p
     INNER JOIN country_person_match AS m
       ON m.country_iso2 = p.country_iso2
      AND m.person_id = p.person_id
     INNER JOIN candidate_observations AS o
       ON o.country_iso2 = m.country_iso2
      AND o.observation_id = m.observation_id
     WHERE p.country_iso2 = {country:String}
       AND p.person_id != {personId:UUID}
       AND p.resolution_status != 'merged'
       AND p.preferred_name_normalized = {normalizedName:String}
       AND o.relationship_kind IN {relationshipKinds:Array(String)}
     GROUP BY
       p.country_iso2, p.person_id, p.preferred_name,
       p.preferred_name_normalized, p.resolution_status,
       p.resolution_method, p.merged_into_person_id,
       p.first_observed_year, p.last_observed_year,
       p.observation_count, p.company_count, p.resolved_at,
       o.company_id, o.relationship_kind
     ORDER BY p.company_count DESC, p.observation_count DESC,
       last_year DESC, company_name
     LIMIT 100`,
    {
      country: person.country_iso2,
      personId: person.person_id,
      normalizedName: person.preferred_name_normalized,
      relationshipKinds,
    },
  );

  const suggestions = new Map<string, CountryPersonSuggestion>();
  for (const row of rows) {
    const existing = suggestions.get(row.person_id);
    const connection: CountryPersonCompanyConnection = {
      company_id: row.company_id,
      company_name: row.company_name,
      role_kind: row.role_kind,
      role_original: row.role_original,
      relationship_kind: row.relationship_kind,
      first_year: Number(row.first_year),
      last_year: Number(row.last_year),
      observation_count: Number(row.company_observation_count),
    };
    if (existing) {
      existing.connections.push(connection);
      continue;
    }
    suggestions.set(row.person_id, {
      person: {
        country_iso2: row.country_iso2,
        person_id: row.person_id,
        preferred_name: row.preferred_name,
        preferred_name_normalized: row.preferred_name_normalized,
        resolution_status: row.resolution_status,
        resolution_method: row.resolution_method,
        merged_into_person_id: row.merged_into_person_id,
        first_observed_year: Number(row.first_observed_year),
        last_observed_year: Number(row.last_observed_year),
        observation_count: Number(row.observation_count),
        company_count: Number(row.company_count),
        resolved_at: row.resolved_at,
      },
      connections: [connection],
      reason: "compatible_relationship_and_name",
    });
  }
  return [...suggestions.values()];
}

function groupCorrectionReviews(
  corrections: CountryPersonCorrection[],
  profileResolvedAt: string,
): CountryPersonCorrectionReview[] {
  const reviews = new Map<string, CountryPersonCorrectionReview>();
  for (const correction of corrections) {
    const existing = reviews.get(correction.review_id);
    if (existing) {
      existing.corrections.push(correction);
      existing.is_current &&= correction.is_current === 1;
      existing.is_applied &&= correction.created_at <= profileResolvedAt;
      continue;
    }
    reviews.set(correction.review_id, {
      review_id: correction.review_id,
      correction_kind: correction.correction_kind,
      reason: correction.reason,
      decided_by: correction.decided_by,
      created_at: correction.created_at,
      corrections: [correction],
      is_current: correction.is_current === 1,
      is_applied: correction.created_at <= profileResolvedAt,
    });
  }
  return Array.from(reviews.values());
}

export class PersonCorrectionValidationError extends Error {}

interface CurrentCorrectionPointer {
  correction_id: string;
  observation_id: string | null;
  from_person_id: string;
  to_person_id: string;
}

interface CountryPersonCorrectionInsertRow {
  country_iso2: string;
  correction_id: string;
  review_id: string;
  observation_id: string | null;
  from_person_id: string;
  to_person_id: string;
  correction_kind: CountryPersonCorrectionKind;
  reason: string;
  decided_by: string;
  supersedes_correction_id: string | null;
  created_at: string;
}

function validateCorrectionText(
  value: string,
  label: string,
  max: number,
): string {
  const normalized = value.trim();
  if (normalized.length < 2 || normalized.length > max) {
    throw new PersonCorrectionValidationError(
      `${label} must contain between 2 and ${max} characters.`,
    );
  }
  return normalized;
}

function validateCorrectionIdentity(
  countryIso2: string,
  personId: string,
): string {
  const country = countryIso2.trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(country) || !UUID_PATTERN.test(personId)) {
    throw new PersonCorrectionValidationError(
      "Invalid country person identity.",
    );
  }
  return country;
}

function uniqueObservationIds(observationIds: string[]): string[] {
  const unique = Array.from(new Set(observationIds));
  if (unique.length === 0 || unique.some((id) => !UUID_PATTERN.test(id))) {
    throw new PersonCorrectionValidationError(
      "Select at least one valid source observation.",
    );
  }
  return unique;
}

function correctionTimestamp(): string {
  return new Date().toISOString().replace("T", " ").replace("Z", "");
}

async function currentObservationCorrections(
  country: string,
  observationIds: string[],
): Promise<Map<string, CurrentCorrectionPointer>> {
  if (observationIds.length === 0) return new Map();
  const rows = await chQuery<CurrentCorrectionPointer>(
    `SELECT
       toString(argMax(c.correction_id, (c.created_at, c.correction_id))) AS correction_id,
       toString(c.observation_id) AS observation_id,
       toString(argMax(c.from_person_id, (c.created_at, c.correction_id))) AS from_person_id,
       toString(argMax(c.to_person_id, (c.created_at, c.correction_id))) AS to_person_id
     FROM country_person_correction AS c
     WHERE c.country_iso2 = {country:String}
       AND c.observation_id IN {observationIds:Array(UUID)}
     GROUP BY c.observation_id`,
    { country, observationIds },
  );
  return new Map(rows.map((row) => [row.observation_id!, row]));
}

async function currentPersonCorrection(
  country: string,
  personId: string,
): Promise<CurrentCorrectionPointer | null> {
  const rows = await chQuery<CurrentCorrectionPointer>(
    `SELECT
       toString(argMax(c.correction_id, (c.created_at, c.correction_id))) AS correction_id,
       CAST(NULL, 'Nullable(String)') AS observation_id,
       toString(c.from_person_id) AS from_person_id,
       toString(argMax(c.to_person_id, (c.created_at, c.correction_id))) AS to_person_id
     FROM country_person_correction AS c
     WHERE c.country_iso2 = {country:String}
       AND c.observation_id IS NULL
       AND c.from_person_id = {personId:UUID}
     GROUP BY c.from_person_id`,
    { country, personId },
  );
  return rows[0] ?? null;
}

async function sourceHasActiveIncomingMerge(
  country: string,
  personId: string,
): Promise<boolean> {
  const rows = await chQuery<{ total: string }>(
    `SELECT count() AS total
     FROM (
       SELECT c.from_person_id,
         argMax(c.to_person_id, (c.created_at, c.correction_id)) AS current_target
       FROM country_person_correction AS c
       WHERE c.country_iso2 = {country:String} AND c.observation_id IS NULL
       GROUP BY c.from_person_id
     ) AS current_merges
     WHERE current_merges.current_target != current_merges.from_person_id
       AND current_merges.current_target = {personId:UUID}`,
    { country, personId },
  );
  return Number(rows[0]?.total ?? 0) > 0;
}

function assertNoPendingCorrection(detail: CountryPersonDetail): void {
  if (
    detail.correction_reviews.some(
      (review) => review.is_current && !review.is_applied,
    )
  ) {
    throw new PersonCorrectionValidationError(
      "Wait for the pending correction to be applied before reviewing this identity again.",
    );
  }
}

function assertCorrectionTarget(
  source: CountryPersonDetail,
  target: CountryPersonDetail | null,
): asserts target is CountryPersonDetail {
  if (!target || target.person.resolution_status === "merged") {
    throw new PersonCorrectionValidationError(
      "The target must be an active person ID in the same country.",
    );
  }
  if (source.person.person_id === target.person.person_id) {
    throw new PersonCorrectionValidationError(
      "The source and target person IDs must be different.",
    );
  }
  if (source.identifiers.length > 0 || target.identifiers.length > 0) {
    throw new PersonCorrectionValidationError(
      "Profiles with published identifiers require source-data review and cannot be manually combined here.",
    );
  }
  assertNoPendingCorrection(target);
}

function correctionRow({
  country,
  reviewId,
  observationId,
  fromPersonId,
  toPersonId,
  correctionKind,
  reason,
  supersedesCorrectionId,
  createdAt,
}: {
  country: string;
  reviewId: string;
  observationId: string | null;
  fromPersonId: string;
  toPersonId: string;
  correctionKind: CountryPersonCorrectionKind;
  reason: string;
  supersedesCorrectionId: string | null;
  createdAt: string;
}): CountryPersonCorrectionInsertRow {
  return {
    country_iso2: country,
    correction_id: randomUUID(),
    review_id: reviewId,
    observation_id: observationId,
    from_person_id: fromPersonId,
    to_person_id: toPersonId,
    correction_kind: correctionKind,
    reason,
    decided_by: CORRECTION_ACTOR,
    supersedes_correction_id: supersedesCorrectionId,
    created_at: createdAt,
  };
}

export async function applyCountryPersonCorrection(
  command: CountryPersonCorrectionCommand,
): Promise<CountryPersonCorrectionResult> {
  const country = validateCorrectionIdentity(
    command.countryIso2,
    command.sourcePersonId,
  );
  const reason = validateCorrectionText(command.reason, "Reason", 1000);
  const source = await getCountryPerson(country, command.sourcePersonId);
  if (!source || source.person.resolution_status === "merged") {
    throw new PersonCorrectionValidationError(
      "The source person identity is no longer active.",
    );
  }
  assertNoPendingCorrection(source);

  const reviewId = randomUUID();
  const createdAt = correctionTimestamp();

  if (command.kind === "merge") {
    validateCorrectionIdentity(country, command.targetPersonId);
    const target = await getCountryPerson(country, command.targetPersonId);
    assertCorrectionTarget(source, target);
    if (await sourceHasActiveIncomingMerge(country, command.sourcePersonId)) {
      throw new PersonCorrectionValidationError(
        "This identity already has merged aliases. Undo that merge before merging it again.",
      );
    }
    const current = await currentPersonCorrection(
      country,
      command.sourcePersonId,
    );
    await chInsertPersonCorrections([
      correctionRow({
        country,
        reviewId,
        observationId: null,
        fromPersonId: command.sourcePersonId,
        toPersonId: command.targetPersonId,
        correctionKind: "merge",
        reason,
        supersedesCorrectionId: current?.correction_id ?? null,
        createdAt,
      }),
    ]);
    return {
      reviewId,
      targetPersonId: command.targetPersonId,
      correctionCount: 1,
    };
  }

  if (command.kind === "undo") {
    return undoCountryPersonCorrection({
      country,
      sourcePersonId: command.sourcePersonId,
      reviewId: command.reviewId,
      reason,
      createdAt,
    });
  }

  const observationIds = uniqueObservationIds(command.observationIds);
  const sourceObservationIds = new Set(
    source.observations.map((observation) => observation.observation_id),
  );
  if (observationIds.some((id) => !sourceObservationIds.has(id))) {
    throw new PersonCorrectionValidationError(
      "Every selected observation must currently belong to the source identity.",
    );
  }
  if (observationIds.length >= source.person.observation_count) {
    throw new PersonCorrectionValidationError(
      command.kind === "split"
        ? "A split must leave at least one observation on the original identity."
        : "Use Merge identities when moving every observation.",
    );
  }
  const selectedIdentifiers = new Set(
    source.identifiers
      .filter((identifier) =>
        observationIds.includes(identifier.observation_id),
      )
      .map((identifier) => identifier.observation_id),
  );
  if (selectedIdentifiers.size > 0) {
    throw new PersonCorrectionValidationError(
      "Observations carrying a published identifier cannot be manually reassigned.",
    );
  }

  let targetPersonId: string;
  if (command.kind === "reassign") {
    validateCorrectionIdentity(country, command.targetPersonId);
    const target = await getCountryPerson(country, command.targetPersonId);
    assertCorrectionTarget(source, target);
    targetPersonId = command.targetPersonId;
  } else {
    targetPersonId = randomUUID();
  }

  const currentCorrections = await currentObservationCorrections(
    country,
    observationIds,
  );
  const rows = observationIds.map((observationId) =>
    correctionRow({
      country,
      reviewId,
      observationId,
      fromPersonId: command.sourcePersonId,
      toPersonId: targetPersonId,
      correctionKind: command.kind,
      reason,
      supersedesCorrectionId:
        currentCorrections.get(observationId)?.correction_id ?? null,
      createdAt,
    }),
  );
  await chInsertPersonCorrections(rows);
  return {
    reviewId,
    targetPersonId,
    correctionCount: rows.length,
  };
}

async function undoCountryPersonCorrection({
  country,
  sourcePersonId,
  reviewId,
  reason,
  createdAt,
}: {
  country: string;
  sourcePersonId: string;
  reviewId: string;
  reason: string;
  createdAt: string;
}): Promise<CountryPersonCorrectionResult> {
  if (!UUID_PATTERN.test(reviewId)) {
    throw new PersonCorrectionValidationError("Invalid correction review ID.");
  }
  const corrections = await correctionReviewRows(country, reviewId);
  if (
    corrections.length === 0 ||
    !corrections.some(
      (row) =>
        row.from_person_id === sourcePersonId ||
        row.to_person_id === sourcePersonId,
    )
  ) {
    throw new PersonCorrectionValidationError("Correction review not found.");
  }
  if (corrections.some((row) => row.is_current !== 1)) {
    throw new PersonCorrectionValidationError(
      "This review has already been superseded and cannot be undone again.",
    );
  }

  const supersededIds = corrections.flatMap((row) =>
    row.supersedes_correction_id ? [row.supersedes_correction_id] : [],
  );
  const supersededTargets = new Map<string, string>();
  if (supersededIds.length > 0) {
    const rows = await chQuery<{ correction_id: string; to_person_id: string }>(
      `SELECT toString(correction_id) AS correction_id,
         toString(to_person_id) AS to_person_id
       FROM country_person_correction
       WHERE country_iso2 = {country:String}
         AND correction_id IN {correctionIds:Array(UUID)}`,
      { country, correctionIds: supersededIds },
    );
    for (const row of rows) {
      supersededTargets.set(row.correction_id, row.to_person_id);
    }
  }

  const undoReviewId = randomUUID();
  const rows = corrections.map((correction) => {
    const targetPersonId = correction.supersedes_correction_id
      ? (supersededTargets.get(correction.supersedes_correction_id) ??
        correction.from_person_id)
      : correction.from_person_id;
    return correctionRow({
      country,
      reviewId: undoReviewId,
      observationId: correction.observation_id,
      fromPersonId:
        correction.observation_id === null
          ? correction.from_person_id
          : correction.to_person_id,
      toPersonId: targetPersonId,
      correctionKind: "undo",
      reason,
      supersedesCorrectionId: correction.correction_id,
      createdAt,
    });
  });
  await chInsertPersonCorrections(rows);
  return {
    reviewId: undoReviewId,
    targetPersonId: rows[0].to_person_id,
    correctionCount: rows.length,
  };
}

async function correctionReviewRows(
  country: string,
  reviewId: string,
): Promise<CountryPersonCorrection[]> {
  return chQuery<CountryPersonCorrection>(
    `WITH current_observation_corrections AS (
       SELECT c.country_iso2, c.observation_id,
         argMax(c.correction_id, (c.created_at, c.correction_id)) AS current_correction_id
       FROM country_person_correction AS c
       WHERE c.observation_id IS NOT NULL
       GROUP BY c.country_iso2, c.observation_id
     ), current_person_corrections AS (
       SELECT c.country_iso2, c.from_person_id,
         argMax(c.correction_id, (c.created_at, c.correction_id)) AS current_correction_id
       FROM country_person_correction AS c
       WHERE c.observation_id IS NULL
       GROUP BY c.country_iso2, c.from_person_id
     )
     SELECT
       toString(c.correction_id) AS correction_id,
       toString(c.review_id) AS review_id,
       toString(c.observation_id) AS observation_id,
       toString(c.from_person_id) AS from_person_id,
       toString(c.to_person_id) AS to_person_id,
       c.correction_kind,
       c.reason,
       c.decided_by,
       toString(c.supersedes_correction_id) AS supersedes_correction_id,
       toString(c.created_at) AS created_at,
       toUInt8(if(
         c.observation_id IS NULL,
         c.correction_id = pc.current_correction_id,
         c.correction_id = oc.current_correction_id
       )) AS is_current
     FROM country_person_correction AS c
     LEFT JOIN current_observation_corrections AS oc
       ON oc.country_iso2 = c.country_iso2
      AND oc.observation_id = c.observation_id
     LEFT JOIN current_person_corrections AS pc
       ON pc.country_iso2 = c.country_iso2
      AND pc.from_person_id = c.from_person_id
     WHERE c.country_iso2 = {country:String} AND c.review_id = {reviewId:UUID}
     ORDER BY c.correction_id`,
    { country, reviewId },
  );
}
