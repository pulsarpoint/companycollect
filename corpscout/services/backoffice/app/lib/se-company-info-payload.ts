/**
 * The payload of every `se_company_info_*` artifact table, in the order a
 * reviewer reads it, with the label each field is shown under.
 *
 * Client-safe on purpose: this is the ONE list of payload columns per source.
 * `se-company-info.server.ts` builds `ARTIFACT_ROWS_SQL`'s
 * `toJSONString(map(...))` from it (the Dagster `build_artifact_rows_sql`
 * convention -- every value `ifNull(toString(col), '')`, so typed NULLs arrive
 * as '' and numbers as text), and the review page renders it from the same
 * list. Keeping one list means a column added to migration 000297's tables is
 * projected and displayed by editing one array, and can never be selected but
 * unlabelled (or labelled but unselected).
 *
 * The keys and their order mirror the DDL (migrations 000297, 000300's
 * `activity_description_en` and 000306's two legal-form labels) minus the
 * envelope every leg reads by name
 * (`company_id`, `source_record_uid`, `observed_at`, `source_run_id`) and the
 * MATERIALIZED `evidence_hash`. ESEF is the one source whose display order
 * differs from its DDL order (entity name leads, the two JSON blobs trail);
 * map-key order does not affect the query, only the page.
 */

/** How the review page renders one payload value. */
export type ArtifactFieldKind = "text" | "url" | "wikidata-id" | "json-list";

export interface ArtifactPayloadField {
  key: string;
  label: string;
  kind: ArtifactFieldKind;
}

export const ARTIFACT_SOURCES = ["scb", "wikidata", "esef"] as const;
export type ArtifactSource = (typeof ARTIFACT_SOURCES)[number];

/** Heading each source's cards sit under. */
export const ARTIFACT_SOURCE_LABELS: Record<string, string> = {
  scb: "SCB register",
  wikidata: "Wikidata",
  esef: "ESEF filing",
};

export function artifactSourceLabel(source: string): string {
  return ARTIFACT_SOURCE_LABELS[source] ?? source;
}

export const ARTIFACT_PAYLOAD_FIELDS: Record<
  ArtifactSource,
  readonly ArtifactPayloadField[]
> = {
  scb: [
    { key: "legal_name", label: "Legal name", kind: "text" },
    { key: "legal_name_raw", label: "Raw name", kind: "text" },
    { key: "legal_form_code", label: "Legal form", kind: "text" },
    { key: "legal_form_label_sv", label: "Legal form (sv)", kind: "text" },
    { key: "legal_form_label_en", label: "Legal form (en)", kind: "text" },
    { key: "status", label: "Status", kind: "text" },
    { key: "incorporation_date", label: "Registration date", kind: "text" },
    { key: "dissolution_date", label: "Dissolution date", kind: "text" },
    {
      key: "activity_description",
      label: "Activity description (sv)",
      kind: "text",
    },
    {
      key: "activity_description_en",
      label: "Activity description (en)",
      kind: "text",
    },
    { key: "primary_sni_code", label: "SNI", kind: "text" },
    { key: "primary_nace_code", label: "NACE", kind: "text" },
  ],
  wikidata: [
    { key: "wikidata_id", label: "Wikidata id", kind: "wikidata-id" },
    { key: "wikidata_url", label: "Wikidata URL", kind: "url" },
    { key: "name", label: "Label", kind: "text" },
    { key: "official_name", label: "Official name", kind: "text" },
    { key: "company_description", label: "Description", kind: "text" },
    { key: "inception_date", label: "Inception", kind: "text" },
    { key: "legal_form_label", label: "Legal form", kind: "text" },
    { key: "industry_wikidata_id", label: "Industry id", kind: "text" },
    { key: "industry_label", label: "Industry", kind: "text" },
    { key: "headquarters_label", label: "Headquarters", kind: "text" },
    { key: "employee_count", label: "Employees", kind: "text" },
  ],
  esef: [
    { key: "entity_name", label: "Entity name", kind: "text" },
    { key: "lei", label: "LEI", kind: "text" },
    { key: "fiscal_year", label: "Fiscal year", kind: "text" },
    { key: "source_document_id", label: "Source document", kind: "text" },
    { key: "company_description", label: "Description", kind: "text" },
    { key: "description_language", label: "Description language", kind: "text" },
    {
      key: "description_confidence",
      label: "Description confidence",
      kind: "text",
    },
    {
      key: "products_and_services_json",
      label: "Products & services",
      kind: "json-list",
    },
    {
      key: "business_segments_json",
      label: "Business segments",
      kind: "json-list",
    },
  ],
};

export function artifactPayloadFields(
  source: string,
): readonly ArtifactPayloadField[] {
  return ARTIFACT_PAYLOAD_FIELDS[source as ArtifactSource] ?? [];
}

/** `industry_wikidata_id` -> `Industry wikidata id`, for a column this app
 * does not know about yet (a future migration's). */
function humanizeKey(key: string): string {
  const spaced = key.replace(/_/g, " ").trim();
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

export interface ArtifactPayloadEntry extends ArtifactPayloadField {
  value: string;
}

/**
 * Every field of one artifact row, known fields first in reading order (missing
 * ones as ''), then any key the payload carries that this app does not know --
 * a column added to the artifact table upstream still shows up on the page
 * (labelled from its name) instead of silently disappearing.
 */
export function artifactPayloadEntries(
  source: string,
  payload: Record<string, string>,
): ArtifactPayloadEntry[] {
  const known = artifactPayloadFields(source);
  const knownKeys = new Set(known.map((field) => field.key));
  const entries: ArtifactPayloadEntry[] = known.map((field) => ({
    ...field,
    value: payload[field.key] ?? "",
  }));
  for (const key of Object.keys(payload).sort()) {
    if (knownKeys.has(key)) continue;
    entries.push({
      key,
      label: humanizeKey(key),
      kind: "text",
      value: payload[key] ?? "",
    });
  }
  return entries;
}

/** One item of a JSON-blob field: the readable part, plus whatever else the
 * item carried (never dropped -- shown muted beside it). */
export interface JsonListItem {
  text: string;
  detail: string;
}

const ITEM_NAME_KEYS = ["name", "label", "title"] as const;

/**
 * The rest of a list item, in prose rather than JSON: the two keys the ESEF
 * extractor always writes read as `confidence 0.9 · E0010, E0015`, and any
 * other key as `key: value`, so nothing is dropped and nothing has to be read
 * as raw JSON on the page.
 */
function formatItemDetail(rest: Record<string, unknown>): string {
  const parts: string[] = [];
  for (const [key, value] of Object.entries(rest)) {
    if (key === "confidence" && typeof value === "number") {
      parts.push(`confidence ${value}`);
    } else if (key === "evidence_ids" && Array.isArray(value)) {
      if (value.length > 0) parts.push(value.map(String).join(", "));
    } else {
      parts.push(
        `${key}: ${typeof value === "string" ? value : JSON.stringify(value)}`,
      );
    }
  }
  return parts.join(" · ");
}

/**
 * A JSON-blob field rendered as a list. ESEF's products/segments blobs are
 * arrays of objects ({name, confidence, evidence_ids}) written by the
 * disclosure extractor; the public company page shows such an item by its
 * `name`, and this does the same, keeping the item's other fields as a muted
 * suffix rather than hiding them. Anything else (a bare array of strings,
 * unparseable text, a non-array) falls back so nothing is silently dropped.
 */
export function parseJsonList(raw: string): JsonListItem[] | null {
  if (raw.trim() === "") return [];
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!Array.isArray(parsed)) return null;
  return parsed.map((item) => {
    if (item === null || typeof item !== "object" || Array.isArray(item)) {
      return { text: typeof item === "string" ? item : JSON.stringify(item), detail: "" };
    }
    const record = item as Record<string, unknown>;
    const nameKey = ITEM_NAME_KEYS.find((key) => typeof record[key] === "string");
    if (nameKey === undefined) return { text: JSON.stringify(item), detail: "" };
    const rest = Object.fromEntries(
      Object.entries(record).filter(([key]) => key !== nameKey),
    );
    return { text: record[nameKey] as string, detail: formatItemDetail(rest) };
  });
}

/** Wikidata's own page for an entity, when the artifact carried no URL. */
export function wikidataHref(id: string, url: string): string {
  return url !== "" ? url : `https://www.wikidata.org/wiki/${encodeURIComponent(id)}`;
}
