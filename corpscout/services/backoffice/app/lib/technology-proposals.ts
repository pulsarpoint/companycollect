import casefold from "~/lib/unicode-casefold.json";

export function normalizeTechnologyName(value: string): string {
  const folds = casefold as Record<string, string>;
  return Array.from(
    value.normalize("NFKC"),
    (character) => folds[character] ?? character.toLowerCase(),
  )
    .join("")
    .split(
      /[\u0009-\u000d\u001c-\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+/u,
    )
    .filter(Boolean)
    .join(" ");
}

export class TechnologyValidationError extends Error {}

export const TECHNOLOGY_REVIEW_LABELS = {
  approve_new: "Approved new technology",
  map_existing: "Mapped to existing technology",
  reject: "Rejected",
} as const;

export function technologyObject(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TechnologyValidationError("Expected an object.");
  }
  return value as Record<string, unknown>;
}

export function technologyText(
  value: unknown,
  field: string,
  max = 2000,
): string {
  if (typeof value !== "string" || !value.trim() || value.length > max) {
    throw new TechnologyValidationError(
      `${field} requires nonempty text (up to ${max} characters).`,
    );
  }
  return value.trim();
}

export function technologyUrl(value: unknown, field: string): string {
  const text = technologyText(value, field, 4000);
  let url: URL;
  try {
    url = new URL(text);
  } catch {
    throw new TechnologyValidationError(`${field} requires an HTTP(S) URL.`);
  }
  if (
    !["http:", "https:"].includes(url.protocol) ||
    !url.hostname ||
    url.username ||
    url.password
  ) {
    throw new TechnologyValidationError(
      `${field} requires an HTTP(S) URL without credentials.`,
    );
  }
  return text;
}

export function technologyArray(
  value: unknown,
  field: string,
  max: number,
): unknown[] {
  if (!Array.isArray(value) || value.length > max) {
    throw new TechnologyValidationError(
      `${field} requires an array of at most ${max} items.`,
    );
  }
  return value;
}

export interface ProposedTechnologyRow {
  proposal_id: string;
  run_id: string;
  record_id: string;
  observed_name: string;
  proposed_name: string;
  proposed_key: string;
  description: string;
  website: string;
  category_ids: number[];
  category_suggestion: string;
  saas: number | null;
  oss: number | null;
  pricing: string[];
  company: string;
  job_employer: string;
  job_title: string;
  job_url: string;
  signal: string;
  scope: string;
  context: string;
  as_of: string;
  alternative_group: string;
  site_url: string;
  sources: {
    url: string;
    html_sha256: string;
    fetched_at: string;
    evidence: string[];
    evidence_status: string;
  }[];
  catalog_version: string;
  search_queries: string[];
  search_candidates: string[];
  proposal_reason: string;
  existing_technology: string;
  model: string;
  received_at: string;
}

export interface TechnologyReview {
  review_id: string;
  proposal_id: string;
  decision: "approve_new" | "map_existing" | "reject";
  technology: string;
  description: string;
  website: string;
  category_ids: number[];
  categories: string[];
  groups: string[];
  saas: number;
  oss: number;
  pricing: string[];
  alias: string;
  alias_key: string;
  reviewed_by: string;
  review_note: string;
  source_references: string[];
  reviewed_at: string;
}
