/**
 * Client-safe validator for the Sweden company-info field-value store
 * (`corpscout.se_company_info_field_value`, migration 000371).
 *
 * The store replaced the company-info correction ledger: there are no kinds,
 * no evidence hashes and no undo chain here, because a field's live value is
 * simply the row written last for it (greatest `(created_at, value_id)`), and
 * an undo is just the previous value written again -- or NULL, which releases
 * the field back to the value the pipeline computes. So this validator only
 * has to answer "is this one row insertable?".
 *
 * WHICH fields and sources exist is not this module's to say: the field
 * registry (dagster_v3 code, exported to corpscout.se_company_field_registry,
 * spec 2026-09-02 section 4) declares them, and the table's CHECK constraints
 * are widened to the same lists by migration. The caller hands the registry's
 * vocabulary in (`fieldVocabulary(await loadFieldRegistry())`), so this file
 * stays importable by the client bundle. The per-source `source_ref` rule
 * mirrors what Dagster reads back out of the column.
 *
 * The ADDRESS and PERSON ledgers are unrelated and still correction-shaped;
 * their validators (`se-address-corrections.ts`, `se-person-corrections.ts`)
 * are untouched by this module.
 */

/** A reviewer's own wording. Not a registry source (decisions win by
 * construction, spec 4.1), but a valid `source` of a decision row (spec 6). */
export const REVIEWER_SOURCE = "reviewer";

/** What the validator checks a row against: the registry's field names, and
 * the union of every field's sources plus `reviewer`. */
export interface SeInfoFieldVocabulary {
  fields: string[];
  sources: string[];
}

/** Derives the vocabulary from a registry export (`FieldRegistry` from
 * se-company-field-registry.server.ts, typed structurally so this module
 * never imports a `.server` module). Sources keep first-seen order. */
export function fieldVocabulary(registry: {
  fields: ReadonlyArray<{ field: string; sources: ReadonlyArray<string> }>;
}): SeInfoFieldVocabulary {
  const sources = new Set<string>();
  for (const entry of registry.fields) {
    for (const source of entry.sources) sources.add(source);
  }
  sources.add(REVIEWER_SOURCE);
  return {
    fields: registry.fields.map((entry) => entry.field),
    sources: [...sources],
  };
}

export class SeInfoFieldValueValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SeInfoFieldValueValidationError";
  }
}

export interface SeInfoFieldValueInput {
  companyId: string;
  field: string;
  /** The text to publish, or null to release the field to the pipeline. */
  value: string | null;
  source: string;
  /** The record this text came from: a source_record_uid, a suggestion_id for
   * `llm`, and nothing at all for `reviewer`. */
  sourceRef?: string;
  /** When the source observed it (the artifact's observed_at / the
   * suggestion's created_at); null when there is no such moment. */
  sourceAt?: string | null;
  note?: string;
}

/** One insertable `se_company_info_field_value` row, minus the columns the
 * server fills (`value_id`, `decided_by`, `created_at`). */
export interface SeInfoFieldValueDraft {
  company_id: string;
  field: string;
  value: string | null;
  source: string;
  source_ref: string;
  source_at: string | null;
  note: string;
}

// Legal entities carry a 10-digit organisationsnummer; sole traders (enskild
// firma) carry a 12-digit personnummer-based id. Mirrors has_company in
// migration 000371 -- both are published to se_company_info.
const COMPANY_ID_PATTERN = /^([0-9]{10}|[0-9]{12})$/;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const NOTE_MAX_LENGTH = 1000;

function fail(message: string): never {
  throw new SeInfoFieldValueValidationError(message);
}

/**
 * `source_ref` is validated per source because each source means something
 * different by it: `llm` names the suggestion by id and Dagster parses it as a
 * UUID (an unparseable one loses the suggestion link), the artifact sources
 * name a source_record_uid, and a reviewer's own wording comes from no record
 * at all -- so its ref is forced to '' whatever the form posted, rather than
 * carrying a stray value into the column.
 */
function sourceRefFor(source: string, raw: string): string {
  if (source === REVIEWER_SOURCE) return "";
  const clean = raw.trim();
  if (source === "llm") {
    if (!UUID_PATTERN.test(clean)) fail("source_ref must be a UUID.");
    return clean.toLowerCase();
  }
  if (clean === "") fail("source_ref is required.");
  return clean;
}

export function validateSeInfoFieldValue(
  input: SeInfoFieldValueInput,
  registry: SeInfoFieldVocabulary,
): SeInfoFieldValueDraft {
  const companyId = input.companyId.trim();
  if (!COMPANY_ID_PATTERN.test(companyId)) {
    fail("Company must be a 10-digit or 12-digit Swedish company id.");
  }
  if (!registry.fields.includes(input.field)) fail("Unknown field.");
  if (!registry.sources.includes(input.source)) fail("Unknown source.");

  // null is the release instruction ("hand this field back to the pipeline"),
  // which is a decision in its own right -- keep it null instead of coercing
  // it to '' , which would pin the published column to an empty string. An
  // absent value (undefined, which is what FormData plumbing hands over for a
  // field that was not posted) means the same thing, so `== null` covers both
  // rather than throwing on the property access.
  let value: string | null = null;
  if (input.value != null) {
    value = input.value.trim();
    if (value === "") fail("Value cannot be empty.");
  }

  const note = (input.note ?? "").trim();
  if (note.length > NOTE_MAX_LENGTH) {
    fail(`Note is too long (max ${NOTE_MAX_LENGTH} characters).`);
  }

  // Passed through as text: the callers hand over a timestamp ClickHouse
  // already printed (an artifact's observed_at, a suggestion's created_at),
  // so re-parsing it here would only risk changing it.
  const sourceAt = (input.sourceAt ?? "").trim();

  return {
    company_id: companyId,
    field: input.field,
    value,
    source: input.source,
    source_ref: sourceRefFor(input.source, input.sourceRef ?? ""),
    source_at: sourceAt === "" ? null : sourceAt,
    note,
  };
}
