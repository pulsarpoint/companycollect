import { randomUUID } from "node:crypto";
import { chCommand, chQuery } from "~/lib/clickhouse.server";
import {
  loadFieldRegistry,
  type FieldRegistry,
} from "~/lib/se-company-field-registry.server";

/**
 * Resolves ONE company's fields with the registry's generated SQL right after
 * a decision (spec 2026-09-02, section 9), so the reviewer sees the resolved
 * value on the reload instead of waiting for the sensor. Dagster runs the same
 * statements in bulk later; the bulk result is identical and lands as a
 * same-value version of the ReplacingMergeTree rows.
 *
 * `resolved` reports what the run WROTE, not what it attempted, which is why
 * the run reads its own rows back: every generated statement is an
 * `INSERT ... SELECT`, and a SELECT that matches nothing inserts nothing while
 * still succeeding. A release of a field with no candidate left to fall back
 * to, or a company whose se_company_field holds no register name for the
 * projection to key on, leaves the PREVIOUS row standing (se_company_field is
 * a ReplacingMergeTree with a non-empty-value CHECK, so there is no tombstone
 * to write). Reporting those as resolved would put "Saved and resolved." over
 * a page that still shows the old value.
 */

export type SkippedField = {
  field: string;
  reason: "python_only" | "unknown_field" | "no_row";
};

export interface ResolveCompanyFieldsResult {
  resolved: string[];
  skipped: SkippedField[];
}

/**
 * ClickHouse's DateTime64(3) text form, `YYYY-MM-DD HH:MM:SS.mmm` in UTC.
 * Bound as a string on purpose: the driver renders a JS Date as epoch seconds,
 * which ClickHouse also parses but which is not the form the field-value
 * rows' created_at carries, and one form is easier to grep for than two.
 */
export function formatClickHouseDateTime64(date: Date): string {
  return date.toISOString().replace("T", " ").replace("Z", "");
}

/**
 * Raised by appendSeCompanyInfoFieldValues when the decision rows were
 * inserted but resolving them failed. The decision is NOT lost: it is in
 * se_company_info_field_value, and se_company_info_field_value_sensor
 * re-resolves the company in bulk. Carries the ids so the page can say so.
 */
export class SeCompanyFieldResolveError extends Error {
  readonly valueIds: string[];

  constructor(valueIds: string[], cause: unknown) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    super(
      `Saved, but not resolved: ${reason}. The decision is kept and applies on the next pipeline run.`,
      { cause },
    );
    this.name = "SeCompanyFieldResolveError";
    this.valueIds = valueIds;
  }
}

/**
 * Which fields THIS run wrote: se_company_field stamps every inserted row with
 * the run's own source_run_id, so the run's rows are exactly its own. FINAL
 * because the table is a ReplacingMergeTree and an unmerged part could
 * otherwise show an older version of the same (company_id, field) key -- one
 * carrying a different run's id.
 */
export const RESOLVED_FIELDS_SQL = `SELECT toString(field) AS field
FROM corpscout.se_company_field FINAL
WHERE company_id = {companyId:String} AND source_run_id = {sourceRunId:String}`;

/**
 * Executes each field's resolve statement for `[companyId]` in the order
 * given, reads back which of them actually produced a row, then runs the wide
 * projection once (one statement for the company, not one per field).
 *
 * A python_only field belongs to Dagster alone and is reported as skipped; so
 * is a field the registry does not know (the validator refuses those earlier,
 * but this function is also called directly), and so is an attempted field
 * whose statement wrote nothing (`no_row`, see the module comment) -- the
 * field kept whatever it had.
 *
 * The projection is skipped when nothing was resolved: the long table did not
 * change, so the wide row would not either. `project: false` skips it
 * unconditionally -- the live test's production branch resolves a synthetic
 * company into the long table only, because a wide row would flow into
 * se_companies_serving; the action path never sets it.
 */
export async function resolveCompanyFields(
  companyId: string,
  fields: string[],
  opts: {
    registry?: FieldRegistry;
    now?: Date;
    sourceRunId?: string;
    project?: boolean;
  } = {},
): Promise<ResolveCompanyFieldsResult> {
  const registry = opts.registry ?? (await loadFieldRegistry());
  const sourceRunId = opts.sourceRunId ?? `backoffice:${randomUUID()}`;
  const resolvedAt = formatClickHouseDateTime64(opts.now ?? new Date());
  const byName = new Map(registry.fields.map((entry) => [entry.field, entry]));

  const attempted: string[] = [];
  const skipped: SkippedField[] = [];
  for (const field of fields) {
    const entry = byName.get(field);
    if (!entry) {
      skipped.push({ field, reason: "unknown_field" });
      continue;
    }
    if (entry.pythonOnly) {
      skipped.push({ field, reason: "python_only" });
      continue;
    }
    await chCommand(entry.resolveSql, {
      field,
      company_ids: [companyId],
      source_run_id: sourceRunId,
      resolved_at: resolvedAt,
    });
    attempted.push(field);
  }

  // Nothing was attempted -> no row can carry this run id, so the read-back
  // would be a query with a foregone answer.
  const resolved: string[] = [];
  if (attempted.length > 0) {
    const rows = await chQuery<{ field: string }>(RESOLVED_FIELDS_SQL, {
      companyId,
      sourceRunId,
    });
    const written = new Set(rows.map((row) => row.field));
    for (const field of attempted) {
      if (written.has(field)) resolved.push(field);
      else skipped.push({ field, reason: "no_row" });
    }
  }

  if (opts.project !== false && resolved.length > 0) {
    await chCommand(registry.projectionSql, { company_ids: [companyId] });
  }
  return { resolved, skipped };
}
