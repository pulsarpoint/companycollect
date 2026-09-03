import { randomUUID } from "node:crypto";
import { chCommand } from "~/lib/clickhouse.server";
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
 */

export type SkippedField = {
  field: string;
  reason: "python_only" | "unknown_field";
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
 * Executes each field's resolve statement for `[companyId]` in the order
 * given, then the wide projection once (one statement for the company, not
 * one per field). A python_only field belongs to Dagster alone and is
 * reported as skipped; so is a field the registry does not know (the
 * validator refuses those earlier, but this function is also called directly).
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

  const resolved: string[] = [];
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
    resolved.push(field);
  }
  if (opts.project !== false && resolved.length > 0) {
    await chCommand(registry.projectionSql, { company_ids: [companyId] });
  }
  return { resolved, skipped };
}
