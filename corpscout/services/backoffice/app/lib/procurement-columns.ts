/** Which register columns the records TABLE hides. The record detail page
 * deliberately shows everything a register publishes; the table is for
 * scanning, so load plumbing, FX bookkeeping, and the estimated/framework
 * value variants move to the detail page only. One shared rule set — every
 * register (TED, Doffin, Hilma, PNCP, UHM) inherits it. */

const HIDDEN_EXACT = new Set([
  "source_slug",
  "source_run_id",
  "partition_key",
  "resolved_at",
]);

const HIDDEN_PATTERN = /^fx_|^framework_|estimated_value/;

export function isHiddenTableColumn(name: string): boolean {
  return HIDDEN_EXACT.has(name) || HIDDEN_PATTERN.test(name);
}

export function visibleColumns(columns: string[]): string[] {
  return columns.filter((name) => !isHiddenTableColumn(name));
}
