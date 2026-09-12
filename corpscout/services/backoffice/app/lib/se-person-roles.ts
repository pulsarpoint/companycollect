/**
 * Role-first grouping for the People tab's person panel (owner request 2026-09-12:
 * "when the user clicks on a person it should show all roles and, for each role, its
 * years"). Pure and structural on purpose -- no `.server` import, no dependency on the
 * entity server's own types -- so it groups both the roles view's stored rows
 * (`SePersonRoleRow`) and, via `personRoleYearsToRows`, the fold's array summary
 * (`SePersonRoleYear`) through the same minimal shape.
 */

/** The minimal row shape `groupPersonRoles` needs: one role observation, whichever
 * source it came from. */
export interface SePersonRoleGroupRow {
  role_code: string;
  /** 0 means the observation carried no fiscal year at all. */
  role_year: number;
  role_from: string;
  role_to: string;
  source: string;
  is_current: number;
}

/** One role, every year it was held compressed into ranges, its from/to span and the
 * sources and current flag folded across all its rows. */
export interface SePersonRoleGroup {
  role_code: string;
  /** Distinct, ascending, year 0 excluded. */
  years: number[];
  /** `compressYears(years)`; `""` when `years` is empty. */
  yearRanges: string;
  /** Some row in this group carried role_year 0 (no fiscal year at all). */
  noFiscalYear: boolean;
  /** `""`, or the earliest non-empty `role_from` across the group's rows. */
  from: string;
  /** `""`, or the latest non-empty `role_to` across the group's rows. */
  to: string;
  /** Distinct, sorted. */
  sources: string[];
  /** Any row in the group is current. */
  current: boolean;
}

/** A run of consecutive years as one range (`2021–2023`), separate years comma-joined
 * (`2021–2023, 2026`); a single year renders as itself, an empty list as `""`. The en
 * dash (not a hyphen) matches the panel's other year spans. */
export function compressYears(years: readonly number[]): string {
  const sorted = [...new Set(years)].sort((a, b) => a - b);
  if (sorted.length === 0) return "";
  const ranges: string[] = [];
  let start = sorted[0];
  let end = sorted[0];
  for (let index = 1; index < sorted.length; index += 1) {
    const year = sorted[index];
    if (year === end + 1) {
      end = year;
      continue;
    }
    ranges.push(start === end ? `${start}` : `${start}–${end}`);
    start = year;
    end = year;
  }
  ranges.push(start === end ? `${start}` : `${start}–${end}`);
  return ranges.join(", ");
}

/** One role, one line: every row for a `role_code` folded into its years, its from/to
 * span, its sources and whether any row is current. Groups sort by the latest year held
 * (desc), then `role_code` (asc) -- a group with no real year (only role_year 0 rows)
 * sorts last, ties broken the same way. */
export function groupPersonRoles(
  rows: readonly SePersonRoleGroupRow[],
): SePersonRoleGroup[] {
  const byCode = new Map<string, SePersonRoleGroupRow[]>();
  for (const row of rows) {
    const existing = byCode.get(row.role_code);
    if (existing === undefined) byCode.set(row.role_code, [row]);
    else existing.push(row);
  }
  const groups = [...byCode.entries()].map(([role_code, group]) => {
    const years = [...new Set(group.filter((row) => row.role_year !== 0).map((row) => row.role_year))].sort(
      (a, b) => a - b,
    );
    const noFiscalYear = group.some((row) => row.role_year === 0);
    const froms = group.map((row) => row.role_from).filter((value) => value !== "").sort();
    const tos = group.map((row) => row.role_to).filter((value) => value !== "").sort();
    return {
      role_code,
      years,
      yearRanges: compressYears(years),
      noFiscalYear,
      from: froms.length === 0 ? "" : froms[0],
      to: tos.length === 0 ? "" : tos[tos.length - 1],
      sources: [...new Set(group.map((row) => row.source))].sort(),
      current: group.some((row) => row.is_current === 1),
    };
  });
  return groups.sort((a, b) => {
    const latestA = a.years.length === 0 ? -Infinity : a.years[a.years.length - 1];
    const latestB = b.years.length === 0 ? -Infinity : b.years[b.years.length - 1];
    if (latestA !== latestB) return latestB - latestA;
    return a.role_code.localeCompare(b.role_code);
  });
}

/** Adapts the fold's array summary (`SePersonRoleYear`-shaped code/year/sources
 * triples, zipped out of the person row's parallel arrays) into `groupPersonRoles`'s row
 * shape: the array carries no per-row from/to or current flag, so both come out as `""`
 * / the person's own `current_roles`, and a year's several sources become one row each
 * (structurally typed, not `SePersonRoleYear` itself, so this module stays free of the
 * `.server` entity types). */
export function personRoleYearsToRows(
  roles: readonly { code: string; year: number; sources: readonly string[] }[],
  currentRoles: readonly string[],
): SePersonRoleGroupRow[] {
  const current = new Set(currentRoles);
  const rows: SePersonRoleGroupRow[] = [];
  for (const role of roles) {
    for (const source of role.sources) {
      rows.push({
        role_code: role.code,
        role_year: role.year,
        role_from: "",
        role_to: "",
        source,
        is_current: current.has(role.code) ? 1 : 0,
      });
    }
  }
  return rows;
}
