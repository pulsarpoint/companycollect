import type React from "react";
import { cn } from "~/lib/utils";

/** What "the source recorded nothing here" looks like across the company area. */
export const EMPTY_VALUE = <span className="text-muted-foreground">—</span>;

/**
 * A register string, or the em dash when it is empty.
 *
 * Every loader in this area collapses a missing value to "" (a NULL column, a
 * LEFT JOIN miss), so one helper covers all of them and nothing has to
 * distinguish "" from null at the call site.
 */
export function text(value: string): React.ReactNode {
  return value === "" ? EMPTY_VALUE : value;
}

/**
 * One label/value pair. The label doubles as the React key: within a single
 * list every label is distinct by construction, and a list that repeated one
 * would be unreadable regardless.
 */
export type DefinitionEntry = [label: string, value: React.ReactNode];

/**
 * The label/value grid every "here is a database row, field by field" block in
 * the admin company area uses.
 *
 * Two columns on wide screens, stacked pairs on narrow ones. `display:
 * contents` on each pair's wrapper keeps its dt and dd in the grid's own flow
 * instead of nesting a box around every pair, which is what lets the labels
 * share one column width.
 */
export function DefinitionList({
  entries,
  valueClassName,
  className,
}: {
  entries: DefinitionEntry[];
  /** Extra classes for every `<dd>` -- `break-all` for hashes, ids and URLs. */
  valueClassName?: string;
  className?: string;
}) {
  return (
    <dl
      className={cn(
        "grid grid-cols-1 gap-x-6 gap-y-1 text-sm sm:grid-cols-[minmax(11rem,auto)_1fr]",
        className,
      )}
    >
      {entries.map(([label, value]) => (
        <div key={label} className="contents">
          <dt className="text-muted-foreground text-xs uppercase tracking-wide sm:pt-0.5">
            {label}
          </dt>
          <dd className={cn("mb-2 sm:mb-0", valueClassName)}>{value}</dd>
        </div>
      ))}
    </dl>
  );
}
