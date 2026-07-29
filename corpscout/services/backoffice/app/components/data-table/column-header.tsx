import { Link } from "react-router";
import { ArrowDown, ArrowUp, ChevronsUpDown } from "lucide-react";
import type { SortDir } from "~/lib/countries";
import { Button } from "~/components/ui/button";
import { nextSortDir, tableSearch } from "~/components/data-table/url";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";

export function DataTableColumnHeader({
  label,
  sortKey,
  currentSort,
  currentDir,
  align = "start",
}: {
  label: string;
  /** undefined → not sortable, render plain label */
  sortKey?: string;
  currentSort: string;
  currentDir: SortDir;
  /** "end" right-aligns the button (and its edge-hugging negative margin)
   * for numeric columns whose header sits under a right-aligned cell. */
  align?: "start" | "end";
}) {
  const searchParams = useEffectiveSearchParams();
  if (!sortKey) {
    return (
      <span
        className={`text-muted-foreground block text-xs font-medium uppercase tracking-wide ${align === "end" ? "text-right" : ""}`}
      >
        {label}
      </span>
    );
  }
  const isActive = currentSort === sortKey;
  const target = tableSearch(searchParams, {
    sort: sortKey,
    dir: nextSortDir(currentSort, currentDir, sortKey),
  });
  const Icon = !isActive ? ChevronsUpDown : currentDir === "asc" ? ArrowUp : ArrowDown;
  return (
    <Button
      variant="ghost"
      size="sm"
      className={`h-7 gap-1 text-xs font-medium uppercase tracking-wide data-[active=true]:text-foreground ${align === "end" ? "-mr-2" : "-ml-2"}`}
      data-active={isActive}
      render={<Link to={target} preventScrollReset />}
      nativeButton={false}
    >
      {label}
      <Icon className="size-3.5" />
    </Button>
  );
}
