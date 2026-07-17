import { Link, useNavigate } from "react-router";
import {
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
} from "lucide-react";
import { PAGE_SIZES } from "~/lib/countries";
import { Button } from "~/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import { tableSearch } from "~/components/data-table/url";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";

const nf = new Intl.NumberFormat("en-US");

export function DataTablePagination({
  total,
  page,
  pageSize,
  maxPage,
}: {
  total: number;
  page: number;
  pageSize: number;
  maxPage?: number;
}) {
  const searchParams = useEffectiveSearchParams();
  const navigate = useNavigate();
  const rawLast = Math.max(1, Math.ceil(total / pageSize));
  const lastPage = maxPage ? Math.min(rawLast, maxPage) : rawLast;
  const capped = maxPage ? rawLast > maxPage : false;

  function nav(target: number, disabled: boolean, icon: React.ReactNode, label: string) {
    if (disabled) {
      return (
        <Button variant="outline" size="icon-sm" disabled aria-label={label}>
          {icon}
        </Button>
      );
    }
    return (
      <Button
        variant="outline"
        size="icon-sm"
        aria-label={label}
        render={<Link to={tableSearch(searchParams, { page: target })} preventScrollReset />}
        nativeButton={false}
      >
        {icon}
      </Button>
    );
  }

  return (
    <div className="flex flex-wrap items-center justify-between gap-3">
      <span className="text-muted-foreground text-sm tabular-nums">
        {nf.format(total)} companies
      </span>
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground text-sm">Rows per page</span>
          <Select
            value={String(pageSize)}
            onValueChange={(value: string | null) => {
              if (value === null) return;
              navigate(tableSearch(searchParams, { pageSize: Number(value) }), {
                preventScrollReset: true,
              });
            }}
          >
            <SelectTrigger className="h-8 w-[4.5rem]" size="sm">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {PAGE_SIZES.map((size) => (
                <SelectItem key={size} value={String(size)}>
                  {size}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <span className="text-sm tabular-nums">
          Page {nf.format(page)} of {nf.format(lastPage)}
          {capped ? " (capped)" : ""}
        </span>
        <div className="flex items-center gap-1.5">
          {nav(1, page <= 1, <ChevronsLeft className="size-4" />, "First page")}
          {nav(page - 1, page <= 1, <ChevronLeft className="size-4" />, "Previous page")}
          {nav(page + 1, page >= lastPage, <ChevronRight className="size-4" />, "Next page")}
          {nav(lastPage, page >= lastPage, <ChevronsRight className="size-4" />, "Last page")}
        </div>
      </div>
    </div>
  );
}
