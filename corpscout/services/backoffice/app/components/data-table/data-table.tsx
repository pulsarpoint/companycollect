import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
} from "@tanstack/react-table";
import { useNavigate } from "react-router";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

export function DataTable<TData>({
  columns,
  data,
  emptyText = "No results.",
  minWidthClassName = "min-w-[56rem]",
  rowHref,
}: {
  columns: ColumnDef<TData, unknown>[];
  data: TData[];
  emptyText?: string;
  /** Table's own min-width, so it never gets squeezed below a readable
   * layout — the surrounding `overflow-x-auto` div is what scrolls, not the
   * page. Wider tables (more/denser columns) pass a larger value. */
  minWidthClassName?: string;
  /** When given, the whole row navigates to this URL on click / Enter. Clicks
   * that land on an inner link, button or form control keep their own
   * behaviour. The URL is also exposed as `data-href` for tests. */
  rowHref?: (row: TData) => string;
}) {
  const navigate = useNavigate();
  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
    manualPagination: true,
    manualSorting: true,
    manualFiltering: true,
  });

  return (
    <div className="overflow-x-auto rounded-md border">
      <Table className={minWidthClassName}>
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <TableHead key={header.id} className="whitespace-nowrap">
                  {header.isPlaceholder
                    ? null
                    : flexRender(header.column.columnDef.header, header.getContext())}
                </TableHead>
              ))}
            </TableRow>
          ))}
        </TableHeader>
        <TableBody>
          {table.getRowModel().rows.length === 0 ? (
            <TableRow>
              <TableCell
                colSpan={columns.length}
                className="text-muted-foreground h-24 text-center"
              >
                {emptyText}
              </TableCell>
            </TableRow>
          ) : (
            table.getRowModel().rows.map((row) => {
              const href = rowHref?.(row.original);
              const ownControl = (target: EventTarget | null) =>
                target instanceof Element &&
                target.closest("a, button, input, select, textarea, form, label") !== null;
              return (
              <TableRow
                key={row.id}
                data-href={href}
                role={href ? "link" : undefined}
                tabIndex={href ? 0 : undefined}
                className={href ? "cursor-pointer hover:bg-muted/50" : undefined}
                onClick={
                  href
                    ? (event) => {
                        if (ownControl(event.target)) return;
                        void navigate(href);
                      }
                    : undefined
                }
                onKeyDown={
                  href
                    ? (event) => {
                        if (event.key !== "Enter" || ownControl(event.target)) return;
                        void navigate(href);
                      }
                    : undefined
                }
              >
                {row.getVisibleCells().map((cell) => (
                  <TableCell key={cell.id} className="align-top">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                ))}
              </TableRow>
              );
            })
          )}
        </TableBody>
      </Table>
    </div>
  );
}
