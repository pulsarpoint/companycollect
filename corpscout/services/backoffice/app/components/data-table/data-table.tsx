import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
  type OnChangeFn,
} from "@tanstack/react-table";
import { useNavigate } from "react-router";
import type { RowSelection } from "~/lib/row-selection";
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
  selection,
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
  /**
   * Turns on TanStack's row-selection model, as ONE object so a half-wired
   * table is a type error rather than a table whose ticks go nowhere.
   *
   * The state is owned above this component -- in the route component -- so
   * that filtering, sorting and paging (all search-param navigations, which
   * re-render this table with a different page of rows) keep the selection.
   * The columns are what render the checkboxes; this only carries the state.
   */
  selection?: {
    state: RowSelection;
    onChange: OnChangeFn<RowSelection>;
    /** A row's identity ACROSS pages. The selection outlives the rows it was
     * made from, so this must be the row's own key (e.g. `company_id`), never
     * the row index TanStack keys by when this is not given. */
    getRowId: (row: TData) => string;
  };
}) {
  const navigate = useNavigate();
  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
    manualPagination: true,
    manualSorting: true,
    manualFiltering: true,
    ...(selection
      ? {
          state: { rowSelection: selection.state },
          onRowSelectionChange: selection.onChange,
          getRowId: (row: TData) => selection.getRowId(row),
        }
      : {}),
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
