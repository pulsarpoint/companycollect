import { useNavigate, useSearchParams } from "react-router";
import { Columns3 } from "lucide-react";

import {
  serializeCompanyColumns,
  type CompanyColumn,
} from "~/lib/company-columns";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
import { Label } from "~/components/ui/label";
import { Popover, PopoverContent, PopoverTrigger } from "~/components/ui/popover";

/**
 * Which columns the companies table shows.
 *
 * The core is listed first and ticked by default in every country, so the
 * table reads the same wherever a reader is. Below it sit the columns only
 * that register has — Brazil's trade name and size, Norway's website — off
 * until asked for.
 *
 * A locked column cannot be unticked: hiding the name would leave a reader
 * with rows and no way into any of them.
 *
 * The choice lives in the URL like every other view setting here, so a
 * customised table is linkable and survives the back button. Every OTHER
 * param is carried across, which matters most for the filters and the search:
 * changing a column must not silently drop what a reader is looking through.
 */
export function CompanyColumnPicker({
  countryCode,
  visible,
  available,
}: {
  countryCode: string;
  visible: string[];
  available: CompanyColumn[];
}) {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();

  const core = available.filter((c) => c.core);
  const extras = available.filter((c) => !c.core);
  const hidden = available.length - visible.length;

  function toggle(id: string) {
    const next = visible.includes(id)
      ? visible.filter((v) => v !== id)
      : [...visible, id];

    const params = new URLSearchParams(searchParams);
    const serialized = serializeCompanyColumns(next, available);
    // Dropped rather than written when the selection is the default, so a
    // table nobody customised keeps a clean URL.
    if (serialized === null) params.delete("cols");
    else params.set("cols", serialized);

    const query = params.toString();
    navigate(`/countries/${countryCode}/companies${query ? `?${query}` : ""}`, {
      // Column changes do not alter the row set, so the reader stays where
      // they are -- and the history does not fill with one entry per tick.
      replace: true,
      preventScrollReset: true,
    });
  }

  function row(column: CompanyColumn) {
    return (
      <label
        key={column.id}
        className={`flex items-center gap-2 text-sm ${
          column.locked ? "cursor-default opacity-60" : "cursor-pointer"
        }`}
      >
        <Checkbox
          checked={visible.includes(column.id)}
          disabled={column.locked}
          onCheckedChange={() => {
            if (!column.locked) toggle(column.id);
          }}
        />
        <span className="flex-1">{column.label}</span>
      </label>
    );
  }

  return (
    <Popover>
      <PopoverTrigger render={<Button variant="outline" size="sm" />}>
        <Columns3 className="size-4" />
        Columns
        {hidden > 0 ? (
          <span className="text-muted-foreground ml-1 text-xs tabular-nums">
            {visible.length}/{available.length}
          </span>
        ) : null}
      </PopoverTrigger>
      <PopoverContent className="w-64" align="end">
        <div className="flex flex-col gap-3">
          <div className="flex flex-col gap-1.5">
            <Label className="text-xs tracking-wide uppercase">Columns</Label>
            {core.map(row)}
          </div>
          {extras.length > 0 ? (
            <div className="flex flex-col gap-1.5">
              <Label className="text-muted-foreground text-xs tracking-wide uppercase">
                Only in this register
              </Label>
              {extras.map(row)}
            </div>
          ) : null}
        </div>
      </PopoverContent>
    </Popover>
  );
}
