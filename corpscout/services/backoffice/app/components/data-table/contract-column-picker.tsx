import { useNavigate, useSearchParams } from "react-router";
import { Columns3 } from "lucide-react";

import {
  CONTRACT_COLUMNS,
  serializeContractColumns,
  type ContractColumnId,
} from "~/lib/contract-columns";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
import { Label } from "~/components/ui/label";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "~/components/ui/popover";

/**
 * Which columns the contracts table shows.
 *
 * Only columns the country's register actually fills are offered, so the list
 * differs per country without anyone maintaining a per-country config: Brazil
 * offers Agreement type and not CPV, Estonia and Norway the reverse, Sweden
 * both. A locked column cannot be unticked — hiding the contract title would
 * leave a reader with rows and no way into any of them.
 *
 * The choice lives in the URL like every other view setting here, so a
 * customised table is linkable and survives the back button. Every OTHER param
 * is carried across, which matters most for the filters: changing a column must
 * not silently drop the filter a reader is looking through.
 */
export function ContractColumnPicker({
  countryCode,
  visible,
  available,
}: {
  countryCode: string;
  visible: ContractColumnId[];
  available: ContractColumnId[];
}) {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();

  const offered = CONTRACT_COLUMNS.filter((c) => available.includes(c.id));
  const hidden = offered.length - visible.length;

  function toggle(id: ContractColumnId) {
    const next = visible.includes(id)
      ? visible.filter((v) => v !== id)
      : [...visible, id];

    const params = new URLSearchParams(searchParams);
    const serialized = serializeContractColumns(next, available);
    // Dropped rather than written when the selection is the default, so a table
    // nobody customised keeps a clean URL.
    if (serialized === null) params.delete("cols");
    else params.set("cols", serialized);

    const query = params.toString();
    navigate(`/countries/${countryCode}/contracts${query ? `?${query}` : ""}`, {
      // Column changes do not alter the row set, so the reader stays where they
      // are -- and the history does not fill with one entry per tick.
      replace: true,
      preventScrollReset: true,
    });
  }

  return (
    <Popover>
      <PopoverTrigger render={<Button variant="outline" size="sm" />}>
        <Columns3 className="size-4" />
        Columns
        {hidden > 0 ? (
          <span className="text-muted-foreground ml-1 text-xs tabular-nums">
            {visible.length}/{offered.length}
          </span>
        ) : null}
      </PopoverTrigger>
      <PopoverContent className="w-64" align="end">
        <div className="flex flex-col gap-2">
          <Label className="text-xs tracking-wide uppercase">Columns</Label>
          <div className="flex flex-col gap-1.5">
            {offered.map((column) => (
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
            ))}
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
}
