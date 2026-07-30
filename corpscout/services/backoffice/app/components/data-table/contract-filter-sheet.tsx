import { useState } from "react";
import { useNavigate, useSearchParams } from "react-router";
import { ListFilter } from "lucide-react";

import {
  contractFilterCount,
  EMPTY_CONTRACT_FILTERS,
  serializeContractFilters,
  type ContractFilters,
} from "~/lib/contract-filters";
import { cpvDivisionLabel } from "~/lib/cpv";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Separator } from "~/components/ui/separator";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "~/components/ui/sheet";

export type AgreementFacetOption = { value: string; count: number };
export type CpvFacetOption = { division: string; count: number };

const nf = new Intl.NumberFormat("en-US");

/**
 * Filters for a country's contracts, in a sheet, with every choice in the URL.
 *
 * Edits are held locally and applied on submit rather than navigating per
 * keystroke: an amount range is only meaningful once both ends are typed, and a
 * request per digit would refetch a 116k-row aggregate each time.
 *
 * `agreementLabel` translates a value for display while the URL and the query
 * carry the register's own wording — Brazil's types are Portuguese and the page is
 * English, and the filter has to match what is stored, not what is shown.
 */
export function ContractFilterSheet({
  countryCode,
  filters,
  agreementOptions,
  agreementLabel,
  cpvOptions = [],
}: {
  countryCode: string;
  filters: ContractFilters;
  agreementOptions: AgreementFacetOption[];
  agreementLabel?: (value: string) => string;
  cpvOptions?: CpvFacetOption[];
}) {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState<ContractFilters>(filters);
  const active = contractFilterCount(filters);

  function apply(next: ContractFilters) {
    const params = new URLSearchParams(serializeContractFilters(next));
    // The chosen columns are not a filter and must survive one being applied --
    // otherwise every Apply silently resets the table back to its defaults.
    const columns = searchParams.get("cols");
    if (columns !== null) params.set("cols", columns);
    const query = params.toString();
    // Page is deliberately dropped: after changing a filter, page 7 of the old
    // result set is meaningless and often past the new end.
    navigate(`/countries/${countryCode}/contracts${query ? `?${query}` : ""}`);
    setOpen(false);
  }

  function toggleAgreement(value: string) {
    setDraft((d) => ({
      ...d,
      agreement: d.agreement.includes(value)
        ? d.agreement.filter((v) => v !== value)
        : [...d.agreement, value],
    }));
  }

  function toggleCpv(division: string) {
    setDraft((d) => ({
      ...d,
      cpv: d.cpv.includes(division)
        ? d.cpv.filter((v) => v !== division)
        : [...d.cpv, division],
    }));
  }

  function numberOrNull(raw: string): number | null {
    const value = Number(raw);
    return raw.trim() !== "" && Number.isFinite(value) && value >= 0 ? value : null;
  }

  return (
    <div className="flex items-center gap-1.5">
    <Sheet
      open={open}
      onOpenChange={(next) => {
        // Reopening starts from what the URL says, so an abandoned edit does not
        // linger and silently apply later.
        if (next) setDraft(filters);
        setOpen(next);
      }}
    >
      <SheetTrigger render={<Button variant="outline" size="sm" />}>
        <ListFilter className="size-4" />
        Filter
        {active > 0 ? (
          <Badge variant="secondary" className="ml-1 px-1.5">
            {active}
          </Badge>
        ) : null}
      </SheetTrigger>
      <SheetContent side="right" className="flex w-96 flex-col overflow-y-auto">
        <SheetHeader>
          <SheetTitle>Filter contracts</SheetTitle>
        </SheetHeader>

        <div className="flex flex-col gap-6 px-4">
          {agreementOptions.length > 0 ? (
            <section className="flex flex-col gap-2">
              <Label className="text-xs tracking-wide uppercase">Agreement type</Label>
              <div className="flex flex-col gap-1.5">
                {agreementOptions.map((option) => (
                  <label
                    key={option.value}
                    className="flex cursor-pointer items-center gap-2 text-sm"
                  >
                    <Checkbox
                      checked={draft.agreement.includes(option.value)}
                      onCheckedChange={() => toggleAgreement(option.value)}
                    />
                    <span className="flex-1">
                      {agreementLabel ? agreementLabel(option.value) : option.value}
                    </span>
                    <span className="text-muted-foreground text-xs tabular-nums">
                      {nf.format(option.count)}
                    </span>
                  </label>
                ))}
              </div>
            </section>
          ) : null}

          {cpvOptions.length > 0 ? (
            <>
              <Separator />
              <section className="flex flex-col gap-2">
                <Label className="text-xs tracking-wide uppercase">
                  What was procured
                </Label>
                {/* Divisions, not the ~9,500 individual codes: this is the level
                    that has a name a reader recognises, and selecting one catches
                    every depth a buyer published beneath it. */}
                <div className="flex max-h-72 flex-col gap-1.5 overflow-y-auto pr-1">
                  {cpvOptions.map((option) => (
                    <label
                      key={option.division}
                      className="flex cursor-pointer items-start gap-2 text-sm"
                    >
                      <Checkbox
                        checked={draft.cpv.includes(option.division)}
                        onCheckedChange={() => toggleCpv(option.division)}
                        className="mt-0.5"
                      />
                      <span className="flex-1">
                        {cpvDivisionLabel(`${option.division}000000`) ??
                          `CPV division ${option.division}`}
                        <span className="text-muted-foreground ml-1.5 font-mono text-[10px]">
                          {option.division}
                        </span>
                      </span>
                      <span className="text-muted-foreground text-xs tabular-nums">
                        {nf.format(option.count)}
                      </span>
                    </label>
                  ))}
                </div>
              </section>
            </>
          ) : null}

          <Separator />

          <section className="flex flex-col gap-2">
            <Label className="text-xs tracking-wide uppercase">Amount (USD)</Label>
            {/* USD because a country can mix currencies -- Sweden carries SEK
                from UHM and EUR from TED -- so a threshold against the original
                amount would compare unlike numbers. */}
            <div className="flex items-center gap-2">
              <Input
                type="number"
                min={0}
                inputMode="decimal"
                placeholder="min"
                value={draft.amountMin ?? ""}
                onChange={(e) =>
                  setDraft((d) => ({ ...d, amountMin: numberOrNull(e.target.value) }))
                }
              />
              <span className="text-muted-foreground text-sm">to</span>
              <Input
                type="number"
                min={0}
                inputMode="decimal"
                placeholder="max"
                value={draft.amountMax ?? ""}
                onChange={(e) =>
                  setDraft((d) => ({ ...d, amountMax: numberOrNull(e.target.value) }))
                }
              />
            </div>
            <p className="text-muted-foreground text-xs">
              Either end may be left blank for an open-ended range.
            </p>
          </section>

          <Separator />

          <section className="flex flex-col gap-2">
            <Label className="text-xs tracking-wide uppercase">Published</Label>
            <div className="flex items-center gap-2">
              <Input
                type="date"
                value={draft.from ?? ""}
                onChange={(e) =>
                  setDraft((d) => ({ ...d, from: e.target.value || null }))
                }
              />
              <span className="text-muted-foreground text-sm">to</span>
              <Input
                type="date"
                value={draft.to ?? ""}
                onChange={(e) => setDraft((d) => ({ ...d, to: e.target.value || null }))}
              />
            </div>
          </section>
        </div>

        <div className="mt-auto flex gap-2 border-t p-4">
          <Button className="flex-1" onClick={() => apply(draft)}>
            Apply
          </Button>
          <Button
            variant="outline"
            onClick={() => apply(EMPTY_CONTRACT_FILTERS)}
          >
            Clear
          </Button>
        </div>
      </SheetContent>
    </Sheet>
      {active > 0 ? (
        <Button
          variant="ghost"
          size="sm"
          onClick={() => apply(EMPTY_CONTRACT_FILTERS)}
        >
          Clear
          <span className="sr-only"> all filters</span>
        </Button>
      ) : null}
    </div>
  );
}
