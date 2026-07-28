import { useEffect, useRef, useState } from "react";
import { useFetcher, useNavigate } from "react-router";
import { Check, ListFilter } from "lucide-react";
import type { CountryConfig } from "~/lib/countries";
import type { CompanyFilters } from "~/lib/filters";
import { filterableFacetKeys } from "~/lib/filters";
import type { FacetOption } from "~/lib/facets.server";
import { toggleFilterValue } from "~/components/data-table/url";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandInput,
  CommandItem,
  CommandList,
} from "~/components/ui/command";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "~/components/ui/popover";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "~/components/ui/sheet";

const nf = new Intl.NumberFormat("en-US");

export function facetLabel(country: CountryConfig, key: string): string {
  if (key === "industry") return "Industry";
  return country.columns.find((c) => c.key === key)?.label ?? key;
}

function FacetCombobox({
  country,
  facetKey,
  selected,
}: {
  country: CountryConfig;
  facetKey: string;
  selected: string[];
}) {
  const fetcher = useFetcher<{ options: FacetOption[] }>();
  const navigate = useNavigate();
  const effectiveParams = useEffectiveSearchParams();
  const [open, setOpen] = useState(false);
  const debounce = useRef<ReturnType<typeof setTimeout>>(undefined);
  const base = `/countries/${country.code}/facet-options?column=${facetKey}`;

  function onOpenChange(next: boolean) {
    // Cancel any pending debounced query fetch so a stale `q=` result can't
    // land after reopen and overwrite the fetch-on-open list.
    clearTimeout(debounce.current);
    setOpen(next);
    if (next) fetcher.load(base);
  }

  useEffect(() => () => clearTimeout(debounce.current), []);

  function onQueryChange(q: string) {
    clearTimeout(debounce.current);
    debounce.current = setTimeout(() => {
      fetcher.load(`${base}&q=${encodeURIComponent(q)}`);
    }, 200);
  }

  const options = fetcher.data?.options ?? [];

  return (
    <div className="space-y-1.5">
      <p className="text-sm font-medium">{facetLabel(country, facetKey)}</p>
      <Popover open={open} onOpenChange={onOpenChange}>
        <PopoverTrigger
          render={
            <Button variant="outline" size="sm" className="w-full justify-between font-normal" />
          }
        >
          {selected.length > 0 ? `${selected.length} selected` : "Any"}
          <ListFilter className="text-muted-foreground size-3.5" />
        </PopoverTrigger>
        <PopoverContent className="w-80 p-0" align="start">
          <Command shouldFilter={false}>
            <CommandInput
              placeholder="Type to search…"
              onValueChange={onQueryChange}
            />
            <CommandList>
              <CommandEmpty>
                {fetcher.state === "idle" ? "No options." : "Loading…"}
              </CommandEmpty>
              {options.map((option) => {
                const isSelected = selected.includes(option.value);
                return (
                  <CommandItem
                    key={option.value}
                    value={option.value}
                    onSelect={() =>
                      navigate(
                        toggleFilterValue(effectiveParams, facetKey, option.value),
                        { preventScrollReset: true },
                      )
                    }
                  >
                    <Check
                      className={`size-4 ${isSelected ? "opacity-100" : "opacity-0"}`}
                    />
                    {option.label !== option.value ? (
                      <span className="text-muted-foreground font-mono text-xs">
                        {option.value}
                      </span>
                    ) : null}
                    <span className="flex-1 truncate" title={option.label}>
                      {option.label}
                    </span>
                    <span className="text-muted-foreground text-xs tabular-nums">
                      {nf.format(option.count)}
                    </span>
                  </CommandItem>
                );
              })}
            </CommandList>
          </Command>
        </PopoverContent>
      </Popover>
    </div>
  );
}

export function FilterSidebar({
  country,
  filters,
}: {
  country: CountryConfig;
  filters: CompanyFilters;
}) {
  const activeCount = Object.values(filters).reduce((n, v) => n + v.length, 0);
  return (
    <Sheet>
      <SheetTrigger render={<Button variant="outline" size="sm" />}>
        <ListFilter className="size-4" />
        Filters
        {activeCount > 0 ? <Badge variant="secondary">{activeCount}</Badge> : null}
      </SheetTrigger>
      <SheetContent side="right" className="w-96 overflow-y-auto">
        <SheetHeader>
          <SheetTitle>Filter companies</SheetTitle>
        </SheetHeader>
        <div className="space-y-4 px-4 pb-6">
          {filterableFacetKeys(country).map((key) => (
            <FacetCombobox
              key={key}
              country={country}
              facetKey={key}
              selected={filters[key] ?? []}
            />
          ))}
        </div>
      </SheetContent>
    </Sheet>
  );
}
