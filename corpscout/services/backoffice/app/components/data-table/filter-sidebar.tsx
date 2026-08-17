import { useEffect, useRef, useState } from "react";
import { Link, useFetcher, useNavigate } from "react-router";
import { Check, ListFilter } from "lucide-react";
import type { CountryConfig } from "~/lib/countries";
import type { CompanyFilters } from "~/lib/filters";
import { filterableFacetKeys } from "~/lib/filters";
import { availableCompanyFlags, flagFilterKey } from "~/lib/company-flags";
import {
  FINANCIAL_FILING_FILTER_KEY,
  isFinancialFilingStatus,
} from "~/lib/financial-filing-status";
import type { FacetOption } from "~/lib/facets.server";
import {
  setFilterValues,
  toggleFilterValue,
} from "~/components/data-table/url";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";
import { FINANCIAL_FILING_STATUS_PRESENTATION } from "~/components/financial-filing-status";
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
import { ToggleGroup, ToggleGroupItem } from "~/components/ui/toggle-group";

const nf = new Intl.NumberFormat("en-US");

export function facetLabel(country: CountryConfig, key: string): string {
  if (key === "industry") return "Industry";
  if (key === FINANCIAL_FILING_FILTER_KEY) return "Annual report";
  // Flag filters are not columns, so the active-filter chip would otherwise
  // read "flag_financials: yes" -- the raw key, leaked to the reader.
  const flag = availableCompanyFlags(country.code).find(
    (f) => flagFilterKey(f.id) === key,
  );
  if (flag) return flag.label;
  return country.columns.find((c) => c.key === key)?.label ?? key;
}

export function facetValueLabel(key: string, value: string): string {
  if (key !== FINANCIAL_FILING_FILTER_KEY) return value;
  return (
    FINANCIAL_FILING_STATUS_PRESENTATION.find(
      (definition) => definition.value === value,
    )?.label ?? value
  );
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
            <Button
              variant="outline"
              size="sm"
              className="w-full justify-between font-normal"
            />
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
                        toggleFilterValue(
                          effectiveParams,
                          facetKey,
                          option.value,
                        ),
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

function FinancialFilingFilters({ filters }: { filters: CompanyFilters }) {
  const navigate = useNavigate();
  const effectiveParams = useEffectiveSearchParams();
  const selected = (filters[FINANCIAL_FILING_FILTER_KEY] ?? []).filter(
    isFinancialFilingStatus,
  );

  return (
    <div className="flex flex-col gap-2">
      <div>
        <p className="text-sm font-medium">Annual report</p>
        <p className="text-muted-foreground text-xs">
          Latest evidence-backed filing state
        </p>
      </div>
      <ToggleGroup
        multiple
        value={selected}
        onValueChange={(values) =>
          navigate(
            setFilterValues(
              effectiveParams,
              FINANCIAL_FILING_FILTER_KEY,
              values,
            ),
            { preventScrollReset: true },
          )
        }
        variant="outline"
        size="sm"
        className="w-full flex-wrap justify-start"
        aria-label="Annual report filing status"
      >
        {FINANCIAL_FILING_STATUS_PRESENTATION.map((definition) => {
          const Icon = definition.icon;
          return (
            <ToggleGroupItem
              key={definition.value}
              value={definition.value}
              title={definition.meaning}
              aria-label={definition.label}
            >
              <Icon data-icon="inline-start" />
              {definition.shortLabel}
            </ToggleGroupItem>
          );
        })}
      </ToggleGroup>
    </div>
  );
}

/**
 * A switch per kind of data we hold.
 *
 * Two states, not three: off means "any", on means "only companies that have
 * it". The URL model still understands `no` -- `?f_flag_financials=no` returns
 * the companies we hold no accounts for, which is how a coverage gap gets
 * found -- but that is a rarer question than "show me the ones with data", and
 * a control that reads as a switch should behave like one.
 *
 * Rendered as a Link rather than a stateful control so the filter stays in the
 * URL like every other one here: shareable, back-button-safe, and rendered on
 * the server.
 */
function FlagFilters({
  country,
  filters,
}: {
  country: CountryConfig;
  filters: CompanyFilters;
}) {
  const effectiveParams = useEffectiveSearchParams();
  const flags = availableCompanyFlags(country.code).filter(
    (flag) => country.code !== "se" || flag.id !== "financials",
  );
  if (flags.length === 0) return null;

  return (
    <div className="space-y-2">
      <p className="text-sm font-medium">Data held</p>
      <div className="flex flex-col gap-2">
        {flags.map((flag) => {
          const key = flagFilterKey(flag.id);
          const on = (filters[key] ?? []).includes("yes");
          return (
            <Link
              key={flag.id}
              to={toggleFilterValue(effectiveParams, key, "yes")}
              preventScrollReset
              role="switch"
              aria-checked={on}
              aria-label={`Only companies with ${flag.label.toLowerCase()}`}
              className="flex items-center justify-between gap-2 py-0.5"
              title={flag.meaning}
            >
              <span className="text-muted-foreground text-sm">
                {flag.label}
              </span>
              <span
                className={`relative inline-flex h-5 w-9 shrink-0 items-center rounded-full border transition-colors ${
                  on
                    ? "border-emerald-600/50 bg-emerald-500/70"
                    : "border-border bg-muted"
                }`}
              >
                <span
                  className={`size-3.5 rounded-full bg-white shadow-sm transition-transform ${
                    on ? "translate-x-[1.15rem]" : "translate-x-[0.15rem]"
                  }`}
                />
              </span>
            </Link>
          );
        })}
      </div>
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
        {activeCount > 0 ? (
          <Badge variant="secondary">{activeCount}</Badge>
        ) : null}
      </SheetTrigger>
      <SheetContent side="right" className="w-96 overflow-y-auto">
        <SheetHeader>
          <SheetTitle>Filter companies</SheetTitle>
        </SheetHeader>
        <div className="flex flex-col gap-4 px-4 pb-6">
          {country.code === "se" ? (
            <FinancialFilingFilters filters={filters} />
          ) : null}
          <FlagFilters country={country} filters={filters} />
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
