import { useEffect, useRef, useState } from "react";
import { useFetcher } from "react-router";
import { ChevronsUpDownIcon, XIcon } from "lucide-react";
import { ListFilterSheet } from "~/components/admin/list-filter-sheet";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandInput,
  CommandItem,
  CommandList,
} from "~/components/ui/command";
import { Field, FieldDescription, FieldGroup, FieldLabel } from "~/components/ui/field";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "~/components/ui/popover";
import type { FacetOption } from "~/lib/facets.server";
import {
  EMPTY_WORKSPACE_IP_FILTERS,
  IP_LIST_FILTER_KEYS,
  MAX_FILTER_VALUES,
  workspaceIpAddressesHref,
  withoutFilterValue,
  type IpListFilterKey,
  type WorkspaceIpFilters,
} from "~/lib/workspace-ip-addresses";

const nf = new Intl.NumberFormat("en-US");

const FILTER_NAMES: Record<IpListFilterKey, string> = {
  asn: "ASN",
  country: "Country",
  region: "Region",
  city: "City",
};

/** How a chosen value reads on a chip or in the picker's selected list. */
export function ipFilterValueLabel(
  key: IpListFilterKey,
  value: string,
  labels: Record<string, string>,
): string {
  const label = labels[`${key}:${value}`];
  if (key === "asn") return label ? `AS${value} ${label}` : `AS${value}`;
  if (key === "country" || key === "region")
    return label ? `${label} (${value})` : value;
  return value;
}

/**
 * A multi-select typeahead over the filter-options resource route. The
 * choices live in the enclosing form as repeated hidden inputs, so Apply
 * submits them like any other GET field.
 */
function FacetPicker({
  name,
  kind,
  label,
  placeholder,
  scope = "",
  selected,
  labels,
  onChange,
}: {
  name: IpListFilterKey;
  kind: IpListFilterKey;
  label: string;
  placeholder: string;
  /** Extra query string for region and city (the chosen country and regions). */
  scope?: string;
  selected: string[];
  labels: Record<string, string>;
  onChange: (values: string[], optionLabel?: [string, string]) => void;
}) {
  const fetcher = useFetcher<{ options: FacetOption[]; error?: boolean }>();
  const [open, setOpen] = useState(false);
  const debounce = useRef<ReturnType<typeof setTimeout>>(undefined);
  const base = `/admin/ip-addresses/filter-options?kind=${kind}${scope}`;
  useEffect(() => () => clearTimeout(debounce.current), []);
  const options = fetcher.data?.options ?? [];
  const full = selected.length >= MAX_FILTER_VALUES;
  return (
    <Field className="gap-1">
      <FieldLabel>{label}</FieldLabel>
      {selected.map((value) => (
        <input key={value} type="hidden" name={name} value={value} />
      ))}
      <Popover
        open={open}
        onOpenChange={(next) => {
          // A pending debounced query must not overwrite the fresh list.
          clearTimeout(debounce.current);
          setOpen(next);
          if (next) fetcher.load(base);
        }}
      >
        <PopoverTrigger
          render={
            <Button
              variant="outline"
              size="sm"
              className="w-full justify-between font-normal"
              aria-label={`Choose ${label.toLowerCase()}`}
            />
          }
        >
          {selected.length > 0 ? `${selected.length} selected` : "Any"}
          <ChevronsUpDownIcon data-icon="inline-end" />
        </PopoverTrigger>
        <PopoverContent className="w-80 p-0" align="start">
          <Command shouldFilter={false}>
            <CommandInput
              placeholder={placeholder}
              onValueChange={(q) => {
                clearTimeout(debounce.current);
                debounce.current = setTimeout(
                  () => fetcher.load(`${base}&q=${encodeURIComponent(q)}`),
                  200,
                );
              }}
            />
            <CommandList>
              <CommandEmpty>
                {fetcher.state !== "idle"
                  ? "Loading…"
                  : fetcher.data?.error
                    ? "Options are temporarily unavailable."
                    : "No matches."}
              </CommandEmpty>
              {options.map((option) => {
                const checked = selected.includes(option.value);
                return (
                  <CommandItem
                    key={option.value}
                    value={option.value}
                    data-checked={checked}
                    disabled={!checked && full}
                    onSelect={() =>
                      onChange(
                        checked
                          ? selected.filter((value) => value !== option.value)
                          : [...selected, option.value],
                        [`${kind}:${option.value}`, option.label],
                      )
                    }
                  >
                    {kind === "asn" ? (
                      <span className="text-muted-foreground font-mono text-xs">
                        AS{option.value}
                      </span>
                    ) : kind === "country" || kind === "region" ? (
                      <span className="text-muted-foreground font-mono text-xs">
                        {option.value}
                      </span>
                    ) : null}
                    <span className="flex-1 truncate" title={option.label}>
                      {option.label || "—"}
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
      {selected.length > 0 ? (
        <div className="flex flex-wrap gap-1">
          {selected.map((value) => (
            <Badge key={value} variant="secondary" className="gap-1 pr-1">
              <span className="max-w-56 truncate">
                {ipFilterValueLabel(kind, value, labels)}
              </span>
              <button
                type="button"
                aria-label={`Remove ${ipFilterValueLabel(kind, value, labels)}`}
                className="rounded-sm opacity-70 hover:opacity-100"
                onClick={() =>
                  onChange(selected.filter((item) => item !== value))
                }
              >
                <XIcon className="size-3" />
              </button>
            </Badge>
          ))}
        </div>
      ) : null}
    </Field>
  );
}

/** The sheet's named inputs; separate from the portal so tests can render them. */
export function IpAddressFilterFields({
  filters,
  labels: initialLabels,
}: {
  filters: WorkspaceIpFilters;
  labels: Record<string, string>;
}) {
  const [draft, setDraft] = useState(filters);
  const [labels, setLabels] = useState(initialLabels);
  const change =
    (key: IpListFilterKey) =>
    (values: string[], optionLabel?: [string, string]) => {
      if (optionLabel)
        setLabels((current) => ({
          ...current,
          [optionLabel[0]]: optionLabel[1],
        }));
      setDraft((current) =>
        key === "country"
          ? {
              ...current,
              country: values,
              // Regions and cities belong to the country they were chosen in.
              ...(values.length === 1 && values[0] === current.country[0]
                ? {}
                : { region: [], city: [] }),
            }
          : { ...current, [key]: values },
      );
    };
  const country = draft.country.length === 1 ? draft.country[0] : "";
  const regionScope = draft.region
    .map((region) => `&region=${encodeURIComponent(region)}`)
    .join("");
  return (
    <FieldGroup className="gap-4 px-4">
      {filters.search ? (
        <input type="hidden" name="search" value={filters.search} />
      ) : null}
      {filters.version !== "any" ? (
        <input type="hidden" name="version" value={filters.version} />
      ) : null}
      <FacetPicker
        name="asn"
        kind="asn"
        label="ASN / organization"
        placeholder="AS15169, 15169 or an organization…"
        selected={draft.asn}
        labels={labels}
        onChange={change("asn")}
      />
      <FacetPicker
        name="country"
        kind="country"
        label="Country"
        placeholder="Country name or code…"
        selected={draft.country}
        labels={labels}
        onChange={change("country")}
      />
      {country ? (
        <>
          <FacetPicker
            name="region"
            kind="region"
            label="Region"
            placeholder="Region name or code…"
            scope={`&country=${country}`}
            selected={draft.region}
            labels={labels}
            onChange={change("region")}
          />
          <FacetPicker
            name="city"
            kind="city"
            label="City"
            placeholder="City name…"
            scope={`&country=${country}${regionScope}`}
            selected={draft.city}
            labels={labels}
            onChange={change("city")}
          />
        </>
      ) : (
        <FieldDescription>
          Choose exactly one country to filter by region or city.
        </FieldDescription>
      )}
    </FieldGroup>
  );
}

export function IpAddressFilterSheet({
  filters,
  labels,
}: {
  filters: WorkspaceIpFilters;
  labels: Record<string, string>;
}) {
  const chips = IP_LIST_FILTER_KEYS.flatMap((key) =>
    filters[key].map((value) => ({
      param: `${key}:${value}`,
      label: `${FILTER_NAMES[key]} ${ipFilterValueLabel(key, value, labels)}`,
    })),
  );
  return (
    <ListFilterSheet
      chips={chips}
      clearHref={workspaceIpAddressesHref({
        ...EMPTY_WORKSPACE_IP_FILTERS,
        search: filters.search,
        version: filters.version,
      })}
      hrefWithout={(chip) =>
        workspaceIpAddressesHref(withoutFilterValue(filters, chip))
      }
      title="Filter IP addresses"
      description="Filter by ASN or organization, country, and, within one country, region or city. Counts are addresses in the search table. Filters can be bookmarked or shared; Select all matching uses them too."
    >
      <IpAddressFilterFields filters={filters} labels={labels} />
    </ListFilterSheet>
  );
}
