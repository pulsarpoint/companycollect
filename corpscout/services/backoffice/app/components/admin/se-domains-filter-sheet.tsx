import { companySourceLabel } from "~/components/admin/company-source-strip";
import { ListFilterSheet } from "~/components/admin/list-filter-sheet";
import { Checkbox } from "~/components/ui/checkbox";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from "~/components/ui/select";
import {
  DOMAIN_ASSOCIATIONS, DOMAIN_SOURCE_VALUES, DOMAIN_STATUSES,
  EMPTY_SE_DOMAINS_FILTERS, seDomainsHref, type SeDomainsFilters,
} from "~/lib/se-domains-filters";

const ANY = "__any__";

function DomainsSelect({ name, label, value, options, labelOf = (option: string) => option }: {
  name: string;
  label: string;
  value: string;
  options: readonly string[];
  labelOf?: (option: string) => string;
}) {
  const items = [{ value: ANY, label: "Any" }, ...options.map((option) => ({ value: option, label: labelOf(option) }))];
  return (
    <Select name={name} defaultValue={value === "" ? ANY : value} items={items}>
      <SelectTrigger className="w-full" size="sm" aria-label={label}>
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {items.map((item) => <SelectItem key={item.value} value={item.value}>{item.label}</SelectItem>)}
        </SelectGroup>
      </SelectContent>
    </Select>
  );
}

/** Separate from the portal so the form's named inputs can also be rendered in tests. */
export function SeDomainsFilterFields({ filters, pageSize }: { filters: SeDomainsFilters; pageSize: number }) {
  return (
    <FieldGroup className="gap-3 px-4">
      <input type="hidden" name="pageSize" value={pageSize} />
      <Field className="gap-1">
        <FieldLabel htmlFor="domains-domain">Domain</FieldLabel>
        <Input id="domains-domain" name="domain" defaultValue={filters.domain} placeholder="Contains…" />
      </Field>
      <Field className="gap-1">
        <FieldLabel htmlFor="domains-company">Company id</FieldLabel>
        <Input id="domains-company" name="company" defaultValue={filters.company} placeholder="Exact id" inputMode="numeric" />
      </Field>
      <Field className="gap-1">
        <FieldLabel>Source</FieldLabel>
        <DomainsSelect name="source" label="Source" value={filters.source} options={DOMAIN_SOURCE_VALUES} labelOf={companySourceLabel} />
      </Field>
      <Field className="gap-1">
        <FieldLabel>Association</FieldLabel>
        <DomainsSelect name="association" label="Association" value={filters.association} options={DOMAIN_ASSOCIATIONS} labelOf={(value) => value.replaceAll("_", " ")} />
      </Field>
      <Field className="gap-1">
        <FieldLabel>Status</FieldLabel>
        <DomainsSelect name="status" label="Status" value={filters.status} options={DOMAIN_STATUSES} labelOf={(value) => value[0].toUpperCase() + value.slice(1)} />
      </Field>
      <Field className="gap-1">
        <FieldLabel htmlFor="domains-min-confidence">Min confidence</FieldLabel>
        <Input id="domains-min-confidence" name="minConfidence" type="number" min={0} max={1} step="any" defaultValue={filters.minConfidence} placeholder="0" />
      </Field>
      <Field className="gap-1">
        <FieldLabel htmlFor="domains-max-confidence">Max confidence</FieldLabel>
        <Input id="domains-max-confidence" name="maxConfidence" type="number" min={0} max={1} step="any" defaultValue={filters.maxConfidence} placeholder="1" />
      </Field>
      <Field orientation="horizontal">
        <Checkbox id="domains-shared" name="shared" value="1" defaultChecked={filters.shared === "1"} />
        <FieldLabel htmlFor="domains-shared">Shared only</FieldLabel>
      </Field>
    </FieldGroup>
  );
}

const FILTER_LABELS: Record<keyof SeDomainsFilters, string> = {
  domain: "Domain", company: "Company", source: "Source", association: "Association",
  status: "Status", minConfidence: "Min confidence", maxConfidence: "Max confidence", shared: "Shared only",
};

export function SeDomainsFilterSheet({ filters, pageSize }: { filters: SeDomainsFilters; pageSize: number }) {
  const chips = (Object.keys(FILTER_LABELS) as (keyof SeDomainsFilters)[])
    .filter((param) => filters[param] !== "")
    .map((param) => ({
      param,
      label: param === "shared" ? FILTER_LABELS[param] : `${FILTER_LABELS[param]} ${param === "source" ? companySourceLabel(filters[param]) : filters[param].replaceAll("_", " ")}`,
    }));
  return (
    <ListFilterSheet
      chips={chips}
      clearHref={seDomainsHref(EMPTY_SE_DOMAINS_FILTERS, 1, pageSize)}
      hrefWithout={(param) => seDomainsHref({ ...filters, [param]: "" }, 1, pageSize)}
      title="Filter domains"
      description="Filter by source, company, association, confidence or shared domains. Filters can be bookmarked or shared. Page size is kept."
    >
      <SeDomainsFilterFields filters={filters} pageSize={pageSize} />
    </ListFilterSheet>
  );
}
