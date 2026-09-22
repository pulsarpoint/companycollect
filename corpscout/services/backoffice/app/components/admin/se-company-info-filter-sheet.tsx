import { legalFormOptionLabel } from "~/lib/se-legal-form";
import { Checkbox } from "~/components/ui/checkbox";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import { ListFilterSheet } from "~/components/admin/list-filter-sheet";
import type {
  SeCompanyInfoCorrectionFilterOptions,
  SeCompanyInfoFilterOptions,
} from "~/lib/se-company-info-lists.server";
import {
  ANY_FILTER_VALUE,
  correctionFilterChips,
  correctionsListSearch,
  EMPTY_CORRECTION_FILTERS,
  EMPTY_INFO_FILTERS,
  infoFilterChips,
  infoListSearch,
  optionLabel,
  optionValue,
  PROFILE_DATATYPES,
  PROFILE_SOURCE_VALUES,
  profileSourceLabel,
  selectValue,
  type SeCompanyInfoCorrectionsTableFilters,
  type SeCompanyInfoTableFilters,
  type TableView,
  YES_NO_VALUES,
} from "~/lib/se-company-info-filters";
import {
  SE_INFO_CORRECTION_KINDS,
  SE_INFO_CORRECTION_STATUSES,
} from "~/lib/se-info-corrections";

/**
 * Both list pages' filters, in a right-hand `Sheet` opened by one "Filters"
 * button, mirroring `data-table/contract-filter-sheet.tsx`'s Sheet usage and
 * button-with-count.
 *
 * The sheet holds a plain GET `<Form>`: every field is a named input, Apply is
 * its submit, and the browser builds the next URL -- so the filters work with
 * no JavaScript state to keep in step, and `pageSize`/`sort`/`dir` ride along
 * as hidden fields (a filter change deliberately resets `page`, but must never
 * silently reset the reviewer's page size or the column they sorted by).
 *
 * The fields are exported separately from the sheet that wraps them: a Base UI
 * dialog renders through a portal, which produces nothing at all during SSR,
 * so a test asserting on the fields renders `...FilterFields` directly.
 */

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <Label className="text-xs font-medium">{label}</Label>
      {children}
    </div>
  );
}

/** A select whose options are fixed in code (an enum) or read from the column
 * itself; either way "Any" is a real, selectable item, because Base UI
 * reserves the empty string for the unselected state. */
function FilterSelect({
  name,
  label,
  value,
  options,
  labelOf = (option: string) => option,
}: {
  name: string;
  /** Also the control's aria-label: a Base UI select's trigger is a button
   * whose only text is the current value, so the visible <Label> above it does
   * not name it to a screen reader. */
  label: string;
  value: string;
  options: readonly string[];
  labelOf?: (option: string) => string;
}) {
  return (
    <Select name={name} defaultValue={selectValue(value)}>
      <SelectTrigger className="w-full" size="sm" aria-label={label}>
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value={ANY_FILTER_VALUE}>Any</SelectItem>
        {options.map((option) => (
          <SelectItem key={option} value={optionValue(option)}>
            {labelOf(option)}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

/** Hidden fields for everything that is not a filter but must survive one
 * being applied. */
function ViewFields({ view }: { view: TableView }) {
  return (
    <>
      <input type="hidden" name="pageSize" value={view.pageSize} />
      <input type="hidden" name="sort" value={view.sort} />
      <input type="hidden" name="dir" value={view.dir} />
    </>
  );
}

export function SeCompanyInfoFilterFields({
  filters,
  options,
  view,
}: {
  filters: SeCompanyInfoTableFilters;
  options: SeCompanyInfoFilterOptions;
  view: TableView;
}) {
  const byCode = new Map(options.legalForms.map((form) => [form.code, form]));
  return (
    <div className="flex flex-col gap-3 px-4">
      <ViewFields view={view} />
      <Field label="Company id">
        <Input
          name="companyId"
          aria-label="Company id"
          defaultValue={filters.companyId}
          placeholder="Exact id"
        />
      </Field>
      <Field label="Name">
        <Input
          name="name"
          aria-label="Name"
          defaultValue={filters.name}
          placeholder="Legal name contains"
        />
      </Field>
      <Field label="Status">
        <FilterSelect
          name="status"
          label="Status"
          value={filters.status}
          options={options.statuses}
          labelOf={optionLabel}
        />
      </Field>
      <Field label="Legal form">
        {/* The option VALUE is the code the filter submits; its TEXT is what
            the curated dictionary calls that code, in both languages, since a
            bare AB-ORGFO or 71 is unreadable. */}
        <FilterSelect
          name="legalForm"
          label="Legal form"
          value={filters.legalForm}
          options={options.legalForms.map((form) => form.code)}
          labelOf={(code) =>
            legalFormOptionLabel(
              byCode.get(code) ?? { code, label_sv: "", label_en: "" },
            )
          }
        />
      </Field>
      <Field label="Entity">
        <Select name="entity" defaultValue={selectValue(filters.entity)}>
          <SelectTrigger className="w-full" size="sm" aria-label="Entity">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ANY_FILTER_VALUE}>Any</SelectItem>
            <SelectItem value="legal">Legal (10-digit)</SelectItem>
            <SelectItem value="sole">Sole trader (12-digit)</SelectItem>
          </SelectContent>
        </Select>
      </Field>
      <Field label="Description">
        <FilterSelect
          name="description"
          label="Description"
          value={filters.description}
          options={YES_NO_VALUES}
        />
      </Field>
      <fieldset className="flex flex-col gap-1.5">
        {/* The presence columns as a filter: checkboxes, because unlike the
            single-choice selects above a reviewer may require several at once
            -- and they AND together (each becomes its own `= 1` predicate), so
            the description says "ALL" in as many words. Built by mapping the
            catalog, like the columns themselves, so the group can never offer
            a flag the table does not show. Each checkbox submits the same
            `datatype` name with its own key as the value: the plain GET form
            emits one repeated `?datatype=` param per ticked box. */}
        <Label className="text-xs font-medium">Has data</Label>
        <p className="text-xs text-muted-foreground">
          Only companies that have ALL selected data.
        </p>
        {PROFILE_DATATYPES.map((datatype) => (
          <label
            key={datatype.key}
            className="flex cursor-pointer items-center gap-2 text-sm"
          >
            <Checkbox
              name="datatype"
              value={datatype.key}
              defaultChecked={filters.datatypes.includes(datatype.key)}
              aria-label={`Has ${datatype.label}`}
            />
            {datatype.label}
          </label>
        ))}
      </fieldset>
      <Field label="Source">
        {/* Which registers built the profile, in ANY datatype -- the same
            question the Sources column's letters answer, and the same
            predicates. The option VALUE is the register's own name (what the
            pipelines' `sources` arrays call it, and what the URL carries); its
            TEXT is what a reader calls it. SCB is offered even though every
            company has it -- the four sources read as one list, and the
            column's own 'S' says the same thing. */}
        <FilterSelect
          name="source"
          label="Source"
          value={filters.source}
          options={PROFILE_SOURCE_VALUES}
          labelOf={profileSourceLabel}
        />
      </Field>
    </div>
  );
}

/**
 * Shared by BOTH correction ledgers: the four filters are the ledger shape, not
 * the info list's, and only the kinds a reviewer may decide differ -- so they
 * are props defaulting to the info ledger's enum. (The "Info" in the name is
 * the info list's; renaming it would churn that page and its tests for nothing.)
 */
export function SeCompanyInfoCorrectionsFilterFields({
  filters,
  options,
  view,
  kinds = SE_INFO_CORRECTION_KINDS,
  statuses = SE_INFO_CORRECTION_STATUSES,
}: {
  filters: SeCompanyInfoCorrectionsTableFilters;
  options: SeCompanyInfoCorrectionFilterOptions;
  view: TableView;
  kinds?: readonly string[];
  statuses?: readonly string[];
}) {
  return (
    <div className="flex flex-col gap-3 px-4">
      <ViewFields view={view} />
      <Field label="Company id">
        <Input
          name="companyId"
          aria-label="Company id"
          defaultValue={filters.companyId}
          placeholder="Exact id"
        />
      </Field>
      <Field label="Kind">
        <FilterSelect name="kind" label="Kind" value={filters.kind} options={kinds} />
      </Field>
      <Field label="Status">
        <FilterSelect
          name="status"
          label="Status"
          value={filters.status}
          options={statuses}
        />
      </Field>
      <Field label="Decided by">
        <FilterSelect
          name="decidedBy"
          label="Decided by"
          value={filters.decidedBy}
          options={options.decidedBy}
          labelOf={optionLabel}
        />
      </Field>
    </div>
  );
}

export function SeCompanyInfoFilterSheet({
  filters,
  view,
  options,
}: {
  filters: SeCompanyInfoTableFilters;
  view: TableView;
  options: SeCompanyInfoFilterOptions;
}) {
  return (
    <ListFilterSheet
      chips={infoFilterChips(filters)}
      clearHref={infoListSearch(EMPTY_INFO_FILTERS, view)}
      hrefWithout={(param) => infoListSearch(filters, view, param)}
      title="Filter companies"
      description="Every filter is a URL parameter, so a filtered list can be shared or bookmarked. Sorting and page size are kept."
    >
      <SeCompanyInfoFilterFields filters={filters} options={options} view={view} />
    </ListFilterSheet>
  );
}

export function SeCompanyInfoCorrectionsFilterSheet({
  filters,
  view,
  options,
  kinds,
  statuses,
}: {
  filters: SeCompanyInfoCorrectionsTableFilters;
  view: TableView;
  options: SeCompanyInfoCorrectionFilterOptions;
  /** The ledger's own kinds/statuses; the info ledger's when omitted. */
  kinds?: readonly string[];
  statuses?: readonly string[];
}) {
  return (
    <ListFilterSheet
      chips={correctionFilterChips(filters)}
      clearHref={correctionsListSearch(EMPTY_CORRECTION_FILTERS, view)}
      hrefWithout={(param) => correctionsListSearch(filters, view, param)}
      title="Filter corrections"
      description="Every filter is a URL parameter, so a filtered ledger can be shared or bookmarked. Sorting and page size are kept."
    >
      <SeCompanyInfoCorrectionsFilterFields
        filters={filters}
        options={options}
        view={view}
        kinds={kinds}
        statuses={statuses}
      />
    </ListFilterSheet>
  );
}
