import { Form } from "react-router";
import { ListFilter } from "lucide-react";
import { EU_EEA_COUNTRIES } from "~/lib/eu-countries";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetFooter,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "~/components/ui/sheet";

export interface ProcurementFilterValues {
  country: string;
  from: string;
  to: string;
  buyer: string;
  winner: string;
  noticeType: string;
  awardResult: string;
  valueMin: string;
  valueMax: string;
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="space-y-1.5">
      <Label className="text-sm font-medium">{label}</Label>
      {children}
    </div>
  );
}

/** Sheet-based filters for one register table, mirroring the companies
 * FilterSidebar interaction. Sections render only when the selected table
 * has the backing column. Submitting navigates via GET so every filter
 * lives in the URL; page resets implicitly because no `page` input is kept. */
export function ProcurementFilterSheet({
  values,
  available,
  options,
  table,
}: {
  values: ProcurementFilterValues;
  available: {
    country: boolean;
    date: boolean;
    buyer: boolean;
    winner: boolean;
    noticeType: boolean;
    awardResult: boolean;
    usdValue: boolean;
  };
  options: { noticeTypes: string[]; awardResults: string[]; activeCountries: string[] };
  table: string;
}) {
  const activeCount = Object.values(values).filter((v) => v !== "").length;
  const active = new Set(options.activeCountries);

  function enumSelect(name: string, value: string, choices: string[]) {
    return (
      <Select name={name} defaultValue={value === "" ? undefined : value}>
        <SelectTrigger className="w-full" size="sm">
          <SelectValue placeholder="Any" />
        </SelectTrigger>
        <SelectContent>
          {choices.map((choice) => (
            <SelectItem key={choice} value={choice}>
              {choice}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    );
  }

  return (
    <Sheet>
      <SheetTrigger render={<Button variant="outline" size="sm" />}>
        <ListFilter className="size-4" />
        Filters
        {activeCount > 0 ? <Badge variant="secondary">{activeCount}</Badge> : null}
      </SheetTrigger>
      <SheetContent side="right" className="w-96 overflow-y-auto">
        <SheetHeader>
          <SheetTitle>Filter records</SheetTitle>
        </SheetHeader>
        {/* Keyed on the applied values so navigation remounts the uncontrolled fields — otherwise Clear all would leave stale DOM values in the still-mounted sheet. */}
        <Form key={JSON.stringify(values)} method="get" className="space-y-4 px-4 pb-6">
          <input type="hidden" name="table" value={table} />
          {available.country ? (
            <Field label="Country">
              <Select name="country" defaultValue={values.country === "" ? undefined : values.country}>
                <SelectTrigger className="w-full" size="sm">
                  <SelectValue placeholder="Any" />
                </SelectTrigger>
                <SelectContent>
                  {EU_EEA_COUNTRIES.map((c) => (
                    <SelectItem key={c.iso2} value={c.iso2} disabled={!active.has(c.iso2)}>
                      {c.name}
                      {active.has(c.iso2) ? "" : " (not loaded)"}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </Field>
          ) : null}
          {available.date ? (
            <>
              <Field label="Published from">
                <Input type="date" name="from" defaultValue={values.from} />
              </Field>
              <Field label="Published to">
                <Input type="date" name="to" defaultValue={values.to} />
              </Field>
            </>
          ) : null}
          {available.buyer ? (
            <Field label="Buyer name contains">
              <Input name="buyer" defaultValue={values.buyer} placeholder="e.g. kommun" />
            </Field>
          ) : null}
          {available.winner ? (
            <Field label="Winner name or org number">
              <Input name="winner" defaultValue={values.winner} placeholder="name or org number" />
            </Field>
          ) : null}
          {available.noticeType ? (
            <Field label="Notice type">{enumSelect("noticeType", values.noticeType, options.noticeTypes)}</Field>
          ) : null}
          {available.awardResult ? (
            <Field label="Award result">{enumSelect("awardResult", values.awardResult, options.awardResults)}</Field>
          ) : null}
          {available.usdValue ? (
            <div className="grid grid-cols-2 gap-2">
              <Field label="Min value (USD)">
                <Input type="number" name="valueMin" defaultValue={values.valueMin} min="0" />
              </Field>
              <Field label="Max value (USD)">
                <Input type="number" name="valueMax" defaultValue={values.valueMax} min="0" />
              </Field>
            </div>
          ) : null}
          <SheetFooter className="px-0">
            <Button type="submit">Apply filters</Button>
            <Button
              type="submit"
              variant="ghost"
              name="clear"
              value="1"
              formAction="?"
              formMethod="get"
            >
              Clear all
            </Button>
          </SheetFooter>
        </Form>
      </SheetContent>
    </Sheet>
  );
}
