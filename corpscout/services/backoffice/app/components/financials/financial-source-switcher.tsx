import { Building2, FileSpreadsheet } from "lucide-react";
import {
  financialSourceCopy,
  type FinancialLocale,
  type SwedenFinancialSourceId,
} from "~/components/financials/copy";
import {
  Field,
  FieldLabel,
  FieldLegend,
  FieldSet,
} from "~/components/ui/field";
import { RadioGroup, RadioGroupItem } from "~/components/ui/radio-group";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import type { CompanyFinancialSource } from "~/lib/queries.server";

function isSwedenFinancialSourceId(id: string): id is SwedenFinancialSourceId {
  return id === "bolagsverket-annual-accounts" || id === "esef";
}

export function FinancialSourceSwitcher({
  sources,
  selectedSourceId,
  onSourceChange,
  locale,
  onLocaleChange,
}: {
  sources: CompanyFinancialSource[];
  selectedSourceId: string;
  onSourceChange: (sourceId: string) => void;
  locale: FinancialLocale;
  onLocaleChange: (locale: FinancialLocale) => void;
}) {
  const copy = financialSourceCopy[locale];

  return (
    <section className="border-b pb-6">
      <div className="flex flex-wrap items-end justify-between gap-5">
        <div>
          <h2 className="text-xl font-semibold tracking-tight">
            {copy.selectorTitle}
          </h2>
          <p className="text-muted-foreground mt-1 max-w-3xl text-sm">
            {copy.selectorDescription}
          </p>
        </div>

        <FieldSet className="w-fit gap-2">
          <FieldLegend variant="label">
            {locale === "sv" ? "Språk" : "Language"}
          </FieldLegend>
          <RadioGroup
            value={locale}
            onValueChange={(value) => onLocaleChange(value as FinancialLocale)}
            className="flex w-fit items-center gap-4"
            aria-label={locale === "sv" ? "Språk" : "Language"}
          >
            <Field orientation="horizontal" className="w-fit">
              <RadioGroupItem value="sv" id="financial-language-sv" />
              <FieldLabel
                htmlFor="financial-language-sv"
                className="font-normal"
              >
                Svenska
              </FieldLabel>
            </Field>
            <Field orientation="horizontal" className="w-fit">
              <RadioGroupItem value="en" id="financial-language-en" />
              <FieldLabel
                htmlFor="financial-language-en"
                className="font-normal"
              >
                English
              </FieldLabel>
            </Field>
          </RadioGroup>
        </FieldSet>
      </div>

      <Tabs
        value={selectedSourceId}
        onValueChange={onSourceChange}
        className="mt-5"
      >
        <TabsList className="h-auto max-w-full justify-start gap-1 overflow-x-auto p-1">
          {sources.map((source) => {
            const sourceCopy = isSwedenFinancialSourceId(source.id)
              ? copy.sources[source.id]
              : null;
            const years = source.financials.map((row) => row.fiscal_year);
            const yearRange = years.length
              ? years.length === 1
                ? years[0]
                : `${years.at(-1)}–${years[0]}`
              : null;
            const SourceIcon =
              source.kind === "registry" ? Building2 : FileSpreadsheet;

            return (
              <TabsTrigger
                key={source.id}
                value={source.id}
                className="h-auto min-w-44 items-start justify-start px-3 py-2 text-left"
              >
                <SourceIcon className="mt-0.5" />
                <span className="flex flex-col items-start">
                  <span className="text-foreground">
                    {sourceCopy?.shortTitle ?? source.title}
                  </span>
                  <span className="text-muted-foreground text-xs font-normal">
                    {sourceCopy?.scope ??
                      source.financials[0]?.accounting_scope}
                    {yearRange ? ` · ${yearRange}` : ""}
                  </span>
                </span>
              </TabsTrigger>
            );
          })}
        </TabsList>
      </Tabs>
    </section>
  );
}
