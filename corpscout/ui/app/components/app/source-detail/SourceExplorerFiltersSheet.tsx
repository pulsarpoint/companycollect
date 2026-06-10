import { useEffect, useMemo, useState } from "react";
import { Filter, X } from "lucide-react";

import type {
  SourceExplorerFormFilterOption,
  SourceExplorerIndustryFilterOption,
} from "~/types/api";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
import { Input } from "~/components/ui/input";
import { Separator } from "~/components/ui/separator";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";

interface SourceExplorerFiltersValue {
  activeOnly: boolean;
  formCodes: string[];
  industryNACE: string[];
  sourceIndustries: string[];
}

interface SourceExplorerFiltersSheetProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  forms: SourceExplorerFormFilterOption[];
  industries: SourceExplorerIndustryFilterOption[];
  value: SourceExplorerFiltersValue;
  loading?: boolean;
  error?: string;
  onApply: (value: SourceExplorerFiltersValue) => void;
  onClear: () => void;
}

function formOptionLabel(option: SourceExplorerFormFilterOption): string {
  if (option.description) return `${option.description} (${option.code})`;
  return option.code;
}

function industrySelectionKey(
  kind: SourceExplorerIndustryFilterOption["kind"],
  value: string,
): string {
  return `${kind}:${value}`;
}

function industryOptionLabel(option: SourceExplorerIndustryFilterOption): string {
  if (option.kind === "nace") {
    const level = option.level_name ? `${option.level_name} ` : "";
    const title =
      option.title && option.title !== option.code ? ` ${option.title}` : "";
    return `${level}${option.code}${title}`.trim();
  }
  const code = [option.code_set, option.code].filter(Boolean).join(" ");
  if (option.title) return `${option.title} (${code})`;
  return code || option.filter_value;
}

function industryOptionDetail(option: SourceExplorerIndustryFilterOption): string {
  if (option.kind === "nace") {
    return option.breadcrumb || option.title || option.code;
  }
  const mapped = option.mapped_nace_code
    ? `NACE ${option.mapped_nace_code}`
    : "Unmapped";
  return [option.code_set, option.code, mapped].filter(Boolean).join(" / ");
}

function fallbackIndustryLabel(
  kind: SourceExplorerIndustryFilterOption["kind"],
  value: string,
): string {
  if (kind === "nace") return `NACE ${value}`;
  return value;
}

export function SourceExplorerFiltersSheet({
  open,
  onOpenChange,
  forms,
  industries,
  value,
  loading = false,
  error,
  onApply,
  onClear,
}: SourceExplorerFiltersSheetProps) {
  const [draftActiveOnly, setDraftActiveOnly] = useState(value.activeOnly);
  const [draftFormCodes, setDraftFormCodes] = useState(value.formCodes);
  const [draftIndustryNACE, setDraftIndustryNACE] = useState(value.industryNACE);
  const [draftSourceIndustries, setDraftSourceIndustries] = useState(
    value.sourceIndustries,
  );
  const [formSearch, setFormSearch] = useState("");
  const [industrySearch, setIndustrySearch] = useState("");

  useEffect(() => {
    if (!open) return;
    setDraftActiveOnly(value.activeOnly);
    setDraftFormCodes(value.formCodes);
    setDraftIndustryNACE(value.industryNACE);
    setDraftSourceIndustries(value.sourceIndustries);
    setFormSearch("");
    setIndustrySearch("");
  }, [
    open,
    value.activeOnly,
    value.formCodes,
    value.industryNACE,
    value.sourceIndustries,
  ]);

  const formsByCode = useMemo(
    () => new Map(forms.map((form) => [form.code, form])),
    [forms],
  );

  const selectedForms = useMemo(
    () =>
      draftFormCodes.map(
        (code) =>
          formsByCode.get(code) ?? {
            code,
            description: "",
            count: 0,
          },
      ),
    [draftFormCodes, formsByCode],
  );

  const industriesByKey = useMemo(
    () =>
      new Map(
        industries.map((industry) => [
          industrySelectionKey(industry.kind, industry.filter_value),
          industry,
        ]),
      ),
    [industries],
  );

  const selectedIndustries = useMemo(
    () => [
      ...draftIndustryNACE.map((value) => {
        const key = industrySelectionKey("nace", value);
        const option = industriesByKey.get(key);
        return {
          key,
          kind: "nace" as const,
          value,
          label: option
            ? industryOptionLabel(option)
            : fallbackIndustryLabel("nace", value),
        };
      }),
      ...draftSourceIndustries.map((value) => {
        const key = industrySelectionKey("source_industry", value);
        const option = industriesByKey.get(key);
        return {
          key,
          kind: "source_industry" as const,
          value,
          label: option
            ? industryOptionLabel(option)
            : fallbackIndustryLabel("source_industry", value),
        };
      }),
    ],
    [draftIndustryNACE, draftSourceIndustries, industriesByKey],
  );

  const filteredForms = useMemo(() => {
    const query = formSearch.trim().toLowerCase();
    if (!query) return forms;
    return forms.filter((form) =>
      `${form.code} ${form.description}`.toLowerCase().includes(query),
    );
  }, [formSearch, forms]);

  const filteredIndustries = useMemo(() => {
    const query = industrySearch.trim().toLowerCase();
    if (!query) return industries.slice(0, 200);
    return industries
      .filter((industry) =>
        [
          industry.search_text,
          industry.title,
          industry.breadcrumb,
          industry.code,
          industry.code_set,
          industry.filter_value,
          industry.mapped_nace_code,
        ]
          .join(" ")
          .toLowerCase()
          .includes(query),
      )
      .slice(0, 200);
  }, [industries, industrySearch]);

  function toggleForm(code: string, checked: boolean) {
    setDraftFormCodes((current) => {
      if (checked) {
        if (current.includes(code)) return current;
        return [...current, code];
      }
      return current.filter((item) => item !== code);
    });
  }

  function toggleIndustry(
    option: SourceExplorerIndustryFilterOption,
    checked: boolean,
  ) {
    if (option.kind === "nace") {
      setDraftIndustryNACE((current) => {
        if (checked) {
          if (current.includes(option.filter_value)) return current;
          return [...current, option.filter_value];
        }
        return current.filter((item) => item !== option.filter_value);
      });
      return;
    }

    setDraftSourceIndustries((current) => {
      if (checked) {
        if (current.includes(option.filter_value)) return current;
        return [...current, option.filter_value];
      }
      return current.filter((item) => item !== option.filter_value);
    });
  }

  function removeIndustry(
    kind: SourceExplorerIndustryFilterOption["kind"],
    value: string,
  ) {
    if (kind === "nace") {
      setDraftIndustryNACE((current) => current.filter((item) => item !== value));
      return;
    }
    setDraftSourceIndustries((current) =>
      current.filter((item) => item !== value),
    );
  }

  function clearDraft() {
    setDraftActiveOnly(false);
    setDraftFormCodes([]);
    setDraftIndustryNACE([]);
    setDraftSourceIndustries([]);
    setFormSearch("");
    setIndustrySearch("");
  }

  function applyFilters() {
    onApply({
      activeOnly: draftActiveOnly,
      formCodes: draftFormCodes,
      industryNACE: draftIndustryNACE,
      sourceIndustries: draftSourceIndustries,
    });
    onOpenChange(false);
  }

  function clearFilters() {
    clearDraft();
    onClear();
    onOpenChange(false);
  }

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-xl">
        <SheetHeader>
          <SheetTitle>Filters</SheetTitle>
          <SheetDescription>
            Narrow the Finland PRH YTJ explorer results.
          </SheetDescription>
        </SheetHeader>

        <div className="flex flex-col gap-5 px-4">
          <label className="flex cursor-pointer items-center gap-3 rounded-md border p-3 text-sm">
            <Checkbox
              checked={draftActiveOnly}
              onCheckedChange={(checked) =>
                setDraftActiveOnly(checked === true)
              }
            />
            <span className="font-medium">Active only</span>
          </label>

          <Separator />

          <section className="flex flex-col gap-3">
            <div className="flex items-center justify-between gap-3">
              <div>
                <h3 className="text-sm font-medium">Industry</h3>
                <p className="text-xs text-muted-foreground">
                  Filter by NACE hierarchy or original PRH YTJ industry code.
                </p>
              </div>
              {selectedIndustries.length > 0 ? (
                <Badge variant="secondary">
                  {selectedIndustries.length.toLocaleString()} selected
                </Badge>
              ) : null}
            </div>

            {selectedIndustries.length > 0 ? (
              <div className="flex flex-wrap gap-2">
                {selectedIndustries.map((industry) => (
                  <Badge
                    key={industry.key}
                    variant="outline"
                    className="max-w-full"
                    asChild
                  >
                    <button
                      type="button"
                      onClick={() =>
                        removeIndustry(industry.kind, industry.value)
                      }
                      aria-label={`Remove ${industry.label}`}
                    >
                      <span className="truncate">{industry.label}</span>
                      <X />
                    </button>
                  </Badge>
                ))}
              </div>
            ) : null}

            <Input
              value={industrySearch}
              onChange={(event) => setIndustrySearch(event.target.value)}
              placeholder="Search industries"
            />

            <div className="max-h-96 overflow-y-auto rounded-md border">
              {loading ? (
                <div className="p-3 text-sm text-muted-foreground">
                  Loading industries...
                </div>
              ) : error ? (
                <div className="p-3 text-sm text-muted-foreground">{error}</div>
              ) : filteredIndustries.length > 0 ? (
                <div className="flex flex-col">
                  {filteredIndustries.map((industry) => {
                    const checked =
                      industry.kind === "nace"
                        ? draftIndustryNACE.includes(industry.filter_value)
                        : draftSourceIndustries.includes(industry.filter_value);
                    return (
                      <label
                        key={industry.id}
                        className="flex cursor-pointer items-start gap-3 border-b p-3 text-sm last:border-b-0 hover:bg-muted/40"
                      >
                        <Checkbox
                          checked={checked}
                          onCheckedChange={(nextChecked) =>
                            toggleIndustry(industry, nextChecked === true)
                          }
                        />
                        <span className="flex min-w-0 flex-1 flex-col gap-1">
                          <span className="truncate font-medium">
                            {industryOptionLabel(industry)}
                          </span>
                          <span className="truncate font-mono text-xs text-muted-foreground">
                            {industryOptionDetail(industry)}
                          </span>
                        </span>
                        <span className="text-xs text-muted-foreground">
                          {industry.count.toLocaleString()}
                        </span>
                      </label>
                    );
                  })}
                </div>
              ) : (
                <div className="p-3 text-sm text-muted-foreground">
                  No industries found.
                </div>
              )}
            </div>
          </section>

          <Separator />

          <section className="flex flex-col gap-3">
            <div className="flex items-center justify-between gap-3">
              <div>
                <h3 className="text-sm font-medium">Form</h3>
                <p className="text-xs text-muted-foreground">
                  Select one or more company forms.
                </p>
              </div>
              {draftFormCodes.length > 0 ? (
                <Badge variant="secondary">
                  {draftFormCodes.length.toLocaleString()} selected
                </Badge>
              ) : null}
            </div>

            {selectedForms.length > 0 ? (
              <div className="flex flex-wrap gap-2">
                {selectedForms.map((form) => (
                  <Badge
                    key={form.code}
                    variant="outline"
                    className="max-w-full"
                    asChild
                  >
                    <button
                      type="button"
                      onClick={() => toggleForm(form.code, false)}
                      aria-label={`Remove ${formOptionLabel(form)}`}
                    >
                      <span className="truncate">{formOptionLabel(form)}</span>
                      <X />
                    </button>
                  </Badge>
                ))}
              </div>
            ) : null}

            <Input
              value={formSearch}
              onChange={(event) => setFormSearch(event.target.value)}
              placeholder="Search forms"
            />

            <div className="max-h-80 overflow-y-auto rounded-md border">
              {loading ? (
                <div className="p-3 text-sm text-muted-foreground">
                  Loading forms...
                </div>
              ) : error ? (
                <div className="p-3 text-sm text-muted-foreground">{error}</div>
              ) : filteredForms.length > 0 ? (
                <div className="flex flex-col">
                  {filteredForms.map((form) => {
                    const checked = draftFormCodes.includes(form.code);
                    return (
                      <label
                        key={form.code}
                        className="flex cursor-pointer items-start gap-3 border-b p-3 text-sm last:border-b-0 hover:bg-muted/40"
                      >
                        <Checkbox
                          checked={checked}
                          onCheckedChange={(nextChecked) =>
                            toggleForm(form.code, nextChecked === true)
                          }
                        />
                        <span className="flex min-w-0 flex-1 flex-col gap-1">
                          <span className="truncate font-medium">
                            {form.description || form.code}
                          </span>
                          <span className="font-mono text-xs text-muted-foreground">
                            {form.code}
                          </span>
                        </span>
                        <span className="text-xs text-muted-foreground">
                          {form.count.toLocaleString()}
                        </span>
                      </label>
                    );
                  })}
                </div>
              ) : (
                <div className="p-3 text-sm text-muted-foreground">
                  No forms found.
                </div>
              )}
            </div>
          </section>
        </div>

        <SheetFooter>
          <div className="flex flex-col gap-2 sm:flex-row sm:justify-between">
            <Button type="button" variant="ghost" onClick={clearDraft}>
              Reset selection
            </Button>
            <div className="flex gap-2">
              <Button type="button" variant="outline" onClick={clearFilters}>
                Clear filters
              </Button>
              <Button type="button" onClick={applyFilters}>
                <Filter data-icon="inline-start" />
                Apply
              </Button>
            </div>
          </div>
        </SheetFooter>
      </SheetContent>
    </Sheet>
  );
}
