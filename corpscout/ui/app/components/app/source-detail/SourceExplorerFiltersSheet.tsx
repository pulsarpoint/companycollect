import { useEffect, useMemo, useState } from "react";
import { Filter, X } from "lucide-react";

import type { SourceExplorerFormFilterOption } from "~/types/api";
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
}

interface SourceExplorerFiltersSheetProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  forms: SourceExplorerFormFilterOption[];
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

export function SourceExplorerFiltersSheet({
  open,
  onOpenChange,
  forms,
  value,
  loading = false,
  error,
  onApply,
  onClear,
}: SourceExplorerFiltersSheetProps) {
  const [draftActiveOnly, setDraftActiveOnly] = useState(value.activeOnly);
  const [draftFormCodes, setDraftFormCodes] = useState(value.formCodes);
  const [formSearch, setFormSearch] = useState("");

  useEffect(() => {
    if (!open) return;
    setDraftActiveOnly(value.activeOnly);
    setDraftFormCodes(value.formCodes);
    setFormSearch("");
  }, [open, value.activeOnly, value.formCodes]);

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

  const filteredForms = useMemo(() => {
    const query = formSearch.trim().toLowerCase();
    if (!query) return forms;
    return forms.filter((form) =>
      `${form.code} ${form.description}`.toLowerCase().includes(query),
    );
  }, [formSearch, forms]);

  function toggleForm(code: string, checked: boolean) {
    setDraftFormCodes((current) => {
      if (checked) {
        if (current.includes(code)) return current;
        return [...current, code];
      }
      return current.filter((item) => item !== code);
    });
  }

  function clearDraft() {
    setDraftActiveOnly(false);
    setDraftFormCodes([]);
    setFormSearch("");
  }

  function applyFilters() {
    onApply({
      activeOnly: draftActiveOnly,
      formCodes: draftFormCodes,
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
                  <Badge key={form.code} variant="outline" asChild>
                    <button
                      type="button"
                      onClick={() => toggleForm(form.code, false)}
                      aria-label={`Remove ${formOptionLabel(form)}`}
                    >
                      <span>{formOptionLabel(form)}</span>
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
