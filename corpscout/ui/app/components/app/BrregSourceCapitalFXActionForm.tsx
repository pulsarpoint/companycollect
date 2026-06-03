import { useEffect, useMemo, useState } from "react";
import { ChevronDown, Play } from "lucide-react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Separator } from "~/components/ui/separator";
import { Switch } from "~/components/ui/switch";
import { api, errorMessage } from "~/lib/api";
import type { BrregActionScope } from "~/components/app/BrregActionScope";

interface Props {
  selectedIds: string[];
  totalCount: number;
  filters: Record<string, string>;
  initialScope?: BrregActionScope;
  onStarted?: () => void;
  onClose: () => void;
}

type CapitalFXRecordMode = "limit" | "all";

function parseRequiredPositiveNumber(value: string) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.floor(parsed);
}

function scopeLabel(
  scope: BrregActionScope,
  selectedCount: number,
  totalCount: number,
) {
  if (scope === "selected")
    return `${selectedCount.toLocaleString()} selected source entries`;
  if (scope === "filtered")
    return `${totalCount.toLocaleString()} source entries matching the current filters`;
  return "Next capital rows missing USD conversion";
}

export function BrregSourceCapitalFXActionForm({
  selectedIds,
  totalCount,
  filters,
  initialScope,
  onStarted,
  onClose,
}: Props) {
  const selectedCount = selectedIds.length;
  const hasFilters = Object.keys(filters).length > 0;
  const defaultScope: BrregActionScope =
    initialScope ??
    (selectedCount > 0 ? "selected" : hasFilters ? "filtered" : "eligible");
  const [scope, setScope] = useState<BrregActionScope>(defaultScope);
  const [recordMode, setRecordMode] = useState<CapitalFXRecordMode>("limit");
  const [limit, setLimit] = useState("1000");
  const [rateDate, setRateDate] = useState("");
  const [forceReprocess, setForceReprocess] = useState(false);
  const [trigger, setTrigger] = useState("manual");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    setScope(defaultScope);
    setRecordMode("limit");
    setLimit(selectedCount > 0 ? String(selectedCount) : "1000");
    setRateDate("");
    setForceReprocess(false);
    setTrigger("manual");
    setAdvancedOpen(false);
  }, [defaultScope, selectedCount]);

  const scopeOptions = useMemo(
    () => [
      {
        value: "selected" as const,
        label: `${selectedCount.toLocaleString()} selected`,
        disabled: selectedCount === 0,
      },
      {
        value: "filtered" as const,
        label: `${totalCount.toLocaleString()} matching filters`,
        disabled: !hasFilters,
      },
      {
        value: "eligible" as const,
        label: "Next eligible capital rows",
        disabled: false,
      },
    ],
    [hasFilters, selectedCount, totalCount],
  );

  const limitDisabled = scope === "selected" || recordMode === "all";
  const effectiveLimit = limitDisabled
    ? scope === "selected"
      ? selectedCount
      : 0
    : parseRequiredPositiveNumber(limit);
  const canSubmit =
    effectiveLimit !== null && (scope !== "selected" || selectedCount > 0);

  async function submit() {
    if (!canSubmit || effectiveLimit === null) return;

    setSubmitting(true);
    try {
      const body: {
        ids?: string[];
        filters?: Record<string, string>;
        limit?: number;
        rate_date?: string;
        force_reprocess?: boolean;
        trigger?: string;
      } = {
        limit: effectiveLimit,
        trigger: trigger.trim() || "manual",
      };

      const trimmedRateDate = rateDate.trim();
      if (trimmedRateDate) body.rate_date = trimmedRateDate;
      if (forceReprocess) body.force_reprocess = true;
      if (scope === "selected") body.ids = selectedIds;
      if (scope === "filtered") body.filters = filters;

      await api.convertBrregSourceCapitalToUSD(body);
      toast.success("BRREG capital FX conversion workflow started.");
      onStarted?.();
      onClose();
    } catch (error) {
      toast.error(
        errorMessage(error, "Failed to start BRREG capital FX conversion."),
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="rounded-md border bg-muted/20 p-3">
        <div className="text-sm font-medium">Convert capital to USD</div>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Reads BRREG source capital rows, uses locally synced exchange rates,
          and stores the converted amount in USD cents.
        </p>
      </div>

      <div className="flex flex-col gap-4">
        <div>
          <h3 className="text-sm font-medium">Required</h3>
          <p className="mt-1 text-xs text-muted-foreground">
            {scopeLabel(scope, selectedCount, totalCount)}
          </p>
        </div>

        <div className="flex flex-col gap-2">
          <Label htmlFor="brreg-source-capital-fx-scope">Records source</Label>
          <select
            id="brreg-source-capital-fx-scope"
            className="h-9 rounded-md border border-input bg-background px-3 text-sm"
            value={scope}
            onChange={(event) =>
              setScope(event.target.value as BrregActionScope)
            }
          >
            {scopeOptions.map((option) => (
              <option
                key={option.value}
                value={option.value}
                disabled={option.disabled}
              >
                {option.label}
              </option>
            ))}
          </select>
          <p className="text-xs leading-5 text-muted-foreground">
            Choose checked rows, the currently filtered table, or the next
            capital rows that have original amount and currency but no USD
            conversion.
          </p>
        </div>

        <div className="flex flex-col gap-2">
          <Label htmlFor="brreg-source-capital-fx-record-mode">
            Records to convert
          </Label>
          <select
            id="brreg-source-capital-fx-record-mode"
            className="h-9 rounded-md border border-input bg-background px-3 text-sm"
            disabled={scope === "selected"}
            value={scope === "selected" ? "limit" : recordMode}
            onChange={(event) =>
              setRecordMode(event.target.value as CapitalFXRecordMode)
            }
          >
            <option value="limit">Limited number</option>
            <option value="all">All matching records</option>
          </select>
          <p className="text-xs leading-5 text-muted-foreground">
            Limited runs are useful while checking conversion behavior. All
            matching records sends limit 0 to the workflow.
          </p>
        </div>

        <div className="flex flex-col gap-2">
          <Label htmlFor="brreg-source-capital-fx-limit">Records</Label>
          <Input
            id="brreg-source-capital-fx-limit"
            type="number"
            min={1}
            disabled={limitDisabled}
            placeholder={recordMode === "all" ? "All matching records" : undefined}
            value={
              scope === "selected"
                ? selectedCount
                : recordMode === "all"
                  ? ""
                  : limit
            }
            onChange={(event) => setLimit(event.target.value)}
          />
          <p className="text-xs leading-5 text-muted-foreground">
            Maximum number of source entries to convert. Selected rows always
            use the selected count.
          </p>
        </div>
      </div>

      <Separator />

      <Button
        type="button"
        variant="ghost"
        className="h-8 justify-between px-0 text-sm font-medium hover:bg-transparent"
        onClick={() => setAdvancedOpen((open) => !open)}
        aria-expanded={advancedOpen}
      >
        Advanced options
        <ChevronDown
          className={`size-4 transition-transform ${advancedOpen ? "rotate-180" : ""}`}
        />
      </Button>

      {advancedOpen && (
        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-2">
            <Label htmlFor="brreg-source-capital-fx-rate-date">Rate date</Label>
            <Input
              id="brreg-source-capital-fx-rate-date"
              type="date"
              value={rateDate}
              onChange={(event) => setRateDate(event.target.value)}
            />
            <p className="text-xs leading-5 text-muted-foreground">
              Optional ECB sheet date. Empty uses the latest synced exchange
              rate sheet.
            </p>
          </div>

          <div className="flex items-start justify-between gap-4 rounded-md border p-3">
            <div className="flex flex-col gap-1">
              <Label htmlFor="brreg-source-capital-fx-force">
                Force reprocess
              </Label>
              <p className="text-xs leading-5 text-muted-foreground">
                Recalculate rows that already have USD cents stored.
              </p>
            </div>
            <Switch
              id="brreg-source-capital-fx-force"
              checked={forceReprocess}
              onCheckedChange={setForceReprocess}
            />
          </div>

          <div className="flex flex-col gap-2">
            <Label htmlFor="brreg-source-capital-fx-trigger">Trigger label</Label>
            <Input
              id="brreg-source-capital-fx-trigger"
              value={trigger}
              onChange={(event) => setTrigger(event.target.value)}
            />
            <p className="text-xs leading-5 text-muted-foreground">
              Metadata label recorded with the workflow request.
            </p>
          </div>
        </div>
      )}

      <div className="flex justify-end gap-2">
        <Button variant="outline" type="button" onClick={onClose}>
          Cancel
        </Button>
        <Button type="button" disabled={!canSubmit || submitting} onClick={submit}>
          <Play className="mr-2 size-4" />
          {submitting ? "Starting..." : "Start conversion"}
        </Button>
      </div>
    </div>
  );
}
