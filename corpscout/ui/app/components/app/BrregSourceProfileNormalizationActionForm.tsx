import { useEffect, useMemo, useState } from "react";
import { ChevronDown, Play } from "lucide-react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Separator } from "~/components/ui/separator";
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

type SourceSyncRecordMode = "limit" | "all";

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
    return `${selectedCount.toLocaleString()} selected records`;
  if (scope === "filtered")
    return `${totalCount.toLocaleString()} records matching the current filters`;
  return "Next raw records that can be synced to brreg_source";
}

export function BrregSourceProfileNormalizationActionForm({
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
  const [recordMode, setRecordMode] = useState<SourceSyncRecordMode>("limit");
  const [limit, setLimit] = useState("1000");
  const [trigger, setTrigger] = useState("manual");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    setScope(defaultScope);
    setRecordMode("limit");
    setLimit(selectedCount > 0 ? String(selectedCount) : "1000");
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
        label: "Next records",
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
        trigger?: string;
      } = {
        limit: effectiveLimit,
        trigger: trigger.trim() || "manual",
      };

      if (scope === "selected") body.ids = selectedIds;
      if (scope === "filtered") body.filters = filters;

      await api.syncBrregSourceProfiles(body);
      toast.success("BRREG source sync workflow started.");
      onStarted?.();
      onClose();
    } catch (error) {
      toast.error(
        errorMessage(error, "Failed to start BRREG source sync."),
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="rounded-md border bg-muted/20 p-3">
        <div className="text-sm font-medium">Sync to brreg_source</div>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Reads BRREG raw payloads and upserts company profile, address,
          industry, website, domain, contact, and capital rows into the
          normalized BRREG source tables.
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
          <Label htmlFor="brreg-source-profile-scope">Records source</Label>
          <select
            id="brreg-source-profile-scope"
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
            Choose checked rows, the currently filtered table, or the next raw
            records by BRREG organization number.
          </p>
        </div>

        <div className="flex flex-col gap-2">
          <Label htmlFor="brreg-source-profile-record-mode">Records to sync</Label>
          <select
            id="brreg-source-profile-record-mode"
            className="h-9 rounded-md border border-input bg-background px-3 text-sm"
            disabled={scope === "selected"}
            value={scope === "selected" ? "limit" : recordMode}
            onChange={(event) =>
              setRecordMode(event.target.value as SourceSyncRecordMode)
            }
          >
            <option value="limit">Limited number</option>
            <option value="all">All matching records</option>
          </select>
          <p className="text-xs leading-5 text-muted-foreground">
            Use a limited number while validating changes, or all matching
            records for full batch source sync.
          </p>
        </div>

        <div className="flex flex-col gap-2">
          <Label htmlFor="brreg-source-profile-limit">Records</Label>
          <Input
            id="brreg-source-profile-limit"
            type="number"
            min={1}
            disabled={limitDisabled}
            placeholder={recordMode === "all" ? "All matching records" : undefined}
            value={scope === "selected" ? selectedCount : recordMode === "all" ? "" : limit}
            onChange={(event) => setLimit(event.target.value)}
          />
          <p className="text-xs leading-5 text-muted-foreground">
            Maximum number of raw records to sync. All matching records sends
            limit 0 to the workflow.
          </p>
        </div>
      </div>

      <Separator />

      <button
        type="button"
        className="flex items-center gap-2 text-sm font-medium"
        onClick={() => setAdvancedOpen((open) => !open)}
      >
        <ChevronDown
          className={`h-4 w-4 transition-transform ${advancedOpen ? "rotate-180" : ""}`}
        />
        Advanced
      </button>

      {advancedOpen && (
        <div className="flex flex-col gap-2">
          <Label htmlFor="brreg-source-profile-trigger">Trigger label</Label>
          <Input
            id="brreg-source-profile-trigger"
            value={trigger}
            onChange={(event) => setTrigger(event.target.value)}
          />
          <p className="text-xs leading-5 text-muted-foreground">
            Metadata label recorded with the workflow request.
          </p>
        </div>
      )}

      <div className="flex justify-end gap-2">
        <Button variant="outline" type="button" onClick={onClose}>
          Cancel
        </Button>
        <Button type="button" disabled={!canSubmit || submitting} onClick={submit}>
          <Play className="mr-2 h-4 w-4" />
          {submitting ? "Starting..." : "Start workflow"}
        </Button>
      </div>
    </div>
  );
}
