import { useEffect, useMemo, useState } from "react";
import { Play } from "lucide-react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import { api, errorMessage } from "~/lib/api";

type ActionScope = "selected" | "filtered" | "eligible";

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  selectedIds: string[];
  totalCount: number;
  filters: Record<string, string>;
  initialScope?: ActionScope;
  onStarted?: () => void;
}

function scopeLabel(scope: ActionScope, selectedCount: number, totalCount: number) {
  if (scope === "selected") return `${selectedCount.toLocaleString()} selected`;
  if (scope === "filtered") return `${totalCount.toLocaleString()} matching filters`;
  return "Next eligible records";
}

function positiveNumber(value: string, fallback: number) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.floor(parsed);
}

export function BrregRawRecordActionSheet({
  open,
  onOpenChange,
  selectedIds,
  totalCount,
  filters,
  initialScope,
  onStarted,
}: Props) {
  const selectedCount = selectedIds.length;
  const hasFilters = Object.keys(filters).length > 0;
  const defaultScope: ActionScope = initialScope ?? (selectedCount > 0 ? "selected" : hasFilters ? "filtered" : "eligible");
  const [scope, setScope] = useState<ActionScope>(defaultScope);
  const [limit, setLimit] = useState("1000");
  const [batchSize, setBatchSize] = useState("50");
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (!open) return;
    setScope(defaultScope);
    setLimit(selectedCount > 0 ? String(selectedCount) : "1000");
    setBatchSize("50");
  }, [defaultScope, open, selectedCount]);

  const scopeOptions = useMemo(
    () => [
      { value: "selected" as const, label: `${selectedCount.toLocaleString()} selected`, disabled: selectedCount === 0 },
      { value: "filtered" as const, label: `${totalCount.toLocaleString()} matching filters`, disabled: !hasFilters },
      { value: "eligible" as const, label: "Next eligible records", disabled: false },
    ],
    [hasFilters, selectedCount, totalCount],
  );

  async function submit() {
    setSubmitting(true);
    try {
      const body: {
        ids?: string[];
        filters?: Record<string, string>;
        limit?: number;
        batch_size?: number;
      } = {};
      body.batch_size = positiveNumber(batchSize, 50);
      if (scope === "selected") {
        body.ids = selectedIds;
        body.limit = selectedIds.length;
      } else {
        body.limit = positiveNumber(limit, 1000);
        if (scope === "filtered") body.filters = filters;
      }
      await api.translateBrreg(body);
      toast.success("BRREG translation workflow started.");
      onStarted?.();
      onOpenChange(false);
    } catch (error) {
      toast.error(errorMessage(error, "Failed to start BRREG translation."));
    } finally {
      setSubmitting(false);
    }
  }

  const limitDisabled = scope === "selected";
  const canSubmit = scope !== "selected" || selectedCount > 0;

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="sm:max-w-md">
        <SheetHeader>
          <SheetTitle>BRREG action</SheetTitle>
          <SheetDescription>{scopeLabel(scope, selectedCount, totalCount)}</SheetDescription>
        </SheetHeader>

        <div className="flex flex-col gap-5 px-4">
          <div className="flex flex-col gap-2">
            <Label htmlFor="brreg-action-scope">Scope</Label>
            <select
              id="brreg-action-scope"
              className="h-9 rounded-md border border-input bg-background px-3 text-sm"
              value={scope}
              onChange={(event) => setScope(event.target.value as ActionScope)}
            >
              {scopeOptions.map((option) => (
                <option key={option.value} value={option.value} disabled={option.disabled}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-2">
              <Label htmlFor="brreg-action-limit">Records</Label>
              <Input
                id="brreg-action-limit"
                min={1}
                type="number"
                value={limitDisabled ? String(selectedCount) : limit}
                disabled={limitDisabled}
                onChange={(event) => setLimit(event.target.value)}
              />
            </div>
            <div className="flex flex-col gap-2">
              <Label htmlFor="brreg-action-batch-size">Batch size</Label>
              <Input
                id="brreg-action-batch-size"
                min={1}
                type="number"
                value={batchSize}
                onChange={(event) => setBatchSize(event.target.value)}
              />
            </div>
          </div>
        </div>

        <SheetFooter>
          <Button disabled={!canSubmit || submitting} onClick={submit}>
            <Play className="size-4" />
            {submitting ? "Starting..." : "Start translation"}
          </Button>
        </SheetFooter>
      </SheetContent>
    </Sheet>
  );
}
