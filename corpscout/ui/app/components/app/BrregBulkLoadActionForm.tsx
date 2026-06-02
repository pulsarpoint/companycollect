import { useEffect, useState } from "react";
import { DatabaseZap } from "lucide-react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { api, errorMessage } from "~/lib/api";

type BulkLoadMode = "limit" | "all";

interface Props {
  onStarted?: () => void;
  onClose: () => void;
}

function parsePositiveNumber(value: string) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.floor(parsed);
}

export function BrregBulkLoadActionForm({ onStarted, onClose }: Props) {
  const [mode, setMode] = useState<BulkLoadMode>("limit");
  const [limit, setLimit] = useState("1000");
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (mode === "limit" && limit.trim() === "") setLimit("1000");
  }, [limit, mode]);

  const limitInvalid = mode === "limit" && parsePositiveNumber(limit) === null;
  const effectiveLimit = mode === "all" ? 0 : parsePositiveNumber(limit);
  const canSubmit = effectiveLimit !== null;

  async function submit() {
    if (!canSubmit || effectiveLimit === null) return;

    setSubmitting(true);
    try {
      await api.loadBrregBulkRawRecords({
        limit: effectiveLimit,
        trigger: "manual",
      });
      toast.success(
        mode === "all"
          ? "Full BRREG bulk ingest workflow started."
          : "BRREG bulk ingest workflow started.",
      );
      onStarted?.();
      onClose();
    } catch (error) {
      toast.error(errorMessage(error, "Failed to start BRREG bulk ingest."));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="rounded-md border bg-muted/20 p-3">
        <div className="text-sm font-medium">Load BRREG bulk</div>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Load raw BRREG rows from the bulk source into
          brreg_workflow.raw_records.
        </p>
      </div>

      <div className="flex flex-col gap-2">
        <Label htmlFor="brreg-bulk-load-mode">Records to load</Label>
        <select
          id="brreg-bulk-load-mode"
          className="h-9 rounded-md border border-input bg-background px-3 text-sm"
          value={mode}
          onChange={(event) => setMode(event.target.value as BulkLoadMode)}
        >
          <option value="limit">Limited number</option>
          <option value="all">All records</option>
        </select>
        <p className="text-xs leading-5 text-muted-foreground">
          Use a limited number such as 1000 while validating the pipeline; use
          all to ingest the complete BRREG bulk export.
        </p>
      </div>

      {mode === "limit" && (
        <div className="flex flex-col gap-2">
          <Label htmlFor="brreg-bulk-load-limit">Limit</Label>
          <Input
            id="brreg-bulk-load-limit"
            type="number"
            min={1}
            value={limit}
            onChange={(event) => setLimit(event.target.value)}
          />
          <p className="text-xs leading-5 text-muted-foreground">
            Maximum number of rows to load from the BRREG bulk file.
          </p>
          {limitInvalid && (
            <p className="text-xs leading-5 text-destructive">
              Limit must be a positive number.
            </p>
          )}
        </div>
      )}

      <div className="flex justify-end gap-2">
        <Button variant="outline" type="button" onClick={onClose}>
          Cancel
        </Button>
        <Button type="button" disabled={!canSubmit || submitting} onClick={submit}>
          <DatabaseZap className="mr-2 h-4 w-4" />
          {submitting ? "Starting..." : "Start bulk ingest"}
        </Button>
      </div>
    </div>
  );
}
