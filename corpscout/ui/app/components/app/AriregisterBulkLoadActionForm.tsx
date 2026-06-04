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

export function AriregisterBulkLoadActionForm({ onStarted, onClose }: Props) {
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
      await api.loadAriregisterBulkRawRecords({
        limit: effectiveLimit,
        trigger: "manual",
      });
      toast.success(
        mode === "all"
          ? "Full Ariregister general data ingest workflow started."
          : "Ariregister general data ingest workflow started.",
      );
      onStarted?.();
      onClose();
    } catch (error) {
      toast.error(errorMessage(error, "Failed to start Ariregister bulk ingest."));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="rounded-md border bg-muted/20 p-3">
        <div className="text-sm font-medium">Load Ariregister bulk</div>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Load general-data JSON rows from the Estonian Business Register into
          ariregister_workflow.raw_records.
        </p>
      </div>

      <div className="flex flex-col gap-2">
        <Label htmlFor="ariregister-bulk-load-mode">Records to load</Label>
        <select
          id="ariregister-bulk-load-mode"
          className="h-9 rounded-md border border-input bg-background px-3 text-sm"
          value={mode}
          onChange={(event) => setMode(event.target.value as BulkLoadMode)}
        >
          <option value="limit">Limited number</option>
          <option value="all">All records</option>
        </select>
        <p className="text-xs leading-5 text-muted-foreground">
          Use a limited number such as 1000 while validating the pipeline; use
          all to ingest the complete Ariregister general-data JSON file.
        </p>
      </div>

      {mode === "limit" && (
        <div className="flex flex-col gap-2">
          <Label htmlFor="ariregister-bulk-load-limit">Limit</Label>
          <Input
            id="ariregister-bulk-load-limit"
            type="number"
            min={1}
            value={limit}
            onChange={(event) => setLimit(event.target.value)}
          />
          <p className="text-xs leading-5 text-muted-foreground">
            Maximum number of rows to load from the Ariregister general-data file.
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
          {submitting ? "Starting..." : "Start general ingest"}
        </Button>
      </div>
    </div>
  );
}
