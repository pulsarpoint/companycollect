import { useEffect, useState } from "react";
import { DatabaseZap } from "lucide-react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { api, errorMessage } from "~/lib/api";

type BulkLoadMode = "all" | "limit";

interface Props {
  onStarted?: () => void;
  onClose: () => void;
}

function parsePositiveNumber(value: string) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.floor(parsed);
}

export function FranceBulkLoadActionForm({ onStarted, onClose }: Props) {
  const [mode, setMode] = useState<BulkLoadMode>("limit");
  const [limit, setLimit] = useState("1000");
  const [batchSize, setBatchSize] = useState("1000");
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (mode === "limit" && limit.trim() === "") setLimit("1000");
  }, [limit, mode]);

  const effectiveLimit = mode === "all" ? 0 : parsePositiveNumber(limit);
  const parsedBatchSize = parsePositiveNumber(batchSize);
  const limitInvalid = mode === "limit" && effectiveLimit === null;
  const batchSizeInvalid = parsedBatchSize === null;
  const canSubmit = effectiveLimit !== null && parsedBatchSize !== null;

  async function submit() {
    if (!canSubmit || effectiveLimit === null || parsedBatchSize === null) return;

    setSubmitting(true);
    try {
      await api.loadFranceBulkRawRecords({
        limit: effectiveLimit,
        batch_size: parsedBatchSize,
        trigger: "manual",
      });
      toast.success(
        mode === "all"
          ? "Full France SIRENE bulk ingest workflow started."
          : "France SIRENE sample ingest workflow started.",
      );
      onStarted?.();
      onClose();
    } catch (error) {
      toast.error(errorMessage(error, "Failed to start France SIRENE bulk ingest."));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="rounded-md border bg-muted/20 p-3">
        <div className="text-sm font-medium">Load France SIRENE bulk</div>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Load legal-unit and establishment parquet rows into france_workflow raw tables.
        </p>
      </div>

      <div className="flex flex-col gap-2">
        <Label htmlFor="france-bulk-load-mode">Records to load</Label>
        <select
          id="france-bulk-load-mode"
          className="h-9 rounded-md border border-input bg-background px-3 text-sm"
          value={mode}
          onChange={(event) => setMode(event.target.value as BulkLoadMode)}
        >
          <option value="limit">Limited sample</option>
          <option value="all">All records</option>
        </select>
        <p className="text-xs leading-5 text-muted-foreground">
          Full ingest downloads both official SIRENE stock files. Use a limited sample when validating changes.
        </p>
      </div>

      {mode === "limit" && (
        <div className="flex flex-col gap-2">
          <Label htmlFor="france-bulk-load-limit">Limit per file</Label>
          <Input
            id="france-bulk-load-limit"
            type="number"
            min={1}
            value={limit}
            onChange={(event) => setLimit(event.target.value)}
          />
          {limitInvalid && (
            <p className="text-xs leading-5 text-destructive">
              Limit must be a positive number.
            </p>
          )}
        </div>
      )}

      <div className="flex flex-col gap-2">
        <Label htmlFor="france-bulk-load-batch-size">Database batch size</Label>
        <Input
          id="france-bulk-load-batch-size"
          type="number"
          min={1}
          value={batchSize}
          onChange={(event) => setBatchSize(event.target.value)}
        />
        {batchSizeInvalid && (
          <p className="text-xs leading-5 text-destructive">
            Batch size must be a positive number.
          </p>
        )}
      </div>

      <div className="flex justify-end gap-2">
        <Button variant="outline" type="button" onClick={onClose}>
          Cancel
        </Button>
        <Button type="button" disabled={!canSubmit || submitting} onClick={submit}>
          <DatabaseZap className="mr-2 h-4 w-4" />
          {submitting ? "Starting..." : "Start SIRENE ingest"}
        </Button>
      </div>
    </div>
  );
}
