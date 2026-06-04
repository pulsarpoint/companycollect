import { useEffect, useState } from "react";
import { ChevronDown, Play } from "lucide-react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Separator } from "~/components/ui/separator";
import { api, errorMessage } from "~/lib/api";

interface Props {
  onStarted?: () => void;
  onClose: () => void;
}

type FinancialRecordMode = "all" | "limit";

function parseRequiredPositiveNumber(value: string) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.floor(parsed);
}

export function BrregSourceFinancialActionForm({ onStarted, onClose }: Props) {
  const [recordMode, setRecordMode] = useState<FinancialRecordMode>("all");
  const [limit, setLimit] = useState("1000");
  const [batchSize, setBatchSize] = useState("10");
  const [maxParallelTasks, setMaxParallelTasks] = useState("25");
  const [leaseSeconds, setLeaseSeconds] = useState("900");
  const [maxAttempts, setMaxAttempts] = useState("3");
  const [trigger, setTrigger] = useState("manual");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    setRecordMode("all");
    setLimit("1000");
    setBatchSize("10");
    setMaxParallelTasks("25");
    setLeaseSeconds("900");
    setMaxAttempts("3");
    setTrigger("manual");
    setAdvancedOpen(false);
  }, []);

  const effectiveLimit =
    recordMode === "all" ? 0 : parseRequiredPositiveNumber(limit);
  const parsedBatchSize = parseRequiredPositiveNumber(batchSize);
  const parsedMaxParallelTasks = parseRequiredPositiveNumber(maxParallelTasks);
  const parsedLeaseSeconds = parseRequiredPositiveNumber(leaseSeconds);
  const parsedMaxAttempts = parseRequiredPositiveNumber(maxAttempts);
  const canSubmit =
    effectiveLimit !== null &&
    parsedBatchSize !== null &&
    parsedMaxParallelTasks !== null &&
    parsedLeaseSeconds !== null &&
    parsedMaxAttempts !== null;

  async function submit() {
    if (
      !canSubmit ||
      effectiveLimit === null ||
      parsedBatchSize === null ||
      parsedMaxParallelTasks === null ||
      parsedLeaseSeconds === null ||
      parsedMaxAttempts === null
    ) {
      return;
    }
    setSubmitting(true);
    try {
      await api.fetchBrregSourceFinancialStatements({
        limit: effectiveLimit,
        batch_size: parsedBatchSize,
        max_parallel_tasks: parsedMaxParallelTasks,
        lease_seconds: parsedLeaseSeconds,
        max_attempts: parsedMaxAttempts,
        trigger: trigger.trim() || "manual",
      });
      toast.success("BRREG financial records workflow started.");
      onStarted?.();
      onClose();
    } catch (error) {
      toast.error(
        errorMessage(error, "Failed to start BRREG financial records workflow."),
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="rounded-md border bg-muted/20 p-3">
        <div className="text-sm font-medium">Fetch financial records</div>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Claims BRREG source companies without financial statements, fetches
          official annual-account key figures from BRREG, and stores them in
          brreg_source.financial_statements.
        </p>
      </div>

      <div className="flex flex-col gap-4">
        <div>
          <h3 className="text-sm font-medium">Required</h3>
          <p className="mt-1 text-xs text-muted-foreground">
            Companies that already have financial statement rows are skipped by
            the database claim query.
          </p>
        </div>

        <div className="flex flex-col gap-2">
          <Label htmlFor="brreg-source-financial-record-mode">
            Records to fetch
          </Label>
          <select
            id="brreg-source-financial-record-mode"
            className="h-9 rounded-md border border-input bg-background px-3 text-sm"
            value={recordMode}
            onChange={(event) =>
              setRecordMode(event.target.value as FinancialRecordMode)
            }
          >
            <option value="all">All eligible companies</option>
            <option value="limit">Limited number</option>
          </select>
          <p className="text-xs leading-5 text-muted-foreground">
            All eligible sends limit 0. Limited runs are useful for checking the
            official BRREG financial endpoint on a small sample.
          </p>
        </div>

        <div className="flex flex-col gap-2">
          <Label htmlFor="brreg-source-financial-limit">Records</Label>
          <Input
            id="brreg-source-financial-limit"
            type="number"
            min={1}
            disabled={recordMode === "all"}
            placeholder={recordMode === "all" ? "All eligible companies" : undefined}
            value={recordMode === "all" ? "" : limit}
            onChange={(event) => setLimit(event.target.value)}
          />
          <p className="text-xs leading-5 text-muted-foreground">
            When limited, this is the maximum number of companies claimed by the
            workflow.
          </p>
        </div>
      </div>

      <Separator />

      <button
        type="button"
        className="flex items-center justify-between text-sm font-medium"
        onClick={() => setAdvancedOpen((open) => !open)}
      >
        Advanced
        <ChevronDown
          className={`size-4 transition-transform ${advancedOpen ? "rotate-180" : ""}`}
        />
      </button>

      {advancedOpen && (
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="flex flex-col gap-2">
            <Label htmlFor="brreg-source-financial-batch-size">
              Batch size
            </Label>
            <Input
              id="brreg-source-financial-batch-size"
              type="number"
              min={1}
              value={batchSize}
              onChange={(event) => setBatchSize(event.target.value)}
            />
            <p className="text-xs leading-5 text-muted-foreground">
              Number of companies claimed from Postgres before BRREG requests
              are executed.
            </p>
          </div>

          <div className="flex flex-col gap-2">
            <Label htmlFor="brreg-source-financial-max-parallel">
              Max active leases
            </Label>
            <Input
              id="brreg-source-financial-max-parallel"
              type="number"
              min={1}
              value={maxParallelTasks}
              onChange={(event) => setMaxParallelTasks(event.target.value)}
            />
            <p className="text-xs leading-5 text-muted-foreground">
              Global DB cap for active financial claims.
            </p>
          </div>

          <div className="flex flex-col gap-2">
            <Label htmlFor="brreg-source-financial-lease">
              Lease seconds
            </Label>
            <Input
              id="brreg-source-financial-lease"
              type="number"
              min={1}
              value={leaseSeconds}
              onChange={(event) => setLeaseSeconds(event.target.value)}
            />
            <p className="text-xs leading-5 text-muted-foreground">
              Claimed companies become eligible again after this lease expires
              if the activity crashes.
            </p>
          </div>

          <div className="flex flex-col gap-2">
            <Label htmlFor="brreg-source-financial-attempts">
              Max attempts
            </Label>
            <Input
              id="brreg-source-financial-attempts"
              type="number"
              min={1}
              value={maxAttempts}
              onChange={(event) => setMaxAttempts(event.target.value)}
            />
            <p className="text-xs leading-5 text-muted-foreground">
              Retryable lookup failures become terminal after this many
              attempts.
            </p>
          </div>

          <div className="flex flex-col gap-2 sm:col-span-2">
            <Label htmlFor="brreg-source-financial-trigger">
              Trigger label
            </Label>
            <Input
              id="brreg-source-financial-trigger"
              value={trigger}
              onChange={(event) => setTrigger(event.target.value)}
            />
            <p className="text-xs leading-5 text-muted-foreground">
              Stored with process metadata and Temporal workflow input.
            </p>
          </div>
        </div>
      )}

      <Button type="button" onClick={submit} disabled={!canSubmit || submitting}>
        <Play className="size-4" />
        {submitting ? "Starting..." : "Start financial fetch"}
      </Button>
    </div>
  );
}
