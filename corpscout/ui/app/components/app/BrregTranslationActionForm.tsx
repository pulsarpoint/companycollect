import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { ChevronDown, Play } from "lucide-react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Separator } from "~/components/ui/separator";
import { api, errorMessage } from "~/lib/api";

export type BrregActionScope = "selected" | "filtered" | "eligible";

interface Props {
  selectedIds: string[];
  totalCount: number;
  filters: Record<string, string>;
  initialScope?: BrregActionScope;
  onStarted?: () => void;
  onClose: () => void;
}

function scopeLabel(scope: BrregActionScope, selectedCount: number, totalCount: number) {
  if (scope === "selected") return `${selectedCount.toLocaleString()} selected records`;
  if (scope === "filtered") return `${totalCount.toLocaleString()} records matching the current filters`;
  return "Next eligible records";
}

function parseRequiredPositiveNumber(value: string) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.floor(parsed);
}

function parseOptionalPositiveNumber(value: string) {
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  return parseRequiredPositiveNumber(trimmed) ?? undefined;
}

function optionalText(value: string) {
  const trimmed = value.trim();
  return trimmed || undefined;
}

function FieldDescription({ children }: { children: ReactNode }) {
  return <p className="text-xs leading-5 text-muted-foreground">{children}</p>;
}

export function BrregTranslationActionForm({
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
    initialScope ?? (selectedCount > 0 ? "selected" : hasFilters ? "filtered" : "eligible");
  const [scope, setScope] = useState<BrregActionScope>(defaultScope);
  const [limit, setLimit] = useState("1000");
  const [batchSize, setBatchSize] = useState("50");
  const [leaseSeconds, setLeaseSeconds] = useState("900");
  const [maxAttempts, setMaxAttempts] = useState("3");
  const [maxParallelTasks, setMaxParallelTasks] = useState("50");
  const [maxServiceRetries, setMaxServiceRetries] = useState("2");
  const [provider, setProvider] = useState("");
  const [model, setModel] = useState("");
  const [promptVersion, setPromptVersion] = useState("");
  const [sourceLang, setSourceLang] = useState("");
  const [targetLang, setTargetLang] = useState("");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    setScope(defaultScope);
    setLimit(selectedCount > 0 ? String(selectedCount) : "1000");
    setBatchSize("50");
    setLeaseSeconds("900");
    setMaxAttempts("3");
    setMaxParallelTasks("50");
    setMaxServiceRetries("2");
    setProvider("");
    setModel("");
    setPromptVersion("");
    setSourceLang("");
    setTargetLang("");
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
      { value: "eligible" as const, label: "Next eligible records", disabled: false },
    ],
    [hasFilters, selectedCount, totalCount],
  );

  const limitDisabled = scope === "selected";
  const effectiveLimit = limitDisabled ? selectedCount : parseRequiredPositiveNumber(limit);
  const canSubmit = effectiveLimit !== null && (scope !== "selected" || selectedCount > 0);

  async function submit() {
    if (!canSubmit || effectiveLimit === null) return;

    setSubmitting(true);
    try {
      const body: {
        ids?: string[];
        filters?: Record<string, string>;
        limit?: number;
        batch_size?: number;
        max_attempts?: number;
        max_parallel_tasks?: number;
        lease_seconds?: number;
        provider?: string;
        model?: string;
        prompt_version?: string;
        source_lang?: string;
        target_lang?: string;
        max_service_retries?: number;
      } = {
        limit: effectiveLimit,
        batch_size: parseOptionalPositiveNumber(batchSize),
        max_attempts: parseOptionalPositiveNumber(maxAttempts),
        max_parallel_tasks: parseOptionalPositiveNumber(maxParallelTasks),
        lease_seconds: parseOptionalPositiveNumber(leaseSeconds),
        provider: optionalText(provider),
        model: optionalText(model),
        prompt_version: optionalText(promptVersion),
        source_lang: optionalText(sourceLang),
        target_lang: optionalText(targetLang),
        max_service_retries: parseOptionalPositiveNumber(maxServiceRetries),
      };

      if (scope === "selected") body.ids = selectedIds;
      if (scope === "filtered") body.filters = filters;

      await api.translateBrreg(body);
      toast.success("BRREG translation workflow started.");
      onStarted?.();
      onClose();
    } catch (error) {
      toast.error(errorMessage(error, "Failed to start BRREG translation."));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="rounded-md border bg-muted/20 p-3">
        <div className="text-sm font-medium">Translation</div>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Starts the Temporal workflow that claims BRREG raw records, sends payloads to the translation service,
          and writes translation artifacts back to Corpscout.
        </p>
      </div>

      <div className="flex flex-col gap-4">
        <div>
          <h3 className="text-sm font-medium">Required</h3>
          <p className="mt-1 text-xs text-muted-foreground">{scopeLabel(scope, selectedCount, totalCount)}</p>
        </div>

        <div className="flex flex-col gap-2">
          <Label htmlFor="brreg-translation-scope">Records source</Label>
          <select
            id="brreg-translation-scope"
            className="h-9 rounded-md border border-input bg-background px-3 text-sm"
            value={scope}
            onChange={(event) => setScope(event.target.value as BrregActionScope)}
          >
            {scopeOptions.map((option) => (
              <option key={option.value} value={option.value} disabled={option.disabled}>
                {option.label}
              </option>
            ))}
          </select>
          <FieldDescription>
            Choose whether this run uses checked rows, the current table filters, or the next eligible untranslated
            records.
          </FieldDescription>
        </div>

        <div className="flex flex-col gap-2">
          <Label htmlFor="brreg-translation-limit">Records</Label>
          <Input
            id="brreg-translation-limit"
            min={1}
            type="number"
            value={limitDisabled ? String(selectedCount) : limit}
            disabled={limitDisabled}
            onChange={(event) => setLimit(event.target.value)}
          />
          <FieldDescription>
            Maximum number of raw records this workflow can select. For checked rows, this is fixed to the selected
            count.
          </FieldDescription>
        </div>
      </div>

      <Separator />

      <div className="flex flex-col gap-4">
        <Button
          type="button"
          variant="ghost"
          className="h-8 justify-between px-0 text-sm font-medium hover:bg-transparent"
          onClick={() => setAdvancedOpen((current) => !current)}
          aria-expanded={advancedOpen}
        >
          Advanced options
          <ChevronDown className={`size-4 transition-transform ${advancedOpen ? "rotate-180" : ""}`} />
        </Button>

        {advancedOpen && (
          <div className="flex flex-col gap-4">
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-translation-batch-size">Batch size</Label>
                <Input
                  id="brreg-translation-batch-size"
                  min={1}
                  type="number"
                  value={batchSize}
                  onChange={(event) => setBatchSize(event.target.value)}
                />
                <FieldDescription>Number of records sent to the translation service in one workflow batch.</FieldDescription>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-translation-timeout">Timeout seconds</Label>
                <Input
                  id="brreg-translation-timeout"
                  min={1}
                  type="number"
                  value={leaseSeconds}
                  onChange={(event) => setLeaseSeconds(event.target.value)}
                />
                <FieldDescription>
                  Lease and activity timeout budget for a claimed translation batch before it can be retried.
                </FieldDescription>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-translation-max-attempts">Retry attempts</Label>
                <Input
                  id="brreg-translation-max-attempts"
                  min={1}
                  type="number"
                  value={maxAttempts}
                  onChange={(event) => setMaxAttempts(event.target.value)}
                />
                <FieldDescription>Maximum DB task attempts for a record before it becomes terminally failed.</FieldDescription>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-translation-service-retries">Service retries</Label>
                <Input
                  id="brreg-translation-service-retries"
                  min={1}
                  type="number"
                  value={maxServiceRetries}
                  onChange={(event) => setMaxServiceRetries(event.target.value)}
                />
                <FieldDescription>Retries inside one batch call when the translation service returns retryable errors.</FieldDescription>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-translation-parallel">Parallel batches</Label>
                <Input
                  id="brreg-translation-parallel"
                  min={1}
                  type="number"
                  value={maxParallelTasks}
                  onChange={(event) => setMaxParallelTasks(event.target.value)}
                />
                <FieldDescription>Maximum translation batches the workflow can keep active at the same time.</FieldDescription>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-translation-prompt">Prompt version</Label>
                <Input
                  id="brreg-translation-prompt"
                  value={promptVersion}
                  placeholder="Server default"
                  onChange={(event) => setPromptVersion(event.target.value)}
                />
                <FieldDescription>Prompt contract version passed to the translation service.</FieldDescription>
              </div>
            </div>

            <Separator />

            <div className="grid gap-4 sm:grid-cols-2">
              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-translation-provider">LLM provider</Label>
                <Input
                  id="brreg-translation-provider"
                  value={provider}
                  placeholder="Server default"
                  onChange={(event) => setProvider(event.target.value)}
                />
                <FieldDescription>Optional provider override, for example local or deepseek.</FieldDescription>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-translation-model">LLM model</Label>
                <Input
                  id="brreg-translation-model"
                  value={model}
                  placeholder="Server default"
                  onChange={(event) => setModel(event.target.value)}
                />
                <FieldDescription>Optional model override sent to the translation service.</FieldDescription>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-translation-source-lang">Source language</Label>
                <Input
                  id="brreg-translation-source-lang"
                  value={sourceLang}
                  placeholder="Server default"
                  onChange={(event) => setSourceLang(event.target.value)}
                />
                <FieldDescription>Language code or name for the original BRREG payload text.</FieldDescription>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-translation-target-lang">Target language</Label>
                <Input
                  id="brreg-translation-target-lang"
                  value={targetLang}
                  placeholder="Server default"
                  onChange={(event) => setTargetLang(event.target.value)}
                />
                <FieldDescription>Language code or name for translated output.</FieldDescription>
              </div>
            </div>
          </div>
        )}
      </div>

      <div className="flex justify-end">
        <Button disabled={!canSubmit || submitting} onClick={submit}>
          <Play className="size-4" />
          {submitting ? "Starting..." : "Start translation"}
        </Button>
      </div>
    </div>
  );
}
