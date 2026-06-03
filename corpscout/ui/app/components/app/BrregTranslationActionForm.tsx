import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { ChevronDown, Play } from "lucide-react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Separator } from "~/components/ui/separator";
import { api, errorMessage } from "~/lib/api";
import type {
  BrregTermTranslationRequest,
  BrregTranslationRequest,
  LLMProvider,
} from "~/types/api";
import type { BrregActionScope } from "~/components/app/BrregActionScope";

type BrregTranslationMode = "raw" | "source_terms";
type TermTranslationScope = "eligible" | "all";

interface Props {
  selectedIds: string[];
  totalCount: number;
  filters: Record<string, string>;
  mode?: BrregTranslationMode;
  initialScope?: BrregActionScope;
  recordLabel?: string;
  description?: string;
  showAdvancedOptions?: boolean;
  onStarted?: () => void;
  onClose: () => void;
}

function scopeLabel(
  scope: BrregActionScope,
  selectedCount: number,
  totalCount: number,
  recordLabel: string,
) {
  if (scope === "selected")
    return `${selectedCount.toLocaleString()} selected ${recordLabel}`;
  if (scope === "filtered")
    return `${totalCount.toLocaleString()} ${recordLabel} matching the current filters`;
  return `Next eligible ${recordLabel}`;
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
  mode = "raw",
  initialScope,
  recordLabel = "raw records",
  description = "Starts the Temporal workflow that claims BRREG raw records, sends payloads to the translation service, and writes translation artifacts back to Corpscout.",
  showAdvancedOptions = true,
  onStarted,
  onClose,
}: Props) {
  const usesScopedRecords = mode === "raw";
  const selectedCount = selectedIds.length;
  const hasFilters = Object.keys(filters).length > 0;
  const defaultScope: BrregActionScope =
    initialScope ??
    (selectedCount > 0 ? "selected" : hasFilters ? "filtered" : "eligible");
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
  const [termScope, setTermScope] = useState<TermTranslationScope>("eligible");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [llmProviders, setLLMProviders] = useState<LLMProvider[]>([]);
  const [llmProvidersLoading, setLLMProvidersLoading] = useState(false);
  const [llmProvidersError, setLLMProvidersError] = useState("");

  useEffect(() => {
    setScope(defaultScope);
    setLimit(
      usesScopedRecords && selectedCount > 0 ? String(selectedCount) : "1000",
    );
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
    setTermScope("eligible");
    setAdvancedOpen(false);
  }, [defaultScope, selectedCount, usesScopedRecords]);

  useEffect(() => {
    if (!showAdvancedOptions || !advancedOpen) return;

    let cancelled = false;
    setLLMProvidersLoading(true);
    setLLMProvidersError("");
    api
      .getLLMProviders()
      .then((response) => {
        if (cancelled) return;
        setLLMProviders(response.providers);
      })
      .catch((error) => {
        if (cancelled) return;
        setLLMProvidersError(
          errorMessage(error, "Failed to load LLM providers."),
        );
      })
      .finally(() => {
        if (!cancelled) setLLMProvidersLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [advancedOpen, showAdvancedOptions]);

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
        label: "Next eligible records",
        disabled: false,
      },
    ],
    [hasFilters, selectedCount, totalCount],
  );

  const allSourceTermsSelected = mode === "source_terms" && termScope === "all";
  const limitDisabled =
    (usesScopedRecords && scope === "selected") || allSourceTermsSelected;
  const effectiveLimit = limitDisabled
    ? usesScopedRecords
      ? selectedCount
      : 0
    : parseRequiredPositiveNumber(limit);
  const canSubmit =
    effectiveLimit !== null &&
    (!usesScopedRecords || scope !== "selected" || selectedCount > 0);
  const availableLLMProviders = useMemo(
    () => llmProviders.filter((llmProvider) => llmProvider.enabled),
    [llmProviders],
  );
  const selectedLLMProvider = useMemo(
    () =>
      availableLLMProviders.find(
        (llmProvider) => llmProvider.slug === provider,
      ),
    [availableLLMProviders, provider],
  );

  function changeProvider(nextProvider: string) {
    setProvider(nextProvider);
    const selected = availableLLMProviders.find(
      (llmProvider) => llmProvider.slug === nextProvider,
    );
    setModel(selected?.model ?? "");
  }

  async function submit() {
    if (!canSubmit || effectiveLimit === null) return;

    setSubmitting(true);
    try {
      if (mode === "source_terms") {
        const body: BrregTermTranslationRequest = {
          all_records: allSourceTermsSelected,
          limit: allSourceTermsSelected ? undefined : effectiveLimit,
          term_batch_size: parseOptionalPositiveNumber(batchSize),
          max_attempts: parseOptionalPositiveNumber(maxAttempts),
          provider: optionalText(provider),
          model: optionalText(model),
          prompt_version: optionalText(promptVersion),
          trigger: "manual",
        };
        await api.translateBrregTerms(body);
        toast.success("BRREG term translation workflow started.");
        onStarted?.();
        onClose();
        return;
      }

      const body: BrregTranslationRequest = { limit: effectiveLimit };

      if (showAdvancedOptions) {
        Object.assign(body, {
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
        });
      }

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
          {description}
        </p>
      </div>

      <div className="flex flex-col gap-4">
        {usesScopedRecords && (
          <>
            <div>
              <h3 className="text-sm font-medium">Required</h3>
              <p className="mt-1 text-xs text-muted-foreground">
                {scopeLabel(scope, selectedCount, totalCount, recordLabel)}
              </p>
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="brreg-translation-scope">Records source</Label>
              <select
                id="brreg-translation-scope"
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
              <FieldDescription>
                Choose whether this run uses checked rows, the current table
                filters, or the next eligible untranslated records.
              </FieldDescription>
            </div>
          </>
        )}

        {!usesScopedRecords && (
          <div className="flex flex-col gap-2">
            <Label htmlFor="brreg-term-translation-scope">Records source</Label>
            <select
              id="brreg-term-translation-scope"
              className="h-9 rounded-md border border-input bg-background px-3 text-sm"
              value={termScope}
              onChange={(event) =>
                setTermScope(event.target.value as TermTranslationScope)
              }
            >
              <option value="eligible">Next eligible records</option>
              <option value="all">All records</option>
            </select>
            <FieldDescription>
              Choose whether this run prepares a limited window of missing
              source terms or scans every currently missing BRREG source term.
            </FieldDescription>
          </div>
        )}

        <div className="flex flex-col gap-2">
          <Label htmlFor="brreg-translation-limit">Records</Label>
          <Input
            id="brreg-translation-limit"
            min={allSourceTermsSelected ? undefined : 1}
            type={allSourceTermsSelected ? "text" : "number"}
            value={
              allSourceTermsSelected
                ? "All"
                : limitDisabled
                  ? String(selectedCount)
                  : limit
            }
            disabled={limitDisabled}
            onChange={(event) => setLimit(event.target.value)}
          />
          <FieldDescription>
            {allSourceTermsSelected
              ? "No record limit is sent. The workflow scans all currently missing BRREG source terms."
              : usesScopedRecords
                ? `Maximum number of ${recordLabel} this workflow can select. For checked rows, this is fixed to the selected count.`
                : "Maximum number of missing source terms the workflow can prepare and apply per loop."}
          </FieldDescription>
        </div>
      </div>

      {showAdvancedOptions && <Separator />}

      {showAdvancedOptions && (
        <div className="flex flex-col gap-4">
          <Button
            type="button"
            variant="ghost"
            className="h-8 justify-between px-0 text-sm font-medium hover:bg-transparent"
            onClick={() => setAdvancedOpen((current) => !current)}
            aria-expanded={advancedOpen}
          >
            Advanced options
            <ChevronDown
              className={`size-4 transition-transform ${advancedOpen ? "rotate-180" : ""}`}
            />
          </Button>

          {advancedOpen && (
            <div className="flex flex-col gap-4">
              <div className="grid gap-4 sm:grid-cols-2">
                <div className="flex flex-col gap-2">
                  <Label htmlFor="brreg-translation-batch-size">
                    Batch size
                  </Label>
                  <Input
                    id="brreg-translation-batch-size"
                    min={1}
                    type="number"
                    value={batchSize}
                    onChange={(event) => setBatchSize(event.target.value)}
                  />
                  <FieldDescription>
                    Number of records sent to the translation service in one
                    workflow batch.
                  </FieldDescription>
                </div>

                {usesScopedRecords && (
                  <div className="flex flex-col gap-2">
                    <Label htmlFor="brreg-translation-timeout">
                      Timeout seconds
                    </Label>
                    <Input
                      id="brreg-translation-timeout"
                      min={1}
                      type="number"
                      value={leaseSeconds}
                      onChange={(event) => setLeaseSeconds(event.target.value)}
                    />
                    <FieldDescription>
                      Lease and activity timeout budget for a claimed
                      translation batch before it can be retried.
                    </FieldDescription>
                  </div>
                )}

                <div className="flex flex-col gap-2">
                  <Label htmlFor="brreg-translation-max-attempts">
                    Retry attempts
                  </Label>
                  <Input
                    id="brreg-translation-max-attempts"
                    min={1}
                    type="number"
                    value={maxAttempts}
                    onChange={(event) => setMaxAttempts(event.target.value)}
                  />
                  <FieldDescription>
                    {usesScopedRecords
                      ? "Maximum DB task attempts for a record before it becomes terminally failed."
                      : "Maximum translation attempts for a source term before it is skipped by this run."}
                  </FieldDescription>
                </div>

                {usesScopedRecords && (
                  <>
                    <div className="flex flex-col gap-2">
                      <Label htmlFor="brreg-translation-service-retries">
                        Service retries
                      </Label>
                      <Input
                        id="brreg-translation-service-retries"
                        min={1}
                        type="number"
                        value={maxServiceRetries}
                        onChange={(event) =>
                          setMaxServiceRetries(event.target.value)
                        }
                      />
                      <FieldDescription>
                        Retries inside one batch call when the translation
                        service returns retryable errors.
                      </FieldDescription>
                    </div>

                    <div className="flex flex-col gap-2">
                      <Label htmlFor="brreg-translation-parallel">
                        Parallel batches
                      </Label>
                      <Input
                        id="brreg-translation-parallel"
                        min={1}
                        type="number"
                        value={maxParallelTasks}
                        onChange={(event) =>
                          setMaxParallelTasks(event.target.value)
                        }
                      />
                      <FieldDescription>
                        Maximum translation batches the workflow can keep active
                        at the same time.
                      </FieldDescription>
                    </div>
                  </>
                )}

                <div className="flex flex-col gap-2">
                  <Label htmlFor="brreg-translation-prompt">
                    Prompt version
                  </Label>
                  <Input
                    id="brreg-translation-prompt"
                    value={promptVersion}
                    placeholder="Server default"
                    onChange={(event) => setPromptVersion(event.target.value)}
                  />
                  <FieldDescription>
                    Prompt contract version passed to the translation service.
                  </FieldDescription>
                </div>
              </div>

              <Separator />

              <div className="grid gap-4 sm:grid-cols-2">
                <div className="flex flex-col gap-2">
                  <Label htmlFor="brreg-translation-provider">
                    LLM provider
                  </Label>
                  <select
                    id="brreg-translation-provider"
                    className="h-9 rounded-md border border-input bg-background px-3 text-sm"
                    value={provider}
                    onChange={(event) => changeProvider(event.target.value)}
                  >
                    <option value="">Default provider</option>
                    {availableLLMProviders.map((llmProvider) => (
                      <option key={llmProvider.id} value={llmProvider.slug}>
                        {llmProvider.display_name}
                        {llmProvider.is_default ? " (default)" : ""}
                      </option>
                    ))}
                  </select>
                  <FieldDescription>
                    {llmProvidersLoading
                      ? "Loading configured LLM providers."
                      : selectedLLMProvider
                        ? `Uses ${selectedLLMProvider.display_name} (${selectedLLMProvider.slug}) for this run.`
                        : "Uses the default provider configured by the scheduler and translation service."}
                  </FieldDescription>
                  {llmProvidersError && (
                    <p className="text-xs leading-5 text-destructive">
                      {llmProvidersError}
                    </p>
                  )}
                </div>

                <div className="flex flex-col gap-2">
                  <Label htmlFor="brreg-translation-model">LLM model</Label>
                  <Input
                    id="brreg-translation-model"
                    value={model}
                    placeholder="Server default"
                    onChange={(event) => setModel(event.target.value)}
                  />
                  <FieldDescription>
                    Optional model override sent to the translation service.
                    Selecting an LLM provider fills this with that provider's
                    configured model.
                  </FieldDescription>
                </div>

                {usesScopedRecords && (
                  <>
                    <div className="flex flex-col gap-2">
                      <Label htmlFor="brreg-translation-source-lang">
                        Source language
                      </Label>
                      <Input
                        id="brreg-translation-source-lang"
                        value={sourceLang}
                        placeholder="Server default"
                        onChange={(event) => setSourceLang(event.target.value)}
                      />
                      <FieldDescription>
                        Language code or name for the original BRREG payload
                        text.
                      </FieldDescription>
                    </div>

                    <div className="flex flex-col gap-2">
                      <Label htmlFor="brreg-translation-target-lang">
                        Target language
                      </Label>
                      <Input
                        id="brreg-translation-target-lang"
                        value={targetLang}
                        placeholder="Server default"
                        onChange={(event) => setTargetLang(event.target.value)}
                      />
                      <FieldDescription>
                        Language code or name for translated output.
                      </FieldDescription>
                    </div>
                  </>
                )}
              </div>
            </div>
          )}
        </div>
      )}

      <div className="flex justify-end">
        <Button disabled={!canSubmit || submitting} onClick={submit}>
          <Play className="size-4" />
          {submitting ? "Starting..." : "Start translation"}
        </Button>
      </div>
    </div>
  );
}
