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
  BrregCompanyTranslationRequest,
  LLMProvider,
} from "~/types/api";

type CompanyTranslationScope = "all" | "limited";

interface Props {
  selectedIds: string[];
  totalCount: number;
  filters: Record<string, string>;
  initialScope?: unknown;
  recordLabel?: string;
  description?: string;
  sourceLabel?: string;
  sourceDatabase?: string;
  formIdPrefix?: string;
  submitTranslation?: (
    body: BrregCompanyTranslationRequest,
  ) => Promise<unknown>;
  successMessage?: string;
  failureMessage?: string;
  showAdvancedOptions?: boolean;
  onStarted?: () => void;
  onClose: () => void;
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
  totalCount: _totalCount,
  filters,
  initialScope: _initialScope,
  recordLabel: _recordLabel = "source entries",
  description,
  sourceLabel = "BRREG",
  sourceDatabase = "brreg_source",
  formIdPrefix = "brreg",
  submitTranslation = api.translateBrregSourceCompanies,
  successMessage = "BRREG company translation workflow started.",
  failureMessage = "Failed to start BRREG translation.",
  showAdvancedOptions = true,
  onStarted,
  onClose,
}: Props) {
  const [limit, setLimit] = useState("1000");
  const [leaseSeconds, setLeaseSeconds] = useState("1800");
  const [maxAttempts, setMaxAttempts] = useState("8");
  const [batchDelaySeconds, setBatchDelaySeconds] = useState("60");
  const [maxParallelTasks, setMaxParallelTasks] = useState("1");
  const [maxRequestChars, setMaxRequestChars] = useState("6000");
  const [maxTerms, setMaxTerms] = useState("25");
  const [maxCompaniesPerBatch, setMaxCompaniesPerBatch] = useState("100");
  const [provider, setProvider] = useState("");
  const [model, setModel] = useState("");
  const [promptVersion, setPromptVersion] = useState("");
  const [companyScope, setCompanyScope] =
    useState<CompanyTranslationScope>("all");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [llmProviders, setLLMProviders] = useState<LLMProvider[]>([]);
  const [llmProvidersLoading, setLLMProvidersLoading] = useState(false);
  const [llmProvidersError, setLLMProvidersError] = useState("");

  useEffect(() => {
    setLimit("10");
    setLeaseSeconds("1800");
    setMaxAttempts("8");
    setBatchDelaySeconds("60");
    setMaxParallelTasks("1");
    setMaxRequestChars("6000");
    setMaxTerms("25");
    setMaxCompaniesPerBatch("100");
    setProvider("");
    setModel("");
    setPromptVersion("");
    setCompanyScope("all");
    setAdvancedOpen(false);
  }, []);

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

  const allSourceCompaniesSelected = companyScope === "all";
  const effectiveLimit = allSourceCompaniesSelected
    ? 0
    : parseRequiredPositiveNumber(limit);
  const canSubmit = effectiveLimit !== null;
  const availableLLMProviders = useMemo(
    () => llmProviders.filter((llmProvider) => llmProvider.enabled),
    [llmProviders],
  );
  const effectiveDescription =
    description ??
    `Starts the Temporal workflow that prepares a Postgres company translation queue, translates uncached terms, and writes English values back to ${sourceDatabase}.`;
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
      const hasSelectedRows = selectedIds.length > 0;
      const useAutoClaim = allSourceCompaniesSelected;
      const body: BrregCompanyTranslationRequest = {
        all_records: allSourceCompaniesSelected,
        ids: hasSelectedRows ? selectedIds : undefined,
        filters: hasSelectedRows ? undefined : filters,
        limit: hasSelectedRows
          ? selectedIds.length
          : effectiveLimit,
        batch_size: allSourceCompaniesSelected ? undefined : effectiveLimit,
        claim_mode: useAutoClaim ? "auto" : "fixed",
        max_request_chars: useAutoClaim
          ? parseOptionalPositiveNumber(maxRequestChars)
          : undefined,
        max_terms: parseOptionalPositiveNumber(maxTerms),
        max_companies_per_batch: useAutoClaim
          ? parseOptionalPositiveNumber(maxCompaniesPerBatch)
          : undefined,
        max_attempts: parseOptionalPositiveNumber(maxAttempts),
        batch_delay_seconds: parseOptionalPositiveNumber(batchDelaySeconds),
        max_parallel_tasks: parseOptionalPositiveNumber(maxParallelTasks),
        lease_seconds: parseOptionalPositiveNumber(leaseSeconds),
        provider: optionalText(provider),
        model: optionalText(model),
        prompt_version: optionalText(promptVersion),
        trigger: "manual",
      };
      await submitTranslation(body);
      toast.success(successMessage);
      onStarted?.();
      onClose();
    } catch (error) {
      toast.error(errorMessage(error, failureMessage));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="rounded-md border bg-muted/20 p-3">
        <div className="text-sm font-medium">Translation</div>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          {effectiveDescription}
        </p>
      </div>

      <div className="flex flex-col gap-4">
        <div className="flex flex-col gap-2">
          <Label htmlFor={`${formIdPrefix}-company-translation-scope`}>
            Records to process
          </Label>
          <select
            id={`${formIdPrefix}-company-translation-scope`}
            className="h-9 rounded-md border border-input bg-background px-3 text-sm"
            value={companyScope}
            onChange={(event) =>
              setCompanyScope(event.target.value as CompanyTranslationScope)
            }
          >
            <option value="all">All eligible entries</option>
            <option value="limited">Specific number</option>
          </select>
          <FieldDescription>
            All eligible entries prepares queue rows for every {sourceLabel}
            source company with missing English fields. Specific number is
            intended for testing.
          </FieldDescription>
        </div>

        <div className="flex flex-col gap-2">
          <Label htmlFor={`${formIdPrefix}-translation-limit`}>Records</Label>
          <Input
            id={`${formIdPrefix}-translation-limit`}
            min={allSourceCompaniesSelected ? undefined : 1}
            type={allSourceCompaniesSelected ? "text" : "number"}
            value={allSourceCompaniesSelected ? "All eligible entries" : limit}
            disabled={allSourceCompaniesSelected}
            onChange={(event) => setLimit(event.target.value)}
          />
          <FieldDescription>
            {allSourceCompaniesSelected
              ? `No test limit is sent. The workflow drains eligible ${sourceLabel} source companies in bounded queue chunks.`
              : `Maximum number of ${sourceLabel} source companies prepared in the queue for this test run.`}
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
                  <Label htmlFor={`${formIdPrefix}-translation-timeout`}>
                    Timeout seconds
                  </Label>
                  <Input
                    id={`${formIdPrefix}-translation-timeout`}
                    min={1}
                    type="number"
                    value={leaseSeconds}
                    onChange={(event) => setLeaseSeconds(event.target.value)}
                  />
                  <FieldDescription>
                    Activity timeout budget for translation queue steps.
                  </FieldDescription>
                </div>

                <div className="flex flex-col gap-2">
                  <Label htmlFor={`${formIdPrefix}-translation-max-attempts`}>
                    Retry attempts
                  </Label>
                  <Input
                    id={`${formIdPrefix}-translation-max-attempts`}
                    min={1}
                    type="number"
                    value={maxAttempts}
                    onChange={(event) => setMaxAttempts(event.target.value)}
                  />
                  <FieldDescription>
                    Maximum attempts for queue preparation, claim, translation,
                    and completion activities.
                  </FieldDescription>
                </div>

                <div className="flex flex-col gap-2">
                  <Label htmlFor={`${formIdPrefix}-translation-batch-delay`}>
                    Batch delay seconds
                  </Label>
                  <Input
                    id={`${formIdPrefix}-translation-batch-delay`}
                    min={1}
                    type="number"
                    value={batchDelaySeconds}
                    onChange={(event) =>
                      setBatchDelaySeconds(event.target.value)
                    }
                  />
                  <FieldDescription>
                    Retry backoff hint for Temporal activities. A blocked queue
                    claim is checked again every 5 seconds.
                  </FieldDescription>
                </div>

                <div className="flex flex-col gap-2">
                  <Label htmlFor={`${formIdPrefix}-translation-parallel`}>
                    Parallel worksets
                  </Label>
                  <Input
                    id={`${formIdPrefix}-translation-parallel`}
                    min={1}
                    type="number"
                    value={maxParallelTasks}
                    onChange={(event) =>
                      setMaxParallelTasks(event.target.value)
                    }
                  />
                  <FieldDescription>
                    Kept for workflow compatibility. The queue admits one
                    running translation batch per source.
                  </FieldDescription>
                </div>

                {allSourceCompaniesSelected && (
                  <div className="flex flex-col gap-2">
                    <Label
                      htmlFor={`${formIdPrefix}-company-translation-max-chars`}
                    >
                      Max request characters
                    </Label>
                    <Input
                      id={`${formIdPrefix}-company-translation-max-chars`}
                      min={1}
                      type="number"
                      value={maxRequestChars}
                      onChange={(event) =>
                        setMaxRequestChars(event.target.value)
                      }
                    />
                    <FieldDescription>
                      Queue claim keeps adding companies until the estimated
                      untranslated text reaches this LLM request budget.
                    </FieldDescription>
                  </div>
                )}

                <div className="flex flex-col gap-2">
                  <Label htmlFor={`${formIdPrefix}-company-translation-max-terms`}>
                    Max claim candidates
                  </Label>
                  <Input
                    id={`${formIdPrefix}-company-translation-max-terms`}
                    min={1}
                    type="number"
                    value={maxTerms}
                    onChange={(event) => setMaxTerms(event.target.value)}
                  />
                  <FieldDescription>
                    Caps pending company queue rows inspected per claim. The
                    character budget decides the final LLM batch size.
                  </FieldDescription>
                </div>

                {allSourceCompaniesSelected && (
                  <div className="flex flex-col gap-2">
                    <Label
                      htmlFor={`${formIdPrefix}-company-translation-max-companies`}
                    >
                      Max companies per queue prep
                    </Label>
                    <Input
                      id={`${formIdPrefix}-company-translation-max-companies`}
                      min={1}
                      type="number"
                      value={maxCompaniesPerBatch}
                      onChange={(event) =>
                        setMaxCompaniesPerBatch(event.target.value)
                      }
                    />
                    <FieldDescription>
                      Safety cap for one all-records queue preparation pass.
                    </FieldDescription>
                  </div>
                )}

                <div className="flex flex-col gap-2">
                  <Label htmlFor={`${formIdPrefix}-translation-prompt`}>
                    Prompt version
                  </Label>
                  <Input
                    id={`${formIdPrefix}-translation-prompt`}
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
                  <Label htmlFor={`${formIdPrefix}-translation-provider`}>
                    LLM provider
                  </Label>
                  <select
                    id={`${formIdPrefix}-translation-provider`}
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
                  <Label htmlFor={`${formIdPrefix}-translation-model`}>
                    LLM model
                  </Label>
                  <Input
                    id={`${formIdPrefix}-translation-model`}
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
