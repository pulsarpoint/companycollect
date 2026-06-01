import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { ChevronDown, Play } from "lucide-react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Separator } from "~/components/ui/separator";
import { api, errorMessage } from "~/lib/api";
import type { LLMProvider } from "~/types/api";
import type { BrregActionScope } from "~/components/app/BrregTranslationActionForm";

interface Props {
  selectedIds: string[];
  totalCount: number;
  filters: Record<string, string>;
  initialScope?: BrregActionScope;
  onStarted?: () => void;
  onClose: () => void;
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

export function BrregDomainSearchActionForm({
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
  const [limit, setLimit] = useState("1000");
  const [searchEngine, setSearchEngine] = useState("duckduckgo");
  const [batchSize, setBatchSize] = useState("10");
  const [leaseSeconds, setLeaseSeconds] = useState("900");
  const [maxAttempts, setMaxAttempts] = useState("3");
  const [maxParallelTasks, setMaxParallelTasks] = useState("5");
  const [timeoutSeconds, setTimeoutSeconds] = useState("120");
  const [candidateThreshold, setCandidateThreshold] = useState("50");
  const [maxCandidates, setMaxCandidates] = useState("10");
  const [provider, setProvider] = useState("");
  const [model, setModel] = useState("");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [llmProviders, setLLMProviders] = useState<LLMProvider[]>([]);
  const [llmProvidersLoading, setLLMProvidersLoading] = useState(false);
  const [llmProvidersError, setLLMProvidersError] = useState("");

  useEffect(() => {
    setScope(defaultScope);
    setLimit(selectedCount > 0 ? String(selectedCount) : "1000");
    setSearchEngine("duckduckgo");
    setBatchSize("10");
    setLeaseSeconds("900");
    setMaxAttempts("3");
    setMaxParallelTasks("5");
    setTimeoutSeconds("120");
    setCandidateThreshold("50");
    setMaxCandidates("10");
    setProvider("");
    setModel("");
    setAdvancedOpen(false);
  }, [defaultScope, selectedCount]);

  useEffect(() => {
    if (!advancedOpen) return;

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
  }, [advancedOpen]);

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

  const limitDisabled = scope === "selected";
  const effectiveLimit = limitDisabled
    ? selectedCount
    : parseRequiredPositiveNumber(limit);
  const canSubmit =
    effectiveLimit !== null && (scope !== "selected" || selectedCount > 0);
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
      const body: {
        ids?: string[];
        filters?: Record<string, string>;
        limit?: number;
        batch_size?: number;
        max_attempts?: number;
        max_parallel_tasks?: number;
        lease_seconds?: number;
        search_engine?: string;
        provider?: string;
        model?: string;
        candidate_threshold?: number;
        max_candidates?: number;
        timeout_seconds?: number;
      } = {
        limit: effectiveLimit,
        batch_size: parseOptionalPositiveNumber(batchSize),
        max_attempts: parseOptionalPositiveNumber(maxAttempts),
        max_parallel_tasks: parseOptionalPositiveNumber(maxParallelTasks),
        lease_seconds: parseOptionalPositiveNumber(leaseSeconds),
        search_engine: optionalText(searchEngine),
        provider: optionalText(provider),
        model: optionalText(model),
        candidate_threshold: parseOptionalPositiveNumber(candidateThreshold),
        max_candidates: parseOptionalPositiveNumber(maxCandidates),
        timeout_seconds: parseOptionalPositiveNumber(timeoutSeconds),
      };

      if (scope === "selected") body.ids = selectedIds;
      if (scope === "filtered") body.filters = filters;

      await api.searchBrregDomains(body);
      toast.success("BRREG domain discovery workflow started.");
      onStarted?.();
      onClose();
    } catch (error) {
      toast.error(
        errorMessage(error, "Failed to start BRREG domain discovery."),
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="rounded-md border bg-muted/20 p-3">
        <div className="text-sm font-medium">Domain discovery</div>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Starts the Temporal workflow that fetches a search page, asks the LLM
          to extract candidate URLs, and writes search artifacts to Corpscout.
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
          <Label htmlFor="brreg-domain-search-scope">Records source</Label>
          <select
            id="brreg-domain-search-scope"
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
            filters, or the next eligible records without domain artifacts.
          </FieldDescription>
        </div>

        <div className="grid gap-4 sm:grid-cols-2">
          <div className="flex flex-col gap-2">
            <Label htmlFor="brreg-domain-search-limit">Records</Label>
            <Input
              id="brreg-domain-search-limit"
              min={1}
              type="number"
              value={limitDisabled ? String(selectedCount) : limit}
              disabled={limitDisabled}
              onChange={(event) => setLimit(event.target.value)}
            />
            <FieldDescription>
              Maximum number of BRREG records this workflow can select.
            </FieldDescription>
          </div>

          <div className="flex flex-col gap-2">
            <Label htmlFor="brreg-domain-search-engine">Search engine</Label>
            <select
              id="brreg-domain-search-engine"
              className="h-9 rounded-md border border-input bg-background px-3 text-sm"
              value={searchEngine}
              onChange={(event) => setSearchEngine(event.target.value)}
            >
              <option value="duckduckgo">DuckDuckGo</option>
              <option value="yandex">Yandex</option>
            </select>
            <FieldDescription>
              Search provider used for the first-page crawl.
            </FieldDescription>
          </div>
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
          <ChevronDown
            className={`size-4 transition-transform ${advancedOpen ? "rotate-180" : ""}`}
          />
        </Button>

        {advancedOpen && (
          <div className="flex flex-col gap-4">
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-domain-search-batch-size">
                  Batch size
                </Label>
                <Input
                  id="brreg-domain-search-batch-size"
                  min={1}
                  type="number"
                  value={batchSize}
                  onChange={(event) => setBatchSize(event.target.value)}
                />
                <FieldDescription>
                  Number of records claimed and processed in one workflow loop.
                </FieldDescription>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-domain-search-lease">
                  Lease seconds
                </Label>
                <Input
                  id="brreg-domain-search-lease"
                  min={1}
                  type="number"
                  value={leaseSeconds}
                  onChange={(event) => setLeaseSeconds(event.target.value)}
                />
                <FieldDescription>
                  DB task lease and Temporal activity timeout budget for a
                  claimed domain-search batch.
                </FieldDescription>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-domain-search-attempts">
                  Retry attempts
                </Label>
                <Input
                  id="brreg-domain-search-attempts"
                  min={1}
                  type="number"
                  value={maxAttempts}
                  onChange={(event) => setMaxAttempts(event.target.value)}
                />
                <FieldDescription>
                  Maximum DB task attempts before a record becomes terminally
                  failed.
                </FieldDescription>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-domain-search-parallel">
                  Parallel tasks
                </Label>
                <Input
                  id="brreg-domain-search-parallel"
                  min={1}
                  type="number"
                  value={maxParallelTasks}
                  onChange={(event) => setMaxParallelTasks(event.target.value)}
                />
                <FieldDescription>
                  Maximum active domain-search DB leases across this workflow.
                </FieldDescription>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-domain-search-timeout">
                  Service timeout
                </Label>
                <Input
                  id="brreg-domain-search-timeout"
                  min={1}
                  type="number"
                  value={timeoutSeconds}
                  onChange={(event) => setTimeoutSeconds(event.target.value)}
                />
                <FieldDescription>
                  Timeout sent to crawl-service for search fetch and LLM
                  analysis requests.
                </FieldDescription>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-domain-search-threshold">
                  Candidate threshold
                </Label>
                <Input
                  id="brreg-domain-search-threshold"
                  min={0}
                  max={100}
                  type="number"
                  value={candidateThreshold}
                  onChange={(event) => setCandidateThreshold(event.target.value)}
                />
                <FieldDescription>
                  Minimum LLM score for candidate URLs to keep in the artifact.
                </FieldDescription>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-domain-search-max-candidates">
                  Max candidates
                </Label>
                <Input
                  id="brreg-domain-search-max-candidates"
                  min={1}
                  type="number"
                  value={maxCandidates}
                  onChange={(event) => setMaxCandidates(event.target.value)}
                />
                <FieldDescription>
                  Maximum candidate URLs returned by search-page analysis.
                </FieldDescription>
              </div>
            </div>

            <Separator />

            <div className="grid gap-4 sm:grid-cols-2">
              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-domain-search-provider">
                  LLM provider
                </Label>
                <select
                  id="brreg-domain-search-provider"
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
                      ? `Uses ${selectedLLMProvider.display_name} (${selectedLLMProvider.slug}) for search analysis.`
                      : "Uses the default provider configured by the scheduler and crawl service."}
                </FieldDescription>
                {llmProvidersError && (
                  <p className="text-xs leading-5 text-destructive">
                    {llmProvidersError}
                  </p>
                )}
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="brreg-domain-search-model">LLM model</Label>
                <Input
                  id="brreg-domain-search-model"
                  value={model}
                  placeholder="Server default"
                  onChange={(event) => setModel(event.target.value)}
                />
                <FieldDescription>
                  Optional model override sent to crawl-service search
                  analysis.
                </FieldDescription>
              </div>
            </div>
          </div>
        )}
      </div>

      <div className="flex justify-end">
        <Button disabled={!canSubmit || submitting} onClick={submit}>
          <Play className="size-4" />
          {submitting ? "Starting..." : "Start domain discovery"}
        </Button>
      </div>
    </div>
  );
}
