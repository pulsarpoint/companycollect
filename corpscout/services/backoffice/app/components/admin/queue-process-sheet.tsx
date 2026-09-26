import { useState } from "react";
import { useFetcher } from "react-router";
import { PlayIcon } from "lucide-react";
import { QUEUE_NUMBER_LIMITS, QUEUE_TEMPLATES, type QueueFilters } from "~/lib/queues";
import { CrawlSettingsFields } from "~/components/admin/crawl-settings-fields";
import { LlmProfileField } from "~/components/admin/llm-profile-field";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { Field, FieldDescription, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { Textarea } from "~/components/ui/textarea";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";
import { Sheet, SheetContent, SheetDescription, SheetFooter, SheetHeader, SheetTitle } from "~/components/ui/sheet";

const LEGACY_BRAVE = {llm_profile_id: "", force: false, rescan_old: false, query_type: "official_website", query_template: "Find the official website of {company_name}."};

type LaunchResult = { ok: false; error: string } | { ok: true; runId: string; status: string; runUrl: string | null; taskId: string };

const LABELS: Record<string, string> = {
  force_rescan: "Force rescan", recent_days: "Freshness window (days)", batch_size: "Input batch size",
  force: "Force new search", rescan_old: "Rescan results older than 30 days", query_type: "Query type",
  query_template: "Search prompt template", requests_per_route: "Concurrent requests per route",
  input_batch_size: "Input batch size", answer_timeout_seconds: "Answer timeout (seconds)",
  force_rdap: "Force RDAP lookup", rdap_cache_days: "RDAP cache window (days)", parent_depth: "RDAP parent depth",
  max_requests: "RDAP request budget", request_delay_seconds: "Request delay (seconds)",
  rate_limit_retry_seconds: "Rate limit retry delay (seconds)", transient_retry_seconds: "Transient error retry delay (seconds)",
};

export function QueueProcessSheet({legacyTask = false, filters, total, asset, blockedReason, llmProfileId, onClose}: {
  legacyTask?: boolean; filters: QueueFilters; total: number; asset: string; blockedReason?: string | null; llmProfileId?: string; onClose: () => void;
}) {
  const fetcher = useFetcher<LaunchResult>();
  const [requestId] = useState(() => crypto.randomUUID());
  const [manual, setManual] = useState(false);
  const [json, setJson] = useState("{}");
  const [localError, setLocalError] = useState<string | null>(null);
  const busy = fetcher.state !== "idle";
  const done = fetcher.data?.ok === true ? fetcher.data : null;
  const error = busy ? null : localError ?? (fetcher.data?.ok === false ? fetcher.data.error : null);
  const defaults = filters.type === "crawler" ? null : filters.type === "brave" && legacyTask ? LEGACY_BRAVE : QUEUE_TEMPLATES[filters.type];
  const usesLlm = filters.type === "crawler" || filters.type === "brave";

  function readFields(form: HTMLFormElement) {
    const values = new FormData(form);
    const config: Record<string, unknown> = {};
    if (defaults) {
      for (const [key, defaultValue] of Object.entries(defaults)) {
        const raw = String(values.get(key) ?? "");
        config[key] = typeof defaultValue === "boolean" ? raw === "true"
          : typeof defaultValue === "number" ? Number(raw)
          : key === "max_requests" ? (raw.trim() ? Number(raw) : null) : raw;
      }
    } else {
      for (const [key, raw] of values) {
        if (key === "execution_id") continue;
        config[key] = ["force_refresh", "full_crawl_all"].includes(key) ? raw === "true"
          : ["challenge_agent_max_runs", "max_pages", "max_model_calls", "max_in_flight", "refresh_interval_days"].includes(key) ? Number(raw) : String(raw);
      }
    }
    const execution = String(values.get("execution_id") ?? "").trim();
    if (execution) config.execution_id = execution;
    return config;
  }

  return <Sheet open onOpenChange={open => { if (!open && !busy) onClose(); }}>
    <SheetContent className="data-[side=right]:w-full data-[side=right]:sm:max-w-2xl" showCloseButton={!busy}>
      <SheetHeader>
        <SheetTitle>Process queue</SheetTitle>
        <SheetDescription>{total.toLocaleString()} {total === 1 ? "input" : "inputs"} in task {filters.task}. This starts processing the whole task, including inputs outside the preview filter.</SheetDescription>
      </SheetHeader>
      <form className="flex min-h-0 flex-1 flex-col" onSubmit={event => {
        event.preventDefault();
        if (blockedReason) { setLocalError(blockedReason); return; }
        setLocalError(null);
        let config: unknown;
        try { config = manual ? JSON.parse(json) : readFields(event.currentTarget); }
        catch { setLocalError("Enter valid JSON parameters."); return; }
        fetcher.submit({task: filters.task, crawlType: filters.crawlType, requestId, config: JSON.stringify(config)}, {method: "post", action: `/admin/queues/${filters.type}`});
      }}>
        <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-y-auto px-4 pb-4">
          {done ? <Alert><AlertTitle>Processing submitted · {done.status}</AlertTitle><AlertDescription>
            <p>Dagster will validate the stored task and prepare execution when this run starts.</p>
            {done.runUrl && <a className="underline" href={done.runUrl} target="_blank" rel="noreferrer">Open run and progress</a>}
          </AlertDescription></Alert> : <>
            <p className="text-sm text-muted-foreground">{(filters.type === "webtech" || filters.type === "crawler" || (filters.type === "brave" && !legacyTask))
              ? "Freshness is checked when execution is prepared. Recent inputs remain in the queue and are counted as skipped. A draft freezes when the results asset begins."
              : filters.type === "ip-enrichment" ? "GeoIP, ASN and RDAP are saved per address. Leave the RDAP request budget empty to process the full task."
              : filters.type === "brave" ? "Searches use the saved company inputs. Freshness and force options are evaluated during processing."
              : "All enabled domains in this task are processed. Saved browser and proxy settings are preserved; recent successful results can be skipped."}</p>
            {manual ? <FieldGroup><Field><FieldLabel htmlFor="queue-json">Processing parameters (JSON)</FieldLabel>
              <Textarea id="queue-json" value={json} onChange={event => setJson(event.target.value)} className="min-h-80 font-mono" spellCheck={false} required />
              <FieldDescription>{usesLlm ? <>Use <code>llm_profile_id</code> for the selected saved LLM. Its configuration is resolved and checked before processing. API keys must not be included in this JSON.</> : "Only results-asset parameters. The task ID is fixed by your selection. Credentials belong in the service environment."}</FieldDescription>
            </Field></FieldGroup> : <>
              {defaults ? <FieldGroup className="grid grid-cols-1 sm:grid-cols-2">
                {Object.entries(defaults).map(([key, value]) => key === "llm_profile_id" ? <LlmProfileField key={key} idPrefix="queue-brave" label="Browser assistant LLM" initialProfileId={llmProfileId}
                  description="The selected LLM controls Brave's browser and CAPTCHA assistant. Before processing starts, it must pass an image and JSON response check. If the check fails, the task stays in the queue." /> : <Field key={key} className={key === "query_template" ? "sm:col-span-2" : undefined}>
                  <FieldLabel htmlFor={`queue-${key}`}>{LABELS[key] ?? key}</FieldLabel>
                  {typeof value === "boolean" ? <NativeSelect id={`queue-${key}`} name={key} defaultValue={String(value)}>
                    <NativeSelectOption value="false">No</NativeSelectOption><NativeSelectOption value="true">Yes</NativeSelectOption>
                  </NativeSelect> : <Input id={`queue-${key}`} name={key} type={typeof value === "number" || value === null ? "number" : "text"}
                    min={filters.type === "crawler" ? undefined : QUEUE_NUMBER_LIMITS[filters.type][key]?.[0]} max={filters.type === "crawler" ? undefined : QUEUE_NUMBER_LIMITS[filters.type][key]?.[1]}
                    defaultValue={value === null ? "" : String(value)} step={key === "request_delay_seconds" ? "any" : 1} required={value !== null}
                    placeholder={key === "max_requests" ? "Unlimited" : undefined} />}
                </Field>)}
              </FieldGroup> : <CrawlSettingsFields type={filters.crawlType} idPrefix="queue-crawl" initialProfileId={llmProfileId} />}
              <FieldGroup><Field><FieldLabel htmlFor="queue-execution">Execution ID (optional)</FieldLabel>
                <Input id="queue-execution" name="execution_id" placeholder="Use the original execution ID to resume" />
                <FieldDescription>{(filters.type === "webtech" || filters.type === "crawler" || (filters.type === "brave" && !legacyTask)) ? "For draft queues, leave empty to start or resume the saved execution. Pipeline failures retain their inputs. Fully processed tasks, including saved errors, clear their inputs; add inputs to a new queue to scan them again. Legacy tasks keep their existing execution rules." : "Leave empty for a new execution. To resume, use the original results run ID and its original settings."}</FieldDescription>
              </Field></FieldGroup>
              <Button type="button" variant="outline" onClick={event => {
                const form = event.currentTarget.form;
                if (form && form.reportValidity()) { setJson(JSON.stringify(readFields(form), null, 2)); setManual(true); }
              }}>Edit all parameters as JSON</Button>
            </>}
            <p className="text-xs text-muted-foreground">Results asset: <code>{asset}</code>. {(filters.type === "webtech" || filters.type === "crawler" || (filters.type === "brave" && !legacyTask)) ? "Inputs are removed when every input has a saved outcome or is skipped as recent. Individual errors remain in results and history. Pipeline failures keep inputs for recovery. Use the processing profile to control this execution." : "Existing input rows are retained for retries."}</p>
          </>}
          {!done && blockedReason && <Alert><AlertTitle>Processing unavailable</AlertTitle><AlertDescription>{blockedReason}</AlertDescription></Alert>}
          {error && <Alert variant="destructive"><AlertTitle>Could not start processing</AlertTitle><AlertDescription>{error}</AlertDescription></Alert>}
        </div>
        <SheetFooter className="border-t">
          {!done && <Button type="submit" disabled={busy || Boolean(blockedReason)}><PlayIcon data-icon="inline-start" />{busy ? usesLlm ? "Checking LLM and starting…" : "Submitting…" : `Start processing ${total.toLocaleString()} ${total === 1 ? "input" : "inputs"}`}</Button>}
          <Button type="button" variant="outline" disabled={busy} onClick={onClose}>{done ? "Close" : "Cancel"}</Button>
        </SheetFooter>
      </form>
    </SheetContent>
  </Sheet>;
}
