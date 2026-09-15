import { useEffect, useRef, useState } from "react";
import { Link, useFetcher } from "react-router";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
import { Input } from "~/components/ui/input";
import { Field, FieldContent, FieldDescription, FieldGroup, FieldLabel } from "~/components/ui/field";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "~/components/ui/dialog";
import { COMPANY_ACTION_AREAS, type CompanyActionSelection, type CompanyActionResult } from "~/lib/company-actions";
import type { LlmProfile } from "~/lib/llm-settings.server";
import type { PeoplePrompt } from "~/lib/people-prompts.server";

export function CompanyActionDialog({ profiles, prompts, selected, disabled, onClose, onLaunched }: {
  selected: CompanyActionSelection;
  disabled: boolean;
  onClose: () => void;
  onLaunched: (result: CompanyActionResult, selection: CompanyActionSelection) => void;
  profiles: Pick<LlmProfile, "profileId" | "name" | "provider" | "model" | "isActive">[];
  prompts: PeoplePrompt[];
}) {
  const [verifyDomains, setVerifyDomains] = useState(true);
  const [changedOnly, setChangedOnly] = useState(true);
  const [profileId, setProfileId] = useState(profiles.find((p) => p.isActive)?.profileId ?? profiles[0]?.profileId ?? "");
  const [promptId, setPromptId] = useState(prompts[0]?.promptId ?? "");
  const prompt = prompts.find((p) => p.promptId === promptId);
  const fetcher = useFetcher<CompanyActionResult>();
  const busy = fetcher.state !== "idle";
  const area = COMPANY_ACTION_AREAS.find((entry) => entry.value === selected?.area);
  const fullProcessing = selected?.operation === "process";
  const processesDomains = fullProcessing && selected.area === "domains";
  const needsModel = fullProcessing && (selected.area === "info" || selected.area === "people" || (processesDomains && verifyDomains));
  const needsPrompt = fullProcessing && (selected.area === "people" || (processesDomains && verifyDomains));
  const operationLabel = fullProcessing ? "Full processing" : "Sync inputs";

  const handledRun = useRef<string | undefined>(undefined);
  useEffect(() => {
    if (fetcher.data?.ok && fetcher.data.runId && handledRun.current !== fetcher.data.runId) {
      handledRun.current = fetcher.data.runId;
      onLaunched(fetcher.data, selected);
    }
  }, [fetcher.data, selected, onLaunched]);

  return <Dialog open onOpenChange={(open) => { if (!open && !busy) onClose(); }}>
      <DialogContent showCloseButton={!busy} className="max-h-[90vh] overflow-y-auto sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>{area?.label} · {operationLabel}</DialogTitle>
          <DialogDescription>Run across all Swedish companies using the sources already ingested in Dagster.</DialogDescription>
        </DialogHeader>
        {selected && area && <fetcher.Form method="post" action="/admin/se/company-actions" className="flex flex-col gap-4">
          <input type="hidden" name="area" value={selected.area} />
          <input type="hidden" name="operation" value={selected.operation} />
          <p className="text-sm text-muted-foreground">{area[selected.operation]}</p>
          {processesDomains && <>
            <input type="hidden" name="verify_domains" value={String(verifyDomains)} />
            <Field orientation="horizontal" data-disabled={busy}>
              <Checkbox id="company-action-verify-domains" checked={verifyDomains} onCheckedChange={setVerifyDomains} disabled={busy} />
              <FieldContent><FieldLabel htmlFor="company-action-verify-domains">Verify uncertain or conflicting associations</FieldLabel>
                <FieldDescription>{verifyDomains ? "Check all eligible associations across all companies. Use the LLM only when source evidence is inconclusive or conflicts. Strong source claims and reviewer decisions do not need a call." : "Use source precedence and existing verifications. Unresolved associations remain unpublished."}</FieldDescription></FieldContent>
            </Field>
          </>}
          {needsModel && <FieldGroup>
            {needsModel && <Field data-disabled={busy}>
              <div className="flex justify-between"><FieldLabel htmlFor="company-action-llm">LLM profile</FieldLabel><Link to="/admin/settings/llms" className="text-xs underline">Manage LLMs</Link></div>
              <NativeSelect id="company-action-llm" name="profile_id" value={profileId} onChange={(e) => setProfileId(e.target.value)} className="w-full" required disabled={busy}>
                <NativeSelectOption value="" disabled>Choose a model</NativeSelectOption>
                {profiles.map((p) => <NativeSelectOption key={p.profileId} value={p.profileId}>{p.name} · {p.provider} / {p.model}</NativeSelectOption>)}
              </NativeSelect>
            </Field>}
            {fullProcessing && selected.area === "info" && <Field data-disabled={busy}>
              <FieldLabel htmlFor="company-action-llm-limit">Description processing limit</FieldLabel>
              <Input id="company-action-llm-limit" name="llm_max_companies" type="number" min={1} max={1_000_000} defaultValue={5_000} required disabled={busy} />
              <FieldDescription>Maximum companies considered for description synthesis in this run. Uses the existing company-description prompt and reuses cached answers. Source sync and publishing cover all companies.</FieldDescription>
            </Field>}
            {needsPrompt && <>
              <Field data-disabled={busy}>
                <div className="flex justify-between"><FieldLabel htmlFor="company-action-prompt">{processesDomains ? "Domain prompt" : "People prompt"}</FieldLabel><Link to={processesDomains ? "/admin/settings/domain-prompts" : "/admin/settings/people-prompts"} className="text-xs underline">Manage prompts</Link></div>
                <NativeSelect id="company-action-prompt" name="prompt_id" value={promptId} onChange={(e) => setPromptId(e.target.value)} className="w-full" required disabled={busy}>
                  <NativeSelectOption value="" disabled>Choose a prompt</NativeSelectOption>
                  {prompts.map((p) => <NativeSelectOption key={p.promptId} value={p.promptId}>{p.name} · revision {p.revision}</NativeSelectOption>)}
                </NativeSelect>
                <input type="hidden" name="prompt_revision" value={prompt?.revision ?? ""} />
                <FieldDescription>Changing the model settings or prompt text requests new processing. This run keeps a copy of the selected prompt.</FieldDescription>
                {prompt && <details className="rounded-md border p-3"><summary className="cursor-pointer text-sm">Review prompt</summary>
                  <pre className="mt-3 max-h-60 overflow-y-auto whitespace-pre-wrap text-xs leading-relaxed">{prompt.systemPrompt}</pre></details>}
              </Field>
              <input type="hidden" name="changed_only" value={String(changedOnly)} />
              <Field orientation="horizontal" data-disabled={busy}>
                <Checkbox id="company-action-changed-only" checked={changedOnly} onCheckedChange={setChangedOnly} aria-describedby="company-action-changed-only-description" disabled={busy} />
                <FieldContent>
                  <FieldLabel htmlFor="company-action-changed-only">{processesDomains ? "Only verify new or changed input" : "Only match new or changed input"}</FieldLabel>
                  <FieldDescription id="company-action-changed-only-description">{changedOnly
                    ? processesDomains ? "Reuse saved results when company identity, domain evidence, prompt and model are unchanged." : "Reuse saved results when people, prompt, and model are unchanged."
                    : "Call the LLM again for eligible inputs, including unchanged evidence. This incurs new LLM usage."}</FieldDescription>
                </FieldContent>
              </Field>
            </>}
          </FieldGroup>}
          {fetcher.data?.error && <Alert variant="destructive"><AlertDescription>{fetcher.data.error} {fetcher.data.runUrl &&
            <a href={fetcher.data.runUrl} target="_blank" rel="noreferrer">View existing run</a>}</AlertDescription></Alert>}
          {disabled && <p role="status" className="text-sm text-muted-foreground">Launching is unavailable while this workflow is active or its status cannot be verified.</p>}
          {fetcher.data?.ok && <p role="status" className="text-sm">Run submitted ({fetcher.data.status}). {fetcher.data.runUrl &&
            <a href={fetcher.data.runUrl} target="_blank" rel="noreferrer" className="underline">View run in Dagster</a>}</p>}
          <div className="flex justify-end gap-2">
            <Button variant="outline" type="button" disabled={busy} onClick={onClose}>Close</Button>
            <Button type="submit" disabled={disabled || busy || Boolean(fetcher.data?.ok) || (needsModel && !profileId) || (needsPrompt && !prompt)}>
              {busy ? "Submitting…" : fullProcessing ? "Start full processing" : "Start input sync"}
            </Button>
          </div>
        </fetcher.Form>}
      </DialogContent>
    </Dialog>;
}
