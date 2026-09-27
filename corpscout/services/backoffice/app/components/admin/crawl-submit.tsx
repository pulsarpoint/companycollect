import { useEffect, useRef, useState } from "react";
import { useFetcher } from "react-router";
import { PlayIcon } from "lucide-react";
import type { action } from "~/routes/admin-crawls";
import type { CrawlPublishReceipt } from "~/lib/crawler";
import { crawlSettingsFor } from "~/lib/crawl-settings";
import { CRAWL_TEST_PROFILES, type CrawlTestProfile } from "~/lib/crawl-test";
import { LlmProfileField } from "~/components/admin/llm-profile-field";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { Field, FieldDescription, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";
import { Textarea } from "~/components/ui/textarea";

export function CrawlSubmit({enabled, unavailableReason, initialProfile = "site_info", onPublished}: {
  enabled: boolean; unavailableReason?: string; initialProfile?: CrawlTestProfile; onPublished: (receipt: CrawlPublishReceipt) => void;
}) {
  const fetcher = useFetcher<typeof action>();
  const [profile, setProfile] = useState<CrawlTestProfile>(initialProfile);
  const [decisionSteps, setDecisionSteps] = useState({site_eligibility: "processing", link_selection: "processing"});
  const [candidatePages, setCandidatePages] = useState("");
  const submitted = useRef<{settings: string; id: string} | null>(null);
  const handled = useRef<unknown>(null);
  const busy = fetcher.state !== "idle";
  const basic = profile === "site_info";
  const selected = CRAWL_TEST_PROFILES.find(item => item.value === profile)!;

  useEffect(() => {
    if (fetcher.state !== "idle" || !fetcher.data || handled.current === fetcher.data) return;
    handled.current = fetcher.data;
    if ("receipt" in fetcher.data && fetcher.data.receipt) {
      submitted.current = null;
      onPublished(fetcher.data.receipt);
    }
  }, [fetcher.state, fetcher.data, onPublished]);

  return <section className="flex max-w-4xl flex-col gap-5" aria-labelledby="test-crawl-heading">
    <div className="flex flex-col gap-1">
      <h2 id="test-crawl-heading" className="text-lg font-semibold">Crawl a website</h2>
      <p className="text-sm text-muted-foreground">Choose what to collect and configure the crawl. Each test records a live debug trace with steps, timings, prompts and responses. Credentials are redacted.</p>
    </div>
    {!enabled && <Alert><AlertTitle>Test crawling is disabled</AlertTitle><AlertDescription>{unavailableReason || "Enable test submissions in the Backoffice configuration to use this form."}</AlertDescription></Alert>}
    <fetcher.Form method="post" action="/admin/crawls" className="flex flex-col gap-5" onSubmit={event => {
      event.preventDefault();
      const form = new FormData(event.currentTarget);
      const settings = JSON.stringify([...form]);
      if (submitted.current?.settings !== settings) submitted.current = {settings, id: crypto.randomUUID()};
      form.set("submission_id", submitted.current.id);
      fetcher.submit(form, {method: "post", action: "/admin/crawls"});
    }}>
      <input type="hidden" name="intent" value="submit" />
      <fieldset disabled={busy || !enabled} className="flex min-w-0 flex-col gap-5">
        <FieldGroup className="grid gap-4 sm:grid-cols-2 [container-type:normal]">
          <Field className="sm:col-span-2"><FieldLabel htmlFor="test-crawl-url">Domain or website URL</FieldLabel>
            <Input id="test-crawl-url" name="url" placeholder="example.com or https://example.com/careers" maxLength={8192} required />
            <FieldDescription>Each new crawl processes this website again, even if a previous result exists.</FieldDescription>
          </Field>
          <Field className="sm:col-span-2"><FieldLabel htmlFor="test-crawl-profile">Crawler profile</FieldLabel>
            <NativeSelect id="test-crawl-profile" name="crawl_profile" value={profile} onChange={event => {setProfile(event.target.value as CrawlTestProfile); setDecisionSteps({site_eligibility: "processing", link_selection: "processing"}); setCandidatePages("");}}>
              {CRAWL_TEST_PROFILES.map(item => <NativeSelectOption key={item.value} value={item.value}>{item.label}</NativeSelectOption>)}
            </NativeSelect><FieldDescription>{selected.description}</FieldDescription>
          </Field>
          <LlmProfileField idPrefix="test-crawl" label="Processing LLM" description="We verify the selected configuration before starting. Model and reasoning settings come from the saved LLM." />
          {profile !== "pages" && <>
            <Field><FieldLabel htmlFor="test-site-decision">Site eligibility decision</FieldLabel><NativeSelect id="test-site-decision" name="decision.site_eligibility" value={decisionSteps.site_eligibility} onChange={event => setDecisionSteps({...decisionSteps, site_eligibility: event.target.value})}><NativeSelectOption value="processing">Processing LLM</NativeSelectOption><NativeSelectOption value="jev">Jev</NativeSelectOption></NativeSelect><FieldDescription>Choose the model that decides whether this is a company site, shop, news site or other content site.</FieldDescription></Field>
            {!basic && <Field><FieldLabel htmlFor="test-link-decision">Page and link selection</FieldLabel><NativeSelect id="test-link-decision" name="decision.link_selection" value={decisionSteps.link_selection} onChange={event => setDecisionSteps({...decisionSteps, link_selection: event.target.value})}><NativeSelectOption value="processing">Processing LLM</NativeSelectOption><NativeSelectOption value="jev">Jev</NativeSelectOption></NativeSelect><FieldDescription>Choose which candidate pages to collect and which links to follow for this crawl's purpose.</FieldDescription></Field>}
            {Object.values(decisionSteps).includes("jev") && <LlmProfileField idPrefix="test-decision" role="decision" label="Jev decision model" description="Jev makes the selected typed decisions. Descriptions and search-query planning stay with the processing LLM. Both models share the model call limit." />}
          </>}
          {profile === "custom" && <Field className="sm:col-span-2"><FieldLabel htmlFor="test-crawl-instructions">Discovery instructions</FieldLabel><Textarea id="test-crawl-instructions" name="instructions" rows={4} maxLength={20000} placeholder="Find company contacts, office locations and annual reports" required /></Field>}
          {(profile === "pages" || profile === "custom") && <Field className="sm:col-span-2"><FieldLabel htmlFor="test-crawl-pages">{profile === "pages" ? "Pages to collect" : "Candidate pages (optional)"}</FieldLabel><Textarea id="test-crawl-pages" name="pages" rows={4} value={candidatePages} onChange={event => setCandidatePages(event.target.value)} placeholder={"/about\n/contact\nhttps://example.com/careers"} required={profile === "pages"} /><FieldDescription>One URL or relative path per line. {profile === "pages" ? "Collects these pages directly without site classification." : "Leave blank to discover pages. If supplied, collection is limited to these candidates after classifying the website."}</FieldDescription></Field>}
          <Field><FieldLabel htmlFor="test-crawl-max-pages">Page limit</FieldLabel><Input key={profile} id="test-crawl-max-pages" name="max_pages" type="number" min={1} max={500} defaultValue={basic ? 1 : 20} readOnly={basic} required /></Field>
          <Field><FieldLabel htmlFor="test-crawl-max-calls">Model call limit</FieldLabel><Input id="test-crawl-max-calls" name="max_model_calls" type="number" min={1} max={1000} defaultValue={100} required /></Field>
          {basic || profile === "pages" ? <input type="hidden" name="full_crawl_all" value="false" /> : <Field className="sm:col-span-2"><FieldLabel htmlFor="test-crawl-all">Site scope</FieldLabel><NativeSelect id="test-crawl-all" name="full_crawl_all" defaultValue="false"><NativeSelectOption value="false">Company websites only</NativeSelectOption><NativeSelectOption value="true">All sites, including shops, news and forums</NativeSelectOption></NativeSelect><FieldDescription>Company-only crawls stop after classification when a site is a shop or content site.</FieldDescription></Field>}
        </FieldGroup>
        <details>
          <summary className="cursor-pointer text-sm font-medium">Browser and CAPTCHA options</summary>
          <FieldGroup className="mt-4 grid gap-4 sm:grid-cols-2">
            <Field><FieldLabel htmlFor="test-crawl-agent">CAPTCHA model</FieldLabel><NativeSelect id="test-crawl-agent" name="challenge_agent_model" defaultValue="deepseek-flash"><NativeSelectOption value="deepseek-flash">DeepSeek Flash</NativeSelectOption><NativeSelectOption value="z-ai/glm-5.3-flash">GLM 5.3 Flash</NativeSelectOption></NativeSelect></Field>
            <Field><FieldLabel htmlFor="test-crawl-agent-runs">CAPTCHA run budget</FieldLabel><Input id="test-crawl-agent-runs" name="challenge_agent_max_runs" type="number" min={3} max={1000} defaultValue={3} required /></Field>
            <Field><FieldLabel htmlFor="test-crawl-interactive">Browser assistance</FieldLabel><NativeSelect id="test-crawl-interactive" name="interactive" defaultValue="false"><NativeSelectOption value="false">Automatic</NativeSelectOption><NativeSelectOption value="true">Allow interactive assistance</NativeSelectOption></NativeSelect></Field>
            <Field><FieldLabel htmlFor="test-crawl-artifacts">Save page artifacts</FieldLabel><NativeSelect id="test-crawl-artifacts" name="save_artifacts" defaultValue="true"><NativeSelectOption value="true">Yes</NativeSelectOption><NativeSelectOption value="false">No</NativeSelectOption></NativeSelect></Field>
            <Field className="sm:col-span-2"><FieldLabel htmlFor="test-crawl-session">Browser session ID (optional)</FieldLabel><Input id="test-crawl-session" name="session_id" pattern="[a-f0-9]{32}" maxLength={32} placeholder="Use an existing browser session" /><FieldDescription>Leave empty for an automatic browser session.</FieldDescription></Field>
          </FieldGroup>
        </details>
        {(["fetch", "model", "discovery", "search"] as const).map(group => {
          const settings = crawlSettingsFor(profile, candidatePages.trim().length > 0).filter(setting => setting.group === group);
          if (!settings.length) return null;
          return <details key={`${profile}-${group}`} open={group === "discovery" || group === "search"}>
            <summary className="cursor-pointer text-sm font-medium">{{fetch: "Page fetching", model: "Model limits and retries", discovery: "Discovery limits", search: "Search discovery"}[group]}</summary>
            <FieldGroup className="mt-4 grid gap-4 sm:grid-cols-2">
              {settings.map(setting => {
                const id = `test-config-${setting.name}`;
                const defaultValue = profile === "full" ? setting.fullDefault ?? setting.defaultValue : setting.defaultValue;
                return <Field key={setting.name}><FieldLabel htmlFor={id}>{setting.label}</FieldLabel>
                  {typeof defaultValue === "boolean" ? <NativeSelect id={id} name={`config.${setting.name}`} defaultValue={String(defaultValue)}><NativeSelectOption value="true">Yes</NativeSelectOption><NativeSelectOption value="false">No</NativeSelectOption></NativeSelect>
                    : <Input id={id} name={`config.${setting.name}`} type="number" min={setting.min} max={setting.max} step={1} defaultValue={defaultValue} required />}
                  <FieldDescription>{setting.description} Default: {typeof defaultValue === "boolean" ? defaultValue ? "Yes" : "No" : defaultValue}.</FieldDescription>
                </Field>;
              })}
            </FieldGroup>
          </details>;
        })}
      </fieldset>
      {fetcher.data?.error && <Alert variant="destructive"><AlertTitle>Crawl not confirmed</AlertTitle><AlertDescription>{fetcher.data.error} Retry with the same settings to check an uncertain submission.</AlertDescription></Alert>}
      <div className="flex items-center gap-3"><Button type="submit" disabled={busy || !enabled}><PlayIcon data-icon="inline-start" />{busy ? "Verifying and submitting…" : "Crawl"}</Button><p className="text-xs text-muted-foreground">Saved LLM credentials are encrypted before being sent to the crawler.</p></div>
    </fetcher.Form>
  </section>;
}
