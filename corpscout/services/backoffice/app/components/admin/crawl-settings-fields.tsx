import { useEffect, useRef, useState } from "react";
import { Link, useFetcher } from "react-router";
import type { DomainCrawlType } from "~/lib/se-domain-selection";
import type { loader as llmProfilesLoader } from "~/routes/admin-crawl-llm-profiles";
import { Button } from "~/components/ui/button";
import { Field, FieldDescription, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { Textarea } from "~/components/ui/textarea";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";

/**
 * Results-asset execution settings, submitted as named form fields. Shared by the
 * Crawler page's saved-input sheet and the domain page's "Send for crawl" sheet;
 * the server validates them with parseCrawlSettings.
 */
export function CrawlSettingsFields({type, idPrefix, initialProfileId = ""}: {type: DomainCrawlType; idPrefix: string; initialProfileId?: string}) {
  const [pageSelection, setPageSelection] = useState("saved");
  const [profileId, setProfileId] = useState(initialProfileId);
  const profiles = useFetcher<typeof llmProfilesLoader>();
  const requestedProfiles = useRef(false);
  useEffect(() => {
    if (!requestedProfiles.current) {
      requestedProfiles.current = true;
      void profiles.load("/admin/crawls/llm-profiles");
    }
  }, [profiles.load]);
  const availableProfiles = profiles.data?.profiles ?? [];
  const loadingProfiles = profiles.state !== "idle" || !profiles.data;
  const selectedProfile = availableProfiles.find(profile => profile.profileId === profileId && profile.apiKeyAvailable);
  const id = (name: string) => `${idPrefix}-${name}`;
  return <FieldGroup className="grid grid-cols-1 gap-4 sm:grid-cols-2">
    <Field className="sm:col-span-2">
      <FieldLabel htmlFor={id("llm-profile")}>Crawl LLM</FieldLabel>
      <NativeSelect id={id("llm-profile")} name="llm_profile_id" value={selectedProfile?.profileId ?? ""}
        onChange={event => setProfileId(event.target.value)} aria-describedby={id("llm-description")} required>
        <NativeSelectOption value="">{loadingProfiles ? "Loading saved LLMs…" : "Choose a saved LLM"}</NativeSelectOption>
        {availableProfiles.map(profile => <NativeSelectOption key={profile.profileId} value={profile.profileId} disabled={!profile.apiKeyAvailable}>
          {profile.name} · {profile.provider} · {profile.model}{!profile.apiKeyAvailable ? " · API key unavailable" : ""}
        </NativeSelectOption>)}
      </NativeSelect>
      <FieldDescription id={id("llm-description")}>The selected LLM is checked with the crawler before processing starts. If the check fails, the task stays in the queue. <Link className="underline" to="/admin/settings/llms" target="_blank" rel="noreferrer">Manage LLMs</Link>.</FieldDescription>
      {profiles.data?.error ? <><p className="text-sm text-destructive" role="alert">{profiles.data.error}</p><Button type="button" variant="outline" disabled={loadingProfiles} onClick={() => void profiles.load("/admin/crawls/llm-profiles")}>Reload LLMs</Button></>
        : !loadingProfiles && !availableProfiles.some(profile => profile.apiKeyAvailable) && <p className="text-sm text-muted-foreground">No saved LLM has an available API key. Configure a model and its API key in LLM settings before starting.</p>}
    </Field>
    <Field><FieldLabel htmlFor={id("captcha-model")}>CAPTCHA model</FieldLabel><NativeSelect id={id("captcha-model")} name="challenge_agent_model" defaultValue="deepseek-flash" required><NativeSelectOption value="deepseek-flash">DeepSeek Flash</NativeSelectOption><NativeSelectOption value="z-ai/glm-5.3-flash">GLM 5.3 Flash</NativeSelectOption></NativeSelect></Field>
    <Field><FieldLabel htmlFor={id("captcha-runs")}>CAPTCHA run budget</FieldLabel><Input id={id("captcha-runs")} name="challenge_agent_max_runs" type="number" min={3} max={1000} defaultValue={3} required /></Field>
    <Field><FieldLabel htmlFor={id("max-pages")}>Page limit</FieldLabel><Input key={type} id={id("max-pages")} name="max_pages" type="number" min={1} max={500} defaultValue={type === "site_info" ? 1 : 20} readOnly={type === "site_info"} required /></Field>
    <Field><FieldLabel htmlFor={id("max-calls")}>Model call limit</FieldLabel><Input id={id("max-calls")} name="max_model_calls" type="number" min={1} max={1000} defaultValue={20} required /></Field>
    <Field><FieldLabel htmlFor={id("page-selection")}>Page selection</FieldLabel><NativeSelect id={id("page-selection")} name="page_selection" value={type === "site_info" ? "basic_info" : pageSelection} onChange={(event) => setPageSelection(event.target.value)} required>{type === "site_info" ? <NativeSelectOption value="basic_info">Basic info · single page</NativeSelectOption> : <><NativeSelectOption value="saved">Saved pages / discovery preset</NativeSelectOption><NativeSelectOption value="instructions">Custom discovery instructions</NativeSelectOption></>}</NativeSelect></Field>
    <Field><FieldLabel htmlFor={id("refresh")}>Fresh results</FieldLabel><NativeSelect id={id("refresh")} name="force_refresh" defaultValue="false"><NativeSelectOption value="false">Skip valid recent results</NativeSelectOption><NativeSelectOption value="true">Force a new crawl</NativeSelectOption></NativeSelect></Field>
    {type !== "site_info" && pageSelection === "instructions" && <Field className="sm:col-span-2"><FieldLabel htmlFor={id("instructions")}>Page-selection instructions</FieldLabel><Textarea id={id("instructions")} name="instructions" placeholder="Find current vacancies and collect the full job descriptions" maxLength={20000} required /></Field>}
    <Field><FieldLabel htmlFor={id("parallel")}>Concurrent crawls</FieldLabel><Input id={id("parallel")} name="max_in_flight" type="number" min={1} max={20} defaultValue={3} required /></Field>
    <Field><FieldLabel htmlFor={id("fresh-days")}>Freshness window (days)</FieldLabel><Input id={id("fresh-days")} name="refresh_interval_days" type="number" min={1} max={3650} defaultValue={30} required /></Field>
  </FieldGroup>;
}
