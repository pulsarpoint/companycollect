import { useEffect, useRef, useState } from "react";
import { Link, useFetcher } from "react-router";
import type { loader as llmProfilesLoader } from "~/routes/admin-crawl-llm-profiles";
import { Button } from "~/components/ui/button";
import { Field, FieldDescription, FieldLabel } from "~/components/ui/field";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";
import { reasoningLabel } from "~/lib/llm-model-options";

export function LlmProfileField({idPrefix, label, description, initialProfileId = "", role = "processing"}: {
  idPrefix: string; label: string; description: string; initialProfileId?: string; role?: "processing" | "decision";
}) {
  const [profileId, setProfileId] = useState(initialProfileId);
  const profiles = useFetcher<typeof llmProfilesLoader>();
  const requestedProfiles = useRef(false);
  useEffect(() => {
    if (!requestedProfiles.current) {
      requestedProfiles.current = true;
      void profiles.load(`/admin/crawls/llm-profiles${role === "decision" ? "?role=decision" : ""}`);
    }
  }, [profiles.load, role]);
  const availableProfiles = profiles.data?.profiles ?? [];
  const loadingProfiles = profiles.state !== "idle" || !profiles.data;
  const selectedProfile = availableProfiles.find(profile => profile.profileId === profileId);
  const id = (name: string) => `${idPrefix}-${name}`;
  return <Field className="sm:col-span-2">
    <FieldLabel htmlFor={id("llm-profile")}>{label}</FieldLabel>
    <NativeSelect id={id("llm-profile")} name={role === "decision" ? "decision_llm_profile_id" : "llm_profile_id"} value={selectedProfile?.profileId ?? ""}
      onChange={event => setProfileId(event.target.value)} aria-describedby={id("llm-description")} required>
      <NativeSelectOption value="">{loadingProfiles ? "Loading saved LLMs…" : `Choose a saved ${role === "decision" ? "Jev model" : "LLM"}`}</NativeSelectOption>
      {availableProfiles.map(profile => <NativeSelectOption key={profile.profileId} value={profile.profileId}>
        {profile.name} · {profile.provider} · {profile.model} · {reasoningLabel(profile.reasoningEffort)}{!profile.apiKeyAvailable ? " · No API key" : ""}
      </NativeSelectOption>)}
    </NativeSelect>
    <FieldDescription id={id("llm-description")}>{description} <Link className="underline" to="/admin/settings/llms" target="_blank" rel="noreferrer">Manage LLMs</Link>.</FieldDescription>
    {profiles.data?.error ? <><p className="text-sm text-destructive" role="alert">{profiles.data.error}</p><Button type="button" variant="outline" disabled={loadingProfiles} onClick={() => void profiles.load(`/admin/crawls/llm-profiles${role === "decision" ? "?role=decision" : ""}`)}>Reload LLMs</Button></>
      : !loadingProfiles && availableProfiles.length === 0 && <p className="text-sm text-muted-foreground">No enabled LLM is available. Add or enable a model in LLM settings before starting.</p>}
  </Field>;
}
