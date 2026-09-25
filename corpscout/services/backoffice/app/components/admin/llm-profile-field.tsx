import { useEffect, useRef, useState } from "react";
import { Link, useFetcher } from "react-router";
import type { loader as llmProfilesLoader } from "~/routes/admin-crawl-llm-profiles";
import { Button } from "~/components/ui/button";
import { Field, FieldDescription, FieldLabel } from "~/components/ui/field";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";

export function LlmProfileField({idPrefix, label, description, initialProfileId = ""}: {
  idPrefix: string; label: string; description: string; initialProfileId?: string;
}) {
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
  return <Field className="sm:col-span-2">
    <FieldLabel htmlFor={id("llm-profile")}>{label}</FieldLabel>
    <NativeSelect id={id("llm-profile")} name="llm_profile_id" value={selectedProfile?.profileId ?? ""}
      onChange={event => setProfileId(event.target.value)} aria-describedby={id("llm-description")} required>
      <NativeSelectOption value="">{loadingProfiles ? "Loading saved LLMs…" : "Choose a saved LLM"}</NativeSelectOption>
      {availableProfiles.map(profile => <NativeSelectOption key={profile.profileId} value={profile.profileId} disabled={!profile.apiKeyAvailable}>
        {profile.name} · {profile.provider} · {profile.model}{!profile.apiKeyAvailable ? " · API key unavailable" : ""}
      </NativeSelectOption>)}
    </NativeSelect>
    <FieldDescription id={id("llm-description")}>{description} <Link className="underline" to="/admin/settings/llms" target="_blank" rel="noreferrer">Manage LLMs</Link>.</FieldDescription>
    {profiles.data?.error ? <><p className="text-sm text-destructive" role="alert">{profiles.data.error}</p><Button type="button" variant="outline" disabled={loadingProfiles} onClick={() => void profiles.load("/admin/crawls/llm-profiles")}>Reload LLMs</Button></>
      : !loadingProfiles && !availableProfiles.some(profile => profile.apiKeyAvailable) && <p className="text-sm text-muted-foreground">No saved LLM has an available API key. Configure a model and its API key in LLM settings before starting.</p>}
  </Field>;
}
