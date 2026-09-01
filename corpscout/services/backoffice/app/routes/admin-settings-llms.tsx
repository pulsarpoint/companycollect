import { redirect } from "react-router";
import type { Route } from "./+types/admin-settings-llms";
import {
  LlmSettingsWorkspace,
  type LlmSettingsFormValues,
} from "~/components/admin/llm-settings-workspace";
import {
  activateLlmProfile,
  getLlmProfile,
  isLocalCodexEnabled,
  listLlmProfiles,
  LlmSettingsValidationError,
  saveAndActivateLlmProfile,
  setLocalCodexEnabled,
} from "~/lib/llm-settings.server";

function formValue(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}

export async function loader({ request }: Route.LoaderArgs) {
  const searchParams = new URL(request.url).searchParams;
  const editingProfileId = searchParams.get("edit")?.trim() ?? "";
  return {
    profiles: listLlmProfiles(),
    editingProfile:
      editingProfileId === "" ? null : getLlmProfile(editingProfileId),
    saved: searchParams.get("saved") === "yes",
    localCodexEnabled: isLocalCodexEnabled(),
  };
}

export async function action({ request }: Route.ActionArgs) {
  const form = await request.formData();
  const intent = formValue(form, "intent");

  try {
    if (intent === "activate") {
      activateLlmProfile(formValue(form, "profile_id"));
      return redirect("/admin/settings/llms?saved=yes");
    }
    if (intent === "set_local_codex") {
      setLocalCodexEnabled(formValue(form, "local_codex") === "on");
      return redirect("/admin/settings/llms?saved=yes");
    }
    if (intent === "save") {
      const values: LlmSettingsFormValues = {
        profileId: formValue(form, "profile_id"),
        name: formValue(form, "name"),
        provider: formValue(form, "provider"),
        baseUrl: formValue(form, "base_url"),
        model: formValue(form, "model"),
        apiKeyEnvironmentVariable: formValue(
          form,
          "api_key_environment_variable",
        ),
      };
      saveAndActivateLlmProfile(values);
      return redirect("/admin/settings/llms?saved=yes");
    }
    return { error: "Unknown LLM settings action.", values: null };
  } catch (error) {
    if (error instanceof LlmSettingsValidationError) {
      return {
        error: error.message,
        values:
          intent === "save"
            ? {
                profileId: formValue(form, "profile_id"),
                name: formValue(form, "name"),
                provider: formValue(form, "provider"),
                baseUrl: formValue(form, "base_url"),
                model: formValue(form, "model"),
                apiKeyEnvironmentVariable: formValue(
                  form,
                  "api_key_environment_variable",
                ),
              }
            : null,
      };
    }
    throw error;
  }
}

export function meta() {
  return [{ title: "LLM settings | CompanyCollect" }];
}

export default function AdminLlmSettings({
  loaderData,
  actionData,
}: Route.ComponentProps) {
  return (
    <LlmSettingsWorkspace
      profiles={loaderData.profiles}
      editingProfile={loaderData.editingProfile}
      saved={loaderData.saved}
      localCodexEnabled={loaderData.localCodexEnabled}
      submittedValues={actionData?.values ?? null}
      error={actionData?.error ?? ""}
    />
  );
}
