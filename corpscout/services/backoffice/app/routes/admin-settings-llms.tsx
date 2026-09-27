import { recentLlmRuns } from "~/lib/llm-runs.server";
import { dagsterRunUrl } from "~/lib/dagster.server";
import { redirect } from "react-router";
import type { Route } from "./+types/admin-settings-llms";
import { CrawlLlmError, inspectLlmTest } from "~/lib/crawl-llm.server";
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
  setLlmProfileState,
} from "~/lib/llm-settings.server";

function formValue(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}

export async function loader({ request }: Route.LoaderArgs) {
  const searchParams = new URL(request.url).searchParams;
  const editingProfileId = searchParams.get("edit")?.trim() ?? "";
  const testingProfileId = searchParams.get("test")?.trim() ?? "";
  const testingProfile = testingProfileId ? await getLlmProfile(testingProfileId) : null;
  let testPreview = null;
  let testPreviewError = testingProfileId && !testingProfile ? "This model was removed. Choose another model." : "";
  if (testingProfile) {
    try { testPreview = await inspectLlmTest(testingProfile.profileId, testingProfile.revision, true); }
    catch (error) { testPreviewError = error instanceof CrawlLlmError ? error.message : "Could not load the test request. Check the crawler connection and try again."; }
  }
  return {
    profiles: await listLlmProfiles(true, true),
    testingProfile, testPreview, testPreviewError,
    creating: searchParams.get("add") === "yes",
    runs: (await recentLlmRuns()).map(run => ({...run, runUrl: run.runId ? dagsterRunUrl(run.runId) ?? undefined : undefined})),
    editingProfile:
      editingProfileId === "" ? null : await getLlmProfile(editingProfileId),
    saved: searchParams.get("saved") === "yes",
    localCodexEnabled: isLocalCodexEnabled(),
  };
}

export async function action({ request }: Route.ActionArgs) {
  const form = await request.formData();
  const intent = formValue(form, "intent");
  if (intent === "test") {
    const profileId = formValue(form, "profile_id");
    try {
      const exchange = await inspectLlmTest(profileId, Number(formValue(form, "profile_revision")), false);
      return {
        testResult: {profileId, exchange, ok: true, message: "Connection successful. The saved model configuration returned a valid test response.", checkedAt: new Date().toISOString()},
        values: null, error: "",
      };
    } catch (error) {
      return {
        testResult: {
          profileId, exchange: error instanceof CrawlLlmError ? error.exchange : null, ok: false,
          message: error instanceof CrawlLlmError || error instanceof LlmSettingsValidationError
            ? error.message : "Could not test this model. Try again or check the service connection.",
          checkedAt: new Date().toISOString(),
        },
        values: null, error: "",
      };
    }
  }
  const values: LlmSettingsFormValues | null = intent === "save" ? {
    profileId: formValue(form, "profile_id"),
    name: formValue(form, "name"),
    provider: formValue(form, "provider"),
    baseUrl: formValue(form, "base_url"),
    model: formValue(form, "model"),
    reasoningEffort: formValue(form, "reasoning_effort"),
  } : null;

  try {
    if (intent === "archive" || intent === "disable") {
      await setLlmProfileState(formValue(form, "profile_id"), intent === "archive" ? "archived" : "disabled");
      return redirect("/admin/settings/llms?saved=yes");
    }
    if (intent === "activate") {
      await activateLlmProfile(formValue(form, "profile_id"));
      return redirect("/admin/settings/llms?saved=yes");
    }
    if (intent === "set_local_codex") {
      setLocalCodexEnabled(formValue(form, "local_codex") === "on");
      return redirect("/admin/settings/llms?saved=yes");
    }
    if (values !== null) {
      await saveAndActivateLlmProfile({...values, apiKey: formValue(form, "api_key")});
      return redirect("/admin/settings/llms?saved=yes");
    }
    return { error: "Unknown LLM settings action.", values: null };
  } catch (error) {
    if (error instanceof LlmSettingsValidationError) {
      const apiKey = formValue(form, "api_key");
      return {
        error: apiKey ? error.message.replaceAll(apiKey, "[redacted]") : error.message,
        values,
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
      testingProfile={loaderData.testingProfile}
      testPreview={loaderData.testPreview}
      testPreviewError={loaderData.testPreviewError}
      creating={loaderData.creating}
      runs={loaderData.runs}
      editingProfile={loaderData.editingProfile}
      saved={loaderData.saved}
      localCodexEnabled={loaderData.localCodexEnabled}
      submittedValues={actionData?.values ?? null}
      error={actionData?.error ?? ""}
    />
  );
}
