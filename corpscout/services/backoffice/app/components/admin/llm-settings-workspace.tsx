import { useEffect, useState } from "react";
import { Form, Link, useNavigate, useNavigation, useRevalidator } from "react-router";
import { BotIcon, KeyRoundIcon, PlusIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetDescription } from "~/components/ui/sheet";
import { Switch } from "~/components/ui/switch";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "~/components/ui/tabs";
import { Dialog, DialogTrigger, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter, DialogClose } from "~/components/ui/dialog";
import type { recentLlmRuns } from "~/lib/llm-runs.server";
import { LlmTestSheet } from "./llm-test-sheet";
import type { LlmTestExchange } from "~/lib/crawl-llm.server";
import type { LlmProfile } from "~/lib/llm-settings.server";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";
import { isDecisionModel, LLM_MODEL_PRESETS, reasoningLabel, reasoningOptions } from "~/lib/llm-model-options";

export interface LlmSettingsFormValues {
  profileId: string;
  name: string;
  provider: string;
  baseUrl: string;
  model: string;
  reasoningEffort?: string;
}

function profileFormValues(profile: LlmProfile | null): LlmSettingsFormValues {
  return {
    profileId: profile?.profileId ?? "",
    name: profile?.name ?? "",
    provider: profile?.provider ?? "",
    baseUrl: profile?.baseUrl ?? "",
    model: profile?.model ?? "",
    reasoningEffort: profile?.reasoningEffort ?? "",
  };
}

function ActiveLlmCard({ profile }: { profile: LlmProfile | null }) {
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center gap-2">
          <CardTitle>Default LLM</CardTitle>
          {profile ? <Badge>In use</Badge> : <Badge variant="destructive">Not configured</Badge>}
        </div>
        <CardDescription>
          This profile is selected by default for LLM processing tasks.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {profile ? (
          <dl className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <div className="flex flex-col gap-1">
              <dt className="text-xs text-muted-foreground">Profile</dt>
              <dd className="font-medium">{profile.name}</dd>
            </div>
            <div className="flex flex-col gap-1">
              <dt className="text-xs text-muted-foreground">Provider</dt>
              <dd>{profile.provider}</dd>
            </div>
            <div className="flex flex-col gap-1">
              <dt className="text-xs text-muted-foreground">Model</dt>
              <dd className="font-mono text-sm">{profile.model}</dd>
            </div>
            <div className="flex flex-col gap-1">
              <dt className="text-xs text-muted-foreground">API key</dt>
              <dd className="flex flex-wrap items-center gap-2">
                <Badge
                  variant="secondary"
                >
                  {profile.apiKeyAvailable ? "Saved" : "Not configured"}
                </Badge>
              </dd>
            </div>
          </dl>
        ) : (
          <p className="text-sm text-muted-foreground">
            Add an LLM profile below before running an LLM-backed processing
            step.
          </p>
        )}
      </CardContent>
    </Card>
  );
}

function LlmProfileRow({ profile }: { profile: LlmProfile }) {
  const result = profile.lastCheck;

  return <>
    <TableRow>
      <TableCell className="font-medium">{profile.name}</TableCell>
      <TableCell>{profile.provider}</TableCell>
      <TableCell><div className="flex flex-col gap-1"><code className="text-xs">{profile.model}</code>
        <span className="text-xs text-muted-foreground">{isDecisionModel(profile.model) ? "Typed decisions · no reasoning effort" : `Reasoning: ${reasoningLabel(profile.reasoningEffort)}`}</span>
      </div></TableCell>
      <TableCell>
        <Badge variant="secondary">
          {profile.apiKeyAvailable ? "Key saved" : "No API key"}
        </Badge>
      </TableCell>
      <TableCell>
        {profile.state === "disabled" ? <Badge variant="destructive">Disabled</Badge> : profile.isActive ? <Badge>Default</Badge> : <Badge variant="outline">Enabled</Badge>}
      </TableCell>
      <TableCell>
        <div className="flex justify-end gap-2">
          <Button variant="outline" size="sm" nativeButton={false}
            render={<Link to={`/admin/settings/llms?test=${encodeURIComponent(profile.profileId)}`} />}>
            Test
          </Button>
          <Button variant="outline" size="sm" nativeButton={false}
            render={<Link to={`/admin/settings/llms?edit=${encodeURIComponent(profile.profileId)}`} />}>
            Edit
          </Button>
          {!profile.isActive && profile.state !== "disabled" && !isDecisionModel(profile.model) && <Form method="post">
            <input type="hidden" name="intent" value="activate" />
            <input type="hidden" name="profile_id" value={profile.profileId} />
            <Button type="submit" variant="secondary" size="sm">Use this LLM</Button>
          </Form>}
          {profile.state !== "disabled" && <Form method="post">
            <input type="hidden" name="intent" value="disable" />
            <input type="hidden" name="profile_id" value={profile.profileId} />
            <Button type="submit" variant="outline" size="sm">Disable</Button>
          </Form>}
          <Dialog>
            <DialogTrigger render={<Button variant="outline" size="sm" />}>Remove</DialogTrigger>
            <DialogContent>
              <DialogHeader><DialogTitle>Remove {profile.name}?</DialogTitle>
                <DialogDescription>This removes the model from configuration and requests cancellation of its active tasks. Completed results and task history are kept.</DialogDescription>
              </DialogHeader>
              <DialogFooter>
                <DialogClose render={<Button variant="outline" />}>Keep model</DialogClose>
                <Form method="post">
                  <input type="hidden" name="intent" value="archive" />
                  <input type="hidden" name="profile_id" value={profile.profileId} />
                  <Button variant="destructive" type="submit">Remove and stop tasks</Button>
                </Form>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
      </TableCell>
    </TableRow>
    {result && <TableRow><TableCell colSpan={6} className="whitespace-normal">
      <div role="status" aria-live="polite" aria-atomic="true" className="flex flex-wrap items-center gap-2">
        <Badge variant={result.ok ? "secondary" : "destructive"}>{result.ok ? "Test passed" : "Test failed"}</Badge>
        <span className="min-w-0 [overflow-wrap:anywhere]">{result.message}</span>
      </div>
    </TableCell></TableRow>}
  </>;
}

function LlmProfilesCard({ profiles }: { profiles: LlmProfile[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Configured LLMs</CardTitle>
        <CardDescription>
          Store several provider/model combinations and select a default profile. Permanent credential or model failures disable the affected configuration and stop its tasks. Temporary errors leave the model enabled. Test checks the saved endpoint, model and optional API key with a short text request, or a typed decision for Jev.
          Queue checks verify any additional vision or CAPTCHA capabilities before processing.
        </CardDescription>
      </CardHeader>
      <CardContent className="px-0">
        {profiles.length === 0 ? (
          <Empty className="min-h-48">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <BotIcon />
              </EmptyMedia>
              <EmptyTitle>No LLM profiles configured</EmptyTitle>
              <EmptyDescription>
                Choose Add new model to configure your first profile.
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Provider</TableHead>
                <TableHead>Model</TableHead>
                <TableHead>API key</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {profiles.map((profile) => (
                <LlmProfileRow key={`${profile.profileId}:${profile.revision}`} profile={profile} />
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  );
}

export function LlmProfileForm({
  editingProfile,
  submittedValues,
  error,
}: {
  editingProfile: LlmProfile | null;
  submittedValues: LlmSettingsFormValues | null;
  error: string;
}) {
  const [values, setValues] = useState(submittedValues ?? profileFormValues(editingProfile));
  const decisionModel = isDecisionModel(values.model);
  const efforts = reasoningOptions(values.model, values.baseUrl);
  const hasSavedKey = editingProfile?.profileId === values.profileId && editingProfile.apiKeyAvailable;

  const navigation = useNavigation();
  const saving = navigation.state !== "idle" && navigation.formData?.get("intent") === "save";
  return (
      <Form method="post" action={values.profileId ? `/admin/settings/llms?edit=${encodeURIComponent(values.profileId)}` : "/admin/settings/llms?add=yes"} className="flex min-h-0 flex-1 flex-col">
        <div className="min-h-0 flex-1 overflow-y-auto px-5 pb-5">

          {error ? (
            <Alert variant="destructive">
              <AlertTitle>Could not save LLM profile</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          ) : null}
          <input type="hidden" name="intent" value="save" />
          <input type="hidden" name="profile_id" value={values.profileId} />
          <FieldGroup className="grid gap-4 sm:grid-cols-2 [container-type:normal]">
            <Field className="sm:col-span-2">
              <FieldLabel htmlFor="llm-preset">Model preset</FieldLabel>
              <NativeSelect id="llm-preset" value={LLM_MODEL_PRESETS.find(preset => preset.model === values.model && preset.baseUrl === values.baseUrl)?.model ?? "custom"}
                onChange={event => {
                  const preset = LLM_MODEL_PRESETS.find(preset => preset.model === event.target.value);
                  if (preset) setValues({...values, ...preset, name: values.name && !LLM_MODEL_PRESETS.some(item => item.name === values.name) ? values.name : preset.name, reasoningEffort: ""});
                  else setValues({...values, model: "", reasoningEffort: ""});
                }}>
                <NativeSelectOption value="custom">Custom model</NativeSelectOption>
                {LLM_MODEL_PRESETS.map(preset => <NativeSelectOption key={preset.model} value={preset.model}>{preset.name}</NativeSelectOption>)}
              </NativeSelect>
              <FieldDescription>Fill provider, endpoint and model from a preset, or enter your own configuration.</FieldDescription>
            </Field>
            <Field>
              <FieldLabel htmlFor="llm-profile-name">Profile name</FieldLabel>
              <Input
                id="llm-profile-name"
                name="name"
                value={values.name}
                onChange={event => setValues({...values, name: event.target.value})}
                placeholder="DeepSeek production"
                required
              />
              <FieldDescription>
                A recognizable label used only in the backoffice.
              </FieldDescription>
            </Field>
            <Field>
              <FieldLabel htmlFor="llm-provider">Provider</FieldLabel>
              <Input
                id="llm-provider"
                name="provider"
                value={values.provider}
                onChange={event => setValues({...values, provider: event.target.value})}
                placeholder="DeepSeek"
                required
              />
            </Field>
            <Field>
              <FieldLabel htmlFor="llm-base-url">Base URL</FieldLabel>
              <Input
                id="llm-base-url"
                name="base_url"
                type="url"
                value={values.baseUrl}
                onChange={event => setValues({...values, baseUrl: event.target.value, reasoningEffort: ""})}
                placeholder="https://api.deepseek.com"
                required
              />
              <FieldDescription>
                The API base URL must be reachable from the processing service. For a local model, use its network address; localhost refers to the service host. Jev uses OpenRouter’s Decisions API.
              </FieldDescription>
            </Field>
            <Field>
              <FieldLabel htmlFor="llm-model">Model</FieldLabel>
              <Input
                id="llm-model"
                name="model"
                value={values.model}
                onChange={event => setValues({...values, model: event.target.value, reasoningEffort: ""})}
                placeholder="deepseek-flash"
                required
              />
            </Field>
            <Field className="sm:col-span-2">
              <FieldLabel htmlFor="llm-reasoning">Reasoning effort</FieldLabel>
              {decisionModel ? <>
                <input type="hidden" name="reasoning_effort" value="" />
                <p className="text-sm text-muted-foreground">Not applicable. Jev returns typed decisions and does not generate reasoning text.</p>
              </> : <NativeSelect id="llm-reasoning" name="reasoning_effort" value={values.reasoningEffort ?? ""}
                onChange={event => setValues({...values, reasoningEffort: event.target.value})}>
                {efforts.map(effort => <NativeSelectOption key={effort || "default"} value={effort}>{reasoningLabel(effort)}</NativeSelectOption>)}
              </NativeSelect>}
              <FieldDescription>{decisionModel ? "Save and test this decision model here. Text-generation tasks require a processing model." : "Saved with this model revision and used by verification and processing. Provider default leaves the effort unspecified; Off disables thinking. More reasoning can increase cost and latency."}</FieldDescription>
            </Field>
            <Field className="sm:col-span-2">
              <FieldLabel htmlFor="llm-api-key">
                API key (optional)
              </FieldLabel>
              <Input
                id="llm-api-key"
                name="api_key"
                type="password"
                placeholder={hasSavedKey ? "Leave blank to keep the saved key" : "Leave blank for models without authentication"}
                autoComplete="new-password"
              />
              <FieldDescription>
                {hasSavedKey
                  ? "Leave blank to keep the saved API key, or enter a replacement. Keys are encrypted before saving and are never shown."
                  : "Leave blank for local or other endpoints without authentication. If provided, the key is encrypted before saving and is never shown."}
              </FieldDescription>
            </Field>
          </FieldGroup>
        </div>
        <div className="flex shrink-0 justify-end gap-2 border-t p-4">
          <Button variant="outline" nativeButton={false} render={<Link to="/admin/settings/llms" />}>Cancel</Button>
          <Button type="submit" disabled={saving}>
            <BotIcon data-icon="inline-start" />
            {saving ? "Saving…" : decisionModel ? "Save decision model" : "Save and use this LLM"}
          </Button>
        </div>
      </Form>
  );
}

function LocalCodexCard({ enabled }: { enabled: boolean }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Local codex agent</CardTitle>
        <CardDescription>
          When enabled, ESEF enrichment launches can pick the locally running
          codex agent instead of a remote provider. No API key or model
          parameters apply — the agent runs on this machine.
        </CardDescription>
      </CardHeader>
      <Form method="post">
        <CardContent>
          <input type="hidden" name="intent" value="set_local_codex" />
          <label className="flex items-center gap-3 text-sm font-medium">
            <Switch name="local_codex" defaultChecked={enabled} />
            local_codex
          </label>
        </CardContent>
        <CardFooter className="justify-between">
          <Button
            variant="outline"
            nativeButton={false}
            render={<Link to="/admin/settings/llms/local" />}
          >
            Open local codex workspace →
          </Button>
          <Button type="submit" variant="secondary">
            Save local setting
          </Button>
        </CardFooter>
      </Form>
    </Card>
  );
}

export function LlmSettingsWorkspace({
  profiles,
  testingProfile = null, testPreview = null, testPreviewError = "",
  editingProfile,
  creating = false,
  submittedValues = null,
  error = "",
  saved = false,
  localCodexEnabled = false,
  initialTab = "remote",
  runs = [],
}: {
  profiles: LlmProfile[];
  testingProfile?: LlmProfile | null;
  testPreview?: LlmTestExchange | null;
  testPreviewError?: string;
  editingProfile: LlmProfile | null;
  creating?: boolean;
  submittedValues?: LlmSettingsFormValues | null;
  error?: string;
  saved?: boolean;
  localCodexEnabled?: boolean;
  initialTab?: "remote" | "local";
  runs?: (Awaited<ReturnType<typeof recentLlmRuns>>[number] & {runUrl?: string})[];
}) {
  const revalidator = useRevalidator();
  const navigate = useNavigate();
  const formOpen = creating || editingProfile !== null || Boolean(error && submittedValues);
  const pending = runs.some(run => run.pendingExternal > 0 || ['launching','queued','running'].includes(run.status));
  useEffect(() => {
    if (!pending) return;
    const timer = setInterval(() => { if (revalidator.state === 'idle') void revalidator.revalidate(); }, 10_000);
    return () => clearInterval(timer);
  }, [pending, revalidator]);
  const activeProfile = profiles.find((profile) => profile.isActive) ?? null;

  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-wrap items-start justify-between gap-3">
        <div className="flex flex-col gap-1">
        <div className="flex items-center gap-2">
          <h1 className="text-2xl font-semibold tracking-tight">LLM settings</h1>
          <Badge variant="outline">Global</Badge>
        </div>
        <p className="max-w-3xl text-sm text-muted-foreground">
          Configure the model endpoint used by backoffice processing workflows.
        </p>
        </div>
        <Button nativeButton={false} render={<Link to="/admin/settings/llms?add=yes" />}><PlusIcon data-icon="inline-start" />Add new model</Button>
      </header>

      <Alert>
        <KeyRoundIcon />
        <AlertTitle>API keys are encrypted in the settings database</AlertTitle>
        <AlertDescription>
          API keys are optional for local or unauthenticated endpoints. Saved keys are never shown;
          leave the key field blank when editing to keep the current key.
        </AlertDescription>
      </Alert>

      {saved ? (
        <Alert>
          <AlertTitle>LLM settings saved</AlertTitle>
          <AlertDescription>
            The model configuration was updated.
          </AlertDescription>
        </Alert>
      ) : null}

      <Tabs defaultValue={initialTab}>
        <TabsList><TabsTrigger value="remote">Models</TabsTrigger><TabsTrigger value="local">Local Codex</TabsTrigger></TabsList>
        <TabsContent value="remote" className="flex flex-col gap-6">
      <ActiveLlmCard profile={activeProfile} />
      <LlmProfilesCard profiles={profiles} />
      <Card>
        <CardHeader><CardTitle>Recent LLM tasks</CardTitle><CardDescription>The latest 30 launches using saved models. Stopping is confirmed by Dagster and the external service.</CardDescription></CardHeader>
        <CardContent>
          {runs.length === 0 ? <p>No tracked LLM tasks yet.</p> : <Table>
            <TableHeader><TableRow><TableHead>Task</TableHead><TableHead>Model</TableHead><TableHead>Status</TableHead><TableHead>External requests</TableHead></TableRow></TableHeader>
            <TableBody>{runs.map(run => <TableRow key={run.requestId}>
              <TableCell>{run.runId ? <a href={run.runUrl} target="_blank" rel="noreferrer">{run.job}</a> : run.job}<p className="text-xs text-muted-foreground">{run.createdAt}</p></TableCell>
              <TableCell>{run.models}</TableCell>
              <TableCell className="whitespace-normal"><Badge variant="outline">{run.stopRequestedAt && (run.pendingExternal > 0 || !['succeeded','failed','canceled','launch_failed'].includes(run.status)) ? "Stopping" : run.status}</Badge><p>{run.stopReason ?? run.lastError}</p></TableCell>
              <TableCell>{run.pendingExternal} awaiting confirmation</TableCell>
            </TableRow>)}</TableBody>
          </Table>}
        </CardContent>
      </Card>
        </TabsContent>
        <TabsContent value="local"><LocalCodexCard enabled={localCodexEnabled} /></TabsContent>
      </Tabs>
      {testPreviewError && !testingProfile && <Alert variant="destructive"><AlertTitle>Could not open model test</AlertTitle><AlertDescription>{testPreviewError}</AlertDescription></Alert>}
      {testingProfile && <LlmTestSheet key={`${testingProfile.profileId}:${testingProfile.revision}`} profile={testingProfile} preview={testPreview} error={testPreviewError} />}
      <Sheet open={formOpen} onOpenChange={open => {if (!open) void navigate("/admin/settings/llms");}}>
        <SheetContent side="right" className="gap-0 data-[side=right]:w-full data-[side=right]:sm:max-w-xl">
          <SheetHeader className="shrink-0 p-5 pr-12">
            <SheetTitle>{editingProfile || submittedValues?.profileId ? "Edit model" : "Add new model"}</SheetTitle>
            <SheetDescription>Configure a hosted or local model. API keys are optional for endpoints without authentication.</SheetDescription>
          </SheetHeader>
          {formOpen && <LlmProfileForm
            key={JSON.stringify([editingProfile?.profileId, editingProfile?.revision, submittedValues])}
            editingProfile={editingProfile}
            submittedValues={submittedValues}
            error={error}
          />}
        </SheetContent>
      </Sheet>
    </div>
  );
}
