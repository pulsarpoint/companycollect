import { Form, Link, useFetcher } from "react-router";
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
  FieldError,
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
import { Switch } from "~/components/ui/switch";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "~/components/ui/tabs";
import type { LlmProfile } from "~/lib/llm-settings.server";

export interface LlmSettingsFormValues {
  profileId: string;
  name: string;
  provider: string;
  baseUrl: string;
  model: string;
}

function profileFormValues(profile: LlmProfile | null): LlmSettingsFormValues {
  return {
    profileId: profile?.profileId ?? "",
    name: profile?.name ?? "",
    provider: profile?.provider ?? "",
    baseUrl: profile?.baseUrl ?? "",
    model: profile?.model ?? "",
  };
}

function ActiveLlmCard({ profile }: { profile: LlmProfile | null }) {
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center gap-2">
          <CardTitle>Active LLM</CardTitle>
          {profile ? <Badge>In use</Badge> : <Badge variant="destructive">Not configured</Badge>}
        </div>
        <CardDescription>
          This profile will be used by backoffice LLM processing tasks.
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
                  variant={profile.apiKeyAvailable ? "secondary" : "destructive"}
                >
                  {profile.apiKeyAvailable ? "Saved" : "Missing"}
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
  const test = useFetcher<{
    testResult: {profileId: string; ok: boolean; message: string; checkedAt: string};
    values: null; error: string;
  }>();
  const testing = test.state !== "idle";
  const result = test.data?.testResult.profileId === profile.profileId ? test.data.testResult : null;

  return <>
    <TableRow>
      <TableCell className="font-medium">{profile.name}</TableCell>
      <TableCell>{profile.provider}</TableCell>
      <TableCell><code className="text-xs">{profile.model}</code></TableCell>
      <TableCell>
        <Badge variant={profile.apiKeyAvailable ? "secondary" : "destructive"}>
          {profile.apiKeyAvailable ? "Key saved" : "Key missing"}
        </Badge>
      </TableCell>
      <TableCell>
        {profile.isActive ? <Badge>Active</Badge> : <Badge variant="outline">Inactive</Badge>}
      </TableCell>
      <TableCell>
        <div className="flex justify-end gap-2">
          <test.Form method="post" action="/admin/settings/llms" aria-label={`Test ${profile.name}`}>
            <input type="hidden" name="intent" value="test" />
            <input type="hidden" name="profile_id" value={profile.profileId} />
            <Button type="submit" variant="outline" size="sm" disabled={testing}>
              {testing ? "Testing…" : "Test"}
            </Button>
          </test.Form>
          <Button variant="outline" size="sm" nativeButton={false} disabled={testing}
            render={<Link to={`/admin/settings/llms?edit=${encodeURIComponent(profile.profileId)}`} />}>
            Edit
          </Button>
          {!profile.isActive && <Form method="post">
            <input type="hidden" name="intent" value="activate" />
            <input type="hidden" name="profile_id" value={profile.profileId} />
            <Button type="submit" variant="secondary" size="sm" disabled={testing}>Use this LLM</Button>
          </Form>}
        </div>
      </TableCell>
    </TableRow>
    {(testing || result) && <TableRow><TableCell colSpan={6} className="whitespace-normal">
      <div role="status" aria-live="polite" aria-atomic="true" className="flex flex-wrap items-center gap-2">
        {testing ? <span>Testing the saved configuration for {profile.name}…</span> : result && <>
          <Badge variant={result.ok ? "secondary" : "destructive"}>{result.ok ? "Test passed" : "Test failed"}</Badge>
          <span className="min-w-0 [overflow-wrap:anywhere]">{result.message}</span>
        </>}
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
          Store several provider/model combinations and select one active
          profile. Test checks the saved endpoint, model and API key with a short text request.
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
                Add the first profile using the form below.
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
                <LlmProfileRow key={`${profile.profileId}:${profile.updatedAt}`} profile={profile} />
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  );
}

function LlmProfileForm({
  editingProfile,
  submittedValues,
  error,
}: {
  editingProfile: LlmProfile | null;
  submittedValues: LlmSettingsFormValues | null;
  error: string;
}) {
  const values = submittedValues ?? profileFormValues(editingProfile);
  const editing = values.profileId !== "";
  const hasSavedKey = editingProfile?.profileId === values.profileId && editingProfile.apiKeyAvailable;

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex flex-col gap-1.5">
            <CardTitle>{editing ? "Edit LLM profile" : "Add LLM profile"}</CardTitle>
            <CardDescription>
              Saving a profile also makes it the active LLM.
            </CardDescription>
          </div>
          {editing ? (
            <Button
              variant="outline"
              nativeButton={false}
              render={<Link to="/admin/settings/llms" />}
            >
              <PlusIcon data-icon="inline-start" />
              New profile
            </Button>
          ) : null}
        </div>
      </CardHeader>
      <Form method="post">
        <CardContent className="flex flex-col gap-5">
          {error ? (
            <Alert variant="destructive">
              <AlertTitle>Could not save LLM profile</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          ) : null}
          <input type="hidden" name="intent" value="save" />
          <input type="hidden" name="profile_id" value={values.profileId} />
          <FieldGroup className="grid md:grid-cols-2">
            <Field>
              <FieldLabel htmlFor="llm-profile-name">Profile name</FieldLabel>
              <Input
                id="llm-profile-name"
                name="name"
                defaultValue={values.name}
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
                defaultValue={values.provider}
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
                defaultValue={values.baseUrl}
                placeholder="https://api.deepseek.com"
                required
              />
              <FieldDescription>
                The OpenAI-compatible API endpoint; no secret values belong in
                this field.
              </FieldDescription>
            </Field>
            <Field>
              <FieldLabel htmlFor="llm-model">Model</FieldLabel>
              <Input
                id="llm-model"
                name="model"
                defaultValue={values.model}
                placeholder="deepseek-v4-flash"
                required
              />
            </Field>
            <Field className="md:col-span-2" data-invalid={Boolean(error)}>
              <FieldLabel htmlFor="llm-api-key">
                API key
              </FieldLabel>
              <Input
                id="llm-api-key"
                name="api_key"
                type="password"
                placeholder={hasSavedKey ? "Leave blank to keep the saved key" : "Enter API key"}
                aria-invalid={Boolean(error)}
                autoComplete="new-password"
                required={!hasSavedKey}
              />
              <FieldDescription>
                {hasSavedKey
                  ? "Leave blank to keep the saved API key, or enter a replacement. Keys are encrypted before saving and are never shown."
                  : "Enter the provider API key. It is encrypted before saving and is never shown."}
              </FieldDescription>
              <FieldError>{error}</FieldError>
            </Field>
          </FieldGroup>
        </CardContent>
        <CardFooter className="justify-end">
          <Button type="submit">
            <BotIcon data-icon="inline-start" />
            Save and use this LLM
          </Button>
        </CardFooter>
      </Form>
    </Card>
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
  editingProfile,
  submittedValues = null,
  error = "",
  saved = false,
  localCodexEnabled = false,
  initialTab = "remote",
}: {
  profiles: LlmProfile[];
  editingProfile: LlmProfile | null;
  submittedValues?: LlmSettingsFormValues | null;
  error?: string;
  saved?: boolean;
  localCodexEnabled?: boolean;
  initialTab?: "remote" | "local";
}) {
  const activeProfile = profiles.find((profile) => profile.isActive) ?? null;

  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-1">
        <div className="flex items-center gap-2">
          <h1 className="text-2xl font-semibold tracking-tight">LLM settings</h1>
          <Badge variant="outline">Global</Badge>
        </div>
        <p className="max-w-3xl text-sm text-muted-foreground">
          Configure the model endpoint used by backoffice processing workflows.
        </p>
      </header>

      <Alert>
        <KeyRoundIcon />
        <AlertTitle>API keys are encrypted in the settings database</AlertTitle>
        <AlertDescription>
          Enter a key when adding a profile. Saved keys are never shown;
          leave the key field blank when editing to keep the current key.
        </AlertDescription>
      </Alert>

      {saved ? (
        <Alert>
          <AlertTitle>LLM settings saved</AlertTitle>
          <AlertDescription>
            The selected profile is now the active backoffice LLM.
          </AlertDescription>
        </Alert>
      ) : null}

      <ActiveLlmCard profile={activeProfile} />
      <LlmProfilesCard profiles={profiles} />
      <Tabs defaultValue={initialTab}>
        <TabsList>
          <TabsTrigger value="remote">Remote</TabsTrigger>
          <TabsTrigger value="local">Local</TabsTrigger>
        </TabsList>
        <TabsContent value="remote">
          <LlmProfileForm
            editingProfile={editingProfile}
            submittedValues={submittedValues}
            error={error}
          />
        </TabsContent>
        <TabsContent value="local">
          <LocalCodexCard enabled={localCodexEnabled} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
