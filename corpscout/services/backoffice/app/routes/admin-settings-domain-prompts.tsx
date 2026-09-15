import { Form, Link, redirect, useNavigation } from "react-router";
import type { Route } from "./+types/admin-settings-domain-prompts";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Textarea } from "~/components/ui/textarea";
import { listDomainPrompts, saveDomainPrompt, DomainPromptValidationError } from "~/lib/domain-prompts.server";

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const prompts = listDomainPrompts();
  const editing = prompts.find((prompt) => prompt.promptId === url.searchParams.get("edit")) ?? null;
  return { prompts, editing, saved: url.searchParams.get("saved") === "yes" };
}

export async function action({ request }: Route.ActionArgs) {
  const form = await request.formData();
  const values = {
    promptId: String(form.get("prompt_id") ?? ""),
    name: String(form.get("name") ?? ""),
    systemPrompt: String(form.get("system_prompt") ?? ""),
    revision: Number(form.get("revision") ?? 0),
  };
  try {
    const id = saveDomainPrompt(values);
    return redirect(`/admin/settings/domain-prompts?edit=${encodeURIComponent(id)}&saved=yes`);
  } catch (error) {
    if (error instanceof DomainPromptValidationError) return { error: error.message, values };
    throw error;
  }
}

export function meta() { return [{ title: "Domain prompts | CompanyCollect" }]; }

export default function DomainPrompts({ loaderData, actionData }: Route.ComponentProps) {
  const { prompts, editing, saved } = loaderData;
  const values = actionData?.values ?? editing;
  const busy = useNavigation().state !== "idle";
  return <main className="flex flex-col gap-6 p-6">
    <div><h1 className="text-2xl font-semibold">Domain prompts</h1>
      <p className="mt-1 text-sm text-muted-foreground">Saved instructions for verifying company–domain associations from stored evidence. Choose a prompt and an LLM for the Full processing action.</p></div>
    <div className="grid gap-8 lg:grid-cols-[280px_minmax(0,1fr)]">
      <nav aria-label="Saved Domain prompts" className="flex flex-col gap-2">
        {prompts.map((prompt) => <Link key={prompt.promptId} to={`?edit=${encodeURIComponent(prompt.promptId)}`}
          className={`rounded-md border p-3 text-sm ${editing?.promptId === prompt.promptId ? "bg-muted" : "hover:bg-muted/50"}`}>
          <span className="font-medium">{prompt.name}</span><span className="mt-1 block text-xs text-muted-foreground">Revision {prompt.revision}</span>
        </Link>)}
        <Button variant="outline" render={<Link to="/admin/settings/domain-prompts" />}>New prompt</Button>
      </nav>
      <Form method="post" key={`${editing?.promptId ?? "new"}:${editing?.revision ?? 0}`} className="flex max-w-4xl flex-col gap-4">
        <h2 className="text-lg font-medium">{editing ? "Edit prompt" : "New prompt"}</h2>
        {actionData?.error && <p role="alert" className="text-sm text-destructive">{actionData.error}</p>}
        {saved && !actionData?.error && <p role="status" className="text-sm">Prompt saved. Queued runs keep their original prompt.</p>}
        <input type="hidden" name="prompt_id" value={values?.promptId ?? ""} />
        <input type="hidden" name="revision" value={values?.revision ?? 0} />
        <div className="space-y-2"><Label htmlFor="prompt-name">Name</Label>
          <Input id="prompt-name" name="name" required maxLength={120} defaultValue={values?.name ?? ""} /></div>
        <div className="space-y-2"><Label htmlFor="system-prompt">Verification instructions</Label>
          <p className="text-xs text-muted-foreground">The model receives company identity and evidence with IDs such as e0. It returns a verdict (connected, not_connected or uncertain), confidence, reason and evidence_ids. Verification runs only for uncertain or conflicting associations. Reviewer decisions take priority.</p>
          <Textarea id="system-prompt" name="system_prompt" required maxLength={30_000} defaultValue={values?.systemPrompt ?? ""}
            className="min-h-80 max-h-[60vh] font-mono text-sm leading-relaxed" /></div>
        <div><Button type="submit" disabled={busy}>{busy ? "Saving…" : "Save prompt"}</Button></div>
      </Form>
    </div>
  </main>;
}
