import { Form, Link, redirect, useNavigation } from "react-router";
import type { Route } from "./+types/admin-settings-llms-local";
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
import { Switch } from "~/components/ui/switch";
import { Textarea } from "~/components/ui/textarea";
import {
  CodexAgentError,
  deleteCodexThread,
  getCodexThreadHistory,
  listCodexThreads,
  runCodexTurn,
  type CodexMessage,
  type CodexThreadSummary,
} from "~/lib/codex-agent.server";
import {
  isLocalCodexEnabled,
  setLocalCodexEnabled,
} from "~/lib/llm-settings.server";

// Threads started from this page carry it as their origin marker; the
// thread-listing API is page-scoped for exactly this kind of grouping.
const PAGE = "/admin/settings/llms/local";

export async function loader({ request }: Route.LoaderArgs) {
  const threadId =
    new URL(request.url).searchParams.get("thread")?.trim() ?? "";
  return {
    localCodexEnabled: isLocalCodexEnabled(),
    threads: listCodexThreads(PAGE),
    history: threadId === "" ? null : getCodexThreadHistory(threadId),
  };
}

function formValue(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value.trim() : "";
}

export async function action({ request }: Route.ActionArgs) {
  const form = await request.formData();
  const intent = formValue(form, "intent");
  try {
    if (intent === "set_local_codex") {
      setLocalCodexEnabled(formValue(form, "local_codex") === "on");
      return redirect(PAGE);
    }
    if (intent === "new-thread") {
      const result = await runCodexTurn({
        page: PAGE,
        input: formValue(form, "input"),
      });
      return redirect(`${PAGE}?thread=${encodeURIComponent(result.threadId)}`);
    }
    if (intent === "send") {
      const threadId = formValue(form, "thread_id");
      await runCodexTurn({
        page: PAGE,
        input: formValue(form, "input"),
        threadId,
      });
      return redirect(`${PAGE}?thread=${encodeURIComponent(threadId)}`);
    }
    if (intent === "remove") {
      deleteCodexThread(formValue(form, "thread_id"));
      return redirect(PAGE);
    }
    return { error: "Unknown local codex action." };
  } catch (error) {
    if (error instanceof CodexAgentError) return { error: error.message };
    throw error;
  }
}

export function meta() {
  return [{ title: "Local codex | CompanyCollect" }];
}

function ToggleCard({ enabled }: { enabled: boolean }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Local codex agent</CardTitle>
        <CardDescription>
          Conversations run through the codex CLI installed on this machine, in
          a read-only scratch workspace.
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
        <CardFooter className="justify-end">
          <Button type="submit" variant="secondary">
            Save local setting
          </Button>
        </CardFooter>
      </Form>
    </Card>
  );
}

function ThreadList({
  threads,
  selectedThreadId,
}: {
  threads: CodexThreadSummary[];
  selectedThreadId: string;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Conversations</CardTitle>
        <CardDescription>
          Threads started from this page. Select one to resume it.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-2">
        {threads.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            No conversations yet.
          </p>
        ) : (
          threads.map((thread) => (
            <div
              key={thread.threadId}
              className="flex items-center justify-between gap-2 border-b pb-2 last:border-b-0"
            >
              <Link
                className={
                  thread.threadId === selectedThreadId
                    ? "font-medium underline"
                    : "underline"
                }
                to={`?thread=${encodeURIComponent(thread.threadId)}`}
              >
                {thread.title}
              </Link>
              <Form method="post">
                <input type="hidden" name="intent" value="remove" />
                <input
                  type="hidden"
                  name="thread_id"
                  value={thread.threadId}
                />
                <Button type="submit" variant="outline" size="sm">
                  Remove
                </Button>
              </Form>
            </div>
          ))
        )}
      </CardContent>
    </Card>
  );
}

function MessageHistory({ messages }: { messages: CodexMessage[] }) {
  return (
    <div className="flex flex-col gap-3">
      {messages.map((message) => (
        <div
          key={message.messageId}
          className={
            message.role === "user"
              ? "ml-auto max-w-[85%] rounded-lg bg-primary/10 p-3 text-sm"
              : "mr-auto max-w-[85%] rounded-lg border p-3 text-sm"
          }
        >
          <Badge variant="outline" className="mb-1">
            {message.role}
          </Badge>
          <p className="whitespace-pre-wrap">{message.content}</p>
        </div>
      ))}
    </div>
  );
}

export function LocalCodexWorkspaceView({
  localCodexEnabled,
  threads,
  history,
  error,
}: {
  localCodexEnabled: boolean;
  threads: CodexThreadSummary[];
  history: { thread: CodexThreadSummary; messages: CodexMessage[] } | null;
  error: string;
}) {
  // A codex turn runs inside the submit; without feedback the operator
  // double-sends and hits the per-conversation lock. Show the submitted
  // message optimistically and hold the Send button until the turn lands.
  const navigation = useNavigation();
  const pendingIntent = navigation.formData?.get("intent");
  const thinking =
    navigation.state !== "idle" &&
    (pendingIntent === "send" || pendingIntent === "new-thread");
  const pendingInputValue = navigation.formData?.get("input");
  const pendingInput =
    thinking && typeof pendingInputValue === "string" ? pendingInputValue : "";

  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-1">
        <div className="flex items-center gap-2">
          <h1 className="text-2xl font-semibold tracking-tight">
            Local codex
          </h1>
          <Badge variant="outline">Demo</Badge>
        </div>
        <p className="max-w-3xl text-sm text-muted-foreground">
          A demo workspace for codex threads triggered from the backoffice.
        </p>
      </header>

      <ToggleCard enabled={localCodexEnabled} />

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Codex turn failed</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {!localCodexEnabled ? (
        <Alert>
          <AlertTitle>Enable local codex</AlertTitle>
          <AlertDescription>
            Turn the local_codex switch on to start conversations with the
            local agent.
          </AlertDescription>
        </Alert>
      ) : (
        <div className="grid gap-6 lg:grid-cols-[minmax(16rem,1fr)_2fr]">
          <ThreadList
            threads={threads}
            selectedThreadId={history?.thread.threadId ?? ""}
          />
          <Card>
            <CardHeader>
              <CardTitle>
                {history ? history.thread.title : "New conversation"}
              </CardTitle>
              {history ? (
                <CardDescription>
                  Resumed thread {history.thread.threadId}
                </CardDescription>
              ) : (
                <CardDescription>
                  Sending a message starts a fresh codex thread.
                </CardDescription>
              )}
            </CardHeader>
            <CardContent className="flex flex-col gap-4">
              {history ? <MessageHistory messages={history.messages} /> : null}
              {thinking ? (
                <div className="flex flex-col gap-3">
                  {pendingInput ? (
                    <div className="ml-auto max-w-[85%] rounded-lg bg-primary/10 p-3 text-sm opacity-70">
                      <Badge variant="outline" className="mb-1">
                        user
                      </Badge>
                      <p className="whitespace-pre-wrap">{pendingInput}</p>
                    </div>
                  ) : null}
                  <p className="mr-auto animate-pulse text-sm text-muted-foreground">
                    Codex is thinking…
                  </p>
                </div>
              ) : null}
              <Form method="post" className="flex flex-col gap-3">
                <input
                  type="hidden"
                  name="intent"
                  value={history ? "send" : "new-thread"}
                />
                {history ? (
                  <input
                    type="hidden"
                    name="thread_id"
                    value={history.thread.threadId}
                  />
                ) : null}
                <Textarea
                  name="input"
                  placeholder="Message the local codex agent…"
                  required
                  rows={3}
                />
                <div className="flex justify-end gap-2">
                  {history ? (
                    <Button
                      variant="outline"
                      nativeButton={false}
                      render={<Link to={PAGE} />}
                    >
                      New conversation
                    </Button>
                  ) : null}
                  <Button type="submit" disabled={thinking}>
                    {thinking ? "Thinking…" : "Send"}
                  </Button>
                </div>
              </Form>
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}

export default function AdminLocalCodex({
  loaderData,
  actionData,
}: Route.ComponentProps) {
  return (
    <LocalCodexWorkspaceView
      localCodexEnabled={loaderData.localCodexEnabled}
      threads={loaderData.threads}
      history={loaderData.history}
      error={actionData?.error ?? ""}
    />
  );
}
