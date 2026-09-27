import { useFetcher, useNavigate } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Sheet, SheetContent, SheetDescription, SheetFooter, SheetHeader, SheetTitle } from "~/components/ui/sheet";
import type { LlmTestExchange } from "~/lib/crawl-llm.server";
import type { LlmProfile } from "~/lib/llm-settings.server";

export interface LlmTestResult {
  profileId: string;
  ok: boolean;
  message: string;
  checkedAt: string;
  exchange: LlmTestExchange | null;
}

function responseText(body: string): string {
  try { return JSON.stringify(JSON.parse(body), null, 2); }
  catch { return body; }
}

export function LlmTestDetails({preview, result, testing, error}: {
  preview: LlmTestExchange | null; result: LlmTestResult | null; testing: boolean; error: string;
}) {
  const exchange = result?.exchange ?? preview;
  const request = exchange?.request ?? preview?.request;
  return <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-y-auto px-5 pb-5">
    {error && <Alert variant="destructive"><AlertTitle>Could not prepare test</AlertTitle><AlertDescription>{error}</AlertDescription></Alert>}
    <section className="flex flex-col gap-2" aria-label="Request JSON">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="font-medium">{result?.exchange?.request ? "Request sent" : "Request preview"}</h3>
        {request && <Badge variant="outline">{request.method}</Badge>}
      </div>
      {request ? <>
        <p className="break-all font-mono text-xs text-muted-foreground">{request.url}</p>
        <pre className="max-h-80 overflow-auto rounded-md border bg-muted/30 p-3 font-mono text-xs whitespace-pre-wrap [overflow-wrap:anywhere]">{JSON.stringify(request.body, null, 2)}</pre>
      </> : <p className="text-sm text-muted-foreground">Request preview is unavailable.</p>}
    </section>
    <section className="flex flex-col gap-2" aria-label="Full model response">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="font-medium">Full response</h3>
        {!testing && result && <div className="flex flex-wrap items-center gap-2">
          {exchange?.response && <Badge variant="outline">HTTP {exchange.response.status}</Badge>}
          {exchange?.elapsed_ms != null && <span className="text-xs text-muted-foreground">{(exchange.elapsed_ms / 1000).toFixed(2)} s</span>}
        </div>}
      </div>
      <div role="status" aria-live="polite" className="flex flex-col gap-2">
        {testing ? <p>Waiting for the model response…</p> : result ? <>
          <Badge className="self-start" variant={result.ok ? "secondary" : "destructive"}>{result.ok ? "Test passed" : "Test failed"}</Badge>
          <p className="text-sm [overflow-wrap:anywhere]">{result.message}</p>
        </> : <p className="text-sm text-muted-foreground">Run the test to see the complete response, including model output, usage, and any provider error.</p>}
      </div>
      {!testing && result && (exchange?.response ? <>
        <p className="text-xs text-muted-foreground">{exchange.response.content_type || "No content type returned"}</p>
        <pre className="overflow-auto rounded-md border bg-muted/30 p-3 font-mono text-xs whitespace-pre-wrap [overflow-wrap:anywhere]">{responseText(exchange.response.body)}</pre>
      </> : <p className="text-sm text-muted-foreground">No HTTP response was received.</p>)}
    </section>
  </div>;
}

export function LlmTestSheet({profile, preview, error}: {
  profile: LlmProfile; preview: LlmTestExchange | null; error: string;
}) {
  const navigate = useNavigate();
  const test = useFetcher<{testResult: LlmTestResult}>();
  const testing = test.state !== "idle";
  const result = test.data?.testResult?.profileId === profile.profileId ? test.data.testResult : null;
  return <Sheet open onOpenChange={open => {if (!open) void navigate("/admin/settings/llms");}}>
    <SheetContent side="right" className="gap-0 data-[side=right]:w-full data-[side=right]:sm:max-w-3xl">
      <SheetHeader className="shrink-0 p-5 pr-12">
        <SheetTitle>Test {profile.name}</SheetTitle>
        <SheetDescription>Saved revision {profile.revision} · {profile.model}. Previewing makes no model call. Credentials are hidden from this view.</SheetDescription>
      </SheetHeader>
      <LlmTestDetails preview={preview} result={result} testing={testing} error={error} />
      <SheetFooter className="shrink-0 border-t p-5">
        <p className="text-xs text-muted-foreground">A successful test enables this model. Credential or model errors disable it and request cancellation of dependent tasks.</p>
        <div className="flex justify-end gap-2">
          <Button variant="outline" onClick={() => void navigate("/admin/settings/llms")}>Close</Button>
          <test.Form method="post" action="/admin/settings/llms" aria-label={`Run test for ${profile.name}`}>
            <input type="hidden" name="intent" value="test" />
            <input type="hidden" name="profile_id" value={profile.profileId} />
            <input type="hidden" name="profile_revision" value={profile.revision} />
            <Button type="submit" disabled={testing || !preview?.request || Boolean(error)}>{testing ? "Testing…" : result ? "Run again" : "Run test"}</Button>
          </test.Form>
        </div>
      </SheetFooter>
    </SheetContent>
  </Sheet>;
}
