import { BrowserDesktop } from "~/components/admin/browser-desktop";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import type { ChallengeAgentResult, CrawlAttempt } from "~/lib/crawler";
import { isCrawlWaiting } from "~/lib/crawler";

export function CrawlBrowser({attempt, browserUrl, onVerify, onResume, onCancel, onReconnect, busy, onAgent, agentBusy = false, agentResult, agentError}: {
  attempt: CrawlAttempt;
  browserUrl: string | null;
  onVerify: () => void;
  onResume: () => void;
  onCancel: () => void;
  onReconnect: () => void;
  busy: boolean;
  onAgent?: () => void;
  agentBusy?: boolean;
  agentResult?: ChallengeAgentResult | null;
  agentError?: string | null;
}) {
  const waiting = isCrawlWaiting(attempt);
  const collectedPages = attempt.collected_pages || attempt.s3_event?.page_count || 0;
  agentBusy = agentBusy || attempt.challenge_agent_running === true;
  const agentResults = agentResult ? [agentResult]
    : attempt.challenge_agent_results?.length ? attempt.challenge_agent_results
    : attempt.challenge_agent_result ? [attempt.challenge_agent_result] : [];

  return <section className="flex flex-col gap-3 rounded-lg border p-4" aria-label="Interactive crawler browser">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div><h2 className="font-semibold">{attempt.domain} · Browser assistance</h2>
        <p className="text-sm text-muted-foreground">{waiting ? "Waiting for verification" : attempt.state === "failed" && collectedPages > 0 ? `Crawl stopped with ${collectedPages} pages saved` : `Crawl ${attempt.state.replaceAll("_", " ")}`}</p>
      </div>
      <div className="flex gap-2">
        {waiting && attempt.verification_available && <Button onClick={onVerify} disabled={busy}>Start verification</Button>}
        {waiting && attempt.browser_available && <Button variant="outline" onClick={onReconnect} disabled={busy}>Reconnect browser</Button>}
        {waiting && attempt.browser_available && onAgent && <Button variant="outline" onClick={onAgent} disabled={busy || agentBusy}>{agentBusy ? "Agent running…" : "Try CAPTCHA agent"}</Button>}
        <Button onClick={onResume} disabled={busy || agentBusy || !waiting || !attempt.browser_available}>Resume crawl</Button>
        <Button variant="outline" onClick={onCancel} disabled={busy || ["completed", "failed", "cancelled"].includes(attempt.state)}>Cancel</Button>
      </div>
    </div>
    <p className="text-sm break-all">{attempt.current_url || attempt.url}</p>
    <Alert><AlertDescription>
      {attempt.reason || "Preparing the browser."}
      {attempt.assistance_deadline && ` Session expires at ${new Date(attempt.assistance_deadline).toLocaleTimeString()}.`}
    </AlertDescription></Alert>
    {waiting && attempt.browser_available && onAgent && <p className="text-sm text-muted-foreground">The agent sends screenshots of this tab to {attempt.challenge_agent_model === "z-ai/glm-5.3-flash" ? "GLM-5.3 Flash through OpenRouter" : "DeepSeek V4.1 Flash"} and can interact for up to two minutes. {attempt.challenge_agent_running ? "The crawler will check access and continue automatically if verification succeeds." : "The crawl stays paused until you resume it."}</p>}
    {Boolean(attempt.challenge_agent_max_runs) && <p className="text-sm text-muted-foreground">{attempt.challenge_agent_results?.length || 0} / {attempt.challenge_agent_max_runs} automatic agent runs used{attempt.challenge_agent_budget_exhausted ? " · Budget exhausted" : ""}</p>}
    {agentBusy && <Alert><AlertDescription>Agent is working. Wait before interacting with the browser. You can still cancel the crawl.</AlertDescription></Alert>}
    {agentError && <Alert variant="destructive"><AlertDescription>{agentError}</AlertDescription></Alert>}
    {!agentBusy && agentResults.map((agentResult, index) => <Alert key={agentResult.runId ?? index}><AlertDescription>
      {agentResult.pageUrl && <p className="break-all">Agent run {index + 1} · {agentResult.pageUrl}</p>}
      <p>{agentResult.trigger === "automatic" ? (agentResult.accessVerified ? "Automatic agent completed. The crawler verified access and continued." : "Automatic agent finished without verified access.") : agentResult.state === "appears_clear" ? (waiting ? "The agent reports that verification appears complete. Resume the crawl to check access." : "The agent reported that verification appeared complete.") : `Agent stopped: ${agentResult.state.replaceAll("_", " ")}.`} {agentResult.reason}</p>
      <p>{agentResult.model && `${agentResult.model} · `}{agentResult.steps.length} steps · {agentResult.elapsedSeconds}s · {agentResult.usage.prompt_tokens + agentResult.usage.completion_tokens} tokens</p>
      <details><summary>Agent actions</summary><ol className="list-decimal pl-5">{agentResult.steps.map(step => <li key={step.number}>{step.action?.action || "observe"} · {step.state}{step.action?.reason && ` · ${step.action.reason}`}</li>)}</ol>{agentResult.runId && <p className="text-xs">Run {agentResult.runId}</p>}</details>
    </AlertDescription></Alert>)}
    {waiting && attempt.browser_available && <BrowserDesktop url={browserUrl} />}
  </section>;
}
