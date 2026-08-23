import { Form, Link } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Checkbox } from "~/components/ui/checkbox";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import type { InstigatorStates } from "~/lib/dagster.server";
import type { SeCompanyInfoPipelineStats } from "~/lib/se-company-info-pipeline.server";
import {
  artifactSelectItems,
  INFO_ARTIFACT_SOURCES,
  MAX_COMPANIES,
  MAX_CONCURRENCY,
  MIN_COMPANIES,
  MIN_CONCURRENCY,
  profileSelectItems,
  type PipelineConfirmation,
  type PipelineProfileOption,
} from "~/lib/se-company-info-pipeline";

const nf = new Intl.NumberFormat("en-US");

export interface PipelineRunRow {
  runId: string;
  status: string;
  jobName: string;
  startTime: number | null;
  endTime: number | null;
  tags: Record<string, string>;
  url: string | null;
  /** Integer metadata from the run's materialization of se_company_info. */
  numbers: Record<string, number>;
}

export interface PipelineLaunched {
  runId: string;
  url: string | null;
  job: string;
}

/** Dagster reports seconds since the epoch as a float. */
function instant(seconds: number | null): string {
  if (seconds === null) return "—";
  return new Date(seconds * 1000).toISOString().replace("T", " ").slice(0, 19);
}

/** Finished runs only. A running run's duration would need the clock, and
 * reading it during render makes the markup differ between server and client
 * (a hydration mismatch) and go stale the moment it is painted. */
function duration(run: PipelineRunRow): string {
  if (run.startTime === null) return "—";
  if (run.endTime === null) return "running";
  const total = Math.max(0, Math.round(run.endTime - run.startTime));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

function statusVariant(status: string): "default" | "secondary" | "destructive" | "outline" {
  if (status === "SUCCESS") return "default";
  if (status === "FAILURE" || status === "CANCELED") return "destructive";
  if (status === "STARTED" || status === "STARTING" || status === "QUEUED") return "secondary";
  return "outline";
}

function count(run: PipelineRunRow, label: string): string {
  const value = run.numbers[label];
  return value === undefined ? "—" : nf.format(value);
}

function Stat({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-muted-foreground text-xs">{label}</span>
      <span className="text-2xl font-semibold tabular-nums">{value}</span>
      {hint ? <span className="text-muted-foreground text-xs">{hint}</span> : null}
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <Label className="text-xs font-medium">{label}</Label>
      {children}
    </div>
  );
}

function InstigatorBadge({
  name,
  status,
  detail,
}: {
  name: string;
  status: string;
  detail?: string;
}) {
  return (
    <Badge variant={status === "RUNNING" ? "default" : "outline"}>
      {name}
      <span className="ml-1 opacity-80">{status.toLowerCase()}</span>
      {detail ? <span className="ml-1 opacity-60">{detail}</span> : null}
    </Badge>
  );
}

function ProfileSelect({ profiles }: { profiles: PipelineProfileOption[] }) {
  const preselected = profiles.find((profile) => profile.isActive) ?? profiles[0];
  return (
    <Field label="LLM profile">
      {/* `items` is what Base UI renders the trigger from -- without it the
        * trigger shows the chosen value, which for a profile is a UUID. */}
      <Select
        items={profileSelectItems(profiles)}
        name="profile_id"
        defaultValue={preselected?.profileId ?? ""}
      >
        <SelectTrigger className="w-56" size="sm">
          <SelectValue placeholder="No profile configured" />
        </SelectTrigger>
        <SelectContent>
          {profiles.map((profile) => (
            <SelectItem key={profile.profileId} value={profile.profileId}>
              {`${profile.name} — ${profile.model}`}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </Field>
  );
}

function ConcurrencyField() {
  return (
    <Field label="Calls at once">
      <Input
        name="concurrency"
        type="number"
        min={MIN_CONCURRENCY}
        max={MAX_CONCURRENCY}
        defaultValue={MIN_CONCURRENCY}
        className="w-24"
      />
    </Field>
  );
}

function MaxCompaniesField({ defaultValue }: { defaultValue: number }) {
  return (
    <Field label="Stop after">
      <Input
        name="max_companies"
        type="number"
        min={MIN_COMPANIES}
        max={MAX_COMPANIES}
        defaultValue={defaultValue}
        className="w-32"
      />
    </Field>
  );
}

function ConfirmationPanel({
  confirmation,
}: {
  confirmation: PipelineConfirmation;
}) {
  return (
    <Alert>
      <AlertTitle>{confirmation.title} — confirm</AlertTitle>
      <AlertDescription>
        <ul className="list-disc pl-4">
          {confirmation.lines.map((line) => (
            <li key={line}>{line}</li>
          ))}
        </ul>
        <Form method="post" className="mt-3 flex items-center gap-2">
          <input type="hidden" name="intent" value={confirmation.intent} />
          {Object.entries(confirmation.fields).map(([name, value]) => (
            <input key={name} type="hidden" name={name} value={value} />
          ))}
          <Button type="submit" size="sm">
            Launch this run
          </Button>
          <span className="text-muted-foreground text-xs">
            Nothing has been launched yet.
          </span>
        </Form>
      </AlertDescription>
    </Alert>
  );
}

export function SeCompanyInfoPipeline({
  stats,
  statsError,
  profiles,
  runs,
  instigators,
  dagsterError,
  confirmation,
  launched,
  error,
}: {
  stats: SeCompanyInfoPipelineStats | null;
  statsError: string;
  profiles: PipelineProfileOption[];
  runs: PipelineRunRow[];
  instigators: InstigatorStates | null;
  dagsterError: string;
  confirmation: PipelineConfirmation | null;
  launched: PipelineLaunched | null;
  error: string;
}) {
  const mismatched = profiles.filter(
    (profile) => profile.apiKeyEnvironmentVariable !== profile.dagsterApiKeyVariable,
  );

  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold tracking-tight">Pipeline</h1>
        <p className="text-muted-foreground text-sm">
          What a se_company_info run would do right now, what has run recently,
          and the three runs this page can start. Every launch from here sends
          <code className="mx-1">execute: true</code> and an explicit model
          profile — a Materialize click in the Dagster UI does not, and is a
          preview that writes nothing.
        </p>
        <div className="flex flex-wrap items-center gap-2">
          {instigators === null ? (
            <Badge variant="outline">automation status unavailable</Badge>
          ) : (
            <>
              {instigators.schedules.map((schedule) => (
                <InstigatorBadge
                  key={schedule.name}
                  name={schedule.name}
                  status={schedule.status}
                  detail={schedule.cronSchedule}
                />
              ))}
              {instigators.sensors.map((sensor) => (
                <InstigatorBadge key={sensor.name} name={sensor.name} status={sensor.status} />
              ))}
            </>
          )}
        </div>
      </header>

      {dagsterError ? (
        <Alert variant="destructive">
          <AlertTitle>Dagster is unreachable</AlertTitle>
          <AlertDescription>
            {dagsterError} The counts below still come from ClickHouse; launching
            a run will fail until this is fixed.
          </AlertDescription>
        </Alert>
      ) : null}

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>That did not run</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {launched ? (
        <Alert>
          <AlertTitle>Launched on {launched.job}</AlertTitle>
          <AlertDescription>
            Run{" "}
            {launched.url ? (
              <a
                className="font-mono underline underline-offset-2"
                href={launched.url}
                target="_blank"
                rel="noreferrer"
              >
                {launched.runId}
              </a>
            ) : (
              <span className="font-mono">{launched.runId}</span>
            )}{" "}
            is queued. Reload this page to see it in the runs table.
          </AlertDescription>
        </Alert>
      ) : null}

      {confirmation ? <ConfirmationPanel confirmation={confirmation} /> : null}

      {stats === null ? (
        <Card>
          <CardHeader>
            <CardTitle>Selection counts unavailable</CardTitle>
            <CardDescription>
              ClickHouse did not answer, so this page cannot say what a run would
              select — and the actions below refuse to confirm anything until it
              does.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-muted-foreground text-sm">{statsError}</p>
          </CardContent>
        </Card>
      ) : (
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <Card>
          <CardHeader>
            <CardTitle>Changed companies</CardTitle>
            <CardDescription>What a model-on resolve run selects.</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            <Stat
              label="Selected"
              value={nf.format(stats.selection.changedCount)}
              hint={`${nf.format(stats.selection.changedWithoutModelCount)} without the model term`}
            />
            <div className="flex flex-wrap gap-1.5 text-xs">
              <Badge variant="secondary">
                never published
                <span className="ml-1 tabular-nums">
                  {nf.format(stats.selection.neverPublishedCount)}
                </span>
              </Badge>
              {INFO_ARTIFACT_SOURCES.map((source) => (
                <Badge key={source} variant="secondary">
                  new {source}
                  <span className="ml-1 tabular-nums">
                    {nf.format(stats.selection.newEvidenceCounts[source])}
                  </span>
                </Badge>
              ))}
              <Badge variant="secondary">
                ledger
                <span className="ml-1 tabular-nums">
                  {nf.format(stats.selection.ledgerPendingCount)}
                </span>
              </Badge>
            </div>
            <p className="text-muted-foreground text-xs">
              Reasons overlap: a never-published company also has evidence newer
              than its epoch resolution.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Model work</CardTitle>
            <CardDescription>Companies that enter the model step.</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            <Stat
              label="Would call the model"
              value={nf.format(stats.selection.wouldCallModelCount)}
              hint="Selected and last published with several description sources"
            />
            <Stat
              label="Still owed a description"
              value={nf.format(stats.selection.pendingModelCount)}
              hint="What the model pass selects"
            />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Artifacts</CardTitle>
            <CardDescription>Newest evidence per source.</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-2 text-sm">
            {stats.artifacts.map((artifact) => (
              <div key={artifact.source} className="flex items-baseline justify-between gap-2">
                <span className="font-medium">{artifact.source}</span>
                <span className="text-muted-foreground text-xs">
                  {artifact.latestObservedAt} · {nf.format(artifact.rowCount)} rows
                </span>
              </div>
            ))}
            <p className="text-muted-foreground text-xs">
              {nf.format(stats.selection.companyCount)} companies carry at least one
              artifact row.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Observed model cost</CardTitle>
            <CardDescription>Averages per stored call.</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-2 text-sm">
            {stats.models.length === 0 ? (
              <span className="text-muted-foreground text-xs">
                No stored observations yet.
              </span>
            ) : (
              stats.models.map((model) => (
                <div key={model.modelName} className="flex flex-col gap-0.5">
                  <span className="font-medium">{model.modelName}</span>
                  <span className="text-muted-foreground text-xs tabular-nums">
                    {nf.format(model.callCount)} calls · {nf.format(model.promptTokens)} prompt ·{" "}
                    {nf.format(model.completionTokens)} completion
                  </span>
                </div>
              ))
            )}
          </CardContent>
        </Card>
      </div>
      )}

      {mismatched.length > 0 ? (
        <Alert variant="destructive">
          <AlertTitle>Profile key variable will not be read</AlertTitle>
          <AlertDescription>
            {mismatched
              .map((profile) =>
                profile.dagsterApiKeyVariable === ""
                  ? `provider "${profile.provider}" (${profile.name}) does not name an environment variable at all, so no key can be read for it`
                  : `${profile.name} stores ${profile.apiKeyEnvironmentVariable}, but the Dagster host reads ${profile.dagsterApiKeyVariable} for provider "${profile.provider}"`,
              )
              .join("; ")}
            . Rename the variable on the Dagster host or the profile at{" "}
            <Link className="underline underline-offset-2" to="/admin/settings/llms">
              LLM settings
            </Link>
            .
          </AlertDescription>
        </Alert>
      ) : null}

      <div className="grid gap-4 lg:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle>Re-resolve changed companies</CardTitle>
            <CardDescription>
              Everything the ordinary run picks up: new evidence, new ledger rows,
              never-published companies.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Form method="post" className="flex flex-wrap items-end gap-3">
              <input type="hidden" name="intent" value="confirm-resolve" />
              <MaxCompaniesField defaultValue={1000} />
              <label className="flex items-center gap-2 pb-1.5 text-sm">
                <Checkbox name="use_model" value="1" defaultChecked />
                Call the model
              </label>
              <ProfileSelect profiles={profiles} />
              <ConcurrencyField />
              <Button type="submit" size="sm" variant="outline">
                Review…
              </Button>
            </Form>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Run the model pass</CardTitle>
            <CardDescription>
              Only the companies published with several description sources and no
              suggestion yet.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Form method="post" className="flex flex-wrap items-end gap-3">
              <input type="hidden" name="intent" value="confirm-model-pass" />
              <MaxCompaniesField defaultValue={500} />
              <ProfileSelect profiles={profiles} />
              <ConcurrencyField />
              <Button type="submit" size="sm" variant="outline">
                Review…
              </Button>
            </Form>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Refresh an artifact</CardTitle>
            <CardDescription>
              Re-reads one source into its artifact table; no model, no final write.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Form method="post" className="flex flex-wrap items-end gap-3">
              <input type="hidden" name="intent" value="confirm-artifact" />
              <Field label="Artifact">
                <Select items={artifactSelectItems()} name="artifact" defaultValue="scb">
                  <SelectTrigger className="w-40" size="sm">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {INFO_ARTIFACT_SOURCES.map((artifact) => (
                      <SelectItem key={artifact} value={artifact}>
                        {artifact}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </Field>
              <Button type="submit" size="sm" variant="outline">
                Review…
              </Button>
            </Form>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Recent runs</CardTitle>
          <CardDescription>
            Counts come from each run&apos;s materialization metadata; a preview
            run reports what it selected and nothing else.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <Table className="min-w-[56rem]">
              <TableHeader>
                <TableRow>
                  <TableHead>Run</TableHead>
                  <TableHead>Job</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Started</TableHead>
                  <TableHead>Duration</TableHead>
                  <TableHead className="text-right">Selected</TableHead>
                  <TableHead className="text-right">Inserted</TableHead>
                  <TableHead className="text-right">Model calls</TableHead>
                  <TableHead>Launched by</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {runs.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={9} className="text-muted-foreground text-sm">
                      No runs to show.
                    </TableCell>
                  </TableRow>
                ) : (
                  runs.map((run) => (
                    <TableRow key={run.runId}>
                      <TableCell className="font-mono text-xs">
                        {run.url ? (
                          <a
                            className="underline underline-offset-2"
                            href={run.url}
                            target="_blank"
                            rel="noreferrer"
                          >
                            {run.runId.slice(0, 8)}
                          </a>
                        ) : (
                          run.runId.slice(0, 8)
                        )}
                      </TableCell>
                      <TableCell className="text-xs">{run.jobName}</TableCell>
                      <TableCell>
                        <Badge variant={statusVariant(run.status)}>{run.status}</Badge>
                      </TableCell>
                      <TableCell className="text-muted-foreground text-xs">
                        {instant(run.startTime)}
                      </TableCell>
                      <TableCell className="text-muted-foreground text-xs">
                        {duration(run)}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        {count(run, "selected_company_count")}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        {count(run, "inserted_count")}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        {count(run, "llm_request_count")}
                      </TableCell>
                      <TableCell className="text-xs">
                        {run.tags.pilot ?? run.tags["dagster/schedule_name"] ??
                          run.tags["dagster/sensor_name"] ?? "—"}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
