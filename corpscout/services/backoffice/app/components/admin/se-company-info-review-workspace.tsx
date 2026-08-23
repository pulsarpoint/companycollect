import {
  CheckCircle2Icon,
  FileSearchIcon,
  TriangleAlertIcon,
} from "lucide-react";
import { Form, Link, useNavigation } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button, buttonVariants } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { Checkbox } from "~/components/ui/checkbox";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { Input } from "~/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { Textarea } from "~/components/ui/textarea";
import type { SeCompanyInfoDetail } from "~/lib/se-company-info.server";
import { liveOverrideRefusal } from "~/lib/se-info-review-form";

export type SeCompanyInfoReviewResult =
  | { ok: true; correctionId: string }
  | { ok: false; error: string }
  | null;

/**
 * Every non-undo correction form posts the kind it represents plus the
 * evidence hash the reviewer actually saw, so a concurrent rebuild is
 * rejected server-side rather than silently applied to different evidence.
 * Undo forms carry no evidence hash at all -- they supersede a decision, not
 * evidence, and the action always writes ZERO_EVIDENCE_HASH for them.
 */
function HiddenCommon({
  kind,
  evidenceHash,
}: {
  kind: string;
  evidenceHash: string;
}) {
  return (
    <>
      <input type="hidden" name="correction_kind" value={kind} />
      <input type="hidden" name="evidence_hash" value={evidenceHash} />
    </>
  );
}

/**
 * Best-effort parse of a suggestion's JSON body for a friendlier display.
 * Each field is guarded with its own `typeof === "string"` check -- an
 * enrichment run that ever emits a non-string `description` (an object or
 * array, say) must not crash SSR by handing React a non-renderable child.
 */
function parseSuggestion(raw: string): {
  description?: string;
  descriptionSv?: string;
  language?: string;
  rationale?: string;
} | null {
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    const obj = parsed as Record<string, unknown>;
    return {
      description:
        typeof obj.description === "string" ? obj.description : undefined,
      // Both languages come from one model call (prompt v3); a suggestion recorded
      // before that carries only the English half and simply shows nothing here.
      descriptionSv:
        typeof obj.description_sv === "string" ? obj.description_sv : undefined,
      language: typeof obj.language === "string" ? obj.language : undefined,
      rationale:
        typeof obj.rationale === "string" ? obj.rationale : undefined,
    };
  } catch {
    // Not JSON (or malformed) -- the raw text still renders below.
    return null;
  }
}

/** Shown when Dagster has not published this company into se_company_info. */
export function SeCompanyInfoNotPublished({
  companyId,
}: {
  companyId: string;
}) {
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <FileSearchIcon />
          </EmptyMedia>
          <EmptyTitle>Not published yet</EmptyTitle>
          <EmptyDescription>
            Company {companyId} is not published in se_company_info yet.
            Dagster publishes a company once its enrichment run completes, so
            this page fills in after the next run.
          </EmptyDescription>
        </EmptyHeader>
        <EmptyContent>
          {/* Plain anchor, not <Link>: this component can render without a
              Router (see tests/admin-se-company-info.test.tsx). */}
          <a
            className={buttonVariants({ variant: "outline" })}
            href={`/company/se/${encodeURIComponent(companyId)}`}
          >
            Back to company
          </a>
        </EmptyContent>
      </Empty>
    </div>
  );
}

export function SeCompanyInfoReviewWorkspace({
  detail,
  result,
}: {
  detail: SeCompanyInfoDetail;
  result: SeCompanyInfoReviewResult;
}) {
  const { info, artifacts, suggestions, corrections } = detail;
  const hash = info.evidence_set_hash;
  // One click is one ledger row: block every submit while one is in flight.
  const busy = useNavigation().state !== "idle";
  // While a live override stands, Dagster's kind-ranking always lets it win
  // over any approve/reject, so offering those decisions here would be
  // misleading -- disable them and point at the override instead. The two
  // kinds always share one answer (the check doesn't look at which kind was
  // asked), so one call covers both buttons below.
  const overrideRefusal = liveOverrideRefusal("approve_suggestion", corrections);

  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline">ClickHouse</Badge>
          <Badge variant="secondary">{info.status}</Badge>
          {info.legal_form_code ? (
            <Badge variant="secondary">{info.legal_form_code}</Badge>
          ) : null}
          <Badge variant="secondary">{info.description_source}</Badge>
          {info.correction_ids.length > 0 ? <Badge>reviewed</Badge> : null}
        </div>
        <h1 className="text-2xl font-semibold tracking-tight">
          {info.legal_name}
        </h1>
        <p className="text-sm text-muted-foreground">
          Company{" "}
          <Link
            className="underline"
            to={`/company/se/${encodeURIComponent(info.company_id)}`}
          >
            {info.company_id}
          </Link>{" "}
          · incorporated {info.incorporation_date ?? "unknown"} · NACE{" "}
          {info.primary_nace_code} · SNI {info.primary_sni_code}
          {info.wikidata_id ? <> · wikidata {info.wikidata_id}</> : null}
          {info.lei ? <> · LEI {info.lei}</> : null} · evidence{" "}
          <code className="font-mono text-xs">{hash.slice(0, 12)}</code> ·
          model {info.model_provider} · {info.model_name} · prompt{" "}
          {info.prompt_version} · resolved {info.resolved_at}
        </p>
        {/* The published row holds both languages natively (migration 000301):
            the English text is what surfaces publish, the Swedish one the
            register's own wording (or the model's Swedish summary). */}
        <div className="grid gap-3 md:grid-cols-2">
          <div className="flex flex-col gap-1">
            <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              English
            </span>
            {info.description
              ? (() => {
                  // description_source is the winning source; don't repeat it
                  // inside the full description_sources list.
                  const otherSources = info.description_sources.filter(
                    (source) => source !== info.description_source,
                  );
                  return (
                    <p className="text-sm">
                      {info.description}{" "}
                      <span className="text-xs text-muted-foreground">
                        ({info.description_language} · {info.description_source}
                        {otherSources.length > 0
                          ? ` · ${otherSources.join(", ")}`
                          : ""}
                        )
                      </span>
                    </p>
                  );
                })()
              : <p className="text-sm text-muted-foreground">No description.</p>}
          </div>
          <div className="flex flex-col gap-1">
            <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Swedish
            </span>
            {info.description_sv ? (
              <p className="text-sm">{info.description_sv}</p>
            ) : (
              <p className="text-sm text-muted-foreground">
                No Swedish description.
              </p>
            )}
          </div>
        </div>
      </header>

      {result?.ok ? (
        <Alert>
          <CheckCircle2Icon />
          <AlertTitle>Saved</AlertTitle>
          <AlertDescription>
            Correction {result.correctionId} is in the ledger. Queued for the
            next Dagster review run; reload to see the result once it lands.
          </AlertDescription>
        </Alert>
      ) : null}
      {result && !result.ok ? (
        <Alert variant="destructive">
          <TriangleAlertIcon />
          <AlertTitle>Not saved</AlertTitle>
          <AlertDescription>{result.error}</AlertDescription>
        </Alert>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle>Sources</CardTitle>
          <CardDescription>
            The artifact rows this published company info was resolved from.
            Their set is what the evidence hash covers.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Source</TableHead>
                <TableHead>Record</TableHead>
                {/* The artifact stamp is when this pipeline recorded the version,
                    not a register date: SCB's bulk load carries one constant
                    updated_from_raw_at for every company. */}
                <TableHead title="when the pipeline recorded this version">
                  Observed
                </TableHead>
                <TableHead>Summary</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {artifacts.length === 0 ? (
                <TableRow>
                  <TableCell
                    colSpan={4}
                    className="text-sm text-muted-foreground"
                  >
                    No source artifacts.
                  </TableCell>
                </TableRow>
              ) : null}
              {artifacts.map((artifact) => (
                <TableRow key={`${artifact.source}:${artifact.source_record_uid}`}>
                  <TableCell>{artifact.source}</TableCell>
                  <TableCell className="font-mono text-xs">
                    {artifact.source_record_uid}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {artifact.observed_at}
                  </TableCell>
                  <TableCell>{artifact.summary}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Model suggestions</CardTitle>
          <CardDescription>
            Newest first. Approve or reject only apply to the newest
            suggestion; older ones answered evidence this company no longer
            has.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          {suggestions.length === 0 ? (
            <span className="text-sm text-muted-foreground">
              No suggestions recorded.
            </span>
          ) : null}
          {suggestions.map((suggestion) => {
            const parsed = parseSuggestion(suggestion.suggestion);
            return (
              <div
                key={suggestion.suggestion_id}
                className="rounded-lg border p-3"
              >
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant="outline">
                    {suggestion.model_provider} · {suggestion.model_name}
                  </Badge>
                  {suggestion.is_published ? <Badge>published</Badge> : null}
                  {suggestion.is_newest ? null : (
                    <Badge variant="outline">superseded evidence</Badge>
                  )}
                  <span className="text-xs text-muted-foreground">
                    {suggestion.created_at}
                  </span>
                </div>
                {parsed?.description ? (
                  <p className="mt-2 text-sm">{parsed.description}</p>
                ) : null}
                {parsed?.descriptionSv ? (
                  <p className="mt-1 text-sm">
                    Swedish: {parsed.descriptionSv}
                  </p>
                ) : null}
                {parsed?.language ? (
                  <p className="mt-1 text-xs text-muted-foreground">
                    Language: {parsed.language}
                  </p>
                ) : null}
                {parsed?.rationale ? (
                  <p className="mt-1 text-xs text-muted-foreground">
                    Rationale: {parsed.rationale}
                  </p>
                ) : null}
                <details className="mt-2">
                  <summary className="cursor-pointer text-xs text-muted-foreground">
                    raw suggestion
                  </summary>
                  <pre className="mt-1 overflow-x-auto whitespace-pre-wrap break-words rounded-lg border bg-muted p-2 font-mono text-xs">
                    {suggestion.suggestion}
                  </pre>
                </details>
                <p className="mt-2 text-xs text-muted-foreground">
                  prompt {suggestion.prompt_version}
                </p>
                {/* Approving/rejecting a suggestion the pipeline no longer
                    asks for is stale on arrival, so only the newest row gets
                    decision forms. */}
                {suggestion.is_newest ? (
                  <div className="mt-2 flex flex-wrap items-center gap-2">
                    {(["approve_suggestion", "reject_suggestion"] as const).map(
                      (kind) => (
                        <Form
                          key={kind}
                          method="post"
                          className="flex items-center gap-2"
                        >
                          <HiddenCommon kind={kind} evidenceHash={hash} />
                          <input
                            type="hidden"
                            name="suggestion_id"
                            value={suggestion.suggestion_id}
                          />
                          <Input
                            name="reason"
                            placeholder="Reason"
                            aria-label="Reason"
                            required
                            className="w-56"
                          />
                          {kind === "reject_suggestion" ? (
                            <Input
                              name="note"
                              placeholder="Note (optional)"
                              aria-label="Note"
                              className="w-48"
                            />
                          ) : null}
                          <Button
                            size="sm"
                            variant={
                              kind === "approve_suggestion"
                                ? "default"
                                : "outline"
                            }
                            type="submit"
                            disabled={busy || overrideRefusal !== null}
                            aria-busy={busy}
                          >
                            {kind === "approve_suggestion"
                              ? "Approve"
                              : "Reject"}
                          </Button>
                        </Form>
                      ),
                    )}
                    {overrideRefusal ? (
                      <span className="text-xs text-muted-foreground">
                        {overrideRefusal}
                      </span>
                    ) : null}
                  </div>
                ) : (
                  <p className="mt-2 text-xs text-muted-foreground">
                    Answered evidence this company no longer has. Nothing to
                    decide until the model is asked again.
                  </p>
                )}
              </div>
            );
          })}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Override description</CardTitle>
          <CardDescription>
            Every decision is appended to the correction ledger with a
            reason; nothing here rewrites the published row directly. Each
            language is sent only when you change it — except the English
            text, which the ledger always requires, so an override decides
            both. Leave a text as-is and tick its box to clear that language
            entirely.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Form method="post" className="flex max-w-2xl flex-col gap-2">
            <HiddenCommon kind="override_field" evidenceHash={hash} />
            {/* Diffed server-side: an untouched description must not enter
                the payload, because Dagster replays the correction every
                run. */}
            <input
              type="hidden"
              name="original_description"
              value={info.description ?? ""}
            />
            <Textarea
              name="description"
              defaultValue={info.description ?? ""}
              aria-label="English description"
              placeholder="English description"
              rows={4}
            />
            <label className="flex items-center gap-2 text-sm">
              <Checkbox name="clear_description" value="yes" />
              <span>Clear the description</span>
            </label>
            <input
              type="hidden"
              name="original_description_sv"
              value={info.description_sv ?? ""}
            />
            <Textarea
              name="description_sv"
              defaultValue={info.description_sv ?? ""}
              aria-label="Swedish description"
              placeholder="Swedish description"
              rows={4}
            />
            <label className="flex items-center gap-2 text-sm">
              <Checkbox name="clear_description_sv" value="yes" />
              <span>Clear the Swedish description</span>
            </label>
            <Input
              name="reason"
              placeholder="Reason"
              aria-label="Reason"
              required
            />
            <Button type="submit" disabled={busy} aria-busy={busy}>
              Save override
            </Button>
          </Form>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Ledger</CardTitle>
          <CardDescription>
            Every correction ever recorded for this company, newest first.
            Undo appends a superseding row instead of deleting one.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-2">
          {corrections.length === 0 ? (
            <span className="text-sm text-muted-foreground">
              No corrections yet.
            </span>
          ) : null}
          {corrections.map((correction) => (
            <div
              key={correction.correction_id}
              className="flex flex-wrap items-center gap-2 rounded-lg border p-2 text-sm"
            >
              <Badge variant={correction.is_current ? "default" : "outline"}>
                {correction.correction_kind}
              </Badge>
              <code className="font-mono text-xs text-muted-foreground">
                {correction.correction_id.slice(0, 8)}
              </code>
              {correction.is_stale ? (
                <Badge variant="destructive">evidence changed</Badge>
              ) : null}
              {correction.is_applied ? (
                <Badge variant="secondary">applied</Badge>
              ) : null}
              <span>{correction.reason}</span>
              <span className="text-xs text-muted-foreground">
                {correction.decided_by} · {correction.created_at}
              </span>
              <code className="font-mono text-xs">{correction.payload}</code>
              {correction.is_current &&
              correction.correction_kind !== "undo" ? (
                <Form method="post" className="ml-auto flex items-center gap-2">
                  <input type="hidden" name="correction_kind" value="undo" />
                  <input
                    type="hidden"
                    name="supersedes_correction_id"
                    value={correction.correction_id}
                  />
                  <Input
                    name="reason"
                    placeholder="Why undo"
                    aria-label="Why undo"
                    required
                    className="w-40"
                  />
                  <Button
                    size="sm"
                    variant="ghost"
                    type="submit"
                    disabled={busy}
                    aria-busy={busy}
                  >
                    Undo
                  </Button>
                </Form>
              ) : null}
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}
