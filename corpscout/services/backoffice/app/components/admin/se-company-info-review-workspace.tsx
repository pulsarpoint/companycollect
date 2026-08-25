import {
  CheckCircle2Icon,
  FileSearchIcon,
  TriangleAlertIcon,
} from "lucide-react";
import { Form, useNavigation } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import {
  CompanySourceStrip,
  companySourceLabels,
} from "~/components/admin/company-source-strip";
import {
  DefinitionList,
  EMPTY_VALUE,
  text,
} from "~/components/admin/definition-list";
import { LegalForm } from "~/components/admin/legal-form";
import { Badge } from "~/components/ui/badge";
import { Button, buttonVariants } from "~/components/ui/button";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "~/components/ui/accordion";
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
import { Separator } from "~/components/ui/separator";
import { Textarea } from "~/components/ui/textarea";
import type {
  SeCompanyInfoArtifactRow,
  SeCompanyInfoDetail,
  SeCompanyInfoRow,
} from "~/lib/se-company-info.server";
import {
  ARTIFACT_SOURCES,
  artifactPayloadEntries,
  artifactSourceLabel,
  parseJsonList,
  wikidataHref,
  type ArtifactPayloadEntry,
} from "~/lib/se-company-info-payload";
import { liveOverrideRefusal } from "~/lib/se-info-review-form";

export type SeCompanyInfoReviewResult =
  { ok: true; correctionId: string } | { ok: false; error: string } | null;

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
      rationale: typeof obj.rationale === "string" ? obj.rationale : undefined,
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
  // No padding of its own: the Info tab -- its only caller since the layout
  // began rendering SeCompanyNotFound for ids no table knows -- renders this
  // inside the layout's padded container.
  return (
    <div className="flex flex-col gap-6">
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <FileSearchIcon />
          </EmptyMedia>
          <EmptyTitle>Not published yet</EmptyTitle>
          <EmptyDescription>
            Company {companyId} is not published in se_company_info yet. Dagster
            publishes a company once its enrichment run completes, so this page
            fills in after the next run.
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

/** One payload value, rendered the way its kind asks: a link for the two
 * Wikidata identity fields, a list for ESEF's JSON blobs, plain text for
 * everything else (including a column this app has never seen). */
function PayloadValue({
  entry,
  payload,
}: {
  entry: ArtifactPayloadEntry;
  payload: Record<string, string>;
}) {
  if (entry.value === "") return EMPTY_VALUE;
  if (entry.kind === "wikidata-id") {
    return (
      <a
        className="underline underline-offset-2"
        href={wikidataHref(entry.value, payload.wikidata_url ?? "")}
        target="_blank"
        rel="noreferrer"
      >
        {entry.value}
      </a>
    );
  }
  if (entry.kind === "url") {
    return (
      <a
        className="underline underline-offset-2 break-all"
        href={entry.value}
        target="_blank"
        rel="noreferrer"
      >
        {entry.value}
      </a>
    );
  }
  if (entry.kind === "json-list") {
    const items = parseJsonList(entry.value);
    // Unparseable or not an array: show the raw text rather than dropping it.
    if (items === null) return <span className="break-all">{entry.value}</span>;
    if (items.length === 0) return EMPTY_VALUE;
    return (
      <ul className="list-disc pl-4">
        {items.map((item, index) => (
          <li key={`${index}-${item.text}`}>
            {item.text}
            {item.detail === "" ? null : (
              <span className="text-muted-foreground ml-1 text-xs break-all">
                {item.detail}
              </span>
            )}
          </li>
        ))}
      </ul>
    );
  }
  return <span>{entry.value}</span>;
}

/** One artifact version: its envelope, then every payload column of its table
 * as a labelled definition list. */
function ArtifactCard({
  artifact,
  contributes,
}: {
  artifact: SeCompanyInfoArtifactRow;
  contributes: boolean;
}) {
  const entries = artifactPayloadEntries(artifact.source, artifact.payload);
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center gap-2">
          <CardTitle className="font-mono text-sm">
            {artifact.source_record_uid}
          </CardTitle>
          {contributes ? (
            <Badge variant="secondary">contributes to description</Badge>
          ) : null}
        </div>
        <CardDescription>
          {/* The artifact stamp is when this pipeline recorded the version, not
              a register date: SCB's bulk load carries one constant
              updated_from_raw_at for every company. */}
          <span title="when the pipeline recorded this version">
            observed {artifact.observed_at}
          </span>{" "}
          · evidence{" "}
          <code className="font-mono text-xs">
            {artifact.evidence_hash.slice(0, 8)}
          </code>
        </CardDescription>
      </CardHeader>
      <CardContent>
        {/* Labels are unique within one source's payload field list, so the
            label is a stable key here. */}
        <DefinitionList
          entries={entries.map((entry) => [
            entry.label,
            <PayloadValue entry={entry} payload={artifact.payload} />,
          ])}
        />
      </CardContent>
    </Card>
  );
}

/** Artifacts grouped by source, the known sources in reading order and any
 * source this app does not know about after them (a fourth artifact table
 * would show up here rather than vanish). */
function groupArtifactsBySource(
  artifacts: SeCompanyInfoArtifactRow[],
): Array<{ source: string; rows: SeCompanyInfoArtifactRow[] }> {
  const seen = new Set<string>(ARTIFACT_SOURCES);
  const order = [
    ...ARTIFACT_SOURCES,
    ...artifacts.map((row) => row.source).filter((source) => !seen.has(source)),
  ];
  const groups: Array<{ source: string; rows: SeCompanyInfoArtifactRow[] }> =
    [];
  for (const source of order) {
    if (groups.some((group) => group.source === source)) continue;
    const rows = artifacts.filter((row) => row.source === source);
    if (rows.length > 0) groups.push({ source, rows });
  }
  return groups;
}

/** The published row: what surfaces actually serve for this company. */
function PublishedCard({ info }: { info: SeCompanyInfoRow }) {
  // Task 17: one flag, not a source label. "Where the text came from" is the
  // sources list beside it (every source that contributed a candidate); "who
  // decided" is the correction ids further down. `llm_enhanced` answers only
  // "did the model write this", which no other column on the row says.
  const llmEnhanced = Boolean(Number(info.llm_enhanced));
  const sources = info.description_sources.join(", ");
  const sourceLabels = companySourceLabels(info.description_sources);
  return (
    <Card className="[--card-spacing:--spacing(5)]">
      <CardHeader className="border-b">
        <CardTitle>Published version</CardTitle>
        <CardDescription>
          Current descriptions and classification shown across company surfaces.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-5">
        <div className="grid gap-5 md:grid-cols-2 md:gap-0">
          <section
            aria-labelledby="published-description-en"
            className="flex flex-col gap-2 md:pr-6"
          >
            <div className="flex items-center gap-2">
              <Badge variant="outline">EN</Badge>
              <h3 id="published-description-en" className="font-medium">
                English
              </h3>
            </div>
            {info.description ? (
              <p className="text-base leading-relaxed" lang="en">
                {info.description}
              </p>
            ) : (
              <p className="text-sm text-muted-foreground">No description.</p>
            )}
          </section>
          <section
            aria-labelledby="published-description-sv"
            className="flex flex-col gap-2 border-t pt-5 md:border-t-0 md:border-l md:pt-0 md:pl-6"
          >
            <div className="flex items-center gap-2">
              <Badge variant="outline">SV</Badge>
              <h3 id="published-description-sv" className="font-medium">
                Swedish
              </h3>
            </div>
            {info.description_sv ? (
              <p className="text-base leading-relaxed" lang="sv">
                {info.description_sv}
              </p>
            ) : (
              <p className="text-sm text-muted-foreground">
                No Swedish description.
              </p>
            )}
          </section>
        </div>
        <Separator />
        <dl className="grid grid-cols-2 gap-x-6 gap-y-4 sm:grid-cols-4">
          <div className="flex flex-col gap-1">
            <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Incorporated
            </dt>
            <dd className="font-medium tabular-nums">
              {text(info.incorporation_date ?? "")}
            </dd>
          </div>
          <div className="flex flex-col gap-1">
            <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              NACE
            </dt>
            <dd className="font-medium tabular-nums">
              {text(info.primary_nace_code)}
            </dd>
          </div>
          <div className="flex flex-col gap-1">
            <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              SNI
            </dt>
            <dd className="font-medium tabular-nums">
              {text(info.primary_sni_code)}
            </dd>
          </div>
          <div className="flex flex-col gap-1">
            <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Sources
            </dt>
            <dd className="flex flex-wrap gap-1">
              {sourceLabels.length === 0
                ? EMPTY_VALUE
                : sourceLabels.map((source) => (
                    <Badge key={source} variant="secondary">
                      {source}
                    </Badge>
                  ))}
            </dd>
          </div>
        </dl>
        <Separator />
        <Accordion hiddenUntilFound>
          <AccordionItem value="published-metadata">
            <AccordionTrigger>
              <span className="flex flex-col gap-0.5">
                <span>Additional information</span>
                <span className="text-xs font-normal text-muted-foreground">
                  Legal form, identifiers, and processing details
                </span>
              </span>
            </AccordionTrigger>
            <AccordionContent className="pt-2">
              <DefinitionList
                valueClassName="break-all"
                entries={[
                  ["Status", text(info.status)],
                  // Copied from the register with the code it names, not
                  // written by the model and not overridable here.
                  [
                    "Legal form",
                    <LegalForm
                      key="legal-form"
                      form={{
                        code: info.legal_form_code ?? "",
                        label_sv: info.legal_form_label_sv,
                        label_en: info.legal_form_label_en,
                      }}
                    />,
                  ],
                  ["Description language", text(info.description_language)],
                  ["LLM enhanced", llmEnhanced ? "yes" : "no"],
                  ["Wikidata id", text(info.wikidata_id ?? "")],
                  ["LEI", text(info.lei ?? "")],
                  ["Description sources", text(sources)],
                  [
                    "Description source records",
                    text(info.description_source_record_uids.join(", ")),
                  ],
                  ["Source records", text(info.source_record_uids.join(", "))],
                  ["Evidence set hash", text(info.evidence_set_hash)],
                  ["Correction ids", text(info.correction_ids.join(", "))],
                  [
                    "Model",
                    `${info.model_provider} · ${info.model_name} · prompt ${info.prompt_version}`,
                  ],
                  ["Resolved at", text(info.resolved_at)],
                  ["Run", text(info.source_run_id)],
                ]}
              />
            </AccordionContent>
          </AccordionItem>
        </Accordion>
      </CardContent>
    </Card>
  );
}

function SectionHeading({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <div className="flex flex-col gap-1">
      <h2 className="text-lg font-semibold tracking-tight">{title}</h2>
      <p className="text-sm text-muted-foreground">{description}</p>
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
  const overrideRefusal = liveOverrideRefusal(
    "approve_suggestion",
    corrections,
  );
  const contributing = new Set(info.description_source_record_uids);
  const groups = groupArtifactsBySource(artifacts);

  return (
    // No header of its own: Task 18 moved the company identity, the links and
    // the sub-menu up into admin-se-company-layout.tsx, which renders this
    // tab inside its own padded container.
    <div className="flex flex-col gap-6">
      {/* The artifact legs this company actually has (scb / esef / wikidata),
          which is the same set the "Sources" section below groups its cards
          by -- named once at the top so all five tabs open the same way. */}
      <CompanySourceStrip
        sources={artifacts.map((artifact) => artifact.source)}
      />
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

      <PublishedCard info={info} />

      <section className="flex flex-col gap-4">
        <SectionHeading
          title="Sources"
          description="Every artifact row connected to this company, in full. Their set is what the evidence hash covers."
        />
        {groups.length === 0 ? (
          <p className="text-sm text-muted-foreground">No source artifacts.</p>
        ) : null}
        {groups.map((group) => (
          <div key={group.source} className="flex flex-col gap-3">
            <h3 className="flex items-center gap-2 text-sm font-semibold">
              {artifactSourceLabel(group.source)}
              <Badge variant="outline">{group.rows.length}</Badge>
            </h3>
            {group.rows.map((artifact) => (
              <ArtifactCard
                key={`${artifact.source}:${artifact.source_record_uid}`}
                artifact={artifact}
                contributes={contributing.has(artifact.source_record_uid)}
              />
            ))}
          </div>
        ))}
      </section>

      <Card>
        <CardHeader>
          <CardTitle>Model suggestions</CardTitle>
          <CardDescription>
            Newest first. Approve or reject only apply to the newest suggestion;
            older ones answered evidence this company no longer has.
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
                {/* Both halves are labelled: one model call answers in two
                    languages, and an unlabelled pair reads as one text repeated. */}
                {parsed?.description ? (
                  <p className="mt-2 text-sm">English: {parsed.description}</p>
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

      <section className="flex flex-col gap-4">
        <SectionHeading
          title="Corrections"
          description="Reviewer decisions for this company. Every one is appended to the ledger with a reason and applied by the next Dagster review run."
        />

        <Card>
          <CardHeader>
            <CardTitle>Override description</CardTitle>
            <CardDescription>
              Every decision is appended to the correction ledger with a reason;
              nothing here rewrites the published row directly. Each language is
              sent only when you change it — except the English text, which the
              ledger always requires, so an override decides both. Leave a text
              as-is and tick its box to clear that language entirely.
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
                  <Form
                    method="post"
                    className="ml-auto flex items-center gap-2"
                  >
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
      </section>
    </div>
  );
}
