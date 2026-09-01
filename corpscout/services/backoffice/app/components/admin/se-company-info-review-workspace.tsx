import {
  CheckCircle2Icon,
  FileSearchIcon,
  TriangleAlertIcon,
} from "lucide-react";
import { useState } from "react";
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
import { CompanyDescriptionCard } from "~/components/admin/company-description-card";
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
  SeCompanyInfoFieldValueRow,
  SeCompanyInfoRow,
} from "~/lib/se-company-info.server";
import {
  ARTIFACT_SOURCES,
  artifactPayloadEntries,
  artifactSourceLabel,
  descriptionProposals,
  parseJsonList,
  parseSuggestionText,
  wikidataHref,
  type ArtifactPayloadEntry,
  type DescriptionProposal,
} from "~/lib/se-company-info-payload";
import { SE_INFO_FIELDS } from "~/lib/se-info-field-values";
import type { DescriptionShown } from "~/components/admin/company-description-card";

export type SeCompanyInfoReviewResult =
  | { ok: true; valueIds: string[] }
  | { ok: false; error: string }
  | null;

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
  // decided" is the Value history card further down, which says it in full.
  // `llm_enhanced` answers only "did the model write this", which no other
  // column on the row says.
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

/** One labelled fact in the facts card; an absent value renders as an em dash. */
function Fact({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col gap-1">
      <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
        {label}
      </dt>
      <dd className="text-sm font-medium">{children}</dd>
    </div>
  );
}

/**
 * The company facts a reviewer wants right under the description: status,
 * incorporation, legal form, and the industry classification by number AND
 * name (NACE label resolved from corpscout.nace_categories; SNI is Sweden's
 * 5-digit refinement of the same class and has no label catalog yet).
 */
function CompanyFactsCard({
  info,
  naceLabel,
}: {
  info: SeCompanyInfoRow;
  naceLabel: string;
}) {
  const statusVariant = info.status === "active" ? "default" : "secondary";
  return (
    <Card>
      <CardHeader>
        <CardTitle>Company facts</CardTitle>
        <CardDescription>
          Registry status and classification behind the published record.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <dl className="grid grid-cols-2 gap-x-6 gap-y-5 md:grid-cols-3 lg:grid-cols-6">
          <Fact label="Status">
            <Badge variant={statusVariant}>{info.status || EMPTY_VALUE}</Badge>
          </Fact>
          <Fact label="Incorporated">
            <span className="tabular-nums">
              {info.incorporation_date || EMPTY_VALUE}
            </span>
          </Fact>
          <Fact label="Legal form">
            {info.legal_form_label_en || info.legal_form_code || EMPTY_VALUE}
          </Fact>
          <Fact label="NACE">
            <span className="flex flex-col gap-0.5">
              <span className="tabular-nums">
                {info.primary_nace_code || EMPTY_VALUE}
              </span>
              {naceLabel !== "" ? (
                <span className="font-normal text-muted-foreground">
                  {naceLabel}
                </span>
              ) : null}
            </span>
          </Fact>
          <Fact label="SNI">
            <span className="flex flex-col gap-0.5">
              <span className="tabular-nums">
                {info.primary_sni_code || EMPTY_VALUE}
              </span>
              <span className="font-normal text-xs text-muted-foreground">
                Swedish 5-digit NACE refinement
              </span>
            </span>
          </Fact>
          <Fact label="LEI">
            <span className="font-mono text-xs">{info.lei || EMPTY_VALUE}</span>
          </Fact>
        </dl>
      </CardContent>
    </Card>
  );
}

/**
 * "Use this" under a source option: the text on screen, written as this
 * company's value for the field that text belongs to.
 *
 * The record it came from travels with it (`source_ref` = the artifact's
 * source_record_uid, `source_at` = its observed_at) because Dagster reads
 * those back as the published row's provenance -- a value copied from SCB
 * must stay attributable to the SCB row the reviewer actually read, not to
 * "a reviewer typed this".
 */
function UseSourceForm({
  proposal,
  shown,
  busy,
  repeats,
}: {
  proposal: DescriptionProposal;
  shown: DescriptionShown;
  busy: boolean;
  /** Does this source offer more than one option (ESEF, one filing per year)?
   * Then the meta line is what tells them apart in the menu, so it has to tell
   * their buttons apart too. */
  repeats: boolean;
}) {
  // The column name is not a name a screen reader should read out, and every
  // option's button says the same two words, so the accessible name carries
  // the source, the option and the language the click would write.
  const fieldName =
    shown.field === "description_sv" ? "Swedish description" : "English description";
  const sourceName = repeats
    ? `${proposal.sourceLabel} (${proposal.meta})`
    : proposal.sourceLabel;
  return (
    <Form method="post" className="flex items-center gap-2">
      <input type="hidden" name="intent" value="use-source" />
      <input type="hidden" name="field" value={shown.field} />
      <input type="hidden" name="value" value={shown.text} />
      <input type="hidden" name="source" value={proposal.source} />
      <input type="hidden" name="source_ref" value={proposal.sourceRecordUid} />
      <input type="hidden" name="source_at" value={proposal.observedAt} />
      <Button
        size="sm"
        type="submit"
        disabled={busy}
        aria-busy={busy}
        aria-label={`Use the ${sourceName} text as the ${fieldName}`}
      >
        Use this
      </Button>
    </Form>
  );
}

/** One language of the inline editor: the textarea, the original it will be
 * diffed against, and the box that clears the field. */
function EditField({
  field,
  label,
  value,
}: {
  field: string;
  label: string;
  value: string;
}) {
  return (
    <div className="flex flex-col gap-1.5">
      {/* ALWAYS posted, even empty: the builder skips a field whose original is
          absent, and reads a present-but-empty one as "no text yet". */}
      <input type="hidden" name={`original_${field}`} value={value} />
      <Textarea
        name={field}
        defaultValue={value}
        aria-label={label}
        placeholder={label}
        rows={4}
      />
      <div className="flex flex-wrap items-center gap-2">
        <label className="flex items-center gap-2 text-sm">
          <Checkbox name={`clear_${field}`} value="yes" />
          <span>Clear</span>
        </label>
        <span className="text-xs text-muted-foreground">
          To remove this text, tick Clear — an emptied box is refused.
        </span>
      </div>
    </div>
  );
}

/**
 * "Edit" under the published Final option, opening the reviewer's own wording
 * in place. The form stays in the document while it is closed (hidden rather
 * than unmounted) so a half-written edit survives a tab switch.
 *
 * Each language is diffed server-side against the `original_*` it was opened
 * with: a value is permanent until something later replaces it, so writing an
 * untouched description back would pin today's computed text for ever.
 */
function FinalDescriptionEditor({
  info,
  busy,
}: {
  info: SeCompanyInfoRow;
  busy: boolean;
}) {
  const [editing, setEditing] = useState(false);
  return (
    <div className="flex flex-col gap-3">
      <div>
        <Button
          type="button"
          size="sm"
          variant="outline"
          aria-expanded={editing}
          onClick={() => setEditing((open) => !open)}
        >
          Edit
        </Button>
      </div>
      <div hidden={!editing}>
        <Form method="post" className="flex max-w-2xl flex-col gap-3">
          <input type="hidden" name="intent" value="edit" />
          <EditField
            field="description"
            label="English description"
            value={info.description ?? ""}
          />
          <EditField
            field="description_sv"
            label="Swedish description"
            value={info.description_sv ?? ""}
          />
          <Input name="note" placeholder="Note (optional)" aria-label="Note" />
          <div className="flex items-center gap-2">
            <Button type="submit" size="sm" disabled={busy} aria-busy={busy}>
              Save
            </Button>
            <Button
              type="button"
              size="sm"
              variant="ghost"
              onClick={() => setEditing(false)}
            >
              Cancel
            </Button>
          </div>
        </Form>
      </div>
    </div>
  );
}

/**
 * Every value ever decided for this company, newest first, with the release
 * buttons above them.
 *
 * There is no ranking to explain here and no staleness to warn about: the row
 * marked live is simply the latest one written for its field, and that is what
 * the next rebuild publishes. A row with no value is a RELEASE -- the field
 * handed back to whatever the pipeline computes for it -- which is why it says
 * so in words instead of showing an empty cell.
 */
function ValueHistoryCard({
  rows,
  busy,
}: {
  rows: SeCompanyInfoFieldValueRow[];
  busy: boolean;
}) {
  return (
    <section className="flex flex-col gap-4">
      <SectionHeading
        title="Value history"
        description="Every value decided for this company, newest first. The live row per field is what the next rebuild publishes."
      />
      <Card>
        <CardContent className="flex flex-col gap-3">
          <div className="flex flex-wrap items-center gap-3">
            {SE_INFO_FIELDS.map((field) => (
              <Form
                key={field}
                method="post"
                className="flex items-center gap-2"
              >
                <input type="hidden" name="intent" value="release" />
                <input type="hidden" name="field" value={field} />
                <Badge variant="outline">{field}</Badge>
                <Button
                  size="sm"
                  variant="outline"
                  type="submit"
                  disabled={busy}
                  aria-busy={busy}
                  aria-label={`Release ${field} to the pipeline`}
                >
                  Release to pipeline
                </Button>
              </Form>
            ))}
          </div>
          <Separator />
          {rows.length === 0 ? (
            <span className="text-sm text-muted-foreground">
              No values decided yet.
            </span>
          ) : null}
          {rows.map((row) => (
            <div
              key={row.value_id}
              className="flex flex-wrap items-center gap-2 rounded-lg border p-2 text-sm"
            >
              <Badge variant="outline">{row.field}</Badge>
              <Badge variant="secondary">{row.source}</Badge>
              {row.is_live === 1 ? <Badge>live</Badge> : null}
              {row.value === null ? (
                <span className="text-muted-foreground italic">
                  released to pipeline
                </span>
              ) : (
                <span className="max-w-[70ch] whitespace-pre-wrap">
                  {row.value}
                </span>
              )}
              {row.source_ref === "" ? null : (
                <code className="font-mono text-xs text-muted-foreground">
                  {row.source_ref}
                </code>
              )}
              {row.note === "" ? null : (
                <span className="text-xs text-muted-foreground">
                  {row.note}
                </span>
              )}
              <span className="ml-auto text-xs text-muted-foreground">
                {row.decided_by} · {row.created_at}
              </span>
            </div>
          ))}
        </CardContent>
      </Card>
    </section>
  );
}

export function SeCompanyInfoReviewWorkspace({
  detail,
  result,
}: {
  detail: SeCompanyInfoDetail;
  result: SeCompanyInfoReviewResult;
}) {
  const { info, artifacts, suggestions } = detail;
  // One click is one decision: block every submit while one is in flight, so a
  // double-click cannot write two rows whose order then decides the field.
  const busy = useNavigation().state !== "idle";
  const contributing = new Set(info.description_source_record_uids);
  const groups = groupArtifactsBySource(artifacts);
  const proposals = [
    // The published row is the LLM's final composition (stored in
    // corpscout.se_company_info); it leads the menu so reviewers see the
    // outcome before the per-source raw material.
    //
    // Offered for EVERY published row, including one with no description at
    // all: that company is precisely the one a reviewer opens this page to
    // fix, and the option is where its editor lives. The card place-holds the
    // empty text.
    {
      key: "final:llm",
      source: "final",
      sourceLabel: "Final (LLM)",
      meta: "published",
      english: info.description ?? "",
      original: info.description_sv ?? "",
      originalLanguage: info.description_sv ? "sv" : "",
      // Composed by the pipeline out of several artifacts, so it names no
      // single record and no single moment.
      sourceRecordUid: "",
      observedAt: "",
    },
    ...descriptionProposals(artifacts),
  ];
  // A source with several options (ESEF files once a year) needs its meta line
  // in each button's accessible name, exactly as the menu needs it in each
  // option's label.
  const sourceCounts = new Map<string, number>();
  for (const proposal of proposals) {
    sourceCounts.set(
      proposal.source,
      (sourceCounts.get(proposal.source) ?? 0) + 1,
    );
  }

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
            {result.valueIds.length === 1
              ? "1 value row saved"
              : `${result.valueIds.length} value rows saved`}{" "}
            — published on the next rebuild.
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

      <CompanyDescriptionCard
        proposals={proposals}
        // A source's text is copied as-is; the published text is the one a
        // reviewer rewrites, so it gets the editor rather than a copy button.
        renderAction={(proposal, shown) =>
          proposal.source === "final" ? (
            <FinalDescriptionEditor info={info} busy={busy} />
          ) : (
            <UseSourceForm
              proposal={proposal}
              shown={shown}
              busy={busy}
              repeats={(sourceCounts.get(proposal.source) ?? 0) > 1}
            />
          )
        }
      />

      <CompanyFactsCard info={info} naceLabel={detail.naceLabel} />

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
            Newest first. Using one writes its wording as this company's value,
            in both languages when it has both. An older suggestion answered
            evidence this company no longer has, and can still be used.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          {suggestions.length === 0 ? (
            <span className="text-sm text-muted-foreground">
              No suggestions recorded.
            </span>
          ) : null}
          {suggestions.map((suggestion) => {
            const parsed = parseSuggestionText(suggestion.suggestion);
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
                {/* Every suggestion, not just the newest: a field value is a
                    value rather than an approval, so the model's older wording
                    is as usable as its latest -- the "superseded evidence"
                    badge above says which is which. */}
                <div className="mt-2">
                  <Form method="post" className="flex items-center gap-2">
                    <input
                      type="hidden"
                      name="intent"
                      value="use-suggestion"
                    />
                    <input
                      type="hidden"
                      name="suggestion_id"
                      value={suggestion.suggestion_id}
                    />
                    <Button
                      size="sm"
                      type="submit"
                      // A body the model left without a description is refused
                      // server-side ("That suggestion has no description.");
                      // saying so up front costs the reviewer no round trip.
                      disabled={busy || !parsed?.description}
                      aria-busy={busy}
                      aria-label={`Use suggestion ${suggestion.suggestion_id.slice(0, 8)}`}
                    >
                      Use this suggestion
                    </Button>
                  </Form>
                </div>
              </div>
            );
          })}
        </CardContent>
      </Card>

      <ValueHistoryCard rows={detail.fieldValues} busy={busy} />
    </div>
  );
}
