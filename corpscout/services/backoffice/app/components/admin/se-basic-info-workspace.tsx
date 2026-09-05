import { CheckCircle2Icon, FileSearchIcon, TriangleAlertIcon } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { Form, Link, useFetcher, useNavigation, useRevalidator } from "react-router";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "~/components/ui/accordion";
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
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { Dialog, DialogContent, DialogFooter, DialogHeader } from "~/components/ui/dialog";
import { Input } from "~/components/ui/input";
import { EMPTY_VALUE, text } from "~/components/admin/definition-list";
import { LegalForm } from "~/components/admin/legal-form";
import {
  BASIC_INFO_FIELDS,
  BASIC_INFO_SOURCES,
  basicInfoFieldKind,
  basicInfoFieldLabel,
  basicInfoSourceLabel,
  type SeBasicInfoField,
} from "~/lib/se-basic-info-fields";
import type {
  SeBasicInfoDetail,
  SeBasicInfoRow,
  SeBasicInfoSuggestionRow,
} from "~/lib/se-basic-info.server";
import { cn } from "~/lib/utils";

export type SeBasicInfoResult =
  | { ok: true; decidedAt: string }
  | { ok: true; launched: { runId: string; url: string | null } }
  | { ok: false; error: string }
  | null;

/** Shown when the company has neither a folded row nor a suggestion. */
export function SeBasicInfoNotFolded({ companyId }: { companyId: string }) {
  return (
    <div className="flex flex-col gap-6">
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <FileSearchIcon />
          </EmptyMedia>
          <EmptyTitle>Not folded yet</EmptyTitle>
          <EmptyDescription>
            Company {companyId} is not in se_company_basic_info yet and no source
            has suggested anything for it. The extractors write suggestions from
            the registers; the fold publishes the row.
          </EmptyDescription>
        </EmptyHeader>
        <EmptyContent>
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

function sourceOf(info: SeBasicInfoRow | null, field: SeBasicInfoField): string {
  return info ? info[`${field}_source`] : "";
}

function valueOf(
  row: SeBasicInfoRow | SeBasicInfoSuggestionRow | null,
  field: SeBasicInfoField,
): string {
  return row ? row[field] : "";
}

function FieldValue({
  field,
  value,
  language,
  labels,
}: {
  field: SeBasicInfoField;
  value: string;
  language: string;
  labels: SeBasicInfoDetail["legalFormLabels"];
}) {
  if (value === "") return EMPTY_VALUE;
  const kind = basicInfoFieldKind(field);
  if (kind === "code") {
    const label = labels[value] ?? { label_en: "", label_sv: "" };
    return <LegalForm form={{ code: value, ...label }} />;
  }
  if (kind === "paragraph") {
    return (
      <span className="whitespace-pre-line">
        {value}
        {language === "" ? null : (
          <Badge variant="outline" className="ml-2 align-middle">
            {language}
          </Badge>
        )}
      </span>
    );
  }
  if (kind === "identifier") return <span className="font-mono break-all">{value}</span>;
  return text(value);
}

function FieldsCard({
  detail,
  selectedField,
}: {
  detail: SeBasicInfoDetail;
  selectedField: SeBasicInfoField;
}) {
  const { info } = detail;
  return (
    <Card>
      <CardHeader>
        <CardTitle>Basic info</CardTitle>
        <CardDescription>
          The folded row: every value with the source that won it. Click a row to
          see what each source suggests for it. The header above still reads the
          old se_company_info row until slice 4 switches it.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {info ? null : (
          <Alert className="mb-4">
            <TriangleAlertIcon />
            <AlertTitle>Not folded yet</AlertTitle>
            <AlertDescription>
              Sources have suggested values but no fold has published this company.
            </AlertDescription>
          </Alert>
        )}
        {/* A list of links, not a <dl>: an anchor may not wrap dt/dd pairs. */}
        <ul className="grid grid-cols-1 gap-y-1 text-sm">
          {BASIC_INFO_FIELDS.map((field) => {
            const selected = field.name === selectedField;
            const source = sourceOf(info, field.name);
            return (
              <li key={field.name}>
                <Link
                  to={{ search: `?field=${field.name}` }}
                  preventScrollReset
                  aria-current={selected ? "true" : undefined}
                  className={cn(
                    "grid grid-cols-1 gap-x-6 rounded-md px-2 py-2 hover:bg-muted/60 sm:grid-cols-[minmax(11rem,auto)_1fr_auto]",
                    selected && "bg-muted",
                  )}
                >
                  <span className="text-muted-foreground text-xs uppercase tracking-wide sm:pt-0.5">
                    {field.label}
                  </span>
                  <span>
                    <FieldValue
                      field={field.name}
                      value={valueOf(info, field.name)}
                      language={field.name === "description" ? (info?.description_language ?? "") : ""}
                      labels={detail.legalFormLabels}
                    />
                  </span>
                  <span className="sm:text-right">
                    {source === "" ? null : (
                      <Badge variant="secondary">{basicInfoSourceLabel(source)}</Badge>
                    )}
                  </span>
                </Link>
              </li>
            );
          })}
        </ul>
        {info ? (
          <p className="text-muted-foreground mt-4 text-xs">
            Folded {info.folded_at} · {info.fold_version} · run{" "}
            <span className="font-mono">{info.source_run_id}</span>
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}

/** Effective precedence per source for one field: the company's active rule
 * when present, else the global row -- a rule may also rank a source the
 * global map does not. */
function effectivePrecedence(detail: SeBasicInfoDetail, field: SeBasicInfoField): Map<string, number> {
  const effective = new Map<string, number>();
  for (const row of detail.precedence) {
    if (row.field === field) effective.set(row.source, row.precedence);
  }
  for (const row of detail.rules) {
    if (row.field === field) effective.set(row.source, row.precedence);
  }
  return effective;
}

/** The panel's rows for one field: effective precedence order first, then any
 * suggesting source neither table ranks, then the rest of the catalogue. */
function panelSources(detail: SeBasicInfoDetail, field: SeBasicInfoField): string[] {
  const ranked = [...effectivePrecedence(detail, field).entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([source]) => source);
  const suggesting = detail.suggestions.map((row) => row.source);
  const ordered: string[] = [];
  for (const source of [...ranked, ...suggesting, ...BASIC_INFO_SOURCES]) {
    if (!ordered.includes(source)) ordered.push(source);
  }
  return ordered;
}

function SuggestionsPanel({
  companyId,
  detail,
  selectedField,
  result,
}: {
  companyId: string;
  detail: SeBasicInfoDetail;
  selectedField: SeBasicInfoField;
  result: SeBasicInfoResult;
}) {
  const navigation = useNavigation();
  // React Router's navigation.formMethod is lower- or upper-cased depending on
  // version, so compare case-insensitively; a GET revalidation must not read
  // as busy.
  const busy = navigation.state !== "idle" && (navigation.formMethod ?? "").toUpperCase() === "POST";
  // The decision being confirmed: Use this / Release open a dialog that shows
  // the value about to be written and takes the optional note; nothing posts
  // until the reviewer confirms there. A result from the action closes it.
  const [pending, setPending] = useState<PendingDecision | null>(null);
  useEffect(() => {
    if (result) setPending(null);
  }, [result]);
  const winner = sourceOf(detail.info, selectedField);
  const effective = effectivePrecedence(detail, selectedField);
  const rankedSources = new Set(effective.keys());
  // The rule in force for this field, if any: its source is the one the
  // reviewer prefers, whether or not the last fold already agrees.
  const ruledSource = detail.rules.find((row) => row.field === selectedField) ?? null;
  const rows = panelSources(detail, selectedField).map((source) => ({
    source,
    row: detail.suggestions.find((row) => row.source === source) ?? null,
  }));
  return (
    <Card>
      <CardHeader>
        <CardTitle>{basicInfoFieldLabel(selectedField)}</CardTitle>
        <CardDescription>
          What each source suggests, highest precedence first. The active one is
          what the fold published.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        {detail.foldPending ? (
          <Alert>
            <TriangleAlertIcon />
            <AlertTitle>Fold pending</AlertTitle>
            <AlertDescription>
              A suggestion is newer than the last fold. Fold now publishes it.
              <Form method="post" className="mt-2">
                <input type="hidden" name="intent" value="fold-now" />
                <Button type="submit" size="sm" disabled={busy}>
                  Fold now
                </Button>
              </Form>
            </AlertDescription>
          </Alert>
        ) : null}
        {result && result.ok && "launched" in result ? (
          // Keyed by run: a relaunch after the ten-minute cap must start a fresh
          // poller (new start instant, timed-out flag cleared), not reuse the old one.
          <FoldRunPoller
            key={result.launched.runId}
            companyId={companyId}
            runId={result.launched.runId}
            url={result.launched.url}
            foldPending={detail.foldPending}
          />
        ) : null}
        {result && !result.ok ? (
          <Alert variant="destructive">
            <TriangleAlertIcon />
            <AlertTitle>Not saved</AlertTitle>
            <AlertDescription>{result.error}</AlertDescription>
          </Alert>
        ) : null}
        {result && result.ok && "decidedAt" in result ? (
          <Alert>
            <CheckCircle2Icon />
            <AlertTitle>Decision saved</AlertTitle>
            <AlertDescription>Rule written at {result.decidedAt}. Fold now to publish it.</AlertDescription>
          </Alert>
        ) : null}
        <ul className="flex flex-col gap-2">
          {rows.map(({ source, row }) => {
            const value = valueOf(row, selectedField);
            // The fold's current, published winner for this field -- distinct
            // from `ruled` below, which is about the pending rule.
            const foldedActive = source !== "" && source === winner;
            const hasValue = value !== "";
            // This source holds the field's active rule: it is what the next
            // fold will publish regardless of the global precedence map.
            const ruled = ruledSource !== null && source === ruledSource.source;
            // A source with a value but no precedence rank for this field can
            // only win through Use this -- the fold will never pick it on its own.
            const notRanked = hasValue && !rankedSources.has(source);
            return (
              <li
                key={source}
                data-source={source}
                className={cn(
                  "rounded-md border p-3 text-sm",
                  foldedActive && "border-primary bg-primary/5",
                  !hasValue && "text-muted-foreground opacity-70",
                )}
              >
                <div className="flex items-center gap-2">
                  <span className="font-medium">{basicInfoSourceLabel(source)}</span>
                  {notRanked ? (
                    <span className="text-muted-foreground text-xs">not ranked</span>
                  ) : null}
                  {foldedActive ? <Badge>Active</Badge> : null}
                  {ruled ? <Badge variant="outline">preferred by reviewer</Badge> : null}
                  {row ? (
                    <span className="text-muted-foreground ml-auto text-xs">{row.observed_at}</span>
                  ) : null}
                </div>
                <div className="mt-1">
                  {hasValue ? (
                    <FieldValue
                      field={selectedField}
                      value={value}
                      language={selectedField === "description" ? (row?.description_language ?? "") : ""}
                      labels={detail.legalFormLabels}
                    />
                  ) : (
                    <span>no opinion</span>
                  )}
                </div>
                {row?.note ? <p className="text-muted-foreground mt-1 text-xs">{row.note}</p> : null}
                {ruled && ruledSource?.note ? (
                  <p className="text-muted-foreground mt-1 text-xs">{ruledSource.note}</p>
                ) : null}
                {hasValue && !ruled ? (
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    className="mt-2"
                    disabled={busy}
                    onClick={() =>
                      setPending({
                        intent: "use-this",
                        field: selectedField,
                        source,
                        value,
                        language: selectedField === "description" ? (row?.description_language ?? "") : "",
                      })
                    }
                  >
                    Use this
                  </Button>
                ) : null}
                {ruled ? (
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    className="mt-2"
                    disabled={busy}
                    onClick={() =>
                      setPending({
                        intent: "release",
                        field: selectedField,
                        source,
                        value,
                        language: selectedField === "description" ? (row?.description_language ?? "") : "",
                      })
                    }
                  >
                    Release
                  </Button>
                ) : null}
              </li>
            );
          })}
        </ul>
        <DecisionDialog
          pending={pending}
          labels={detail.legalFormLabels}
          busy={busy}
          onClose={() => setPending(null)}
        />
      </CardContent>
    </Card>
  );
}

/** What a Use this / Release click proposes, shown for confirmation before it posts. */
export interface PendingDecision {
  intent: "use-this" | "release";
  field: SeBasicInfoField;
  /** The suggesting source for Use this; the ruled source for Release. */
  source: string;
  value: string;
  language: string;
}

/**
 * The confirmation's form: what will be written, for which field, from which
 * source, plus the optional note. Portal-free so tests can render it statically;
 * `DecisionDialog` wraps it in the dialog chrome.
 */
export function DecisionDialogBody({
  pending,
  labels,
  busy,
  onClose,
}: {
  pending: PendingDecision;
  labels: SeBasicInfoDetail["legalFormLabels"];
  busy: boolean;
  onClose: () => void;
}) {
  const release = pending.intent === "release";
  return (
    <Form method="post" onSubmit={onClose} className="flex flex-col gap-4">
      {/* Plain heading and paragraph, not DialogTitle/DialogDescription: those
          need the dialog root's context, and this body also renders on its own
          in tests. */}
      <DialogHeader>
        <h2 className="text-base font-semibold leading-none">
          {release ? "Release " : "Use this "}
          {basicInfoFieldLabel(pending.field).toLowerCase()}
        </h2>
        <p className="text-muted-foreground text-sm">
          {release
            ? `The rule preferring ${basicInfoSourceLabel(pending.source)} for this field is withdrawn and the next fold applies the normal precedence.`
            : `The next fold prefers ${basicInfoSourceLabel(pending.source)}'s value for this field once the rule is applied.`}
        </p>
      </DialogHeader>
      <div className="rounded-md border p-3 text-sm" data-testid="decision-value">
        <div className="text-muted-foreground mb-1 text-xs">
          {basicInfoSourceLabel(pending.source)}
        </div>
        <FieldValue field={pending.field} value={pending.value} language={pending.language} labels={labels} />
      </div>
      <input type="hidden" name="intent" value={pending.intent} />
      <input type="hidden" name="field" value={pending.field} />
      <input type="hidden" name="source" value={pending.source} />
      <label className="flex flex-col gap-1 text-sm" htmlFor="basic-info-note">
        <span className="text-muted-foreground text-xs">Why this value (optional)</span>
        <Input id="basic-info-note" name="note" maxLength={500} placeholder="Note saved with the decision" />
      </label>
      <DialogFooter>
        <Button type="button" variant="outline" onClick={onClose} disabled={busy}>
          Cancel
        </Button>
        <Button type="submit" disabled={busy}>
          {release ? "Release" : "Use this"}
        </Button>
      </DialogFooter>
    </Form>
  );
}

function DecisionDialog({
  pending,
  labels,
  busy,
  onClose,
}: {
  pending: PendingDecision | null;
  labels: SeBasicInfoDetail["legalFormLabels"];
  busy: boolean;
  onClose: () => void;
}) {
  return (
    <Dialog open={pending !== null} onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogContent>
        {pending ? (
          <DecisionDialogBody pending={pending} labels={labels} busy={busy} onClose={onClose} />
        ) : null}
      </DialogContent>
    </Dialog>
  );
}

/** Stop polling and tell the reviewer to check Dagster directly after this long. */
const POLL_TIMEOUT_MS = 600_000;
const POLL_INTERVAL_MS = 3000;

/** True only when it is safe to assume the tab is in front of someone --
 * skips polling a backgrounded tab. `document` guard first: this runs from an
 * effect, so it is always browser-side, but the check stays defensive. */
function tabIsVisible(): boolean {
  return typeof document !== "undefined" && document.visibilityState === "visible";
}

/** Polls the run resource route until the fold finishes, then reloads the page.
 * Stops (and tells the reviewer to check Dagster) after ten minutes, and never
 * polls a backgrounded tab. Disappears once the fold has finished and nothing
 * is pending; a failed or canceled run keeps the alert up with its status. */
function FoldRunPoller({
  companyId,
  runId,
  url,
  foldPending,
}: {
  companyId: string;
  runId: string;
  url: string | null;
  foldPending: boolean;
}) {
  const fetcher = useFetcher<{ status: string; finished: boolean }>();
  const revalidator = useRevalidator();
  const finished = fetcher.data?.finished ?? false;
  const startedAtRef = useRef(Date.now());
  const [timedOut, setTimedOut] = useState(false);
  useEffect(() => {
    if (finished) {
      revalidator.revalidate();
      return;
    }
    const path = `/admin/se/company/${encodeURIComponent(companyId)}/info/run/${encodeURIComponent(runId)}`;
    if (tabIsVisible()) fetcher.load(path);
    const timer = setInterval(() => {
      if (Date.now() - startedAtRef.current > POLL_TIMEOUT_MS) {
        clearInterval(timer);
        setTimedOut(true);
        return;
      }
      if (tabIsVisible()) fetcher.load(path);
    }, POLL_INTERVAL_MS);
    return () => clearInterval(timer);
    // fetcher is stable per React Router's contract; re-run only on identity changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [companyId, runId, finished]);

  if (finished && !foldPending) return null;

  const dagsterLink = url ? (
    <>
      {" "}
      (<a className="underline" href={url} target="_blank" rel="noreferrer">open in Dagster</a>)
    </>
  ) : null;

  if (timedOut) {
    return (
      <Alert>
        <TriangleAlertIcon />
        <AlertTitle>Still running</AlertTitle>
        <AlertDescription>
          Fold not finished after 10 minutes; open it in Dagster.
          {dagsterLink}
        </AlertDescription>
      </Alert>
    );
  }

  return (
    <Alert>
      <CheckCircle2Icon />
      <AlertTitle>{finished ? `Fold ${fetcher.data?.status?.toLowerCase() ?? "finished"}` : "Folding"}</AlertTitle>
      <AlertDescription>
        Run <span className="font-mono">{runId}</span>
        {dagsterLink}
        {finished ? " -- reloading." : " -- the page reloads when it finishes."}
      </AlertDescription>
    </Alert>
  );
}

function HistoryCard({ detail }: { detail: SeBasicInfoDetail }) {
  return (
    <Card>
      <Accordion>
        <AccordionItem value="history" className="border-0">
          <CardHeader>
            <AccordionTrigger className="py-0">
              <div className="text-left">
                <CardTitle>History</CardTitle>
                <CardDescription>
                  Every fold that changed a value, newest first ({detail.history.length}).
                </CardDescription>
              </div>
            </AccordionTrigger>
          </CardHeader>
          <AccordionContent>
            <CardContent>
              {detail.history.length === 0 ? (
                <p className="text-muted-foreground text-sm">No fold has published this company yet.</p>
              ) : (
                <ul className="flex flex-col gap-2 text-sm">
                  {detail.history.map((row) => (
                    <li key={`${row.folded_at}-${row.source_run_id}`} className="grid gap-x-4 sm:grid-cols-[auto_1fr_auto]">
                      <span className="font-mono text-xs">{row.folded_at}</span>
                      <span>
                        {row.changed_fields.map((field) => (
                          <Badge key={field} variant="outline" className="mr-1">
                            {field}
                          </Badge>
                        ))}
                      </span>
                      <span className="text-muted-foreground text-xs">{row.fold_version}</span>
                    </li>
                  ))}
                </ul>
              )}
            </CardContent>
          </AccordionContent>
        </AccordionItem>
      </Accordion>
    </Card>
  );
}

export function SeBasicInfoWorkspace({
  companyId,
  detail,
  selectedField,
  result,
}: {
  companyId: string;
  detail: SeBasicInfoDetail;
  selectedField: SeBasicInfoField;
  result: SeBasicInfoResult;
}) {
  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,2fr)_minmax(20rem,1fr)]">
      <div className="flex flex-col gap-6">
        <FieldsCard detail={detail} selectedField={selectedField} />
        <HistoryCard detail={detail} />
      </div>
      <aside className="lg:sticky lg:top-4 lg:self-start">
        <SuggestionsPanel companyId={companyId} detail={detail} selectedField={selectedField} result={result} />
      </aside>
    </div>
  );
}
