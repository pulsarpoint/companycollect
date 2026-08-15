import { useEffect, useState } from "react";
import { Form, Link, useFetcher, useNavigation } from "react-router";
import {
  CheckCircle2,
  History,
  ShieldCheck,
  TriangleAlert,
} from "lucide-react";
import type {
  CountryPersonCorrectionKind,
  CountryPersonCorrectionReview,
  CountryPersonObservation,
  CountryPersonSummary,
} from "~/lib/people.server";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { Checkbox } from "~/components/ui/checkbox";
import {
  Combobox,
  ComboboxContent,
  ComboboxEmpty,
  ComboboxInput,
  ComboboxItem,
  ComboboxList,
} from "~/components/ui/combobox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "~/components/ui/dialog";
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
  FieldGroup,
  FieldLabel,
} from "~/components/ui/field";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "~/components/ui/tabs";
import { Textarea } from "~/components/ui/textarea";

const ROLE_LABELS: Record<string, string> = {
  chairman: "Chairman",
  ceo: "CEO",
  board_member: "Board member",
  deputy_board_member: "Deputy board member",
  liquidator: "Liquidator",
  auditor: "External auditor",
  other: "Other",
  unknown: "Report signatory",
};

const CORRECTION_LABELS: Record<CountryPersonCorrectionKind, string> = {
  reassign: "Reassigned observations",
  split: "Split identity",
  merge: "Merged identities",
  undo: "Undid correction",
};

interface CorrectionSubmitted {
  correctionId: string;
  targetPersonId: string;
  correctionCount: number;
}

interface PersonCorrectionWorkspaceProps {
  countryCode: string;
  sourcePersonId: string;
  observationCount: number;
  observations: CountryPersonObservation[];
  correctionReviews: CountryPersonCorrectionReview[];
  actionError: string | null;
  correctionSubmitted: CorrectionSubmitted | null;
}

export function PersonCorrectionWorkspace({
  countryCode,
  sourcePersonId,
  observationCount,
  observations,
  correctionReviews,
  actionError,
  correctionSubmitted,
}: PersonCorrectionWorkspaceProps) {
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const allSelected =
    observations.length > 0 && selected.size === observations.length;

  function setObservationSelected(
    observationId: string,
    checked: boolean,
  ): void {
    setSelected((current) => {
      const next = new Set(current);
      if (checked) next.add(observationId);
      else next.delete(observationId);
      return next;
    });
  }

  function setAllSelected(checked: boolean): void {
    setSelected(
      checked
        ? new Set(observations.map((observation) => observation.observation_id))
        : new Set(),
    );
  }

  return (
    <>
      {actionError ? (
        <Alert variant="destructive">
          <TriangleAlert />
          <AlertTitle>Correction was not recorded</AlertTitle>
          <AlertDescription>{actionError}</AlertDescription>
        </Alert>
      ) : null}
      {correctionSubmitted ? (
        <Alert>
          <CheckCircle2 />
          <AlertTitle>Correction recorded</AlertTitle>
          <AlertDescription>
            Review {correctionSubmitted.correctionId} contains{" "}
            {correctionSubmitted.correctionCount} decision
            {correctionSubmitted.correctionCount === 1 ? "" : "s"}. Dagster will
            apply it shortly. Target: {countryCode.toUpperCase()}:
            {correctionSubmitted.targetPersonId}.
          </AlertDescription>
        </Alert>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle>Source observations</CardTitle>
          <CardDescription>
            Select the raw occurrences that need review. Their source values are
            never changed or deleted.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>
                  <Checkbox
                    aria-label="Select all observations"
                    checked={allSelected}
                    onCheckedChange={(checked) => setAllSelected(checked)}
                  />
                </TableHead>
                <TableHead>Year</TableHead>
                <TableHead>Company</TableHead>
                <TableHead>Observed name</TableHead>
                <TableHead>Role</TableHead>
                <TableHead>Resolution</TableHead>
                <TableHead>Provenance</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {observations.map((observation) => (
                <TableRow key={observation.observation_id}>
                  <TableCell>
                    <Checkbox
                      aria-label={`Select ${observation.observed_full_name} from ${observation.fiscal_year}`}
                      checked={selected.has(observation.observation_id)}
                      onCheckedChange={(checked) =>
                        setObservationSelected(
                          observation.observation_id,
                          checked,
                        )
                      }
                    />
                  </TableCell>
                  <TableCell className="tabular-nums">
                    {observation.fiscal_year || "—"}
                  </TableCell>
                  <TableCell>
                    <Link
                      to={`/company/${countryCode}/${observation.company_id}`}
                      className="font-medium hover:underline"
                    >
                      {observation.company_name || observation.company_id}
                    </Link>
                  </TableCell>
                  <TableCell>{observation.observed_full_name}</TableCell>
                  <TableCell>
                    <div className="flex flex-col gap-1">
                      <Badge variant="outline">
                        {ROLE_LABELS[observation.role_kind] ??
                          observation.role_kind}
                      </Badge>
                      {observation.role_original ? (
                        <span className="text-muted-foreground text-xs">
                          {observation.role_original}
                        </span>
                      ) : null}
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-col gap-1">
                      <Badge variant="outline">
                        {observation.match_status}
                      </Badge>
                      <span className="text-muted-foreground text-xs">
                        {observation.match_method} · {observation.confidence}%
                      </span>
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-col gap-1 font-mono text-xs">
                      <span>{observation.source}</span>
                      <span
                        className="text-muted-foreground"
                        title={observation.source_statement_key}
                      >
                        statement{" "}
                        {observation.source_statement_key.slice(0, 12)}… ·{" "}
                        {observation.source_person_key}
                      </span>
                      {observation.fiscal_year > 0 ? (
                        <Link
                          to={`/company/${countryCode}/${observation.company_id}/facts/${observation.fiscal_year}`}
                          className="font-sans hover:underline"
                        >
                          View filing facts
                        </Link>
                      ) : null}
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Review identity assignment</CardTitle>
          <CardDescription>
            Decisions stay inside {countryCode.toUpperCase()} and are appended
            to an immutable audit ledger.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <Alert>
            <ShieldCheck />
            <AlertTitle>
              {selected.size} observation{selected.size === 1 ? "" : "s"}{" "}
              selected
            </AlertTitle>
            <AlertDescription>
              Published source identifiers cannot be overridden here. Pending
              decisions must be applied before another review.
            </AlertDescription>
          </Alert>
          <Tabs defaultValue="reassign">
            <TabsList>
              <TabsTrigger value="reassign">Reassign</TabsTrigger>
              <TabsTrigger value="split">Split</TabsTrigger>
              <TabsTrigger value="merge">Merge</TabsTrigger>
            </TabsList>
            <TabsContent value="reassign">
              <CorrectionForm
                kind="reassign"
                countryCode={countryCode}
                sourcePersonId={sourcePersonId}
                selectedObservationIds={selected}
                observationCount={observationCount}
              />
            </TabsContent>
            <TabsContent value="split">
              <CorrectionForm
                kind="split"
                countryCode={countryCode}
                sourcePersonId={sourcePersonId}
                selectedObservationIds={selected}
                observationCount={observationCount}
              />
            </TabsContent>
            <TabsContent value="merge">
              <CorrectionForm
                kind="merge"
                countryCode={countryCode}
                sourcePersonId={sourcePersonId}
                selectedObservationIds={selected}
                observationCount={observationCount}
              />
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>

      <CorrectionHistory
        sourcePersonId={sourcePersonId}
        reviews={correctionReviews}
      />
    </>
  );
}

function CorrectionForm({
  kind,
  countryCode,
  sourcePersonId,
  selectedObservationIds,
  observationCount,
}: {
  kind: "reassign" | "split" | "merge";
  countryCode: string;
  sourcePersonId: string;
  selectedObservationIds: Set<string>;
  observationCount: number;
}) {
  const [targetPersonId, setTargetPersonId] = useState("");
  const navigation = useNavigation();
  const submitting = navigation.state === "submitting";
  const requiresSelection = kind !== "merge";
  const targetRequired = kind !== "split";
  const disabled =
    submitting ||
    (requiresSelection && selectedObservationIds.size === 0) ||
    (targetRequired && targetPersonId === "");
  const preview =
    kind === "merge"
      ? `All ${observationCount} observation${observationCount === 1 ? "" : "s"} and this person ID will move to the target identity.`
      : kind === "split"
        ? `${selectedObservationIds.size} selected observation${selectedObservationIds.size === 1 ? "" : "s"} will receive a new country-scoped person ID.`
        : `${selectedObservationIds.size} selected observation${selectedObservationIds.size === 1 ? "" : "s"} will move to the target identity.`;

  return (
    <Form method="post">
      <input type="hidden" name="correction_kind" value={kind} />
      {requiresSelection
        ? Array.from(selectedObservationIds).map((observationId) => (
            <input
              key={observationId}
              type="hidden"
              name="observation_id"
              value={observationId}
            />
          ))
        : null}
      <FieldGroup>
        {targetRequired ? (
          <Field>
            <FieldLabel htmlFor={`${kind}-target`}>Target person</FieldLabel>
            <TargetPersonPicker
              id={`${kind}-target`}
              countryCode={countryCode}
              sourcePersonId={sourcePersonId}
              onTargetChange={setTargetPersonId}
            />
            <FieldDescription>
              Search by name or paste a person ID. Only active identities in{" "}
              {countryCode.toUpperCase()} are available.
            </FieldDescription>
          </Field>
        ) : null}
        <Field>
          <FieldLabel htmlFor={`${kind}-reason`}>Reason</FieldLabel>
          <Textarea
            id={`${kind}-reason`}
            name="reason"
            minLength={2}
            maxLength={1000}
            required
          />
          <FieldDescription>{preview}</FieldDescription>
        </Field>
        <Button type="submit" disabled={disabled}>
          {submitting ? "Recording…" : `Record ${kind}`}
        </Button>
      </FieldGroup>
    </Form>
  );
}

interface TargetSearchResponse {
  rows: CountryPersonSummary[];
  error: string | null;
}

function TargetPersonPicker({
  id,
  countryCode,
  sourcePersonId,
  onTargetChange,
}: {
  id: string;
  countryCode: string;
  sourcePersonId: string;
  onTargetChange: (personId: string) => void;
}) {
  const fetcher = useFetcher<TargetSearchResponse>();
  const [query, setQuery] = useState("");
  const [selectedPerson, setSelectedPerson] =
    useState<CountryPersonSummary | null>(null);

  useEffect(() => {
    const normalized = query.trim();
    if (
      normalized.length < 2 ||
      normalized === selectedPerson?.preferred_name
    ) {
      return;
    }
    const timer = window.setTimeout(() => {
      const search = new URLSearchParams({
        q: normalized,
        source: sourcePersonId,
      });
      void fetcher.load(
        `/country/${countryCode}/person-targets?${search.toString()}`,
      );
    }, 250);
    return () => window.clearTimeout(timer);
  }, [countryCode, fetcher.load, query, selectedPerson, sourcePersonId]);

  const results = fetcher.data?.rows ?? [];
  const emptyMessage =
    query.trim().length < 2
      ? "Enter at least two characters."
      : fetcher.state !== "idle"
        ? "Searching…"
        : "No active person matches this search.";

  return (
    <Combobox
      items={results}
      filteredItems={results}
      filter={null}
      name="target_person_id"
      required
      value={selectedPerson}
      inputValue={query}
      itemToStringLabel={(person) => person.preferred_name}
      itemToStringValue={(person) => person.person_id}
      isItemEqualToValue={(person, value) =>
        person.person_id === value.person_id
      }
      onInputValueChange={(value, details) => {
        setQuery(value);
        if (
          details.reason === "input-change" ||
          details.reason === "input-clear" ||
          details.reason === "clear-press"
        ) {
          setSelectedPerson(null);
          onTargetChange("");
        }
      }}
      onValueChange={(person) => {
        setSelectedPerson(person);
        setQuery(person?.preferred_name ?? "");
        onTargetChange(person?.person_id ?? "");
      }}
    >
      <ComboboxInput
        id={id}
        placeholder="Search by name or person ID"
        showClear
      />
      <ComboboxContent>
        <ComboboxEmpty>{emptyMessage}</ComboboxEmpty>
        <ComboboxList>
          {(person: CountryPersonSummary) => (
            <ComboboxItem key={person.person_id} value={person}>
              <div className="flex min-w-0 flex-1 flex-col gap-0.5 py-1">
                <div className="flex items-center gap-2">
                  <span className="truncate font-medium">
                    {person.preferred_name}
                  </span>
                  <Badge variant="outline">{person.resolution_status}</Badge>
                </div>
                <span className="text-muted-foreground text-xs">
                  {person.company_count} compan
                  {person.company_count === 1 ? "y" : "ies"} ·{" "}
                  {person.observation_count} observation
                  {person.observation_count === 1 ? "" : "s"}
                </span>
                <span className="text-muted-foreground truncate font-mono text-xs">
                  {person.person_id}
                </span>
              </div>
            </ComboboxItem>
          )}
        </ComboboxList>
      </ComboboxContent>
    </Combobox>
  );
}

function CorrectionHistory({
  sourcePersonId,
  reviews,
}: {
  sourcePersonId: string;
  reviews: CountryPersonCorrectionReview[];
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Correction history</CardTitle>
        <CardDescription>
          Every review remains visible after it is applied or superseded.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {reviews.length === 0 ? (
          <Empty>
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <History />
              </EmptyMedia>
              <EmptyTitle>No reviewed corrections</EmptyTitle>
              <EmptyDescription>
                This profile currently reflects only source and resolver
                evidence.
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <ul className="divide-y">
            {reviews.map((review) => (
              <li key={review.review_id} className="flex flex-col gap-2 py-3">
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant="outline">
                    {CORRECTION_LABELS[review.correction_kind]}
                  </Badge>
                  <Badge variant={review.is_applied ? "secondary" : "outline"}>
                    {!review.is_current
                      ? "Superseded"
                      : review.is_applied
                        ? "Applied"
                        : "Pending Dagster"}
                  </Badge>
                  <span className="text-muted-foreground font-mono text-xs">
                    {review.review_id}
                  </span>
                </div>
                <p className="text-sm">{review.reason}</p>
                <p className="text-muted-foreground text-xs">
                  {review.decided_by} · {review.created_at} UTC ·{" "}
                  {review.corrections.length} decision
                  {review.corrections.length === 1 ? "" : "s"}
                </p>
                {review.is_current && review.is_applied ? (
                  <UndoCorrectionDialog
                    sourcePersonId={sourcePersonId}
                    review={review}
                  />
                ) : null}
              </li>
            ))}
          </ul>
        )}
      </CardContent>
    </Card>
  );
}

function UndoCorrectionDialog({
  sourcePersonId,
  review,
}: {
  sourcePersonId: string;
  review: CountryPersonCorrectionReview;
}) {
  return (
    <Dialog>
      <DialogTrigger render={<Button variant="outline" size="sm" />}>
        Undo review
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Undo reviewed correction</DialogTitle>
          <DialogDescription>
            A new immutable decision will restore the assignment that preceded
            review {review.review_id}. Existing history will remain unchanged.
          </DialogDescription>
        </DialogHeader>
        <Form method="post">
          <input type="hidden" name="correction_kind" value="undo" />
          <input type="hidden" name="review_id" value={review.review_id} />
          <input type="hidden" name="source_person_id" value={sourcePersonId} />
          <FieldGroup>
            <Field>
              <FieldLabel htmlFor={`undo-reason-${review.review_id}`}>
                Reason
              </FieldLabel>
              <Textarea
                id={`undo-reason-${review.review_id}`}
                name="reason"
                minLength={2}
                maxLength={1000}
                required
              />
              <FieldDescription>
                This action appends a new immutable backoffice decision.
              </FieldDescription>
            </Field>
            <DialogFooter>
              <Button type="submit" variant="destructive">
                Record undo
              </Button>
            </DialogFooter>
          </FieldGroup>
        </Form>
      </DialogContent>
    </Dialog>
  );
}
