import { CheckCircle2Icon, TriangleAlertIcon } from "lucide-react";
import { Form, Link } from "react-router";
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
import type { SeCompanyPersonDetail } from "~/lib/se-company-person.server";

export type SePersonReviewResult =
  | { ok: true; correctionId: string }
  | { ok: false; error: string }
  | null;

/**
 * Every correction form posts the kind it represents plus the evidence hash the
 * reviewer actually saw, so a concurrent rebuild is rejected server-side rather
 * than silently applied to different evidence.
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

function DraftCheckboxes({
  drafts,
}: {
  drafts: SeCompanyPersonDetail["drafts"];
}) {
  if (drafts.length === 0) {
    return (
      <span className="text-sm text-muted-foreground">
        No source observations to select.
      </span>
    );
  }
  return (
    <div className="flex flex-col gap-1.5">
      {drafts.map((draft) => (
        <label
          key={draft.draft_id}
          className="flex items-start gap-2 text-sm leading-5"
        >
          <Checkbox
            name="draft_id"
            value={draft.draft_id}
            className="mt-0.5"
          />
          <span>
            {draft.source} · {draft.name} · {draft.role_original || "—"} ·{" "}
            {draft.fiscal_year ?? "undated"}
          </span>
        </label>
      ))}
    </div>
  );
}

export function SePersonReviewWorkspace({
  detail,
  activeRoleCodes,
  result,
}: {
  detail: SeCompanyPersonDetail;
  activeRoleCodes: string[];
  result: SePersonReviewResult;
}) {
  const { person, drafts, roles, suggestions, corrections } = detail;
  const hash = person.draft_set_hash;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline">ClickHouse</Badge>
          <Badge variant="secondary">
            {person.model_provider} · {person.model_name}
          </Badge>
          {person.correction_ids.length > 0 ? <Badge>reviewed</Badge> : null}
          {person.merged_into_person_id ? (
            <Badge variant="destructive">
              merged into {person.merged_into_person_id}
            </Badge>
          ) : null}
        </div>
        <h1 className="text-2xl font-semibold tracking-tight">{person.name}</h1>
        <p className="text-sm text-muted-foreground">
          Company{" "}
          <Link
            className="underline"
            to={`/company/se/${encodeURIComponent(person.company_id)}`}
          >
            {person.company_id}
          </Link>{" "}
          · {drafts.length} source observations · evidence{" "}
          <code className="font-mono text-xs">{hash.slice(0, 12)}</code> ·
          prompt {person.prompt_version} · updated {person.updated_at}
        </p>
        {person.description ? (
          <p className="text-sm">{person.description}</p>
        ) : null}
      </header>

      {result?.ok ? (
        <Alert>
          <CheckCircle2Icon />
          <AlertTitle>Saved</AlertTitle>
          <AlertDescription>
            Correction {result.correctionId} is in the ledger. Dagster will
            re-run company {person.company_id} within a minute; reload to see
            the result.
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
          <CardTitle>Source observations</CardTitle>
          <CardDescription>
            The Draft rows this published person was built from. Their set is
            what the evidence hash covers.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Source</TableHead>
                <TableHead>Name</TableHead>
                <TableHead>Role</TableHead>
                <TableHead>Year</TableHead>
                <TableHead>Observed</TableHead>
                <TableHead>Draft</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {drafts.map((draft) => (
                <TableRow key={draft.draft_id}>
                  <TableCell>{draft.source}</TableCell>
                  <TableCell>{draft.name}</TableCell>
                  <TableCell>{draft.role_original || "—"}</TableCell>
                  <TableCell className="tabular-nums">
                    {draft.fiscal_year ?? "—"}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {draft.source_observed_at}
                  </TableCell>
                  <TableCell className="font-mono text-xs">
                    {draft.draft_id}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Roles</CardTitle>
          <CardDescription>
            Canonical role rows derived for this person. Superseded rows stay
            visible as outlines.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-2">
          {roles.length === 0 ? (
            <span className="text-sm text-muted-foreground">
              No role rows.
            </span>
          ) : null}
          {roles.map((role) => (
            <Badge
              key={role.role_id}
              variant={role.is_current ? "default" : "outline"}
            >
              {role.role_code} · {role.fiscal_year ?? "undated"}
              {role.correction_ids.length > 0 ? " · corrected" : ""}
            </Badge>
          ))}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Model suggestions</CardTitle>
          <CardDescription>
            Newest first. Approve pins one; reject falls back to the
            deterministic name.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          {suggestions.length === 0 ? (
            <span className="text-sm text-muted-foreground">
              No suggestions recorded.
            </span>
          ) : null}
          {suggestions.map((suggestion) => (
            <div
              key={suggestion.suggestion_id}
              className="rounded-lg border p-3"
            >
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="outline">
                  {suggestion.model_provider} · {suggestion.model_name}
                </Badge>
                {suggestion.is_published ? <Badge>published</Badge> : null}
                <span className="text-xs text-muted-foreground">
                  {suggestion.created_at}
                </span>
              </div>
              <pre className="mt-2 overflow-x-auto whitespace-pre-wrap break-words font-mono text-xs">
                {suggestion.suggestion}
              </pre>
              <div className="mt-2 flex flex-wrap gap-2">
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
                        required
                        className="w-56"
                      />
                      {kind === "reject_suggestion" ? (
                        <Input
                          name="note"
                          placeholder="Note (optional)"
                          className="w-48"
                        />
                      ) : null}
                      <Button
                        size="sm"
                        variant={
                          kind === "approve_suggestion" ? "default" : "outline"
                        }
                        type="submit"
                      >
                        {kind === "approve_suggestion" ? "Approve" : "Reject"}
                      </Button>
                    </Form>
                  ),
                )}
              </div>
            </div>
          ))}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Correct this person</CardTitle>
          <CardDescription>
            Every decision is appended to the correction ledger with a reason;
            nothing here rewrites the published row directly.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-6 lg:grid-cols-2">
          <Form method="post" className="flex flex-col gap-2">
            <HiddenCommon kind="override_field" evidenceHash={hash} />
            <h3 className="text-sm font-medium">Override name / description</h3>
            <Input name="name" defaultValue={person.name} aria-label="Name" />
            <Textarea
              name="description"
              defaultValue={person.description ?? ""}
              aria-label="Description"
              placeholder="Description"
            />
            <Input name="reason" placeholder="Reason" required />
            <Button type="submit">Save override</Button>
          </Form>

          <Form method="post" className="flex flex-col gap-2">
            <HiddenCommon kind="merge_persons" evidenceHash={hash} />
            <h3 className="text-sm font-medium">
              Merge into another person (same company)
            </h3>
            <Input
              name="target_person_id"
              placeholder="Target person_id (UUID)"
              required
            />
            <Input name="reason" placeholder="Reason" required />
            <Button type="submit" variant="outline">
              Merge
            </Button>
          </Form>

          <Form method="post" className="flex flex-col gap-2">
            <HiddenCommon kind="reassign_draft" evidenceHash={hash} />
            <h3 className="text-sm font-medium">
              Move one observation to another person
            </h3>
            <DraftCheckboxes drafts={drafts} />
            <Input
              name="target_person_id"
              placeholder="Target person_id (UUID)"
              required
            />
            <Input name="reason" placeholder="Reason" required />
            <Button type="submit" variant="outline">
              Reassign
            </Button>
          </Form>

          <Form method="post" className="flex flex-col gap-2">
            <HiddenCommon kind="split_person" evidenceHash={hash} />
            <h3 className="text-sm font-medium">
              Split observations into a new person
            </h3>
            <DraftCheckboxes drafts={drafts} />
            <Input name="name" placeholder="Name of the new person" required />
            <Input name="reason" placeholder="Reason" required />
            <Button type="submit" variant="outline">
              Split
            </Button>
          </Form>

          <Form method="post" className="flex flex-col gap-2">
            <HiddenCommon kind="set_role" evidenceHash={hash} />
            <h3 className="text-sm font-medium">Set role for observations</h3>
            <DraftCheckboxes drafts={drafts} />
            <select
              name="role_code"
              aria-label="Canonical role"
              className="h-8 rounded-lg border border-input bg-transparent px-2.5 text-sm"
              required
            >
              {activeRoleCodes.map((code) => (
                <option key={code} value={code}>
                  {code}
                </option>
              ))}
            </select>
            <Input
              name="fiscal_year"
              placeholder="Fiscal year (optional)"
              inputMode="numeric"
            />
            <Input name="reason" placeholder="Reason" required />
            <Button type="submit" variant="outline">
              Set role
            </Button>
          </Form>

          <Form method="post" className="flex flex-col gap-2">
            <HiddenCommon kind="remove_role" evidenceHash={hash} />
            <h3 className="text-sm font-medium">
              Remove role from observations
            </h3>
            <DraftCheckboxes drafts={drafts} />
            <Input name="reason" placeholder="Reason" required />
            <Button type="submit" variant="outline">
              Remove role
            </Button>
          </Form>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Ledger</CardTitle>
          <CardDescription>
            Every correction ever recorded for this person, newest first. Undo
            appends a superseding row instead of deleting one.
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
              {correction.is_stale ? (
                <Badge variant="destructive">stale</Badge>
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
                    required
                    className="w-40"
                  />
                  <Button size="sm" variant="ghost" type="submit">
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
