import { ArrowLeftIcon, CheckCircle2Icon, TriangleAlertIcon, UserSearchIcon } from "lucide-react";
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
import type {
  SeCollisionCandidateGroup,
  SeCompanyPersonDetail,
} from "~/lib/se-company-person.server";

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
          <Checkbox name="draft_id" value={draft.draft_id} className="mt-0.5" />
          <span>
            {draft.source} · {draft.name} · {draft.role_original || "—"} ·{" "}
            {draft.fiscal_year ?? "undated"}
          </span>
        </label>
      ))}
    </div>
  );
}

/**
 * Collision candidates (Task 2's se_company_person_collision_candidate) and
 * any merge suggestion filed against each group (Task 4's merge asset),
 * company-wide rather than scoped to this one person -- a group's other
 * members would otherwise never see it on their own page.
 */
function CollisionReviewCard({
  companyId,
  groups,
  busy,
}: {
  companyId: string;
  groups: SeCollisionCandidateGroup[];
  busy: boolean;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Collision candidates &amp; merge review</CardTitle>
        <CardDescription>
          Groups the deterministic K3 identity rule kept apart but a looser
          name match (K1) would have merged. A merge suggestion appears once
          se_company_person_merge_job has run for this company; approving one
          re-checks it against live state first and refuses if it has gone
          stale.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        {groups.length === 0 ? (
          <span className="text-sm text-muted-foreground">
            No open collision candidates for this company.
          </span>
        ) : null}
        {groups.map((group) => (
          <div key={group.candidate_group_id} className="rounded-lg border p-3">
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="outline">{group.candidate_group_id}</Badge>
              {group.is_decided ? <Badge>decided</Badge> : null}
              {group.suggestion ? (
                <Badge
                  variant={group.suggestion.decision === "merge" ? "default" : "outline"}
                >
                  model: {group.suggestion.decision.replace("_", " ")} ·{" "}
                  {Math.round(group.suggestion.confidence * 100)}%
                </Badge>
              ) : (
                <span className="text-xs text-muted-foreground">
                  No merge suggestion yet. Launch a scoped{" "}
                  <Link
                    className="underline"
                    to={`/admin/se/people/pipeline?company_id=${encodeURIComponent(companyId)}`}
                  >
                    merge run
                  </Link>
                  .
                </span>
              )}
            </div>
            <ul className="mt-2 flex flex-col gap-1 text-sm">
              {group.members.map((member) => (
                <li
                  key={`${member.source}:${member.source_record_uid}:${member.full_name}`}
                >
                  {member.full_name} · {member.source}
                </li>
              ))}
            </ul>
            {group.suggestion ? (
              <>
                {group.suggestion.rationale ? (
                  <p className="mt-2 text-sm text-muted-foreground">
                    {group.suggestion.rationale}
                  </p>
                ) : null}
                <p className="mt-1 text-xs text-muted-foreground">
                  Into{" "}
                  <code className="font-mono">{group.suggestion.into_person_id}</code>{" "}
                  from{" "}
                  {group.suggestion.from_person_ids.map((id) => (
                    <code key={id} className="mr-1 font-mono">
                      {id}
                    </code>
                  ))}
                </p>
                {group.is_decided ? null : (
                  <div className="mt-2 flex flex-wrap gap-2">
                    <Form method="post" className="flex items-center gap-2">
                      <input
                        type="hidden"
                        name="correction_kind"
                        value="approve_merge_suggestion"
                      />
                      <input
                        type="hidden"
                        name="suggestion_id"
                        value={group.suggestion.suggestion_id}
                      />
                      <Input name="reason" placeholder="Reason" required className="w-56" />
                      <Button size="sm" type="submit" disabled={busy} aria-busy={busy}>
                        Approve merge
                      </Button>
                    </Form>
                    <Form method="post" className="flex items-center gap-2">
                      <input
                        type="hidden"
                        name="correction_kind"
                        value="keep_separate_suggestion"
                      />
                      <input
                        type="hidden"
                        name="suggestion_id"
                        value={group.suggestion.suggestion_id}
                      />
                      <Input name="reason" placeholder="Reason" required className="w-56" />
                      <Button
                        size="sm"
                        variant="outline"
                        type="submit"
                        disabled={busy}
                        aria-busy={busy}
                      >
                        Keep separate
                      </Button>
                    </Form>
                  </div>
                )}
              </>
            ) : null}
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

/** Shown when Dagster has not published this person into `se_company_person`. */
export function SePersonNotPublished({
  companyId,
  personId,
}: {
  companyId: string;
  personId: string;
}) {
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <UserSearchIcon />
          </EmptyMedia>
          <EmptyTitle>Not published yet</EmptyTitle>
          <EmptyDescription>
            Company {companyId} has no person{" "}
            <code className="font-mono text-xs">{personId}</code> in
            se_company_person. Dagster publishes a person once its drafts are
            built, so this page fills in after the next run.
          </EmptyDescription>
        </EmptyHeader>
        <EmptyContent>
          <Link
            className={buttonVariants({ variant: "outline" })}
            to="/admin/se/people?step=draft-2"
          >
            <ArrowLeftIcon data-icon="inline-start" />
            Back to people
          </Link>
        </EmptyContent>
      </Empty>
    </div>
  );
}

export function SePersonReviewWorkspace({
  detail,
  activeRoleCodes,
  collisionGroups,
  result,
}: {
  detail: SeCompanyPersonDetail;
  activeRoleCodes: string[];
  collisionGroups: SeCollisionCandidateGroup[];
  result: SePersonReviewResult;
}) {
  const { person, drafts, roles, suggestions, corrections } = detail;
  const hash = person.draft_set_hash;
  // One click is one ledger row: block every submit while one is in flight.
  const busy = useNavigation().state !== "idle";
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
        <p className="text-sm">
          <Link className="underline" to="/admin/se/people/stale-corrections">
            Corrections the pipeline is skipping
          </Link>
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
            The draft rows this published person was built from. Their set is
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
                <TableHead>Payload</TableHead>
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
                  <TableCell>
                    <details>
                      <summary className="cursor-pointer text-xs text-muted-foreground">
                        payload
                      </summary>
                      <pre className="mt-1 max-w-lg overflow-x-auto whitespace-pre-wrap break-words rounded-lg border bg-muted p-2 font-mono text-xs">
                        {draft.source_value_json}
                      </pre>
                    </details>
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
            <span className="text-sm text-muted-foreground">No role rows.</span>
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
                {suggestion.is_current ? null : (
                  <Badge variant="outline">superseded evidence</Badge>
                )}
                <span className="text-xs text-muted-foreground">
                  {suggestion.created_at}
                </span>
              </div>
              <pre className="mt-2 overflow-x-auto whitespace-pre-wrap break-words font-mono text-xs">
                {suggestion.suggestion}
              </pre>
              {/* Approving a suggestion the pipeline no longer asks for is
                  stale on arrival (spec §4.3), so those rows stay read-only. */}
              {suggestion.is_current ? null : (
                <p className="mt-2 text-xs text-muted-foreground">
                  Answered evidence this person no longer has. Nothing to decide
                  until the model is asked again.
                </p>
              )}
              <div className="mt-2 flex flex-wrap gap-2">
                {(suggestion.is_current
                  ? (["approve_suggestion", "reject_suggestion"] as const)
                  : ([] as const)
                ).map(
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
                        disabled={busy}
                        aria-busy={busy}
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

      <CollisionReviewCard
        companyId={person.company_id}
        groups={collisionGroups}
        busy={busy}
      />

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
            {/* Diffed server-side: an untouched field must not enter the
                payload, because Dagster replays the correction every run. */}
            <input type="hidden" name="original_name" value={person.name} />
            <input
              type="hidden"
              name="original_description"
              value={person.description ?? ""}
            />
            <h3 className="text-sm font-medium">Override name / description</h3>
            <Input name="name" defaultValue={person.name} aria-label="Name" />
            <Textarea
              name="description"
              defaultValue={person.description ?? ""}
              aria-label="Description"
              placeholder="Description"
            />
            <label className="flex items-center gap-2 text-sm">
              <Checkbox name="clear_description" value="yes" />
              <span>Clear the description</span>
            </label>
            <Input name="reason" placeholder="Reason" required />
            <Button type="submit" disabled={busy} aria-busy={busy}>
              Save override
            </Button>
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
            <Button
              type="submit"
              variant="outline"
              disabled={busy}
              aria-busy={busy}
            >
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
            <Button
              type="submit"
              variant="outline"
              disabled={busy}
              aria-busy={busy}
            >
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
            <Button
              type="submit"
              variant="outline"
              disabled={busy}
              aria-busy={busy}
            >
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
              <option value="">Select a role…</option>
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
            <Button
              type="submit"
              variant="outline"
              disabled={busy}
              aria-busy={busy}
            >
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
            <Button
              type="submit"
              variant="outline"
              disabled={busy}
              aria-busy={busy}
            >
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
