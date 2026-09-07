import { useState } from "react";
import { data, Form, Link, useNavigation } from "react-router";
import type { Route } from "./+types/admin-technology-proposal";
import { Alert, AlertTitle, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { Textarea } from "~/components/ui/textarea";
import {
  NativeSelect,
  NativeSelectOption,
} from "~/components/ui/native-select";
import {
  loadTechnologyProposal,
  reviewTechnologyProposal,
} from "~/lib/technology-proposals.server";
import {
  normalizeTechnologyName,
  TechnologyValidationError,
  TECHNOLOGY_REVIEW_LABELS,
} from "~/lib/technology-proposals";

export async function loader({ params }: Route.LoaderArgs) {
  const detail = await loadTechnologyProposal(params.proposalId);
  if (!detail.observations.length)
    throw new Response("Proposal not found", { status: 404 });
  return detail;
}

export async function action({ request, params }: Route.ActionArgs) {
  // The backoffice is an internal administrative surface. Reject cross-site
  // form posts; reviewer identity follows its existing explicit-actor convention.
  if (request.headers.get("origin") !== new URL(request.url).origin) {
    return data(
      { ok: false, message: "Review must be submitted from this backoffice." },
      { status: 403 },
    );
  }
  try {
    await reviewTechnologyProposal(params.proposalId, await request.formData());
    return {
      ok: true,
      message:
        "Review saved. Approved entries and aliases will be included in the next catalog publication.",
    };
  } catch (error) {
    if (error instanceof TechnologyValidationError)
      return data({ ok: false, message: error.message }, { status: 400 });
    console.error("Technology review failed", {
      errorType: error instanceof Error ? error.name : "unknown",
    });
    return data(
      { ok: false, message: "Could not save the review." },
      { status: 500 },
    );
  }
}

export function meta() {
  return [{ title: "Review technology | CompanyCollect" }];
}

export default function TechnologyProposal({
  loaderData,
  actionData,
}: Route.ComponentProps) {
  const { observations, reviews, catalog } = loaderData;
  const proposal = observations[0],
    latest = reviews[0];
  const [name, setName] = useState(
    latest?.technology ||
      proposal.existing_technology ||
      proposal.proposed_name,
  );
  const [selectedCategories, setSelectedCategories] = useState<number[]>(
    latest?.category_ids.length ? latest.category_ids : proposal.category_ids,
  );
  const pending = useNavigation().state !== "idle";
  const search = normalizeTechnologyName(name);
  const candidates = catalog.entries
    .filter((entry) =>
      normalizeTechnologyName(
        `${entry.technology} ${entry.description}`,
      ).includes(search),
    )
    .slice(0, 15);
  const categories = Array.from(
    new Map(
      catalog.entries.flatMap((entry) =>
        entry.category_ids.map((id, i) => [id, entry.categories[i]] as const),
      ),
    ).entries(),
  )
    .filter(([, label]) => label)
    .sort((a, b) => a[1].localeCompare(b[1]));
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <Link
          className="text-sm text-muted-foreground"
          to="/admin/technology-proposals"
        >
          ← Technology proposals
        </Link>
        <h1 className="text-2xl font-semibold tracking-tight">
          {proposal.proposed_name}
        </h1>
        <div>
          <Badge variant="secondary">
            {latest
              ? TECHNOLOGY_REVIEW_LABELS[latest.decision]
              : "Pending review"}
          </Badge>
        </div>
        <p className="text-muted-foreground">
          Preserve the source meaning when choosing a canonical identity.
          Approval describes the technology; it does not confirm company-wide
          usage.
        </p>
      </header>
      {actionData && (
        <Alert variant={actionData.ok ? "default" : "destructive"}>
          <AlertTitle>
            {actionData.ok ? "Review saved" : "Review needs attention"}
          </AlertTitle>
          <AlertDescription>{actionData.message}</AlertDescription>
        </Alert>
      )}
      <div className="grid gap-8 lg:grid-cols-2">
        <section
          className="flex flex-col gap-5"
          aria-label="Submitted evidence"
        >
          <h2 className="text-lg font-semibold">Evidence and observations</h2>
          <p>{proposal.proposal_reason}</p>
          {observations.map((row) => (
            <article
              key={`${row.run_id}:${row.record_id}`}
              className="flex flex-col gap-3 border-b pb-5"
            >
              <h3 className="font-medium">
                {row.company || "Company not established"} · {row.observed_name}
              </h3>
              <p>{row.context}</p>
              <p className="text-sm text-muted-foreground">
                {row.signal} · {row.scope}
                {row.job_title ? ` · ${row.job_title}` : ""}
              </p>
              {row.job_employer && (
                <p className="text-sm">Job employer: {row.job_employer}</p>
              )}
              {row.sources.map((source, i) => (
                <div key={i} className="flex flex-col gap-2">
                  <Link
                    to={source.url}
                    target="_blank"
                    rel="noreferrer"
                    className="break-all text-sm underline"
                  >
                    {source.url}
                  </Link>
                  <Badge variant="outline">{source.evidence_status}</Badge>
                  {source.evidence.map((fragment, index) => (
                    <blockquote key={index} className="border-l-2 pl-3 text-sm">
                      {fragment}
                    </blockquote>
                  ))}
                </div>
              ))}
              <details>
                <summary className="cursor-pointer text-sm">
                  Catalog search and provenance
                </summary>
                <div className="flex flex-col gap-2 pt-2 text-sm text-muted-foreground">
                  <p>Queries: {row.search_queries.join(", ")}</p>
                  <p>
                    Candidates: {row.search_candidates.join(", ") || "None"}
                  </p>
                  <p className="break-all">
                    Catalog version: {row.catalog_version}
                  </p>
                  <p>Model: {row.model}</p>
                </div>
              </details>
            </article>
          ))}
          {observations.length === 100 && (
            <p className="text-sm text-muted-foreground">
              Showing the latest 100 observations.
            </p>
          )}
        </section>
        <section
          className="flex flex-col gap-5"
          aria-label="Administrator decision"
        >
          <h2 className="text-lg font-semibold">Review decision</h2>
          <Form method="post" className="flex flex-col gap-5">
            <input
              type="hidden"
              name="expected_review_id"
              value={latest?.review_id || ""}
            />
            <input
              type="hidden"
              name="category_ids"
              value={selectedCategories.join(",")}
            />
            <FieldGroup>
              <Field>
                <FieldLabel htmlFor="technology">
                  Canonical technology
                </FieldLabel>
                <Input
                  id="technology"
                  name="technology"
                  list="technology-candidates"
                  value={name}
                  onChange={(event) => setName(event.target.value)}
                  maxLength={200}
                />
                <datalist id="technology-candidates">
                  {candidates.map((entry) => (
                    <option key={entry.technology} value={entry.technology}>
                      {entry.description}
                    </option>
                  ))}
                </datalist>
                <FieldDescription>
                  Search by name, or enter the name of a new technology.
                </FieldDescription>
              </Field>
              {candidates.length > 0 && (
                <details>
                  <summary className="cursor-pointer text-sm">
                    Compare existing candidates
                  </summary>
                  <ul className="flex flex-col gap-2 pt-2">
                    {candidates.map((entry) => (
                      <li key={entry.technology} className="text-sm">
                        <strong>{entry.technology}</strong>: {entry.description}
                      </li>
                    ))}
                  </ul>
                </details>
              )}
              <Field>
                <FieldLabel htmlFor="description">
                  Description for a new entry
                </FieldLabel>
                <Textarea
                  id="description"
                  name="description"
                  defaultValue={latest?.description || proposal.description}
                  maxLength={2000}
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="website">Official website</FieldLabel>
                <Input
                  id="website"
                  name="website"
                  type="url"
                  defaultValue={latest?.website || proposal.website}
                />
                <FieldDescription>
                  Verify the official product website before approving a new
                  entry.
                </FieldDescription>
              </Field>
              <Field>
                <FieldLabel htmlFor="categories">Categories</FieldLabel>
                <select
                  id="categories"
                  multiple
                  className="min-h-32 rounded-md border bg-background p-2 text-sm"
                  value={selectedCategories.map(String)}
                  onChange={(event) =>
                    setSelectedCategories(
                      Array.from(event.target.selectedOptions, (option) =>
                        Number(option.value),
                      ),
                    )
                  }
                >
                  {categories.map(([id, label]) => (
                    <option key={id} value={id}>
                      {label}
                    </option>
                  ))}
                </select>
                <FieldDescription>
                  {proposal.category_suggestion
                    ? `Crawler suggestion: ${proposal.category_suggestion}. `
                    : ""}
                  Select at least one category for a new entry.
                </FieldDescription>
              </Field>
              {(["saas", "oss"] as const).map((flag) => (
                <Field key={flag}>
                  <FieldLabel htmlFor={flag}>
                    {flag === "saas" ? "Software as a service" : "Open source"}
                  </FieldLabel>
                  <NativeSelect
                    id={flag}
                    name={flag}
                    defaultValue={
                      (latest?.[flag] ?? proposal[flag]) === null
                        ? ""
                        : (latest?.[flag] ?? proposal[flag])
                          ? "true"
                          : "false"
                    }
                  >
                    <NativeSelectOption value="">
                      Needs verification
                    </NativeSelectOption>
                    <NativeSelectOption value="true">Yes</NativeSelectOption>
                    <NativeSelectOption value="false">No</NativeSelectOption>
                  </NativeSelect>
                </Field>
              ))}
              <Field>
                <FieldLabel htmlFor="pricing">Pricing labels</FieldLabel>
                <Input
                  id="pricing"
                  name="pricing"
                  defaultValue={(latest?.pricing || proposal.pricing).join(
                    ", ",
                  )}
                />
                <FieldDescription>
                  Comma-separated catalog labels; leave empty if not
                  established.
                </FieldDescription>
              </Field>
              <Field>
                <FieldLabel htmlFor="alias">
                  Optional accepted synonym
                </FieldLabel>
                <Input
                  id="alias"
                  name="alias"
                  defaultValue={latest?.alias || ""}
                  maxLength={200}
                />
                <FieldDescription>
                  Add only a reviewed synonym for the same technology. Case
                  variations need no alias.
                </FieldDescription>
              </Field>
              <Field>
                <FieldLabel htmlFor="reviewed_by">Reviewer</FieldLabel>
                <Input
                  id="reviewed_by"
                  name="reviewed_by"
                  required
                  maxLength={200}
                  autoComplete="name"
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="review_note">
                  Decision and evidence notes
                </FieldLabel>
                <Textarea
                  id="review_note"
                  name="review_note"
                  required
                  maxLength={4000}
                />
              </Field>
            </FieldGroup>
            <div className="flex flex-wrap gap-2">
              <Button
                type="submit"
                name="decision"
                value="approve_new"
                disabled={pending}
              >
                Approve new technology
              </Button>
              <Button
                type="submit"
                name="decision"
                value="map_existing"
                variant="outline"
                disabled={pending}
              >
                Map to existing
              </Button>
              <Button
                type="submit"
                name="decision"
                value="reject"
                variant="destructive"
                disabled={pending}
              >
                Reject proposal
              </Button>
            </div>
          </Form>
          <h2 className="text-lg font-semibold">Review history</h2>
          {reviews.length ? (
            reviews.map((review) => (
              <article
                key={review.review_id}
                className="flex flex-col gap-1 border-b pb-3 text-sm"
              >
                <p>
                  <strong>{TECHNOLOGY_REVIEW_LABELS[review.decision]}</strong>
                  {review.technology ? ` → ${review.technology}` : ""}
                </p>
                <p>{review.review_note}</p>
                <p className="text-muted-foreground">
                  {review.reviewed_by} · {review.reviewed_at}
                </p>
              </article>
            ))
          ) : (
            <p className="text-sm text-muted-foreground">
              No administrator decision yet.
            </p>
          )}
        </section>
      </div>
    </div>
  );
}
