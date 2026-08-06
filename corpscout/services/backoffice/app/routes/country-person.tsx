import { Link, redirect } from "react-router";
import { ArrowLeft, Users } from "lucide-react";
import type { Route } from "./+types/country-person";
import { getCountry } from "~/lib/countries";
import {
  applyCountryPersonCorrection,
  getCountryPerson,
  PersonCorrectionValidationError,
  type CountryPersonObservation,
} from "~/lib/people.server";
import { PersonCorrectionWorkspace } from "~/components/person-correction-workspace";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";

const ROLE_LABELS: Record<string, string> = {
  chairman: "Chairman",
  ceo: "CEO",
  board_member: "Board member",
  deputy_board_member: "Deputy board member",
  liquidator: "Liquidator",
  auditor: "Auditor",
  other: "Other",
  unknown: "Signatory",
};

const RESOLUTION_LABELS = {
  verified: "Verified identifier",
  reviewed: "Reviewed correction",
  provisional: "Provisional match",
  unresolved: "Unresolved singleton",
  merged: "Merged identity",
} as const;

interface CompanySummary {
  company_id: string;
  company_name: string;
  role_kind: string;
  role_original: string;
  first_year: number;
  last_year: number;
  observation_count: number;
}

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });
  const detail = await getCountryPerson(country.code, params.id);
  if (!detail) throw new Response("Person not found", { status: 404 });
  if (
    detail.person.resolution_status === "merged" &&
    detail.person.merged_into_person_id
  ) {
    throw redirect(
      `/country/${country.code}/person/${detail.person.merged_into_person_id}`,
    );
  }
  const search = new URL(request.url).searchParams;
  const correctionId = search.get("correction");
  return {
    country,
    detail,
    correctionSubmitted: correctionId
      ? {
          correctionId,
          targetPersonId: search.get("target") ?? "",
          correctionCount: Number(search.get("count") ?? 0),
        }
      : null,
  };
}

export async function action({ request, params }: Route.ActionArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });
  const form = await request.formData();
  const kind = String(form.get("correction_kind") ?? "");
  const common = {
    countryIso2: country.code,
    sourcePersonId: params.id,
    reason: String(form.get("reason") ?? ""),
  };

  try {
    let result;
    if (kind === "merge") {
      result = await applyCountryPersonCorrection({
        ...common,
        kind,
        targetPersonId: String(form.get("target_person_id") ?? ""),
      });
    } else if (kind === "reassign") {
      result = await applyCountryPersonCorrection({
        ...common,
        kind,
        targetPersonId: String(form.get("target_person_id") ?? ""),
        observationIds: form
          .getAll("observation_id")
          .map((value) => String(value)),
      });
    } else if (kind === "split") {
      result = await applyCountryPersonCorrection({
        ...common,
        kind,
        observationIds: form
          .getAll("observation_id")
          .map((value) => String(value)),
      });
    } else if (kind === "undo") {
      result = await applyCountryPersonCorrection({
        ...common,
        kind,
        reviewId: String(form.get("review_id") ?? ""),
      });
    } else {
      throw new PersonCorrectionValidationError("Unknown correction action.");
    }
    const query = new URLSearchParams({
      correction: result.reviewId,
      target: result.targetPersonId,
      count: String(result.correctionCount),
    });
    return redirect(
      `/country/${country.code}/person/${params.id}?${query.toString()}`,
    );
  } catch (error) {
    if (error instanceof PersonCorrectionValidationError) {
      return { error: error.message };
    }
    throw error;
  }
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  const name = loaderData?.detail.person.preferred_name ?? params.id;
  return [{ title: `${name} – CompanyCollect Backoffice` }];
}

function yearSpan(first: number, last: number): string {
  if (last <= 0) return "";
  if (first <= 0 || first === last) return String(last);
  return `${first}–${last}`;
}

function summarizeCompanies(
  observations: CountryPersonObservation[],
): CompanySummary[] {
  const companies = new Map<string, CompanySummary>();
  for (const observation of observations) {
    const existing = companies.get(observation.company_id);
    if (!existing) {
      companies.set(observation.company_id, {
        company_id: observation.company_id,
        company_name: observation.company_name,
        role_kind: observation.role_kind,
        role_original: observation.role_original,
        first_year: observation.fiscal_year,
        last_year: observation.fiscal_year,
        observation_count: 1,
      });
      continue;
    }
    existing.observation_count += 1;
    if (observation.fiscal_year > 0) {
      existing.first_year =
        existing.first_year > 0
          ? Math.min(existing.first_year, observation.fiscal_year)
          : observation.fiscal_year;
      if (observation.fiscal_year >= existing.last_year) {
        existing.last_year = observation.fiscal_year;
        existing.role_kind = observation.role_kind;
        existing.role_original = observation.role_original;
      }
    }
  }
  return Array.from(companies.values()).sort(
    (left, right) =>
      right.last_year - left.last_year ||
      left.company_name.localeCompare(right.company_name),
  );
}

function resolutionExplanation(status: keyof typeof RESOLUTION_LABELS): string {
  if (status === "verified") {
    return "The source published a stable person identifier used by this profile.";
  }
  if (status === "provisional") {
    return "These observations share a name and company within one country. That is useful evidence, but it is not proof that they describe the same physical person.";
  }
  if (status === "reviewed") {
    return "A backoffice correction changed how one or more source observations are assigned. The audit history below preserves that decision.";
  }
  if (status === "merged") {
    return "This identity was merged into another country-scoped person ID.";
  }
  return "This observation was kept separate because the available evidence could not safely combine it with another record.";
}

export default function CountryPersonPage({
  loaderData,
  actionData,
}: Route.ComponentProps) {
  const { country, detail } = loaderData;
  const { person, observations, identifiers } = detail;
  const companies = summarizeCompanies(observations);
  const roles = Array.from(new Set(observations.map((row) => row.role_kind)));

  return (
    <div className="mx-auto flex w-full max-w-6xl flex-col gap-4">
      <div>
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2"
          nativeButton={false}
          render={
            <Link
              to={`/people?q=${encodeURIComponent(person.preferred_name)}`}
            />
          }
        >
          <ArrowLeft data-icon="inline-start" />
          People
        </Button>
      </div>

      <header className="flex flex-wrap items-center gap-3">
        <Users className="text-muted-foreground size-6" />
        <h1 className="text-2xl font-semibold">{person.preferred_name}</h1>
        <span title={country.name}>{country.flag}</span>
        <Badge
          variant={
            person.resolution_status === "verified" ? "default" : "outline"
          }
        >
          {RESOLUTION_LABELS[person.resolution_status]}
        </Badge>
      </header>
      <p className="text-muted-foreground -mt-2 font-mono text-xs">
        {person.country_iso2}:{person.person_id}
      </p>

      <Card>
        <CardHeader>
          <CardTitle>Combined profile</CardTitle>
          <CardDescription>
            {resolutionExplanation(person.resolution_status)}
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <div className="flex flex-wrap items-center gap-2">
            {roles.map((role) => (
              <Badge
                key={role}
                variant={
                  role === "ceo" || role === "chairman" ? "default" : "outline"
                }
              >
                {ROLE_LABELS[role] ?? role}
              </Badge>
            ))}
            <span className="text-muted-foreground text-xs">
              {person.company_count}{" "}
              {person.company_count === 1 ? "company" : "companies"} ·{" "}
              {person.observation_count}{" "}
              {person.observation_count === 1 ? "observation" : "observations"}
              {person.last_observed_year > 0
                ? ` · ${yearSpan(person.first_observed_year, person.last_observed_year)}`
                : ""}
            </span>
          </div>
          <ul className="divide-y">
            {companies.map((company) => (
              <li
                key={company.company_id}
                className="flex flex-wrap items-baseline gap-x-3 gap-y-1 py-2"
              >
                <Link
                  to={`/company/${country.code}/${company.company_id}`}
                  className="font-medium hover:underline"
                >
                  {company.company_name || company.company_id}
                </Link>
                <Badge variant="outline">
                  {ROLE_LABELS[company.role_kind] ?? company.role_kind}
                </Badge>
                <span className="text-muted-foreground text-xs">
                  {yearSpan(company.first_year, company.last_year)} ·{" "}
                  {company.observation_count}{" "}
                  {company.observation_count === 1
                    ? "observation"
                    : "observations"}
                </span>
                {company.role_original &&
                company.role_original !==
                  (ROLE_LABELS[company.role_kind] ?? "") ? (
                  <span className="text-muted-foreground text-xs">
                    {company.role_original}
                  </span>
                ) : null}
              </li>
            ))}
          </ul>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Identifiers</CardTitle>
          <CardDescription>
            Public identifiers remain scoped to their country and source.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {identifiers.length > 0 ? (
            <ul className="divide-y">
              {identifiers.map((identifier) => (
                <li
                  key={identifier.identifier_id}
                  className="flex flex-wrap items-baseline gap-3 py-2"
                >
                  <Badge variant="outline">{identifier.identifier_kind}</Badge>
                  <span className="font-mono text-xs">
                    {identifier.identifier_value}
                  </span>
                  <span className="text-muted-foreground text-xs">
                    {identifier.source}
                  </span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-muted-foreground text-sm">
              This source did not publish a public person identifier.
            </p>
          )}
        </CardContent>
      </Card>

      <PersonCorrectionWorkspace
        countryCode={country.code}
        sourcePersonId={person.person_id}
        observationCount={person.observation_count}
        observations={observations}
        correctionReviews={detail.correction_reviews}
        actionError={actionData?.error ?? null}
        correctionSubmitted={loaderData.correctionSubmitted}
      />
    </div>
  );
}
