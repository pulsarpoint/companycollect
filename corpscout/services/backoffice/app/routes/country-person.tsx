import { Link, redirect } from "react-router";
import { ArrowLeft, Mail, Users } from "lucide-react";
import type { Route } from "./+types/country-person";
import { getCountry } from "~/lib/countries";
import {
  applyCountryPersonCorrection,
  findPossibleCountryPersonMatches,
  getCountryPerson,
  PersonCorrectionValidationError,
  type CountryPersonContact,
  type CountryPersonObservation,
  type CountryPersonSuggestion,
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
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";

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

const MAX_SUGGESTIONS_SHOWN = 12;

const RESOLUTION_LABELS = {
  verified: "Verified identifier",
  reviewed: "Reviewed correction",
  provisional: "Provisional match",
  unresolved: "Unresolved singleton",
  merged: "Merged identity",
} as const;

interface CompanyRoleSummary {
  role_kind: string;
  role_original: string;
}

interface CompanySummary {
  company_id: string;
  company_name: string;
  roles: CompanyRoleSummary[];
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
  const possibleMatches = await findPossibleCountryPersonMatches(
    detail.person,
    detail.observations,
  );
  const search = new URL(request.url).searchParams;
  const correctionId = search.get("correction");
  return {
    country,
    detail,
    possibleMatches,
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
  const companies = new Map<
    string,
    Omit<CompanySummary, "roles"> & {
      rolesByKind: Map<string, CompanyRoleSummary>;
    }
  >();
  for (const observation of observations) {
    const existing = companies.get(observation.company_id);
    if (!existing) {
      companies.set(observation.company_id, {
        company_id: observation.company_id,
        company_name: observation.company_name,
        rolesByKind: new Map([
          [
            observation.role_kind,
            {
              role_kind: observation.role_kind,
              role_original: observation.role_original,
            },
          ],
        ]),
        first_year: observation.fiscal_year,
        last_year: observation.fiscal_year,
        observation_count: 1,
      });
      continue;
    }
    existing.observation_count += 1;
    const existingRole = existing.rolesByKind.get(observation.role_kind);
    if (!existingRole || existingRole.role_original === "") {
      existing.rolesByKind.set(observation.role_kind, {
        role_kind: observation.role_kind,
        role_original: observation.role_original,
      });
    }
    if (observation.fiscal_year > 0) {
      existing.first_year =
        existing.first_year > 0
          ? Math.min(existing.first_year, observation.fiscal_year)
          : observation.fiscal_year;
      if (observation.fiscal_year >= existing.last_year) {
        existing.last_year = observation.fiscal_year;
      }
    }
  }
  return Array.from(companies.values())
    .map(({ rolesByKind, ...company }) => {
      const roles = [...rolesByKind.values()];
      const substantiveRoles = roles.filter(
        (role) => role.role_kind !== "unknown",
      );
      return {
        ...company,
        roles: substantiveRoles.length > 0 ? substantiveRoles : roles,
      };
    })
    .sort(
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

const CONTACT_LABELS = {
  email: "Email",
  phone: "Phone",
  website: "Website",
  social: "Social profile",
} as const;

function contactHref(contact: CountryPersonContact): string | null {
  if (contact.contact_kind === "email") {
    return `mailto:${contact.contact_value}`;
  }
  if (contact.contact_kind === "phone") {
    return `tel:${contact.contact_value.replace(/[^+\d]/g, "")}`;
  }
  try {
    const url = new URL(contact.contact_value);
    return url.protocol === "http:" || url.protocol === "https:"
      ? url.toString()
      : null;
  } catch {
    return null;
  }
}

export function PersonContactCard({
  contacts,
}: {
  contacts: CountryPersonContact[];
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Contact information</CardTitle>
        <CardDescription>
          Only public contact details explicitly connected to this person by a
          source are shown here.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {contacts.length > 0 ? (
          <ul className="divide-y">
            {contacts.map((contact) => {
              const href = contactHref(contact);
              return (
                <li
                  key={`${contact.observation_id}-${contact.contact_kind}-${contact.contact_value}`}
                  className="flex flex-wrap items-baseline gap-3 py-2"
                >
                  <Badge variant="outline">
                    {CONTACT_LABELS[contact.contact_kind]}
                  </Badge>
                  {href ? (
                    <a
                      href={href}
                      className="font-medium hover:underline"
                      target={
                        contact.contact_kind === "email" ? undefined : "_blank"
                      }
                      rel={
                        contact.contact_kind === "email"
                          ? undefined
                          : "noreferrer"
                      }
                    >
                      {contact.contact_value}
                    </a>
                  ) : (
                    <span className="font-medium">{contact.contact_value}</span>
                  )}
                  <span className="text-muted-foreground text-xs">
                    {contact.source}
                  </span>
                </li>
              );
            })}
          </ul>
        ) : (
          <Empty className="border py-5">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <Mail />
              </EmptyMedia>
              <EmptyTitle>No person-specific contact information</EmptyTitle>
              <EmptyDescription>
                Company email addresses and phone numbers are not attributed to
                a person unless a source makes that connection explicit.
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        )}
      </CardContent>
    </Card>
  );
}

export function PossiblePersonMatchesCard({
  countryCode,
  suggestions,
}: {
  countryCode: string;
  suggestions: CountryPersonSuggestion[];
}) {
  if (suggestions.length === 0) return null;
  const shownSuggestions = suggestions.slice(0, MAX_SUGGESTIONS_SHOWN);
  const remainingSuggestions = suggestions.length - shownSuggestions.length;
  return (
    <Card>
      <CardHeader>
        <CardTitle>Possible same-person profiles</CardTitle>
        <CardDescription>
          These are separate country identities with the same normalized name
          and a compatible relationship type. They are suggestions for review,
          not confirmed connections.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ul className="divide-y">
          {shownSuggestions.map((suggestion) => (
            <li key={suggestion.person.person_id} className="grid gap-2 py-3">
              <div className="flex flex-wrap items-center gap-2">
                <Link
                  to={`/country/${countryCode}/person/${suggestion.person.person_id}`}
                  className="font-medium hover:underline"
                >
                  {suggestion.person.preferred_name}
                </Link>
                <Badge variant="outline">Possible match</Badge>
              </div>
              <ul className="text-muted-foreground grid gap-1 text-xs">
                {suggestion.connections.map((connection) => (
                  <li key={connection.company_id}>
                    <span className="text-foreground font-medium">
                      {ROLE_LABELS[connection.role_kind] ??
                        connection.role_kind}
                    </span>{" "}
                    at{" "}
                    <Link
                      to={`/company/${countryCode}/${connection.company_id}`}
                      className="text-foreground hover:underline"
                    >
                      {connection.company_name || connection.company_id}
                    </Link>
                    {connection.last_year > 0
                      ? ` · ${yearSpan(connection.first_year, connection.last_year)}`
                      : ""}
                    {` · ${connection.observation_count} source ${connection.observation_count === 1 ? "observation" : "observations"}`}
                  </li>
                ))}
              </ul>
            </li>
          ))}
          {remainingSuggestions > 0 ? (
            <li className="text-muted-foreground py-3 text-xs">
              …and {remainingSuggestions} more compatible profiles. Use the
              identity review search to inspect or merge a specific person.
            </li>
          ) : null}
        </ul>
      </CardContent>
    </Card>
  );
}

export default function CountryPersonPage({
  loaderData,
  actionData,
}: Route.ComponentProps) {
  const { country, detail } = loaderData;
  const { person, observations, identifiers, contacts } = detail;
  const companies = summarizeCompanies(observations);
  const allRoles = Array.from(
    new Set(observations.map((row) => row.role_kind)),
  );
  const substantiveRoles = allRoles.filter((role) => role !== "unknown");
  const roles = substantiveRoles.length > 0 ? substantiveRoles : allRoles;

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
          <CardTitle>Company relationships</CardTitle>
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
                {company.roles.map((role) => (
                  <Badge key={role.role_kind} variant="outline">
                    {ROLE_LABELS[role.role_kind] ?? role.role_kind}
                  </Badge>
                ))}
                <span className="text-muted-foreground text-xs">
                  {yearSpan(company.first_year, company.last_year)} ·{" "}
                  {company.observation_count}{" "}
                  {company.observation_count === 1
                    ? "observation"
                    : "observations"}
                </span>
                {company.roles.map((role) =>
                  role.role_original &&
                  role.role_original !== (ROLE_LABELS[role.role_kind] ?? "") ? (
                    <span
                      key={`${role.role_kind}:${role.role_original}`}
                      className="text-muted-foreground text-xs"
                    >
                      {role.role_original}
                    </span>
                  ) : null,
                )}
              </li>
            ))}
          </ul>
        </CardContent>
      </Card>

      <PersonContactCard contacts={contacts} />

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

      <PossiblePersonMatchesCard
        countryCode={country.code}
        suggestions={loaderData.possibleMatches}
      />

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
