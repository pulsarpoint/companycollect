import { Link } from "react-router";
import { getCountry } from "~/lib/countries";
import type {
  AuditRow,
  EsefPersonObservation,
  EvidenceRef,
  OfficerRow,
  PeopleMatchRow,
  WikidataPersonRow,
} from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { Separator } from "~/components/ui/separator";
import { EvidencePanel } from "~/components/detail/evidence";

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

// Chairman and CEO get the emphasized (solid) badge; everyone else is outline.
const EMPHASIZED_ROLES = new Set(["chairman", "ceo"]);

const MAX_MATCHES_SHOWN = 10;

interface PersonRole {
  key: string;
  kind: string;
  label: string;
  originalLabel: string;
}

interface DistinctCompanyPerson {
  key: string;
  normalizedName: string;
  name: string;
  profileUrl: string;
  externalUrl: string;
  roles: PersonRole[];
  evidence: EvidenceRef[];
}

interface MutableCompanyPerson extends Omit<
  DistinctCompanyPerson,
  "roles" | "evidence"
> {
  rolesByKey: Map<string, PersonRole>;
  evidenceByUid: Map<string, EvidenceRef>;
}

const ROLE_ORDER: Record<string, number> = {
  chairman: 0,
  ceo: 1,
  board_member: 2,
  deputy_board_member: 3,
  liquidator: 4,
  auditor: 5,
  other: 6,
  unknown: 7,
};

function relationshipKindForRole(roleKind: string): string {
  if (roleKind === "auditor") return "external_audit";
  if (roleKind === "ceo") return "leadership";
  if (
    ["chairman", "board_member", "deputy_board_member", "liquidator"].includes(
      roleKind,
    )
  ) {
    return "governance";
  }
  if (roleKind === "unknown") return "report_signature";
  return "other";
}

function normalizePersonName(name: string): string {
  return name.normalize("NFKC").trim().replace(/\s+/g, " ").toLocaleLowerCase();
}

function evidenceWithConnection(
  evidence: EvidenceRef,
  connectionKind: string,
): EvidenceRef {
  return evidence.connectionKind &&
    evidence.connectionKind !== "management_evidence"
    ? evidence
    : { ...evidence, connectionKind };
}

function addEvidence(
  person: MutableCompanyPerson,
  evidence: EvidenceRef[],
  connectionKind: string,
): void {
  for (const reference of evidence) {
    const connected = evidenceWithConnection(reference, connectionKind);
    const existing = person.evidenceByUid.get(connected.sourceRecordUid);
    if (!existing) {
      person.evidenceByUid.set(connected.sourceRecordUid, connected);
      continue;
    }
    const origins = [...existing.origins];
    for (const origin of connected.origins) {
      if (
        !origins.some(
          (candidate) =>
            candidate.sourceSlug === origin.sourceSlug &&
            candidate.sourceRecordKey === origin.sourceRecordKey &&
            candidate.sourceObjectKey === origin.sourceObjectKey,
        )
      ) {
        origins.push(origin);
      }
    }
    person.evidenceByUid.set(connected.sourceRecordUid, {
      ...existing,
      origins,
    });
  }
}

function addRole(
  person: MutableCompanyPerson,
  kind: string,
  label: string,
  originalLabel: string,
): void {
  const key = `${kind}:${label}`;
  const existing = person.rolesByKey.get(key);
  if (!existing || (existing.originalLabel === "" && originalLabel !== "")) {
    person.rolesByKey.set(key, { key, kind, label, originalLabel });
  }
}

function buildDistinctCompanyPeople(
  officers: OfficerRow[],
  wikidataPeople: WikidataPersonRow[],
  esefPeople: EsefPersonObservation[],
): DistinctCompanyPerson[] {
  const people = new Map<string, MutableCompanyPerson>();
  const keysByName = new Map<string, Set<string>>();
  const officerKeysByName = new Map<string, Set<string>>();

  const getOrCreate = (
    key: string,
    name: string,
    profileUrl = "",
    externalUrl = "",
  ): MutableCompanyPerson => {
    const existing = people.get(key);
    if (existing) {
      if (!existing.profileUrl) existing.profileUrl = profileUrl;
      if (!existing.externalUrl) existing.externalUrl = externalUrl;
      return existing;
    }
    const normalizedName = normalizePersonName(name);
    const person: MutableCompanyPerson = {
      key,
      normalizedName,
      name,
      profileUrl,
      externalUrl,
      rolesByKey: new Map(),
      evidenceByUid: new Map(),
    };
    people.set(key, person);
    const nameKeys = keysByName.get(normalizedName) ?? new Set();
    nameKeys.add(key);
    keysByName.set(normalizedName, nameKeys);
    return person;
  };

  const uniqueKeyForName = (name: string): string | undefined => {
    const keys = keysByName.get(normalizePersonName(name));
    return keys?.size === 1 ? [...keys][0] : undefined;
  };

  const uniqueOfficerKeyForName = (name: string): string | undefined => {
    const keys = officerKeysByName.get(normalizePersonName(name));
    return keys?.size === 1 ? [...keys][0] : undefined;
  };

  for (const officer of officers) {
    const name = `${officer.first_name} ${officer.last_name}`.trim();
    const key = `country:${officer.country_iso2}:${officer.person_id}`;
    const person = getOrCreate(
      key,
      name,
      officer.person_profile_available === false
        ? ""
        : `/country/${officer.country_iso2.toLowerCase()}/person/${officer.person_id}`,
    );
    const officerKeys =
      officerKeysByName.get(person.normalizedName) ?? new Set();
    officerKeys.add(key);
    officerKeysByName.set(person.normalizedName, officerKeys);
    const label = ROLE_LABELS[officer.role_kind] ?? officer.role_kind;
    addRole(person, officer.role_kind, label, officer.role_original);
    addEvidence(person, officer.evidence ?? [], "annual_report_signature");
  }

  const wikidataKeys = new Map<string, string>();
  for (const sourcePerson of wikidataPeople) {
    const key =
      uniqueOfficerKeyForName(sourcePerson.name) ??
      wikidataKeys.get(sourcePerson.person_wikidata_id) ??
      `wikidata:${sourcePerson.person_wikidata_id}`;
    wikidataKeys.set(sourcePerson.person_wikidata_id, key);
    const person = getOrCreate(
      key,
      sourcePerson.name,
      "",
      sourcePerson.wikidata_url,
    );
    addRole(
      person,
      sourcePerson.role_label.toLocaleLowerCase().replaceAll(" ", "_"),
      sourcePerson.role_label,
      "",
    );
    addEvidence(
      person,
      sourcePerson.evidence ?? [],
      "public_knowledge_graph_company_role",
    );
  }

  for (const sourcePerson of esefPeople) {
    const key =
      uniqueKeyForName(sourcePerson.name) ??
      `annual-report-name:${normalizePersonName(sourcePerson.name)}`;
    const person = getOrCreate(key, sourcePerson.name);
    addRole(person, sourcePerson.roleCategory, sourcePerson.role, "");
    addEvidence(person, sourcePerson.evidence, "annual_report_narrative_role");
  }

  return [...people.values()]
    .map((person) => {
      const roles = [...person.rolesByKey.values()];
      const substantiveRoles = roles.filter((role) => role.kind !== "unknown");
      return {
        key: person.key,
        normalizedName: person.normalizedName,
        name: person.name,
        profileUrl: person.profileUrl,
        externalUrl: person.externalUrl,
        roles: (substantiveRoles.length > 0 ? substantiveRoles : roles).sort(
          (left, right) =>
            (ROLE_ORDER[left.kind] ?? 99) - (ROLE_ORDER[right.kind] ?? 99) ||
            left.label.localeCompare(right.label),
        ),
        evidence: [...person.evidenceByUid.values()].sort((left, right) =>
          right.lastSeenAt.localeCompare(left.lastSeenAt),
        ),
      };
    })
    .sort(
      (left, right) =>
        (ROLE_ORDER[left.roles[0]?.kind] ?? 99) -
          (ROLE_ORDER[right.roles[0]?.kind] ?? 99) ||
        left.name.localeCompare(right.name),
    );
}

function SameNameMatches({ matches }: { matches: PeopleMatchRow[] }) {
  if (matches.length === 0) return null;
  const shown = matches.slice(0, MAX_MATCHES_SHOWN);
  const remaining = matches.length - shown.length;
  return (
    <details className="mt-1">
      <summary className="text-muted-foreground cursor-pointer text-xs select-none">
        {matches.length} {matches.length === 1 ? "company" : "companies"} with
        the same name — may be a different person
      </summary>
      <ul className="text-muted-foreground mt-1 ml-4 flex list-disc flex-col gap-0.5 text-xs">
        {shown.map((m) => {
          const flag =
            getCountry(m.country_iso2.toLowerCase())?.flag ?? m.country_iso2;
          return (
            <li key={`${m.country_iso2}-${m.person_id}-${m.company_id}`}>
              {flag}{" "}
              <Link
                to={`/company/${m.country_iso2.toLowerCase()}/${m.company_id}`}
                className="text-foreground hover:underline"
              >
                {m.company_name}
              </Link>
              {" — "}
              {ROLE_LABELS[m.role_kind] ?? m.role_kind}
              {m.fiscal_year ? `, ${m.fiscal_year}` : ""}
              {" · "}
              <Link
                to={`/country/${m.country_iso2.toLowerCase()}/person/${m.person_id}`}
                className="text-foreground hover:underline"
              >
                person profile
              </Link>
            </li>
          );
        })}
        {remaining > 0 ? <li>…and {remaining} more</li> : null}
      </ul>
    </details>
  );
}

function PersonRow({
  person,
  matches,
}: {
  person: DistinctCompanyPerson;
  matches: PeopleMatchRow[];
}) {
  const originalLabels = [
    ...new Set(
      person.roles
        .map((role) => role.originalLabel)
        .filter(
          (originalLabel) =>
            originalLabel !== "" &&
            !person.roles.some((role) => role.label === originalLabel),
        ),
    ),
  ];
  return (
    <li className="py-1.5">
      <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
        {person.profileUrl ? (
          <Link to={person.profileUrl} className="font-medium hover:underline">
            {person.name}
          </Link>
        ) : person.externalUrl ? (
          <a
            href={person.externalUrl}
            target="_blank"
            rel="noreferrer"
            className="font-medium hover:underline"
          >
            {person.name}
          </a>
        ) : (
          <span className="font-medium">{person.name}</span>
        )}
        {person.roles.map((role) => (
          <Badge
            key={role.key}
            variant={EMPHASIZED_ROLES.has(role.kind) ? "default" : "outline"}
          >
            {role.label}
          </Badge>
        ))}
        {originalLabels.length > 0 ? (
          <span className="text-muted-foreground text-xs">
            {originalLabels.join(" · ")}
          </span>
        ) : null}
      </div>
      <SameNameMatches matches={matches} />
      <EvidencePanel
        evidence={person.evidence}
        summaryLabel="Sources and connections"
      />
    </li>
  );
}

/** The audit-firm and opinion-form line under the Auditor block. A modified
 * opinion (audit report deviating from the standard form) is a rare,
 * strong distress signal — styled destructively on purpose. */
function AuditLine({ audit }: { audit: AuditRow | null }) {
  if (!audit || (audit.audit_firm === "" && audit.opinion_kind === "unknown"))
    return null;
  return (
    <div className="mt-1 flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
      {audit.audit_firm !== "" ? (
        <span className="text-muted-foreground text-xs">
          Audited by {audit.audit_firm}
        </span>
      ) : null}
      {audit.opinion_kind === "standard" ? (
        <Badge variant="outline">standard opinion</Badge>
      ) : null}
      {audit.opinion_kind === "modified" ? (
        <Badge variant="destructive">modified opinion</Badge>
      ) : null}
      {audit.opinion_date && audit.opinion_date !== "" ? (
        <span className="text-muted-foreground text-xs tabular-nums">
          {audit.opinion_date}
        </span>
      ) : null}
    </div>
  );
}

/** Management section for countries with signatory data (currently SE, from
 * XBRL annual-report signatures). Empty → render nothing. */
export function ManagementSection({
  officers,
  peopleMatches,
  audit,
  wikidataPeople = [],
  esefPeople = [],
}: {
  officers: OfficerRow[];
  peopleMatches: PeopleMatchRow[];
  audit: AuditRow | null;
  wikidataPeople?: WikidataPersonRow[];
  esefPeople?: EsefPersonObservation[];
}) {
  if (
    officers.length === 0 &&
    wikidataPeople.length === 0 &&
    esefPeople.length === 0
  ) {
    return null;
  }

  const people = buildDistinctCompanyPeople(
    officers,
    wikidataPeople,
    esefPeople,
  );

  const matchesByName = new Map<string, PeopleMatchRow[]>();
  for (const m of peopleMatches) {
    const list = matchesByName.get(m.full_name_normalized) ?? [];
    list.push(m);
    matchesByName.set(m.full_name_normalized, list);
  }
  const matchesFor = (person: DistinctCompanyPerson) => {
    const relationshipKinds = new Set(
      person.roles.map((role) => relationshipKindForRole(role.kind)),
    );
    return (matchesByName.get(person.normalizedName) ?? []).filter((match) =>
      relationshipKinds.has(relationshipKindForRole(match.role_kind)),
    );
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">People and roles</CardTitle>
        <CardDescription>
          Distinct people connected to this company, including leadership,
          governance, and external professionals, with source evidence combined.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-5">
          {people.length > 0 ? (
            <section>
              <p className="text-muted-foreground mb-1 text-xs font-medium uppercase tracking-wide">
                Company connections
              </p>
              <ul className="divide-y">
                {people.map((person) => (
                  <PersonRow
                    key={person.key}
                    person={person}
                    matches={matchesFor(person)}
                  />
                ))}
              </ul>
            </section>
          ) : null}
        </div>
        {audit ? (
          <div className="mt-3 flex flex-col gap-2">
            {people.length > 0 ? <Separator /> : null}
            <p className="text-muted-foreground mb-1 text-xs font-medium uppercase tracking-wide">
              Audit details
            </p>
            <AuditLine audit={audit} />
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}
