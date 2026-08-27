import { CompanySourceStrip } from "~/components/admin/company-source-strip";
import { UsersRoundIcon } from "lucide-react";
import { Link } from "react-router";
import { EMPTY_VALUE } from "~/components/admin/definition-list";
import { Badge } from "~/components/ui/badge";
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import type {
  SeCompanyPersonEvidenceGroup,
  SeCompanyPersonRoleRow,
  SeCompanyPersonRow,
} from "~/lib/se-company-people.server";


/** A role reads as "Board member 2024 · esef" -- the label, the year it was
 * observed for and who observed it, because all three change independently. */
function RoleBadge({ role }: { role: SeCompanyPersonRoleRow }) {
  return (
    <Badge variant={role.is_current ? "secondary" : "outline"}>
      {role.role_label}
      {role.fiscal_year === "" ? "" : ` ${role.fiscal_year}`}
      {role.sources.length === 0 ? "" : ` · ${role.sources.join(", ")}`}
    </Badge>
  );
}

/** What each source says about this company's people, verbatim: one row per
 * person name, the roles exactly as the source wrote them (no canonical
 * mapping, no identity resolution -- two spellings are two rows). This is the
 * tab's primary content until the Ratsit spine lands. */
function SourceEvidenceCard({
  evidence,
}: {
  evidence: SeCompanyPersonEvidenceGroup[];
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">
          {evidence.length} {evidence.length === 1 ? "person" : "people"} found
          in sources
        </CardTitle>
        <CardDescription>
          Everything the Bolagsverket, ESEF and Wikidata source tables say about
          this company's people, with roles exactly as each source wrote them.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Roles as reported</TableHead>
              <TableHead>Sources</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {evidence.map((group) => (
              <TableRow key={group.full_name}>
                <TableCell className="font-medium">{group.full_name}</TableCell>
                <TableCell>
                  <div className="flex flex-wrap gap-1">
                    {group.entries.map((entry, index) => (
                      <Badge
                        key={`${entry.source}:${entry.role}:${entry.period}:${index}`}
                        variant="outline"
                      >
                        {entry.role}
                        {entry.period === "" ? "" : ` · ${entry.period}`}
                      </Badge>
                    ))}
                  </div>
                </TableCell>
                <TableCell>
                  <div className="flex flex-wrap gap-1">
                    {group.sources.map((source) => (
                      <Badge key={source} variant="secondary">
                        {source}
                      </Badge>
                    ))}
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

/**
 * Source evidence first (verbatim per-source people and roles), then the
 * published/merged people when the pipeline has produced any. Each published
 * row links to the person review page, which is keyed on (company, person) --
 * the same person name at another company is another person here.
 */
export function SeCompanyPeopleTab({
  companyId,
  people,
  evidence,
}: {
  companyId: string;
  people: SeCompanyPersonRow[];
  evidence: SeCompanyPersonEvidenceGroup[];
}) {
  if (people.length === 0 && evidence.length === 0) {
    return (
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <UsersRoundIcon />
          </EmptyMedia>
          <EmptyTitle>No people recorded</EmptyTitle>
          <EmptyDescription>
            No source has reported people for this company, and Dagster has
            published none.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  if (people.length === 0) {
    return <SourceEvidenceCard evidence={evidence} />;
  }
  const withoutRoles = people.filter(
    (person) => person.roles.length === 0,
  ).length;
  return (
    <div className="flex flex-col gap-4">
      {evidence.length === 0 ? null : <SourceEvidenceCard evidence={evidence} />}
      {/* The registers behind the ROLES, which is the only provenance the
          published person rows carry (se_company_person has no source column;
          se_company_person_role.sources does). A company whose people have no
          resolved role yet therefore shows an em dash here -- the same "no
          role the curation could place" the count sentence below reports. */}
      <CompanySourceStrip
        sources={people.flatMap((person) =>
          person.roles.flatMap((role) => role.sources),
        )}
      />
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-center gap-2">
            {/* One count sentence rather than a title plus two count badges:
                "how many, and how many are incomplete" is one fact. */}
            <CardTitle className="text-base">
              {people.length} {people.length === 1 ? "person" : "people"}
              {withoutRoles > 0 ? ` · ${withoutRoles} without a role` : ""}
            </CardTitle>
          </div>
          <CardDescription>
            Published rows from se_company_person, with the roles resolved into
            se_company_person_role. A person with no role has evidence but no
            role the curation could place yet.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Roles</TableHead>
                <TableHead className="text-right">Drafts</TableHead>
                <TableHead className="text-right">Corrections</TableHead>
                <TableHead>Updated</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {people.map((person) => (
                <TableRow key={person.person_id}>
                  <TableCell>
                    <Link
                      className="underline underline-offset-2"
                      to={`/admin/se/people/person/${encodeURIComponent(companyId)}/${encodeURIComponent(person.person_id)}`}
                    >
                      {person.name}
                    </Link>
                    {person.merged_into_person_id === "" ? null : (
                      <Badge className="ml-2" variant="outline">
                        merged
                      </Badge>
                    )}
                    {person.description === "" ? null : (
                      <p className="text-xs text-muted-foreground">
                        {person.description}
                      </p>
                    )}
                  </TableCell>
                  <TableCell>
                    {person.roles.length === 0 ? (
                      EMPTY_VALUE
                    ) : (
                      <div className="flex flex-wrap gap-1">
                        {person.roles.map((role) => (
                          <RoleBadge
                            key={`${role.role_code}:${role.fiscal_year}`}
                            role={role}
                          />
                        ))}
                      </div>
                    )}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {person.draft_count}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {person.correction_count}
                  </TableCell>
                  <TableCell className="text-xs tabular-nums">
                    {person.updated_at}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
