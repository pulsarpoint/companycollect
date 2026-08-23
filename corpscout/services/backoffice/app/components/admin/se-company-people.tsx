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

/**
 * The published people of this company. Each row links to the person review
 * page, which is keyed on (company, person) -- the same person name at
 * another company is another person here.
 */
export function SeCompanyPeopleTab({
  companyId,
  people,
}: {
  companyId: string;
  people: SeCompanyPersonRow[];
}) {
  if (people.length === 0) {
    return (
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <UsersRoundIcon />
          </EmptyMedia>
          <EmptyTitle>No people recorded</EmptyTitle>
          <EmptyDescription>
            Dagster has published no people for this company yet. People appear
            once their drafts are resolved into se_company_person.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  const withoutRoles = people.filter(
    (person) => person.roles.length === 0,
  ).length;
  return (
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
  );
}
