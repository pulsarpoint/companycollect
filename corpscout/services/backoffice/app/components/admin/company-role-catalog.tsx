import { DatabaseIcon, TagsIcon } from "lucide-react";
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
import type { CompanyPersonRoleType } from "~/lib/company-roles.server";

function displayGroup(roleGroup: string): string {
  return roleGroup.charAt(0).toUpperCase() + roleGroup.slice(1);
}

function formattedTimestamp(value: string): string {
  return value.replace("T", " ").slice(0, 19);
}

export function CompanyRoleCatalog({
  roles,
}: {
  roles: CompanyPersonRoleType[];
}) {
  const activeRoles = roles.filter((role) => role.is_active === 1).length;
  const roleGroups = new Set(roles.map((role) => role.role_group)).size;

  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-1">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-2xl font-semibold tracking-tight">
            Canonical company roles
          </h1>
          <Badge variant="outline">ClickHouse</Badge>
        </div>
        <p className="max-w-3xl text-sm text-muted-foreground">
          The controlled role vocabulary used to normalize source roles before
          company people are published.
        </p>
      </header>

      <section
        aria-label="Canonical role summary"
        className="grid gap-3 sm:grid-cols-3"
      >
        <Card size="sm">
          <CardHeader>
            <CardDescription>Total roles</CardDescription>
            <CardTitle className="text-2xl tabular-nums">
              {roles.length}
            </CardTitle>
          </CardHeader>
        </Card>
        <Card size="sm">
          <CardHeader>
            <CardDescription>Active roles</CardDescription>
            <CardTitle className="text-2xl tabular-nums">
              {activeRoles}
            </CardTitle>
          </CardHeader>
        </Card>
        <Card size="sm">
          <CardHeader>
            <CardDescription>Role groups</CardDescription>
            <CardTitle className="text-2xl tabular-nums">
              {roleGroups}
            </CardTitle>
          </CardHeader>
        </Card>
      </section>

      <Card className="overflow-hidden">
        <CardHeader className="border-b">
          <div className="flex items-start gap-3">
            <span className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-muted">
              <DatabaseIcon />
            </span>
            <div className="flex flex-col gap-1">
              <CardTitle>Role pool</CardTitle>
              <CardDescription>
                Live rows from corpscout.company_person_role_type.
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          {roles.length === 0 ? (
            <Empty>
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <TagsIcon />
                </EmptyMedia>
                <EmptyTitle>No canonical roles found</EmptyTitle>
                <EmptyDescription>
                  The ClickHouse role pool does not currently contain any rows.
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <div className="overflow-x-auto">
              <Table className="min-w-[58rem]">
                <TableHeader>
                  <TableRow>
                    <TableHead>Role</TableHead>
                    <TableHead>Code</TableHead>
                    <TableHead>Group</TableHead>
                    <TableHead>Description</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Updated</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {roles.map((role) => (
                    <TableRow key={role.role_code}>
                      <TableCell className="font-medium">
                        {role.display_name}
                      </TableCell>
                      <TableCell>
                        <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
                          {role.role_code}
                        </code>
                      </TableCell>
                      <TableCell>
                        <Badge variant="secondary">
                          {displayGroup(role.role_group)}
                        </Badge>
                      </TableCell>
                      <TableCell className="max-w-md whitespace-normal text-muted-foreground">
                        {role.description}
                      </TableCell>
                      <TableCell>
                        <Badge
                          variant={role.is_active === 1 ? "default" : "outline"}
                        >
                          {role.is_active === 1 ? "Active" : "Inactive"}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-xs tabular-nums text-muted-foreground">
                        {formattedTimestamp(role.updated_at)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
