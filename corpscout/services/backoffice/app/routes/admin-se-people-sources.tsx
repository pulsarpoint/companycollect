import { ArrowRightIcon, DatabaseIcon } from "lucide-react";
import { Link } from "react-router";
import type { Route } from "./+types/admin-se-people-sources";
import { SourceRoleMappingsSheet } from "~/components/admin/source-role-mappings-sheet";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  getCompanyPersonRoleTypes,
  type CompanyPersonRoleType,
} from "~/lib/company-roles.server";
import {
  isPeopleSourceName,
  PEOPLE_SOURCE_CATALOG,
} from "~/lib/people-sources";
import {
  getSwedenSourceRoleRows,
  saveSwedenRoleMapping,
  type SwedenSourceRoleRow,
} from "~/lib/sweden-role-mappings.server";

export async function loader() {
  const [sourceRoles, canonicalRoles] = await Promise.all([
    getSwedenSourceRoleRows(),
    getCompanyPersonRoleTypes(),
  ]);
  return {
    sourceRoles,
    canonicalRoles: canonicalRoles.filter((role) => role.is_active === 1),
  };
}

export async function action({ request }: Route.ActionArgs) {
  const form = await request.formData();
  const source = String(form.get("source") ?? "");
  const sourceRoleCode = String(form.get("source_role_code") ?? "").trim();
  const sourceRoleName = String(form.get("source_role_name") ?? "").trim();
  const canonicalRoleCode = String(
    form.get("canonical_role_code") ?? "",
  ).trim();

  if (!isPeopleSourceName(source)) {
    return { ok: false as const, error: "Unknown people source." };
  }
  if (
    sourceRoleCode === "" ||
    sourceRoleName === "" ||
    sourceRoleName.toLocaleLowerCase("en") === "unknown"
  ) {
    return { ok: false as const, error: "An original role is required." };
  }

  const canonicalRoles = await getCompanyPersonRoleTypes();
  const canonicalRole = canonicalRoles.find(
    (role) =>
      role.role_code === canonicalRoleCode && role.is_active === 1,
  );
  if (!canonicalRole) {
    return { ok: false as const, error: "Select an active canonical role." };
  }

  saveSwedenRoleMapping({
    source,
    source_role_code: sourceRoleCode,
    source_role_name: sourceRoleName,
    canonical_role_code: canonicalRole.role_code,
  });
  return { ok: true as const };
}

export function meta() {
  return [{ title: "Sweden people sources | CompanyCollect" }];
}

export function SwedenPeopleSourcesCatalog({
  sourceRoles,
  canonicalRoles,
}: {
  sourceRoles: SwedenSourceRoleRow[];
  canonicalRoles: CompanyPersonRoleType[];
}) {
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col justify-between gap-3 md:flex-row md:items-start">
        <div className="flex flex-col gap-1">
          <div className="flex items-center gap-2">
            <h1 className="text-2xl font-semibold tracking-tight">
              People sources
            </h1>
            <Badge variant="outline">ClickHouse</Badge>
          </div>
          <p className="max-w-2xl text-sm text-muted-foreground">
            Inspect the source-owned observations before they enter draft matching
            and person normalization.
          </p>
        </div>
        <SourceRoleMappingsSheet
          rows={sourceRoles}
          canonicalRoles={canonicalRoles}
        />
      </header>

      <section
        aria-label="Available Sweden people sources"
        className="grid gap-4 md:grid-cols-2 xl:grid-cols-3"
      >
        {PEOPLE_SOURCE_CATALOG.map((source) => (
          <Card key={source.name}>
            <CardHeader>
              <span className="flex size-9 items-center justify-center rounded-lg bg-muted">
                <DatabaseIcon />
              </span>
              <CardTitle>{source.label}</CardTitle>
              <CardDescription>{source.description}</CardDescription>
            </CardHeader>
            <CardContent className="flex flex-wrap gap-1.5">
              {source.tables.map((table) => (
                <Badge key={table} variant="secondary">
                  {table}
                </Badge>
              ))}
            </CardContent>
            <CardFooter className="justify-end">
              <Button
                variant="outline"
                nativeButton={false}
                render={
                  <Link to={`/admin/se/people/sources/${source.name}`} />
                }
              >
                View source rows
                <ArrowRightIcon data-icon="inline-end" />
              </Button>
            </CardFooter>
          </Card>
        ))}
      </section>
    </div>
  );
}

export default function AdminSwedenPeopleSources({
  loaderData,
}: Route.ComponentProps) {
  return (
    <SwedenPeopleSourcesCatalog
      sourceRoles={loaderData.sourceRoles}
      canonicalRoles={loaderData.canonicalRoles}
    />
  );
}
