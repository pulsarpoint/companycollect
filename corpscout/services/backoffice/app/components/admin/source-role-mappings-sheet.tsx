import {
  ArrowRightIcon,
  GitMergeIcon,
  LoaderCircleIcon,
  PlusIcon,
} from "lucide-react";
import { useEffect, useId, useState } from "react";
import { useFetcher } from "react-router";
import {
  Alert,
  AlertDescription,
  AlertTitle,
} from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "~/components/ui/dialog";
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "~/components/ui/field";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "~/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "~/components/ui/tabs";
import type { CompanyPersonRoleType } from "~/lib/company-roles.server";
import type { SwedenSourceRoleRow } from "~/lib/sweden-role-mappings.server";
import {
  getPeopleSourceDefinition,
  type PeopleSourceName,
} from "~/lib/people-sources";

const SOURCE_ORDER: PeopleSourceName[] = [
  "bolagsverket",
  "esef",
  "wikidata",
];
const numberFormat = new Intl.NumberFormat("en-US");

type MappingActionData =
  | { ok: true }
  | { ok: false; error: string };

function AddRoleMappingDialog({
  row,
  canonicalRoles,
  onOpenChange,
}: {
  row: SwedenSourceRoleRow;
  canonicalRoles: CompanyPersonRoleType[];
  onOpenChange: (open: boolean) => void;
}) {
  const fetcher = useFetcher<MappingActionData>();
  const canonicalRoleId = useId();
  const submitting = fetcher.state !== "idle";
  const canonicalRoleItems = canonicalRoles.map((role) => ({
    label: role.display_name,
    value: role.role_code,
  }));

  useEffect(() => {
    if (fetcher.state === "idle" && fetcher.data?.ok) {
      onOpenChange(false);
    }
  }, [fetcher.data, fetcher.state, onOpenChange]);

  return (
    <Dialog open onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Add canonical role mapping</DialogTitle>
          <DialogDescription>
            This exact source value will be mapped. Other observations with the
            native code <code>{row.source_role_code}</code> remain unchanged.
          </DialogDescription>
        </DialogHeader>

        <fetcher.Form method="post" className="flex flex-col gap-4">
          <input type="hidden" name="source" value={row.source} />
          <input
            type="hidden"
            name="source_role_code"
            value={row.source_role_code}
          />
          <input
            type="hidden"
            name="source_role_name"
            value={row.source_role_name}
          />

          <FieldGroup>
            <Field>
              <FieldLabel>Original role</FieldLabel>
              <code className="rounded-md bg-muted px-3 py-2 text-sm">
                {row.source_role_name}
              </code>
              <FieldDescription>
                {getPeopleSourceDefinition(row.source).label} · native code{" "}
                {row.source_role_code}
              </FieldDescription>
            </Field>
            <Field>
              <FieldLabel htmlFor={canonicalRoleId}>
                Canonical role
              </FieldLabel>
              <Select
                items={canonicalRoleItems}
                name="canonical_role_code"
                defaultValue={canonicalRoles[0]?.role_code}
                required
              >
                <SelectTrigger
                  id={canonicalRoleId}
                  className="w-full"
                >
                  <SelectValue placeholder="Select a canonical role" />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    {canonicalRoles.map((role) => (
                      <SelectItem key={role.role_code} value={role.role_code}>
                        {role.display_name}
                        <code className="text-xs text-muted-foreground">
                          {role.role_code}
                        </code>
                      </SelectItem>
                    ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
            </Field>
          </FieldGroup>

          {fetcher.data && !fetcher.data.ok ? (
            <Alert variant="destructive">
              <AlertTitle>Mapping was not saved</AlertTitle>
              <AlertDescription>{fetcher.data.error}</AlertDescription>
            </Alert>
          ) : null}

          <DialogFooter>
            <DialogClose render={<Button type="button" variant="outline" />}>
              Cancel
            </DialogClose>
            <Button
              type="submit"
              disabled={submitting || canonicalRoles.length === 0}
            >
              {submitting ? (
                <LoaderCircleIcon
                  data-icon="inline-start"
                  className="animate-spin"
                />
              ) : null}
              Save mapping
            </Button>
          </DialogFooter>
        </fetcher.Form>
      </DialogContent>
    </Dialog>
  );
}

function MappingCell({
  row,
  onAddMapping,
}: {
  row: SwedenSourceRoleRow;
  onAddMapping: (row: SwedenSourceRoleRow) => void;
}) {
  if (row.mapping_status === "unmapped") {
    return (
      <Button variant="outline" size="sm" onClick={() => onAddMapping(row)}>
        <PlusIcon data-icon="inline-start" />
        Add mapping
      </Button>
    );
  }
  if (row.mapping_status === "roleless") {
    return <Badge variant="outline">Roleless</Badge>;
  }
  return (
    <span className="inline-flex items-center gap-1.5">
      <ArrowRightIcon />
      <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
        {row.canonical_role_code}
      </code>
    </span>
  );
}

function SourceRoleTable({
  rows,
  onAddMapping,
}: {
  rows: SwedenSourceRoleRow[];
  onAddMapping: (row: SwedenSourceRoleRow) => void;
}) {
  return (
    <div className="overflow-x-auto rounded-lg border">
      <Table className="min-w-[52rem]">
        <TableHeader>
          <TableRow>
            <TableHead>Original role</TableHead>
            <TableHead>Native code</TableHead>
            <TableHead>Canonical mapping</TableHead>
            <TableHead className="text-right">Observations</TableHead>
            <TableHead className="text-right">Companies</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => (
            <TableRow
              key={`${row.source}:${row.source_role_code}:${row.source_role_name}`}
            >
              <TableCell className="max-w-sm whitespace-normal font-medium">
                {row.source_role_name}
              </TableCell>
              <TableCell>
                <code className="text-xs">{row.source_role_code}</code>
              </TableCell>
              <TableCell>
                <MappingCell row={row} onAddMapping={onAddMapping} />
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {numberFormat.format(row.observation_count)}
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {numberFormat.format(row.company_count)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

export function SourceRoleMappingsSheet({
  rows,
  canonicalRoles,
}: {
  rows: SwedenSourceRoleRow[];
  canonicalRoles: CompanyPersonRoleType[];
}) {
  const [mappingRow, setMappingRow] = useState<SwedenSourceRoleRow | null>(null);
  const rowsBySource = new Map(
    SOURCE_ORDER.map((source) => [
      source,
      rows.filter((row) => row.source === source),
    ]),
  );
  const unmappedCount = rows.filter(
    (row) => row.mapping_status === "unmapped",
  ).length;

  return (
    <Sheet>
      <SheetTrigger render={<Button variant="outline" />}>
        <GitMergeIcon data-icon="inline-start" />
        Source mappings
      </SheetTrigger>
      <SheetContent
        side="right"
        className="w-full data-[side=right]:sm:max-w-4xl"
      >
        <SheetHeader>
          <div className="flex flex-wrap items-center gap-2 pr-8">
            <SheetTitle>Original source roles and mappings</SheetTitle>
            <Badge variant="outline">Sweden</Badge>
            <Badge variant="secondary">{rows.length} distinct values</Badge>
            {unmappedCount > 0 ? (
              <Badge variant="destructive">{unmappedCount} unmapped</Badge>
            ) : null}
          </div>
          <SheetDescription>
            Original role values and counts come from ClickHouse. Canonical
            mappings are stored in content/sweden/people/role_mappings.sqlite,
            seeded from source-owned Dagster mappings and curated here.
          </SheetDescription>
        </SheetHeader>

        <Tabs defaultValue="bolagsverket" className="min-h-0 flex-1">
          <TabsList className="mx-4 w-auto">
            {SOURCE_ORDER.map((source) => {
              const definition = getPeopleSourceDefinition(source);
              return (
                <TabsTrigger key={source} value={source}>
                  {definition.label}
                  <Badge variant="outline">
                    {rowsBySource.get(source)?.length ?? 0}
                  </Badge>
                </TabsTrigger>
              );
            })}
          </TabsList>
          {SOURCE_ORDER.map((source) => (
            <TabsContent
              key={source}
              value={source}
              className="min-h-0 overflow-y-auto px-4 pb-4"
            >
              <SourceRoleTable
                rows={rowsBySource.get(source) ?? []}
                onAddMapping={setMappingRow}
              />
            </TabsContent>
          ))}
        </Tabs>
      </SheetContent>
      {mappingRow ? (
        <AddRoleMappingDialog
          row={mappingRow}
          canonicalRoles={canonicalRoles}
          onOpenChange={(open) => {
            if (!open) {
              setMappingRow(null);
            }
          }}
        />
      ) : null}
    </Sheet>
  );
}
