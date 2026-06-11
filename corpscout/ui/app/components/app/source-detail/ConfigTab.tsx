import { ExternalLink } from "lucide-react";
import { Link } from "react-router";
import type { DataSource } from "~/types/api";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableRow,
} from "~/components/ui/table";

interface ConfigTabProps {
  source: DataSource;
}

interface MetadataRow {
  label: string;
  value: string;
  href?: string;
}

function boolLabel(value: boolean): string {
  return value ? "Yes" : "No";
}

function relatedSourceRows(source: DataSource): MetadataRow[] {
  switch (source.name) {
    case "finland_prhytj":
      return [
        {
          label: "Related financial source",
          value: "finland_prh_xbrl",
          href: "/sources/finland_prh_xbrl/actions",
        },
      ];
    case "finland_prh_xbrl":
      return [
        {
          label: "Related registry source",
          value: "finland_prhytj",
          href: "/sources/finland_prhytj/actions",
        },
      ];
    default:
      return [];
  }
}

function metadataRows(source: DataSource): MetadataRow[] {
  return [
    { label: "Registry key", value: source.registry_key },
    { label: "Country", value: source.country },
    { label: "Source", value: source.source },
    { label: "Source group", value: source.source_group },
    ...relatedSourceRows(source),
    { label: "Source URL", value: source.source_url, href: source.source_url },
    { label: "Documentation", value: source.docs_url, href: source.docs_url },
    { label: "Raw source retention", value: source.raw_source_retention },
    { label: "Primary output file", value: source.source_file_name },
    { label: "Authentication required", value: boolLabel(source.auth_required) },
    { label: "User-Agent required", value: boolLabel(source.user_agent_required) },
    { label: "Capabilities", value: source.capabilities.join(", ") },
    { label: "Requires translation", value: boolLabel(source.requires_translation) },
    { label: "Storage", value: source.storage_kind },
    { label: "ClickHouse database", value: source.clickhouse_database },
    { label: "ClickHouse table prefix", value: source.clickhouse_table_prefix },
  ];
}

function MetadataValue({ row }: { row: MetadataRow }) {
  if (!row.value) {
    return <span className="text-muted-foreground">-</span>;
  }
  if (!row.href) {
    return <span className="font-mono text-xs">{row.value}</span>;
  }
  if (row.href.startsWith("/")) {
    return (
      <Link
        to={row.href}
        className="inline-flex min-w-0 items-center gap-1 font-mono text-xs text-primary hover:underline"
      >
        <span className="break-all">{row.value}</span>
      </Link>
    );
  }
  return (
    <a
      href={row.href}
      target="_blank"
      rel="noreferrer"
      className="inline-flex min-w-0 items-center gap-1 font-mono text-xs text-primary hover:underline"
    >
      <span className="break-all">{row.value}</span>
      <ExternalLink className="size-3 shrink-0" />
    </a>
  );
}

export function ConfigTab({ source }: ConfigTabProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Source catalog</CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableBody>
            {metadataRows(source).map((row) => (
              <TableRow key={row.label}>
                <TableCell className="w-48 text-muted-foreground">
                  {row.label}
                </TableCell>
                <TableCell className="max-w-0 whitespace-normal">
                  <MetadataValue row={row} />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}
