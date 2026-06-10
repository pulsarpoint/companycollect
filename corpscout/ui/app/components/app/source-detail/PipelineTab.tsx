import type { DataSource } from "~/types/api";
import { Badge } from "~/components/ui/badge";

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="grid grid-cols-[180px_1fr] gap-2 py-1.5">
      <span className="text-sm text-muted-foreground">{label}</span>
      <span className="text-sm">{value}</span>
    </div>
  );
}

export function PipelineTab({ source }: { source: DataSource }) {
  return (
    <div className="rounded-md border divide-y">
      <div className="p-4 space-y-0.5">
        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-3">
          Source storage
        </p>
        <Row label="Registry key" value={<code className="font-mono text-xs">{source.registry_key}</code>} />
        <Row label="Storage" value={<Badge variant="outline">{source.storage_kind}</Badge>} />
        <Row label="ClickHouse database" value={<code className="font-mono text-xs">{source.clickhouse_database}</code>} />
        <Row label="Table prefix" value={<code className="font-mono text-xs">{source.clickhouse_table_prefix}</code>} />
      </div>
    </div>
  );
}
