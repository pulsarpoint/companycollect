import type { DataSource } from "~/types/api";
import { sourceDisplayName, sourceInputDisplayName } from "~/components/app/source-detail/sourceDetailUtils";
import { Badge } from "~/components/ui/badge";

interface SourceHeaderProps {
  source: DataSource;
}

function triggerLabel(source: DataSource) {
  if (source.name === "brreg") return "BRREG task actions";
  if (source.capabilities.includes("source_download")) return "Temporal actions";
  if (source.download_workflow_registered) return "Temporal download";
  return "No source trigger";
}

export function SourceHeader({ source }: SourceHeaderProps) {
  return (
    <div className="space-y-3">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0 space-y-2">
          <h1 className="text-2xl font-semibold tracking-tight">
            {sourceDisplayName(source)}
          </h1>
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="outline">{source.source_group}</Badge>
            <Badge variant="outline">{triggerLabel(source)}</Badge>
            {source.enabled ? (
              <Badge className="border-green-200 bg-green-100 text-green-800" variant="outline">
                Enabled
              </Badge>
            ) : (
              <Badge className="text-muted-foreground" variant="outline">
                Disabled
              </Badge>
            )}
          </div>
        </div>
        <div className="text-sm text-muted-foreground sm:text-right">
          <div className="font-mono">{source.name}</div>
          <div>{sourceInputDisplayName(source)}</div>
        </div>
      </div>
      {source.description && (
        <p className="max-w-3xl text-sm leading-6 text-muted-foreground">
          {source.description}
        </p>
      )}
    </div>
  );
}
