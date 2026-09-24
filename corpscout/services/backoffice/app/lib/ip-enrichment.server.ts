import { randomUUID } from "node:crypto";
import { isIP } from "node:net";
import {
  dagsterRunUrl,
  launchRun,
  type DagsterOptions,
} from "~/lib/dagster.server";
import type { WorkspaceIpSelection } from "~/lib/workspace-ip-selection";

export class IpEnrichmentSelectionError extends Error {}

function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new IpEnrichmentSelectionError("Choose addresses to enrich.");
  }
  return value as Record<string, unknown>;
}

function ipList(value: unknown): string[] {
  if (
    !Array.isArray(value) ||
    value.length > 10000 ||
    value.some(
      (ip) =>
        typeof ip !== "string" ||
        ip.length > 45 ||
        ip.includes("%") ||
        !isIP(ip),
    )
  ) {
    throw new IpEnrichmentSelectionError(
      "Select valid IP addresses (up to 10,000 individually), or use Select all matching.",
    );
  }
  return [...new Set(value as string[])];
}

export function parseIpEnrichmentSelection(
  value: unknown,
): WorkspaceIpSelection {
  const selection = record(value);
  if (
    selection.mode === "ips" &&
    Object.keys(selection).every((key) => ["mode", "ips"].includes(key))
  ) {
    const ips = ipList(selection.ips);
    if (!ips.length)
      throw new IpEnrichmentSelectionError("Select at least one IP address.");
    return { mode: "ips", ips };
  }
  if (
    selection.mode === "all" &&
    Object.keys(selection).every((key) =>
      ["mode", "filters", "excludedIps"].includes(key),
    )
  ) {
    const filters = record(selection.filters);
    if (
      Object.keys(filters).some(
        (key) => !["search", "version"].includes(key),
      ) ||
      typeof filters.search !== "string" ||
      (filters.search !== "" && !/^[0-9a-f:.]{1,45}$/.test(filters.search)) ||
      (filters.version !== "any" &&
        filters.version !== "4" &&
        filters.version !== "6")
    ) {
      throw new IpEnrichmentSelectionError(
        "The IP address filters are invalid. Apply the filters again.",
      );
    }
    return {
      mode: "all",
      filters: {
        search: filters.search,
        version: filters.version as "any" | "4" | "6",
      },
      excludedIps: ipList(selection.excludedIps),
    };
  }
  throw new IpEnrichmentSelectionError(
    "Choose selected addresses or all matching addresses.",
  );
}

export async function launchIpEnrichment(
  value: unknown,
  requestedBy: string,
  options: DagsterOptions = {},
) {
  const selection = parseIpEnrichmentSelection(value);
  const taskId = randomUUID();
  const input: Record<string, unknown> = {
    task_id: taskId,
    source_name: "backoffice:ip-addresses",
    source_relation: "corpscout.commoncrawl_ip_addresses",
    observed_at_column: "last_seen",
  };
  if (selection.mode === "ips") {
    input.filters = { ip: selection.ips };
  } else {
    // Dagster freezes the whole matching inventory in ClickHouse, independently of pagination.
    Object.assign(input, {
      select_all: true,
      ip_search: selection.filters.search,
      filters:
        selection.filters.version === "any"
          ? {}
          : { ip_version: [selection.filters.version] },
      excluded_ips: selection.excludedIps,
    });
  }
  const run = await launchRun(
    {
      job: "ip_enrichment_workflow",
      runConfig: {
        ops: {
          ip_enrichment_input: { config: input },
          // Keep RDAP throttling/cache reuse, but process the complete submitted batch.
          ip_enrichment_results: {
            config: { task_id: taskId, max_requests: null },
          },
        },
      },
      tags: {
        "processing/task_id": taskId,
        "corpscout/trigger_source": "backoffice",
        "corpscout/request_id": taskId,
        "corpscout/requested_by": requestedBy,
      },
    },
    options,
  );
  return {
    ok: true as const,
    ...run,
    taskId,
    runUrl: dagsterRunUrl(run.runId, options.url),
  };
}
