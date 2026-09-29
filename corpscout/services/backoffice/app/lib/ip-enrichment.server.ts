import { createHash } from "node:crypto";
import { isIP } from "node:net";
import {
  dagsterRunUrl,
  launchRun,
  listRuns,
  runStatus,
  type DagsterOptions,
} from "~/lib/dagster.server";
import { QUEUE_UUID } from "~/lib/queues";
import {
  IP_LIST_FILTER_KEYS,
  isCityName,
  isCountryCode,
  isRegionCode,
  MAX_FILTER_VALUES,
  normalizeAsn,
  withLocationScope,
  type WorkspaceIpFilters,
} from "~/lib/workspace-ip-addresses";
import type { WorkspaceIpSelection } from "~/lib/workspace-ip-selection";
import { IP_SEARCH_RELATION } from "~/lib/workspace-ip-addresses.server";

const JOB = "ip_enrichment_input_job";
const ASSET = "ip_enrichment_input";
const submissions = new Map<string, Promise<unknown>>();

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

function invalidFilters(): never {
  throw new IpEnrichmentSelectionError(
    "The IP address filters are invalid. Apply the filters again.",
  );
}

function filterValues(
  value: unknown,
  valid: (item: string) => boolean,
): string[] {
  if (value === undefined) return [];
  if (
    !Array.isArray(value) ||
    value.length > MAX_FILTER_VALUES ||
    value.some((item) => typeof item !== "string" || !valid(item))
  )
    invalidFilters();
  return [...new Set(value as string[])].sort();
}

/** Only the list's own filters, each value in its URL form (the list parser's rules). */
function parseSelectionFilters(
  filters: Record<string, unknown>,
): WorkspaceIpFilters {
  if (
    Object.keys(filters).some(
      (key) =>
        !["search", "version", ...IP_LIST_FILTER_KEYS].includes(key),
    ) ||
    typeof filters.search !== "string" ||
    (filters.search !== "" && !/^[0-9a-f:.]{1,45}$/.test(filters.search)) ||
    (filters.version !== "any" &&
      filters.version !== "4" &&
      filters.version !== "6")
  )
    invalidFilters();
  const country = filterValues(filters.country, isCountryCode);
  const region = filterValues(filters.region, isRegionCode);
  const city = filterValues(filters.city, isCityName);
  // Region and city are only meaningful inside exactly one country.
  if ((region.length || city.length) && country.length !== 1) invalidFilters();
  return {
    search: filters.search as string,
    version: filters.version as "any" | "4" | "6",
    asn: filterValues(filters.asn, (item) => normalizeAsn(item) === item),
    country,
    region,
    city,
  };
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
    return {
      mode: "all",
      filters: parseSelectionFilters(record(selection.filters)),
      excludedIps: ipList(selection.excludedIps),
    };
  }
  throw new IpEnrichmentSelectionError(
    "Choose selected addresses or all matching addresses.",
  );
}

/** The search table column each list filter matches exactly. */
const FILTER_COLUMNS = {
  asn: "asn",
  country: "country_iso_code",
  region: "subdivision_iso_code",
  city: "city_name",
} as const;

export function inputConfig(
  selection: WorkspaceIpSelection,
): Record<string, unknown> {
  const input: Record<string, unknown> = {
    source_name: "backoffice:ip-addresses",
    observed_at_column: "last_seen",
  };
  if (selection.mode === "ips") {
    Object.assign(input, {
      source_relation: "corpscout.commoncrawl_ip_addresses",
      filters: { ip: selection.ips },
    });
  } else {
    // "All matching" reads the list's own snapshot, so the draft gets exactly the
    // rows the filters show, with each filter an exact-match column of that table.
    // Dagster selects them in ClickHouse, independently of pagination.
    const filters: Record<string, string[]> = {};
    const chosen = withLocationScope(selection.filters);
    for (const key of IP_LIST_FILTER_KEYS)
      if (chosen[key].length) filters[FILTER_COLUMNS[key]] = [...chosen[key]].sort();
    if (chosen.version !== "any") filters.ip_version = [chosen.version];
    Object.assign(input, {
      source_relation: IP_SEARCH_RELATION,
      select_all: true,
      ip_search: chosen.search,
      filters,
      excluded_ips: selection.excludedIps,
    });
  }
  return input;
}

/** Append the selection to the open workspace draft. Dagster chooses/creates the task atomically. */
export async function addIpsToEnrichmentQueue(
  value: unknown,
  submissionId: string,
  requestedBy: string,
  options: DagsterOptions = {},
) {
  const selection = parseIpEnrichmentSelection(value);
  if (!QUEUE_UUID.test(submissionId))
    throw new IpEnrichmentSelectionError(
      "Invalid submission ID. Reload the page.",
    );
  const config = {
    ...inputConfig(selection),
    queue_scope: "workspace",
    submission_id: submissionId,
  };
  const fingerprint = createHash("sha256")
    .update(JSON.stringify(config))
    .digest("hex");
  const previous = submissions.get(submissionId);
  const pending = previous ? previous.then(submit, submit) : submit();
  submissions.set(submissionId, pending);
  try {
    return await pending;
  } finally {
    if (submissions.get(submissionId) === pending)
      submissions.delete(submissionId);
  }

  async function submit() {
    const [existing] = await listRuns(
      { job: JOB, limit: 1, tags: { "processing/submission_id": submissionId } },
      options,
    );
    if (
      existing &&
      existing.tags["ip_enrichment/selection_sha256"] !== fingerprint
    )
      throw new IpEnrichmentSelectionError(
        "This submission ID belongs to another selection.",
      );
    // Retry failed imports using the same durable receipt, not a new selection.
    if (existing && !["FAILURE", "CANCELED"].includes(existing.status)) {
      return {
        ok: true as const,
        runId: existing.runId,
        status: existing.status,
        runUrl: dagsterRunUrl(existing.runId, options.url),
      };
    }
    const run = await launchRun({
      job: JOB,
      assetSelection: [ASSET],
      runConfig: { ops: { [ASSET]: { config } } },
      tags: {
        "processing/submission_id": submissionId,
        "ip_enrichment/selection_sha256": fingerprint,
        "corpscout/requested_by": requestedBy,
        "backoffice/action": "add-ip-enrichment-input",
      },
    });
    return {
      ok: true as const,
      ...run,
      runUrl: dagsterRunUrl(run.runId, options.url),
    };
  }
}

export async function ipEnrichmentQueueSubmission(
  runId: string,
  options: DagsterOptions = {},
) {
  if (!QUEUE_UUID.test(runId))
    throw new IpEnrichmentSelectionError("Invalid import run ID.");
  const run = await runStatus(runId, options);
  if (
    run.jobName !== JOB ||
    run.tags["backoffice/action"] !== "add-ip-enrichment-input"
  )
    throw new IpEnrichmentSelectionError(
      "IP enrichment queue submission not found.",
    );
  const task = run.tags["processing/task_id"];
  return {
    ok: true as const,
    runId,
    status: run.status,
    finished: ["SUCCESS", "FAILURE", "CANCELED"].includes(run.status),
    taskId: task && QUEUE_UUID.test(task) ? task : null,
  };
}
