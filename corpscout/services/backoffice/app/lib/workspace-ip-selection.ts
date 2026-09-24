import type { WorkspaceIpFilters } from "~/lib/workspace-ip-addresses";

export type WorkspaceIpSelection =
  | { mode: "ips"; ips: string[] }
  | { mode: "all"; filters: WorkspaceIpFilters; excludedIps: string[] };

export function isIpSelected(selection: WorkspaceIpSelection, ip: string) {
  return selection.mode === "ips"
    ? selection.ips.includes(ip)
    : !selection.excludedIps.includes(ip);
}

export function selectIpAddresses(
  selection: WorkspaceIpSelection,
  ips: string[],
  checked: boolean,
): WorkspaceIpSelection {
  const values = new Set(
    selection.mode === "ips" ? selection.ips : selection.excludedIps,
  );
  for (const ip of ips) {
    if (checked === (selection.mode === "ips")) values.add(ip);
    else values.delete(ip);
  }
  return selection.mode === "ips"
    ? { mode: "ips", ips: [...values] }
    : { ...selection, excludedIps: [...values] };
}
