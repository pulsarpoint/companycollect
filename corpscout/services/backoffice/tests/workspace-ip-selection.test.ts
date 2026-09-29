import { expect, it } from "vitest";
import { EMPTY_WORKSPACE_IP_FILTERS } from "~/lib/workspace-ip-addresses";
import {
  isIpSelected,
  selectIpAddresses,
  type WorkspaceIpSelection,
} from "~/lib/workspace-ip-selection";

it("retains individual selections across pages and removes only the unchecked page", () => {
  let selection: WorkspaceIpSelection = { mode: "ips", ips: [] };
  selection = selectIpAddresses(selection, ["1.1.1.1", "8.8.8.8"], true);
  selection = selectIpAddresses(selection, ["9.9.9.9"], true);
  selection = selectIpAddresses(selection, ["1.1.1.1", "8.8.8.8"], false);
  expect(selection).toEqual({ mode: "ips", ips: ["9.9.9.9"] });
});

it("selects unseen matches and tracks exclusions without expanding the inventory", () => {
  let selection: WorkspaceIpSelection = {
    mode: "all",
    filters: { ...EMPTY_WORKSPACE_IP_FILTERS, version: "4" },
    excludedIps: [],
  };
  expect(isIpSelected(selection, "8.8.8.8")).toBe(true);
  selection = selectIpAddresses(selection, ["8.8.8.8", "9.9.9.9"], false);
  expect(isIpSelected(selection, "8.8.8.8")).toBe(false);
  expect(isIpSelected(selection, "1.1.1.1")).toBe(true);
  selection = selectIpAddresses(selection, ["9.9.9.9"], true);
  expect(selection).toMatchObject({ mode: "all", excludedIps: ["8.8.8.8"] });
});
