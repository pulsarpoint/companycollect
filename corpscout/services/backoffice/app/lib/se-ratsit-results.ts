import type { SeRatsitRequestSelection } from "~/lib/se-ratsit-results.server";

const COMPANY_ID = /^\d{10}(?:\d{2})?$/;
const UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function parseSeRatsitRequestSelection(
  url: URL,
): SeRatsitRequestSelection | null {
  const companyId = url.searchParams.get("companyId")?.trim() ?? "";
  const batchId = url.searchParams.get("batchId")?.trim() ?? "";
  return COMPANY_ID.test(companyId) && UUID.test(batchId)
    ? { companyId, batchId }
    : null;
}

export function seRatsitRequestPath(
  selection: SeRatsitRequestSelection,
  currentSearch = "",
): string {
  const search = new URLSearchParams(currentSearch);
  search.set("companyId", selection.companyId);
  search.set("batchId", selection.batchId);
  return `/admin/se/companies/ratsit?${search.toString()}`;
}

export function seRatsitRequestListPath(currentSearch = ""): string {
  const search = new URLSearchParams(currentSearch);
  search.delete("companyId");
  search.delete("batchId");
  const suffix = search.toString();
  return suffix === "" ? "/admin/se/companies/ratsit" : `/admin/se/companies/ratsit?${suffix}`;
}
