import { clampPage } from "~/lib/paging";

export interface DnsRecordValue {
  id: string;
  name: string;
  type: string;
  typeCode: number;
  classCode: number;
  value: string;
  priority: number;
  sources: string[];
  firstSeen: string;
  lastSeen: string;
  seenDates: string[];
}

export interface DnsObservationPeriod {
  firstDay: string;
  lastDay: string;
  days: string[];
  recordIds: string[];
  added: string[];
  notObserved: string[];
}

export function dnsGroupKey(name: string, type: number, recordClass: number) {
  return JSON.stringify([name, type, recordClass]);
}

/** Compare observed daily sets, not individual rows or interpolated date ranges. */
export function dnsObservationHistory(records: DnsRecordValue[]): DnsObservationPeriod[] {
  const dates = new Map<string, Set<string>>();
  for (const record of records) {
    // Before seen_dates was introduced, only the first/last sightings are known.
    const seen = new Set([...record.seenDates, record.firstSeen.slice(0, 10), record.lastSeen.slice(0, 10)]);
    for (const day of seen) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(day) || day === "1970-01-01") continue;
      const values = dates.get(day) ?? new Set<string>();
      values.add(record.id);
      dates.set(day, values);
    }
  }
  const periods: DnsObservationPeriod[] = [];
  for (const [day, ids] of [...dates].sort(([a], [b]) => a.localeCompare(b))) {
    const recordIds = [...ids].sort();
    const previous = periods.at(-1);
    if (previous && JSON.stringify(previous.recordIds) === JSON.stringify(recordIds)) {
      previous.lastDay = day;
      previous.days.push(day);
    } else {
      periods.push({
        firstDay: day, lastDay: day, days: [day], recordIds,
        added: previous ? recordIds.filter((id) => !previous.recordIds.includes(id)) : [],
        notObserved: previous ? previous.recordIds.filter((id) => !ids.has(id)) : [],
      });
    }
  }
  return periods;
}

export function parseDnsFilters(params: URLSearchParams) {
  const rawType = params.get("type") ?? "";
  return {
    name: (params.get("name") ?? "").trim().toLowerCase().slice(0, 253),
    type: /^\d{1,5}$/.test(rawType) && Number(rawType) <= 65535 ? rawType : "",
    page: clampPage(Number(params.get("page") ?? 1)),
  };
}

export type DnsFilters = ReturnType<typeof parseDnsFilters>;

export function domainDnsHref(domain: string, filters: Partial<DnsFilters> = {}) {
  const params = new URLSearchParams();
  if (filters.name) params.set("name", filters.name);
  if (filters.type) params.set("type", filters.type);
  if (filters.page && filters.page > 1) params.set("page", String(filters.page));
  return `/admin/domains/${encodeURIComponent(domain)}/dns${params.size ? `?${params}` : ""}`;
}
