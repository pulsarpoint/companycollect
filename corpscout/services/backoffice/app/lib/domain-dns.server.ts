import { chQuery } from "~/lib/clickhouse.server";
import { graphDomainError } from "~/lib/domain-graph";
import { dnsGroupKey, dnsObservationHistory, type DnsFilters, type DnsRecordValue, type DnsObservationPeriod } from "~/lib/domain-dns";

const PAGE_SIZE = 50;
const TABLE = "corpscout.commoncrawl_domain_dns_records FINAL";
const SETTINGS = "SETTINGS max_execution_time=20, max_memory_usage=536870912, do_not_merge_across_partitions_select_final=1";

interface GroupRow {
  name: string;
  type: string;
  type_code: number;
  class_code: number;
  total_groups: number;
}
interface ValueRow {
  id: string;
  name: string;
  type: string;
  type_code: number;
  class_code: number;
  value: string;
  priority: number;
  sources: string[];
  first_seen: string;
  last_seen: string;
  seen_dates: string[];
}

export interface DomainDns {
  domain: string;
  filters: DnsFilters;
  pageSize: number;
  totalValues: number;
  totalGroups: number;
  matchingGroups: number;
  types: { type: string; code: number; values: number; groups: number }[];
  groups: { key: string; name: string; type: string; classCode: number; values: DnsRecordValue[]; history: DnsObservationPeriod[] }[];
}

export async function getDomainDns(domain: string, filters: DnsFilters): Promise<DomainDns> {
  if (!domain || graphDomainError(domain)) throw new Response("Invalid domain", { status: 400 });
  const where = `root_domain = {domain:String}
    AND ({name:String} = '' OR positionCaseInsensitive(name, {name:String}) > 0)
    AND ({type:String} = '' OR record_type_code = toUInt16OrZero({type:String}))`;
  const params = { domain, ...filters, limit: PAGE_SIZE, offset: (filters.page - 1) * PAGE_SIZE };
  // Metadata is bounded by record type, while records are read only for the page's keys.
  const [types, groups] = await Promise.all([
    chQuery<{ type: string; code: number; values: number; groups: number }>(`SELECT
      any(toString(record_type)) AS type,record_type_code AS code,
      count() AS values,uniqExact((name,record_class_code)) AS groups
      FROM ${TABLE} WHERE root_domain = {domain:String}
      GROUP BY record_type_code ORDER BY code ${SETTINGS}`, { domain }),
    chQuery<GroupRow>(`SELECT name,any(toString(record_type)) AS type,
      record_type_code AS type_code,record_class_code AS class_code,
      count() OVER () AS total_groups
      FROM ${TABLE} WHERE ${where}
      GROUP BY name,record_type_code,record_class_code
      ORDER BY name != {domain:String},name,type_code,class_code
      LIMIT {limit:UInt32} OFFSET {offset:UInt64} ${SETTINGS}`, params),
  ]);
  // Out-of-range bookmarks go back to the first page, retaining filters.
  if (groups.length === 0 && filters.page > 1) return getDomainDns(domain, { ...filters, page: 1 });
  const rows = groups.length ? await chQuery<ValueRow>(`SELECT
    lower(hex(record_id)) AS id,name,toString(record_type) AS type,
    record_type_code AS type_code,record_class_code AS class_code,
    toString(value) AS value,toUInt16(priority) AS priority,sources,
    toString(first_seen) AS first_seen,toString(last_seen) AS last_seen,
    arrayMap(day -> toString(day),seen_dates) AS seen_dates
    FROM ${TABLE} WHERE root_domain = {domain:String}
      AND (name,record_type_code,record_class_code) IN {keys:Array(Tuple(String,UInt16,UInt16))}
    ORDER BY name,record_type_code,record_class_code,priority,value,id ${SETTINGS}`,
    { domain, keys: groups.map((group) => [group.name, group.type_code, group.class_code]) },
  ) : [];
  const byKey = new Map<string, DnsRecordValue[]>();
  for (const row of rows) {
    const key = dnsGroupKey(row.name, row.type_code, row.class_code);
    const values = byKey.get(key) ?? [];
    values.push({ id: row.id, name: row.name, type: row.type || `TYPE${row.type_code}`,
      typeCode: row.type_code, classCode: row.class_code, value: row.value,
      priority: row.priority, sources: row.sources, firstSeen: row.first_seen,
      lastSeen: row.last_seen, seenDates: row.seen_dates });
    byKey.set(key, values);
  }
  return {
    domain, filters, pageSize: PAGE_SIZE,
    totalValues: types.reduce((sum, type) => sum + Number(type.values), 0),
    totalGroups: types.reduce((sum, type) => sum + Number(type.groups), 0),
    matchingGroups: Number(groups[0]?.total_groups ?? 0),
    types: types.map((type) => ({ ...type, type: type.type || `TYPE${type.code}` })),
    groups: groups.map((group) => {
      const key = dnsGroupKey(group.name, group.type_code, group.class_code);
      const values = byKey.get(key) ?? [];
      return { key, name: group.name, type: group.type || `TYPE${group.type_code}`,
        classCode: group.class_code, values, history: dnsObservationHistory(values) };
    }),
  };
}
