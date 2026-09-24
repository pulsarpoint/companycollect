import { Fragment, useId, useState } from "react";
import { Form, Link, useNavigation } from "react-router";
import { HistoryIcon } from "lucide-react";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Field, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "~/components/ui/empty";
import { domainDnsHref, type DnsRecordValue } from "~/lib/domain-dns";
import type { DomainDns } from "~/lib/domain-dns.server";

const dateFormat = new Intl.DateTimeFormat("en-GB", {
  dateStyle: "medium", timeStyle: "short", timeZone: "UTC",
});
function timestamp(value: string) {
  const date = new Date(`${value.replace(" ", "T")}Z`);
  return Number.isNaN(date.getTime()) ? value : dateFormat.format(date);
}

function RecordValue({ record }: { record: DnsRecordValue }) {
  return <code className="break-all whitespace-pre-wrap font-mono text-xs">{record.value || "(empty value)"}</code>;
}

export function DnsRecordHistory({ group }: { group: DomainDns["groups"][number] }) {
  const records = new Map(group.values.map((record) => [record.id, record]));
  return (
    <div className="flex flex-col gap-4 p-3">
      <div>
        <h3 className="font-medium">{group.name} · {group.type} observation history</h3>
        <p className="text-muted-foreground text-xs">Oldest to newest. Identical sets are combined across observed days; gaps are not filled.</p>
      </div>
      <ol className="flex flex-col gap-4 border-l pl-4">
        {group.history.map((period, index) => (
          <li key={period.firstDay} className="flex flex-col gap-2">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-sm font-medium tabular-nums">
                {period.firstDay}{period.lastDay !== period.firstDay ? ` → ${period.lastDay}` : ""}
              </span>
              <Badge variant="outline">{period.days.length} observed {period.days.length === 1 ? "day" : "days"}</Badge>
              {index === 0 ? <Badge variant="secondary">First recorded set</Badge> : <Badge variant="secondary">Observed set changed</Badge>}
            </div>
            <p className="text-muted-foreground break-words text-xs">Seen on: {period.days.join(", ")}</p>
            <ul className="flex flex-col gap-2">
              {period.recordIds.map((id) => (
                <li key={id} className="flex flex-wrap items-baseline gap-2">
                  <RecordValue record={records.get(id)!} />
                  {period.added.includes(id) ? <Badge variant="outline">New in this observation</Badge> : null}
                </li>
              ))}
            </ul>
            {period.notObserved.length ? (
              <div className="text-muted-foreground flex flex-col gap-1 text-xs">
                <p>Previously seen, not observed on these days:</p>
                {period.notObserved.map((id) => <RecordValue key={id} record={records.get(id)!} />)}
              </div>
            ) : null}
          </li>
        ))}
      </ol>
    </div>
  );
}

function RecordGroup({ group }: { group: DomainDns["groups"][number] }) {
  const [expanded, setExpanded] = useState(false);
  const historyId = useId();
  const changes = Math.max(0, group.history.length - 1);
  return (
    <Fragment>
      {group.values.map((record, index) => (
        <TableRow key={record.id}>
          {index === 0 ? <>
            <TableCell rowSpan={group.values.length} className="max-w-56 align-top whitespace-normal">
              <span className="break-all font-mono text-xs font-medium">{group.name}</span>
              <p className="text-muted-foreground mt-1 text-xs">{group.values.length} recorded {group.values.length === 1 ? "value" : "values"}</p>
            </TableCell>
            <TableCell rowSpan={group.values.length} className="align-top">
              <Badge variant="outline">{group.type}</Badge>
              <p className="text-muted-foreground mt-1 text-xs">{group.classCode === 1 ? "IN" : `CLASS${group.classCode}`}</p>
            </TableCell>
          </> : null}
          <TableCell className="min-w-52 max-w-xl whitespace-normal"><RecordValue record={record} /></TableCell>
          <TableCell className="text-muted-foreground text-xs tabular-nums">{timestamp(record.firstSeen)}</TableCell>
          <TableCell className="text-muted-foreground text-xs tabular-nums">{timestamp(record.lastSeen)}</TableCell>
          <TableCell className="text-xs">{record.sources.join(", ") || "Unknown"}</TableCell>
          {index === 0 ? (
            <TableCell rowSpan={group.values.length} className="align-top">
              <div className="flex flex-col items-start gap-2">
                <Badge variant={changes ? "secondary" : "outline"}>
                  {changes ? `${changes} observed set ${changes === 1 ? "change" : "changes"}` : "No set change observed"}
                </Badge>
                <Button variant="ghost" size="sm" aria-expanded={expanded} aria-controls={historyId}
                  aria-label={`${expanded ? "Hide" : "View"} history for ${group.name} ${group.type} class ${group.classCode}`}
                  onClick={() => setExpanded(!expanded)}>
                  <HistoryIcon data-icon="inline-start" />{expanded ? "Hide history" : "View history"}
                </Button>
              </div>
            </TableCell>
          ) : null}
        </TableRow>
      ))}
      {expanded ? <TableRow><TableCell colSpan={7} className="bg-muted/20 whitespace-normal">
        <div id={historyId}><DnsRecordHistory group={group} /></div>
      </TableCell></TableRow> : null}
    </Fragment>
  );
}

export function DomainDnsRecords({ data }: { data: DomainDns }) {
  const busy = useNavigation().state !== "idle";
  const { domain, filters } = data;
  const pages = Math.max(1, Math.ceil(data.matchingGroups / data.pageSize));
  return (
    <section className="flex min-w-0 flex-col gap-4" aria-busy={busy}>
      <header className="flex flex-col gap-2">
        <h2 className="text-xl font-semibold">DNS records</h2>
        <p className="text-muted-foreground text-sm">
          {data.totalValues.toLocaleString("en-US")} distinct records · {data.totalGroups.toLocaleString("en-US")} hostname/type groups · {data.types.length} record types
        </p>
        <p className="text-muted-foreground max-w-4xl text-sm">
          All recorded DNS values for this domain and its hostnames, including historical values. Expand a record group to compare the values observed over time.
        </p>
      </header>
      <Form method="get" action={domainDnsHref(domain)} key={`${filters.name}:${filters.type}`} className="flex flex-wrap items-end gap-3">
        <Field className="w-full sm:max-w-xs">
          <FieldLabel htmlFor="dns-name">Hostname contains</FieldLabel>
          <Input id="dns-name" name="name" placeholder={`www.${domain}`} defaultValue={filters.name} />
        </Field>
        <Field className="w-full sm:max-w-44">
          <FieldLabel htmlFor="dns-type">Record type</FieldLabel>
          <NativeSelect id="dns-type" name="type" defaultValue={filters.type}>
            <NativeSelectOption value="">All types</NativeSelectOption>
            {data.types.map((type) => <NativeSelectOption key={type.code} value={String(type.code)}>{type.type} ({type.values})</NativeSelectOption>)}
          </NativeSelect>
        </Field>
        <Button type="submit" disabled={busy}>{busy ? "Loading…" : "Apply filters"}</Button>
        <Button variant="ghost" nativeButton={false} render={<Link to={domainDnsHref(domain)} />}>Reset</Button>
      </Form>
      <p className="text-muted-foreground max-w-5xl text-xs">
        History is grouped by observation day (UTC). Older records may only have first/last sightings. A changed observed set does not prove removal: scans may be partial, and changes within one day cannot be ordered.
      </p>
      {data.groups.length ? (
        <div className="overflow-hidden rounded-lg border">
          <Table>
            <TableHeader><TableRow>
              <TableHead>Hostname</TableHead><TableHead>Type / class</TableHead><TableHead>Value</TableHead>
              <TableHead>First observed (UTC)</TableHead><TableHead>Last observed (UTC)</TableHead><TableHead>Source</TableHead><TableHead>History</TableHead>
            </TableRow></TableHeader>
            <TableBody>{data.groups.map((group) => <RecordGroup key={group.key} group={group} />)}</TableBody>
          </Table>
        </div>
      ) : (
        <Empty><EmptyHeader>
          <EmptyTitle>{data.totalValues ? "No DNS records match these filters" : "No DNS records recorded yet"}</EmptyTitle>
          <EmptyDescription>{data.totalValues ? "Try another hostname or record type." : "DNS scanner and zone-transfer observations will appear here when available."}</EmptyDescription>
        </EmptyHeader></Empty>
      )}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="text-muted-foreground text-sm">{data.matchingGroups.toLocaleString("en-US")} matching groups · Page {filters.page} of {pages}</p>
        <div className="flex gap-2">
          {filters.page > 1 ? <Button variant="outline" nativeButton={false} render={<Link to={domainDnsHref(domain, { ...filters, page: filters.page - 1 })} />}>Previous</Button> : null}
          {filters.page < pages ? <Button variant="outline" nativeButton={false} render={<Link to={domainDnsHref(domain, { ...filters, page: filters.page + 1 })} />}>Next</Button> : null}
        </div>
      </div>
    </section>
  );
}
