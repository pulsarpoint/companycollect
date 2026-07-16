import type { CompanyListRow, ContactRow, DomainRow } from "~/lib/queries.server";
import type { CountryConfig } from "~/lib/countries";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";

export const EMPTY = <span className="text-muted-foreground">—</span>;

function value(v: unknown) {
  const s = v == null ? "" : String(v);
  return s === "" ? EMPTY : s;
}

export function OverviewSection({
  country,
  company,
}: {
  country: CountryConfig;
  company: CompanyListRow;
}) {
  const fields = country.columns.filter((c) => c.key !== "name");
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Overview</CardTitle>
      </CardHeader>
      <CardContent>
        <dl className="grid grid-cols-1 gap-x-8 gap-y-3 sm:grid-cols-2">
          {fields.map((col) => (
            <div key={col.key} className="flex flex-col gap-0.5">
              <dt className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
                {col.label}
              </dt>
              <dd className={col.kind === "id" ? "font-mono text-sm" : "text-sm"}>
                {value(company[col.key])}
              </dd>
            </div>
          ))}
          <div className="flex flex-col gap-0.5">
            <dt className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
              Industry
            </dt>
            <dd className="text-sm">
              {company.industry_code || company.industry_label ? (
                <span className="flex items-baseline gap-1.5">
                  {company.industry_code ? (
                    <span className="text-muted-foreground font-mono text-xs">
                      {company.industry_code}
                    </span>
                  ) : null}
                  {company.industry_label ? <span>{company.industry_label}</span> : null}
                </span>
              ) : (
                EMPTY
              )}
            </dd>
          </div>
        </dl>
      </CardContent>
    </Card>
  );
}

export function ContactsSection({ contacts }: { contacts: ContactRow[] }) {
  if (contacts.length === 0) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Contacts</CardTitle>
      </CardHeader>
      <CardContent>
        <ul className="space-y-1.5">
          {contacts.map((c, i) => (
            <li key={`${c.contact_type}-${c.contact_value}-${i}`} className="flex items-baseline gap-2 text-sm">
              <Badge variant="outline" className="w-20 justify-center">
                {c.contact_type}
              </Badge>
              <span className="break-all">{c.contact_value}</span>
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
}

export function DomainsSection({ domains }: { domains: DomainRow[] }) {
  if (domains.length === 0) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Domains</CardTitle>
      </CardHeader>
      <CardContent>
        <ul className="space-y-1.5">
          {domains.map((d) => (
            <li key={d.domain} className="flex flex-wrap items-baseline gap-2 text-sm">
              <span className="font-medium">{d.domain}</span>
              {d.is_primary ? <Badge>primary</Badge> : null}
              <span className="text-muted-foreground text-xs">
                {d.domain_source}
                {d.confidence != null ? ` · ${Math.round(d.confidence * 100)}%` : ""}
              </span>
              {d.website_url ? (
                <a
                  href={d.website_url}
                  target="_blank"
                  rel="noreferrer"
                  className="text-muted-foreground truncate text-xs underline"
                >
                  {d.website_url}
                </a>
              ) : null}
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
}
