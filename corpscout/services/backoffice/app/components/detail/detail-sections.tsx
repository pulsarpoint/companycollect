import type { CompanyListRow, ContactRow, DomainRow } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { FieldGrid, splitFields } from "~/components/detail/fields";

export function CompanyRecordSection({
  company,
  record,
}: {
  company: CompanyListRow;
  record: Record<string, unknown>;
}) {
  const { visible, lineage } = splitFields(record);
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Company record</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <FieldGrid
          fields={[
            ...visible,
            ["industry", [company.industry_code, company.industry_label].filter(Boolean).join(" ") || null],
          ]}
        />
        {lineage.length > 0 ? (
          <details>
            <summary className="text-muted-foreground cursor-pointer text-xs font-medium uppercase tracking-wide">
              Source &amp; lineage
            </summary>
            <div className="pt-3">
              <FieldGrid fields={lineage} />
            </div>
          </details>
        ) : null}
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
