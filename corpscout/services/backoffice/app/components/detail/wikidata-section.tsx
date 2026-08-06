import type { WikidataCompanyRow } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { EvidencePanel } from "~/components/detail/evidence";

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="text-muted-foreground text-xs">{label}</div>
      <div className="text-sm">{children}</div>
    </div>
  );
}

/** Information-first company facts observed in the current Wikidata item.
 * Descriptions, people, industries, and websites render in their dedicated
 * multi-source sections instead of being repeated here. */
export function WikidataSection({
  wikidata,
}: {
  wikidata: WikidataCompanyRow | null;
}) {
  if (wikidata === null) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex flex-wrap items-center gap-2 text-base">
          Company facts
          <Badge variant="secondary">Wikidata</Badge>
          {wikidata.has_current_listing ? <Badge variant="outline">listed</Badge> : null}
        </CardTitle>
        <CardDescription>
          Structured observations from the company&apos;s Wikidata item.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <div className="flex items-start gap-4">
          {wikidata.logo_url ? (
            <img
              src={wikidata.logo_url}
              alt=""
              className="max-h-16 max-w-[8rem] shrink-0 object-contain"
              loading="lazy"
            />
          ) : null}
          <div className="grid flex-1 grid-cols-2 gap-x-6 gap-y-2 sm:grid-cols-3">
            {wikidata.official_name ? (
              <Field label="Official name">{wikidata.official_name}</Field>
            ) : null}
            {wikidata.employee_count !== null ? (
              <Field label="Employees">
                {Number(wikidata.employee_count).toLocaleString("en-US")}
                {wikidata.employee_count_as_of
                  ? ` (as of ${wikidata.employee_count_as_of})`
                  : ""}
              </Field>
            ) : null}
            {wikidata.inception_date ? (
              <Field label="Founded">{wikidata.inception_date}</Field>
            ) : null}
            {wikidata.headquarters ? (
              <Field label="Headquarters">
                {wikidata.headquarters}
                {wikidata.headquarters_country ? `, ${wikidata.headquarters_country}` : ""}
              </Field>
            ) : null}
            {wikidata.listings ? (
              <Field label="Stock listings">{wikidata.listings}</Field>
            ) : null}
            <Field label="Source">
              <a
                href={wikidata.wikidata_url}
                target="_blank"
                rel="noreferrer"
                className="hover:underline"
              >
                {wikidata.wikidata_id} ↗
              </a>
            </Field>
          </div>
        </div>
        <EvidencePanel evidence={wikidata.evidence ?? []} />
      </CardContent>
    </Card>
  );
}
