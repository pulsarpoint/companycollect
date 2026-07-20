import type { WikidataCompanyRow } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="text-muted-foreground text-xs">{label}</div>
      <div className="text-sm">{children}</div>
    </div>
  );
}

/** Wikidata enrichment for LEI-matched companies: description, logo,
 * employees, stock listings, official websites, LinkedIn. Community-sourced
 * data — always attributed and linked back to the wikidata.org item. */
export function WikidataSection({ wikidata }: { wikidata: WikidataCompanyRow | null }) {
  if (wikidata === null) return null;
  const websites = wikidata.websites.split(" ").filter((u) => u !== "");
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex flex-wrap items-baseline gap-x-2 text-base">
          Wikidata{" "}
          <a
            href={wikidata.wikidata_url}
            target="_blank"
            rel="noreferrer"
            className="text-muted-foreground text-sm font-normal hover:underline"
          >
            {wikidata.wikidata_id} ↗
          </a>
          {wikidata.has_current_listing ? <Badge variant="outline">listed</Badge> : null}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex items-start gap-4">
          {wikidata.logo_url ? (
            // Wikimedia-hosted logo; plain <img>, no proxying.
            <img
              src={wikidata.logo_url}
              alt=""
              className="max-h-16 max-w-[8rem] shrink-0 object-contain dark:rounded dark:bg-white dark:p-1"
              loading="lazy"
            />
          ) : null}
          {wikidata.description ? (
            <p className="text-sm">{wikidata.description}</p>
          ) : null}
        </div>
        <div className="grid grid-cols-2 gap-x-6 gap-y-2 sm:grid-cols-3">
          {wikidata.official_name ? (
            <Field label="Official name">{wikidata.official_name}</Field>
          ) : null}
          {wikidata.industry_label ? (
            <Field label="Industry">{wikidata.industry_label}</Field>
          ) : null}
          {wikidata.employee_count !== null ? (
            <Field label="Employees">
              {Number(wikidata.employee_count).toLocaleString("en-US")}
              {wikidata.employee_count_as_of ? (
                <span className="text-muted-foreground text-xs">
                  {" "}
                  (as of {wikidata.employee_count_as_of})
                </span>
              ) : null}
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
          {websites.length > 0 ? (
            <Field label="Website">
              {websites.map((url) => (
                <a
                  key={url}
                  href={url}
                  target="_blank"
                  rel="noreferrer"
                  className="block truncate hover:underline"
                >
                  {url.replace(/^https?:\/\//, "")}
                </a>
              ))}
            </Field>
          ) : null}
          {wikidata.linkedin_id ? (
            <Field label="LinkedIn">
              <a
                href={`https://www.linkedin.com/company/${wikidata.linkedin_id}`}
                target="_blank"
                rel="noreferrer"
                className="hover:underline"
              >
                {wikidata.linkedin_id}
              </a>
            </Field>
          ) : null}
        </div>
      </CardContent>
    </Card>
  );
}
