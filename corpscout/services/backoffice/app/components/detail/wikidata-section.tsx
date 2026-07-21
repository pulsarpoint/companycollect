import type { WikidataCompanyRow, WikidataPersonRow } from "~/lib/queries.server";
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
function PersonLine({ person }: { person: WikidataPersonRow }) {
  const dates =
    person.start_date || person.end_date
      ? `${person.start_date || "?"} – ${person.is_current ? "present" : person.end_date || "?"}`
      : "";
  return (
    <li className="flex items-center gap-3 py-1.5">
      {person.image_url ? (
        <img
          src={person.image_url}
          alt=""
          className="size-8 shrink-0 rounded-full object-cover"
          loading="lazy"
        />
      ) : (
        <div className="bg-muted size-8 shrink-0 rounded-full" />
      )}
      <div className="min-w-0">
        <div className="flex flex-wrap items-baseline gap-x-2">
          {person.wikidata_url ? (
            <a
              href={person.wikidata_url}
              target="_blank"
              rel="noreferrer"
              className="font-medium hover:underline"
            >
              {person.name}
            </a>
          ) : (
            <span className="font-medium">{person.name}</span>
          )}
          <Badge variant={person.is_current ? "default" : "outline"}>
            {person.role_label}
          </Badge>
          {dates ? (
            <span className="text-muted-foreground text-xs tabular-nums">{dates}</span>
          ) : null}
        </div>
        {person.description ? (
          <div className="text-muted-foreground truncate text-xs">
            {person.description}
            {person.birth_year ? ` (b. ${person.birth_year})` : ""}
          </div>
        ) : null}
      </div>
    </li>
  );
}

export function WikidataSection({
  wikidata,
  people = [],
}: {
  wikidata: WikidataCompanyRow | null;
  people?: WikidataPersonRow[];
}) {
  if (wikidata === null && people.length === 0) return null;
  const websites = (wikidata?.websites ?? "").split(" ").filter((u) => u !== "");
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex flex-wrap items-baseline gap-x-2 text-base">
          Wikidata{" "}
          {wikidata ? (
            <a
              href={wikidata.wikidata_url}
              target="_blank"
              rel="noreferrer"
              className="text-muted-foreground text-sm font-normal hover:underline"
            >
              {wikidata.wikidata_id} ↗
            </a>
          ) : null}
          {wikidata?.has_current_listing ? <Badge variant="outline">listed</Badge> : null}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {wikidata ? (
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
        ) : null}
        {wikidata ? (
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
        ) : null}
        {people.length > 0 ? (
          <div>
            <div className="text-muted-foreground mb-1 text-xs font-medium uppercase">
              People ({people.length})
            </div>
            <ul className="divide-y">
              {people.map((person) => (
                <PersonLine
                  key={`${person.person_wikidata_id}-${person.role_label}`}
                  person={person}
                />
              ))}
            </ul>
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}
