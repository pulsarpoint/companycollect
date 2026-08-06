import type { CompanyListRow, DomainRow } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { Separator } from "~/components/ui/separator";
import { FieldGrid, formatFieldValue, splitFields } from "~/components/detail/fields";
import { keyFacts, keyFactKeys, resolveRecordFields, type Lang } from "~/components/detail/language";

function KeyFactsStrip({ facts, lang }: { facts: ReturnType<typeof keyFacts>; lang: Lang }) {
  if (facts.length === 0) return null;
  return (
    <dl className="flex flex-wrap gap-x-6 gap-y-3">
      {facts.map((fact) => (
        <div key={fact.label} className="flex flex-col gap-0.5">
          <dt className="text-muted-foreground text-[0.7rem] font-medium uppercase tracking-wide">
            {fact.label}
            {fact.fromOtherLang ? (
              <span className="text-muted-foreground/70 ml-1.5 font-normal normal-case">
                ({lang === "en" ? "original" : "english"})
              </span>
            ) : null}
          </dt>
          <dd className="text-sm font-medium tabular-nums">
            {fact.href?.startsWith("http") ? (
              <a
                href={fact.href}
                target="_blank"
                rel="noreferrer"
                className="underline underline-offset-2"
              >
                {fact.value}
              </a>
            ) : (
              fact.value
            )}
          </dd>
        </div>
      ))}
    </dl>
  );
}

function ProseSections({
  longTexts,
  lang,
}: {
  longTexts: ReturnType<typeof resolveRecordFields>["longTexts"];
  lang: Lang;
}) {
  const sections = longTexts
    .map((field) => ({ field, text: formatFieldValue(field.key, field.value) }))
    .filter((s): s is { field: (typeof longTexts)[number]; text: string } => s.text !== null);
  if (sections.length === 0) return null;
  return (
    <div className="space-y-4">
      {sections.map(({ field, text }) => (
        <div key={field.key} className="space-y-1">
          <h4 className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
            {field.label}
            {field.fromOtherLang ? (
              <span className="text-muted-foreground/70 ml-1.5 font-normal normal-case">
                ({lang === "en" ? "original" : "english"})
              </span>
            ) : null}
          </h4>
          <p className="text-sm leading-relaxed whitespace-pre-wrap">{text}</p>
        </div>
      ))}
    </div>
  );
}

export function CompanyRecordSection({
  company,
  record,
  lang,
  hiddenFieldKeys = new Set<string>(),
}: {
  company: CompanyListRow;
  record: Record<string, unknown>;
  lang: Lang;
  hiddenFieldKeys?: Set<string>;
}) {
  const { fields, longTexts } = resolveRecordFields(record, lang);
  const { lineage } = splitFields(record);
  const facts = keyFacts(record, lang);
  const usedKeys = keyFactKeys(record, lang);
  const markerSuffix = lang === "en" ? "(original)" : "(english)";
  const gridFieldEntries = fields.filter(
    (field) => !usedKeys.has(field.key) && !hiddenFieldKeys.has(field.key),
  );
  const gridFields: [string, unknown][] = [
    ...gridFieldEntries.map((f): [string, unknown] => [f.key, f.value]),
    ["industry", [company.industry_code, company.industry_label].filter(Boolean).join(" ") || null],
  ];
  const gridMarkers = new Map<string, string>(
    gridFieldEntries.filter((f) => f.fromOtherLang).map((f): [string, string] => [f.key, markerSuffix]),
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Company record</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <KeyFactsStrip facts={facts} lang={lang} />
        {facts.length > 0 ? <Separator /> : null}
        <ProseSections
          longTexts={longTexts.filter(
            (field) => !usedKeys.has(field.key) && !hiddenFieldKeys.has(field.key),
          )}
          lang={lang}
        />
        <FieldGrid fields={gridFields} markers={gridMarkers} />
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
