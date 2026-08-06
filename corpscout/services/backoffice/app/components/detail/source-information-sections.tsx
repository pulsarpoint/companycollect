import type {
  CompanyBusinessItemObservation,
  CompanyDescriptionObservation,
  CompanySourceRecord,
  SourceContactObservation,
  ContactRow,
  DomainRow,
  WikidataCompanyRow,
} from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  EvidenceBadges,
  EvidencePanel,
  evidenceSourceLabel,
} from "~/components/detail/evidence";

function descriptionSource(observation: CompanyDescriptionObservation): string {
  const reference = observation.evidence[0];
  return reference ? evidenceSourceLabel(reference) : observation.descriptionKind;
}

export function DescriptionsSection({
  descriptions,
}: {
  descriptions: CompanyDescriptionObservation[];
}) {
  if (descriptions.length === 0) return null;
  const bySource = new Map<string, CompanyDescriptionObservation[]>();
  for (const observation of descriptions) {
    const source = descriptionSource(observation);
    bySource.set(source, [...(bySource.get(source) ?? []), observation]);
  }
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Descriptions</CardTitle>
        <CardDescription>
          Each source remains separate. The newest version is shown first; older source
          versions remain expandable.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-5">
        {[...bySource.entries()].map(([source, observations]) => {
          const [latest, ...history] = observations;
          return (
            <section key={source} className="flex flex-col gap-2">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="secondary">{source}</Badge>
                <Badge variant="outline">{latest.descriptionKind.replaceAll("_", " ")}</Badge>
              </div>
              <p className="text-sm leading-relaxed whitespace-pre-wrap">
                {latest.textEn || latest.textOriginal}
              </p>
              {latest.textEn && latest.textEn !== latest.textOriginal ? (
                <details>
                  <summary className="text-muted-foreground cursor-pointer text-xs">
                    Original ({latest.languageOriginal || "unknown language"})
                  </summary>
                  <p className="pt-2 text-sm leading-relaxed whitespace-pre-wrap">
                    {latest.textOriginal}
                  </p>
                </details>
              ) : null}
              <EvidencePanel evidence={latest.evidence} />
              {history.length > 0 ? (
                <details>
                  <summary className="text-muted-foreground cursor-pointer text-xs">
                    Older versions ({history.length})
                  </summary>
                  <div className="flex flex-col gap-4 pt-3">
                    {history.map((observation) => (
                      <div key={observation.observationUid} className="flex flex-col gap-2">
                        <p className="text-sm leading-relaxed whitespace-pre-wrap">
                          {observation.textEn || observation.textOriginal}
                        </p>
                        <EvidencePanel evidence={observation.evidence} />
                      </div>
                    ))}
                  </div>
                </details>
              ) : null}
            </section>
          );
        })}
      </CardContent>
    </Card>
  );
}

const BUSINESS_ITEM_LABELS: Record<string, string> = {
  product_or_service: "Products and services",
  customer_market: "Customer markets",
  operating_geography: "Operating geographies",
  business_segment: "Business segments",
};

export function ProductsMarketsSection({
  items,
}: {
  items: CompanyBusinessItemObservation[];
}) {
  if (items.length === 0) return null;
  const kinds = [...new Set(items.map((item) => item.itemKind))];
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Products and markets</CardTitle>
        <CardDescription>
          Evidence-backed business details extracted from annual-report narrative facts.
        </CardDescription>
      </CardHeader>
      <CardContent className="grid gap-5 md:grid-cols-2">
        {kinds.map((kind) => (
          <section key={kind} className="flex flex-col gap-2">
            <h3 className="text-sm font-medium">
              {BUSINESS_ITEM_LABELS[kind] ?? kind.replaceAll("_", " ")}
            </h3>
            <ul className="flex flex-col gap-2">
              {items
                .filter((item) => item.itemKind === kind)
                .map((item) => (
                  <li key={item.candidateUid} className="flex flex-col gap-1 text-sm">
                    <div className="flex flex-wrap items-center gap-2">
                      <span>{item.name}</span>
                      {item.geographyType ? (
                        <Badge variant="outline">{item.geographyType}</Badge>
                      ) : null}
                      <Badge variant="outline">fiscal {item.fiscalYear}</Badge>
                    </div>
                    <EvidencePanel evidence={item.evidence} />
                  </li>
                ))}
            </ul>
          </section>
        ))}
      </CardContent>
    </Card>
  );
}

export function ContactsDomainsSection({
  contacts,
  domains,
  sourceContacts,
  wikidata,
}: {
  contacts: ContactRow[];
  domains: DomainRow[];
  sourceContacts: SourceContactObservation[];
  wikidata: WikidataCompanyRow | null;
}) {
  const websites = (wikidata?.websites ?? "").split(" ").filter(Boolean);
  if (
    contacts.length === 0 &&
    domains.length === 0 &&
    sourceContacts.length === 0 &&
    websites.length === 0
  ) {
    return null;
  }
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Contacts and domains</CardTitle>
        <CardDescription>
          Deterministic values and source observations are displayed independently.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {sourceContacts.map((contact) => (
          <div key={contact.candidateId} className="flex flex-col gap-1">
            <div className="flex flex-wrap items-center gap-2 text-sm">
              <Badge variant="outline">{contact.candidateKind}</Badge>
              <span className="break-all">{contact.normalizedValue}</span>
              {contact.registrableDomain &&
              contact.registrableDomain !== contact.normalizedValue ? (
                <span className="text-muted-foreground text-xs">
                  eTLD+1 {contact.registrableDomain}
                </span>
              ) : null}
              <Badge variant="outline">fiscal {contact.fiscalYear}</Badge>
            </div>
            <EvidencePanel evidence={contact.evidence} />
          </div>
        ))}
        {contacts.map((contact, index) => (
          <div key={`${contact.contact_type}:${contact.contact_value}:${index}`}>
            <div className="flex flex-wrap items-center gap-2 text-sm">
              <Badge variant="outline">{contact.contact_type}</Badge>
              <span className="break-all">{contact.contact_value}</span>
            </div>
            <EvidencePanel evidence={contact.evidence ?? []} />
          </div>
        ))}
        {domains.map((domain) => (
          <div key={domain.domain}>
            <div className="flex flex-wrap items-center gap-2 text-sm">
              <Badge variant="outline">domain</Badge>
              <span>{domain.domain}</span>
              {domain.is_primary ? <Badge>primary</Badge> : null}
            </div>
            <EvidencePanel evidence={domain.evidence ?? []} />
          </div>
        ))}
        {websites.map((website) => (
          <div key={website} className="flex flex-col gap-1">
            <div className="flex flex-wrap items-center gap-2 text-sm">
              <Badge variant="outline">website</Badge>
              <a
                href={website}
                target="_blank"
                rel="noreferrer"
                className="break-all underline underline-offset-2"
              >
                {website}
              </a>
            </div>
            <EvidencePanel evidence={wikidata?.evidence ?? []} />
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

export function SourcesSection({ records }: { records: CompanySourceRecord[] }) {
  if (records.length === 0) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Sources</CardTitle>
        <CardDescription>
          {records.length} versioned evidence {records.length === 1 ? "record" : "records"} linked
          to this company.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        {records.map((record) => (
          <div key={record.sourceRecordUid} className="flex flex-col gap-1.5">
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="secondary">{record.recordKind.replaceAll("_", " ")}</Badge>
              <EvidenceBadges evidence={record.evidence} />
            </div>
            <EvidencePanel evidence={record.evidence} />
          </div>
        ))}
      </CardContent>
    </Card>
  );
}
