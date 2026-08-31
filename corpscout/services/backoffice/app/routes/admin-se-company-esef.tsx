import { Link } from "react-router";
import type { Route } from "./+types/admin-se-company-esef";
import {
  loadSeCompanyEsef,
  type SeCompanyEsefDetail,
} from "~/lib/se-company-esef.server";
import { parseJsonList } from "~/lib/se-company-info-payload";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";

export async function loader({ params }: Route.LoaderArgs) {
  const detail = await loadSeCompanyEsef(params.companyId);
  if (!detail) throw new Response("No ESEF data for company", { status: 404 });
  return detail;
}

const JSON_LIST_SECTIONS: ReadonlyArray<
  [keyof SeCompanyEsefDetail["information"][number] & string, string]
> = [
  ["productsAndServicesJson", "Products & services"],
  ["businessSegmentsJson", "Business segments"],
  ["customerMarketsJson", "Customer markets"],
  ["operatingGeographiesJson", "Operating geographies"],
  ["materialGroupRelationshipsJson", "Group relationships"],
];

export function SeCompanyEsefView({
  companyId,
  detail,
}: {
  companyId: string;
  detail: SeCompanyEsefDetail;
}) {
  return (
    <div className="flex flex-col gap-5">
      <Card>
        <CardHeader>
          <CardTitle>Filings</CardTitle>
          <CardDescription>
            Every ESEF annual report we know for this company. A filing without
            parsed facts is cataloged but still waiting for the parse backfill.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          {detail.filings.map((filing) => (
            <div
              key={filing.fxoId}
              className="flex flex-wrap items-center gap-2 border-b pb-2 last:border-b-0"
            >
              <Badge variant="outline">{filing.fiscalYear}</Badge>
              <span className="font-mono text-xs">{filing.fxoId}</span>
              {filing.factCount > 0 ? (
                <>
                  <span>{filing.factCount} facts</span>
                  <Link
                    className="underline"
                    to={`/company/se/${companyId}/financials/esef/${filing.fxoId}`}
                  >
                    Open facts
                  </Link>
                  <Link
                    className="underline"
                    to={`/company/se/${companyId}/financials/esef/${filing.fxoId}/notes`}
                  >
                    Notes ({filing.noteCount})
                  </Link>
                </>
              ) : (
                <Badge variant="secondary">Not parsed yet</Badge>
              )}
              {filing.viewerUrl ? (
                <a
                  className="underline"
                  href={filing.viewerUrl}
                  target="_blank"
                  rel="noreferrer"
                >
                  Viewer ↗
                </a>
              ) : null}
            </div>
          ))}
        </CardContent>
      </Card>

      {detail.information.map((info, idx) => (
        <Card key={`${info.fiscalYear}-${idx}`}>
          <CardHeader>
            <CardTitle>
              Company information · {info.fiscalYear}{" "}
              <Badge variant="outline">{info.extractionStatus}</Badge>
            </CardTitle>
            <CardDescription>
              LLM-extracted from the annual report narrative (
              {info.descriptionLanguage}, confidence{" "}
              {info.descriptionConfidence}).
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            <p className="max-w-[90ch]">{info.companyDescription}</p>
            <div className="grid gap-4 sm:grid-cols-2">
              {JSON_LIST_SECTIONS.map(([key, label]) => {
                const raw = String(info[key] ?? "");
                const items = parseJsonList(raw);
                // Unparseable or not an array: show the raw text rather than dropping it.
                if (items === null) {
                  if (raw.trim() === "") return null;
                  return (
                    <section key={key}>
                      <h3 className="font-medium">{label}</h3>
                      <p className="break-all text-muted-foreground">{raw}</p>
                    </section>
                  );
                }
                if (items.length === 0) return null;
                return (
                  <section key={key}>
                    <h3 className="font-medium">{label}</h3>
                    <ul className="list-disc pl-4">
                      {items.map((item) => (
                        <li key={item.text}>
                          {item.text}
                          {item.detail ? (
                            <span className="text-muted-foreground">
                              {" "}
                              · {item.detail}
                            </span>
                          ) : null}
                        </li>
                      ))}
                    </ul>
                  </section>
                );
              })}
            </div>
          </CardContent>
        </Card>
      ))}

      {detail.people.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>People</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="flex flex-col gap-1">
              {detail.people.map((person) => (
                <li
                  key={`${person.fiscalYear}-${person.name}-${person.role}`}
                  className="flex flex-wrap items-center gap-2"
                >
                  <span className="font-medium">{person.name}</span>
                  <span>{person.role}</span>
                  <Badge variant="outline">{person.roleCategory}</Badge>
                  <Badge variant="outline">fiscal {person.fiscalYear}</Badge>
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>
      ) : null}

      {detail.businessItems.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>Business items</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4 sm:grid-cols-2">
            {["product_or_service", "business_segment", "customer_market", "operating_geography"]
              .filter((kind) =>
                detail.businessItems.some((item) => item.itemKind === kind),
              )
              .map((kind) => (
                <section key={kind}>
                  <h3 className="font-medium">
                    {
                      {
                        product_or_service: "Products and services",
                        business_segment: "Business segments",
                        customer_market: "Customer markets",
                        operating_geography: "Operating geographies",
                      }[kind]
                    }
                  </h3>
                  <ul className="list-disc pl-4">
                    {detail.businessItems
                      .filter((item) => item.itemKind === kind)
                      .map((item) => (
                        <li key={`${item.fiscalYear}-${item.name}`}>
                          {item.name}{" "}
                          <Badge variant="outline">
                            fiscal {item.fiscalYear}
                          </Badge>
                        </li>
                      ))}
                  </ul>
                </section>
              ))}
          </CardContent>
        </Card>
      ) : null}

      {detail.contacts.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>Contact candidates</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="flex flex-col gap-1">
              {detail.contacts.map((contact) => (
                <li
                  key={`${contact.fiscalYear}-${contact.candidateKind}-${contact.normalizedValue}`}
                  className="flex flex-wrap items-center gap-2"
                >
                  <Badge variant="outline">{contact.candidateKind}</Badge>
                  <span>{contact.normalizedValue}</span>
                  <Badge variant="outline">fiscal {contact.fiscalYear}</Badge>
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>
      ) : null}

      {detail.relationships.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>Group relationships</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="flex flex-col gap-1">
              {detail.relationships.map((rel) => (
                <li
                  key={`${rel.fiscalYear}-${rel.relatedCompanyName}`}
                  className="flex flex-wrap items-center gap-2"
                >
                  <span className="font-medium">{rel.relatedCompanyName}</span>
                  <Badge variant="outline">{rel.relationshipType}</Badge>
                  {rel.ownershipPercentage ? (
                    <span>{rel.ownershipPercentage}%</span>
                  ) : null}
                  {rel.jurisdiction ? <span>{rel.jurisdiction}</span> : null}
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}

export default function AdminSwedenCompanyEsef({
  loaderData,
  params,
}: Route.ComponentProps) {
  return (
    <SeCompanyEsefView companyId={params.companyId} detail={loaderData} />
  );
}
