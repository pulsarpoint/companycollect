import { Link } from "react-router";
import { ArrowLeft } from "lucide-react";
import type { Route } from "./+types/admin-se-company-esef-llm";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { parseJsonList } from "~/lib/se-company-info-payload";
import {
  loadSeCompanyEsefLlm,
  type SeCompanyEsefLlmDetail,
} from "~/lib/se-company-esef-llm.server";

// Only `loader`, `meta` and the component live here -- see
// admin-se-company-layout.tsx for why.

// Full transparency into the per-document LLM extraction: the exact request
// that was sent (stored on S3 by the pipeline), the raw response text
// (stored in ClickHouse), and the evidence disclosures the request draws on.
// When no extraction has run yet, the evidence list doubles as "what would
// be sent".
export async function loader({ params }: Route.LoaderArgs) {
  return await loadSeCompanyEsefLlm(params.documentId);
}

export function meta({ params }: Route.MetaArgs) {
  return [
    {
      title: `${params.documentId} · LLM extraction – CompanyCollect Backoffice`,
    },
  ];
}

const RESULT_SECTIONS: ReadonlyArray<
  [keyof NonNullable<SeCompanyEsefLlmDetail["extraction"]> & string, string]
> = [
  ["peopleJson", "People"],
  ["productsAndServicesJson", "Products & services"],
  ["customerMarketsJson", "Customer markets"],
  ["operatingGeographiesJson", "Operating geographies"],
  ["businessSegmentsJson", "Business segments"],
  ["materialGroupRelationshipsJson", "Group relationships"],
];

function CollapsibleText({
  summary,
  text,
}: {
  summary: string;
  text: string;
}) {
  return (
    <details className="min-w-0 rounded-lg border">
      <summary className="cursor-pointer px-3 py-2 text-sm font-medium">
        {summary}
        <span className="text-muted-foreground ml-2 text-xs">
          {text.length.toLocaleString("en-US")} chars
        </span>
      </summary>
      <pre className="max-h-[32rem] overflow-auto whitespace-pre-wrap border-t px-3 py-2 text-xs">
        {text}
      </pre>
    </details>
  );
}

export default function AdminSwedenCompanyEsefLlm({
  loaderData,
  params,
}: Route.ComponentProps) {
  const { extraction, requestModel, requestMessages, requestFetchError, evidence } =
    loaderData;
  const backHref = `/admin/se/company/${params.companyId}/esef/${params.documentId}`;

  return (
    <div className="flex w-full flex-col gap-5">
      <div>
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2"
          nativeButton={false}
          render={<Link to={backHref} />}
        >
          <ArrowLeft data-icon="inline-start" />
          Back to facts
        </Button>
      </div>

      <div className="flex min-w-0 flex-col gap-2">
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="text-xl font-semibold tracking-tight">
            LLM extraction
          </h2>
          {extraction ? (
            <Badge
              variant={
                extraction.extractionStatus === "succeeded"
                  ? "outline"
                  : "destructive"
              }
            >
              {extraction.extractionStatus}
            </Badge>
          ) : (
            <Badge variant="secondary">Not run yet</Badge>
          )}
        </div>
        <p className="text-muted-foreground break-all font-mono text-xs">
          {params.documentId}
        </p>
      </div>

      {extraction ? (
        <dl className="grid grid-cols-2 rounded-xl bg-muted/35 px-4 ring-1 ring-foreground/10 lg:grid-cols-5">
          <div className="border-b py-3 lg:border-r lg:border-b-0 lg:pr-4">
            <dt className="text-muted-foreground text-xs">Model</dt>
            <dd className="mt-1 truncate font-medium" title={extraction.modelName}>
              {extraction.modelName || "—"}
            </dd>
          </div>
          <div className="border-b py-3 lg:border-r lg:border-b-0 lg:px-4">
            <dt className="text-muted-foreground text-xs">Prompt version</dt>
            <dd className="mt-1 font-medium">{extraction.promptVersion || "—"}</dd>
          </div>
          <div className="border-b py-3 lg:border-r lg:border-b-0 lg:px-4">
            <dt className="text-muted-foreground text-xs">Tokens in / out</dt>
            <dd className="mt-1 font-medium tabular-nums">
              {extraction.promptTokens.toLocaleString("en-US")} /{" "}
              {extraction.completionTokens.toLocaleString("en-US")}
            </dd>
          </div>
          <div className="py-3 lg:border-r lg:px-4">
            <dt className="text-muted-foreground text-xs">Input characters</dt>
            <dd className="mt-1 font-medium tabular-nums">
              {extraction.inputCharacterCount.toLocaleString("en-US")}
            </dd>
          </div>
          <div className="py-3 lg:pl-4">
            <dt className="text-muted-foreground text-xs">Extracted at</dt>
            <dd className="mt-1 font-medium">{extraction.extractedAt}</dd>
          </div>
        </dl>
      ) : null}

      <Card className="min-w-0">
        <CardHeader>
          <CardTitle>Sent to the LLM</CardTitle>
          <CardDescription>
            {extraction?.llmRequestObjectKey
              ? `The stored request body (${requestModel || "model unknown"}) from S3: ${extraction.llmRequestObjectKey}`
              : "No stored request for this document yet — the evidence below is what an extraction run would draw on."}
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          {requestFetchError ? (
            <p className="text-destructive text-sm">{requestFetchError}</p>
          ) : null}
          {requestMessages.map((message, index) => (
            <CollapsibleText
              key={index}
              summary={`Message ${index + 1} · ${message.role}`}
              text={message.content}
            />
          ))}
          {requestMessages.length === 0 && !requestFetchError ? (
            <p className="text-muted-foreground text-sm">
              No request messages to show.
            </p>
          ) : null}
        </CardContent>
      </Card>

      {extraction ? (
        <Card className="min-w-0">
          <CardHeader>
            <CardTitle>LLM response</CardTitle>
            <CardDescription>
              Raw response text as returned by the provider, followed by the
              validated result the pipeline published.
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            <CollapsibleText
              summary="Raw response"
              text={extraction.llmResponseText}
            />
            {extraction.companyDescription ? (
              <section>
                <h3 className="font-medium">Company description</h3>
                <p className="max-w-[90ch]">{extraction.companyDescription}</p>
                {extraction.descriptionEvidenceIdsJson ? (
                  <p className="text-muted-foreground mt-1 break-all font-mono text-xs">
                    evidence: {extraction.descriptionEvidenceIdsJson}
                  </p>
                ) : null}
              </section>
            ) : null}
            <div className="grid gap-4 sm:grid-cols-2">
              {RESULT_SECTIONS.map(([key, label]) => {
                const raw = String(extraction[key] ?? "");
                const items = parseJsonList(raw);
                if (items === null) {
                  if (raw.trim() === "" || raw.trim() === "[]") return null;
                  return (
                    <section key={key}>
                      <h3 className="font-medium">{label}</h3>
                      <p className="break-all text-muted-foreground text-sm">
                        {raw}
                      </p>
                    </section>
                  );
                }
                if (items.length === 0) return null;
                return (
                  <section key={key}>
                    <h3 className="font-medium">{label}</h3>
                    <ul className="list-disc pl-4 text-sm">
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
      ) : null}

      <Card className="min-w-0">
        <CardHeader>
          <CardTitle>Evidence disclosures</CardTitle>
          <CardDescription>
            The deterministic narrative evidence extracted from this document —
            the pool the LLM request is assembled from. Result entries cite
            these by disclosure id.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          {evidence.length === 0 ? (
            <p className="text-muted-foreground text-sm">
              No disclosures parsed for this document yet.
            </p>
          ) : (
            evidence.map((item) => (
              <details key={item.disclosureId} className="min-w-0 rounded-lg border">
                <summary className="cursor-pointer px-3 py-2 text-sm">
                  <span className="font-medium">
                    {item.conceptLocalName || item.sectionType || item.disclosureKind}
                  </span>
                  <span className="text-muted-foreground ml-2 text-xs">
                    {item.disclosureKind}
                    {item.language ? ` · ${item.language}` : ""}
                    {item.printedPageNumber ? ` · p. ${item.printedPageNumber}` : ""}
                    {item.tableCount > 0 ? ` · ${item.tableCount} tables` : ""}
                    {" · "}
                    {item.originalCharacterCount.toLocaleString("en-US")} chars
                  </span>
                </summary>
                <div className="border-t px-3 py-2">
                  <p className="text-muted-foreground break-all font-mono text-xs">
                    {item.disclosureId}
                  </p>
                  <p className="mt-2 text-sm whitespace-pre-wrap">
                    {item.textPreview}
                    {item.originalCharacterCount > item.textPreview.length
                      ? "…"
                      : ""}
                  </p>
                </div>
              </details>
            ))
          )}
        </CardContent>
      </Card>
    </div>
  );
}
