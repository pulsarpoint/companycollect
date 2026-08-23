import { SparklesIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import type {
  PersonProfileGenerationActionData,
  PersonProfileSuggestion,
} from "~/lib/person-profile-llm.server";

const numberFormat = new Intl.NumberFormat("en-US");

function EvidenceReferences({ ids }: { ids: string[] }) {
  if (ids.length === 0) return null;
  return (
    <p className="mt-1 break-all font-mono text-xs text-muted-foreground">
      Evidence: {ids.join(", ")}
    </p>
  );
}

export function PersonProfileSuggestionCard({
  suggestion,
  generation,
}: {
  suggestion: PersonProfileSuggestion;
  generation: Extract<
    PersonProfileGenerationActionData,
    { ok: true }
  >["generation"];
}) {
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center gap-2">
          <Badge>LLM suggestion</Badge>
          <Badge variant="outline">
            {generation.provider} · {generation.model}
          </Badge>
        </div>
        <CardTitle>{suggestion.displayName}</CardTitle>
        <CardDescription>
          Comprehensive profile extracted from the compacted source evidence.
          Review it before using it as normalized person data.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-5">
        <dl className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <div className="flex flex-col gap-1">
            <dt className="text-xs text-muted-foreground">Birth date</dt>
            <dd className="text-sm font-medium">
              {suggestion.birthDate ?? "Not found"}
            </dd>
          </div>
          <div className="flex flex-col gap-1">
            <dt className="text-xs text-muted-foreground">Birth year</dt>
            <dd className="text-sm font-medium tabular-nums">
              {suggestion.birthYear ?? "Not found"}
            </dd>
          </div>
          <div className="flex flex-col gap-1">
            <dt className="text-xs text-muted-foreground">Nationalities</dt>
            <dd className="text-sm font-medium">
              {suggestion.nationalities.join(", ") || "Not found"}
            </dd>
          </div>
          <div className="flex flex-col gap-1">
            <dt className="text-xs text-muted-foreground">Occupations</dt>
            <dd className="text-sm font-medium">
              {suggestion.occupations.join(", ") || "Not found"}
            </dd>
          </div>
        </dl>

        <div className="flex flex-col gap-1">
          <h3 className="text-sm font-medium">Description</h3>
          <p className="text-sm text-muted-foreground">
            {suggestion.description ?? "No description found in the evidence."}
          </p>
        </div>

        {suggestion.alternativeNames.length > 0 ? (
          <div className="flex flex-col gap-2">
            <h3 className="text-sm font-medium">Alternative names</h3>
            <div className="flex flex-wrap gap-2">
              {suggestion.alternativeNames.map((name) => (
                <Badge key={name} variant="secondary">
                  {name}
                </Badge>
              ))}
            </div>
          </div>
        ) : null}

        {suggestion.companyRoles.length > 0 ? (
          <div className="flex flex-col gap-2">
            <h3 className="text-sm font-medium">Company roles</h3>
            <div className="grid gap-3 sm:grid-cols-2">
              {suggestion.companyRoles.map((role, index) => (
                <div
                  key={`${role.companyId}:${role.role}:${index}`}
                  className="rounded-lg border p-3"
                >
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant="secondary">{role.role}</Badge>
                    <span className="font-mono text-xs">{role.companyId}</span>
                  </div>
                  <p className="mt-2 text-sm">
                    {role.roleLabel ?? role.role} · {role.startYear ?? "?"}–
                    {role.isCurrent ? "current" : (role.endYear ?? "?")}
                  </p>
                  <EvidenceReferences ids={role.evidenceIds} />
                </div>
              ))}
            </div>
          </div>
        ) : null}

        {suggestion.additionalFacts.length > 0 ? (
          <div className="flex flex-col gap-2">
            <h3 className="text-sm font-medium">Additional facts</h3>
            <dl className="grid gap-3 sm:grid-cols-2">
              {suggestion.additionalFacts.map((fact, index) => (
                <div
                  key={`${fact.label}:${index}`}
                  className="rounded-lg border p-3"
                >
                  <dt className="text-xs text-muted-foreground">
                    {fact.label}
                  </dt>
                  <dd className="mt-1 text-sm">{fact.value}</dd>
                  <EvidenceReferences ids={fact.evidenceIds} />
                </div>
              ))}
            </dl>
          </div>
        ) : null}

        {suggestion.evidenceSummary ? (
          <Alert>
            <SparklesIcon />
            <AlertTitle>Evidence summary</AlertTitle>
            <AlertDescription>{suggestion.evidenceSummary}</AlertDescription>
          </Alert>
        ) : null}

        {suggestion.referenceUrls.length > 0 || suggestion.imageUrl ? (
          <div className="flex flex-col gap-2">
            <h3 className="text-sm font-medium">References</h3>
            <div className="flex flex-col gap-1 text-sm">
              {[suggestion.imageUrl, ...suggestion.referenceUrls]
                .filter((url): url is string => Boolean(url))
                .map((url) => (
                  <a
                    key={url}
                    href={url}
                    target="_blank"
                    rel="noreferrer"
                    className="break-all underline-offset-2 hover:underline"
                  >
                    {url}
                  </a>
                ))}
            </div>
          </div>
        ) : null}
      </CardContent>
      <CardFooter className="text-xs text-muted-foreground">
        {suggestion.evidenceIds.length} compacted evidence records cited
        {generation.totalTokens === null
          ? ""
          : ` · ${numberFormat.format(generation.totalTokens)} tokens`}
      </CardFooter>
    </Card>
  );
}
