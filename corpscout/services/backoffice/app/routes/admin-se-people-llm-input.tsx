import {
  ArrowLeftIcon,
  BracesIcon,
  DatabaseIcon,
  RefreshCwIcon,
  SparklesIcon,
  TriangleAlertIcon,
} from "lucide-react";
import { data, Form, Link, useNavigation } from "react-router";
import type { Route } from "./+types/admin-se-people-llm-input";
import { PersonProfileSuggestionCard } from "~/components/admin/person-profile-suggestion-card";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button, buttonVariants } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  getLlmRequestAvailability,
  type LlmRequestAvailability,
} from "~/lib/llm-availability.server";
import {
  buildPersonProfileLlmInput,
  generatePersonProfileSuggestion,
  PersonProfileLlmError,
} from "~/lib/person-profile-llm.server";
import {
  getLatestPersonProfileResponse,
  personProfileInputHash,
  savePersonProfileResponse,
} from "~/lib/sweden-person-profile-responses.server";
import type { StoredPersonProfileResponse } from "~/lib/sweden-person-profile-responses.server";
import { getSwedenPeopleDraftTwoRow } from "~/lib/sweden-people-draft-two.server";

function peopleReturnTo(request: Request): string {
  const fallback = "/admin/se/people?step=draft-2";
  const requestUrl = new URL(request.url);
  const requestedReturnTo = requestUrl.searchParams.get("return_to");
  if (!requestedReturnTo) return fallback;
  let resolvedReturnTo: URL;
  try {
    resolvedReturnTo = new URL(requestedReturnTo, requestUrl.origin);
  } catch {
    return fallback;
  }
  if (
    resolvedReturnTo.origin !== requestUrl.origin ||
    resolvedReturnTo.pathname !== "/admin/se/people"
  ) {
    return fallback;
  }
  return `${resolvedReturnTo.pathname}${resolvedReturnTo.search}`;
}

export async function loader({ request, params }: Route.LoaderArgs) {
  const candidate = await getSwedenPeopleDraftTwoRow(params.draftTwoId);
  if (!candidate) {
    throw data("Draft 2 person not found", { status: 404 });
  }
  const input = buildPersonProfileLlmInput(candidate);
  const storedResponse = getLatestPersonProfileResponse({
    draftTwoId: candidate.draft_2_id,
  });
  return {
    person: {
      draftTwoId: candidate.draft_2_id,
      companyId: candidate.company_id,
      name: candidate.name,
      position: candidate.position,
      rawObservationCount: candidate.source_observations?.length ?? 0,
      compactedObservationCount: input.sourceRecords.length,
    },
    input,
    llmAvailability: getLlmRequestAvailability(),
    storedResponse,
    storedResponseIsCurrent:
      storedResponse?.inputHash === personProfileInputHash(input),
    returnTo: peopleReturnTo(request),
  };
}

export async function action({ request, params }: Route.ActionArgs) {
  const form = await request.formData();
  const draftTwoId = params.draftTwoId;
  if (form.get("intent") !== "generate_person_profile") {
    return {
      ok: false as const,
      target: "person-profile" as const,
      draftTwoId,
      error: "Unknown LLM request action.",
      requestPrepared: true,
      configurationRequired: false,
    };
  }
  const candidate = await getSwedenPeopleDraftTwoRow(draftTwoId);
  if (!candidate) {
    return {
      ok: false as const,
      target: "person-profile" as const,
      draftTwoId,
      error: "This Draft 2 person no longer exists. Reload the page.",
      requestPrepared: false,
      configurationRequired: false,
    };
  }
  const input = buildPersonProfileLlmInput(candidate);
  try {
    const result = await generatePersonProfileSuggestion({ candidate });
    const storedResponse = savePersonProfileResponse({
      draftTwoId,
      input,
      rawResponse: result.rawResponse,
      suggestion: result.suggestion,
      generation: result.generation,
    });
    return {
      ok: true as const,
      target: "person-profile" as const,
      draftTwoId,
      storedResponse,
    };
  } catch (error) {
    if (error instanceof PersonProfileLlmError) {
      return {
        ok: false as const,
        target: "person-profile" as const,
        draftTwoId,
        error: error.message,
        requestPrepared: true,
        configurationRequired: error.kind === "configuration",
      };
    }
    console.error("Manual person profile generation failed", {
      draftTwoId,
      error,
    });
    return {
      ok: false as const,
      target: "person-profile" as const,
      draftTwoId,
      error: "The LLM profile request failed unexpectedly.",
      requestPrepared: true,
      configurationRequired: false,
    };
  }
}

export function meta({ loaderData }: Route.MetaArgs) {
  return [
    {
      title: `${loaderData?.person.name ?? "Person"} LLM input | CompanyCollect`,
    },
  ];
}

export default function AdminSwedenPeopleLlmInput({
  loaderData,
  actionData,
}: Route.ComponentProps) {
  return (
    <PersonProfileLlmInputWorkspace
      person={loaderData.person}
      input={loaderData.input}
      llmAvailability={loaderData.llmAvailability}
      storedResponse={loaderData.storedResponse}
      storedResponseIsCurrent={loaderData.storedResponseIsCurrent}
      returnTo={loaderData.returnTo}
      result={actionData ?? null}
    />
  );
}

export function PersonProfileLlmInputWorkspace({
  person,
  input,
  llmAvailability,
  storedResponse,
  storedResponseIsCurrent,
  returnTo,
  result,
}: {
  person: {
    draftTwoId: string;
    companyId: string;
    name: string;
    position: string;
    rawObservationCount: number;
    compactedObservationCount: number;
  };
  input: unknown;
  llmAvailability: LlmRequestAvailability;
  storedResponse: StoredPersonProfileResponse | null;
  storedResponseIsCurrent: boolean;
  returnTo: string;
  result: Awaited<ReturnType<typeof action>> | null;
}) {
  const navigation = useNavigation();
  const sending =
    navigation.state === "submitting" &&
    navigation.formData?.get("intent") === "generate_person_profile";
  const displayedResponse = result?.ok ? result.storedResponse : storedResponse;
  const displayedResponseIsCurrent = result?.ok
    ? true
    : storedResponseIsCurrent;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col justify-between gap-3 md:flex-row md:items-start">
        <div className="flex flex-col gap-2">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="outline">LLM input preview</Badge>
            <Badge variant="secondary">{person.position}</Badge>
          </div>
          <div className="flex flex-col gap-1">
            <h1 className="text-2xl font-semibold tracking-tight">
              {person.name}
            </h1>
            <p className="text-sm text-muted-foreground">
              Company {person.companyId} · {person.rawObservationCount} raw
              observations compacted into {person.compactedObservationCount}
              {" "}source records.
            </p>
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Link
            className={buttonVariants({ variant: "outline" })}
            to={returnTo}
          >
            <ArrowLeftIcon data-icon="inline-start" />
            Back to people
          </Link>
          <Form method="post">
            <input
              type="hidden"
              name="intent"
              value="generate_person_profile"
            />
            <Button
              type="submit"
              disabled={!llmAvailability.ready || sending}
              aria-describedby={
                llmAvailability.ready ? undefined : "llm-unavailable-warning"
              }
              title={llmAvailability.warning ?? undefined}
            >
              {displayedResponse ? (
                <RefreshCwIcon data-icon="inline-start" />
              ) : (
                <SparklesIcon data-icon="inline-start" />
              )}
              {sending
                ? displayedResponse
                  ? "Retrying LLM request…"
                  : "Sending LLM request…"
                : displayedResponse
                  ? "Retry LLM request"
                  : "Send LLM request"}
            </Button>
          </Form>
        </div>
      </header>

      {!llmAvailability.ready ? (
        <Alert id="llm-unavailable-warning">
          <TriangleAlertIcon />
          <AlertTitle>LLM request is disabled</AlertTitle>
          <AlertDescription className="flex flex-col gap-3">
            <span>{llmAvailability.warning}</span>
            <span>
              <Button
                size="sm"
                variant="outline"
                nativeButton={false}
                render={<Link to="/admin/settings/llms" />}
              >
                {llmAvailability.profile ? "Review LLM settings" : "Configure LLM"}
              </Button>
            </span>
          </AlertDescription>
        </Alert>
      ) : (
        <Alert>
          <SparklesIcon />
          <AlertTitle>
            {displayedResponse ? "Ready to retry" : "Ready to send"}
          </AlertTitle>
          <AlertDescription>
            This request will use {llmAvailability.profile?.name} ·{" "}
            {llmAvailability.profile?.provider} · {llmAvailability.profile?.model}.
          </AlertDescription>
        </Alert>
      )}

      {displayedResponse ? (
        <Alert>
          {displayedResponseIsCurrent ? <DatabaseIcon /> : <TriangleAlertIcon />}
          <AlertTitle>
            {displayedResponseIsCurrent
              ? "Saved LLM response"
              : "Saved response uses older evidence"}
          </AlertTitle>
          <AlertDescription>
            {displayedResponseIsCurrent ? (
              <>
                Loaded from the local people-curation SQLite database · attempt{" "}
                {displayedResponse.attemptCount} · generated{" "}
                <time dateTime={displayedResponse.createdAt}>
                  {displayedResponse.createdAt}
                </time>
                {" "}· {displayedResponse.reviewStatus} review.
              </>
            ) : (
              <>
                This stored profile is retained and shown, but Draft 2 evidence
                has changed since it was generated. Retry the LLM request before
                reviewing this profile.
              </>
            )}
          </AlertDescription>
        </Alert>
      ) : null}

      {displayedResponse ? (
        <PersonProfileSuggestionCard
          suggestion={displayedResponse.suggestion}
          generation={displayedResponse.generation}
        />
      ) : null}

      {result && !result.ok ? (
        <Alert variant={result.configurationRequired ? "default" : "destructive"}>
          <TriangleAlertIcon />
          <AlertTitle>Could not generate the profile</AlertTitle>
          <AlertDescription>{result.error}</AlertDescription>
        </Alert>
      ) : null}

      <Card>
        <CardHeader>
          <div className="flex items-center gap-2">
            <BracesIcon />
            <CardTitle>Exact JSON sent to the LLM</CardTitle>
          </div>
          <CardDescription>
            Read-only output from the Bolagsverket, ESEF and Wikidata parsers.
            Yearly duplicates with identical facts are represented once with
            their observed fiscal-year range. Internal Draft 1 identifiers and
            timestamps remain server-side.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <pre className="max-h-[70vh] overflow-auto whitespace-pre-wrap break-words rounded-lg border bg-muted p-4 font-mono text-xs">
            {JSON.stringify(input, null, 2)}
          </pre>
        </CardContent>
      </Card>
    </div>
  );
}
