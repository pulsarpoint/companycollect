import type { Route } from "./+types/admin-se-people";
import { PeopleCurationWorkspace } from "~/components/admin/people-curation-workspace";
import type { PeopleDraftStep } from "~/components/admin/people-draft-tables";
import {
  getLatestSwedenPeopleDraftInitializationJob,
  getSwedenPeopleDraftStatus,
  startSwedenPeopleDraftInitialization,
  SwedenPeopleDraftReplacementConfirmationError,
} from "~/lib/sweden-people-draft.server";
import {
  getLatestSwedenPeopleDraftTwoJob,
  getSwedenPeopleDraftOneRowsPage,
  getSwedenPeopleDraftTwoRowsPage,
  getSwedenPeopleDraftTwoStatus,
  startSwedenPeopleDraftTwoBuild,
  SwedenPeopleDraftTwoReplacementConfirmationError,
} from "~/lib/sweden-people-draft-two.server";
import type { SwedenPeopleDraftSource } from "~/lib/sweden-people-draft-two.server";
import { seCompanyPersonId } from "~/lib/se-company-person.server";
import {
  findPersonProfileResponseDraftTwoIds,
  listPersonProfileResponseDraftTwoIds,
} from "~/lib/sweden-person-profile-responses.server";
import { getLlmRequestAvailability } from "~/lib/llm-availability.server";
import {
  getLatestSwedenPeopleProfileBulkJob,
  SwedenPeopleProfileBulkJobAlreadyRunningError,
  SwedenPeopleProfileBulkSelectionError,
} from "~/lib/sweden-person-profile-bulk.server";
import { startSwedenPeopleProfileBulkEnhancement } from "~/lib/sweden-person-profile-temporal.server";

const DRAFT_PAGE_SIZE = 5;
const DRAFT_TWO_SOURCES = new Set<SwedenPeopleDraftSource>([
  "bolagsverket",
  "esef",
  "wikidata",
]);

function pageNumber(searchParams: URLSearchParams, name: string): number {
  const value = Number(searchParams.get(name));
  return Number.isInteger(value) && value > 0 ? value : 1;
}

function wizardStep(searchParams: URLSearchParams): PeopleDraftStep {
  const requestedStep = searchParams.get("step");
  if (
    requestedStep === "draft-1" ||
    requestedStep === "draft-2" ||
    requestedStep === "final"
  ) {
    return requestedStep;
  }
  if (
    searchParams.get("draft_2") === "multiple-sources" ||
    pageNumber(searchParams, "draft_2_page") > 1
  ) {
    return "draft-2";
  }
  return "draft-1";
}

function companyFilter(request: Request) {
  const searchParams = new URL(request.url).searchParams;
  const input = searchParams.get("company_id")?.trim() ?? "";
  const draftOneView =
    searchParams.get("draft_1") === "unmapped" ? "unmapped" : "all";
  const draftTwoView =
    searchParams.get("draft_2") === "multiple-sources"
      ? "multiple-sources"
      : "all";
  const draftTwoSources = [
    ...new Set(
      searchParams
        .getAll("draft_2_source")
        .filter((source): source is SwedenPeopleDraftSource =>
          DRAFT_TWO_SOURCES.has(source as SwedenPeopleDraftSource),
        ),
    ),
  ];
  const draftTwoHasLlmSuggestion =
    searchParams.get("draft_2_llm") === "suggestion";
  const draftOnePage = pageNumber(searchParams, "draft_1_page");
  const draftTwoPage = pageNumber(searchParams, "draft_2_page");
  const currentStep = wizardStep(searchParams);
  if (input === "") {
    return {
      input,
      companyId: "",
      error: "",
      draftOneView,
      draftTwoView,
      draftTwoSources,
      draftTwoHasLlmSuggestion,
      draftOnePage,
      draftTwoPage,
      currentStep,
    } as const;
  }
  const companyId = input.replace(/[^0-9]/g, "");
  if (!/^[0-9\s-]+$/.test(input) || companyId.length !== 10) {
    return {
      input,
      companyId: "",
      error: "Enter a 10-digit Swedish organization number.",
      draftOneView,
      draftTwoView,
      draftTwoSources,
      draftTwoHasLlmSuggestion,
      draftOnePage,
      draftTwoPage,
      currentStep,
    } as const;
  }
  return {
    input,
    companyId,
    error: "",
    draftOneView,
    draftTwoView,
    draftTwoSources,
    draftTwoHasLlmSuggestion,
    draftOnePage,
    draftTwoPage,
    currentStep,
  } as const;
}

export async function loader({ request }: Route.LoaderArgs) {
  const filter = companyFilter(request);
  const llmSuggestionDraftTwoIds = filter.draftTwoHasLlmSuggestion
    ? listPersonProfileResponseDraftTwoIds()
    : undefined;
  const [
    draftStatus,
    initializationJob,
    draftTwoStatus,
    draftTwoJob,
    draftOnePage,
    draftTwoPage,
  ] = await Promise.all([
    getSwedenPeopleDraftStatus(),
    getLatestSwedenPeopleDraftInitializationJob(),
    getSwedenPeopleDraftTwoStatus(),
    getLatestSwedenPeopleDraftTwoJob(),
    getSwedenPeopleDraftOneRowsPage({
      companyId: filter.companyId,
      onlyUnmapped: filter.draftOneView === "unmapped",
      page: filter.draftOnePage,
      pageSize: DRAFT_PAGE_SIZE,
    }),
    getSwedenPeopleDraftTwoRowsPage({
      companyId: filter.companyId,
      onlyMultipleSources: filter.draftTwoView === "multiple-sources",
      selectedSources: filter.draftTwoSources,
      draftTwoIds: llmSuggestionDraftTwoIds,
      page: filter.draftTwoPage,
      pageSize: DRAFT_PAGE_SIZE,
    }),
  ]);
  const rowsWithLlmSuggestions = findPersonProfileResponseDraftTwoIds(
    draftTwoPage.rows.map((row) => row.draft_2_id),
  );
  const decoratedDraftTwoPage = {
    ...draftTwoPage,
    rows: draftTwoPage.rows.map((row) => ({
      ...row,
      has_llm_suggestion: rowsWithLlmSuggestions.has(row.draft_2_id),
      se_person_id: seCompanyPersonId(row.company_id, row.name),
    })),
  };
  return {
    draftStatus,
    initializationJob,
    draftTwoStatus,
    draftTwoJob,
    draftOnePage,
    draftTwoPage: decoratedDraftTwoPage,
    filter,
    bulkJob: getLatestSwedenPeopleProfileBulkJob(),
    llmAvailability: getLlmRequestAvailability(),
  };
}

export async function action({ request }: Route.ActionArgs) {
  const form = await request.formData();
  const intent = form.get("intent");

  try {
    if (intent === "start_people_draft_step_1_initialization") {
      const job = await startSwedenPeopleDraftInitialization({
        confirmReplacement: form.get("confirm_replacement") === "yes",
      });
      return {
        ok: true as const,
        target: "draft-1" as const,
        job,
      };
    }
    if (intent === "start_people_draft_step_2_build") {
      const job = await startSwedenPeopleDraftTwoBuild({
        confirmReplacement: form.get("confirm_replacement") === "yes",
      });
      return {
        ok: true as const,
        target: "draft-2" as const,
        job,
      };
    }
    if (intent === "start_people_profile_bulk_enhancement") {
      const llmAvailability = getLlmRequestAvailability();
      if (!llmAvailability.ready) {
        return {
          ok: false as const,
          target: "person-profile-bulk" as const,
          error:
            llmAvailability.warning ??
            "Configure and activate an LLM before starting bulk enhancement.",
        };
      }
      const selectionMode = form.get("selection_mode");
      const requiredSources = form
        .getAll("required_source")
        .filter((value): value is SwedenPeopleDraftSource =>
          DRAFT_TWO_SOURCES.has(value as SwedenPeopleDraftSource),
        );
      const draftTwoIds =
        selectionMode === "all_filtered"
          ? null
          : form
              .getAll("draft_two_id")
              .map(String)
              .map((value) => value.trim())
              .filter((value) => value !== "");
      const job = await startSwedenPeopleProfileBulkEnhancement({
        companyId: String(form.get("company_id") ?? "").trim(),
        requiredSources,
        requireSavedSuggestion:
          form.get("require_saved_suggestion") === "yes",
        draftTwoIds,
      });
      return {
        ok: true as const,
        target: "person-profile-bulk" as const,
        job,
        existing: false,
      };
    }
    return { ok: false as const, error: "Unknown people processing action." };
  } catch (error) {
    if (error instanceof SwedenPeopleDraftReplacementConfirmationError) {
      return {
        ok: false as const,
        error:
          "Draft 1 already contains observations. Confirm the rebuild before replacing them.",
      };
    }
    if (error instanceof SwedenPeopleDraftTwoReplacementConfirmationError) {
      return {
        ok: false as const,
        error:
          "Draft 2 already contains merged rows. Confirm the rebuild before replacing them.",
      };
    }
    if (error instanceof SwedenPeopleProfileBulkJobAlreadyRunningError) {
      return {
        ok: true as const,
        target: "person-profile-bulk" as const,
        job: error.job,
        existing: true,
      };
    }
    if (error instanceof SwedenPeopleProfileBulkSelectionError) {
      return {
        ok: false as const,
        target: "person-profile-bulk" as const,
        error: error.message,
      };
    }
    if (intent === "start_people_draft_step_1_initialization") {
      console.error("Draft 1 Temporal workflow start failed", { error });
      return {
        ok: false as const,
        error:
          "The Draft 1 Temporal workflow could not be started. Check the Temporal connection and try again.",
      };
    }
    if (intent === "start_people_draft_step_2_build") {
      console.error("Draft 2 Temporal workflow start failed", { error });
      return {
        ok: false as const,
        error:
          "The Draft 2 Temporal workflow could not be started. Check the Temporal connection and try again.",
      };
    }
    if (intent === "start_people_profile_bulk_enhancement") {
      console.error("Bulk person-profile workflow start failed", { error });
      return {
        ok: false as const,
        target: "person-profile-bulk" as const,
        error:
          "The Temporal workflow could not be started. Check the Temporal connection and try again.",
      };
    }
    throw error;
  }
}

export function meta({}: Route.MetaArgs) {
  return [
    { title: "Sweden people curation | CompanyCollect" },
    {
      name: "description",
      content: "Wizard for processing Swedish company people.",
    },
  ];
}

export default function AdminSwedenPeople({ loaderData }: Route.ComponentProps) {
  return (
    <PeopleCurationWorkspace
      draftStatus={loaderData.draftStatus}
      initializationJob={loaderData.initializationJob}
      draftTwoStatus={loaderData.draftTwoStatus}
      draftTwoJob={loaderData.draftTwoJob}
      draftOnePage={loaderData.draftOnePage}
      draftTwoPage={loaderData.draftTwoPage}
      filter={loaderData.filter}
      bulkJob={loaderData.bulkJob}
      llmAvailability={loaderData.llmAvailability}
    />
  );
}
