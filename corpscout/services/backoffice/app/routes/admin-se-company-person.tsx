import type { Route } from "./+types/admin-se-company-person";
import { SePersonWorkspace } from "~/components/admin/se-person-workspace";
import { parseSePersonDecision } from "~/lib/se-person-decision-form";
import { selectedPersonFromSearch } from "~/lib/se-person-fields";
import {
  activateSePersonDraft,
  discardSePersonDraft,
  launchSePersonFold,
  loadSePersonDetail,
  loadSePersonRoleOptions,
  mergeSePersons,
  removeSePerson,
  resetSePersonRules,
  saveSePersonDraft,
  SePersonDecisionError,
  splitSePersonSlots,
  type SePersonDetail,
} from "~/lib/se-company-person-entity.server";

/** Swedish org numbers are 10 digits, or 12 with the century prefix. */
const COMPANY_ID_PATTERN = /^([0-9]{10}|[0-9]{12})$/;

/** A company no source has named a person for is a normal pipeline state, not a broken
 * link -- and the reviewer must still be able to type one. So the tab opens on an empty
 * detail rather than a 404; the company layout already 404s a company that does not
 * exist at all. */
const EMPTY_DETAIL: SePersonDetail = {
  published: [], drafts: [], history: [], rules: [], precedence: [],
  possibleMatches: [], foldPending: false,
};

// Only `loader`, `action`, `meta` and the component live here. Any other export that
// touched `~/lib/*.server` would keep that module in the client bundle and break the
// production build.

export async function loader({ request, params }: Route.LoaderArgs) {
  const [detail, roleOptions] = await Promise.all([
    loadSePersonDetail(params.companyId),
    loadSePersonRoleOptions(),
  ]);
  return {
    detail: detail ?? EMPTY_DETAIL,
    roleOptions,
    selectedKey: selectedPersonFromSearch(new URL(request.url).searchParams),
  };
}

/**
 * One of the entity's eight decisions (spec section 7). The store's refusals are the
 * reviewer's to read; anything else is a real failure and must not be dressed up as a
 * form error. The role catalog is read before parsing: a role code is only valid if
 * `corpscout.company_person_role_type` has it (Ruling 3).
 */
export async function action({ request, params }: Route.ActionArgs) {
  if (!COMPANY_ID_PATTERN.test(params.companyId)) {
    return { ok: false as const, intent: "", error: "Company id must be 10 or 12 digits." };
  }
  const form = await request.formData();
  const intent = String(form.get("intent") ?? "");
  const roleOptions = await loadSePersonRoleOptions();
  const parsed = parseSePersonDecision(form, roleOptions.map((option) => option.code));
  if (!parsed.ok) return { ok: false as const, intent, error: parsed.error };
  const { decision } = parsed;
  if (decision.intent === "fold-now") {
    const { runId, url } = await launchSePersonFold(params.companyId);
    return { ok: true as const, intent, runId, url };
  }
  try {
    if (decision.intent === "save-draft") {
      const { slot } = await saveSePersonDraft(params.companyId, decision);
      return { ok: true as const, intent, slot };
    }
    if (decision.intent === "activate") {
      await activateSePersonDraft(params.companyId, decision);
      // Ruling 6 (spec 7): Activate launches the targeted fold itself, so the reviewer
      // sees the person appear without a second click. The rows are already written by
      // the time this runs, so a launch failure here must not read as a lost write --
      // it answers ok, with a message pointing at Fold now, rather than rethrowing a
      // 500 (Minor 1). A second Activate would only answer "No draft to activate.": the
      // draft is already cleared.
      try {
        const { runId, url } = await launchSePersonFold(params.companyId);
        return { ok: true as const, intent, runId, url };
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);
        return {
          ok: true as const,
          intent,
          runId: undefined,
          message: `Rows written; the fold launch failed: ${reason}. Use Fold now.`,
        };
      }
    }
    if (decision.intent === "discard") {
      await discardSePersonDraft(params.companyId, decision);
      return { ok: true as const, intent };
    }
    if (decision.intent === "remove") {
      await removeSePerson(params.companyId, decision);
      return { ok: true as const, intent };
    }
    if (decision.intent === "merge") {
      await mergeSePersons(params.companyId, decision);
      return { ok: true as const, intent };
    }
    if (decision.intent === "split") {
      await splitSePersonSlots(params.companyId, decision);
      return { ok: true as const, intent };
    }
    await resetSePersonRules(params.companyId, decision);
    return { ok: true as const, intent };
  } catch (error) {
    if (error instanceof SePersonDecisionError) {
      return { ok: false as const, intent, error: error.message };
    }
    throw error;
  }
}

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `${params.companyId} people | CompanyCollect` }];
}

export default function AdminSwedenCompanyPeople({
  loaderData,
  actionData,
  params,
}: Route.ComponentProps) {
  return (
    <SePersonWorkspace
      companyId={params.companyId}
      detail={loaderData.detail}
      roleOptions={loaderData.roleOptions}
      selectedKey={loaderData.selectedKey}
      result={actionData ?? null}
    />
  );
}
