import type { Route } from "./+types/admin-se-company-address";
import { SeAddressWorkspace } from "~/components/admin/se-address-workspace";
import { parseSeAddressDecision } from "~/lib/se-address-decision-form";
import {
  selectedAddressFromSearch,
  WORKPLACE_PAGE_SIZE,
  workplacePageFromSearch,
  workplaceQueryFromSearch,
} from "~/lib/se-address-fields";
import {
  activateSeAddressDraft,
  discardSeAddressDraft,
  launchSeAddressFold,
  loadSeAddressDetail,
  removeSeAddress,
  resetSeAddress,
  saveSeAddressDraft,
  SeAddressDecisionError,
  type SeAddressDetail,
} from "~/lib/se-company-address-entity.server";

/** Swedish org numbers are 10 digits, or 12 with the century prefix. */
const COMPANY_ID_PATTERN = /^([0-9]{10}|[0-9]{12})$/;

/** A company no source has suggested an address for is a normal pipeline
 * state, not a broken link -- and the reviewer must still be able to type one.
 * So the tab opens on an empty detail rather than a 404: the workspace says
 * nothing is published and keeps Add address, Correct's counterpart, in reach.
 * The company layout already 404s a company that does not exist at all. The
 * workplace page echoes what was asked for, so the card's links stay honest
 * even on a company that has none. */
function emptyAddressDetail(workplacePage: number, workplaceQuery: string): SeAddressDetail {
  return {
    published: [],
    selected: null,
    workplaces: {
      rows: [],
      total: 0,
      page: workplacePage,
      pageSize: WORKPLACE_PAGE_SIZE,
      query: workplaceQuery,
    },
    drafts: [],
    history: [],
    rules: [],
    foldPending: false,
  };
}

// Only `loader`, `action`, `meta` and the component live here. Any other
// export that touched `~/lib/*.server` would keep that module in the client
// bundle and break the production build.

export async function loader({ request, params }: Route.LoaderArgs) {
  // Three parameters, all read here and all passed on: the loader does the
  // selection, the filtering, the offset and the count in ClickHouse.
  const search = new URL(request.url).searchParams;
  const selectedKey = selectedAddressFromSearch(search);
  const workplacePage = workplacePageFromSearch(search);
  const workplaceQuery = workplaceQueryFromSearch(search);
  const detail = await loadSeAddressDetail(params.companyId, {
    selectedKey,
    workplacePage,
    workplaceQuery,
  });
  return { detail: detail ?? emptyAddressDetail(workplacePage, workplaceQuery) };
}

/**
 * One of the entity's six decisions (remove, reset, save-draft, activate,
 * discard, fold-now). The store's refusals are the reviewer's to read;
 * anything else is a real failure and must not be dressed up as a form error.
 */
export async function action({ request, params }: Route.ActionArgs) {
  if (!COMPANY_ID_PATTERN.test(params.companyId)) {
    return { ok: false as const, intent: "", error: "Company id must be 10 or 12 digits." };
  }
  const form = await request.formData();
  // Read once, before parsing: every returned result carries the posted
  // intent so the workspace and the edit sheet can each show only their own.
  const intent = String(form.get("intent") ?? "");
  const parsed = parseSeAddressDecision(form);
  if (!parsed.ok) return { ok: false as const, intent, error: parsed.error };
  const { decision } = parsed;
  if (decision.intent === "fold-now") {
    const { runId, url } = await launchSeAddressFold(params.companyId);
    return { ok: true as const, intent, runId, url };
  }
  try {
    if (decision.intent === "save-draft") {
      const { slot } = await saveSeAddressDraft(params.companyId, decision);
      return { ok: true as const, intent, slot };
    }
    if (decision.intent === "activate") {
      await activateSeAddressDraft(params.companyId, decision);
      return { ok: true as const, intent };
    }
    if (decision.intent === "discard") {
      await discardSeAddressDraft(params.companyId, decision);
      return { ok: true as const, intent };
    }
    if (decision.intent === "remove") {
      await removeSeAddress(params.companyId, decision);
      return { ok: true as const, intent };
    }
    await resetSeAddress(params.companyId, decision);
    return { ok: true as const, intent };
  } catch (error) {
    if (error instanceof SeAddressDecisionError) {
      return { ok: false as const, intent, error: error.message };
    }
    throw error;
  }
}

export function meta({ params }: Route.MetaArgs) {
  // The entity carries no company name of its own (unlike the Info tab's
  // `detail.info.legal_name`); the layout's own meta already titles the page
  // with the company, so this just names the tab.
  return [{ title: `${params.companyId} address | CompanyCollect` }];
}

export default function AdminSwedenCompanyAddress({
  loaderData,
  actionData,
  params,
}: Route.ComponentProps) {
  return (
    <SeAddressWorkspace
      companyId={params.companyId}
      detail={loaderData.detail}
      result={actionData ?? null}
    />
  );
}
