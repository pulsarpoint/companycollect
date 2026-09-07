import { FileSearchIcon } from "lucide-react";
import { data } from "react-router";
import type { Route } from "./+types/admin-se-company-address";
import { SeAddressWorkspace } from "~/components/admin/se-address-workspace";
import { buttonVariants } from "~/components/ui/button";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { parseSeAddressDecision } from "~/lib/se-address-decision-form";
import { selectedAddressFromSearch } from "~/lib/se-address-fields";
import {
  activateSeAddressDraft,
  discardSeAddressDraft,
  launchSeAddressFold,
  loadSeAddressDetail,
  removeSeAddress,
  resetSeAddress,
  saveSeAddressDraft,
  SeAddressDecisionError,
} from "~/lib/se-company-address-entity.server";

/** Swedish org numbers are 10 digits, or 12 with the century prefix. */
const COMPANY_ID_PATTERN = /^([0-9]{10}|[0-9]{12})$/;

// Only `loader`, `action`, `meta` and the component live here. Any other
// export that touched `~/lib/*.server` would keep that module in the client
// bundle and break the production build.

export async function loader({ request, params }: Route.LoaderArgs) {
  const detail = await loadSeAddressDetail(params.companyId);
  const selectedKey = selectedAddressFromSearch(new URL(request.url).searchParams);
  // A company no extractor has suggested an address for and no fold has
  // published one is a normal pipeline state, not a broken link: the page
  // says so under a 404, exactly as the Info tab does.
  return data({ detail, selectedKey }, detail ? undefined : { status: 404 });
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

/** Shown when the company has no address at any layer: no published row, no
 * normalized row and no draft. Mirrors `SeBasicInfoNotFolded`. */
function AddressNotFolded({ companyId }: { companyId: string }) {
  return (
    <div className="flex flex-col gap-6">
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <FileSearchIcon />
          </EmptyMedia>
          <EmptyTitle>Not folded yet</EmptyTitle>
          <EmptyDescription>
            Company {companyId} is not in se_company_address_v2 yet and no source
            has suggested an address for it. The extractors write suggestions from
            the registers; the fold publishes the row.
          </EmptyDescription>
        </EmptyHeader>
        <EmptyContent>
          <a
            className={buttonVariants({ variant: "outline" })}
            href={`/company/se/${encodeURIComponent(companyId)}`}
          >
            Back to company
          </a>
        </EmptyContent>
      </Empty>
    </div>
  );
}

export default function AdminSwedenCompanyAddress({
  loaderData,
  actionData,
  params,
}: Route.ComponentProps) {
  if (!loaderData.detail) {
    return <AddressNotFolded companyId={params.companyId} />;
  }
  return (
    <SeAddressWorkspace
      companyId={params.companyId}
      detail={loaderData.detail}
      selectedKey={loaderData.selectedKey}
      result={actionData ?? null}
    />
  );
}
