import type { Route } from "./+types/admin-se-company-address";
import { SeCompanyAddressTab } from "~/components/admin/se-company-address";
import {
  appendSeCompanyAddressCorrection,
  loadSeCompanyAddresses,
} from "~/lib/se-company-address.server";
import { SeAddressCorrectionValidationError } from "~/lib/se-address-corrections";
import {
  buildCorrectionInput,
  liveOverrideRefusal,
} from "~/lib/se-address-review-form";

// Only `loader`, `action` and the component live here -- see
// admin-se-company-layout.tsx for why. Any other export that touched
// `~/lib/*.server` would keep that module in the client bundle and break the
// production build.

export async function loader({ params }: Route.LoaderArgs) {
  return { detail: await loadSeCompanyAddresses(params.companyId) };
}

export async function action({ request, params }: Route.ActionArgs) {
  const built = buildCorrectionInput(await request.formData(), {
    companyId: params.companyId,
  });
  if (!built.ok) {
    return { ok: false as const, error: built.error };
  }
  // A second override of a row that already carries a live one is refused
  // here as well as on the page: the later one would win by created_at and
  // bury the first, and a page left open (or a hand-rolled post) must not be
  // able to do that silently. Only that kind needs the current ledger --
  // a reject and an override decide different questions, and an undo is the
  // way out of an override.
  if (built.input.kind === "override_field") {
    const addressKey = String(built.input.payload?.address_key ?? "");
    const current = await loadSeCompanyAddresses(params.companyId);
    const refusal = liveOverrideRefusal(
      built.input.kind,
      addressKey,
      current.corrections,
    );
    if (refusal) return { ok: false as const, error: refusal };
  }
  try {
    const result = await appendSeCompanyAddressCorrection(built.input);
    return { ok: true as const, correctionId: result.correctionId };
  } catch (error) {
    // The validator's refusals are the reviewer's to read (a malformed
    // payload, an address that is no longer published, evidence that moved
    // while the page was open); anything else is a real failure.
    if (error instanceof SeAddressCorrectionValidationError) {
      return { ok: false as const, error: error.message };
    }
    throw error;
  }
}

export default function AdminSwedenCompanyAddress({
  loaderData,
  actionData,
}: Route.ComponentProps) {
  return (
    <SeCompanyAddressTab
      detail={loaderData.detail}
      result={actionData ?? null}
    />
  );
}
