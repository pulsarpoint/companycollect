import type { Route } from "./+types/admin-se-company-actions";
import { data } from "react-router";
import { launchCompanyAction } from "~/lib/company-actions.server";

export async function action({ request }: Route.ActionArgs) {
  try {
    const form = await request.formData();
    const changedOnly = form.get("changed_only") ?? "true";
    if (changedOnly !== "true" && changedOnly !== "false") throw new Error("changed_only must be true or false.");
    const verifyDomains = form.get("verify_domains") ?? "false";
    if (verifyDomains !== "true" && verifyDomains !== "false") throw new Error("verify_domains must be true or false.");
    // Every workflow owns its global scope. Company filters and selections are not forwarded.
    const result = await launchCompanyAction({
      area: String(form.get("area") ?? ""), operation: String(form.get("operation") ?? ""),
      profileId: String(form.get("profile_id") ?? ""), promptId: String(form.get("prompt_id") ?? ""),
      promptRevision: Number(form.get("prompt_revision")), changedOnly: changedOnly === "true",
      verifyDomains: verifyDomains === "true",
      llmMaxCompanies: Number(form.get("llm_max_companies")),
      requestedBy: process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice",
    });
    return data(result, { status: result.ok ? 200 : 409 });
  } catch (error) {
    return data({ ok: false, error: error instanceof Error ? error.message : "Could not submit the company action." }, { status: 400 });
  }
}
