import { data } from "react-router";
import { retryBraveFailures } from "~/lib/se-company-brave.server";
export async function action({request}: {request: Request}) {
  const origin = request.headers.get("origin");
  if (origin && origin !== new URL(request.url).origin) return data({ok: false as const, error: "Invalid request origin."}, {status: 403});
  const form = await request.formData();
  try { return data(await retryBraveFailures(String(form.get("task") ?? ""), String(form.get("submissionId") ?? ""), process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice")); }
  catch (error) { return data({ok: false as const, error: error instanceof Error ? error.message : "Could not queue failed companies."}, {status: 400}); }
}
