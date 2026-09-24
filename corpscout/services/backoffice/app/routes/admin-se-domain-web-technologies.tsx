import { data } from "react-router";
import { AddDomainWebtechQueue } from "~/components/admin/add-domain-webtech-queue";
import { addDomainToWebtechQueue, WebtechQueueRequestError } from "~/lib/webtech-queue.server";
import { seDomainHref } from "~/lib/se-domains-filters";
import type { Route } from "./+types/admin-se-domain-web-technologies";
import { WebtechSection } from "~/components/detail/webtech-section";
import { getDomainWebtech } from "~/lib/webtech.server";

export async function loader({ params }: Route.LoaderArgs) {
  return getDomainWebtech(params.domain.trim().toLowerCase());
}

export async function action({request, params}: Route.ActionArgs) {
  const origin = request.headers.get("origin");
  if (origin && origin !== new URL(request.url).origin) return data({ok: false as const, error: "Invalid request origin."}, {status: 403});
  const form = await request.formData();
  if (form.get("intent") !== "add-webtech-input") return data({ok: false as const, error: "Choose a supported domain action."}, {status: 400});
  try {
    return data(await addDomainToWebtechQueue(params.domain, String(form.get("submissionId") ?? ""), process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice"));
  } catch (error) {
    if (error instanceof Response) throw error;
    return data({ok: false as const, error: error instanceof WebtechQueueRequestError ? error.message : "Could not confirm the queue import. Check Dagster before retrying; retrying keeps the same submission ID."}, {status: error instanceof WebtechQueueRequestError ? 400 : 502});
  }
}

export default function DomainWebTechnologies({
  loaderData,
}: Route.ComponentProps) {
  return <div className="flex flex-col gap-5">
    <AddDomainWebtechQueue key={loaderData.domain} domain={loaderData.domain} actionPath={`${seDomainHref(loaderData.domain)}/web-technologies`} />
    <WebtechSection data={loaderData} linkTechnologies />
  </div>;
}
