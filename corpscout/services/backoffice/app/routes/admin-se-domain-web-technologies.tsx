import type { Route } from "./+types/admin-se-domain-web-technologies";
import { WebtechSection } from "~/components/detail/webtech-section";
import { getDomainWebtech } from "~/lib/webtech.server";

export async function loader({ params }: Route.LoaderArgs) {
  return getDomainWebtech(params.domain.trim().toLowerCase());
}

export default function DomainWebTechnologies({
  loaderData,
}: Route.ComponentProps) {
  return <WebtechSection data={loaderData} linkTechnologies />;
}
